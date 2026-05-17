# Proto sourcing policy

This document explains the proto-version policy of grpc-bench in
operator-facing terms — what's supported, how to upgrade when a new
Yellowstone release ships, and what to do if `GetVersion` reports
something unexpected.

## What we depend on

grpc-bench pulls the standard Yellowstone gRPC proto and client from
crates.io. The exact versions are recorded in `Cargo.lock` and surface
in every output JSON's `proto_metadata` block:

```jsonc
"proto_metadata": {
  "yellowstone_proto_crate_version":  "12.3.0",
  "yellowstone_grpc_client_crate_version": "13.1.0",
  "endpoint1_server_plugin_version":  "...",
  "endpoint2_server_plugin_version":  "...",
  "compatibility_warnings":           ["..."]
}
```

Pin form in `Cargo.toml`:

```toml
yellowstone-grpc-proto  = "~12"
yellowstone-grpc-client = "~13"
```

These are independently-versioned. The client crate moved to major 13
while the proto crate is at major 12 — client 13.1 depends on proto
`^12.2`. We pin each to caret-major so patch updates land automatically
without surprising minor / major bumps.

## Subscribe Entries: the standard filter, not a Quicknode extension

The early draft spec described `SubscribeEntries` as a Quicknode-specific
proto extension carrying transaction signatures. During Phase 1 it
turned out:

1. Standard `yellowstone-grpc-proto 12.3.0` already includes
   `SubscribeRequestFilterEntry` and `SubscribeUpdateEntry` in the same
   `Subscribe` RPC. The harness uses that filter.
2. The standard `SubscribeUpdateEntry` does **not** carry transaction
   signatures — only `slot`, `index`, `num_hashes`, `hash`,
   `executed_transaction_count`, and `starting_transaction_index`.
3. There is no publicly-published Quicknode crate or `.proto` exposing a
   signature-enriched entries variant (per `cargo search` and a sweep of
   Quicknode public docs / SDK repos in May 2026).

The Phase 1 resolution is therefore: **use the standard Yellowstone
entries filter**. `--entries-endpoint{1,2}` are optional CLI flags that
let the operator route entries to a separate URL (e.g. an entries-only
Quicknode endpoint); when omitted, no entries are subscribed.

### What this means for cross-stream metrics

The spec's `entries_vs_tx` and `entries_vs_account` cross-stream
metrics need a way to pair an entry with the transactions / accounts it
contained. Because the standard `SubscribeUpdateEntry` lacks
signatures, the harness emits `null` for both metrics in v1, with a
note in the `cross_stream.<endpoint>.notes` field that explains the
deferral. The `tx_vs_account` cross-stream metric is fully implemented
because the account stream's `txn_signature` field provides the join
key.

A future implementation can derive the entry→signature mapping by
combining `SubscribeUpdateEntry::starting_transaction_index` +
`executed_transaction_count` with the per-slot tx index from
`SubscribeUpdateTransactionInfo::index`. That join is tracked as a v2
item.

## Minimum supported server versions

Per spec §12.A:

| Plugin                    | Minimum baseline |
|---------------------------|------------------|
| `yellowstone-grpc-geyser` | 12.2.0           |
| `richat`                  | 2.1.0            |

The harness calls `GetVersion` on every endpoint at startup, parses the
returned JSON blob (the standard format used by `yellowstone-grpc-geyser`
and richat), and evaluates compatibility:

| Server reports                                  | Outcome             |
|-------------------------------------------------|---------------------|
| Same major as harness build                     | `Accept`            |
| Newer than harness build                        | `Warn` + continue   |
| Older major than harness build                  | `RefuseOlderMajor`  |
| Same major, older minor / patch than build      | `RefuseOlderMinor`  |
| Below explicit plugin baseline (table above)    | `RefuseBaseline`    |
| No parseable proto version reported             | `Unknown` + continue|

A refusal aborts the run with a clear error message. Compatibility
warnings (newer-than-build, missing fields, non-JSON version blob) end
up in `proto_metadata.compatibility_warnings` so the run can still
proceed and the operator has the audit trail.

## Helius LaserStream connectivity note

This harness targets the **standard Yellowstone-compatible interface**
only. Helius dedicates two paths:

1. **Managed Helius LaserStream** — uses a custom SDK; not Yellowstone-
   compatible at the wire level. **Will fail at handshake here.**
2. **Helius dedicated nodes** — exposes the Yellowstone interface
   (richat-backed; `GetVersion` reports `package: "richat"`). **This is
   the target.**

If a user points grpc-bench at the LaserStream SDK endpoint, the
`Subscribe` open will return a tonic error and the harness logs it
under the affected endpoint. The error message guides the operator to
switch to a dedicated-node endpoint.

## Upgrading the harness when Yellowstone ships a new release

1. Run `cargo update -p yellowstone-grpc-proto -p yellowstone-grpc-client`.
2. Rebuild. `build.rs` will re-read `Cargo.lock` and update the
   `GRPC_BENCH_YELLOWSTONE_PROTO_VER` /
   `GRPC_BENCH_YELLOWSTONE_CLIENT_VER` env vars; the result JSON's
   `proto_metadata` will reflect the new versions on the next run.
3. Run the full test suite (`cargo test`).
4. Run one of the §10 manual validation runs against a known-good
   endpoint to confirm decode still works.

If the new release deprecates a field grpc-bench reads (account
identity tuple, slot status enum values), it will surface as a decode
error or unknown-enum-discriminant rather than silent skipping. The
output JSON's `metadata.dropped_events_*` counters will spike, which is
the operator-visible failure signal.

## What to do if `GetVersion` reports an unknown plugin

The `proto::evaluate` function in `src/proto.rs` only enforces explicit
baselines for `yellowstone-grpc-geyser` and `richat`. If the server's
`package` is anything else (an in-house fork, a new plugin), the
plugin-specific baseline check is skipped and only the proto-version
check applies. This lets the harness work against forks that bump
versions independently. The unknown-package case appears in
`proto_metadata.compatibility_warnings` so the operator sees it.

## Never vendor stale `.proto` files

The spec is explicit (§12.A): "Never vendor `.proto` files into this
repository. Never copy proto message definitions into Rust source."
grpc-bench follows this rule: there is no `proto/` directory and no
`build.rs` codegen of Yellowstone types. All proto types come through
the `yellowstone-grpc-proto` crate dependency.

The only place this rule is consciously relaxed is the Phase 1 design
note above: if a future implementer obtains a signature-enriched
entries proto from Quicknode, it would land under `proto/quicknode/`
with a clear source-URL and date header, and be codegen'd via
`tonic-build` in `build.rs`. That codepath is documented in spec §12.A
but is not in use today.
