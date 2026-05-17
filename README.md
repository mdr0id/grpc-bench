# grpc-bench

Comparative benchmark harness for Solana Yellowstone gRPC providers.
Subscribes to two endpoints simultaneously, measures per-message arrival
deltas with kernel-level precision (Linux), and writes a single JSON
report.

## Start here

| If you want to… | Read |
|---|---|
| Set up a Linux host and run the benchmark | [`RUNBOOK.md`](RUNBOOK.md) |
| See the customer-facing findings (headline numbers, unique-vs-thorofare claims, known limitations) | [`FINDINGS.md`](FINDINGS.md) |
| Trace the chronological measurement history — rig progression, patch series, cross-validation | [`BENCHMARK_HISTORY.md`](BENCHMARK_HISTORY.md) |
| Understand the timing-precision design (kernel timestamps, RT scheduling, allocator, host posture) | [`PRECISION.md`](PRECISION.md) |
| Understand proto versions, supported plugins (richat / yellowstone-grpc-geyser), Helius LaserStream compatibility | [`PROTO.md`](PROTO.md) |
| Build from source | `cargo build --release` |
| See every flag | `target/release/grpc-bench --help` |

The runbook is two tests (~15 minutes total): a 60-second posture check
to verify the host is correctly configured, then a comparative
benchmark run that produces the headline numbers. An optional
1-hour soak (RUNBOOK §5) and thorofare cross-validation (§4) are
available for deeper characterization.

## What this is not

- Not for measuring RPC method latency (`getAccountInfo`, etc.) — gRPC
  streams only.
- Not for measuring outbound transaction landing rate — that's a
  different benchmark.
- Not for distributed multi-region coordination — run from one host in
  the same region as the endpoints.
- Not for the Helius managed LaserStream SDK product. Helius dedicated
  nodes (richat-backed) are supported; see PROTO.md.

## Output

Every run writes a single JSON file:

- `host_metadata` — kernel, governor, THP, NTP, allocator, RT outcome,
  affinity, warnings
- `proto_metadata` — Yellowstone proto/client crate versions, plugin
  versions reported by both endpoints, compatibility warnings
- `config` — echo of the parsed CLI (tokens redacted)
- `metadata` — slot count, duration, per-kind totals, dropped events
- `endpoints[]` — per-endpoint plugin info and total event counts
- `comparative.{slot_status, account_delay, transaction_delay,
  block_delay}` — t-digest summaries (p50/p90/p99/p99.9) and matched /
  ep1_faster / ep2_faster counts
- `per_program_account_delay` — same shape, bucketed per program in the
  filter
- `cross_stream` — intra-endpoint stream-ordering metrics
- `stability` — slot-gap distribution, stalls, disconnects, reconnect
  TTFM, processed↔confirmed drift

Schema details live in spec §8; treat the runbook's jq examples as the
canonical reading recipe.

## Status

v1. Works on Linux x86_64. Builds and runs on macOS for development but
the precision posture is Linux-only (see PRECISION.md). Single-binary,
no runtime dependencies beyond glibc.
