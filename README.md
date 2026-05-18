# grpc-bench

Comparative benchmark harness for Solana Yellowstone gRPC providers.
Subscribes to two endpoints simultaneously, measures per-message arrival
deltas with kernel-level precision (Linux), and writes a single JSON
report.

## TL;DR — up and running in 5 minutes

```bash
# 1. Build (Linux x86_64) and grant the SCHED_FIFO capability.
cargo build --release
sudo setcap cap_sys_nice=eip target/release/grpc-bench

# 2. Provide your endpoint URLs + tokens.
export EP1_URL=<reference-endpoint>:10000   EP1_TOKEN=<token>
export EP2_URL=<system-under-test>:10000    EP2_TOKEN=<token>

# 3. Run a 90-second smoke against pump.fun.
target/release/grpc-bench \
  --endpoint1 "$EP1_URL" --x-token1 "$EP1_TOKEN" \
  --endpoint2 "$EP2_URL" --x-token2 "$EP2_TOKEN" \
  --programs pump-only.tsv \
  --slots 200 --commitment processed \
  --with-transactions --cpu-affinity auto \
  --output results/quick.json

# 4. Read the headline.
jq '{
  p50_account_ms:  .comparative.account_delay.p50,
  p50_tx_ms:       .comparative.transaction_delay.p50,
  capture_acc_pct: ((.metadata.total_account_updates[1]
                    / .metadata.total_account_updates[0]) * 1000 | floor / 10),
  ping_ep1_ms:     .endpoints[0].avg_ping_ms,
  ping_ep2_ms:     .endpoints[1].avg_ping_ms,
  ping_delta_ms:   (.endpoints[1].avg_ping_ms - .endpoints[0].avg_ping_ms),
  disconnects:     [(.stability.endpoint1.disconnects | length),
                    (.stability.endpoint2.disconnects | length)]
}' results/quick.json
```

A healthy run prints something like p50 ~7 ms, capture ~99%, zero
disconnects. Compare `p50_account_ms` against `ping_delta_ms`: if the
delta tracks the ping gap, the latency difference is geographic /
network. If the delta is larger than the ping gap, that's plugin-side
behavior worth investigating.

If anything looks wrong, see
[RUNBOOK §2 — posture check](RUNBOOK.md#2-test-1--posture-check-60-seconds)
to verify the host is configured correctly. For the full production
workload (23 programs, `--realtime`, hour soaks), follow the
[RUNBOOK](RUNBOOK.md) end-to-end.

## Start here

| If you want to… | Read |
|---|---|
| Set up a Linux host and run the benchmark | [`RUNBOOK.md`](RUNBOOK.md) |
| Use macOS for development / testing (not for production numbers) | [`MACOS.md`](MACOS.md) |
| See the headline findings (measured numbers, unique-vs-thorofare claims, known limitations) | [`FINDINGS.md`](FINDINGS.md) |
| Trace the chronological measurement history — rig progression, patch series, cross-validation | [`BENCHMARK_HISTORY.md`](BENCHMARK_HISTORY.md) |
| Understand the timing-precision design (kernel timestamps, RT scheduling, allocator, host posture) | [`PRECISION.md`](PRECISION.md) |
| Understand proto versions, supported plugins (richat / yellowstone-grpc-geyser), Helius LaserStream compatibility | [`PROTO.md`](PROTO.md) |
| Build from source | `cargo build --release` |
| See every flag | `target/release/grpc-bench --help` |

The runbook is two tests (~15 minutes total): a 60-second posture check
to verify the host is correctly configured, then a comparative
benchmark run that produces the headline numbers. An optional
[1-hour soak](RUNBOOK.md#5-optional--1-hour-soak) and
[thorofare cross-validation](RUNBOOK.md#4-thorofare-cross-validation-optional-10-minutes)
are available for deeper characterization.

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

Schema details live in the output JSON schema; treat the runbook's jq examples as the
canonical reading recipe.

## Status

v1. Works on Linux x86_64. Single-binary, no runtime dependencies
beyond glibc. macOS is supported as a development convenience only —
see [`MACOS.md`](MACOS.md) for the caveats.
