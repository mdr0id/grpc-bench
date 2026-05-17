# grpc-bench on macOS

The harness **builds and runs** on macOS x86_64 / arm64, but **cannot
produce defensible sub-10 ms latency numbers** there. macOS is
supported as a development convenience only — for iterating on code,
running tests, and verifying behavior end-to-end against mock
endpoints. Any number that needs to be defended should come from a
Linux host.

## Why Linux-only for real measurements

The features that make grpc-bench's measurements credible at the
sub-10 ms scale are all Linux-only:

| Feature | macOS state |
|---|---|
| `sched_setaffinity` (CPU pinning) | Not available. `--cpu-affinity` is silently ignored. |
| `SCHED_FIFO` realtime priority | Not available. `--realtime` is silently ignored; `host_metadata.realtime_priority` reports `false`. |
| `SO_TIMESTAMPNS` kernel timestamps | Not available. (Linux uses user-space timestamps in v1 too; on Linux v2 this becomes available.) |
| jemalloc warm-start | Disabled — the Linux build uses `tikv-jemallocator`; macOS builds use the system allocator. `host_metadata.allocator` reports `system`. |
| `madvise` THP, `performance` CPU governor | Linux kernel concepts; no macOS equivalent. |

Without these, measured deltas carry tokio scheduling jitter (100s of
µs to ~1 ms on a busy macOS box), allocator pathological tails under
sustained churn, and CPU frequency scaling. The measurements may
*look* similar to Linux numbers but are not directly comparable —
particularly in the p99 tail.

## What macOS is good for

- `cargo test --release` — full unit + integration test suite runs
  cleanly. The 151 unit + 3 integration tests don't depend on any
  Linux-only feature.
- `cargo clippy --release --all-targets` — lint pass.
- Iterating on code, doc edits, schema changes.
- Running the binary against mock endpoints (the integration tests
  do this) to verify end-to-end behavior.
- Running against live endpoints for *qualitative* sanity (does it
  connect, decode, exit cleanly) — not for numbers you'd publish.

## What macOS will report in `host_metadata`

A run on macOS produces an output JSON with these fields populated:

```json
{
  "host_metadata": {
    "realtime_priority": false,
    "kernel_timestamps": false,
    "allocator": "system",
    "cpu_governor": null,
    "transparent_hugepage": null,
    "warnings": [
      "SCHED_FIFO requested via --realtime but the syscall is not available on this platform",
      "SO_TIMESTAMPNS unavailable — receive timestamps will be captured in user space after protobuf decode",
      "jemalloc not linked on macOS — using system allocator"
    ]
  }
}
```

The presence of any warning in `host_metadata.warnings` is the
canonical signal that the measurement is not Linux-grade. Downstream
consumers of the JSON should treat any run with non-empty warnings as
development-only.

## Recommended dev posture

If you're iterating on macOS:

1. Use the binary at `target/release/grpc-bench` (not `--debug`) — the
   release build matches what runs in production, and the
   feature-detection paths exercise more code.
2. Don't pass `--realtime` or `--cpu-affinity` — they're no-ops and
   the warnings clutter the output.
3. For end-to-end checks against a live endpoint, use the smallest
   filter set (`pump-only.tsv` or `sanity-programs.tsv`) so you
   don't burn endpoint quota chasing development questions.
4. Move to a Linux host for any measurement you'd put in a report.
   The [RUNBOOK](./RUNBOOK.md) lists the host-prep recipe.
