# Precision posture

This document explains each timing-related design choice in grpc-bench,
why it matters at sub-10ms deltas, and how to verify the choice is
working at runtime.

## Why precision matters here

The deltas we're trying to measure between two gRPC providers in `us-east-1`
are typically in the **2 – 15 ms** range with sub-2 ms TCP path differences.
At that scale, receiver-side noise from tokio scheduling, kernel buffering,
allocator pressure, and timer coalescing is a meaningful fraction of the
signal. Without controlling for those sources, the numbers are not
defensible.

The harness controls for them by:

1. Using kernel-level receive timestamps where possible.
2. Capturing both a monotonic and a wall clock per event, using the
   monotonic for all duration arithmetic.
3. Pinning receiver threads to specific CPUs.
4. Requesting realtime scheduling priority on receivers when run with the
   appropriate capability.
5. Warming the allocator before subscriptions open so the first message
   doesn't pay the first-allocation tax.
6. Keeping the hot path lock-free and allocation-free.

Each of these is described below.

## Kernel timestamps (`SO_TIMESTAMPNS`, Linux only)

**What.** Enabling `SO_TIMESTAMPNS` on the underlying TCP socket asks the
kernel to attach a `CLOCK_REALTIME` timestamp to every received TCP
segment. On each `recvmsg`, the timestamp arrives in an `SCM_TIMESTAMPNS`
cmsg. This is the canonical "wire arrival" time — the moment the first
byte of the segment was received in the kernel, before user-space sees
anything.

**Why it matters.** Without kernel timestamps, the per-event arrival
time has to be captured in user space, where tokio scheduling and protobuf
decode add 100s of microseconds to a millisecond of jitter on a busy
system. At sub-10 ms deltas, that jitter degrades the signal-to-noise.

**Status today.** The setsockopt and cmsg-parser are implemented in
`src/timing/kernel_ts.rs` (Linux only, with `// SAFETY:` annotations per
). The integration with `tonic` 0.14 — routing decoded gRPC
frames back to the per-segment timestamp — requires a custom tonic
connector that owns the `TcpStream` and exposes its fd. That refactor is
a follow-up. **Today, grpc-bench uses fallback path**: capture
`Instant::now()` immediately after the protobuf decode call returns. A
prominent warning at startup flags this, and the result JSON's
`host_metadata.kernel_timestamps = false` records it for downstream
analysis.

**How to verify the active path.** Check `host_metadata.kernel_timestamps`
in the output JSON:

| Value | Meaning |
|-------|---------|
| `true`  | Kernel timestamps are active. Receiver arrival is from `SCM_TIMESTAMPNS`. |
| `false` | Fallback active. Arrival is `Instant::now()` post-decode. Warning is in `host_metadata.warnings`. |

When the value is `false`, `host_metadata.warnings` will contain the
text `"SO_TIMESTAMPNS unavailable — receive timestamps will be captured
in user space after protobuf decode."`

## Monotonic + wall clock pair

**What.** Every event records both:
- `mono_ns: u64` — from `CLOCK_MONOTONIC` (or `Instant::now().elapsed()`
  on the fallback path), used for all duration math.
- `wall_ms: u64` — milliseconds since the Unix epoch, used only for UI
  timeline and reconnect-correlation.

**Why it matters.** The wall clock can slew via NTP, jump on container
clock resync, and incorporate leap seconds. Subtracting two wall-clock
values to compute a delta produces garbage in any of these conditions.
The monotonic clock never goes backward and is unaffected by NTP, which
is the property we need for `(ep2_arrival - ep1_arrival)`.

**How to verify.** Inspect any raw record with `--raw-records`. Every
line carries both fields. The summary JSON's duration_ms is computed
from monotonic; the wall start time appears only as `run_started_wall_ms`
and `run_started_iso`.

## Receiver thread pinning (`sched_setaffinity`, Linux only)

**What.** When `--cpu-affinity` is set, the harness pins receiver
threads to specific cores via `sched_setaffinity`. Three forms:

- `--cpu-affinity auto` (recommended): derives the layout from the
  host's `nproc`. Reserves cores 0–1 for the kernel, the highest
  core for the control thread, and splits the remainder 50/50
  between ep1 and ep2.
- `--cpu-affinity ep1=2,3,4:ep2=5,6,7:ctrl=8` (structured): hand-pick
  cores per role. Required for NUMA alignment or shared-rig
  scenarios.
- `--cpu-affinity 2,3,4,5` (legacy flat): ep1=2, ep2=3, proc=4,
  ctrl=5. Preserved for older operator scripts; otherwise prefer
  one of the two forms above.

Cores 0–1 are left to the kernel in every form. The `proc=N` pin is
automatically stripped under `--realtime` (a known wedge interaction
on 16+ vCPU rigs).

**Why it matters.** When two receivers share a CPU core, their scheduler
quantum boundaries create correlated jitter that distorts the delta you
care about. Pinning each receiver to a separate core decouples them.

**Status today.** Implemented in `src/collect/mod.rs::apply_cpu_affinity`
(Linux) with stubs elsewhere. Linux uses the `nix` crate's safe wrapper
around `sched_setaffinity`.

**How to verify.** `host_metadata.cpu_affinity` echoes the configured
core list. If the syscall failed, `host_metadata.warnings` will contain
the error.

## Realtime scheduling (`SCHED_FIFO` priority 50, Linux only)

**What.** When `--realtime` is set, receiver threads request `SCHED_FIFO`
with priority 50.

**Why it matters.** Under `SCHED_OTHER` (the default), any other process
sharing a core can preempt the receiver. Under `SCHED_FIFO`, the
receiver runs until it voluntarily yields (or higher-priority RT work
arrives, which on a clean benchmark host shouldn't exist). This removes
preemption jitter from the tail of the latency distribution.

**Requires.** `CAP_SYS_NICE` or running as root. If the syscall is
rejected, the harness fails loud at startup rather than silently
continuing with `SCHED_OTHER` — that would be invisible degradation.

**How to verify.** `host_metadata.realtime_priority` reports whether the
priority was actually applied. If `--realtime` was requested but
rejected, `host_metadata.warnings` will say:

> SCHED_FIFO requested via --realtime but the syscall was rejected;
> continuing with default scheduling. Run as root or grant CAP_SYS_NICE
> for credible measurements.

## Allocator (`jemalloc`)

**What.** The release build links `tikv-jemallocator` and uses it as
the global allocator.

**Why it matters.** glibc's default malloc has more pathological tail
behaviour under sustained allocation churn (per-thread arenas,
fragmentation, brk() calls). jemalloc's tail is tighter, which matters
when measuring p99 latency.

**Warm-up.** `src/bin/grpc-bench.rs::warm_allocator` allocates and
drops ~64 MB of varied-size buffers before any subscription opens. This
pre-faults the arenas so the first received message doesn't pay the
first-allocation tax.

**How to verify.** `host_metadata.allocator` reports `jemalloc` on a
release build.

## Lock-free hot path and pre-allocated rings

**What.** The receiver thread does not allocate, lock, or block. Per-stream
events flow through `crossbeam_channel::bounded(N)` SPSC rings sized to
~16 K events. On overflow the receiver drops events and increments a
counter; it never blocks waiting for ring space.

**Why it matters.** Per the spec, "a `Mutex<Vec<Event>>` shared across
endpoint readers is not acceptable." A shared lock would correlate the
two receivers' timing in exactly the way we're trying to avoid.

**How to verify.** `metadata.dropped_events_ep1` and
`metadata.dropped_events_ep2` in the output JSON should both be 0 on a
healthy run. If they're non-zero, either the ring capacity is
under-sized for the offered rate or the ingest thread is falling behind.

## Disable timer coalescing

**What.** The tokio runtime is built with explicit worker count from the
CPU affinity list. Receiver threads run on dedicated OS threads with a
`current_thread` tokio runtime each, isolating their timer wheel from
the main runtime.

**Why it matters.** A shared multi-thread runtime can coalesce timer
fires across tasks for cache efficiency, which adds jitter to wake-ups.
Single-task `current_thread` runtimes don't have that coalescing.

**How to verify.** This is a structural property of the receiver design;
no runtime knob exposes it. The code is in
`src/run.rs::receiver_thread_main`.

## NTP sync check

**What.** At startup, the harness queries `timedatectl status` (then
`chronyc tracking` as a fallback) to determine whether the system clock
is NTP-synced.

**Why it matters.** When `--reconnect-test` is set, post-reconnect
correlations depend on a stable wall clock. A drifting clock can also
push the monotonic-vs-wall translation off, which matters for
24-hour soaks.

**How to verify.** `host_metadata.ntp_synced` reports `true`, `false`,
or `null` (when no NTP tool is available).

## CPU governor and transparent hugepages

**What.** The harness reads `/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor`
and `/sys/kernel/mm/transparent_hugepage/enabled` at startup and emits
warnings if either is set to a value that introduces measurable jitter.

| Governor    | Verdict | Why |
|-------------|---------|-----|
| `performance` | OK    | Clock frequency stays pinned at max. |
| `powersave`   | WARN  | Frequency floats; ramps add tail latency. |
| `schedutil`   | (no warn yet) | Behaves similarly to `performance` under load but the policy can deboost during quiet periods. |

| THP enabled | Verdict | Why |
|-------------|---------|-----|
| `always`    | WARN   | TLB-shootdown events appear as multi-ms spikes. |
| `madvise`   | OK     | Pages allocated only when explicitly requested. |
| `never`     | OK     | No hugepages, no shootdowns. |

**How to verify.** `host_metadata.cpu_governor` and
`host_metadata.transparent_hugepage` echo the observed values.

## Output-JSON timing provenance fields

Quick reference for which fields tell you the timing-precision posture
of a given run:

| JSON path                                   | Tells you                                    |
|---------------------------------------------|----------------------------------------------|
| `host_metadata.kernel_timestamps`           | Wire arrival vs user-space fallback          |
| `host_metadata.realtime_priority`           | `SCHED_FIFO` priority was actually granted   |
| `host_metadata.cpu_affinity`                | Pinned cores (may be empty if `--cpu-affinity` not set) |
| `host_metadata.allocator`                   | `jemalloc` (Linux release) or `system`       |
| `host_metadata.cpu_governor`                | Live cpufreq governor                        |
| `host_metadata.transparent_hugepage`        | THP policy                                   |
| `host_metadata.ntp_synced`                  | NTP daemon reports sync                      |
| `host_metadata.warnings`                    | Aggregated text of every above check that failed |
| `metadata.dropped_events_ep1` / `_ep2`      | Ring overflow on the receiver                |

If a result JSON's `host_metadata.warnings` is empty and `kernel_timestamps`,
`realtime_priority`, `cpu_affinity` are all non-default, you're looking
at a run with the full precision posture described in this document.

## What's deferred to a follow-up

- Wiring the kernel-timestamp cmsg path into tonic's per-frame receive
  path. The current implementation has the syscall plumbing but the
  default codepath is the user-space fallback. Requires a custom tonic
  `Connector` that exposes the `TcpStream` fd.
- Per-receiver `SCHED_FIFO` outcome tracking. Today the binary requests
  the policy and the receiver thread reports success/failure into the
  log, but `host_metadata.realtime_priority` is reported as the union of
  receiver outcomes only approximately (every-or-nothing). A per-stream
  breakdown would make the failure mode where only one of the receivers
  got `SCHED_FIFO` visible.
