# grpc-bench benchmark history

This document captures the chronological progression of grpc-bench
measurements: the rigs we used, the patches we landed in response to
what each measurement showed, and the cross-validation against
`rpcpool/yellowstone-thorofare`. It is intended as a transparency
artifact for customers who want to know *how* the final-state numbers
were derived, not just what they are.

Customer-facing headline numbers and the operational guidance built
on them live in [`FINDINGS.md`](./FINDINGS.md) and
[`RUNBOOK.md`](./RUNBOOK.md); this file is the substrate.

---

## Methodology

All grpc-bench measurements presented here used:

- Two simultaneous Yellowstone gRPC subscriptions, one per endpoint,
  in the same process.
- `--realtime` for any saturating workload (≥ 16 receivers); off for
  thorofare cross-validation and other light loads.
- Monotonic-clock arrival timestamps captured immediately after
  protobuf decode (`Instant::now()`-equivalent on `CLOCK_MONOTONIC`);
  `SO_TIMESTAMPNS` is implemented but not yet wired into the tonic
  transport (deferred to v2 — see [`PRECISION.md`](./PRECISION.md)).
- Single per-program sub-subscription topology
  (`--accounts-programs-per-filter 1`) for measurement-optimized
  per-program numbers. Higher chunk sizes are used when an endpoint's
  concurrent-stream tier cap is reached (see
  [RUNBOOK — Endpoint stream caps](./RUNBOOK.md#endpoint-stream-caps-eg-qn-tiers-cap-at-25));
  the `KNOWN_HEAVY_PROGRAMS` always-split logic keeps the heavy
  programs individually measurable at any chunk size.
- Linux host with `performance` governor, `madvise` THP,
  jemalloc warm-start, and `setcap cap_sys_nice=eip` on the binary so
  `--realtime` can attach `SCHED_FIFO 50` without root.
- `--cpu-affinity auto` (or an equivalent hand-picked layout) so
  receivers + control are pinned away from the kernel-housekeeping
  cores 0–1.

All comparative latency numbers are `(ep2_arrival − ep1_arrival)` in
milliseconds. Positive = ep1 was faster (`ep1_faster` counter
increments). t-digest compression factor is 100 (sub-1% quantile
error on uniform inputs, comfortably within the manual validation runs's "p99 within
1% of true p99" target).

---

## Reference platforms used during development

All grpc-bench measurement work to date has happened on Digital
Ocean droplets in `nyc3`, against two QuickNode dedicated
yellowstone-grpc-geyser endpoints (US-east + EU-Frankfurt). Endpoints
are referenced only by role (`ep1`, `ep2`) throughout this history
to avoid customer-identifying URL leakage.

| Platform | Use | Notes |
|---|---|---|
| DO 8-vCPU Premium Intel (Xeon Gold 6248) | Initial development + the 23p saturation experiments | First rig where dispatcher CPU surfaced as the binding constraint |
| DO 16-vCPU Premium Intel | Mutex-ceiling identification + DashMap landing | First rig where doubling cores let us isolate "the ceiling is `Mutex<StreamMatchers>` contention, not core count" |
| DO 32-vCPU G-class (Xeon Platinum 8168 @ 2.70 GHz, single NUMA, NYC3) | All v40 validation runs in this history | Current reference rig; the binary's v1 ship-ready validation numbers come from here |
| Customer's AWS rig (~64 vCPU) | Pending | Not yet measured by us; expected to clear most ceilings the DO G-class hits |

All numbers in this history are from Linux rigs.
[`MACOS.md`](./MACOS.md) documents the dev-host posture for anyone
iterating on code; macOS measurements are not used in this document.

---

## Cross-validation against `rpcpool/yellowstone-thorofare`

Thorofare agreement criterion (the publishable agreement criterion). gRPC-bench and
thorofare 0.5.0 were run **simultaneously** against the same two
endpoints in the same time window. gRPC-bench was constrained to a
single-program filter (pump.fun) to match thorofare's
single-`--account-owner` topology; `--realtime` was turned **off** so
neither tool's scheduling posture biased the comparison.

| Metric | grpc-bench | thorofare | Δ |
|---|---|---|---|
| Slot first_shred p50 | 6.93 ms | 7.10 ms | −0.17 |
| Slot processed p50 | 6.70 ms | 6.85 ms | −0.15 |
| Slot confirmed p50 | 6.57 ms | 6.62 ms | −0.05 |
| Slot finalized p50 | 7.86 ms | 8.16 ms | −0.30 |
| Account delay p50 (pump.fun) | 7.16 ms | 6.94 ms | +0.22 |
| Ping ep1 | 2.22 ms | 2.32 ms | −0.10 |
| Ping ep2 | 85.36 ms | 80.75 ms | +4.61 (network jitter) |

**Every shared metric agrees within ±0.3 ms.** This validates the
timing path — the additional streams and dimensions grpc-bench
measures are real measurement, not artifacts of a divergent
implementation. (The 4.61 ms ping delta on ep2 is intercontinental
network jitter between the two tools' separate gRPC pings, not a
timing discrepancy.)

Originally measured 2026-05-16. Re-measurement after the v40 patch
series was not performed because none of v40's changes touch the
timing path; they affect dispatcher CPU cost and ring sizing, not
how arrival timestamps are captured.

---

## Rig-class progression (which finding came from which platform)

A history of what each platform's measurements added to the design.
Numbers in this section are quoted directly from internal measurement
logs; cross-references to those logs are noted where applicable.

### DO 8-vCPU — dispatch ceiling identified and lifted (2026-05-14)

The 8-vCPU rig was the first to push grpc-bench into saturation
hard enough to expose the single-threaded processor as the binding
constraint. Pre-refactor measurements on the 23-program workload:

| Workload | Capture ep1 | Capture ep2 |
|---|---|---|
| Pumpfun-only, dual-cmt, full topology | 100% | 100% |
| 23p × dual × tx × blocks | 19.2% | 19.5% |
| 23p × dual × accounts × blocks | 17.0% | 17.2% |
| 23p × single × tx | 18.3% | 18.6% |

All four 23p configs landed in the same ~18–20% band because the
single processor thread was the bottleneck (~3,740 ev/sec total
across both endpoints — independent of how many streams we trimmed).

**Refactor:** split into per-endpoint dispatcher threads, each owning
its rings + stability + cross-stream trackers, with a coordinator
thread for snapshots / stop conditions / output JSON assembly. Plus
per-sub-matcher locks (one `Mutex` per stream kind) so non-accounts
streams flow in parallel.

Post-refactor on the same 8-vCPU rig (three runs):

| Workload | Capture (post-refactor) |
|---|---|
| Pumpfun-only, dual-cmt, full | 100% / 100% (no regression) |
| 23p single-cmt + tx, RT + structured affinity | 97.5% / 95.3% |
| 23p × dual × full, RT + structured affinity | 60.3% / 59.9% |

Combined dispatch throughput rose from ~3,740 to ~12,000 ev/sec
sustained at saturation (≈ 3.2× ceiling lift).

### DO 16-vCPU — mutex ceiling confirmed, DashMap landed (2026-05-15)

Doubling usable cores from 6 to 14 lifted the 23p × dual × full
ceiling **zero percentage points**. Three independent variations of
the same workload all converged in the 67–69% band:

| Variation | ep1 | ep2 | n |
|---|---|---|---|
| 8 vCPU, per-sub-matcher locks, RT receivers, SCHED_OTHER dispatchers | ~67% | ~68% | multiple |
| 16 vCPU, same, no dispatcher RT | 66.2–68.9% (median 67.7%) | 67.7–69.4% (median 68.9%) | 3 |
| 16 vCPU, same, + dispatcher SCHED_FIFO 50 | 67.1% | 68.0% | 1 |

Independent diagnostic facts that pointed at the accounts `Mutex` as
the binding constraint:
- 16–17 of 23 programs had measurable traffic in 60 s — sharding by
  program would distribute work across many buckets, not just one.
- Stability disconnects: empty on both endpoints across all runs;
  ceiling was internal contention, not server-side kicks.
- Pumpfun-only stayed at 100% capture on the same rig — receivers
  and dispatchers had headroom; only `StreamMatchers` was saturated.

**DashMap-by-owner sharding inside `AccountMatcher`** (one
`PerProgramShard` per program-id key, each with its own
`PairMatcher`) was implemented same-day and re-measured on the same
16-vCPU rig:

| Variant | ep1 | ep2 | n |
|---|---|---|---|
| Mutex baseline | 67.7% | 68.9% | 3 |
| DashMap-by-owner sharding | 74.1% | 75.8% | 5 |
| **Δ** | **+6.4 pp** | **+6.9 pp** | |

The DashMap change removed the cross-endpoint accounts-`Mutex`
bottleneck cleanly. Subsequent long-duration measurements revealed
the 60-second 76% number was warm-up-inflated and the steady-state
dual-cmt capture on this rig class is closer to 50%; the next
binding constraint after the mutex was per-thread dispatcher CPU
cost on the hot accounts stream. Mitigated in the v40 patch series
below.

### DO 32-vCPU G-class — v40 validation reference rig (2026-05-17)

The 32-vCPU G-class rig is the current reference platform for all
v40-era measurements. Single-cmt + tx headline (300-second run,
chunk = 1, `--realtime`):

| Workload | 300s capture (pre-v40, dashmap-only) |
|---|---|
| Pumpfun-only, full topology | 100% / 100% |
| **23p single-cmt + tx, no blocks** | **100% / 100%** |
| 23p single-cmt + tx + blocks | 75% / 74% |
| 23p dual-cmt + tx, no blocks | 44% / 44% |
| 23p dual-cmt + tx + blocks | 45% / 45% |
| 23p dual-cmt, accounts only | 36% / 37% |

The dual-cmt configs on this rig class are dispatcher-CPU-bound.
The harness clears the ±0.1% parity criterion on all
non-saturated workloads (single-cmt + tx, single-cmt + blocks,
pumpfun-only); on the saturated dual-cmt configs, parity holds
between endpoints but the magnitude is reduced because the same
events are dropping symmetrically from both rings.

---

## v40 patch series — head-to-head against v39 (2026-05-17)

Five patches landed before the customer hand-off:

| # | Patch | Target |
|---|---|---|
| 1 | Lazy eviction in `PairMatcher` (batched every 256 observes) | Per-event `HashMap::retain` was dominating dispatcher CPU |
| 2 | Per-stream-kind ring sizing (accounts 4×, tx/entries 1×, blocks ½×, slots ⅛× of baseline) | Uniform ring sizes left accounts under-buffered and slots wildly over-buffered |
| 3 | `KNOWN_HEAVY_PROGRAMS` always-split (system, spl_token, token_2022) | Lets `--accounts-programs-per-filter > 1` chunk the long tail without contaminating heavy-program measurement |
| 4 | `--realtime` strips `--cpu-affinity proc=N` automatically | The combination wedged the coordinator on 16+ vCPU rigs (root cause unconfirmed; symptom reproduced reliably) |
| 5 | Explicit `std::process::exit(0)` after JSON write | Tokio runtime drop was hanging the process ~10 s past the durable output write |

Reference comparison: same 1000-slot × 23p × processed × tx ×
`--realtime` workload on the DO 32-vCPU rig, chunk = 1.

| Metric | v39 (final) | v40a | v40b (replay) |
|---|---|---|---|
| p50 accounts (overall) | 11.07 ms | 8.61 ms | 8.77 ms |
| p99 accounts (overall) | 201.39 ms | 151.23 ms | 129.37 ms |
| p50 tx | 8.24 ms | 7.99 ms | (not separately reported) |
| Capture (acc ep2/ep1) | 99.8% | 99.7% | 99.6% |
| matched_acc | 2,213,189 | 3,076,897 | 3,021,665 |
| total_acc_ep1 | 2,529,645 | 3,086,094 | (similar) |
| Disconnects | 0 / 0 | 0 / 0 | 0 / 0 |
| realtime_priority | true | true | true |

Two-run reproducibility envelope: p50 within ±0.16 ms, p99 within
±22 ms across the consecutive v40 runs. The latency improvements
reproduce across chain windows — they're not a one-off favorable
sample.

The matched/total deltas between v39 and v40 (+22% events received,
+39% events matched) reflect Solana mainnet running at a busier
chain rate during the v40 window than during the original v39 run.
Not a patch-driven effect. The right read is "v40 ran a 22% busier
chain at lower p50 AND lower p99 than v39 did."

### Per-program impact at the heavy programs (v40 1000-slot)

| Program | matched_v39 → v40 | p50 v39 → v40 (Δ) | p99 v39 → v40 (Δ) |
|---|---|---|---|
| `system` | 359k → 1.48M | 13.96 → 8.91 ms (−5.05) | 185 → 124 (−61) |
| `token_2022` | 496k → 323k | 12.64 → 7.87 ms (−4.77) | 160 → 91 (−69) |
| `meteora_dlmm` | 502k → 222k | 11.54 → 9.10 ms (−2.44) | 307 → 342 (+35) |
| `spl_token` | 449k → 733k | 8.43 → 8.44 ms (+0.01) | 126 → 100 (−26) |
| `pump_amm` | 250k → 145k | 8.00 → 7.70 ms (−0.30) | 125 → 101 (−24) |

The three biggest p50 wins are on the highest-volume programs.
`system` and `token_2022` were the programs most affected by the
per-event dispatcher scan that Patch 1 eliminated. The minor
"regressions" elsewhere are all under 1 ms p50 on programs with
<50k matched events — within sample noise.

**Note on the v39 capture number.** An earlier internal report cited
v39 single-cmt capture as 67.7% on this rig. Direct re-measurement
of the v39 output JSON during v40 validation showed the actual ep2/ep1
account-received ratio was 99.8% — the 67.7% figure had been computed
against a different denominator. The 67.7% number is preserved here
only as a historical entry; current ep2/ep1 wire-received parity on
v39 and v40 are both at-ceiling for this rig.

---

## 1-hour sustained-load soak (2026-05-17)

Same workload as the v40a/b 1000-slot runs but extended to a
60-minute duration to characterize behavior under sustained load
(the bounded-memory invariant's "bound memory at all times" invariant + tail-latency
behavior at high event counts).

**Configuration:** 23p × processed × tx × `--realtime` × chunk=1 ×
`--cpu-affinity auto` × `--duration 3600`. The `auto` affinity
derived `ep1=cores 2–15, ep2=cores 16–30, ctrl=core 31` on the
32-vCPU rig.

**Result:**

| Metric | Value |
|---|---|
| Duration | 3602.5 s (60.0 min) |
| Slots collected | 9,018 |
| Total account events ep1 / ep2 | 39,098,927 / 39,248,779 |
| Total tx events ep1 / ep2 | 2,701,672 / 2,700,868 |
| Capture (acc ep2/ep1) | **100.3%** |
| Capture (tx ep2/ep1) | 99.9% |
| Accounts p50 (overall) | 12.66 ms |
| Accounts p99 (overall) | 26,734 ms (see meteora_dlmm note below) |
| Tx p50 | 8.85 ms |
| Tx p99 | 198.87 ms |
| Disconnects | 0 / 0 |
| Stalls > 600 ms | 19 / 18 |
| Longest stall | 1,287.98 / 1,396.20 ms |
| Cross-stream tx_vs_account p50 | ep1 −0.79 ms, ep2 −0.50 ms |
| Ping | ep1 2.25 ms, ep2 83.95 ms |
| realtime_priority | true |

**Stalls were correlated chain-side, not provider-side.** The two
longest stalls on each endpoint occurred within ~57 ms wall-clock
of each other (ep1's longest at wall_ms 1779043146445, ep2's at
1779043146502). That synchronous pattern is the signature of a
Solana network-wide slot-delivery hiccup that both endpoints
observed simultaneously. A second correlated pair appeared ~3 s
later (ep1 1175.7 ms / ep2 1186.9 ms). If either endpoint were
independently flaky, the stalls would be uncorrelated; they were
not. Useful talking point for "worst-case stalls were chain-wide
events that any client at any provider would have seen."

**Slot stages.** All five stages matched the full 9,018 slots on
both endpoints with p50 between 7.3 and 8.7 ms:

| Stage | p50 |
|---|---|
| first_shred | 8.34 ms |
| completed | 7.32 ms |
| processed | 7.76 ms |
| confirmed | 8.68 ms |
| finalized | 8.07 ms |

---

## Known per-program limitation surfaced by the soak

`meteora_dlmm` has a catastrophic tail that only becomes visible at
sustained-load sample sizes:

| Program | matched (1 hr) | p50 | p90 | p99 | p99.9 |
|---|---|---|---|---|---|
| `meteora_dlmm` | 5,906,304 | 70.21 ms | 20,651 ms | 36,387 ms | 38,483 ms |
| All other 17 reporting programs | varies | 8–12 ms | < 130 ms | < 280 ms | < 600 ms |

That single program drags the overall `account_delay.p99` from
~200 ms (where it would land without meteora_dlmm) to 26.7 seconds.
v39 saw 307 ms p99 on a 7-min run; the hour-long soak revealed that
the *real* tail extends into the tens of seconds at saturating
volume.

This is **not a harness defect** — meteora_dlmm has been tail-prone
in every measurement on every rig. Almost certainly a provider-side
filter-matching cost specific to meteora_dlmm's update pattern, not
anything the harness can change. The other 17 programs (including
all three heavy programs — system, spl_token, token_2022) land at
sub-600 ms p99.9 even at full saturation over an hour.

For customer-facing reports: if a customer use case depends on
bounded meteora_dlmm latency, this characteristic is worth surfacing
explicitly. For most customer use cases (where the program-mix
average matters), the headline numbers without the meteora_dlmm
outlier (overall p99 ~200 ms instead of 26.7 s) are the more honest
read.

---

## What's still pending

| Item | Status |
|---|---|
| Customer-rig characterization on AWS 64-vCPU | Pending. The DO 32-vCPU G-class data above is rig-sizing characterization; the customer's silicon should clear most ceilings the G-class hit. |
| Dual-commitment v40 head-to-head capture number | Pending direct measurement. v40 dual-cmt verified to run cleanly under chunk=4 on a 25-stream-cap tier; v39-era projection was ~67% on this rig class for the same workload, and Patch 1 specifically attacks the dispatcher CPU constraint that produced that ceiling. |
| 24-hour soak | Not run. The 1-hour soak is sufficient to validate the bounded-memory invariant bounded memory and surface meteora_dlmm-class tails; 24-hour would primarily exercise endpoint-side reliability over an operational day, which is a customer-rig concern. |
| `SO_TIMESTAMPNS` wire-up | Deferred to v2. Primitives in `src/timing/kernel_ts.rs`; tonic transport integration is days of work. Would close the residual ~3 ms standalone-vs-parallel gap and make sub-10 ms p50 claims rigorously defensible — useful but not load-bearing for v1. |
| Per-stream-kind dispatcher refactor | Deferred to v2. Would lift the 2-dispatcher-thread ceiling on heavy dual-cmt workloads. Not needed for single-cmt + tx, which already clears 99%+ at v40. |
| `entries_vs_*` cross-stream metrics | Deferred to v2 behind feature flag. Requires QN-specific proto extension; tx_vs_account is fully populated in v1. |

---

## How to reproduce any of these measurements

The full operating procedure (host prep, posture check, three
command forms for the comparative run, optional thorofare
cross-validation, optional 1-hour soak) lives in
[`RUNBOOK.md`](./RUNBOOK.md). The customer-facing headline numbers
and the unique-vs-thorofare claims live in
[`FINDINGS.md`](./FINDINGS.md). The timing-precision design (why
each posture knob matters) is in [`PRECISION.md`](./PRECISION.md).
Yellowstone proto / plugin compatibility is in
[`PROTO.md`](./PROTO.md).

This file is the substrate that ties the customer-facing claims back
to the chronological measurement work that produced them. If a
specific number in `FINDINGS.md` is ever questioned, the supporting
run and rig context lives here.
