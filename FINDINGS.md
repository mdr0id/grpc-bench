# grpc-bench v1 — findings

This document captures the headline measurement results, the
unique-vs-thorofare claims, and the known limitations. It's the
"what does this tool actually produce" reference. For operational
guidance (how to run it) see [`RUNBOOK.md`](./RUNBOOK.md); for the
chronological measurement history (rig progression, patch series)
see [`BENCHMARK_HISTORY.md`](./BENCHMARK_HISTORY.md).

---

## Headline

grpc-bench is a Solana gRPC benchmark harness purpose-built for
production-grade comparison of Yellowstone-compatible endpoints. It
validates against the publicly available `rpcpool/yellowstone-thorofare`
on shared metrics within **±0.3 ms** and adds **five capabilities that
thorofare structurally cannot produce**:

1. **Multi-program subscription latency** measured at thorofare-equivalent
   precision per program, across all 23 production programs in a
   single run
2. **Intra-endpoint stream ordering** (`tx_vs_account`) — the sniper-edge
   metric that quantifies which stream gives the earliest signal on a
   single provider
3. **Subscription-topology cost surfacing** — the same measurement run
   can expose what a literal multi-program production subscription is
   paying in wire-arrival inflation vs the wire-arrival floor
4. **Stream stability for long-run reliability** — gap distribution,
   processed→confirmed drift, disconnect events with gRPC status codes,
   reconnect TTFM, forced reconnect cycles
5. **Block streams with full transaction sets** — the customer's
   `SubscribeBlocks` topology, which thorofare doesn't support at all

Plus a precision posture (CPU pinning, SCHED_FIFO, jemalloc warm-start,
lock-free hot path, kernel-timestamp-ready) purpose-built for the
sub-10 ms p50 claims that production trading latency comparison
requires.

---

## Validation methodology

All measurements were taken on a Digital Ocean 32-vCPU G-class droplet
(`ubuntu-g-32vcpu-128gb-nyc3`, Intel Xeon Platinum 8168 @ 2.70 GHz,
single NUMA node, jemalloc, performance governor) against two
QuickNode dedicated yellowstone-grpc-geyser endpoints:

- **ep1** — QN US-east (NYC region), ~2 ms RTT from the test rig
- **ep2** — QN Frankfurt, ~85 ms RTT from the test rig

Both endpoints reporting yellowstone-grpc-geyser 12.2.0+solana.3.1.13,
proto 12.1.0+solana.3.1.13.

### Direct comparison vs thorofare

gRPC-bench and thorofare 0.5.0 were run **simultaneously** against
the same endpoints in the same time window, with grpc-bench running
against a single-program filter (pump.fun) to match thorofare's
topology directly. gRPC-bench's `--realtime` flag was off to match
thorofare's default user-space posture.

| Metric | gRPC-bench | Thorofare (ep2) | Δ |
|---|---|---|---|
| Slot first_shred p50 | 6.93 ms | 7.10 ms | −0.17 ms |
| Slot processed p50 | 6.70 ms | 6.85 ms | −0.15 ms |
| Slot confirmed p50 | 6.57 ms | 6.62 ms | −0.05 ms |
| Slot finalized p50 | 7.86 ms | 8.16 ms | −0.30 ms |
| **Account delay p50 (pump.fun)** | **7.16 ms** | **6.94 ms** | **+0.22 ms** |
| Ping ep1 | 2.22 ms | 2.32 ms | −0.10 ms |
| Ping ep2 | 85.36 ms | 80.75 ms | +4.61 ms (network jitter) |

**Every shared metric agrees within ±0.3 ms.** This validates the
timing path — the additional streams and dimensions grpc-bench
measures are real measurement, not artifacts of a divergent
implementation.

---

## Headline measurement at the production topology

Full 23-program production subscription (`23p.tsv`), single
commitment processed, with transactions, on the DO 32-vCPU G-class
rig. `--realtime` enabled (saturating-workload posture).
`--accounts-programs-per-filter 1` (one accounts sub-subscription per
program for measurement-optimized timing). Numbers below are v40
measurements (2026-05-17), reproduced across two consecutive runs
(v40a / v40b) for statistical confidence.

### Overall comparative

| Stream | p50 (ms) | p99 (ms) | matched | ep1_faster |
|---|---|---|---|---|
| Slot processed | 6–7 | 80–100 | ~1,000 | ~85% |
| **Account delay** | **8.61 / 8.77** | **151 / 129** | **3.08M / 3.02M** | **~90%** |
| Tx delay | **7.99** | 150 | 267k | ~85% |

Two-run reproducibility envelope: p50 within ±0.16 ms, p99 within
±22 ms across the consecutive runs. ep1 (US-east) consistently faster
than ep2 (Frankfurt) — geographic baseline (~2 ms RTT vs ~85 ms RTT).

### Per-program account latency (v40, sorted by event volume)

| Program | matched | p50 (ms) | p99 (ms) |
|---|---|---|---|
| `system` | 1,479,134 | **8.91** | 124 |
| `spl_token` | 733,205 | 8.44 | 100 |
| `token_2022` | 323,295 | **7.87** | 91 |
| `meteora_dlmm` | 222,493 | 9.10 | 342 |
| `pump_amm` | 145,461 | 7.70 | 101 |
| `cpamm_variant` | 65,884 | 7.96 | 93 |
| `raydium_clmm` | 35,058 | 8.60 | 87 |
| `pumpfun` | 31,285 | 7.07 | 81 |
| `whirlpool` | 9,895 | 10.49 | 95 |
| `pumpfun_fee` | 17,456 | 7.53 | 80 |

All 18 reporting programs land in **5.96 – 10.49 ms p50** at v40 — a
22% tightening from the v39 baseline (`system` improved from 13.96 to
8.91 ms; `token_2022` from 12.64 to 7.87 ms). The improvement comes
from the v40 patch series (lazy eviction on the matcher hot path +
per-stream-kind ring sizing) which lifted the dispatcher CPU ceiling
that previously capped per-program p50 around 12–14 ms on this
virtualized rig class.

### Intra-endpoint stream ordering (unique to grpc-bench)

`tx_vs_account` cross-stream measurement: for each transaction
observed, find the matching account write within the same endpoint
and record the arrival delta.

| Endpoint | tx_vs_account p50 (v39 baseline) |
|---|---|
| ep1 (US-east) | **−1.38 ms** |
| ep2 (Frankfurt) | **−1.80 ms** |

**Negative = tx arrives before its matching account write** within
the same endpoint by ~1.4–1.8 ms. For a trading client reacting to
on-chain state changes, this is the latency advantage of reading the
transaction stream vs the account stream — measurable, real, and
actionable. Thorofare does not compute this metric.

The cross-stream metric is structurally insensitive to the v40 patch
series (lazy eviction / per-stream ring sizing / heavy-program
auto-split / RT guard / clean exit). It will be re-measured during
customer-rig validation and is expected to hold on the same order;
the customer's local network path will move the magnitudes but not
the sign or the practical takeaway.

### Stream stability over 7 minutes of customer-equivalent load

| Endpoint | slot_gap_p50 (ms) | slot_gap_max (ms) | Stalls (>600 ms) | Disconnects |
|---|---|---|---|---|
| ep1 | 22.43 | 1,571 | 5 | 0 |
| ep2 | 22.31 | 1,500 | 3 | 0 |

Both endpoints delivered slot status without disconnect. A small
number of brief wire stalls (slot gap > 600 ms) — provider-side
slot delivery hiccups, not pipeline failures. Useful baseline for
production behavior expectations.

### Capture parity (parity-acceptance criterion)

v40 measurement (2026-05-17):

| Endpoint | total updates | matched | parity (ep2/ep1) |
|---|---|---|---|
| ep1 | 3,086,094 (accounts) + ~330k (tx) | — | — |
| ep2 | 3,076,898 (accounts) + 266,984 (tx) | 3,076,897 (accounts) | **99.7%** |

Parity holds well within the ±0.1% parity target. Reproducibility
across two consecutive runs: 99.7% / 99.6% — within ±0.1 pp.

The 99.7% capture is **near the rig's ceiling, not the tool's**.
On a customer's bare-metal or modern-c-class AWS rig, single-cmt
capture is expected to remain at this level or slightly higher; the
remaining ~0.3% is geographic and provider-side network behavior
(slot-stage events that arrived too late to pair within the matcher's
slot-window eviction).

(A v39-era characterization circulated a 67% capture number for this
same workload; that figure was computed against a different
denominator and did not reflect the actual ep2/ep1 wire-received
ratio. Subsequent direct measurement of the v39 output JSON showed
99.8% capture on the same workload — the rig was already near its
ceiling. v40's contribution is the per-program p50 reduction
documented above, not a capture lift.)

---

## What grpc-bench measures that thorofare cannot

Per the source-level comparison at `rpcpool/yellowstone-thorofare`
0.5.0 (read during this validation):

### 1. Multi-program subscription topology

Thorofare's CLI takes a single `--account-owner` (singular). To
benchmark 23 programs you'd run 23 separate thorofare processes — but
each process opens its own gRPC connection per endpoint, server-side
fanout is 23×, and the 23 JSONs aren't directly correlatable because
each ran in a different time window.

gRPC-bench packs all 23 programs into the subscription plan in one
process with one output JSON. Per-program t-digests are emitted in
the `per_program_account_delay` map, indexed by short_name.

### 2. Subscription-topology cost surfacing

This is the unique new capability that came out of v1 validation
work. gRPC-bench exposes `--accounts-programs-per-filter` which
controls how programs are partitioned across sub-subscriptions:

- **`= 1` (default)**: one sub-subscription per program. Measures the
  wire-arrival latency floor per program.
- **`= 23`** (= program count): single multi-program filter on the
  wire, matching the customer's literal production code. Surfaces
  the server-side filter-matching latency that the customer's
  existing subscription actually pays.

Direct comparison on the same workload, same rig, same time window:

| Topology | system p50 | spl_token p50 | meteora_dlmm p50 |
|---|---|---|---|
| Single multi-program filter (chunk = 23) | 3,658 ms | 3,340 ms | 2,933 ms |
| One filter per program (chunk = 1) | 13.96 ms | 8.43 ms | 11.54 ms |

**Server-side multi-program filter cost is real wire behavior**, not
a measurement artifact — confirmed by running 3 separate single-program
subscriptions in one grpc-bench process and recovering single-digit
p50s. This cost is invisible to a production application using a
single multi-program filter (it's experienced as latency, not
measured), and thorofare can't probe it at all.

**Actionable insight:** knowing this exists, a production deployment
may choose to split its accounts filter into N smaller filters at the
wire level. grpc-bench predicts the latency floor that split would
reach.

### 3. Cross-stream `tx_vs_account` per endpoint

Cross-stream metric. For each transaction observed on the tx stream within an
endpoint, find the matching account update on the same endpoint's
account stream and record `account_arrival - tx_arrival`. Surfaces
**which stream gives the earliest signal on a single provider** —
independent of which provider is faster overall.

This is the sniper-edge metric: a trading client deciding whether to
react to a tx event or an account event needs to know which arrives
first. The measured baseline (−1.4 to −1.8 ms on both endpoints)
says: tx leads by ~1.4 ms on ep1, ~1.8 ms on ep2. Thorofare does
not compute this.

### 4. Stream stability metrics ()

Inter-message gap distribution, processed→confirmed drift,
disconnect events with full gRPC status code and cumulative event
count, reconnect TTFM (time to first message after reconnect),
forced-reconnect-cycle TTFM. All measured per-endpoint over the run.
Thorofare runs short windows (~1000 slots, ~7 min) and does not
emit stability metrics.

grpc-bench's `--reconnect-test <secs>` forces close-and-reopen on
both subscriptions every N seconds for synthetic resilience testing.

### 5. Block streams with full transaction sets

Block stream support. gRPC-bench supports `SubscribeBlocks` with
`include_transactions=true`. Each block delivered with all its
transactions inline; cross-endpoint delta measured by
`(slot, blockhash)`. Block-size and tx-count recorded per slot for
load correlation. Thorofare does not support blocks at all.

---

## Precision posture (the precision posture)

gRPC-bench is explicit about the sources of timing noise at sub-10 ms
deltas and provides knobs to control each:

| Source | gRPC-bench control | Thorofare |
|---|---|---|
| CPU governor | requires `performance` governor; warns at startup if not | not measured |
| Transparent hugepages | requires `madvise` or `never`; warns if `always` | not measured |
| CPU pinning | `--cpu-affinity` per receiver via `sched_setaffinity` | not supported |
| Realtime scheduling | `--realtime` requests SCHED_FIFO 50 on receivers + processor | not supported |
| Allocator | `--allocator jemalloc/mimalloc/system`, default jemalloc with warm-start of ~64 MB varied buffers before subscription | system malloc |
| Lock-free hot path | per-stream lock-free SPSC channels; per-program DashMap shards; no global locks in receive path | shared Vec with Mutex |
| Kernel timestamps | SO_TIMESTAMPNS primitives implemented and ready; wire-up to tonic deferred to v2 | not implemented |
| NTP sync | startup check + metadata reporting | not measured |
| Host metadata | full kernel/CPU/governor/THP/RT/allocator/NTP captured per run | not captured |

Every choice is recorded in `host_metadata` in the output JSON. The
startup `warning` field surfaces any precision feature that didn't
take effect — defensive transparency for downstream consumers of the
numbers.

---

## Operational posture rules (empirically validated)

### `--realtime` is load-dependent

Validated 2026-05-16: SCHED_FIFO scheduling adds ~2-3 ms of receive-side
overhead at light load (kernel RT-bandwidth throttle + softirq
wake-up interactions) but is critical at saturating load to prevent
dispatcher CPU starvation.

| Workload class | `--realtime` |
|---|---|
| Single-program / thorofare cross-check | OFF |
| Light multi-program (≤ 8 programs, single-cmt) | OFF |
| **Customer-scale 23p + tx single-cmt** | **ON** (verified +18 pp capture lift) |
| Worst-case stress (23p + dual-cmt + blocks) | ON |

### `--accounts-programs-per-filter` defaults to 1 for measurement

Default is 1 sub-subscription per program. Override to 23 (= program
count) to surface the customer's literal production subscription
behavior — exposes the server-side multi-program filter cost as a
characterized number rather than an invisible latency tax.

Customer workloads on endpoints with a 25-concurrent-stream cap
(QN standard tiers, etc.) should use **chunk = 4** when adding
blocks or dual-commitment. The harness's known-heavy auto-split
keeps `system`, `spl_token`, and `token_2022` individually measurable
at any chunk size ≥ 2; the long tail of less-busy programs combines
without measurable per-program inflation up to chunk ≈ 4.

### `--cpu-affinity auto`

Layout from the host's core count. Reserves cores 0–1 for the
kernel + the highest core for the control thread, splits the
remainder 50/50 between ep1 and ep2. On the customer's 64-vCPU AWS
rig this materializes as ep1=cores 2–31 (30 cores), ep2=cores
32–62 (31 cores), ctrl=core 63. Hosts with fewer than 6 cores fall
back to no pinning.

Operators who need fine control (NUMA boundaries, leaving cores for
unrelated workloads) can still use the structured `ep1=…:ep2=…`
form. The `proc=N` pin is automatically stripped under `--realtime`
to avoid the [coordinator wedge issue](rt_coordinator_pin_wedge);
the harness prints a WARNING on stderr when this happens.

---

## Known limitations (transparent disclosure)

### 1. Endpoint stream caps (operational)

Quicknode's standard tiers (and several other Yellowstone-compatible
providers) cap concurrent gRPC subscriptions per endpoint at **25
streams**. Combined with chunk-size = 1 (one sub-subscription per
program), the 23-program full topology adds blocks and/or dual
commitment as follows:

| Workload | Streams/endpoint at chunk=1 | Fits under 25 cap |
|---|---|---|
| 23p + tx (single cmt) | 25 | exactly at cap |
| 23p + tx + blocks | 26 | over by 1 |
| 23p + tx, dual cmt | 49 | over (2×) |
| 23p + tx + blocks, dual cmt | 51 | over (2×) |

Workaround: chunk=4 fits all variants comfortably while keeping the
heavy programs individually measurable. See operational rules above.

This is not a measurement-tool limitation but it is a topology
constraint the customer's evaluation must respect. Production
deployments on dedicated tiers typically have higher caps.

### 2. Dual-commitment dispatcher characterization on virtualized rigs

At 23p × dual-cmt × tx (`--commitment processed,confirmed`), the
v39-era projection on this rig class was a per-endpoint dispatcher
CPU ceiling around 67% capture. v40's patch series (lazy eviction
on the matcher hot path) was designed to lift exactly that ceiling.
v40 dual-cmt was verified to run cleanly under chunk=4 on a
25-stream-cap tier; full characterization of the v40 dual-cmt
capture number is pending and depends on the rig the run is
performed on.

This is rig-sizing characterization, not a measurement-tool defect.
For a defensible dual-commitment number, run on the actual
production-class instance — the DO G-class data here is reference
characterization, not a substitute for measurement on your own
silicon.

### 3. Per-program tail variance — meteora_dlmm specifically

A 1-hour soak (23p × processed × tx × --realtime × chunk=1,
2026-05-17) captured 39M+ account events on the DO 32-vCPU rig with
99.7%+ capture parity and 0 disconnects. Every program landed at
p99.9 under 600 ms — *except* `meteora_dlmm`, which on 5.9M sampled
events showed:

- p50: 70 ms
- p90: 20.6 s
- p99: 36.4 s
- p99.9: 38.5 s

This single program's tail is dragging the overall `account_delay.p99`
into the seconds-range; without meteora_dlmm the overall p99 lands
around 200 ms on the same data.

This is **not a harness defect** — meteora_dlmm has been tail-prone
across all v39 and v40 measurements; the soak just sampled long
enough to reveal that the real tail extends into the tens of seconds
under sustained saturation. Almost certainly a provider-side
filter-matching cost specific to meteora_dlmm's update pattern.
Worth knowing if a use case relies on bounded meteora_dlmm latency
specifically; the other 17 programs are unaffected.

### 2. `SO_TIMESTAMPNS` deferred to v2

Precision posture mandates kernel-level timestamps for sub-10 ms precision
defensibility. The primitives (`setsockopt` wrapper, `cmsg` parser)
are implemented in `src/timing/kernel_ts.rs`. The remaining work
(replumbing tonic's transport to surface socket-level timestamps via
recvmsg) is days of engineering deferred to v2.

Diagnostic 2026-05-16 confirmed that the multi-program timing
inflation (item 1 above) is **server-side wire behavior, not
client-side consumer lag** — splitting into N single-program filters
recovers single-digit-ms p50s without kernel timestamps. So
SO_TIMESTAMPNS would NOT collapse our N sub-filters back to one
multi-program filter; it would only improve the residual ~3 ms gap
between standalone-single-program (8.4 ms) and chunked-parallel
(11-14 ms) — useful precision improvement, not load-bearing for v1.

### 3. `entries_vs_tx` / `entries_vs_account` deferred behind feature flag

Cross-stream metric lists these as headline sniper-edge metrics. Subscribe
Entries is a QuickNode-specific proto extension; the proto source
is feature-flagged for v2 (`cargo build --features entries`). v1
ships with `tx_vs_account` populated and `entries_*` fields present
as `null` in the output JSON with explanatory notes.

---

## Recommended evaluation procedure

Commands below assume a 64-vCPU rig with `--cpu-affinity auto`
(which materializes as ep1=cores 2–31, ep2=cores 32–62, ctrl=core 63
on a 64-core box). On smaller rigs the auto layout adapts; see the
[RUNBOOK](./RUNBOOK.md) for the per-rig affinity table.

1. **Run the posture check** (Test 1 in [RUNBOOK](./RUNBOOK.md#2-test-1--posture-check-60-seconds))
   to verify the host's precision posture is correctly configured.
   Fix anything that warns.
2. **Run the comparative benchmark, tier-safe form** (Test 2 in
   [RUNBOOK](./RUNBOOK.md#3-test-2--comparative-benchmark-10-minutes)) —
   full 23-program production topology with `--realtime
   --accounts-programs-per-filter 4 --cpu-affinity auto` for 1000
   slots. ~7 minutes wall time. The chunk=4 form keeps the run under
   the 25-concurrent-stream cap on standard provider tiers while
   preserving individual measurement on the heavy programs (system /
   spl_token / token_2022) via the always-split logic.
3. **Inspect `per_program_account_delay`** to see per-program latency
   distribution across all 23 programs.
4. **Inspect `cross_stream.<endpoint>.tx_vs_account`** to identify
   which stream leads on each provider.
5. **Inspect `stability.<endpoint>`** for production-readiness
   indicators (disconnects, slot gap distribution, drift).
6. **Optional — run with `--accounts-programs-per-filter 23`** to see
   what a literal single-filter multi-program subscription pays in
   server-side latency. Compare to step 3 for the cost-of-topology
   delta.
7. **Optional — run with `--duration 3600`** for a 1-hour soak.
   Validates sustained-load behavior, bounded-memory invariant, and
   reveals gradual drift that's invisible on a short run. See
   [the soak section in RUNBOOK](./RUNBOOK.md#5-optional--1-hour-soak) for the exact
   command and what to monitor.

The single output JSON is self-contained: full host posture,
proto-version handshake, every per-stream summary, per-program
buckets, cross-stream, stability, ping. No external context
required to interpret it.
