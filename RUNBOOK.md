# grpc-bench runbook (Linux host)

End-to-end recipe for a credible US-east-1 comparison between two
Yellowstone gRPC providers. Two tests, ~15 minutes start to finish;
optional [1-hour soak](#5-optional--1-hour-soak) for sustained-load
characterization, and optional
[thorofare cross-validation](#4-thorofare-cross-validation-optional-10-minutes).

> This runbook assumes a Linux x86_64 host. For dev hosts on macOS
> (which can build + test but cannot produce defensible sub-10 ms
> numbers), see [`MACOS.md`](./MACOS.md).

---

## 1. Host prep

### Requirements

| Item                                         | Value                              |
|----------------------------------------------|------------------------------------|
| Architecture                                 | x86_64                             |
| Cores                                        | ≥ 4                                |
| RAM                                          | ≥ 16 GB                            |
| Region                                       | Same as the endpoints (e.g. `us-east-1`); sub-2 ms TCP to both providers |
| Kernel                                       | Linux 5.4+                         |
| NTP                                          | `chrony` or `systemd-timesyncd` running and synced |

### One-time tunables

Run as root.

```sh
# Larger socket receive buffer absorbs block-payload bursts. Default
# (~256 KB) is what causes provider-side "slow client receiver"
# disconnects under 23-program load.
sysctl -w net.core.rmem_max=268435456
sysctl -w net.core.rmem_default=16777216

# Pin the CPU frequency so jitter doesn't ride the governor's ramp.
cpupower frequency-set --governor performance

# THP can introduce multi-ms TLB shootdowns; madvise is safer.
echo madvise > /sys/kernel/mm/transparent_hugepage/enabled
```

Persist what your distro supports (`/etc/sysctl.d/`, tuned-adm profile, etc.).

### Build and grant scheduling capability

```sh
# Build the release binary.
cargo build --release

# Required for --realtime. Avoids running as root.
sudo setcap cap_sys_nice=eip target/release/grpc-bench
```

### Filter sets

The repo ships three TSV files. Pick the right one for the test:

| File              | Programs | Use case                                                   |
|-------------------|----------|------------------------------------------------------------|
| `23p.tsv`  | 23       | Full 23-program production filter. Required for the saturating-load validation runs. |
| `20p.tsv`  | 20       | 23 minus SPL/Token-2022/System. Lower volume, useful when comparing AMM + launchpad behaviour only. |
| `pump-only.tsv`   | 1        | pump.fun bonding curve only. Smoke / baseline.             |

### Subscription-topology flag (`--accounts-programs-per-filter`)

Controls how the program list is partitioned across accounts
sub-subscriptions on the wire. The known-heavy programs (`system`,
`spl_token`, `token_2022`) are *always* isolated into their own
sub-subscriptions regardless of chunk size, because they alone dominate
any filter they share.

| Value | Topology | Use case |
|---|---|---|
| **`1`** (default) | One sub-subscription per program | **Maximum measurement fidelity.** Per-program p50 lands in single-digit-ms-to-low-teens band. Use when you have an endpoint tier that comfortably allows ≥25 concurrent gRPC streams *and* you want every program measured in isolation. |
| **`4`** (recommended for tier-limited endpoints) | 4 programs per chunk, with `system`, `spl_token`, `token_2022` always split out | **Production-shape, fits under a 25-stream tier cap.** Heavy programs stay individually measurable; the long tail combines without significant accuracy cost. Required for `--with-blocks` / dual-commitment workloads on any tier with a 25-stream cap (see §Endpoint stream caps below). |
| `23` (or program count) | Single multi-program filter | **Matches a literal production-shape subscription with all programs in one filter.** Surfaces the server-side multi-program-filter cost — useful for measuring what a single-filter production subscription is paying in wire latency. |
| anything else | N programs per chunk, with heavy programs always isolated | Custom tradeoff. |

Empirical evidence:

- A 23-program single-filter run measured per-program p50 at 3,500 ms
  (corrupted by server-side filter-matching cost). The same programs
  in 23 separate filters measured at 6.5–14 ms p50 — 200–500× tighter.
- The known-heavy programs (system, spl_token, token_2022) are *always*
  separated regardless of the chunk size, because they alone dominate
  any filter they share. The long tail of less-busy programs combines
  cleanly at chunk sizes up to ~4 with no measurable inflation.
- Thorofare validated against single-program-filter mode within ±0.3 ms;
  multi-program-single-filter is a real provider artifact that only
  grpc-bench can expose.

### Endpoint stream caps (e.g. QN tiers cap at 25)

Most Yellowstone-compatible providers cap the number of concurrent
gRPC subscriptions per endpoint. Quicknode's standard tiers cap at
**25 streams**; exceeding the cap returns `code: 'Some resource has
been exhausted', message: "concurrent gRPC streams limited to 25"`
on the rejected stream-open, and the receiver retries 5× before
giving up. The run still completes but with **asymmetric stream
loss** — typically endpoint2 (the system-under-test) loses some
subscriptions that endpoint1 keeps, producing nonsensical negative
p50 deltas on high-volume programs.

Stream-count math for `23p.tsv` (3 known-heavy + 20 non-heavy)
per endpoint:

| Workload | chunk=1 | chunk=2 | chunk=4 |
|---|---|---|---|
| single-cmt + tx | 25 (at cap) | 15 | 10 |
| single-cmt + tx + blocks | **26 (over)** | 16 | 11 |
| dual-cmt + tx | **49 (over)** | **29 (over)** | 19 |
| dual-cmt + tx + blocks | **51 (over)** | **31 (over)** | 21 |

Formula: `1 (slots) + Ncommitments × (3 heavy + ⌈20 / chunk⌉ rest + 1 tx
+ (1 blocks if --with-blocks))`.

**If your endpoint has a 25-stream cap, use `--accounts-programs-per-filter
4` for any run that adds blocks or dual-commitment.** The harness's
always-split logic keeps the known-heavy programs (`system`,
`spl_token`, `token_2022`) on their own sub-subscriptions at any
chunk size, so per-program measurement on those stays accurate.

**Symptom diagnosis:** any negative p50 on a high-matched-count program
in the output JSON is almost certainly subscription-topology asymmetry,
not a code regression. Grep the run log for `"concurrent gRPC streams
limited"` before investigating further.

### `--realtime` posture rule

`--realtime` enables `SCHED_FIFO 50` on receiver threads and the
processor. Adds ~2–3 ms of receive-side overhead at light load
(kernel RT-bandwidth throttle + softirq wake-up interactions), but
is **critical** at saturating load to prevent dispatcher starvation.

| Workload | `--realtime` |
|---|---|
| Single-program / thorofare cross-check | **OFF** — cleaner timing precision when not under load |
| Light multi-program (≤ 8 programs, single-cmt, no blocks) | OFF |
| **Customer-scale 23-program single-cmt + tx** | **ON** — required to hold the 99%+ capture ceiling |
| 23p + dual-cmt + blocks (worst-case stress) | ON |

---

## 2. Test 1 — posture check (60 seconds)

This is a single-endpoint smoke run. Its only job is to confirm the
Linux precision features are actually active before you commit to a
real measurement run.

```sh
target/release/grpc-bench \
  --endpoint1 <ep1-url>:10000 \
  --x-token1  <ep1-token> \
  --programs  pump-only.tsv \
  --duration  60 \
  --commitment processed \
  --cpu-affinity 2,3,4,5 \
  --realtime \
  --solo \
  --solo-streams slots,accounts \
  --output    results/posture.json
```

### Read the result

```sh
jq '{
  precision: {
    kernel_timestamps: .host_metadata.kernel_timestamps,
    realtime_priority: .host_metadata.realtime_priority,
    allocator: .host_metadata.allocator,
    cpu_governor: .host_metadata.cpu_governor,
    transparent_hugepage: .host_metadata.transparent_hugepage,
    ntp_synced: .host_metadata.ntp_synced,
    affinity: .host_metadata.cpu_affinity
  },
  warnings: .host_metadata.warnings,
  plugin: .proto_metadata.endpoint1_server_plugin_version
}' results/posture.json
```

### Pass criteria

- `realtime_priority: true`
- `allocator: "jemalloc"`
- `cpu_governor: "performance"`
- `transparent_hugepage: "madvise"` or `"never"`
- `ntp_synced: true`
- `affinity: [2,3,4,5]`
- `warnings: []` (empty)
- `plugin` matches what the provider says they're running

If `kernel_timestamps: false` and the warning text mentions
`SO_TIMESTAMPNS unavailable`, that's expected today — the kernel-timestamp
cmsg path is implemented but not yet wired into the tonic per-frame
receive path (see PRECISION.md "What's deferred"). The rest of the
precision posture (`SCHED_FIFO`, `jemalloc`, CPU pinning, governor)
together produce credible sub-10 ms numbers; missing kernel timestamps
adds ~100 µs of decode-path jitter, well below the signal.

If any other item fails, fix the host before continuing — the
measurement run will be wasted work otherwise.

---

## 3. Test 2 — comparative benchmark (~10 minutes)

This is the actual run. One command, both endpoints, full Customer
production topology.

### CPU affinity: just use `auto`

Run with `--cpu-affinity auto` and the harness derives a layout from
the host's `nproc`: reserves cores 0–1 for the kernel + the highest
core for the control thread, and splits the remainder 50/50 between
ep1 and ep2. Hosts with fewer than 6 cores fall back to no pinning.

If you want to hand-pick cores (e.g. to align with NUMA boundaries or
leave specific cores for an unrelated workload), the structured form
is still available:

| Rig class | Cores | Manual override (equivalent layout) |
|---|---|---|
| 8 vCPU | 2–7 | `ep1=2,3:ep2=4,5,6:ctrl=7` |
| 16 vCPU | 2–15 | `ep1=2,3,4,5,6,7:ep2=8,9,10,11,12,13,14:ctrl=15` |
| 32 vCPU | 2–31 | `ep1=2…15:ep2=16…30:ctrl=31` |
| 64+ vCPU | scale similarly | leave 0–1 + dispatcher cores unpinned |

⚠️ **Do not use `proc=N` in a manual affinity** under `--realtime`
on 16+ vCPU rigs. Combination wedges the coordinator
(see `rt_coordinator_pin_wedge` in project memory). The harness now
strips `proc=` automatically when `--realtime` is set and prints a
WARNING line on stderr; `auto` already omits `proc=` by design.

### Picking the right command for your endpoint tier

Use the [stream-count table above](#endpoint-stream-caps-eg-qn-tiers-cap-at-25)
to know which of these to run. If your endpoint allows ≥51 concurrent
streams, run the **full** form. If it's a typical 25-stream-cap tier
(QN standard tiers, several others), run the **tier-safe** form.
Both produce the same shape of output JSON.

#### Full form (endpoint with ≥51 stream cap)

```sh
target/release/grpc-bench \
  --endpoint1 <ep1-url>:10000 \
  --x-token1  <ep1-token> \
  --endpoint2 <ep2-url>:10000 \
  --x-token2  <ep2-token> \
  --programs  23p.tsv \
  --slots     1000 \
  --commitment processed,confirmed \
  --with-transactions \
  --with-blocks \
  --accounts-programs-per-filter 1 \
  --cpu-affinity auto \
  --realtime \
  --max-decode-mb 256 \
  --output    results/comparison.json
```

#### Tier-safe form (recommended default — fits under a 25-stream cap)

Same as the full form but with `--accounts-programs-per-filter 4`.
Heavy programs (system, spl_token, token_2022) stay individually
measurable; the long tail of 20 non-heavy programs chunks into 5
sub-subscriptions.

```sh
target/release/grpc-bench \
  --endpoint1 <ep1-url>:10000 \
  --x-token1  <ep1-token> \
  --endpoint2 <ep2-url>:10000 \
  --x-token2  <ep2-token> \
  --programs  23p.tsv \
  --slots     1000 \
  --commitment processed,confirmed \
  --with-transactions \
  --with-blocks \
  --accounts-programs-per-filter 4 \
  --cpu-affinity auto \
  --realtime \
  --max-decode-mb 256 \
  --output    results/comparison.json
```

#### Lightest form (single-cmt + tx only, for a clean comparable baseline)

This is the form that produced the published reference baseline: 99.7%
capture, p50 8.61 ms account / 7.99 ms tx, p99 151 ms account, zero
disconnects on a DO 32-vCPU rig. Use it when you want a number directly
comparable to that baseline. See [`BENCHMARK_HISTORY.md`](BENCHMARK_HISTORY.md)
for the full reference workload and rig details.

```sh
target/release/grpc-bench \
  --endpoint1 <ep1-url>:10000 \
  --x-token1  <ep1-token> \
  --endpoint2 <ep2-url>:10000 \
  --x-token2  <ep2-token> \
  --programs  23p.tsv \
  --slots     1000 \
  --commitment processed \
  --with-transactions \
  --accounts-programs-per-filter 1 \
  --cpu-affinity auto \
  --realtime \
  --output    results/comparison-single-cmt.json
```

Wall time: ~7 minutes for 1000 slots at Solana mainnet rate
(400ms per slot). All three forms have similar wall-clock cost.

### Read the result

```sh
F=results/comparison.json
jq '{
  shape: {
    duration_s: (.metadata.duration_ms/1000),
    slots: .metadata.total_slots_collected,
    drops: { ep1: .metadata.dropped_events_ep1, ep2: .metadata.dropped_events_ep2 },
    parity_pct: ((.endpoints[1].total_updates - .endpoints[0].total_updates)
                 / .endpoints[0].total_updates * 100)
  },
  comparative: {
    slot_processed_p50: .comparative.slot_status.processed_delay.p50,
    account: { p50: .comparative.account_delay.p50,
               matched: .comparative.account_delay.matched,
               ep1_faster: .comparative.account_delay.ep1_faster,
               ep2_faster: .comparative.account_delay.ep2_faster },
    tx:      { p50: .comparative.transaction_delay.p50,
               matched: .comparative.transaction_delay.matched },
    block:   { p50: .comparative.block_delay.p50,
               matched: .comparative.block_delay.matched }
  },
  per_program: (.per_program_account_delay | to_entries
                | map({program: .key, matched: .value.matched, p50: .value.p50})
                | sort_by(-.matched))
}' "$F"
```

### Pass criteria

Numbers below are calibrated against the published reference baseline
on a DO 32-vCPU G-class rig against QN US-east + QN-EU dedicated
endpoints. Your numbers may be tighter (faster silicon, bare metal,
closer geographic match) or looser (more contended rig, intercontinental
path). Use these as a sanity envelope, not absolute targets.

| Field | Healthy means |
|---|---|
| `parity_pct` | within ±1% — capture ratio between endpoints must match |
| `slot_processed_p50` | 5–10 ms (geographic baseline) |
| `account.p50` | **8–12 ms** at chunk=1; **9–14 ms** at chunk=4; if you see 50–3500 ms you're hitting the multi-program-filter inflation — check chunk size |
| `account.p99` | < 200 ms at chunk=1 single-cmt; up to ~300 ms acceptable at dual-cmt |
| `account.matched` | within ~1% of `total_account_updates[1]` — verifies pairing rate |
| `tx.p50` | 5–10 ms |
| `block.p50` | < 100 ms (block payloads are heavy; tail allowed) |
| `per_program[].p50` | every program in 7–14 ms range at chunk=1; some variance allowed (whirlpool / meteora can run 1.2× hotter; programs with <10 matched events show statistical noise) |

**Negative `p50` is a topology smell, not a code bug.** If any
high-matched-count program shows a negative p50, you almost
certainly hit an endpoint stream cap — see
[Endpoint stream caps](#endpoint-stream-caps-eg-qn-tiers-cap-at-25)
above, then re-run with `--accounts-programs-per-filter 4`.

### On capture rates

Reference baseline on a virtualized 32-vCPU G-class rig:

| Workload | Capture (ep2/ep1 acc received) | Notes |
|---|---|---|
| 23p × processed × tx, chunk=1 | **99.7%** | reproducible across two consecutive runs; ceiling is network, not dispatcher |
| 23p × processed,confirmed × tx, chunk=4 | runs cleanly under the 25-stream cap; dispatcher-load characterization pending on this rig class | |
| 23p × processed,confirmed × tx × blocks, chunk=4 | runs cleanly; characterization pending | |

On a bare-metal or modern-c-class AWS rig, expect single-cmt to
remain at 99%+ and dual-cmt to land well above the dispatcher-CPU
ceiling that constrains virtualized G-class droplets. The current
matcher uses lazy eviction on the hot path specifically to keep
that ceiling from binding on faster silicon.

If single-cmt capture is below ~95%, something is wrong upstream
(network, endpoint health, or rig sizing). Look at
`stability.endpointN.disconnects` and `stability.endpointN.slot_gap_ms`
before assuming the harness is at fault.

`ep1_faster` vs `ep2_faster` tells you which provider's wire-arrival
order dominates. A roughly 50/50 split means the two paths are
equivalent; a 90/10 split means one provider consistently leads,
with the magnitude shown by `p50`.

### If something looks off

| Symptom | Likely cause |
|---|---|
| `parity_pct` > ±5% | One endpoint dropped a stream. Check the run log for `"concurrent gRPC streams limited"` (tier cap); also check `stability.endpointN.disconnects` for h2 errors. |
| Negative `p50` on high-volume programs | Subscription-topology asymmetry — almost always the 25-stream tier cap. Re-run with `--accounts-programs-per-filter 4`. |
| Any `p50` over 1 second | Precision posture not active — re-run the posture check; look for warnings. Or you're at chunk=program-count seeing server-side filter inflation. |
| `block.matched` ≪ slot count | One endpoint delivering blocks late or at a different commitment, or blocks stream got rejected for tier reasons. Check `total_block_updates` and the run log. |
| `slot_processed_p50` normal, other p50s wild | Receiver-thread scheduling jitter — usually means `--realtime` / `--cpu-affinity` didn't take effect. Re-run the posture check. |
| Ring-overflow drops in metadata | Patch 2 sizes accounts rings at 4× baseline automatically; if you still see drops on accounts, bump `--ring-capacity` to e.g. `131072`. Drops on slots/blocks are exotic and usually point at an upstream issue. |

---

## 4. Thorofare cross-validation (optional, ~10 minutes)

Thorofare agreement criterion — confirm the timing path agrees with
`rpcpool/yellowstone-thorofare` on metrics both tools measure.
Required if your customer wants independent verification.

Run both tools simultaneously against the same endpoints:

```sh
# Thorofare with pump.fun account filter + transactions firehose
/opt/yellowstone-thorofare/target/release/thorofare \
  --endpoint1 "$EP1_URL" --x-token1 "$EP1_TOKEN" \
  --endpoint2 "$EP2_URL" --x-token2 "$EP2_TOKEN" \
  -s 1000 --with-accounts \
  --account-owner 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P \
  --output /tmp/thorofare-validation.json &

# gRPC-bench, pump-only, single-cmt, NO --realtime (lighter
# load = tighter timing precision; matches thorofare's posture).
target/release/grpc-bench \
  --endpoint1 "$EP1_URL" --x-token1 "$EP1_TOKEN" \
  --endpoint2 "$EP2_URL" --x-token2 "$EP2_TOKEN" \
  --programs pump-only.tsv \
  --slots 1000 --commitment processed \
  --cpu-affinity auto \
  --output results/validation-vs-thorofare.json &

wait
```

Compare slot status p50 + account_delay p50 between the two JSONs.
Expected: agreement within ±1 ms on all comparable metrics. If you
see larger discrepancies, check that you dropped `--realtime` on
the grpc-bench side and that both tools observed the same time
window (start both within a few seconds of each other).

## 5. Optional — 1-hour soak

Use this when you need to characterize an endpoint's *sustained*
behavior, not its first-1000-slot behavior. Catches gradual drift,
slow memory growth, periodic stalls that don't show up on a short
run, and validates the the bounded-memory invariant "bound memory at all times"
invariant under continuous load. ~60 minutes wall clock.

```sh
target/release/grpc-bench \
  --endpoint1 "$EP1_URL" --x-token1 "$EP1_TOKEN" \
  --endpoint2 "$EP2_URL" --x-token2 "$EP2_TOKEN" \
  --programs 23p.tsv \
  --duration 3600 \
  --commitment processed \
  --with-transactions \
  --accounts-programs-per-filter 4 \
  --cpu-affinity auto \
  --realtime \
  --output results/soak-1h.json \
  2>&1 | tee /tmp/soak-1h.log
```

What to watch in `/tmp/soak-1h.log`:

- `INFO snapshot …` lines fire every 10 s (the bounded-memory invariant). The 10-min,
  20-min, 30-min, 60-min snapshots' `total_slots` should increase
  roughly linearly — if they plateau, something stalled.
- `disconnects` should stay 0 / 0 unless the endpoint genuinely
  flapped (provider-side issue).
- The process RSS (e.g. `ps -o rss= -p $(pgrep grpc-bench)` in a
  side terminal every 5 min) should stay bounded — the bounded-memory invariant
  requires this and the matcher's slot-window eviction enforces it.

When the run finishes, the same headline jq from
[§3](#3-test-2--comparative-benchmark-10-minutes) applies. For a soak
run you also care about distribution stability — i.e. whether the
60-min p50/p99 differ meaningfully from the first 10 min. If you need
that comparison, dial up snapshot logging verbosity and feed the
snapshot-line timestamps through to a quick spreadsheet.

## 6. Reading the output JSON

The single result file `results/comparison.json` is self-contained:
it carries the full host posture (kernel, allocator, governor, RT
outcome), the proto-version handshake, every per-stream summary,
and per-program buckets. No external context required.

The headline jq in [§3](#3-test-2--comparative-benchmark-10-minutes)
covers the everyday metrics. Below are the fields that aren't in that
headline but are worth knowing about — particularly the ones grpc-bench
measures that other harnesses do
not:

| Field | What it surfaces |
|---|---|
| `per_program_account_delay` (whole map) | Per-program latency table, all 23 programs measured in one run. Thorofare requires 23 separate runs to approximate. |
| `cross_stream.<endpoint>.tx_vs_account` | Within each endpoint, the arrival delta between a matching transaction and account-write pair. Negative = tx leads. **Only grpc-bench measures this.** |
| `stability.<endpoint>.slot_gap_ms` | Inter-event gap distribution for slot status. Stalls > 600 ms are recorded with timestamps in `stall_events`. |
| `stability.<endpoint>.processed_confirmed_drift_ms` | Per-slot delay between Processed and Confirmed stages — provider plugin internal latency (populated only on dual-commitment runs). |
| `stability.<endpoint>.disconnects` | Disconnect events with gRPC status codes + cumulative event counts. Empty list = 0 disconnects. |
| `comparative.block_delay` | Block stream comparison (populated when `--with-blocks` is set). |
| `endpoints[].avg_ping_ms` | gRPC ping per endpoint, captured every 30 s during the run. |
