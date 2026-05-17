//! Run orchestration: connect, evaluate versions, spawn receivers, drain
//! events, serialize output.
//!
//! Clippy posture: this module is the top-level wiring layer, so a number
//! of pedantic lints are quieted at function granularity:
//! - `too_many_lines` on [`execute`] (single orchestration entry point);
//! - `too_many_arguments` on [`dispatch_event`] (one argument per
//!   ingest-state structure, by design);
//! - `needless_pass_by_value` on [`execute`] / [`install_signal_handler`]
//!   (they consume the config / shutdown handle for the lifetime of the
//!   run; references would force the caller to hold them just as long).
//!
//! Each `#[allow]` is annotated at its use site.
//!
//! Architecture:
//!
//! ```text
//!   main thread
//!     ┌──────────────────────────────────────────────────────────────┐
//!     │ load programs, validate config, build host metadata          │
//!     │ connect+evaluate each EndpointSpec on a shared tokio rt      │
//!     │ build SubscriptionPlan                                       │
//!     │ for each spec: spawn an OS thread (pinned, optional RT) that │
//!     │   runs a current_thread tokio runtime, opens the subscription│
//!     │   and pushes decoded `Event` records into a `Ring`           │
//!     │ run the ingest loop:                                         │
//!     │   - drain all rings non-blocking                             │
//!     │   - dispatch into `StreamMatchers` + `StabilityTracker`      │
//!     │   - dispatch into `CrossStreamTracker`(s)                    │
//!     │   - write to RawWriter if configured                         │
//!     │   - check stop conditions                                    │
//!     │ on shutdown: drain remaining events, build RunOutput, write  │
//!     └──────────────────────────────────────────────────────────────┘
//! ```
//!
//! On macOS / dev hosts the CPU-affinity / `SCHED_FIFO` / kernel-timestamp
//! calls are all stubbed; the rest of the pipeline works the same way so
//! a developer can run the binary against a mock or live endpoint and
//! validate end-to-end behaviour. Production timing accuracy requires
//! Linux.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use futures::StreamExt;

use crate::{
    collect::{
        apply_cpu_affinity, apply_realtime, decode, AffinityPlan, Event, EventPayload, Pubkey32,
        ReceiverStats, Ring, SchedOutcome,
    },
    config::{Config, StopCondition},
    crossstream::{CrossStreamSummary, CrossStreamTracker},
    env,
    matching::{accounts::PerProgramSummary, CaptureTotals, StreamMatchers},
    programs::ProgramSet,
    proto::{EndpointVersion, ProtoMetadata},
    raw::RawWriter,
    stability::{StabilitySummary, StabilityTracker},
    subscribe::{
        yellowstone::{connect_with_decode_limit, fetch_and_evaluate_version, open_subscription},
        EndpointRole, SubscriptionPlan, SubscriptionRole, SubscriptionSpec,
    },
    summary::{ComparativeSummary, ConfigEcho, EndpointInfo, RunMetadata, RunOutput},
    timing::ClockOrigin,
};

/// Per-stream ring capacity default. Sized for several seconds of
/// slot-stream burst on a fast endpoint without dropping. Spec §7: drop
/// on overflow rather than block the receiver. Operators can override
/// via `--ring-capacity` when running tier-heavy filter sets (23
/// programs + `--with-blocks` benefit from larger rings on un-pinned
/// hosts where the main processor can stall briefly).
///
/// This is the *baseline*; [`ring_capacity_for`] scales it per
/// [`SubscriptionRole`] / [`MainStream`] kind so the heaviest stream
/// (accounts) gets 4× the others while low-volume streams (slots)
/// shrink to avoid wasted memory.
pub const DEFAULT_RING_CAPACITY: usize = 65_536;

/// Pick a ring capacity for the given subscription, given a baseline
/// (from `--ring-capacity`, default [`DEFAULT_RING_CAPACITY`]).
///
/// Per-kind sizing was added after the v39 DO 32-vCPU run where
/// accounts-stream bursts overflowed the uniformly-sized rings while
/// slots / blocks rings sat <1% full. Multipliers (relative to the
/// baseline):
///
/// | Kind          | Multiplier | Rationale                                  |
/// |---------------|-----------:|--------------------------------------------|
/// | Accounts      |      4×    | hottest stream, bursty (per-slot fan-out). |
/// | Transactions  |      1×    | baseline reference.                        |
/// | Blocks        |      ½×    | low rate, but MB-sized payloads.           |
/// | Slots         |      ⅛×    | ~6 stages × few slots/sec.                 |
/// | Entries       |      1×    | comparable to transactions in volume.      |
///
/// The baseline scales with `--ring-capacity` so an operator override
/// still scales every kind proportionally.
#[must_use]
pub fn ring_capacity_for(role: SubscriptionRole, baseline: usize) -> usize {
    use crate::subscribe::MainStream;
    let mul_num: usize;
    let mul_den: usize;
    match role {
        SubscriptionRole::Main { stream, .. } => match stream {
            MainStream::Accounts => {
                mul_num = 4;
                mul_den = 1;
            }
            MainStream::Transactions => {
                mul_num = 1;
                mul_den = 1;
            }
            MainStream::Blocks => {
                mul_num = 1;
                mul_den = 2;
            }
            MainStream::Slots => {
                mul_num = 1;
                mul_den = 8;
            }
        },
        SubscriptionRole::Entries { .. } => {
            mul_num = 1;
            mul_den = 1;
        }
    }
    // `Ring::with_capacity` asserts capacity > 0, so clamp to a 1-event
    // floor here so unusual `--ring-capacity` overrides (e.g. 1) still
    // produce a positive capacity after the smallest kind's divisor.
    (baseline.saturating_mul(mul_num) / mul_den.max(1)).max(1)
}

/// Periodic snapshot tick (spec §7 "Periodically (every 10s) snapshots
/// current quantile estimates").
pub const SNAPSHOT_TICK: Duration = Duration::from_secs(10);

/// Ingest-loop sleep when all rings are empty.
const IDLE_SLEEP: Duration = Duration::from_millis(1);

/// Maximum events drained from a single ring per outer-loop iteration.
/// Without this bound, a perpetually-hot ring (e.g. Processed Accounts
/// at 23-program full topology) can keep `try_recv` returning `Ok`
/// faster than `dispatch_event` consumes, holding the outer loop hostage
/// in the inner `while` — snapshot ticks never fire, the stop condition
/// is never re-checked, and other rings starve. 1024 amortizes the
/// outer-loop overhead while still letting the loop visit all 16 rings
/// (and re-check `should_stop` / `SNAPSHOT_TICK`) within ~10 ms even
/// when every ring is saturated.
const DRAIN_BUDGET_PER_RING: usize = 1024;

/// Maximum time to wait for receiver threads to exit after shutdown is
/// signalled before forcing process termination. Bounds the worst-case
/// SIGINT-to-exit latency when a stream is blocked on a half-open TCP
/// connection that won't deliver RST quickly.
const RECEIVER_JOIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum time the receiver thread will wait inside `stream.next()` before
/// re-checking the shutdown flag. `stream.next().await` on a half-open
/// TCP connection can sit indefinitely (no bytes, no RST, no FIN); the
/// timeout doesn't affect throughput in steady state — under load every
/// wakeup carries an item — but ensures shutdown latency is bounded.
const STREAM_POLL_TIMEOUT: Duration = Duration::from_millis(500);

/// Maximum time to wait for the per-endpoint dispatcher threads to
/// finish their drain pass after shutdown is signalled. Same bound and
/// rationale as [`RECEIVER_JOIN_TIMEOUT`], applied one layer up the
/// pipeline. Must exceed [`DISPATCHER_POST_SHUTDOWN_BUDGET`] so the
/// dispatchers always observe their internal deadline first and exit
/// cleanly rather than being abandoned by the coordinator.
const DISPATCHER_JOIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Wall-clock budget a dispatcher gives itself, post-shutdown, to keep
/// draining residual events before bailing out. Bounds the drain even
/// when receivers are still feeding the rings or `Mutex<StreamMatchers>`
/// contention slows the per-event hot path enough that a full
/// 65k-capacity ring can't be flushed in [`DISPATCHER_JOIN_TIMEOUT`].
///
/// Buffered events past the stop condition are not statistically
/// load-bearing for the comparative metrics — both endpoints' rings
/// drain concurrently with the same budget, so the truncation is
/// symmetric and doesn't bias the p50/p99.
const DISPATCHER_POST_SHUTDOWN_BUDGET: Duration = Duration::from_secs(2);

/// Per-endpoint, per-thread counter slice owned by a single
/// dispatcher thread. Each [`dispatcher_thread_main`] keeps its own
/// copy, mutates it lock-free, and returns it via the thread's
/// [`JoinHandle`] for the coordinator to merge into the global
/// [`IngestCounters`] before [`assemble_output`].
///
/// `last_counted_slot` tracks the slot-dedup high-water mark for
/// the ep1 dispatcher (the ep2 dispatcher leaves it at zero — slot
/// counts for the `--slots` stop condition come from ep1's
/// `Processed` stage).
#[derive(Debug, Default)]
struct IngestCountersPartial {
    accounts: u64,
    transactions: u64,
    blocks: u64,
    entries: u64,
    last_counted_slot: u64,
}

/// Per-endpoint TCP/gRPC round-trip latency tracker (spec §6.5).
/// A dedicated tokio task per endpoint records GetVersion RPC
/// durations every [`PING_INTERVAL`]; the running average lands in
/// `endpoints[].avg_ping_ms` at end-of-run (spec §8).
///
/// Lock-free: sum + count atomics. Reader at end-of-run computes
/// the average; the per-event hot path never touches this.
#[derive(Debug)]
struct PingTracker {
    sum_us: AtomicU64,
    count: AtomicU64,
}

impl PingTracker {
    fn new() -> Self {
        Self {
            sum_us: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    fn record(&self, duration_us: u64) {
        self.sum_us.fetch_add(duration_us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(clippy::cast_precision_loss)]
    fn average_ms(&self) -> f64 {
        let s = self.sum_us.load(Ordering::Relaxed);
        let n = self.count.load(Ordering::Relaxed);
        if n == 0 {
            f64::NAN
        } else {
            (s as f64) / (n as f64) / 1000.0
        }
    }
}

/// Spec §6.5: ping at subscription open and every 30 s.
const PING_INTERVAL: Duration = Duration::from_secs(30);
/// Bound a single ping's wait so a dead connection is detected
/// promptly and the ping task can reconnect.
const PING_TIMEOUT: Duration = Duration::from_secs(5);

/// Counters accumulated by the ingest loop across all events.
///
/// `total_slots_collected` drives the `--slots` stop condition; the
/// rest populate `metadata.total_*_updates` (spec §8). Built at
/// end-of-run from two [`IngestCountersPartial`] values returned
/// by the per-endpoint dispatcher threads plus the [`AtomicU64`]
/// slot counter that the ep1 dispatcher increments live.
#[derive(Debug, Default)]
struct IngestCounters {
    /// Distinct slots whose `SlotProcessed` stage was observed on
    /// endpoint1. Reflected verbatim into `metadata.total_slots_collected`
    /// and used for the `--slots` stop condition mid-run via the
    /// separate [`AtomicU64`] surfaced to the coordinator.
    total_slots_collected: u64,
    /// Account-update counts per endpoint.
    accounts_ep1: u64,
    /// Account-update counts per endpoint.
    accounts_ep2: u64,
    /// Transaction-update counts per endpoint.
    transactions_ep1: u64,
    /// Transaction-update counts per endpoint.
    transactions_ep2: u64,
    /// Block-update counts per endpoint.
    blocks_ep1: u64,
    /// Block-update counts per endpoint.
    blocks_ep2: u64,
    /// Entry-update counts per endpoint.
    entries_ep1: u64,
    /// Entry-update counts per endpoint.
    entries_ep2: u64,
}

/// Execute one full run from a validated [`Config`].
///
/// Returns the path the result JSON was written to on success.
///
/// # Errors
/// Any of:
/// - programs TSV load failure
/// - endpoint connect / `GetVersion` / version refusal
/// - subscribe open failure
/// - output JSON write failure
///
/// # Panics
/// Panics only on impossible-to-recover-from startup conditions, such as
/// failure to spawn an OS thread for a receiver (a system-level resource
/// limit, not user input). Tagged `expect` sites carry the reason.
#[allow(clippy::too_many_lines, clippy::needless_pass_by_value)]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
pub async fn execute(config: Config) -> Result<std::path::PathBuf> {
    let programs = ProgramSet::load(&config.programs_path)
        .context("load --programs TSV")?;
    tracing::info!(
        program_count = programs.len(),
        "programs loaded from {}",
        config.programs_path.display()
    );

    let start_wall_ms = crate::timing::realtime_now_ns() / 1_000_000;
    let clock_origin = ClockOrigin::capture();
    let start_instant = Instant::now();

    // Connect to all endpoints and evaluate version. Refused endpoints
    // abort the run.
    let ep1_version = connect_and_eval(&config, EndpointRole::One).await?;
    let ep2_version = if config.solo {
        None
    } else {
        Some(connect_and_eval(&config, EndpointRole::Two).await?)
    };
    let proto_metadata = match &ep2_version {
        Some(v2) => ProtoMetadata::from_endpoints(&ep1_version, v2),
        None => ProtoMetadata::from_single_endpoint(&ep1_version),
    };

    // Build subscription plan and spawn receiver threads. Each receiver
    // gets its own Ring.
    let plan = SubscriptionPlan::from_run_config(&config, &programs);
    if plan.specs.is_empty() {
        anyhow::bail!("subscription plan is empty (no endpoints / commitments configured)");
    }

    let affinity = AffinityPlan::from_spec(&config.cpu_affinity);
    let shutdown = Arc::new(AtomicBool::new(false));
    install_signal_handler(Arc::clone(&shutdown));

    // Spec §6.5: per-endpoint ping background task. Opens its own
    // connection (separate from receivers) so the running average
    // reflects baseline endpoint health, not receiver pipeline
    // backpressure. Spawned on the existing tokio runtime; exits on
    // `shutdown`.
    let ping_ep1 = Arc::new(PingTracker::new());
    let ping_ep2 = Arc::new(PingTracker::new());
    let ping_max_decode_bytes = config.max_decode_mb * 1024 * 1024;
    tokio::spawn(ping_task(
        config.endpoint1.clone(),
        ping_max_decode_bytes,
        EndpointRole::One,
        Arc::clone(&ping_ep1),
        Arc::clone(&shutdown),
    ));
    if let Some(ep2_spec) = config.endpoint2.as_ref() {
        tokio::spawn(ping_task(
            ep2_spec.clone(),
            ping_max_decode_bytes,
            EndpointRole::Two,
            Arc::clone(&ping_ep2),
            Arc::clone(&shutdown),
        ));
    }

    let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(plan.specs.len());
    let mut rings: Vec<Ring> = Vec::with_capacity(plan.specs.len());
    let mut stats_per_spec: Vec<Arc<ReceiverStats>> = Vec::with_capacity(plan.specs.len());
    let mut roles: Vec<SubscriptionRole> = Vec::with_capacity(plan.specs.len());

    // Counter shared with every receiver thread; bumped on
    // `SchedOutcome::Applied` so `host_metadata.realtime_priority` can
    // reflect actual per-thread outcomes instead of the v1 hardcoded
    // `false`.
    let rt_applied_count = Arc::new(AtomicU64::new(0));
    let rt_expected_count: u64 = if config.realtime {
        // receivers + coordinator (current thread).
        // Dispatcher threads intentionally run SCHED_OTHER. Escalating
        // them to SCHED_FIFO 50 was measured ~4 pp worse on DO 8-vCPU
        // (overpacked RT cohort on 6 cores) and neutral on DO 16-vCPU
        // (~67–68% capture either way) — confirms the binding
        // constraint is accounts-mutex contention, not scheduling.
        u64::try_from(plan.specs.len() + 1).unwrap_or(u64::MAX)
    } else {
        0
    };

    let mut idx_in_endpoint: HashMap<EndpointRole, usize> = HashMap::new();
    for spec in &plan.specs {
        let endpoint_role = spec.role.endpoint();
        let idx = *idx_in_endpoint
            .entry(endpoint_role)
            .and_modify(|c| *c += 1)
            .or_insert(0);
        let core = affinity.core_for_subscription(spec.role, idx);
        let ring = Ring::with_capacity(ring_capacity_for(spec.role, config.ring_capacity));
        let stats = Arc::clone(&ring.stats);
        let sender = ring.sender.clone();
        let role = spec.role;
        let spec_clone = spec.clone();
        let shutdown_clone = Arc::clone(&shutdown);
        let clock = clock_origin;
        let realtime = config.realtime;
        let rt_counter = Arc::clone(&rt_applied_count);
        let max_decode_bytes = config.max_decode_mb * 1024 * 1024;

        let h = thread::Builder::new()
            .name(format!("grpc-rx-{}-{idx}", endpoint_role.label()))
            .spawn(move || {
                receiver_thread_main(
                    spec_clone,
                    sender,
                    role,
                    core,
                    realtime,
                    max_decode_bytes,
                    rt_counter,
                    shutdown_clone,
                    clock,
                );
            })
            .with_context(|| format!("spawn receiver thread for {:?}", spec.role))
            .expect("thread spawn (programmer error if it fails at startup)");

        handles.push(h);
        rings.push(ring);
        stats_per_spec.push(stats);
        roles.push(spec.role);
    }

    // Pin the processor thread (current thread) if requested.
    if let Some(core) = affinity.processor_core {
        match apply_cpu_affinity(core) {
            SchedOutcome::Applied => {
                tracing::info!(core, "pinned processor thread");
            }
            SchedOutcome::Unsupported => {
                tracing::warn!("CPU affinity unsupported on this platform; ignored");
            }
            SchedOutcome::Failed(e) => {
                tracing::warn!(error = %e, "failed to pin processor thread");
            }
        }
    }

    // Escalate the processor thread to SCHED_FIFO if requested. Without
    // this, receivers run at RT priority while the processor stays on
    // SCHED_OTHER — a priority inversion where ring drains can starve
    // under load. The bounded ingest loop (`DRAIN_BUDGET_PER_RING` plus
    // `IDLE_SLEEP` on empty) yields cooperatively, so RT priority here
    // does not monopolize the core.
    if config.realtime {
        match apply_realtime() {
            SchedOutcome::Applied => {
                tracing::info!("SCHED_FIFO applied to processor thread");
                rt_applied_count.fetch_add(1, Ordering::Relaxed);
            }
            SchedOutcome::Unsupported => {
                tracing::warn!("SCHED_FIFO unsupported on this platform for processor thread");
            }
            SchedOutcome::Failed(e) => {
                tracing::error!(error = %e, "SCHED_FIFO rejected for processor thread (need CAP_SYS_NICE)");
            }
        }
    }

    // Build matchers and the per-stream-kind dispatcher pipeline.
    let program_names: HashMap<Pubkey32, String> = programs
        .entries
        .iter()
        .filter_map(|p| {
            let decoded = bs58::decode(&p.program_id).into_vec().ok()?;
            if decoded.len() != 32 {
                return None;
            }
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&decoded);
            Some((arr, p.short_name.clone()))
        })
        .collect();

    // Per-endpoint dispatcher architecture. Two dispatcher threads
    // (one per endpoint) share an `Arc<StreamMatchers>` for the
    // cross-endpoint pairing. Slots/transactions/blocks matchers use
    // their own `Mutex<>` so dispatchers only serialize on same-kind
    // events; accounts uses internal DashMap-based sharding so its
    // per-event observe acquires only a per-program-shard lock. The
    // raw-record sink stays behind its own outer Mutex (single file,
    // single linearized JSONL stream). Per-endpoint state (stability,
    // cross-stream tracker, partial counters) is owned by each
    // dispatcher exclusively, lock-free.
    let matchers = Arc::new(
        StreamMatchers::new(program_names).with_strict_account_key(config.strict_account_key),
    );

    let raw_writer: Option<Arc<Mutex<RawWriter>>> = match &config.raw_records {
        Some(path) => {
            let w = RawWriter::create(path).with_context(|| {
                format!("open --raw-records {}", path.display())
            })?;
            tracing::info!("raw-records JSONL → {}", path.display());
            Some(Arc::new(Mutex::new(w)))
        }
        None => None,
    };

    // Live slot counter — slots dispatcher writes, coordinator reads
    // for the `--slots` stop-condition check. Atomic avoids a lock on
    // the hot path.
    let total_slots_collected = Arc::new(AtomicU64::new(0));
    let mut last_snapshot = Instant::now();

    // Partition rings/roles by endpoint for dispatcher ownership.
    let mut ep1_rings: Vec<Ring> = Vec::new();
    let mut ep1_roles: Vec<SubscriptionRole> = Vec::new();
    let mut ep2_rings: Vec<Ring> = Vec::new();
    let mut ep2_roles: Vec<SubscriptionRole> = Vec::new();
    for (ring, role) in rings.into_iter().zip(roles.iter().copied()) {
        match role.endpoint() {
            EndpointRole::One => {
                ep1_rings.push(ring);
                ep1_roles.push(role);
            }
            EndpointRole::Two => {
                ep2_rings.push(ring);
                ep2_roles.push(role);
            }
        }
    }

    tracing::info!(
        receivers_total = handles.len(),
        ep1_receivers = ep1_rings.len(),
        ep2_receivers = ep2_rings.len(),
        snapshot_tick_secs = SNAPSHOT_TICK.as_secs(),
        drain_budget_per_ring = DRAIN_BUDGET_PER_RING,
        "coordinator entering; spawning per-endpoint dispatchers"
    );

    let dispatcher_ep1 = {
        let matchers = Arc::clone(&matchers);
        let raw_writer = raw_writer.as_ref().map(Arc::clone);
        let total_slots = Arc::clone(&total_slots_collected);
        let shutdown = Arc::clone(&shutdown);
        let stability = StabilityTracker::new(EndpointRole::One);
        let cross = CrossStreamTracker::new(EndpointRole::One);
        thread::Builder::new()
            .name("grpc-dispatcher-ep1".into())
            .spawn(move || {
                dispatcher_thread_main(
                    EndpointRole::One,
                    ep1_rings,
                    ep1_roles,
                    stability,
                    cross,
                    IngestCountersPartial::default(),
                    matchers,
                    raw_writer,
                    total_slots,
                    shutdown,
                )
            })
            .context("spawn dispatcher thread for endpoint1")
            .expect("dispatcher thread spawn (programmer error if it fails at startup)")
    };

    let dispatcher_ep2 = {
        let matchers = Arc::clone(&matchers);
        let raw_writer = raw_writer.as_ref().map(Arc::clone);
        let total_slots = Arc::clone(&total_slots_collected);
        let shutdown = Arc::clone(&shutdown);
        let stability = StabilityTracker::new(EndpointRole::Two);
        let cross = CrossStreamTracker::new(EndpointRole::Two);
        thread::Builder::new()
            .name("grpc-dispatcher-ep2".into())
            .spawn(move || {
                dispatcher_thread_main(
                    EndpointRole::Two,
                    ep2_rings,
                    ep2_roles,
                    stability,
                    cross,
                    IngestCountersPartial::default(),
                    matchers,
                    raw_writer,
                    total_slots,
                    shutdown,
                )
            })
            .context("spawn dispatcher thread for endpoint2")
            .expect("dispatcher thread spawn (programmer error if it fails at startup)")
    };

    // Coordinator loop. The dispatchers own the event-dispatch hot
    // path; this thread just watches the stop condition, ticks
    // snapshots, and notices when every receiver has exited.
    while !shutdown.load(Ordering::Relaxed) {
        if should_stop(
            &config,
            &start_instant,
            total_slots_collected.load(Ordering::Relaxed),
        ) {
            shutdown.store(true, Ordering::Relaxed);
            break;
        }
        if handles.iter().all(JoinHandle::is_finished) {
            tracing::warn!(
                "all receiver threads have exited; signalling dispatchers to drain and exit"
            );
            shutdown.store(true, Ordering::Relaxed);
            break;
        }
        if last_snapshot.elapsed() >= SNAPSHOT_TICK {
            log_snapshot(
                &stats_per_spec,
                &roles,
                total_slots_collected.load(Ordering::Relaxed),
            );
            last_snapshot = Instant::now();
        }
        thread::sleep(IDLE_SLEEP);
    }

    // Bounded wait for dispatchers to observe `shutdown`, drain
    // residual events on their rings, and exit.
    let dispatcher_deadline = Instant::now() + DISPATCHER_JOIN_TIMEOUT;
    while Instant::now() < dispatcher_deadline {
        if dispatcher_ep1.is_finished() && dispatcher_ep2.is_finished() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let dispatcher_ep1_result = reclaim_dispatcher(dispatcher_ep1, "ep1");
    let dispatcher_ep2_result = reclaim_dispatcher(dispatcher_ep2, "ep2");
    let dispatcher_stuck =
        dispatcher_ep1_result.is_none() || dispatcher_ep2_result.is_none();

    // Bounded wait for receivers (unchanged shutdown semantics from
    // before the dispatcher refactor — receivers are independent of
    // dispatchers, just feeding the rings).
    let join_deadline = Instant::now() + RECEIVER_JOIN_TIMEOUT;
    while Instant::now() < join_deadline {
        if handles.iter().all(JoinHandle::is_finished) {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let stuck = handles.iter().filter(|h| !h.is_finished()).count();
    if stuck > 0 {
        tracing::error!(
            stuck_threads = stuck,
            "receiver threads did not exit within {}s of shutdown; \
             output JSON will be written and the process will force-exit",
            RECEIVER_JOIN_TIMEOUT.as_secs()
        );
    }
    for h in handles {
        if h.is_finished() {
            let _ = h.join();
        }
        // Skip joining wedged threads; the force-exit below terminates them.
    }

    if let Some(w) = raw_writer.as_ref() {
        let mut w = w
            .lock()
            .expect("RawWriter mutex poisoned (a dispatcher panicked)");
        w.flush();
        tracing::info!(
            bytes = w.bytes_written(),
            path = %w.path().display(),
            "raw-records final size"
        );
        if let Some(warning) = w.take_warning() {
            tracing::warn!(warning, "raw-records writer reported error");
        }
    }

    // Reclaim per-endpoint state from dispatcher returns. Wedged
    // dispatchers fall back to empty per-endpoint state so the output
    // JSON is still well-formed; the force-exit below terminates the
    // wedged thread.
    let (mut stability_ep1, mut cross_ep1, counters_ep1) = match dispatcher_ep1_result {
        Some(d) => (d.stability, d.cross, d.counters),
        None => (
            StabilityTracker::new(EndpointRole::One),
            CrossStreamTracker::new(EndpointRole::One),
            IngestCountersPartial::default(),
        ),
    };
    let (mut stability_ep2, mut cross_ep2, counters_ep2) = match dispatcher_ep2_result {
        Some(d) => (d.stability, d.cross, d.counters),
        None => (
            StabilityTracker::new(EndpointRole::Two),
            CrossStreamTracker::new(EndpointRole::Two),
            IngestCountersPartial::default(),
        ),
    };
    let counters = IngestCounters {
        total_slots_collected: total_slots_collected.load(Ordering::Relaxed),
        accounts_ep1: counters_ep1.accounts,
        accounts_ep2: counters_ep2.accounts,
        transactions_ep1: counters_ep1.transactions,
        transactions_ep2: counters_ep2.transactions,
        blocks_ep1: counters_ep1.blocks,
        blocks_ep2: counters_ep2.blocks,
        entries_ep1: counters_ep1.entries,
        entries_ep2: counters_ep2.entries,
    };

    // Build host metadata after run completion so allocator / RT
    // outcomes are reflected.
    let rt_applied = rt_expected_count > 0
        && rt_applied_count.load(Ordering::Relaxed) >= rt_expected_count;

    let host_metadata = env::collect(
        config.realtime,
        rt_applied,
        false, // kernel timestamps active? Not yet wired; fallback path engaged.
        config.cpu_affinity.clone(),
    );

    let duration_ms = u64::try_from(start_instant.elapsed().as_millis()).unwrap_or(u64::MAX);
    let totals = compute_capture_totals(&roles, &stats_per_spec);
    let dropped = compute_dropped_totals(&roles, &stats_per_spec);

    // Reclaim `StreamMatchers` from the shared Arc. Both dispatchers
    // have exited (or wedged — wedged case falls through to empty
    // matchers and the force-exit below cleans up).
    let matchers = match Arc::try_unwrap(matchers) {
        Ok(m) => m,
        Err(arc) => {
            tracing::error!(
                strong_count = Arc::strong_count(&arc),
                "StreamMatchers Arc still held by a wedged dispatcher; emitting empty matchers"
            );
            StreamMatchers::new(HashMap::new())
        }
    };

    let output = assemble_output(
        &config,
        &programs,
        host_metadata,
        proto_metadata,
        &ep1_version,
        ep2_version.as_ref(),
        &matchers,
        &mut stability_ep1,
        &mut stability_ep2,
        &mut cross_ep1,
        &mut cross_ep2,
        &ping_ep1,
        &ping_ep2,
        start_wall_ms,
        duration_ms,
        &counters,
        totals,
        dropped,
    );

    let json = serde_json::to_string_pretty(&output)
        .context("serialize RunOutput to JSON")?;
    std::fs::write(&config.output, json)
        .with_context(|| format!("write --output {}", config.output.display()))?;
    tracing::info!(path = %config.output.display(), "wrote run output JSON");

    // Wedged receiver or dispatcher threads block normal process exit
    // because they are non-daemon. The output JSON is already flushed
    // at this point, so a forced exit is safe and keeps SIGINT latency
    // bounded even if a half-open TCP connection won't deliver RST or
    // a dispatcher is parked in a mutex.
    if stuck > 0 || dispatcher_stuck {
        tracing::warn!(
            stuck_receivers = stuck,
            dispatcher_stuck,
            "forcing process exit because some worker threads did not unwind"
        );
        std::process::exit(0);
    }

    Ok(config.output.clone())
}

/// Bounded reclaim of a dispatcher thread's owned state on shutdown.
/// If the dispatcher exited cleanly the join returns its [`DispatcherReturn`];
/// if it panicked or is still wedged we surface an error log and yield
/// `None` so the caller can fall back to empty per-endpoint state and
/// still produce a well-formed output JSON.
fn reclaim_dispatcher<T>(
    handle: JoinHandle<T>,
    label: &str,
) -> Option<T> {
    if !handle.is_finished() {
        tracing::error!(
            dispatcher = label,
            "dispatcher did not exit within {}s of shutdown; \
             output JSON will be written with empty per-endpoint state \
             and the process will force-exit",
            DISPATCHER_JOIN_TIMEOUT.as_secs()
        );
        return None;
    }
    if let Ok(d) = handle.join() {
        Some(d)
    } else {
        tracing::error!(dispatcher = label, "dispatcher panicked");
        None
    }
}

/// Per-event dispatch executed by a single per-endpoint dispatcher
/// thread.
///
/// `matchers` is shared by reference across both dispatchers; each
/// sub-matcher inside [`StreamMatchers`] carries its own [`Mutex`]
/// (except accounts, which uses DashMap-based per-program sharding
/// internally) so only same-kind events from the two endpoints
/// serialize, and the lock is held only for the per-event observe
/// call. Endpoint-local state (`stability`, `cross`, `counters`) is
/// owned by the calling dispatcher and lock-free.
#[allow(clippy::too_many_arguments)]
fn dispatch_event(
    event: &Event,
    matchers: &StreamMatchers,
    stability: &mut StabilityTracker,
    cross: &mut CrossStreamTracker,
    raw_writer: Option<&Mutex<RawWriter>>,
    counters: &mut IngestCountersPartial,
    total_slots: &AtomicU64,
) {
    crate::matching::dispatch(matchers, event);
    stability.observe(event);
    cross.observe(event);
    if let Some(w) = raw_writer {
        let mut w = w
            .lock()
            .expect("RawWriter mutex poisoned (a dispatcher panicked)");
        w.write(event);
    }

    let endpoint = event.subscription.endpoint();
    match &event.payload {
        EventPayload::Slot { slot, stage } => {
            // `--commitment processed,confirmed` opens two slot
            // subscriptions per endpoint, each carrying every stage;
            // dedup so ep1's `Processed` stage only bumps the counter
            // once per unique slot.
            if endpoint == EndpointRole::One
                && *stage == crate::collect::SlotStage::Processed
                && *slot > counters.last_counted_slot
            {
                counters.last_counted_slot = *slot;
                total_slots.fetch_add(1, Ordering::Relaxed);
            }
        }
        EventPayload::Account { .. } => counters.accounts += 1,
        EventPayload::Transaction { .. } => counters.transactions += 1,
        EventPayload::Block { .. } => counters.blocks += 1,
        EventPayload::Entry { .. } => counters.entries += 1,
    }
}

/// Owned state returned from a dispatcher thread on exit.
struct DispatcherReturn {
    stability: StabilityTracker,
    cross: CrossStreamTracker,
    counters: IngestCountersPartial,
}

/// Drain-and-dispatch loop run by one of the two per-endpoint
/// dispatcher threads. Each owns its endpoint's rings, stability +
/// cross trackers, and partial counters; the cross-endpoint
/// matchers and the optional raw-record sink are shared via
/// `Arc<_>` / `Arc<Mutex<_>>`.
///
/// Exits when `shutdown` is observed and any residual events are
/// drained within `DISPATCHER_POST_SHUTDOWN_BUDGET`.
#[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)]
fn dispatcher_thread_main(
    endpoint: EndpointRole,
    rings: Vec<Ring>,
    roles: Vec<SubscriptionRole>,
    mut stability: StabilityTracker,
    mut cross: CrossStreamTracker,
    mut counters: IngestCountersPartial,
    matchers: Arc<StreamMatchers>,
    raw_writer: Option<Arc<Mutex<RawWriter>>>,
    total_slots: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
) -> DispatcherReturn {
    tracing::info!(
        endpoint = ?endpoint,
        receivers = rings.len(),
        drain_budget_per_ring = DRAIN_BUDGET_PER_RING,
        "dispatcher entering"
    );
    let mut shutdown_observed_at: Option<Instant> = None;
    loop {
        let mut got_any = false;
        for (ring, role) in rings.iter().zip(&roles) {
            let mut drained_this_ring: usize = 0;
            while drained_this_ring < DRAIN_BUDGET_PER_RING {
                let Ok(event) = ring.receiver.try_recv() else {
                    break;
                };
                got_any = true;
                drained_this_ring += 1;
                ring.stats.received.fetch_add(1, Ordering::Relaxed);
                let _ = role;
                dispatch_event(
                    &event,
                    &matchers,
                    &mut stability,
                    &mut cross,
                    raw_writer.as_deref(),
                    &mut counters,
                    &total_slots,
                );
            }
        }

        let is_shutdown = shutdown.load(Ordering::Relaxed);
        if is_shutdown {
            let started = shutdown_observed_at.get_or_insert_with(Instant::now);
            if !got_any || started.elapsed() >= DISPATCHER_POST_SHUTDOWN_BUDGET {
                if got_any {
                    tracing::warn!(
                        endpoint = ?endpoint,
                        budget_secs = DISPATCHER_POST_SHUTDOWN_BUDGET.as_secs(),
                        "dispatcher post-shutdown budget exhausted; \
                         residual events left in rings (not load-bearing for \
                         comparative metrics — symmetric across endpoints)"
                    );
                }
                break;
            }
        } else if !got_any {
            thread::sleep(IDLE_SLEEP);
        }
    }
    tracing::info!(
        endpoint = ?endpoint,
        accounts = counters.accounts,
        transactions = counters.transactions,
        blocks = counters.blocks,
        entries = counters.entries,
        "dispatcher exiting"
    );
    DispatcherReturn {
        stability,
        cross,
        counters,
    }
}

fn should_stop(config: &Config, start: &Instant, total_slots: u64) -> bool {
    match config.stop {
        StopCondition::Slots(n) => total_slots >= n,
        StopCondition::Duration(secs) => start.elapsed() >= Duration::from_secs(secs),
        StopCondition::Either { slots, duration } => {
            total_slots >= slots || start.elapsed() >= Duration::from_secs(duration)
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn install_signal_handler(shutdown: Arc<AtomicBool>) {
    // Tokio's signal::ctrl_c() is async; we want a sync flag. Spawn a
    // task to flip the bit on Ctrl-C.
    let s = Arc::clone(&shutdown);
    tokio::spawn(async move {
        if let Ok(()) = tokio::signal::ctrl_c().await {
            tracing::info!("SIGINT received; initiating shutdown");
            s.store(true, Ordering::Relaxed);
        }
    });
}

/// Sleep for `dur`, polling shutdown every 250 ms. Returns `true`
/// if shutdown was observed (and the caller should exit).
async fn sleep_unless_shutdown(dur: Duration, shutdown: &AtomicBool) -> bool {
    let mut remaining = dur;
    let tick = Duration::from_millis(250);
    while remaining > Duration::ZERO {
        if shutdown.load(Ordering::Relaxed) {
            return true;
        }
        let s = remaining.min(tick);
        tokio::time::sleep(s).await;
        remaining = remaining.saturating_sub(s);
    }
    false
}

/// Per-endpoint ping task (spec §6.5). Opens a dedicated client
/// connection, calls `GetVersion` on subscription open and every
/// [`PING_INTERVAL`] thereafter, records each round-trip duration
/// into the shared [`PingTracker`]. Reconnects on RPC error or
/// timeout. Exits when `shutdown` flips.
async fn ping_task(
    endpoint: crate::config::EndpointSpec,
    max_decode_bytes: usize,
    role: EndpointRole,
    tracker: Arc<PingTracker>,
    shutdown: Arc<AtomicBool>,
) {
    while !shutdown.load(Ordering::Relaxed) {
        let mut client = match connect_with_decode_limit(&endpoint, max_decode_bytes).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    role = ?role,
                    url = endpoint.url,
                    error = %e,
                    "ping client connect failed; retrying after interval"
                );
                if sleep_unless_shutdown(PING_INTERVAL, &shutdown).await {
                    return;
                }
                continue;
            }
        };

        // Inner loop: ping on the same client until an error forces
        // a reconnect or shutdown is observed.
        loop {
            if shutdown.load(Ordering::Relaxed) {
                return;
            }
            let start = Instant::now();
            let result = tokio::time::timeout(
                PING_TIMEOUT,
                fetch_and_evaluate_version(&mut client, &endpoint.url),
            )
            .await;
            match result {
                Ok(Ok(_)) => {
                    let elapsed_us =
                        u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
                    tracker.record(elapsed_us);
                }
                Ok(Err(e)) => {
                    tracing::debug!(
                        role = ?role,
                        error = %e,
                        "ping GetVersion failed; reconnecting"
                    );
                    break;
                }
                Err(_) => {
                    tracing::debug!(
                        role = ?role,
                        "ping GetVersion timed out; reconnecting"
                    );
                    break;
                }
            }
            if sleep_unless_shutdown(PING_INTERVAL, &shutdown).await {
                return;
            }
        }
    }
}

async fn connect_and_eval(
    config: &Config,
    role: EndpointRole,
) -> Result<EndpointVersion> {
    let endpoint = match role {
        EndpointRole::One => &config.endpoint1,
        EndpointRole::Two => config
            .endpoint2
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("endpoint2 not configured (only valid with --solo)"))?,
    };
    let mut client = connect_with_decode_limit(endpoint, config.max_decode_mb * 1024 * 1024)
        .await
        .with_context(|| format!("connect {}", endpoint.url))?;
    let version = fetch_and_evaluate_version(&mut client, &endpoint.url)
        .await
        .with_context(|| format!("GetVersion {}", endpoint.url))?;
    tracing::info!(
        endpoint = endpoint.url,
        package = ?version.package,
        proto = ?version.proto_version,
        "endpoint version evaluated"
    );
    Ok(version)
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)] // single orchestration entry point per receiver thread.
fn receiver_thread_main(
    spec: SubscriptionSpec,
    sender: crate::collect::EventSender,
    role: SubscriptionRole,
    cpu_core: Option<u32>,
    realtime: bool,
    max_decode_bytes: usize,
    rt_applied_count: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    clock: ClockOrigin,
) {
    if let Some(core) = cpu_core {
        match apply_cpu_affinity(core) {
            SchedOutcome::Applied => {
                tracing::info!(role = ?role, core, "pinned receiver");
            }
            SchedOutcome::Unsupported => {
                tracing::debug!(role = ?role, "CPU affinity unsupported");
            }
            SchedOutcome::Failed(e) => {
                tracing::warn!(role = ?role, error = %e, "CPU pin failed");
            }
        }
    }
    if realtime {
        match apply_realtime() {
            SchedOutcome::Applied => {
                tracing::info!(role = ?role, "SCHED_FIFO applied");
                rt_applied_count.fetch_add(1, Ordering::Relaxed);
            }
            SchedOutcome::Unsupported => {
                tracing::warn!(role = ?role, "SCHED_FIFO unsupported on this platform");
            }
            SchedOutcome::Failed(e) => {
                tracing::error!(role = ?role, error = %e, "SCHED_FIFO rejected (need CAP_SYS_NICE)");
            }
        }
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build();
    let Ok(rt) = rt else {
        tracing::error!(role = ?role, "failed to build current_thread runtime");
        return;
    };
    rt.block_on(async move {
        // Outer loop handles reconnects: on stream end, record disconnect
        // and try reconnect after a short backoff. To avoid log spam when
        // the same server error fires every retry (e.g. tier filter cap),
        // we coalesce identical messages: log on transitions only, count
        // silently, and bail after a configurable limit.
        const MAX_CONSECUTIVE_SAME_ERROR: u32 = 5;
        let mut last_error_text: Option<String> = None;
        let mut same_error_count: u32 = 0;

        loop {
            if shutdown.load(Ordering::Relaxed) {
                return;
            }
            let mut client = match connect_with_decode_limit(&spec.endpoint, max_decode_bytes).await {
                Ok(c) => c,
                Err(e) => {
                    let text = format!("{e}");
                    if last_error_text.as_deref().is_none_or(|t| t != text.as_str()) {
                        tracing::warn!(role = ?role, error = %text, "connect failed; retrying in 2s");
                        last_error_text = Some(text);
                        same_error_count = 1;
                    } else {
                        same_error_count += 1;
                    }
                    if same_error_count >= MAX_CONSECUTIVE_SAME_ERROR {
                        tracing::error!(
                            role = ?role,
                            count = same_error_count,
                            "same connect error repeated; giving up on this subscription"
                        );
                        return;
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };
            let stream = match open_subscription(
                &mut client,
                spec.request.clone(),
                &spec.endpoint.url,
                clock,
            )
            .await
            {
                Ok(s) => s,
                Err(e) => {
                    let text = format!("{e}");
                    if last_error_text.as_deref().is_none_or(|t| t != text.as_str()) {
                        tracing::warn!(role = ?role, error = %text, "subscribe open failed; retrying in 2s");
                        last_error_text = Some(text);
                        same_error_count = 1;
                    } else {
                        same_error_count += 1;
                    }
                    if same_error_count >= MAX_CONSECUTIVE_SAME_ERROR {
                        tracing::error!(
                            role = ?role,
                            count = same_error_count,
                            "same subscribe-open error repeated; giving up on this subscription"
                        );
                        return;
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            let stats = sender.stats();
            futures::pin_mut!(stream);
            // Inner stream-drain loop, with `STREAM_POLL_TIMEOUT` bounding
            // how long any single `stream.next().await` can hold up a
            // shutdown signal (see the const definition near the top of
            // this module for the rationale).
            loop {
                if shutdown.load(Ordering::Relaxed) {
                    return;
                }
                let item = match tokio::time::timeout(
                    STREAM_POLL_TIMEOUT,
                    stream.next(),
                )
                .await
                {
                    Ok(Some(item)) => item,
                    // Stream ended cleanly with no error.
                    Ok(None) => break,
                    // Timeout — loop to re-check shutdown and poll again.
                    Err(_) => continue,
                };
                match item {
                    Ok(timed) => match decode(timed, role) {
                        Ok(Some(event)) => {
                            // Reset error coalescing on first successful
                            // decoded event — we know the connection is
                            // healthy and any next failure is a fresh one.
                            last_error_text = None;
                            same_error_count = 0;
                            let _ = sender.try_send(event);
                        }
                        Ok(None) => {} // control/skip
                        Err(e) => {
                            tracing::debug!(role = ?role, error = %e, "decode error");
                            stats.decode_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    },
                    Err(rpc_status) => {
                        let text = format!("{rpc_status}");
                        if last_error_text.as_deref().is_none_or(|t| t != text.as_str()) {
                            tracing::info!(role = ?role, status = %rpc_status, "stream ended");
                            last_error_text = Some(text);
                            same_error_count = 1;
                        } else {
                            same_error_count += 1;
                        }
                        stats.disconnects.fetch_add(1, Ordering::Relaxed);
                        if same_error_count >= MAX_CONSECUTIVE_SAME_ERROR {
                            tracing::error!(
                                role = ?role,
                                count = same_error_count,
                                "same stream-end status repeated; giving up on this subscription"
                            );
                            return;
                        }
                        break;
                    }
                }
            }
            // If we got here, the stream ended cleanly (None) or with a
            // status error. Either way, attempt reconnect.
        }
    });
}

fn log_snapshot(
    stats_per_spec: &[Arc<ReceiverStats>],
    roles: &[SubscriptionRole],
    total_slots: u64,
) {
    let mut ep1_received = 0u64;
    let mut ep2_received = 0u64;
    for (s, r) in stats_per_spec.iter().zip(roles) {
        let snap = s.snapshot();
        match r.endpoint() {
            EndpointRole::One => ep1_received += snap.received,
            EndpointRole::Two => ep2_received += snap.received,
        }
    }
    tracing::info!(
        ep1_received,
        ep2_received,
        total_slots,
        "snapshot",
    );
}

fn compute_capture_totals(
    roles: &[SubscriptionRole],
    stats: &[Arc<ReceiverStats>],
) -> CaptureTotals {
    let mut totals = CaptureTotals::default();
    for (s, r) in stats.iter().zip(roles) {
        let snap = s.snapshot();
        totals.add(r.endpoint(), &snap);
    }
    totals
}

/// Per-endpoint dropped-event counts, summed across that endpoint's
/// receiver rings. Populates `metadata.dropped_events_ep1` /
/// `metadata.dropped_events_ep2` (spec §8). A non-zero value indicates
/// the receiver hit ring saturation and dropped events to keep up
/// (spec §7 "drop on overflow rather than block the receiver").
#[derive(Debug, Default, Clone, Copy)]
struct DroppedTotals {
    ep1: u64,
    ep2: u64,
}

fn compute_dropped_totals(
    roles: &[SubscriptionRole],
    stats: &[Arc<ReceiverStats>],
) -> DroppedTotals {
    let mut out = DroppedTotals::default();
    for (s, r) in stats.iter().zip(roles) {
        let snap = s.snapshot();
        match r.endpoint() {
            EndpointRole::One => out.ep1 = out.ep1.saturating_add(snap.dropped),
            EndpointRole::Two => out.ep2 = out.ep2.saturating_add(snap.dropped),
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
fn assemble_output(
    config: &Config,
    programs: &ProgramSet,
    host_metadata: crate::env::HostMetadata,
    proto_metadata: ProtoMetadata,
    ep1_version: &EndpointVersion,
    ep2_version: Option<&EndpointVersion>,
    matchers: &StreamMatchers,
    stability_ep1: &mut StabilityTracker,
    stability_ep2: &mut StabilityTracker,
    cross_ep1: &mut CrossStreamTracker,
    cross_ep2: &mut CrossStreamTracker,
    ping_ep1: &PingTracker,
    ping_ep2: &PingTracker,
    start_wall_ms: u64,
    duration_ms: u64,
    counters: &IngestCounters,
    totals: CaptureTotals,
    dropped: DroppedTotals,
) -> RunOutput {
    let slot_status = matchers
        .slots
        .lock()
        .expect("slots matcher mutex poisoned")
        .summary();
    let account_delay = matchers.accounts.summary();
    let per_program = matchers.accounts.per_program_summary();
    let per_program: PerProgramSummary = per_program;
    let transaction_delay = if config.with_transactions {
        Some(
            matchers
                .transactions
                .lock()
                .expect("transactions matcher mutex poisoned")
                .summary(),
        )
    } else {
        None
    };
    let block_delay = if config.with_blocks {
        Some(
            matchers
                .blocks
                .lock()
                .expect("blocks matcher mutex poisoned")
                .summary(),
        )
    } else {
        None
    };

    let comparative = ComparativeSummary {
        slot_status,
        account_delay,
        transaction_delay,
        block_delay,
    };

    let mut cross: HashMap<&'static str, CrossStreamSummary> = HashMap::new();
    cross.insert("endpoint1", cross_ep1.summary());
    cross.insert("endpoint2", cross_ep2.summary());

    let mut stability: HashMap<&'static str, StabilitySummary> = HashMap::new();
    stability.insert("endpoint1", stability_ep1.summary());
    stability.insert("endpoint2", stability_ep2.summary());

    let mut endpoints = vec![endpoint_info(
        &config.endpoint1.url,
        EndpointRole::One,
        ep1_version,
        totals.ep1,
        ping_ep1,
    )];
    if let (Some(ep2_spec), Some(ep2_v)) = (config.endpoint2.as_ref(), ep2_version) {
        endpoints.push(endpoint_info(
            &ep2_spec.url,
            EndpointRole::Two,
            ep2_v,
            totals.ep2,
            ping_ep2,
        ));
    }

    let metadata = RunMetadata {
        total_slots_collected: counters.total_slots_collected,
        common_slots: 0, // populated by post-pass; left as 0 in v1 minimum
        duration_ms,
        total_account_updates: [counters.accounts_ep1, counters.accounts_ep2],
        total_transaction_updates: [counters.transactions_ep1, counters.transactions_ep2],
        total_block_updates: [counters.blocks_ep1, counters.blocks_ep2],
        total_entry_updates: [counters.entries_ep1, counters.entries_ep2],
        dropped_events_ep1: dropped.ep1,
        dropped_events_ep2: dropped.ep2,
    };

    RunOutput {
        version: env!("CARGO_PKG_VERSION").to_string(),
        harness: "grpc-bench",
        run_started_wall_ms: start_wall_ms,
        run_started_iso: RunOutput::rfc3339_from_wall_ms(start_wall_ms),
        host_metadata,
        proto_metadata,
        config: ConfigEcho::from_config(config),
        programs: programs.entries.clone(),
        metadata,
        endpoints,
        comparative,
        per_program_account_delay: per_program,
        cross_stream: cross,
        stability,
    }
}

fn endpoint_info(
    url: &str,
    role: EndpointRole,
    version: &EndpointVersion,
    total_updates: u64,
    ping: &PingTracker,
) -> EndpointInfo {
    EndpointInfo {
        endpoint: url.to_string(),
        role: RunOutput::role_label(role),
        plugin_type: version
            .package
            .clone()
            .unwrap_or_else(|| "yellowstone".to_string()),
        plugin_version: version
            .plugin_version
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        avg_ping_ms: ping.average_ms(),
        total_updates,
        unique_slots: 0, // populated by future per-endpoint slot tracker
    }
}

/// Suppress "static fields unused" warnings for now-known follow-ups.
#[allow(dead_code)]
const _UNUSED_DEFERRED: &[&str] = &[
    "AtomicU64 placeholder for kernel-timestamp wiring",
    "common_slots requires a cross-endpoint slot dedup pass",
];
#[allow(dead_code)]
static _SENTINEL: AtomicU64 = AtomicU64::new(0);
