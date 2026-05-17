//! Identity-tuple matching across endpoints (spec §6.1, §6.2).
//!
//! [`PairMatcher`] is the generic, single-threaded core: it holds pending
//! events keyed by identity, and on each `observe` looks up the opposite
//! endpoint's pending entry. A match updates a streaming t-digest of
//! deltas (in milliseconds), plus directional counts. Unmatched entries
//! are evicted by [`EvictionPolicy`] so memory stays bounded over long
//! soaks (spec §7 "Bound memory usage at all times").
//!
//! The per-stream wrappers in [`accounts`], [`transactions`], [`blocks`],
//! and [`slots`] add the stream-specific identity types and (for
//! accounts) per-program bucket dispatch (spec §6.2).
//!
//! Module name is [`matching`] rather than the `match` listed in spec
//! §12.C because `match` is a Rust reserved keyword.

pub mod accounts;
pub mod blocks;
pub mod slots;
pub mod transactions;

use std::{cmp::Ordering, collections::HashMap, hash::Hash, sync::Mutex};

use serde::Serialize;
use tdigest::TDigest;

use crate::{
    collect::{Event, ReceiverStatsSnapshot},
    subscribe::EndpointRole,
    timing::EventTimestamp,
};

/// Buffer size before a `merge_unsorted` flush. The `tdigest` crate is
/// allocation-heavy on each merge call, so we batch values into a Vec and
/// flush periodically. Chosen to amortize the merge cost without making
/// quantile reads stale: at 5000-events/sec a 1024 buffer flushes ~5x/sec.
pub const TDIGEST_BUFFER_FLUSH: usize = 1024;

/// `tdigest` compression factor. Higher = more accurate but slower /
/// bigger. 100 is the default the crate ships and produces sub-1%
/// quantile error on uniform inputs, comfortably within the spec §10
/// "p99 within 1% of true p99" test target.
pub const TDIGEST_COMPRESSION: usize = 100;

/// Observations between eviction passes (spec §7 "Bound memory usage at
/// all times"). Calling `evict()` on every `observe` was the
/// dispatcher-CPU hot path at saturating load: a per-event
/// `HashMap::retain` over thousands of pending entries dominated the
/// per-event budget. Batching to one pass every 256 observes preserves
/// the slot-window memory bound (entries still age out, just on the
/// batched cadence) while removing the per-event scan. End-of-run
/// `LatencyDigestSummary::from_matcher` calls `evict` once explicitly
/// so reported `unmatched_evicted` counts are exact.
const EVICT_EVERY: u64 = 256;

/// Eviction strategy for pending events that never matched.
#[derive(Debug, Clone, Copy)]
pub enum EvictionPolicy {
    /// Drop entries whose slot is more than `window` behind the most
    /// recent slot we've observed on either endpoint. `window = 64`
    /// roughly corresponds to ~25s of slot lag — well beyond what a
    /// healthy network would produce, but short enough to bound memory.
    SlotWindow {
        /// Number of slots to keep on each side of the current slot.
        window: u64,
    },
    /// Drop the oldest entries when the pending map exceeds `max_pending`
    /// per endpoint. Fallback for streams without a slot in their key.
    Lru {
        /// Maximum pending entries per endpoint.
        max_pending: usize,
    },
}

/// Directional counters reported alongside the t-digest summary.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct MatchCounts {
    /// Number of pairs that matched across endpoints.
    pub matched: u64,
    /// Matches where endpoint1 arrived first (positive `delta`).
    pub ep1_faster: u64,
    /// Matches where endpoint2 arrived first (negative `delta`).
    pub ep2_faster: u64,
    /// Pending entries from endpoint1 that aged out before a match was
    /// observed. Surfaces capture parity issues.
    pub ep1_unmatched_evicted: u64,
    /// Pending entries from endpoint2 that aged out before a match.
    pub ep2_unmatched_evicted: u64,
}

/// One pair-matcher for a single identity type.
#[derive(Debug)]
pub struct PairMatcher<K: Eq + Hash + Clone> {
    pending_ep1: HashMap<K, MatchPending>,
    pending_ep2: HashMap<K, MatchPending>,
    digest: TDigest,
    buffer: Vec<f64>,
    counts: MatchCounts,
    eviction: EvictionPolicy,
    /// Highest slot seen across endpoints (only used by
    /// [`EvictionPolicy::SlotWindow`]).
    high_water_slot: u64,
    /// Observe counter; eviction runs once every `EVICT_EVERY` observes
    /// rather than on every event (hot-path amortization).
    obs_count: u64,
}

/// Per-pending entry, holding the arrival timestamp plus any side-channel
/// metadata the matcher might need at pair-time (e.g. owner for accounts,
/// `tx_count` for blocks). Side-channel data is opaque to the generic
/// matcher; callers stash whatever they need.
#[derive(Debug, Clone)]
pub struct MatchPending {
    /// Arrival timestamp.
    pub ts: EventTimestamp,
    /// Slot, used by [`EvictionPolicy::SlotWindow`] to age out entries.
    pub slot: u64,
}

impl<K: Eq + Hash + Clone> PairMatcher<K> {
    /// Fresh matcher with the given eviction policy.
    #[must_use]
    pub fn new(eviction: EvictionPolicy) -> Self {
        Self {
            pending_ep1: HashMap::new(),
            pending_ep2: HashMap::new(),
            digest: TDigest::new_with_size(TDIGEST_COMPRESSION),
            buffer: Vec::with_capacity(TDIGEST_BUFFER_FLUSH),
            counts: MatchCounts::default(),
            eviction,
            high_water_slot: 0,
            obs_count: 0,
        }
    }

    /// Record an observation from one endpoint. On match, returns the
    /// matched pair's `(delta_ms, ep1_ts, ep2_ts)` so callers can apply
    /// per-stream side effects (e.g. per-program bucketing for accounts).
    /// Otherwise returns `None`.
    pub fn observe(
        &mut self,
        endpoint: EndpointRole,
        key: K,
        ts: EventTimestamp,
        slot: u64,
    ) -> Option<MatchResult> {
        if slot > self.high_water_slot {
            self.high_water_slot = slot;
        }
        self.obs_count = self.obs_count.wrapping_add(1);
        if self.obs_count % EVICT_EVERY == 0 {
            self.evict();
        }

        let (own_pending, other_pending, sign): (
            &mut HashMap<K, MatchPending>,
            &mut HashMap<K, MatchPending>,
            i64,
        ) = match endpoint {
            EndpointRole::One => (
                &mut self.pending_ep1,
                &mut self.pending_ep2,
                1, // delta = ep2 - ep1; arriving as ep1 means we'll subtract ours later.
            ),
            EndpointRole::Two => (&mut self.pending_ep2, &mut self.pending_ep1, -1),
        };

        if let Some(other) = other_pending.remove(&key) {
            // Match: delta_ns = ep2 - ep1, always.
            let (ep1_ts, ep2_ts) = match endpoint {
                EndpointRole::One => (ts, other.ts),
                EndpointRole::Two => (other.ts, ts),
            };
            let delta_ns = i64::try_from(ep2_ts.mono_ns).unwrap_or(i64::MAX)
                - i64::try_from(ep1_ts.mono_ns).unwrap_or(i64::MAX);
            // `sign` was carried for symmetry above; computed delta is
            // already signed correctly because we determined which side
            // is ep1 vs ep2 by `endpoint`.
            let _ = sign;
            // Convert ns → ms for the t-digest. f64 has 52 bits of
            // mantissa; ms values comfortably fit.
            #[allow(clippy::cast_precision_loss)]
            let delta_in_ms = (delta_ns as f64) / 1_000_000.0;
            self.push_delta(delta_in_ms);
            self.counts.matched += 1;
            match delta_ns.cmp(&0) {
                Ordering::Greater => self.counts.ep1_faster += 1,
                Ordering::Less => self.counts.ep2_faster += 1,
                Ordering::Equal => {}
            }
            return Some(MatchResult {
                delta_ms: delta_in_ms,
                ep1_ts,
                ep2_ts,
            });
        }

        own_pending.insert(key, MatchPending { ts, slot });
        None
    }

    fn push_delta(&mut self, delta_ms: f64) {
        self.buffer.push(delta_ms);
        if self.buffer.len() >= TDIGEST_BUFFER_FLUSH {
            self.flush();
        }
    }

    /// Flush the buffer into the digest. Idempotent.
    pub fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let taken = std::mem::take(&mut self.buffer);
        self.digest = self.digest.merge_unsorted(taken);
    }

    /// Run a single eviction pass on the configured [`EvictionPolicy`].
    ///
    /// Normally invoked automatically once every [`EVICT_EVERY`] observes
    /// to amortize the per-event scan cost. End-of-run summary code calls
    /// this once explicitly so the reported `unmatched_evicted` counts
    /// reflect all aged-out entries, not just those caught by the last
    /// batched pass.
    pub fn evict(&mut self) {
        match self.eviction {
            EvictionPolicy::SlotWindow { window } => {
                let cutoff = self.high_water_slot.saturating_sub(window);
                let before_ep1 = self.pending_ep1.len();
                self.pending_ep1.retain(|_, v| v.slot >= cutoff);
                let evicted1 = u64::try_from(before_ep1 - self.pending_ep1.len()).unwrap_or(0);
                self.counts.ep1_unmatched_evicted += evicted1;
                let before_ep2 = self.pending_ep2.len();
                self.pending_ep2.retain(|_, v| v.slot >= cutoff);
                let evicted2 = u64::try_from(before_ep2 - self.pending_ep2.len()).unwrap_or(0);
                self.counts.ep2_unmatched_evicted += evicted2;
            }
            EvictionPolicy::Lru { max_pending } => {
                if self.pending_ep1.len() > max_pending {
                    // HashMap iteration order is arbitrary; pop one entry
                    // per excess slot. The fallback policy is here for
                    // streams without slot keys, which is currently none,
                    // so the simple eviction is fine.
                    let excess = self.pending_ep1.len() - max_pending;
                    let keys: Vec<K> = self.pending_ep1.keys().take(excess).cloned().collect();
                    for k in keys {
                        self.pending_ep1.remove(&k);
                    }
                    self.counts.ep1_unmatched_evicted +=
                        u64::try_from(excess).unwrap_or(0);
                }
                if self.pending_ep2.len() > max_pending {
                    let excess = self.pending_ep2.len() - max_pending;
                    let keys: Vec<K> = self.pending_ep2.keys().take(excess).cloned().collect();
                    for k in keys {
                        self.pending_ep2.remove(&k);
                    }
                    self.counts.ep2_unmatched_evicted +=
                        u64::try_from(excess).unwrap_or(0);
                }
            }
        }
    }

    /// Read the current quantile estimate. Flushes the buffer first.
    pub fn quantile(&mut self, q: f64) -> f64 {
        self.flush();
        if self.digest.is_empty() {
            return f64::NAN;
        }
        self.digest.estimate_quantile(q)
    }

    /// Flush the buffer and return a clone of the underlying t-digest.
    /// Used by sharded matchers ([`accounts::AccountMatcher`]) to merge
    /// per-shard digests into one at summary-time via
    /// [`TDigest::merge_digests`]. End-of-run path; not on the hot path.
    pub fn snapshot_digest(&mut self) -> TDigest {
        self.flush();
        self.digest.clone()
    }

    /// Current match counts.
    #[must_use]
    pub fn counts(&self) -> MatchCounts {
        self.counts
    }

    /// Test-only / introspection: number of pending unmatched entries on
    /// each endpoint after the last `evict` (which runs on every
    /// observe).
    #[must_use]
    pub fn pending_counts(&self) -> (usize, usize) {
        (self.pending_ep1.len(), self.pending_ep2.len())
    }
}

/// Outcome of a successful identity match.
#[derive(Debug, Clone, Copy)]
pub struct MatchResult {
    /// `(ep2_arrival - ep1_arrival)` in milliseconds. Positive = ep1 was
    /// faster (i.e. ep2 was slower).
    pub delta_ms: f64,
    /// Endpoint1 arrival timestamp.
    pub ep1_ts: EventTimestamp,
    /// Endpoint2 arrival timestamp.
    pub ep2_ts: EventTimestamp,
}

/// Per-stream quantile summary, ready to serialize into the §8 JSON.
#[derive(Debug, Clone, Serialize)]
pub struct LatencyDigestSummary {
    /// p50 (median) of `(ep2 - ep1)` milliseconds.
    pub p50: f64,
    /// p90.
    pub p90: f64,
    /// p99.
    pub p99: f64,
    /// p99.9.
    pub p99_9: f64,
    /// Matched count.
    pub matched: u64,
    /// Matches where ep1 arrived first.
    pub ep1_faster: u64,
    /// Matches where ep2 arrived first.
    pub ep2_faster: u64,
}

impl LatencyDigestSummary {
    /// Compute a summary from a [`PairMatcher`]. Flushes the buffer as a
    /// side-effect (mutating the matcher reference).
    pub fn from_matcher<K: Eq + Hash + Clone>(m: &mut PairMatcher<K>) -> Self {
        // Force a final eviction so `unmatched_evicted` counts are exact
        // (lazy mid-run eviction may have skipped up to `EVICT_EVERY - 1`
        // observes since the last pass).
        m.evict();
        let counts = m.counts();
        Self {
            p50: m.quantile(0.50),
            p90: m.quantile(0.90),
            p99: m.quantile(0.99),
            p99_9: m.quantile(0.999),
            matched: counts.matched,
            ep1_faster: counts.ep1_faster,
            ep2_faster: counts.ep2_faster,
        }
    }
}

/// Total events received per endpoint, used to populate
/// `metadata.total_*_updates` and capture parity (spec §8, §9.3).
#[derive(Debug, Default, Clone, Copy)]
pub struct CaptureTotals {
    /// Sum from endpoint1 receivers.
    pub ep1: u64,
    /// Sum from endpoint2 receivers.
    pub ep2: u64,
}

impl CaptureTotals {
    /// Accumulate a stats snapshot for the given endpoint.
    pub fn add(&mut self, role: EndpointRole, snap: &ReceiverStatsSnapshot) {
        match role {
            EndpointRole::One => self.ep1 = self.ep1.saturating_add(snap.received),
            EndpointRole::Two => self.ep2 = self.ep2.saturating_add(snap.received),
        }
    }
}

/// Dispatch an [`Event`] into the appropriate per-stream matcher.
///
/// Each non-accounts sub-matcher carries its own [`Mutex`] so the
/// two per-endpoint dispatcher threads serialize only on same-kind
/// events; only the per-event observe call is locked. Accounts
/// uses internal DashMap sharding (single per-program-shard
/// locking), so its `observe` takes `&self`.
///
/// # Panics
/// Panics if a sub-matcher mutex was poisoned by a panic in another
/// dispatcher thread. Treated as a fatal-startup-state invariant.
pub fn dispatch(matchers: &StreamMatchers, event: &Event) {
    use crate::collect::EventPayload;
    use crate::subscribe::SubscriptionRole;
    let endpoint = event.subscription.endpoint();
    let ts = event.ts;
    match &event.payload {
        EventPayload::Slot { slot, stage } => {
            matchers
                .slots
                .lock()
                .expect("slots matcher mutex poisoned")
                .observe_stage(endpoint, *slot, *stage, ts);
        }
        EventPayload::Account {
            slot,
            pubkey,
            owner,
            write_version,
            txn_signature,
            ..
        } => {
            matchers.accounts.observe(
                endpoint, *slot, *pubkey, *write_version, *txn_signature, *owner, ts,
            );
        }
        EventPayload::Transaction { slot, signature, .. } => {
            matchers
                .transactions
                .lock()
                .expect("transactions matcher mutex poisoned")
                .observe(endpoint, *slot, *signature, ts);
        }
        EventPayload::Block { slot, blockhash, .. } => {
            if let SubscriptionRole::Main { commitment, .. } = event.subscription {
                matchers
                    .blocks
                    .lock()
                    .expect("blocks matcher mutex poisoned")
                    .observe(endpoint, commitment, *slot, *blockhash, ts);
            }
        }
        EventPayload::Entry { .. } => {
            // Entries are not matched across endpoints in v1 — the
            // entries-only subscription typically lives on a different URL
            // (Quicknode entries endpoint) and the cross-endpoint metric
            // doesn't apply. Entries feed the cross-stream layer instead
            // (spec §6.3); see [`crate::crossstream`].
        }
    }
}

/// Aggregate of every matcher used by the ingest loop. Slots,
/// transactions, and blocks each have their own [`Mutex`] so the
/// two per-endpoint dispatcher threads serialize only on same-kind
/// events. Accounts uses DashMap-based internal sharding so its
/// `observe` takes `&self` and same-program events serialize at
/// the per-program-shard lock rather than the outer wrapper.
#[derive(Debug)]
pub struct StreamMatchers {
    /// Slot-status matchers (one `PairMatcher` per stage).
    pub slots: Mutex<slots::SlotMatcher>,
    /// Accounts matcher with internal DashMap-based per-program shards.
    pub accounts: accounts::AccountMatcher,
    /// Transactions matcher.
    pub transactions: Mutex<transactions::TransactionMatcher>,
    /// Blocks matcher.
    pub blocks: Mutex<blocks::BlockMatcher>,
}

impl StreamMatchers {
    /// Construct the standard set with the spec-recommended slot-window
    /// eviction policies. The 64-slot window is generous (~25s) so we
    /// don't evict in a healthy network.
    #[must_use]
    pub fn new(program_short_names: HashMap<crate::collect::Pubkey32, String>) -> Self {
        let window = EvictionPolicy::SlotWindow { window: 64 };
        Self {
            slots: Mutex::new(slots::SlotMatcher::new(window)),
            accounts: accounts::AccountMatcher::new(window, program_short_names),
            transactions: Mutex::new(transactions::TransactionMatcher::new(window)),
            blocks: Mutex::new(blocks::BlockMatcher::new(window)),
        }
    }

    /// Enable or disable strict accounts-key mode on the inner
    /// `AccountMatcher`. Builder-style for use immediately after
    /// `new`. See [`accounts::AccountMatcher::with_strict_account_key`].
    #[must_use]
    pub fn with_strict_account_key(mut self, strict: bool) -> Self {
        self.accounts = self.accounts.with_strict_account_key(strict);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pair_matcher_emits_match_with_positive_delta_when_ep1_first() {
        let mut m: PairMatcher<u64> = PairMatcher::new(EvictionPolicy::SlotWindow { window: 64 });
        let ts1 = EventTimestamp {
            mono_ns: 1_000_000,
            wall_ms: 1,
        };
        let ts2 = EventTimestamp {
            mono_ns: 4_000_000, // 3 ms later
            wall_ms: 2,
        };
        assert!(m.observe(EndpointRole::One, 42, ts1, 100).is_none());
        let r = m
            .observe(EndpointRole::Two, 42, ts2, 100)
            .expect("matched");
        assert!((r.delta_ms - 3.0).abs() < 1e-9);
        let counts = m.counts();
        assert_eq!(counts.matched, 1);
        assert_eq!(counts.ep1_faster, 1);
        assert_eq!(counts.ep2_faster, 0);
    }

    #[test]
    fn pair_matcher_emits_match_with_negative_delta_when_ep2_first() {
        let mut m: PairMatcher<u64> = PairMatcher::new(EvictionPolicy::SlotWindow { window: 64 });
        let ts2 = EventTimestamp {
            mono_ns: 1_000_000,
            wall_ms: 1,
        };
        let ts1 = EventTimestamp {
            mono_ns: 4_000_000,
            wall_ms: 2,
        };
        m.observe(EndpointRole::Two, 42, ts2, 100);
        let r = m
            .observe(EndpointRole::One, 42, ts1, 100)
            .expect("matched");
        assert!((r.delta_ms - (-3.0)).abs() < 1e-9);
        let counts = m.counts();
        assert_eq!(counts.matched, 1);
        assert_eq!(counts.ep1_faster, 0);
        assert_eq!(counts.ep2_faster, 1);
    }

    #[test]
    fn pair_matcher_evicts_pending_outside_slot_window() {
        let mut m: PairMatcher<u64> = PairMatcher::new(EvictionPolicy::SlotWindow { window: 4 });
        m.observe(
            EndpointRole::One,
            10,
            EventTimestamp {
                mono_ns: 1,
                wall_ms: 1,
            },
            100,
        );
        // Bump high water by observing a much later slot.
        m.observe(
            EndpointRole::One,
            11,
            EventTimestamp {
                mono_ns: 2,
                wall_ms: 2,
            },
            200,
        );
        // Eviction is now batched every EVICT_EVERY observes; force a
        // pass explicitly so this test's two-observe scenario can still
        // assert the slot-window predicate.
        m.evict();
        let counts = m.counts();
        assert_eq!(counts.ep1_unmatched_evicted, 1);
        // The first key should be gone.
        assert_eq!(m.pending_counts().0, 1);
    }

    #[test]
    fn pair_matcher_runs_lazy_eviction_after_threshold() {
        let mut m: PairMatcher<u64> = PairMatcher::new(EvictionPolicy::SlotWindow { window: 4 });
        // Insert one key at a low slot, then observe EVICT_EVERY-1
        // distinct keys at a high slot. The Nth (EVICT_EVERY-th) observe
        // should trigger an automatic eviction pass.
        m.observe(
            EndpointRole::One,
            0,
            EventTimestamp {
                mono_ns: 0,
                wall_ms: 0,
            },
            100,
        );
        for i in 1..EVICT_EVERY {
            m.observe(
                EndpointRole::One,
                i,
                EventTimestamp {
                    mono_ns: i,
                    wall_ms: 0,
                },
                500,
            );
        }
        // Exactly EVICT_EVERY observes done; the boundary observe ran
        // evict() automatically. Key 0 (slot 100) is now outside the
        // window-4 cutoff of high_water_slot=500.
        let counts = m.counts();
        assert!(
            counts.ep1_unmatched_evicted >= 1,
            "expected automatic eviction at EVICT_EVERY boundary"
        );
    }

    #[test]
    fn quantile_returns_nan_on_empty_digest() {
        let mut m: PairMatcher<u64> = PairMatcher::new(EvictionPolicy::SlotWindow { window: 64 });
        let q = m.quantile(0.5);
        assert!(q.is_nan());
    }

    #[test]
    fn quantile_estimate_within_one_percent_on_uniform_input() {
        // Spec §10: t-digest p99 within 1% of true p99.
        let mut m: PairMatcher<u64> = PairMatcher::new(EvictionPolicy::SlotWindow { window: 9999 });
        // Feed 10k matched pairs with deltas evenly spaced from 0..10000ns
        // ⇒ delta_ms 0..0.01. True p99 = 9900ns ⇒ 0.0099ms.
        for i in 0..10_000_u64 {
            let key = i;
            let ts1 = EventTimestamp {
                mono_ns: 0,
                wall_ms: 0,
            };
            let ts2 = EventTimestamp {
                mono_ns: i,
                wall_ms: 0,
            };
            m.observe(EndpointRole::One, key, ts1, 1);
            m.observe(EndpointRole::Two, key, ts2, 1);
        }
        let p99 = m.quantile(0.99);
        let truth = 9900.0 / 1_000_000.0;
        let err = (p99 - truth).abs() / truth;
        assert!(err < 0.01, "p99={p99} truth={truth} err={err}");
    }

    #[test]
    fn summary_from_matcher_carries_counts() {
        let mut m: PairMatcher<u64> = PairMatcher::new(EvictionPolicy::SlotWindow { window: 64 });
        m.observe(
            EndpointRole::One,
            1,
            EventTimestamp {
                mono_ns: 0,
                wall_ms: 0,
            },
            10,
        );
        m.observe(
            EndpointRole::Two,
            1,
            EventTimestamp {
                mono_ns: 5_000_000,
                wall_ms: 0,
            },
            10,
        );
        let s = LatencyDigestSummary::from_matcher(&mut m);
        assert_eq!(s.matched, 1);
        assert!(s.p50.is_finite());
    }
}
