//! Account-stream matching (spec §6.1, §6.2).
//!
//! Identity tuple: `(slot, pubkey, txn_signature)`.
//!
//! Deviation from spec §6.1, which lists
//! `(slot, pubkey, write_version, tx_signature)`: `write_version` is
//! validator-local state — each validator's Geyser plugin maintains its
//! own monotonic counter, so the same network-level write gets a
//! different `write_version` on different validators. The spec was
//! written for an earlier benchmark where both endpoints shared a
//! single backend, so the counters aligned. For the general case
//! (independent dedicated nodes, different providers, anywhere the
//! two endpoints are distinct validator instances) including
//! `write_version` in the key guarantees zero matches.
//!
//! `txn_signature` is `Option<[u8; 64]>` because the proto field is
//! optional — when both endpoints report `None` for the same
//! `(slot, pubkey)` we still match. `write_version` remains on the
//! [`crate::collect::EventPayload::Account`] payload (and in the raw
//! record stream) so the operator can use it for per-endpoint
//! debugging.
//!
//! Edge case — CPI writes: when a single transaction writes the same
//! pubkey multiple times via cross-program invocation, all writes share
//! `(slot, pubkey, txn_signature)`. The matcher keeps only the last
//! pending entry per key on each endpoint (overwrite on insert), so a
//! CPI burst of N writes contributes one match instead of N. The
//! per-endpoint event counts in `metadata.total_*_updates` still
//! reflect honest capture volume; only the matched count is affected.
//!
//! Per-program sharding via [`DashMap`]: events are routed to one
//! [`PerProgramShard`] keyed on the account `owner`. Each shard owns
//! its own [`PairMatcher`] (with its own pending maps + t-digest) and
//! its own [`ProgramDigest`] for §8 per-program output. DashMap's
//! internal shard striping (4 × num_cpus shards by default) gives
//! lock-free access across most program keys; same-shard contention
//! only kicks in when two events land on the same DashMap-internal
//! shard, which at 23 programs across 64+ internal shards is rare.
//!
//! Interior-mutability (DashMap rather than plain HashMap) is
//! required because in the hybrid dispatcher layout two writers
//! exist — one accounts dispatcher per endpoint, both writing to a
//! shared `Arc<AccountMatcher>` so cross-endpoint pairing can find
//! matches across the two threads. `observe()` therefore takes
//! `&self`.

use std::collections::HashMap;

use dashmap::DashMap;
use serde::Serialize;
use tdigest::TDigest;

use crate::{
    collect::{Pubkey32, Signature64},
    subscribe::EndpointRole,
    timing::EventTimestamp,
};

use super::{EvictionPolicy, LatencyDigestSummary, MatchCounts, PairMatcher, TDIGEST_COMPRESSION};

/// Compound identity key for accounts.
///
/// `(slot, pubkey, txn_signature, write_version_opt)`. The trailing
/// `Option<u64>` is `Some(write_version)` only when strict-key mode
/// is enabled (`--strict-account-key`, for thorofare-parity
/// validation against same-source endpoints); otherwise `None`. See
/// module-level docs for the cross-validator-divergence reasoning.
pub type AccountKey = (u64, Pubkey32, Option<Signature64>, Option<u64>);

/// Per-program digest buffer.
#[derive(Debug)]
pub struct ProgramDigest {
    digest: tdigest::TDigest,
    buffer: Vec<f64>,
    counts: super::MatchCounts,
}

impl ProgramDigest {
    fn new() -> Self {
        Self {
            digest: tdigest::TDigest::new_with_size(super::TDIGEST_COMPRESSION),
            buffer: Vec::with_capacity(super::TDIGEST_BUFFER_FLUSH),
            counts: super::MatchCounts::default(),
        }
    }

    fn push(&mut self, delta_ms: f64) {
        self.buffer.push(delta_ms);
        self.counts.matched += 1;
        if delta_ms > 0.0 {
            self.counts.ep1_faster += 1;
        } else if delta_ms < 0.0 {
            self.counts.ep2_faster += 1;
        }
        if self.buffer.len() >= super::TDIGEST_BUFFER_FLUSH {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let taken = std::mem::take(&mut self.buffer);
        self.digest = self.digest.merge_unsorted(taken);
    }

    fn quantile(&mut self, q: f64) -> f64 {
        self.flush();
        if self.digest.is_empty() {
            return f64::NAN;
        }
        self.digest.estimate_quantile(q)
    }

    fn summary(&mut self) -> LatencyDigestSummary {
        let counts = self.counts;
        LatencyDigestSummary {
            p50: self.quantile(0.50),
            p90: self.quantile(0.90),
            p99: self.quantile(0.99),
            p99_9: self.quantile(0.999),
            matched: counts.matched,
            ep1_faster: counts.ep1_faster,
            ep2_faster: counts.ep2_faster,
        }
    }
}

/// Per-program shard: one `PairMatcher` for cross-endpoint pairing and
/// one `ProgramDigest` for the §8 per-program output. All state for a
/// single program lives here. Same-program events from the two
/// accounts dispatchers serialize at the DashMap-shard write lock;
/// different programs run in parallel.
#[derive(Debug)]
struct PerProgramShard {
    main: PairMatcher<AccountKey>,
    digest: ProgramDigest,
}

impl PerProgramShard {
    fn new(eviction: EvictionPolicy) -> Self {
        Self {
            main: PairMatcher::new(eviction),
            digest: ProgramDigest::new(),
        }
    }
}

/// Account matcher with per-program shard dispatch via [`DashMap`].
/// Shared across the two per-endpoint accounts dispatcher threads;
/// `observe()` takes `&self` because interior mutability flows
/// through the DashMap shard write lock.
#[derive(Debug)]
pub struct AccountMatcher {
    shards: DashMap<Pubkey32, PerProgramShard>,
    program_short_names: HashMap<Pubkey32, String>,
    eviction: EvictionPolicy,
    strict_account_key: bool,
}

impl AccountMatcher {
    /// Construct an accounts matcher with the program-short-name
    /// lookup. Owners not present in the map fall under the
    /// `"unknown"` bucket at summary time. Defaults to lenient
    /// account-key mode (write_version excluded from identity);
    /// chain [`Self::with_strict_account_key`] to opt in to strict
    /// mode for thorofare-parity validation.
    #[must_use]
    pub fn new(eviction: EvictionPolicy, program_short_names: HashMap<Pubkey32, String>) -> Self {
        Self {
            shards: DashMap::new(),
            program_short_names,
            eviction,
            strict_account_key: false,
        }
    }

    /// Enable or disable strict-key mode. When strict, `write_version`
    /// is included in the identity tuple (matches thorofare's
    /// `(slot, pubkey, write_version, sig)` exactly). Use this only
    /// when comparing two endpoints backed by the same upstream
    /// validator; cross-provider runs MUST stay in the default
    /// lenient mode or matches go to zero.
    #[must_use]
    pub fn with_strict_account_key(mut self, strict: bool) -> Self {
        self.strict_account_key = strict;
        self
    }

    /// Observe an account update.
    ///
    /// `write_version` enters the identity key only when
    /// `with_strict_account_key(true)` has been chained at
    /// construction; otherwise it is dropped and the spec §6.1
    /// deviation applies.
    ///
    /// Takes `&self`: interior mutability comes from DashMap's
    /// per-shard write lock plus the `PairMatcher`/`ProgramDigest`
    /// inside each shard.
    #[allow(clippy::too_many_arguments)]
    pub fn observe(
        &self,
        endpoint: EndpointRole,
        slot: u64,
        pubkey: Pubkey32,
        write_version: u64,
        txn_signature: Option<Signature64>,
        owner: Pubkey32,
        ts: EventTimestamp,
    ) {
        let wv_for_key = if self.strict_account_key {
            Some(write_version)
        } else {
            None
        };
        let key: AccountKey = (slot, pubkey, txn_signature, wv_for_key);
        let mut shard = self
            .shards
            .entry(owner)
            .or_insert_with(|| PerProgramShard::new(self.eviction));
        if let Some(r) = shard.main.observe(endpoint, key, ts, slot) {
            shard.digest.push(r.delta_ms);
        }
    }

    /// Build the §8 `comparative.account_delay` block (overall).
    pub fn summary(&self) -> LatencyDigestSummary {
        let mut digests: Vec<TDigest> = Vec::with_capacity(self.shards.len());
        let mut counts = MatchCounts::default();
        for mut entry in self.shards.iter_mut() {
            let shard = entry.value_mut();
            // Final eviction so `unmatched_evicted` is exact (see
            // PairMatcher::evict — lazy mid-run eviction may have
            // skipped up to EVICT_EVERY-1 observes since the last pass).
            shard.main.evict();
            digests.push(shard.main.snapshot_digest());
            let c = shard.main.counts();
            counts.matched += c.matched;
            counts.ep1_faster += c.ep1_faster;
            counts.ep2_faster += c.ep2_faster;
            counts.ep1_unmatched_evicted += c.ep1_unmatched_evicted;
            counts.ep2_unmatched_evicted += c.ep2_unmatched_evicted;
        }
        let merged = if digests.is_empty() {
            TDigest::new_with_size(TDIGEST_COMPRESSION)
        } else {
            TDigest::merge_digests(digests)
        };
        let (p50, p90, p99, p99_9) = if merged.is_empty() {
            (f64::NAN, f64::NAN, f64::NAN, f64::NAN)
        } else {
            (
                merged.estimate_quantile(0.50),
                merged.estimate_quantile(0.90),
                merged.estimate_quantile(0.99),
                merged.estimate_quantile(0.999),
            )
        };
        LatencyDigestSummary {
            p50,
            p90,
            p99,
            p99_9,
            matched: counts.matched,
            ep1_faster: counts.ep1_faster,
            ep2_faster: counts.ep2_faster,
        }
    }

    /// Build the §8 `per_program_account_delay` map.
    pub fn per_program_summary(&self) -> PerProgramSummary {
        let mut out: HashMap<String, LatencyDigestSummary> = HashMap::new();
        for mut entry in self.shards.iter_mut() {
            let owner = *entry.key();
            let name = self
                .program_short_names
                .get(&owner)
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());
            let shard = entry.value_mut();
            let summary = shard.digest.summary();
            out.insert(name, summary);
        }
        PerProgramSummary(out)
    }
}

/// Per-program account-delay summaries, keyed by program short name.
/// Wraps a `HashMap` so the output JSON serializes as a plain map
/// (transparent serde).
#[derive(Debug, Clone, Serialize)]
#[serde(transparent)]
pub struct PerProgramSummary(pub HashMap<String, LatencyDigestSummary>);

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(ms: u64) -> EventTimestamp {
        EventTimestamp {
            mono_ns: ms * 1_000_000,
            wall_ms: ms,
        }
    }

    #[test]
    fn account_matcher_pairs_on_full_identity_tuple() {
        let owner_a = [11u8; 32];
        let mut names = HashMap::new();
        names.insert(owner_a, "raydium".to_string());
        let m = AccountMatcher::new(EvictionPolicy::SlotWindow { window: 64 }, names);

        let pk = [42u8; 32];
        let sig: Option<Signature64> = Some([3u8; 64]);
        m.observe(EndpointRole::One, 100, pk, 1, sig, owner_a, ts(0));
        m.observe(EndpointRole::Two, 100, pk, 1, sig, owner_a, ts(5));

        let overall = m.summary();
        assert_eq!(overall.matched, 1);
        assert_eq!(overall.ep1_faster, 1);

        let per = m.per_program_summary();
        let r = per.0.get("raydium").expect("raydium bucket present");
        assert_eq!(r.matched, 1);
        assert!((r.p50 - 5.0).abs() < 0.5);
    }

    #[test]
    fn account_matcher_matches_across_diverging_write_versions() {
        let owner = [11u8; 32];
        let m = AccountMatcher::new(EvictionPolicy::SlotWindow { window: 64 }, HashMap::new());
        let pk = [42u8; 32];
        let sig: Option<Signature64> = Some([7u8; 64]);
        m.observe(EndpointRole::One, 100, pk, 1_698_798_675_296, sig, owner, ts(0));
        m.observe(EndpointRole::Two, 100, pk, 1_591_937_598_192, sig, owner, ts(5));
        let s = m.summary();
        assert_eq!(s.matched, 1);
        assert!((s.p50 - 5.0).abs() < 0.5);
    }

    #[test]
    fn account_matcher_matches_when_txn_signature_absent() {
        let owner = [11u8; 32];
        let m = AccountMatcher::new(EvictionPolicy::SlotWindow { window: 64 }, HashMap::new());
        let pk = [42u8; 32];
        m.observe(EndpointRole::One, 100, pk, 1, None, owner, ts(0));
        m.observe(EndpointRole::Two, 100, pk, 1, None, owner, ts(2));
        let s = m.summary();
        assert_eq!(s.matched, 1);
    }

    #[test]
    fn unknown_owner_falls_into_unknown_bucket() {
        let m = AccountMatcher::new(EvictionPolicy::SlotWindow { window: 64 }, HashMap::new());
        let pk = [42u8; 32];
        m.observe(EndpointRole::One, 100, pk, 1, None, [9u8; 32], ts(0));
        m.observe(EndpointRole::Two, 100, pk, 1, None, [9u8; 32], ts(3));
        let per = m.per_program_summary();
        assert!(per.0.contains_key("unknown"));
    }
}
