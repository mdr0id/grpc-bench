//! Block-stream matching (spec §6.1).
//!
//! Identity tuple: `(slot, blockhash)`. We additionally track `tx_count`
//! and `block_size_bytes` per matched slot so downstream analysis can
//! correlate latency to load — but those fields live on the [`Event`]
//! payload itself, not in the matcher state. The matcher reports the
//! latency digest; the raw record stream carries the per-block
//! metadata.

use crate::{collect::Pubkey32, config::Commitment, subscribe::EndpointRole, timing::EventTimestamp};

use super::{EvictionPolicy, LatencyDigestSummary, PairMatcher};

/// Compound identity key for blocks.
///
/// Commitment is part of the key so that a `(slot, blockhash)` emitted at
/// processed time on one endpoint is never paired with the same
/// `(slot, blockhash)` emitted at confirmed time on the other — those are
/// physically the same block but separated by the commitment-progression
/// lag (hundreds of ms to seconds), which would dominate the measured
/// wire delta and turn the metric into noise.
pub type BlockKey = (Commitment, u64, Pubkey32);

/// Wraps a [`PairMatcher`] keyed on `(commitment, slot, blockhash)`.
#[derive(Debug)]
pub struct BlockMatcher {
    inner: PairMatcher<BlockKey>,
}

impl BlockMatcher {
    /// Construct a block matcher.
    #[must_use]
    pub fn new(eviction: EvictionPolicy) -> Self {
        Self {
            inner: PairMatcher::new(eviction),
        }
    }

    /// Observe a block arrival.
    pub fn observe(
        &mut self,
        endpoint: EndpointRole,
        commitment: Commitment,
        slot: u64,
        blockhash: Pubkey32,
        ts: EventTimestamp,
    ) {
        let _ = self
            .inner
            .observe(endpoint, (commitment, slot, blockhash), ts, slot);
    }

    /// Build the §8 `comparative.block_delay` block.
    pub fn summary(&mut self) -> LatencyDigestSummary {
        LatencyDigestSummary::from_matcher(&mut self.inner)
    }
}

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
    fn block_matcher_pairs_on_commitment_slot_and_blockhash() {
        let mut m = BlockMatcher::new(EvictionPolicy::SlotWindow { window: 64 });
        let bh = [3u8; 32];
        m.observe(EndpointRole::One, Commitment::Processed, 100, bh, ts(0));
        m.observe(EndpointRole::Two, Commitment::Processed, 100, bh, ts(4));
        let s = m.summary();
        assert_eq!(s.matched, 1);
        assert_eq!(s.ep1_faster, 1);
    }

    #[test]
    fn block_matcher_does_not_cross_pair_across_commitments() {
        // The same physical block (slot, blockhash) is emitted twice per
        // endpoint when --commitment processed,confirmed is requested:
        // once at processed time, once at confirmed time. If commitment
        // were absent from the key, ep1's processed emission would pair
        // with ep2's confirmed emission (or vice versa), producing a
        // delta that is dominated by the commitment-progression lag
        // rather than the wire delta.
        let mut m = BlockMatcher::new(EvictionPolicy::SlotWindow { window: 64 });
        let bh = [3u8; 32];
        // ep1 processed at t=0; ep1 confirmed at t=400 (no cross-endpoint
        // match yet, both pending under different keys).
        m.observe(EndpointRole::One, Commitment::Processed, 100, bh, ts(0));
        m.observe(EndpointRole::One, Commitment::Confirmed, 100, bh, ts(400));
        // ep2 processed at t=10 → should pair with ep1 processed (delta ~10 ms).
        m.observe(EndpointRole::Two, Commitment::Processed, 100, bh, ts(10));
        // ep2 confirmed at t=410 → should pair with ep1 confirmed (delta ~10 ms).
        m.observe(EndpointRole::Two, Commitment::Confirmed, 100, bh, ts(410));
        let s = m.summary();
        assert_eq!(s.matched, 2);
        // Both matches show ep1 faster by ~10 ms; commitment lag (400 ms)
        // never appears because cross-commitment pairing is blocked by
        // the key.
        assert_eq!(s.ep1_faster, 2);
        assert!(s.p50 < 50.0, "p50 should be ~10 ms, got {}", s.p50);
    }
}
