//! Transaction-stream matching ().
//!
//! Identity tuple: `(slot, signature)`. The transactions filter is keyed
//! on program owners so the universe of observed transactions on both
//! endpoints is identical when the filter mirrors are configured the same
//! way (the harness builds them that way; see §4).

use crate::{collect::Signature64, subscribe::EndpointRole, timing::EventTimestamp};

use super::{EvictionPolicy, LatencyDigestSummary, PairMatcher};

/// Compound identity key for transactions.
pub type TxKey = (u64, Signature64);

/// Wraps a [`PairMatcher`] keyed on `(slot, signature)`.
#[derive(Debug)]
pub struct TransactionMatcher {
    inner: PairMatcher<TxKey>,
}

impl TransactionMatcher {
    /// Construct a transaction matcher.
    #[must_use]
    pub fn new(eviction: EvictionPolicy) -> Self {
        Self {
            inner: PairMatcher::new(eviction),
        }
    }

    /// Observe a transaction arrival.
    pub fn observe(
        &mut self,
        endpoint: EndpointRole,
        slot: u64,
        signature: Signature64,
        ts: EventTimestamp,
    ) {
        let _ = self.inner.observe(endpoint, (slot, signature), ts, slot);
    }

    /// Build the §8 `comparative.transaction_delay` block.
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
    fn transaction_matcher_pairs_by_slot_and_signature() {
        let mut m = TransactionMatcher::new(EvictionPolicy::SlotWindow { window: 64 });
        let sig = [7u8; 64];
        m.observe(EndpointRole::One, 10, sig, ts(0));
        m.observe(EndpointRole::Two, 10, sig, ts(2));
        let s = m.summary();
        assert_eq!(s.matched, 1);
        assert_eq!(s.ep1_faster, 1);
    }

    #[test]
    fn transaction_matcher_does_not_match_different_slot_same_signature() {
        let mut m = TransactionMatcher::new(EvictionPolicy::SlotWindow { window: 64 });
        let sig = [7u8; 64];
        m.observe(EndpointRole::One, 10, sig, ts(0));
        m.observe(EndpointRole::Two, 11, sig, ts(2));
        let s = m.summary();
        assert_eq!(s.matched, 0);
    }
}
