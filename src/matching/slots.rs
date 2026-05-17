//! Slot-status matching across endpoints.
//!
//! Identity tuple per : `(slot, stage)`. The 6-stage breakdown
//! `(FirstShredReceived, Completed, CreatedBank, Processed, Confirmed,
//! Finalized)` is reported as one t-digest per stage in the output JSON
//! (`comparative.slot_status.<stage>_delay`).
//!
//! `Dead` is observed but not matched into a per-stage digest — a slot
//! marked dead by one endpoint should not skew the latency distribution
//! for healthy slots. It's preserved in the raw record stream so the
//! operator can correlate.

use serde::Serialize;

use crate::{collect::SlotStage, subscribe::EndpointRole, timing::EventTimestamp};

use super::{EvictionPolicy, LatencyDigestSummary, PairMatcher};

/// One matcher per stage. Keys on `slot` because `stage` is the stage
/// indexed into the array.
#[derive(Debug)]
pub struct SlotMatcher {
    first_shred_received: PairMatcher<u64>,
    completed: PairMatcher<u64>,
    created_bank: PairMatcher<u64>,
    processed: PairMatcher<u64>,
    confirmed: PairMatcher<u64>,
    finalized: PairMatcher<u64>,
}

impl SlotMatcher {
    /// Construct fresh matchers for all six stages.
    #[must_use]
    pub fn new(eviction: EvictionPolicy) -> Self {
        Self {
            first_shred_received: PairMatcher::new(eviction),
            completed: PairMatcher::new(eviction),
            created_bank: PairMatcher::new(eviction),
            processed: PairMatcher::new(eviction),
            confirmed: PairMatcher::new(eviction),
            finalized: PairMatcher::new(eviction),
        }
    }

    /// Observe a slot-status event for the given stage.
    pub fn observe_stage(
        &mut self,
        endpoint: EndpointRole,
        slot: u64,
        stage: SlotStage,
        ts: EventTimestamp,
    ) {
        let Some(m) = self.matcher_for(stage) else {
            return; // Dead and other unsupported stages — drop silently.
        };
        let _ = m.observe(endpoint, slot, ts, slot);
    }

    fn matcher_for(&mut self, stage: SlotStage) -> Option<&mut PairMatcher<u64>> {
        Some(match stage {
            SlotStage::FirstShredReceived => &mut self.first_shred_received,
            SlotStage::Completed => &mut self.completed,
            SlotStage::CreatedBank => &mut self.created_bank,
            SlotStage::Processed => &mut self.processed,
            SlotStage::Confirmed => &mut self.confirmed,
            SlotStage::Finalized => &mut self.finalized,
            SlotStage::Dead => return None,
        })
    }

    /// Build the §8 `comparative.slot_status` block.
    pub fn summary(&mut self) -> SlotStatusSummary {
        SlotStatusSummary {
            first_shred_delay: LatencyDigestSummary::from_matcher(&mut self.first_shred_received),
            completed_delay: LatencyDigestSummary::from_matcher(&mut self.completed),
            created_bank_delay: LatencyDigestSummary::from_matcher(&mut self.created_bank),
            processed_delay: LatencyDigestSummary::from_matcher(&mut self.processed),
            confirmed_delay: LatencyDigestSummary::from_matcher(&mut self.confirmed),
            finalized_delay: LatencyDigestSummary::from_matcher(&mut self.finalized),
        }
    }
}

/// Output-JSON shape for the slot-status section.
#[derive(Debug, Clone, Serialize)]
#[allow(clippy::struct_field_names)] // every field is a *_delay on purpose.
pub struct SlotStatusSummary {
    /// `comparative.slot_status.first_shred_delay`.
    pub first_shred_delay: LatencyDigestSummary,
    /// `comparative.slot_status.completed_delay`.
    pub completed_delay: LatencyDigestSummary,
    /// `comparative.slot_status.created_bank_delay`.
    pub created_bank_delay: LatencyDigestSummary,
    /// `comparative.slot_status.processed_delay`.
    pub processed_delay: LatencyDigestSummary,
    /// `comparative.slot_status.confirmed_delay`.
    pub confirmed_delay: LatencyDigestSummary,
    /// `comparative.slot_status.finalized_delay`.
    pub finalized_delay: LatencyDigestSummary,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_matcher_separates_stages() {
        let mut m = SlotMatcher::new(EvictionPolicy::SlotWindow { window: 64 });
        let t1 = EventTimestamp {
            mono_ns: 0,
            wall_ms: 0,
        };
        let t2 = EventTimestamp {
            mono_ns: 5_000_000,
            wall_ms: 0,
        };
        m.observe_stage(EndpointRole::One, 100, SlotStage::Processed, t1);
        m.observe_stage(EndpointRole::Two, 100, SlotStage::Processed, t2);
        // A confirmed event for the same slot should NOT match the
        // processed pair — separate matchers.
        m.observe_stage(EndpointRole::One, 100, SlotStage::Confirmed, t1);
        let s = m.summary();
        assert_eq!(s.processed_delay.matched, 1);
        assert_eq!(s.confirmed_delay.matched, 0);
    }

    #[test]
    fn slot_matcher_ignores_dead_stage() {
        let mut m = SlotMatcher::new(EvictionPolicy::SlotWindow { window: 64 });
        let t = EventTimestamp {
            mono_ns: 1,
            wall_ms: 1,
        };
        m.observe_stage(EndpointRole::One, 1, SlotStage::Dead, t);
        m.observe_stage(EndpointRole::Two, 1, SlotStage::Dead, t);
        // No matcher should have recorded anything.
        let s = m.summary();
        assert_eq!(s.processed_delay.matched, 0);
        assert_eq!(s.confirmed_delay.matched, 0);
    }
}
