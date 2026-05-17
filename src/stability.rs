//! Stream-stability metrics per spec §6.4.
//!
//! Per endpoint:
//! - Inter-message gap distribution on the slot-status stream.
//! - Stall events: any gap > 600 ms is recorded as a stall.
//! - Processed→Confirmed drift: when both commitment subscriptions are
//!   active, `arrival(confirmed_for_slot_S) - arrival(processed_for_slot_S)`.
//! - Disconnect events: collected by the receiver task on stream end.
//! - Reconnect time-to-first-message: time between successful reconnect
//!   and the first message on the new stream.
//!
//! Inputs come from the same [`Event`] stream the matcher consumes plus
//! explicit notifications from the receiver task for disconnect /
//! reconnect events.

use std::collections::HashMap;

use serde::Serialize;
use tdigest::TDigest;

use crate::{
    collect::{Event, EventPayload, SlotStage},
    config::Commitment,
    matching::TDIGEST_COMPRESSION,
    subscribe::{EndpointRole, SubscriptionRole},
    timing::EventTimestamp,
};

/// A slot-gap above this many milliseconds is recorded as a stall event
/// (spec §6.4: "anything above 600 ms (a missed slot) is a stall").
pub const STALL_THRESHOLD_MS: f64 = 600.0;

/// `comparative.stability.<endpoint>.stall_events[]` entry.
#[derive(Debug, Clone, Serialize)]
pub struct StallEvent {
    /// Wall-clock millisecond when the stall ended (i.e., the late
    /// message's arrival).
    pub wall_ms: u64,
    /// Duration of the gap in milliseconds.
    pub duration_ms: f64,
}

/// `comparative.stability.<endpoint>.disconnects[]` entry.
#[derive(Debug, Clone, Serialize)]
pub struct DisconnectEvent {
    /// Wall-clock millisecond when the disconnect was observed.
    pub wall_ms: u64,
    /// gRPC status text or transport error description.
    pub status: String,
    /// Events received on the affected stream before the disconnect
    /// (cumulative since process start).
    pub events_received_before: u64,
}

/// Streaming t-digest distribution + max tracker.
#[derive(Debug)]
struct DistributionDigest {
    digest: TDigest,
    buffer: Vec<f64>,
    max: f64,
}

impl DistributionDigest {
    fn new() -> Self {
        Self {
            digest: TDigest::new_with_size(TDIGEST_COMPRESSION),
            buffer: Vec::new(),
            max: 0.0,
        }
    }

    fn push(&mut self, v: f64) {
        if v > self.max {
            self.max = v;
        }
        self.buffer.push(v);
        if self.buffer.len() >= crate::matching::TDIGEST_BUFFER_FLUSH {
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
}

/// `comparative.stability.<endpoint>.slot_gap_ms` shape.
#[derive(Debug, Clone, Serialize)]
pub struct SlotGapSummary {
    /// p50.
    pub p50: f64,
    /// p90.
    pub p90: f64,
    /// p99.
    pub p99: f64,
    /// Max observed gap.
    pub max: f64,
}

/// `comparative.stability.<endpoint>.processed_confirmed_drift_ms` shape.
#[derive(Debug, Clone, Serialize)]
pub struct DriftSummary {
    /// p50.
    pub p50: f64,
    /// p99.
    pub p99: f64,
}

/// `comparative.stability.<endpoint>.reconnect_ttfm_ms` shape.
#[derive(Debug, Clone, Serialize)]
pub struct ReconnectTtfmSummary {
    /// p50.
    pub p50: f64,
    /// p99.
    pub p99: f64,
    /// Max observed reconnect TTFM.
    pub max: f64,
}

/// `comparative.stability.<endpoint>` shape.
#[derive(Debug, Clone, Serialize)]
pub struct StabilitySummary {
    /// `slot_gap_ms` digest.
    pub slot_gap_ms: SlotGapSummary,
    /// Stall events (gap > 600ms).
    pub stall_events: Vec<StallEvent>,
    /// Processed→confirmed drift digest.
    pub processed_confirmed_drift_ms: DriftSummary,
    /// Disconnect events.
    pub disconnects: Vec<DisconnectEvent>,
    /// Reconnect TTFM digest.
    pub reconnect_ttfm_ms: ReconnectTtfmSummary,
}

impl StabilitySummary {
    /// Empty summary, used as the value when an endpoint had no
    /// observations.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            slot_gap_ms: SlotGapSummary {
                p50: f64::NAN,
                p90: f64::NAN,
                p99: f64::NAN,
                max: 0.0,
            },
            stall_events: Vec::new(),
            processed_confirmed_drift_ms: DriftSummary {
                p50: f64::NAN,
                p99: f64::NAN,
            },
            disconnects: Vec::new(),
            reconnect_ttfm_ms: ReconnectTtfmSummary {
                p50: f64::NAN,
                p99: f64::NAN,
                max: 0.0,
            },
        }
    }
}

/// Per-endpoint stability tracker. Owned by the ingest layer; updated on
/// each event the receiver delivers and on explicit disconnect /
/// reconnect notifications.
#[derive(Debug)]
pub struct StabilityTracker {
    endpoint: EndpointRole,
    last_slot_event_ts: Option<EventTimestamp>,
    slot_gaps: DistributionDigest,
    stall_events: Vec<StallEvent>,
    /// Map slot -> (first arrival per commitment) so we can compute drift.
    processed_arrivals: HashMap<u64, EventTimestamp>,
    confirmed_arrivals: HashMap<u64, EventTimestamp>,
    drift_digest: DistributionDigest,
    disconnects: Vec<DisconnectEvent>,
    reconnect_ttfm: DistributionDigest,
    /// Set on `record_reconnect`; consumed on the next `observe(...)` so
    /// we can compute TTFM as `first_msg_arrival - reconnect_ts`.
    pending_reconnect_mono_ns: Option<u64>,
}

impl StabilityTracker {
    /// Construct a tracker for a single endpoint.
    #[must_use]
    pub fn new(endpoint: EndpointRole) -> Self {
        Self {
            endpoint,
            last_slot_event_ts: None,
            slot_gaps: DistributionDigest::new(),
            stall_events: Vec::new(),
            processed_arrivals: HashMap::new(),
            confirmed_arrivals: HashMap::new(),
            drift_digest: DistributionDigest::new(),
            disconnects: Vec::new(),
            reconnect_ttfm: DistributionDigest::new(),
            pending_reconnect_mono_ns: None,
        }
    }

    /// Which endpoint this tracker belongs to.
    #[must_use]
    pub fn endpoint(&self) -> EndpointRole {
        self.endpoint
    }

    /// Observe an event. Idempotent across non-slot events.
    pub fn observe(&mut self, event: &Event) {
        if event.subscription.endpoint() != self.endpoint {
            return;
        }
        // Resolve TTFM if a reconnect was just observed.
        if let Some(ts_at_reconnect_ns) = self.pending_reconnect_mono_ns.take() {
            let ttfm_ns =
                i64::try_from(event.ts.mono_ns).unwrap_or(i64::MAX)
                    - i64::try_from(ts_at_reconnect_ns).unwrap_or(i64::MAX);
            #[allow(clippy::cast_precision_loss)]
            let ttfm_in_ms = (ttfm_ns as f64) / 1_000_000.0;
            self.reconnect_ttfm.push(ttfm_in_ms.max(0.0));
        }

        let EventPayload::Slot { slot, stage } = &event.payload else {
            // Drift logic only fires on slot-status events with explicit
            // commitment in `event.subscription`. Other payloads don't
            // affect stability.
            return;
        };

        // Slot gap distribution. Uses the slot stream regardless of
        // stage; the spec describes "consecutive messages on the
        // slot-status stream".
        if let Some(prev) = self.last_slot_event_ts {
            let gap_nanos =
                i64::try_from(event.ts.mono_ns).unwrap_or(i64::MAX)
                    - i64::try_from(prev.mono_ns).unwrap_or(i64::MAX);
            #[allow(clippy::cast_precision_loss)]
            let gap_ms = (gap_nanos as f64).max(0.0) / 1_000_000.0;
            self.slot_gaps.push(gap_ms);
            if gap_ms > STALL_THRESHOLD_MS {
                self.stall_events.push(StallEvent {
                    wall_ms: event.ts.wall_ms,
                    duration_ms: gap_ms,
                });
            }
        }
        self.last_slot_event_ts = Some(event.ts);

        // Processed→Confirmed drift. Only the Processed and Confirmed
        // stages contribute. The commitment for the stream is encoded in
        // `event.subscription`; if the slot also arrives on a Confirmed
        // commitment stream we'll learn that there.
        let commitment = match event.subscription {
            SubscriptionRole::Main { commitment, .. } => Some(commitment),
            SubscriptionRole::Entries { .. } => None,
        };
        match (stage, commitment) {
            (SlotStage::Processed, Some(Commitment::Processed)) => {
                self.processed_arrivals.entry(*slot).or_insert(event.ts);
                if let Some(c) = self.confirmed_arrivals.remove(slot) {
                    self.record_drift(event.ts, c);
                    self.processed_arrivals.remove(slot);
                }
            }
            (SlotStage::Confirmed, Some(Commitment::Confirmed)) => {
                self.confirmed_arrivals.entry(*slot).or_insert(event.ts);
                if let Some(p) = self.processed_arrivals.remove(slot) {
                    self.record_drift(p, event.ts);
                    self.confirmed_arrivals.remove(slot);
                }
            }
            _ => {}
        }
    }

    fn record_drift(&mut self, processed: EventTimestamp, confirmed: EventTimestamp) {
        let drift_ns = i64::try_from(confirmed.mono_ns).unwrap_or(i64::MAX)
            - i64::try_from(processed.mono_ns).unwrap_or(i64::MAX);
        #[allow(clippy::cast_precision_loss)]
        let drift_in_ms = (drift_ns as f64).max(0.0) / 1_000_000.0;
        self.drift_digest.push(drift_in_ms);
    }

    /// Record a disconnect event. `events_received_before` is the
    /// cumulative receiver count at the moment of disconnect.
    pub fn record_disconnect(
        &mut self,
        wall_ms: u64,
        status: String,
        events_received_before: u64,
    ) {
        self.disconnects.push(DisconnectEvent {
            wall_ms,
            status,
            events_received_before,
        });
    }

    /// Record that a reconnect just succeeded; TTFM will be computed on
    /// the next [`Self::observe`] call.
    pub fn record_reconnect(&mut self, mono_ns_at_reconnect: u64) {
        self.pending_reconnect_mono_ns = Some(mono_ns_at_reconnect);
    }

    /// Build the §8 stability summary block.
    pub fn summary(&mut self) -> StabilitySummary {
        StabilitySummary {
            slot_gap_ms: SlotGapSummary {
                p50: self.slot_gaps.quantile(0.50),
                p90: self.slot_gaps.quantile(0.90),
                p99: self.slot_gaps.quantile(0.99),
                max: self.slot_gaps.max,
            },
            stall_events: self.stall_events.clone(),
            processed_confirmed_drift_ms: DriftSummary {
                p50: self.drift_digest.quantile(0.50),
                p99: self.drift_digest.quantile(0.99),
            },
            disconnects: self.disconnects.clone(),
            reconnect_ttfm_ms: ReconnectTtfmSummary {
                p50: self.reconnect_ttfm.quantile(0.50),
                p99: self.reconnect_ttfm.quantile(0.99),
                max: self.reconnect_ttfm.max,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        collect::{Event, EventPayload, SlotStage},
        config::Commitment,
        subscribe::{MainStream, SubscriptionRole},
    };

    fn slot_event(endpoint: EndpointRole, slot: u64, stage: SlotStage, mono_ns: u64, wall_ms: u64) -> Event {
        Event {
            ts: EventTimestamp { mono_ns, wall_ms },
            subscription: SubscriptionRole::Main {
                endpoint,
                commitment: Commitment::Processed,
                stream: MainStream::Slots,
            },
            payload: EventPayload::Slot { slot, stage },
        }
    }

    fn slot_event_with_commitment(
        endpoint: EndpointRole,
        slot: u64,
        stage: SlotStage,
        commitment: Commitment,
        mono_ns: u64,
        wall_ms: u64,
    ) -> Event {
        Event {
            ts: EventTimestamp { mono_ns, wall_ms },
            subscription: SubscriptionRole::Main {
                endpoint,
                commitment,
                stream: MainStream::Slots,
            },
            payload: EventPayload::Slot { slot, stage },
        }
    }

    #[test]
    fn slot_gap_records_gap_between_consecutive_events() {
        let mut t = StabilityTracker::new(EndpointRole::One);
        t.observe(&slot_event(EndpointRole::One, 1, SlotStage::Processed, 0, 0));
        // 50 ms later
        t.observe(&slot_event(
            EndpointRole::One,
            2,
            SlotStage::Processed,
            50_000_000,
            50,
        ));
        let s = t.summary();
        assert!((s.slot_gap_ms.max - 50.0).abs() < 1e-6);
    }

    #[test]
    fn stall_event_recorded_above_threshold() {
        let mut t = StabilityTracker::new(EndpointRole::One);
        t.observe(&slot_event(EndpointRole::One, 1, SlotStage::Processed, 0, 0));
        // 700 ms gap → stall
        t.observe(&slot_event(
            EndpointRole::One,
            2,
            SlotStage::Processed,
            700_000_000,
            700,
        ));
        let s = t.summary();
        assert_eq!(s.stall_events.len(), 1);
        assert!((s.stall_events[0].duration_ms - 700.0).abs() < 1e-6);
    }

    #[test]
    fn ignores_events_from_other_endpoint() {
        let mut t = StabilityTracker::new(EndpointRole::One);
        t.observe(&slot_event(EndpointRole::Two, 1, SlotStage::Processed, 0, 0));
        let s = t.summary();
        assert!(s.slot_gap_ms.max.abs() < f64::EPSILON);
    }

    #[test]
    fn processed_confirmed_drift_paired_via_slot() {
        let mut t = StabilityTracker::new(EndpointRole::One);
        // Processed for slot 100 at t=0
        t.observe(&slot_event_with_commitment(
            EndpointRole::One,
            100,
            SlotStage::Processed,
            Commitment::Processed,
            0,
            0,
        ));
        // Confirmed for slot 100 at t=300ms
        t.observe(&slot_event_with_commitment(
            EndpointRole::One,
            100,
            SlotStage::Confirmed,
            Commitment::Confirmed,
            300_000_000,
            300,
        ));
        let s = t.summary();
        // p50 should be ~300ms.
        assert!(
            (s.processed_confirmed_drift_ms.p50 - 300.0).abs() < 1.0,
            "drift p50 was {}",
            s.processed_confirmed_drift_ms.p50
        );
    }

    #[test]
    fn disconnect_event_round_trips_into_summary() {
        let mut t = StabilityTracker::new(EndpointRole::One);
        t.record_disconnect(123_456, "Unavailable: connection reset".into(), 42);
        let s = t.summary();
        assert_eq!(s.disconnects.len(), 1);
        assert_eq!(s.disconnects[0].events_received_before, 42);
    }

    #[test]
    fn reconnect_ttfm_recorded_on_first_message_after_reconnect() {
        let mut t = StabilityTracker::new(EndpointRole::One);
        t.record_reconnect(1_000_000_000); // mono_ns at reconnect
        // First message arrives 50ms later.
        t.observe(&slot_event(
            EndpointRole::One,
            10,
            SlotStage::Processed,
            1_050_000_000,
            1_050,
        ));
        let s = t.summary();
        assert!(
            (s.reconnect_ttfm_ms.max - 50.0).abs() < 1.0,
            "ttfm max was {}",
            s.reconnect_ttfm_ms.max
        );
    }
}
