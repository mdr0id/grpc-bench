//! Within-endpoint cross-stream ordering (spec §6.3).
//!
//! Per-endpoint metrics (one tracker per endpoint):
//!
//! - **`tx_vs_account`**: for each transaction observed on the tx stream,
//!   find the corresponding account update(s) (same `(slot, signature)`)
//!   on the account stream. Record `account_arrival - tx_arrival`.
//!   Negative means the account write arrived before the producing
//!   transaction notification (rare, but possible).
//! - **`entries_vs_tx`** and **`entries_vs_account`**: per spec these are
//!   "find by `signature`" pairings. Standard Yellowstone
//!   `SubscribeUpdateEntry` does **not** carry transaction signatures
//!   (only `starting_transaction_index` + `executed_transaction_count`).
//!   The Phase 1 design (PROTO.md) is to derive the mapping by index:
//!   tx events have `index`, entries cover a contiguous tx index range,
//!   account updates have `txn_signature` which we can resolve to a tx
//!   index. That join is non-trivial and is implemented as a follow-up;
//!   for v1 the harness emits `null` with a stable warning so the
//!   `summarize.py` output is honest.

use serde::Serialize;
use tdigest::TDigest;

use crate::{
    collect::{Event, EventPayload, Signature64},
    matching::TDIGEST_COMPRESSION,
    subscribe::EndpointRole,
    timing::EventTimestamp,
};

/// `comparative.cross_stream.<endpoint>` shape.
#[derive(Debug, Clone, Serialize)]
pub struct CrossStreamSummary {
    /// p50/p90/p99 of `(account - tx)` arrival deltas for matched
    /// `(slot, signature)` pairs.
    pub tx_vs_account: Option<Quartiles>,
    /// p50/p90/p99 of `(tx - entries)` arrival deltas. `None` in v1; see
    /// the module-level doc.
    pub entries_vs_tx: Option<Quartiles>,
    /// p50/p90/p99 of `(account - entries)` arrival deltas. `None` in v1.
    pub entries_vs_account: Option<Quartiles>,
    /// Free-text explanation of why entries-related fields are null when
    /// they are. Surfaced verbatim so the operator sees the design note
    /// inline in the result JSON.
    pub notes: Vec<String>,
}

impl CrossStreamSummary {
    /// Empty summary with the standard "no entries" notes attached.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            tx_vs_account: None,
            entries_vs_tx: None,
            entries_vs_account: None,
            notes: vec![
                "entries_vs_tx / entries_vs_account are null in v1: standard \
                 Yellowstone SubscribeUpdateEntry does not include transaction \
                 signatures; index-based correlation is a follow-up. See \
                 PROTO.md."
                    .to_string(),
            ],
        }
    }
}

/// p50/p90/p99 triple in milliseconds. Used for cross-stream metrics
/// where the §8 schema asks for `p50/p90/p99` (no `p99_9` / count).
#[derive(Debug, Clone, Serialize)]
pub struct Quartiles {
    /// p50.
    pub p50: f64,
    /// p90.
    pub p90: f64,
    /// p99.
    pub p99: f64,
}

#[derive(Debug)]
struct StreamingDigest {
    digest: TDigest,
    buffer: Vec<f64>,
}

impl StreamingDigest {
    fn new() -> Self {
        Self {
            digest: TDigest::new_with_size(TDIGEST_COMPRESSION),
            buffer: Vec::new(),
        }
    }
    fn push(&mut self, v: f64) {
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
    fn summary(&mut self) -> Option<Quartiles> {
        self.flush();
        if self.digest.is_empty() {
            return None;
        }
        Some(Quartiles {
            p50: self.quantile(0.50),
            p90: self.quantile(0.90),
            p99: self.quantile(0.99),
        })
    }
}

/// Pending-event pool keyed on `(slot, signature)` so tx-arrivals and
/// account-arrivals can rendezvous regardless of which arrives first.
#[derive(Debug)]
struct TxAccountPairer {
    pending_tx: std::collections::HashMap<(u64, Signature64), EventTimestamp>,
    pending_account: std::collections::HashMap<(u64, Signature64), EventTimestamp>,
    digest: StreamingDigest,
    high_water_slot: u64,
}

impl TxAccountPairer {
    fn new() -> Self {
        Self {
            pending_tx: std::collections::HashMap::new(),
            pending_account: std::collections::HashMap::new(),
            digest: StreamingDigest::new(),
            high_water_slot: 0,
        }
    }

    fn observe_tx(&mut self, slot: u64, signature: Signature64, ts: EventTimestamp) {
        self.bump_water(slot);
        let key = (slot, signature);
        if let Some(acc_ts) = self.pending_account.remove(&key) {
            self.record(acc_ts, ts);
        } else {
            self.pending_tx.insert(key, ts);
        }
        self.evict();
    }

    fn observe_account(&mut self, slot: u64, signature: Signature64, ts: EventTimestamp) {
        self.bump_water(slot);
        let key = (slot, signature);
        if let Some(tx_ts) = self.pending_tx.remove(&key) {
            self.record(ts, tx_ts);
        } else {
            self.pending_account.insert(key, ts);
        }
        self.evict();
    }

    fn record(&mut self, acc_ts: EventTimestamp, tx_ts: EventTimestamp) {
        let delta_ns = i64::try_from(acc_ts.mono_ns).unwrap_or(i64::MAX)
            - i64::try_from(tx_ts.mono_ns).unwrap_or(i64::MAX);
        #[allow(clippy::cast_precision_loss)]
        let delta_in_ms = (delta_ns as f64) / 1_000_000.0;
        self.digest.push(delta_in_ms);
    }

    fn bump_water(&mut self, slot: u64) {
        if slot > self.high_water_slot {
            self.high_water_slot = slot;
        }
    }

    fn evict(&mut self) {
        let cutoff = self.high_water_slot.saturating_sub(64);
        self.pending_tx.retain(|(s, _), _| *s >= cutoff);
        self.pending_account.retain(|(s, _), _| *s >= cutoff);
    }
}

/// Per-endpoint cross-stream tracker.
#[derive(Debug)]
pub struct CrossStreamTracker {
    endpoint: EndpointRole,
    tx_vs_account: TxAccountPairer,
}

impl CrossStreamTracker {
    /// Fresh tracker for a single endpoint.
    #[must_use]
    pub fn new(endpoint: EndpointRole) -> Self {
        Self {
            endpoint,
            tx_vs_account: TxAccountPairer::new(),
        }
    }

    /// Which endpoint this tracker belongs to.
    #[must_use]
    pub fn endpoint(&self) -> EndpointRole {
        self.endpoint
    }

    /// Observe an event from this endpoint.
    pub fn observe(&mut self, event: &Event) {
        if event.subscription.endpoint() != self.endpoint {
            return;
        }
        match &event.payload {
            EventPayload::Transaction { slot, signature, .. } => {
                self.tx_vs_account.observe_tx(*slot, *signature, event.ts);
            }
            EventPayload::Account {
                slot,
                txn_signature: Some(sig),
                ..
            } => {
                self.tx_vs_account.observe_account(*slot, *sig, event.ts);
            }
            _ => {}
        }
    }

    /// Build the §8 cross-stream summary.
    pub fn summary(&mut self) -> CrossStreamSummary {
        CrossStreamSummary {
            tx_vs_account: self.tx_vs_account.digest.summary(),
            entries_vs_tx: None,
            entries_vs_account: None,
            notes: CrossStreamSummary::empty().notes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        collect::{Event, EventPayload},
        config::Commitment,
        subscribe::{MainStream, SubscriptionRole},
    };

    fn ts(ms: u64) -> EventTimestamp {
        EventTimestamp {
            mono_ns: ms * 1_000_000,
            wall_ms: ms,
        }
    }

    fn tx_event(slot: u64, sig: Signature64, mono_ms: u64) -> Event {
        Event {
            ts: ts(mono_ms),
            subscription: SubscriptionRole::Main {
                endpoint: EndpointRole::One,
                commitment: Commitment::Processed,
                stream: MainStream::Transactions,
            },
            payload: EventPayload::Transaction {
                slot,
                signature: sig,
                index: 0,
            },
        }
    }

    fn account_event(slot: u64, sig: Option<Signature64>, mono_ms: u64) -> Event {
        Event {
            ts: ts(mono_ms),
            subscription: SubscriptionRole::Main {
                endpoint: EndpointRole::One,
                commitment: Commitment::Processed,
                stream: MainStream::Accounts,
            },
            payload: EventPayload::Account {
                slot,
                pubkey: [0u8; 32],
                owner: [0u8; 32],
                write_version: 0,
                txn_signature: sig,
                lamports: 0,
            },
        }
    }

    #[test]
    fn tx_arrives_before_account_yields_positive_delta() {
        let mut t = CrossStreamTracker::new(EndpointRole::One);
        let sig = [1u8; 64];
        t.observe(&tx_event(10, sig, 0));
        t.observe(&account_event(10, Some(sig), 5));
        let s = t.summary();
        let q = s.tx_vs_account.unwrap();
        assert!((q.p50 - 5.0).abs() < 1.0);
    }

    #[test]
    fn account_arrives_before_tx_yields_negative_delta() {
        let mut t = CrossStreamTracker::new(EndpointRole::One);
        let sig = [1u8; 64];
        t.observe(&account_event(10, Some(sig), 0));
        t.observe(&tx_event(10, sig, 4));
        let s = t.summary();
        let q = s.tx_vs_account.unwrap();
        assert!((q.p50 - (-4.0)).abs() < 1.0);
    }

    #[test]
    fn entries_metrics_remain_null_in_v1() {
        let mut t = CrossStreamTracker::new(EndpointRole::One);
        let s = t.summary();
        assert!(s.entries_vs_tx.is_none());
        assert!(s.entries_vs_account.is_none());
        assert!(!s.notes.is_empty());
    }

    #[test]
    fn ignores_other_endpoint() {
        let mut t = CrossStreamTracker::new(EndpointRole::One);
        let sig = [1u8; 64];
        // tx event from endpoint2
        let mut ev = tx_event(10, sig, 0);
        ev.subscription = SubscriptionRole::Main {
            endpoint: EndpointRole::Two,
            commitment: Commitment::Processed,
            stream: MainStream::Transactions,
        };
        t.observe(&ev);
        t.observe(&account_event(10, Some(sig), 5));
        let s = t.summary();
        assert!(s.tx_vs_account.is_none(), "should not pair cross-endpoint");
    }
}
