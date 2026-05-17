//! Lock-free SPSC ring used to pass [`Event`] records from a receiver
//! task to its matching/ingest thread (spec §7).
//!
//! Built on `crossbeam_channel::bounded(N)`, which preallocates N slots
//! at construction and performs no allocation on send/recv. We restrict
//! ourselves to a single sender / single receiver pair per ring; the
//! channel API allows MPMC but we don't need it and the SPSC discipline
//! is what the spec calls for.
//!
//! Spec §7: "If the ring fills, drop events and increment a
//! `dropped_events` counter — never block the receiver to allocate more."
//! [`EventSender::send`] embodies this: it never blocks, and on overflow
//! it bumps an atomic counter and returns `false`.

use std::sync::{atomic::Ordering, Arc};

use crossbeam_channel::{bounded, Receiver, RecvError, Sender, TryRecvError};

use super::{Event, ReceiverStats};

/// One end of the ring: the receiver task's send-side, plus the shared
/// stats counters that the summary path reads later.
#[derive(Debug, Clone)]
pub struct EventSender {
    inner: Sender<Event>,
    stats: Arc<ReceiverStats>,
}

impl EventSender {
    /// Send an event without blocking.
    ///
    /// Returns `true` on success; `false` when the ring was full or the
    /// receiver has dropped. On overflow this bumps the
    /// [`ReceiverStats::dropped`] counter so the result JSON can report
    /// honest capture counts. The receiver-side disconnect is a fatal
    /// state for that subscription; callers should bail out.
    #[must_use]
    pub fn try_send(&self, event: Event) -> bool {
        // Don't bother counting `received` here — that's the receiver
        // thread's job (it increments before deciding what to do with
        // the event). This sender just handles overflow accounting.
        match self.inner.try_send(event) {
            Ok(()) => true,
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                self.stats.dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => false,
        }
    }

    /// Snapshot accessor for the shared stats. Allows the receiver task
    /// loop to increment `received` / `decode_errors` directly without
    /// going through this struct.
    #[must_use]
    pub fn stats(&self) -> Arc<ReceiverStats> {
        Arc::clone(&self.stats)
    }
}

/// One end of the ring: the ingest thread's receive-side. Drops cleanly
/// when the sender is gone (the receiver task ended).
#[derive(Debug, Clone)]
pub struct EventReceiver {
    inner: Receiver<Event>,
}

impl EventReceiver {
    /// Block until an event arrives.
    ///
    /// # Errors
    /// Returns [`RecvError`] when the sender is dropped (subscription
    /// terminated).
    pub fn recv(&self) -> Result<Event, RecvError> {
        self.inner.recv()
    }

    /// Non-blocking receive, used by the ingest loop's drain step.
    ///
    /// # Errors
    /// - [`TryRecvError::Empty`] if no event is ready.
    /// - [`TryRecvError::Disconnected`] if the sender is gone.
    pub fn try_recv(&self) -> Result<Event, TryRecvError> {
        self.inner.try_recv()
    }

    /// Pending event count. Useful for the periodic snapshot log line so
    /// the operator can see ring pressure.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.inner.len()
    }
}

/// A complete ring: paired sender and receiver halves plus the shared
/// stats counters.
#[derive(Debug)]
pub struct Ring {
    /// Sender half — clone into the receiver task.
    pub sender: EventSender,
    /// Receiver half — handed to the ingest thread.
    pub receiver: EventReceiver,
    /// Shared stats counters; both halves of the ring point at the same
    /// `Arc<ReceiverStats>` so the summary code reads a single, coherent
    /// snapshot.
    pub stats: Arc<ReceiverStats>,
}

impl Ring {
    /// Construct a ring of the given capacity.
    ///
    /// Capacity must be > 0; capacity 0 is the rendezvous channel which
    /// would block the receiver (defeating the "never block" rule from
    /// spec §7). The function panics on capacity 0 as a startup-time
    /// invariant; capacity selection lives at a higher level and is not
    /// runtime data.
    ///
    /// # Panics
    /// Panics if `capacity == 0`. This is a programmer error caught at
    /// startup, not user input.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "ring capacity must be > 0 (spec §7)");
        let (tx, rx) = bounded::<Event>(capacity);
        let stats = ReceiverStats::new();
        Self {
            sender: EventSender {
                inner: tx,
                stats: Arc::clone(&stats),
            },
            receiver: EventReceiver { inner: rx },
            stats,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Commitment,
        subscribe::{EndpointRole, MainStream, SubscriptionRole},
        timing::EventTimestamp,
    };
    use crate::collect::{EventPayload, SlotStage};

    fn ev(slot: u64) -> Event {
        Event {
            ts: EventTimestamp {
                mono_ns: slot * 1_000_000,
                wall_ms: slot,
            },
            subscription: SubscriptionRole::Main {
                endpoint: EndpointRole::One,
                commitment: Commitment::Processed,
                stream: MainStream::Slots,
            },
            payload: EventPayload::Slot {
                slot,
                stage: SlotStage::Processed,
            },
        }
    }

    #[test]
    fn ring_round_trips_events() {
        let ring = Ring::with_capacity(4);
        assert!(ring.sender.try_send(ev(1)));
        assert!(ring.sender.try_send(ev(2)));
        let got = ring.receiver.recv().expect("recv");
        match got.payload {
            EventPayload::Slot { slot: 1, .. } => {}
            other => panic!("unexpected first payload: {other:?}"),
        }
    }

    #[test]
    fn ring_overflow_increments_dropped_counter() {
        let ring = Ring::with_capacity(2);
        assert!(ring.sender.try_send(ev(1)));
        assert!(ring.sender.try_send(ev(2)));
        // Third send must drop.
        assert!(!ring.sender.try_send(ev(3)));
        let snap = ring.stats.snapshot();
        assert_eq!(snap.dropped, 1);
    }

    #[test]
    fn ring_try_recv_returns_empty_when_idle() {
        let ring = Ring::with_capacity(2);
        assert!(matches!(
            ring.receiver.try_recv(),
            Err(crossbeam_channel::TryRecvError::Empty)
        ));
    }

    #[test]
    #[should_panic(expected = "ring capacity must be > 0")]
    fn ring_panics_on_zero_capacity() {
        let _ = Ring::with_capacity(0);
    }
}
