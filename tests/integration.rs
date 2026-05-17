//! End-to-end pipeline tests that don't require a live gRPC endpoint.
//!
//! These exercise the matcher → summary path with synthetic [`Event`]
//! records that simulate what the gRPC receiver would emit. The two
//! scenarios are the Phase 1 deterministic checks:
//!
//! 1. Endpoint2 is consistently 5ms slower than endpoint1 → the
//!    comparative summary's `account_delay.p50` is within ±1ms of 5ms.
//! 2. Forced disconnect mid-run → the stability tracker records a
//!    disconnect event and computes reconnect TTFM correctly.
//!
//! gRPC-transport-level integration (a tonic mock) is out of scope for
//! v1 per the Phase 1 resolution: live runs are the real validation
//! path.

use std::collections::HashMap;

use grpc_bench::{
    collect::{Event, EventPayload, Pubkey32, SlotStage},
    config::Commitment,
    matching::{dispatch, StreamMatchers},
    stability::StabilityTracker,
    subscribe::{EndpointRole, MainStream, SubscriptionRole},
    timing::EventTimestamp,
};

fn ts(ms: u64) -> EventTimestamp {
    EventTimestamp {
        mono_ns: ms.saturating_mul(1_000_000),
        wall_ms: ms,
    }
}

fn account_event(
    endpoint: EndpointRole,
    slot: u64,
    pubkey_byte: u8,
    write_version: u64,
    sig_byte: u8,
    owner: Pubkey32,
    arrival_ms: u64,
) -> Event {
    let mut pk = [0u8; 32];
    pk[0] = pubkey_byte;
    let mut sig = [0u8; 64];
    sig[0] = sig_byte;
    Event {
        ts: ts(arrival_ms),
        subscription: SubscriptionRole::Main {
            endpoint,
            commitment: Commitment::Processed,
            stream: MainStream::Accounts,
        },
        payload: EventPayload::Account {
            slot,
            pubkey: pk,
            owner,
            write_version,
            txn_signature: Some(sig),
            lamports: 1000,
        },
    }
}

#[test]
fn endpoint2_five_ms_slower_yields_p50_around_five_ms() {
    // Build the matcher with a program-name map so the per-program path
    // exercises too.
    let mut owner = [0u8; 32];
    owner[0] = 11;
    let mut names: HashMap<Pubkey32, String> = HashMap::new();
    names.insert(owner, "raydium".to_string());
    let matchers = StreamMatchers::new(names);

    // 2000 matched account pairs, ep2 arrives 5ms after ep1 every time.
    for i in 0..2000_u64 {
        // Unique identity per pair so each pairs cleanly.
        let pubkey_byte = u8::try_from(i % 200).unwrap_or(0).saturating_add(1);
        let sig_byte = u8::try_from((i >> 8) & 0xFF).unwrap_or(0);
        let ep1_arrival = i * 10; // arbitrary spacing in ms
        let ep2_arrival = ep1_arrival + 5; // ep2 is 5ms slower
        let e1 = account_event(
            EndpointRole::One,
            100 + i,
            pubkey_byte,
            i,
            sig_byte,
            owner,
            ep1_arrival,
        );
        let e2 = account_event(
            EndpointRole::Two,
            100 + i,
            pubkey_byte,
            i,
            sig_byte,
            owner,
            ep2_arrival,
        );
        dispatch(&matchers, &e1);
        dispatch(&matchers, &e2);
    }

    let summary = matchers.accounts.summary();
    assert_eq!(
        summary.matched, 2000,
        "expected 2000 matched pairs, got {}",
        summary.matched
    );
    assert!(
        (summary.p50 - 5.0).abs() < 1.0,
        "p50 was {:.4} ms (want ~5)",
        summary.p50
    );
    assert!(
        (summary.p99 - 5.0).abs() < 1.0,
        "p99 was {:.4} ms (want ~5; the input is constant so the digest should be tight)",
        summary.p99
    );
    assert_eq!(summary.ep1_faster, 2000);
    assert_eq!(summary.ep2_faster, 0);

    let per_program = matchers.accounts.per_program_summary();
    let raydium = per_program
        .0
        .get("raydium")
        .expect("raydium bucket populated");
    assert_eq!(raydium.matched, 2000);
    assert!(
        (raydium.p50 - 5.0).abs() < 1.0,
        "per-program p50 was {:.4} ms (want ~5)",
        raydium.p50
    );
}

#[test]
fn disconnect_and_reconnect_records_event_and_ttfm() {
    let mut tracker = StabilityTracker::new(EndpointRole::One);

    // Feed a few normal slot events.
    for i in 0..10_u64 {
        let event = Event {
            ts: ts(i * 50),
            subscription: SubscriptionRole::Main {
                endpoint: EndpointRole::One,
                commitment: Commitment::Processed,
                stream: MainStream::Slots,
            },
            payload: EventPayload::Slot {
                slot: i,
                stage: SlotStage::Processed,
            },
        };
        tracker.observe(&event);
    }

    // Disconnect at wall_ms = 500 with 10 events accumulated.
    tracker.record_disconnect(500, "Unavailable: stream reset".into(), 10);

    // Reconnect at 1500ms (1500ms - 500ms downtime).
    tracker.record_reconnect(1500_u64.saturating_mul(1_000_000));

    // First message arrives 40ms after the reconnect → TTFM = 40ms.
    let post_reconnect = Event {
        ts: ts(1540),
        subscription: SubscriptionRole::Main {
            endpoint: EndpointRole::One,
            commitment: Commitment::Processed,
            stream: MainStream::Slots,
        },
        payload: EventPayload::Slot {
            slot: 11,
            stage: SlotStage::Processed,
        },
    };
    tracker.observe(&post_reconnect);

    let summary = tracker.summary();
    assert_eq!(summary.disconnects.len(), 1, "disconnect should be recorded");
    assert_eq!(summary.disconnects[0].events_received_before, 10);
    assert!(
        (summary.reconnect_ttfm_ms.max - 40.0).abs() < 1.0,
        "reconnect TTFM max was {:.4} ms (want ~40)",
        summary.reconnect_ttfm_ms.max
    );
}

#[test]
fn slot_stream_processed_confirmed_drift_paired_within_endpoint() {
    let mut tracker = StabilityTracker::new(EndpointRole::One);

    // Slot 1000: processed at t=0, confirmed at t=300ms.
    let proc_e = Event {
        ts: ts(0),
        subscription: SubscriptionRole::Main {
            endpoint: EndpointRole::One,
            commitment: Commitment::Processed,
            stream: MainStream::Slots,
        },
        payload: EventPayload::Slot {
            slot: 1000,
            stage: SlotStage::Processed,
        },
    };
    let conf_e = Event {
        ts: ts(300),
        subscription: SubscriptionRole::Main {
            endpoint: EndpointRole::One,
            commitment: Commitment::Confirmed,
            stream: MainStream::Slots,
        },
        payload: EventPayload::Slot {
            slot: 1000,
            stage: SlotStage::Confirmed,
        },
    };
    tracker.observe(&proc_e);
    tracker.observe(&conf_e);

    let summary = tracker.summary();
    assert!(
        (summary.processed_confirmed_drift_ms.p50 - 300.0).abs() < 1.0,
        "p50 drift was {:.4} ms (want 300)",
        summary.processed_confirmed_drift_ms.p50
    );
}
