//! Receiver-task spawning, ring buffers, and Linux-only CPU/realtime
//! affinity helpers (the precision posture, §7).
//!
//! Hot path per stream (the bounded-memory invariant):
//!
//! ```text
//! [receiver thread, pinned, optional SCHED_FIFO]
//!   recv yellowstone TimedUpdate
//!   convert SubscribeUpdate -> Event
//!   ring.try_push(event)
//!     on overflow: ring.dropped_count += 1, drop event
//! ```
//!
//! The ring is a crossbeam-channel `bounded(N)` whose capacity is fixed
//! at receiver construction. Send and receive are lock-free; no heap
//! allocation occurs on the send/recv path past the initial channel
//! allocation. The receiver never blocks: `try_send` returns immediately
//! on overflow and the harness records the drop.

pub mod ring;

use std::{
    convert::TryFrom,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, SlotStatus, SubscribeUpdate,
};

use crate::{
    subscribe::{
        yellowstone::TimedUpdate,
        EndpointRole, SubscriptionRole,
    },
    timing::EventTimestamp,
};

pub use ring::{EventReceiver, EventSender, Ring};

/// Spec §6.1 — the 7 `SlotStatus` stages we observe (the proto adds `Dead`,
/// which we surface but don't tag in the main t-digests since it's an
/// error mode, not a normal stage).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlotStage {
    /// `SLOT_FIRST_SHRED_RECEIVED` — the earliest stage a slot is observed.
    FirstShredReceived,
    /// `SLOT_COMPLETED` — all shreds received.
    Completed,
    /// `SLOT_CREATED_BANK` — bank created for the slot.
    CreatedBank,
    /// `SLOT_PROCESSED`.
    Processed,
    /// `SLOT_CONFIRMED`.
    Confirmed,
    /// `SLOT_FINALIZED` (sometimes called "rooted").
    Finalized,
    /// `SLOT_DEAD` — a fork chose a different bank. Tracked for stability
    /// metrics; not part of the main t-digest set.
    Dead,
}

impl SlotStage {
    /// Stable lowercase tag used in the output JSON.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::FirstShredReceived => "first_shred_received",
            Self::Completed => "completed",
            Self::CreatedBank => "created_bank",
            Self::Processed => "processed",
            Self::Confirmed => "confirmed",
            Self::Finalized => "finalized",
            Self::Dead => "dead",
        }
    }

    /// Decode from the yellowstone proto enum value.
    #[must_use]
    pub fn from_proto(status: SlotStatus) -> Self {
        match status {
            SlotStatus::SlotFirstShredReceived => Self::FirstShredReceived,
            SlotStatus::SlotCompleted => Self::Completed,
            SlotStatus::SlotCreatedBank => Self::CreatedBank,
            SlotStatus::SlotProcessed => Self::Processed,
            SlotStatus::SlotConfirmed => Self::Confirmed,
            SlotStatus::SlotFinalized => Self::Finalized,
            SlotStatus::SlotDead => Self::Dead,
        }
    }
}

/// 32-byte Solana pubkey / blockhash / owner.
pub type Pubkey32 = [u8; 32];
/// 64-byte Solana signature.
pub type Signature64 = [u8; 64];

/// One canonical event flowing into the matcher and t-digest pipeline.
#[derive(Debug, Clone)]
pub struct Event {
    /// `(mono_ns, wall_ms)` pair.
    pub ts: EventTimestamp,
    /// Which subscription emitted this event.
    pub subscription: SubscriptionRole,
    /// Typed identity + side-channel fields.
    pub payload: EventPayload,
}

/// Identity + side-channel metadata, per-stream.
#[derive(Debug, Clone)]
pub enum EventPayload {
    /// Slot status. Identity: `(slot, stage)`.
    Slot {
        /// Slot number.
        slot: u64,
        /// Stage in the slot lifecycle.
        stage: SlotStage,
    },
    /// Account update. Identity: `(slot, pubkey, write_version, txn_signature)`.
    Account {
        /// Slot containing the account write.
        slot: u64,
        /// Account public key.
        pubkey: Pubkey32,
        /// Owner program (used for per-program t-digest bucketing,
        /// spec §6.2).
        owner: Pubkey32,
        /// Monotonic per-account version assigned by the validator.
        write_version: u64,
        /// Producing transaction signature, when known.
        txn_signature: Option<Signature64>,
        /// Lamports value, used to detect `0 -> >0` pool-creation events
        /// for the `entries_vs_account` cross-stream metric ().
        lamports: u64,
    },
    /// Transaction stream. Identity: `(slot, signature)`.
    Transaction {
        /// Slot containing the transaction.
        slot: u64,
        /// 64-byte signature.
        signature: Signature64,
        /// Position within the slot.
        index: u64,
    },
    /// Block. Identity: `(slot, blockhash)`.
    Block {
        /// Slot.
        slot: u64,
        /// 32-byte blockhash, base58-decoded from the proto's
        /// `string blockhash` field.
        blockhash: Pubkey32,
        /// Number of executed transactions (proto
        /// `executed_transaction_count`).
        tx_count: u64,
        /// Number of entries in the block.
        entries_count: u64,
        /// Approximate encoded size of the block payload in bytes
        /// (`prost::Message::encoded_len`). Used by downstream analysis
        /// to correlate latency with load ().
        block_size_bytes: u64,
    },
    /// Entry. Identity: `(slot, index)`.
    Entry {
        /// Slot containing the entry.
        slot: u64,
        /// Entry index within the slot.
        index: u64,
        /// Number of transactions executed within this entry. Combined
        /// with `starting_transaction_index` this slices the tx stream
        /// for index-based correlation (PROTO.md, the entries-no-sigs
        /// Phase 1 design).
        executed_transaction_count: u64,
        /// Starting transaction index within the slot.
        starting_transaction_index: u64,
    },
}

/// Errors when decoding a [`SubscribeUpdate`] into an [`Event`].
#[derive(Debug, Error)]
pub enum DecodeError {
    /// Account update arrived without the embedded `AccountInfo` payload.
    #[error("account update missing AccountInfo")]
    AccountMissingInfo,
    /// Transaction update arrived without the embedded `TransactionInfo`.
    #[error("transaction update missing TransactionInfo")]
    TxMissingInfo,
    /// Pubkey / owner field had a non-32-byte length.
    #[error("expected 32-byte pubkey/owner, got {got} bytes")]
    BadPubkeyLen {
        /// Observed length.
        got: usize,
    },
    /// Signature field had a non-64-byte length.
    #[error("expected 64-byte signature, got {got} bytes")]
    BadSignatureLen {
        /// Observed length.
        got: usize,
    },
    /// Blockhash string decoded to a non-32-byte payload.
    #[error("expected 32-byte blockhash, got {got} bytes")]
    BadBlockhashLen {
        /// Observed length after base58 decode.
        got: usize,
    },
    /// Blockhash was not valid base58.
    #[error("blockhash {hash:?} is not valid base58: {reason}")]
    BadBlockhashEncoding {
        /// Verbatim hash string.
        hash: String,
        /// Underlying base58 error description.
        reason: String,
    },
    /// `SlotStatus` decoded to an unknown enum value.
    #[error("unknown SlotStatus discriminant {0}")]
    UnknownSlotStatus(i32),
}

/// Convert one `TimedUpdate` to zero or one events.
///
/// Returns `Ok(None)` for non-data control messages (Ping / Pong /
/// `BlockMeta` / `TransactionStatus` / Deshred — none of which feed the
/// spec's measurements).
///
/// # Errors
/// Returns [`DecodeError`] when an account/transaction/block update is
/// missing required fields or has a malformed pubkey/signature/blockhash.
pub fn decode(
    timed: TimedUpdate,
    subscription: SubscriptionRole,
) -> Result<Option<Event>, DecodeError> {
    use yellowstone_grpc_proto::prost::Message;

    let TimedUpdate { ts, update } = timed;
    let SubscribeUpdate { update_oneof, .. } = update;
    let Some(payload) = update_oneof else {
        return Ok(None);
    };

    let event_payload = match payload {
        UpdateOneof::Slot(s) => {
            let status = SlotStatus::try_from(s.status)
                .map_err(|_| DecodeError::UnknownSlotStatus(s.status))?;
            EventPayload::Slot {
                slot: s.slot,
                stage: SlotStage::from_proto(status),
            }
        }
        UpdateOneof::Account(a) => {
            let info = a.account.ok_or(DecodeError::AccountMissingInfo)?;
            let pubkey = to_pubkey(&info.pubkey)?;
            let owner = to_pubkey(&info.owner)?;
            let txn_signature = info
                .txn_signature
                .as_ref()
                .map(|s| to_signature(s))
                .transpose()?;
            EventPayload::Account {
                slot: a.slot,
                pubkey,
                owner,
                write_version: info.write_version,
                txn_signature,
                lamports: info.lamports,
            }
        }
        UpdateOneof::Transaction(t) => {
            let info = t.transaction.ok_or(DecodeError::TxMissingInfo)?;
            let signature = to_signature(&info.signature)?;
            EventPayload::Transaction {
                slot: t.slot,
                signature,
                index: info.index,
            }
        }
        UpdateOneof::Block(b) => {
            let bh_bytes = bs58::decode(&b.blockhash)
                .into_vec()
                .map_err(|e| DecodeError::BadBlockhashEncoding {
                    hash: b.blockhash.clone(),
                    reason: e.to_string(),
                })?;
            if bh_bytes.len() != 32 {
                return Err(DecodeError::BadBlockhashLen {
                    got: bh_bytes.len(),
                });
            }
            let mut blockhash = [0u8; 32];
            blockhash.copy_from_slice(&bh_bytes);
            let block_size_bytes = u64::try_from(b.encoded_len()).unwrap_or(u64::MAX);
            EventPayload::Block {
                slot: b.slot,
                blockhash,
                tx_count: b.executed_transaction_count,
                entries_count: b.entries_count,
                block_size_bytes,
            }
        }
        UpdateOneof::Entry(e) => EventPayload::Entry {
            slot: e.slot,
            index: e.index,
            executed_transaction_count: e.executed_transaction_count,
            starting_transaction_index: e.starting_transaction_index,
        },
        // Control / unused variants — silently skipped.
        UpdateOneof::Ping(_)
        | UpdateOneof::Pong(_)
        | UpdateOneof::BlockMeta(_)
        | UpdateOneof::TransactionStatus(_) => return Ok(None),
    };

    Ok(Some(Event {
        ts,
        subscription,
        payload: event_payload,
    }))
}

fn to_pubkey(b: &[u8]) -> Result<Pubkey32, DecodeError> {
    if b.len() != 32 {
        return Err(DecodeError::BadPubkeyLen { got: b.len() });
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(b);
    Ok(out)
}

fn to_signature(b: &[u8]) -> Result<Signature64, DecodeError> {
    if b.len() != 64 {
        return Err(DecodeError::BadSignatureLen { got: b.len() });
    }
    let mut out = [0u8; 64];
    out.copy_from_slice(b);
    Ok(out)
}

/// Plan-time receiver task assignment of CPU cores. Precision posture default for a
/// 4-core run: endpoint1 receiver on core 2, endpoint2 receiver on core 3,
/// processor on core 4, control plane on core 5. Cores 0-1 left to the
/// kernel.
#[derive(Debug, Clone)]
pub struct AffinityPlan {
    /// Cores receiving streams from endpoint1, in order of the
    /// subscriptions enumerated by [`crate::subscribe::SubscriptionPlan`].
    /// If fewer cores than subscriptions are configured, later
    /// subscriptions cycle through the available cores.
    pub endpoint1_cores: Vec<u32>,
    /// Cores receiving streams from endpoint2.
    pub endpoint2_cores: Vec<u32>,
    /// Core used for the matcher / summary thread.
    pub processor_core: Option<u32>,
    /// Core used for control-plane work (signal handling, periodic
    /// snapshots).
    pub control_core: Option<u32>,
}

impl AffinityPlan {
    /// Derive the spec's suggested 4-core layout from a raw CLI affinity
    /// list. Empty input yields a no-pin plan (`None` everywhere); shorter
    /// lists fall back gracefully:
    /// - 1 core: all receivers + processor share that core; control =
    ///   `None`.
    /// - 2 cores: endpoint1 receivers on core[0], endpoint2 receivers on
    ///   core[1]; no processor/control pin.
    /// - 3 cores: as 2, plus processor on core[2].
    /// - 4+ cores: spec default.
    ///
    /// Returning `None` for unassigned cores means the OS scheduler is
    /// left free, which is correct on non-Linux dev hosts where pinning
    /// is unsupported anyway.
    #[must_use]
    pub fn from_cli(cores: &[u32]) -> Self {
        match cores {
            [] => Self {
                endpoint1_cores: vec![],
                endpoint2_cores: vec![],
                processor_core: None,
                control_core: None,
            },
            [c] => Self {
                endpoint1_cores: vec![*c],
                endpoint2_cores: vec![*c],
                processor_core: Some(*c),
                control_core: None,
            },
            [a, b] => Self {
                endpoint1_cores: vec![*a],
                endpoint2_cores: vec![*b],
                processor_core: None,
                control_core: None,
            },
            [a, b, p] => Self {
                endpoint1_cores: vec![*a],
                endpoint2_cores: vec![*b],
                processor_core: Some(*p),
                control_core: None,
            },
            [a, b, p, c, ..] => Self {
                endpoint1_cores: vec![*a],
                endpoint2_cores: vec![*b],
                processor_core: Some(*p),
                control_core: Some(*c),
            },
        }
    }

    /// Resolve the core for a given subscription, cycling when fewer cores
    /// than subscriptions are configured.
    #[must_use]
    pub fn core_for_subscription(&self, role: SubscriptionRole, idx_in_endpoint: usize) -> Option<u32> {
        let cores = match role.endpoint() {
            EndpointRole::One => &self.endpoint1_cores,
            EndpointRole::Two => &self.endpoint2_cores,
        };
        if cores.is_empty() {
            None
        } else {
            Some(cores[idx_in_endpoint % cores.len()])
        }
    }

    /// Build from the parsed user spec. The runtime adapter shape
    /// ([`Self`]) is kept distinct from the serializable user-input
    /// shape ([`AffinitySpec`]) so the runtime methods
    /// (`core_for_subscription`) stay decoupled from the CLI surface.
    #[must_use]
    pub fn from_spec(spec: &AffinitySpec) -> Self {
        Self {
            endpoint1_cores: spec.endpoint1.clone(),
            endpoint2_cores: spec.endpoint2.clone(),
            processor_core: spec.processor,
            control_core: spec.control,
        }
    }
}

/// Parsed `--cpu-affinity` value. Both the legacy flat-comma form
/// (`"2,3,4,5"`, the precision posture default) and the structured per-endpoint form
/// (`"ep1=2,3,4,5:ep2=6,7,8,9:proc=10:ctrl=11"`) deserialize into this
/// shape. The structured form is required to express per-endpoint
/// multi-core layouts; the flat form is preserved for backwards-
/// compatibility with existing operator scripts and TOML overlays.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AffinitySpec {
    /// Cores assigned to endpoint1 receivers; empty means no pin.
    /// Multiple subscriptions on the same endpoint cycle through this
    /// list (see [`AffinityPlan::core_for_subscription`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub endpoint1: Vec<u32>,
    /// Cores assigned to endpoint2 receivers; empty means no pin.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub endpoint2: Vec<u32>,
    /// Pinned core for the processor (main) thread, or `None` for unpinned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processor: Option<u32>,
    /// Pinned core for the control thread, or `None` for unpinned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control: Option<u32>,
}

impl AffinitySpec {
    /// Parse a raw `--cpu-affinity` string. Empty input yields the
    /// default no-pin spec. Strings containing `=` are parsed as the
    /// structured form; pure comma-separated digit lists go through the
    /// legacy flat path. The literal `"auto"` (case-insensitive) maps to
    /// [`Self::auto_from_nproc`]. Mixed forms (e.g. `"2,3,ep1=4"`) are
    /// rejected.
    ///
    /// # Errors
    /// Returns [`AffinityParseError`] on malformed input, unknown
    /// roles, duplicate cores, single-core role violations, or — for
    /// `auto` — a host with too few cores to split safely.
    pub fn parse(raw: &str) -> Result<Self, AffinityParseError> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(Self::default());
        }
        if trimmed.eq_ignore_ascii_case("auto") {
            return Ok(Self::auto_from_nproc());
        }
        if trimmed.contains('=') {
            Self::from_structured(trimmed)
        } else {
            Self::from_flat_str(trimmed)
        }
    }

    /// Derive a sensible per-endpoint affinity from the host's available
    /// parallelism. Layout policy:
    ///
    /// - Reserve cores 0 and 1 for kernel timers / softirqs / system tasks.
    /// - Reserve the highest core for the control thread.
    /// - Split the remaining contiguous range 50/50 between ep1 and ep2
    ///   (ep1 takes the lower half, ep2 the upper).
    /// - Leave `processor` unpinned — under `--realtime` it would be
    ///   stripped anyway (`rt_coordinator_pin_wedge`); without
    ///   `--realtime` an unpinned processor matches the spec's default.
    ///
    /// Hosts with fewer than 6 cores fall back to [`Self::default`] (no
    /// pinning). Splitting receivers across two cores on a 4-core host
    /// would starve the kernel of scheduling slack and produce worse
    /// numbers than letting the scheduler handle it.
    ///
    /// Reads core count from [`std::thread::available_parallelism`];
    /// falls back to 4 if that query fails.
    #[must_use]
    pub fn auto_from_nproc() -> Self {
        let nproc = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(4);
        Self::auto_for_core_count(nproc)
    }

    /// Deterministic core layout for a given `nproc`. Pulled out of
    /// [`Self::auto_from_nproc`] for testability.
    #[must_use]
    pub fn auto_for_core_count(nproc: usize) -> Self {
        // Need at least 6 cores to safely reserve 2 system + 1 control
        // and split the remaining 3 across endpoints. Anything smaller
        // and the gain from pinning is overwhelmed by the loss of
        // scheduling flexibility.
        if nproc < 6 {
            return Self::default();
        }
        // Clamp to u32 range — sched_setaffinity takes u32 cpu indices,
        // and any host with > 2^32 logical cores doesn't exist outside
        // a hypothetical future.
        let nproc_u32 = u32::try_from(nproc).unwrap_or(u32::MAX);
        let last = nproc_u32 - 1;
        let control_core = last;
        let receiver_start: u32 = 2;
        let receiver_end_inclusive = last - 1; // one below control
        let receiver_count = receiver_end_inclusive - receiver_start + 1;
        // Round-down split: ep1 gets the lower half, ep2 gets the upper
        // half (which may have one extra core on odd counts).
        let half = receiver_count / 2;
        let split_at = receiver_start + half;
        let endpoint1: Vec<u32> = (receiver_start..split_at).collect();
        let endpoint2: Vec<u32> = (split_at..=receiver_end_inclusive).collect();
        Self {
            endpoint1,
            endpoint2,
            processor: None,
            control: Some(control_core),
        }
    }

    /// Build from a pre-parsed flat slice (as TOML overlays supply).
    /// Maps to the spec's 4-core default layout, identical to how
    /// [`AffinityPlan::from_cli`] interpreted the same input.
    ///
    /// # Errors
    /// Returns [`AffinityParseError::DuplicateCore`] if the slice
    /// contains a repeated core id.
    pub fn from_flat_vec(cores: &[u32]) -> Result<Self, AffinityParseError> {
        for (i, c) in cores.iter().enumerate() {
            if cores[..i].contains(c) {
                return Err(AffinityParseError::DuplicateCore { core: *c });
            }
        }
        Ok(match cores {
            [] => Self::default(),
            [c] => Self {
                endpoint1: vec![*c],
                endpoint2: vec![*c],
                processor: Some(*c),
                control: None,
            },
            [a, b] => Self {
                endpoint1: vec![*a],
                endpoint2: vec![*b],
                processor: None,
                control: None,
            },
            [a, b, p] => Self {
                endpoint1: vec![*a],
                endpoint2: vec![*b],
                processor: Some(*p),
                control: None,
            },
            [a, b, p, c, ..] => Self {
                endpoint1: vec![*a],
                endpoint2: vec![*b],
                processor: Some(*p),
                control: Some(*c),
            },
        })
    }

    fn from_flat_str(raw: &str) -> Result<Self, AffinityParseError> {
        let cores = parse_core_list(raw, "flat")?;
        Self::from_flat_vec(&cores)
    }

    fn from_structured(raw: &str) -> Result<Self, AffinityParseError> {
        let mut me = Self::default();
        let mut seen_ep1 = false;
        let mut seen_ep2 = false;
        let mut seen_proc = false;
        let mut seen_ctrl = false;
        for section in raw.split(':') {
            let section = section.trim();
            if section.is_empty() {
                return Err(AffinityParseError::EmptySection);
            }
            let (role, value) = section
                .split_once('=')
                .ok_or(AffinityParseError::Mixed)?;
            let role = role.trim();
            let value = value.trim();
            let cores = parse_core_list(value, role.to_string())?;
            if cores.is_empty() {
                return Err(AffinityParseError::EmptySection);
            }
            match role {
                "ep1" => {
                    if seen_ep1 {
                        return Err(AffinityParseError::DuplicateRole { role: "ep1" });
                    }
                    seen_ep1 = true;
                    me.endpoint1 = cores;
                }
                "ep2" => {
                    if seen_ep2 {
                        return Err(AffinityParseError::DuplicateRole { role: "ep2" });
                    }
                    seen_ep2 = true;
                    me.endpoint2 = cores;
                }
                "proc" => {
                    if seen_proc {
                        return Err(AffinityParseError::DuplicateRole { role: "proc" });
                    }
                    if cores.len() != 1 {
                        return Err(AffinityParseError::SingleCoreOnly { role: "proc" });
                    }
                    seen_proc = true;
                    me.processor = Some(cores[0]);
                }
                "ctrl" => {
                    if seen_ctrl {
                        return Err(AffinityParseError::DuplicateRole { role: "ctrl" });
                    }
                    if cores.len() != 1 {
                        return Err(AffinityParseError::SingleCoreOnly { role: "ctrl" });
                    }
                    seen_ctrl = true;
                    me.control = Some(cores[0]);
                }
                other => {
                    return Err(AffinityParseError::UnknownRole {
                        role: other.to_string(),
                    });
                }
            }
        }
        // Cross-role dedup — a core can't double up as ep1+ep2, ep1+proc, etc.
        // Build the flat list directly (do NOT use `all_cores()`, which
        // silently de-dups for the legacy-echo path).
        let mut flat: Vec<u32> = Vec::new();
        flat.extend(&me.endpoint1);
        flat.extend(&me.endpoint2);
        if let Some(p) = me.processor {
            flat.push(p);
        }
        if let Some(c) = me.control {
            flat.push(c);
        }
        for (i, c) in flat.iter().enumerate() {
            if flat[..i].contains(c) {
                return Err(AffinityParseError::DuplicateCore { core: *c });
            }
        }
        Ok(me)
    }

    /// Flattened, dedup-stable union of every core referenced. Used to
    /// keep the legacy `host_metadata.cpu_affinity: Vec<u32>` field
    /// populated alongside the structured shape so existing JSON
    /// consumers continue to parse.
    #[must_use]
    pub fn all_cores(&self) -> Vec<u32> {
        let mut out: Vec<u32> = Vec::new();
        for c in &self.endpoint1 {
            if !out.contains(c) {
                out.push(*c);
            }
        }
        for c in &self.endpoint2 {
            if !out.contains(c) {
                out.push(*c);
            }
        }
        if let Some(p) = self.processor {
            if !out.contains(&p) {
                out.push(p);
            }
        }
        if let Some(c) = self.control {
            if !out.contains(&c) {
                out.push(c);
            }
        }
        out
    }

    /// True when no pin is requested anywhere.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.endpoint1.is_empty()
            && self.endpoint2.is_empty()
            && self.processor.is_none()
            && self.control.is_none()
    }
}

fn parse_core_list<S: Into<String>>(value: &str, section_label: S) -> Result<Vec<u32>, AffinityParseError> {
    let label = section_label.into();
    let mut cores: Vec<u32> = Vec::new();
    for part in value.split(',') {
        let t = part.trim();
        if t.is_empty() {
            return Err(AffinityParseError::EmptySection);
        }
        let n: u32 = t.parse().map_err(|_| AffinityParseError::InvalidCore {
            core: t.to_string(),
            section: label.clone(),
        })?;
        if cores.contains(&n) {
            return Err(AffinityParseError::DuplicateCore { core: n });
        }
        cores.push(n);
    }
    Ok(cores)
}

/// Errors emitted by [`AffinitySpec::parse`] /
/// [`AffinitySpec::from_flat_vec`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AffinityParseError {
    /// A section in the structured form was missing the `role=value`
    /// separator, or the input mixed flat and structured forms.
    #[error("--cpu-affinity has mixed or malformed sections (use either `2,3,4,5` or `ep1=...:ep2=...:proc=...:ctrl=...`)")]
    Mixed,
    /// A structured section contained no cores (e.g. trailing `:` or
    /// `ep1=`).
    #[error("--cpu-affinity has an empty section")]
    EmptySection,
    /// A structured section named an unknown role.
    #[error("--cpu-affinity has unknown role `{role}` (expected ep1, ep2, proc, ctrl)")]
    UnknownRole {
        /// The unrecognized role string.
        role: String,
    },
    /// A structured role appeared more than once.
    #[error("--cpu-affinity has duplicate role `{role}`")]
    DuplicateRole {
        /// The repeated role label.
        role: &'static str,
    },
    /// A core id failed to parse as `u32`.
    #[error("--cpu-affinity has invalid core id `{core}` in section `{section}`")]
    InvalidCore {
        /// The unparseable token.
        core: String,
        /// The role section it appeared in, or `flat` for the legacy form.
        section: String,
    },
    /// A role expecting exactly one core was given a list.
    #[error("--cpu-affinity role `{role}` accepts exactly one core")]
    SingleCoreOnly {
        /// Role that violated the single-core constraint (`proc` or `ctrl`).
        role: &'static str,
    },
    /// The same core id was assigned to two roles (e.g. `ep1=2:ep2=2`),
    /// or repeated within one section.
    #[error("--cpu-affinity has duplicate core {core}")]
    DuplicateCore {
        /// The repeated core id.
        core: u32,
    },
}

/// Outcome of `apply_cpu_affinity` / `apply_realtime` — `Applied` means the
/// syscall succeeded, `Unsupported` means the call is a no-op on this
/// platform, `Failed` carries the underlying error.
#[derive(Debug)]
pub enum SchedOutcome {
    /// The requested OS-level affinity / scheduler policy was applied.
    Applied,
    /// The current platform doesn't support this operation (off-Linux
    /// dev). Falling back is expected; emit a startup warning.
    Unsupported,
    /// The syscall was attempted but rejected (lacking `CAP_SYS_NICE`,
    /// bad core id, etc.).
    Failed(String),
}

impl SchedOutcome {
    /// Did this attempt succeed?
    #[must_use]
    pub fn is_applied(&self) -> bool {
        matches!(self, Self::Applied)
    }
}

/// Pin the current thread to `core`. Linux-only; non-Linux returns
/// [`SchedOutcome::Unsupported`].
#[cfg(target_os = "linux")]
pub fn apply_cpu_affinity(core: u32) -> SchedOutcome {
    use nix::sched::{sched_setaffinity, CpuSet};
    use nix::unistd::Pid;

    let mut set = CpuSet::new();
    let core_usize = match usize::try_from(core) {
        Ok(v) => v,
        Err(_) => return SchedOutcome::Failed(format!("core {core} exceeds usize range")),
    };
    if let Err(e) = set.set(core_usize) {
        return SchedOutcome::Failed(format!("CpuSet::set({core}) failed: {e}"));
    }
    // Pid::from_raw(0) targets the calling thread.
    match sched_setaffinity(Pid::from_raw(0), &set) {
        Ok(()) => SchedOutcome::Applied,
        Err(e) => SchedOutcome::Failed(format!("sched_setaffinity({core}) failed: {e}")),
    }
}

/// Non-Linux stub.
#[cfg(not(target_os = "linux"))]
#[must_use]
pub fn apply_cpu_affinity(_core: u32) -> SchedOutcome {
    SchedOutcome::Unsupported
}

/// Request `SCHED_FIFO` priority 50 on the current thread. Linux-only;
/// non-Linux returns [`SchedOutcome::Unsupported`].
///
/// Requires `CAP_SYS_NICE` or running as root. Per the precision posture we fail loud
/// (`Failed`) when the syscall is rejected; the binary's startup wiring
/// converts that into a prominent startup warning if `--realtime` was set.
#[cfg(target_os = "linux")]
pub fn apply_realtime() -> SchedOutcome {
    // libc::sched_setscheduler with SCHED_FIFO is unsafe (raw FFI). Per
    //  this is one of the locations where `unsafe` is allowed;
    // we localize it here with a SAFETY justification.
    #[allow(unsafe_code)]
    {
        let pid: libc::pid_t = 0; // self
        let policy = libc::SCHED_FIFO;
        let param = libc::sched_param { sched_priority: 50 };
        // SAFETY: `param` is a stack value of the correct type, and
        // `sched_setscheduler` reads from `&param` for `sizeof(param)`
        // bytes. The pointer is valid and non-null. The call has no
        // aliasing requirements. On error, -1 is returned and `errno`
        // carries the reason; we read it via `io::Error::last_os_error`
        // without holding any reference into the kernel.
        let rc = unsafe {
            libc::sched_setscheduler(pid, policy, std::ptr::addr_of!(param))
        };
        if rc == 0 {
            SchedOutcome::Applied
        } else {
            let err = std::io::Error::last_os_error();
            SchedOutcome::Failed(format!("sched_setscheduler(SCHED_FIFO, 50) failed: {err}"))
        }
    }
}

/// Non-Linux stub.
#[cfg(not(target_os = "linux"))]
#[must_use]
pub fn apply_realtime() -> SchedOutcome {
    SchedOutcome::Unsupported
}

/// Snapshot of receiver-thread statistics, surfaced into the output JSON.
#[derive(Debug)]
pub struct ReceiverStats {
    /// Total events received from the gRPC stream (before drop).
    pub received: AtomicU64,
    /// Events dropped because the ring was full.
    pub dropped: AtomicU64,
    /// Decode errors (proto-level malformations). Surfaced as a warning.
    pub decode_errors: AtomicU64,
    /// Disconnect count for this stream (incremented by the receiver loop
    /// when the underlying stream ends).
    pub disconnects: AtomicU64,
    /// Flipped to true when shutdown is requested by the main loop.
    pub shutdown: AtomicBool,
}

impl ReceiverStats {
    /// Fresh stats counters.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            received: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            decode_errors: AtomicU64::new(0),
            disconnects: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
        })
    }

    /// Snapshot read of all counters (`Ordering::Relaxed` — final summary
    /// happens after all receivers have stopped, so synchronization is
    /// supplied by the join, not the load).
    #[must_use]
    pub fn snapshot(&self) -> ReceiverStatsSnapshot {
        ReceiverStatsSnapshot {
            received: self.received.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
            disconnects: self.disconnects.load(Ordering::Relaxed),
        }
    }
}

/// Plain-value view of [`ReceiverStats`] for use in synchronous code paths
/// (summary, tests).
#[derive(Debug, Clone, Copy)]
pub struct ReceiverStatsSnapshot {
    /// See [`ReceiverStats::received`].
    pub received: u64,
    /// See [`ReceiverStats::dropped`].
    pub dropped: u64,
    /// See [`ReceiverStats::decode_errors`].
    pub decode_errors: u64,
    /// See [`ReceiverStats::disconnects`].
    pub disconnects: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Commitment;
    use crate::subscribe::SubscriptionRole;
    use crate::timing::ClockOrigin;
    use yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateAccount,
        SubscribeUpdateAccountInfo, SubscribeUpdateBlock, SubscribeUpdateEntry, SubscribeUpdatePing,
        SubscribeUpdateSlot, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
    };

    fn ts() -> EventTimestamp {
        ClockOrigin::capture().now_user_space()
    }

    fn sub() -> SubscriptionRole {
        // Test fixture — the matcher dispatch reads payload type, not the
        // subscription's stream tag, so `Accounts` here is arbitrary.
        SubscriptionRole::Main {
            endpoint: EndpointRole::One,
            commitment: Commitment::Processed,
            stream: crate::subscribe::MainStream::Accounts,
        }
    }

    #[test]
    fn slot_stage_round_trip_via_proto_enum() {
        assert!(matches!(
            SlotStage::from_proto(SlotStatus::SlotProcessed),
            SlotStage::Processed
        ));
        assert!(matches!(
            SlotStage::from_proto(SlotStatus::SlotConfirmed),
            SlotStage::Confirmed
        ));
        assert!(matches!(
            SlotStage::from_proto(SlotStatus::SlotFinalized),
            SlotStage::Finalized
        ));
    }

    #[test]
    fn decode_slot_update_produces_event() {
        let update = SubscribeUpdate {
            filters: vec!["slots-all".into()],
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 42,
                parent: Some(41),
                status: SlotStatus::SlotProcessed as i32,
                dead_error: None,
            })),
            created_at: None,
        };
        let ev = decode(TimedUpdate { ts: ts(), update }, sub()).unwrap().unwrap();
        match ev.payload {
            EventPayload::Slot { slot, stage } => {
                assert_eq!(slot, 42);
                assert!(matches!(stage, SlotStage::Processed));
            }
            other => panic!("expected Slot, got {other:?}"),
        }
    }

    #[test]
    fn decode_account_update_carries_identity_fields() {
        let pubkey = [7u8; 32];
        let owner = [8u8; 32];
        let sig = Some(vec![9u8; 64]);
        let update = SubscribeUpdate {
            filters: vec!["all-programs".into()],
            update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                slot: 100,
                is_startup: false,
                account: Some(SubscribeUpdateAccountInfo {
                    pubkey: pubkey.to_vec(),
                    lamports: 5_000,
                    owner: owner.to_vec(),
                    executable: false,
                    rent_epoch: 0,
                    data: vec![],
                    write_version: 42,
                    txn_signature: sig.clone(),
                }),
            })),
            created_at: None,
        };
        let ev = decode(TimedUpdate { ts: ts(), update }, sub()).unwrap().unwrap();
        match ev.payload {
            EventPayload::Account {
                slot,
                pubkey: p,
                owner: o,
                write_version,
                txn_signature,
                lamports,
            } => {
                assert_eq!(slot, 100);
                assert_eq!(p, pubkey);
                assert_eq!(o, owner);
                assert_eq!(write_version, 42);
                assert_eq!(lamports, 5_000);
                let sig_arr = txn_signature.expect("signature must round-trip");
                assert_eq!(sig_arr[0], 9);
                assert_eq!(sig_arr[63], 9);
            }
            other => panic!("expected Account, got {other:?}"),
        }
    }

    #[test]
    fn decode_transaction_carries_signature() {
        let signature = vec![3u8; 64];
        let update = SubscribeUpdate {
            filters: vec!["all-programs-tx".into()],
            update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                slot: 200,
                transaction: Some(SubscribeUpdateTransactionInfo {
                    signature: signature.clone(),
                    is_vote: false,
                    transaction: None,
                    meta: None,
                    index: 7,
                }),
            })),
            created_at: None,
        };
        let ev = decode(TimedUpdate { ts: ts(), update }, sub()).unwrap().unwrap();
        match ev.payload {
            EventPayload::Transaction { slot, signature: sig, index } => {
                assert_eq!(slot, 200);
                assert_eq!(sig.to_vec(), signature);
                assert_eq!(index, 7);
            }
            other => panic!("expected Transaction, got {other:?}"),
        }
    }

    #[test]
    fn decode_block_decodes_blockhash_from_base58() {
        // bs58 of 32 zero bytes:
        let zero_hash = bs58::encode([0u8; 32]).into_string();
        let update = SubscribeUpdate {
            filters: vec!["all-blocks".into()],
            update_oneof: Some(UpdateOneof::Block(SubscribeUpdateBlock {
                slot: 300,
                blockhash: zero_hash.clone(),
                rewards: None,
                block_time: None,
                block_height: None,
                parent_slot: 299,
                parent_blockhash: zero_hash,
                executed_transaction_count: 25,
                transactions: vec![],
                updated_account_count: 0,
                accounts: vec![],
                entries_count: 4,
                entries: vec![],
            })),
            created_at: None,
        };
        let ev = decode(TimedUpdate { ts: ts(), update }, sub()).unwrap().unwrap();
        match ev.payload {
            EventPayload::Block {
                slot,
                blockhash,
                tx_count,
                entries_count,
                block_size_bytes,
            } => {
                assert_eq!(slot, 300);
                assert_eq!(blockhash, [0u8; 32]);
                assert_eq!(tx_count, 25);
                assert_eq!(entries_count, 4);
                assert!(block_size_bytes > 0);
            }
            other => panic!("expected Block, got {other:?}"),
        }
    }

    #[test]
    fn decode_entry_carries_index_fields() {
        let update = SubscribeUpdate {
            filters: vec!["all-entries".into()],
            update_oneof: Some(UpdateOneof::Entry(SubscribeUpdateEntry {
                slot: 500,
                index: 13,
                num_hashes: 12_500,
                hash: vec![1u8; 32],
                executed_transaction_count: 11,
                starting_transaction_index: 1_000,
            })),
            created_at: None,
        };
        let ev = decode(TimedUpdate { ts: ts(), update }, sub()).unwrap().unwrap();
        match ev.payload {
            EventPayload::Entry {
                slot,
                index,
                executed_transaction_count,
                starting_transaction_index,
            } => {
                assert_eq!(slot, 500);
                assert_eq!(index, 13);
                assert_eq!(executed_transaction_count, 11);
                assert_eq!(starting_transaction_index, 1_000);
            }
            other => panic!("expected Entry, got {other:?}"),
        }
    }

    #[test]
    fn decode_skips_pings() {
        let update = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
            created_at: None,
        };
        let ev = decode(TimedUpdate { ts: ts(), update }, sub()).unwrap();
        assert!(ev.is_none());
    }

    #[test]
    fn decode_skips_empty_update_oneof() {
        let update = SubscribeUpdate {
            filters: vec![],
            update_oneof: None,
            created_at: None,
        };
        let ev = decode(TimedUpdate { ts: ts(), update }, sub()).unwrap();
        assert!(ev.is_none());
    }

    #[test]
    fn decode_rejects_bad_pubkey_length() {
        let update = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                slot: 1,
                is_startup: false,
                account: Some(SubscribeUpdateAccountInfo {
                    pubkey: vec![1, 2, 3], // wrong length
                    lamports: 0,
                    owner: vec![0u8; 32],
                    executable: false,
                    rent_epoch: 0,
                    data: vec![],
                    write_version: 0,
                    txn_signature: None,
                }),
            })),
            created_at: None,
        };
        let err = decode(TimedUpdate { ts: ts(), update }, sub()).unwrap_err();
        assert!(matches!(err, DecodeError::BadPubkeyLen { got: 3 }));
    }

    #[test]
    fn decode_rejects_account_missing_info() {
        let update = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                slot: 1,
                is_startup: false,
                account: None,
            })),
            created_at: None,
        };
        let err = decode(TimedUpdate { ts: ts(), update }, sub()).unwrap_err();
        assert!(matches!(err, DecodeError::AccountMissingInfo));
    }

    #[test]
    fn affinity_plan_from_four_cores_matches_spec_default() {
        let plan = AffinityPlan::from_cli(&[2, 3, 4, 5]);
        assert_eq!(plan.endpoint1_cores, vec![2]);
        assert_eq!(plan.endpoint2_cores, vec![3]);
        assert_eq!(plan.processor_core, Some(4));
        assert_eq!(plan.control_core, Some(5));
    }

    #[test]
    fn affinity_plan_from_empty_yields_no_pin() {
        let plan = AffinityPlan::from_cli(&[]);
        assert!(plan.endpoint1_cores.is_empty());
        assert!(plan.processor_core.is_none());
    }

    #[test]
    fn affinity_plan_partial_specs_fall_back_gracefully() {
        let plan = AffinityPlan::from_cli(&[2, 3]);
        assert_eq!(plan.endpoint1_cores, vec![2]);
        assert_eq!(plan.endpoint2_cores, vec![3]);
        assert!(plan.processor_core.is_none());
        assert!(plan.control_core.is_none());
    }

    #[test]
    fn affinity_spec_structured_parse_round_trips() {
        let spec = AffinitySpec::parse("ep1=2,3,4,5:ep2=6,7,8,9:proc=10:ctrl=11").unwrap();
        assert_eq!(spec.endpoint1, vec![2, 3, 4, 5]);
        assert_eq!(spec.endpoint2, vec![6, 7, 8, 9]);
        assert_eq!(spec.processor, Some(10));
        assert_eq!(spec.control, Some(11));
    }

    #[test]
    fn affinity_spec_flat_parse_equivalent_to_from_flat_vec() {
        let parsed = AffinitySpec::parse("2,3,4,5").unwrap();
        let via_vec = AffinitySpec::from_flat_vec(&[2, 3, 4, 5]).unwrap();
        assert_eq!(parsed, via_vec);
    }

    #[test]
    fn affinity_spec_all_cores_preserves_order_and_dedups() {
        // ep1 and ep2 sharing a core would actually be rejected by the
        // parser; the union builder still needs to dedup defensively for
        // hand-built specs and the legacy back-compat echo.
        let spec = AffinitySpec {
            endpoint1: vec![2, 3],
            endpoint2: vec![3, 4],
            processor: Some(2),
            control: Some(5),
        };
        assert_eq!(spec.all_cores(), vec![2, 3, 4, 5]);
    }

    #[test]
    fn affinity_spec_partial_structured_omits_unsetroles() {
        let spec = AffinitySpec::parse("ep1=2,3").unwrap();
        assert_eq!(spec.endpoint1, vec![2, 3]);
        assert!(spec.endpoint2.is_empty());
        assert!(spec.processor.is_none());
        assert!(spec.control.is_none());
    }

    #[test]
    fn affinity_spec_empty_parse_yields_default() {
        let spec = AffinitySpec::parse("").unwrap();
        assert!(spec.is_empty());
        let spec_ws = AffinitySpec::parse("   ").unwrap();
        assert!(spec_ws.is_empty());
    }

    #[test]
    fn affinity_auto_8_cores_balances_endpoints() {
        // 8 cores → reserve 0,1 system + 7 control → 5 receiver cores
        // (2-6). Floor-division split: ep1 gets 2 cores, ep2 gets 3
        // (the extra on odd counts goes to ep2 — the "system under test"
        // side, which is the more relevant axis to give breathing room
        // when cores are scarce).
        let spec = AffinitySpec::auto_for_core_count(8);
        assert_eq!(spec.endpoint1, vec![2, 3]);
        assert_eq!(spec.endpoint2, vec![4, 5, 6]);
        assert_eq!(spec.processor, None);
        assert_eq!(spec.control, Some(7));
    }

    #[test]
    fn affinity_auto_16_cores_balances_endpoints() {
        // 16 cores → reserve 0,1 system + 15 control → 12 receiver cores
        // (2-14) split 6/7 (lower half is floor).
        let spec = AffinitySpec::auto_for_core_count(16);
        assert_eq!(spec.endpoint1, vec![2, 3, 4, 5, 6, 7]);
        assert_eq!(spec.endpoint2, vec![8, 9, 10, 11, 12, 13, 14]);
        assert_eq!(spec.processor, None);
        assert_eq!(spec.control, Some(15));
    }

    #[test]
    fn affinity_auto_64_cores_customer_rig() {
        // The customer's production rig is AWS 64-vCPU. Layout: reserve
        // 0,1 (system) + 63 (control), 61 receiver cores (2-62) split
        // 30/31. ep2 gets the extra core (system under test, more
        // breathing room for the harder side of the measurement).
        let spec = AffinitySpec::auto_for_core_count(64);
        assert_eq!(spec.endpoint1.first(), Some(&2));
        assert_eq!(spec.endpoint1.last(), Some(&31));
        assert_eq!(spec.endpoint1.len(), 30);
        assert_eq!(spec.endpoint2.first(), Some(&32));
        assert_eq!(spec.endpoint2.last(), Some(&62));
        assert_eq!(spec.endpoint2.len(), 31);
        assert_eq!(spec.processor, None);
        assert_eq!(spec.control, Some(63));
        // Sanity: no overlap between the two endpoint ranges.
        for c in &spec.endpoint1 {
            assert!(!spec.endpoint2.contains(c), "core {c} in both endpoints");
        }
    }

    #[test]
    fn affinity_auto_32_cores_matches_runbook_layout() {
        // 32 cores → reserve 0,1 system + 31 control → 28 receiver cores
        // (2-30) split 14/15. Close to the runbook's manual 10/10 layout
        // but uses more cores (no good reason to leave them idle on the
        // 32-vCPU rig).
        let spec = AffinitySpec::auto_for_core_count(32);
        assert_eq!(spec.endpoint1.first(), Some(&2));
        assert_eq!(spec.endpoint1.last(), Some(&15));
        assert_eq!(spec.endpoint1.len(), 14);
        assert_eq!(spec.endpoint2.first(), Some(&16));
        assert_eq!(spec.endpoint2.last(), Some(&30));
        assert_eq!(spec.endpoint2.len(), 15);
        assert_eq!(spec.processor, None);
        assert_eq!(spec.control, Some(31));
    }

    #[test]
    fn affinity_auto_small_host_falls_back_to_no_pin() {
        // Below 6 cores there's no margin for pinning — the kernel
        // scheduler does a better job than forcing receivers onto two
        // cores while starving the rest of the workload.
        for n in 1..6 {
            let spec = AffinitySpec::auto_for_core_count(n);
            assert!(spec.is_empty(), "n={n} expected no-pin default");
        }
    }

    #[test]
    fn affinity_parse_auto_keyword() {
        // `auto` (case-insensitive) maps to the nproc-derived layout.
        // We can't assert the exact cores (depends on host's
        // available_parallelism), but it must produce a non-empty spec
        // on any host with 6+ cores.
        let spec_lower = AffinitySpec::parse("auto").unwrap();
        let spec_upper = AffinitySpec::parse("AUTO").unwrap();
        let spec_mixed = AffinitySpec::parse("  Auto  ").unwrap();
        // All three should be identical (deterministic for the same host).
        assert_eq!(spec_lower, spec_upper);
        assert_eq!(spec_lower, spec_mixed);
    }

    #[test]
    fn affinity_plan_from_spec_drives_multi_core_cycling() {
        // Per-endpoint multi-core layout exercises the
        // `core_for_subscription` cycling that was already in place but
        // unreachable through the legacy flat parser.
        let spec = AffinitySpec::parse("ep1=2,3,4,5:ep2=6,7,8,9:proc=10:ctrl=11").unwrap();
        let plan = AffinityPlan::from_spec(&spec);
        let role0 = SubscriptionRole::Main {
            endpoint: EndpointRole::One,
            commitment: Commitment::Processed,
            stream: crate::subscribe::MainStream::Slots,
        };
        assert_eq!(plan.core_for_subscription(role0, 0), Some(2));
        assert_eq!(plan.core_for_subscription(role0, 1), Some(3));
        assert_eq!(plan.core_for_subscription(role0, 3), Some(5));
        // Cycle: idx 4 mod len 4 == 0
        assert_eq!(plan.core_for_subscription(role0, 4), Some(2));
        assert_eq!(plan.processor_core, Some(10));
        assert_eq!(plan.control_core, Some(11));
    }

    #[test]
    fn affinity_plan_core_for_subscription_cycles() {
        let plan = AffinityPlan {
            endpoint1_cores: vec![2, 6],
            endpoint2_cores: vec![3],
            processor_core: None,
            control_core: None,
        };
        let r0 = plan.core_for_subscription(
            SubscriptionRole::Main {
                endpoint: EndpointRole::One,
                commitment: Commitment::Processed,
                stream: crate::subscribe::MainStream::Slots,
            },
            0,
        );
        let r1 = plan.core_for_subscription(
            SubscriptionRole::Main {
                endpoint: EndpointRole::One,
                commitment: Commitment::Confirmed,
                stream: crate::subscribe::MainStream::Slots,
            },
            1,
        );
        let r2 = plan.core_for_subscription(
            SubscriptionRole::Main {
                endpoint: EndpointRole::One,
                commitment: Commitment::Processed,
                stream: crate::subscribe::MainStream::Slots,
            },
            2,
        );
        assert_eq!(r0, Some(2));
        assert_eq!(r1, Some(6));
        assert_eq!(r2, Some(2)); // cycle
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn apply_cpu_affinity_unsupported_off_linux() {
        assert!(matches!(apply_cpu_affinity(0), SchedOutcome::Unsupported));
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn apply_realtime_unsupported_off_linux() {
        assert!(matches!(apply_realtime(), SchedOutcome::Unsupported));
    }

    #[test]
    fn receiver_stats_snapshot_reads_relaxed() {
        let s = ReceiverStats::new();
        s.received.fetch_add(7, Ordering::Relaxed);
        s.dropped.fetch_add(1, Ordering::Relaxed);
        s.decode_errors.fetch_add(2, Ordering::Relaxed);
        s.disconnects.fetch_add(3, Ordering::Relaxed);
        let snap = s.snapshot();
        assert_eq!(snap.received, 7);
        assert_eq!(snap.dropped, 1);
        assert_eq!(snap.decode_errors, 2);
        assert_eq!(snap.disconnects, 3);
    }
}
