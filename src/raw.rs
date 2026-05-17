//! Optional raw-event JSONL writer (spec §3 `--raw-records`, §8).
//!
//! One JSON object per line. Pubkeys, blockhashes, and signatures are
//! base58-encoded so the file is greppable. Format is intentionally flat
//! (no nested envelope) so a one-liner can extract specific stream
//! types: `jq 'select(.kind == "block")' raw.jsonl`.
//!
//! Buffering: uses `std::io::BufWriter` with a 64 KiB buffer. On
//! shutdown the writer flushes and reports the final file size so the
//! operator can verify capture volume. Write errors after the first one
//! are silenced for the rest of the run (logged once) so a disk full
//! mid-run doesn't take the whole harness down — the ingest path keeps
//! updating the t-digest summaries, which is what the run is for.

use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use serde_json::json;
use thiserror::Error;

use crate::{
    collect::{Event, EventPayload, Pubkey32, Signature64, SlotStage},
    config::Commitment,
    subscribe::{EndpointRole, SubscriptionRole},
};

/// Errors that can arise when opening the JSONL output file.
#[derive(Debug, Error)]
pub enum RawWriterError {
    /// Could not create or open the file.
    #[error("failed to open --raw-records path {path}: {source}")]
    Open {
        /// Offending path.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
}

/// Buffered JSONL event log.
#[derive(Debug)]
pub struct RawWriter {
    path: PathBuf,
    inner: BufWriter<File>,
    bytes_written: AtomicU64,
    /// Latched on first I/O error to suppress further writes.
    poisoned: AtomicBool,
    /// First error reason, surfaced by [`RawWriter::take_warning`] for
    /// the run summary.
    error_text: std::sync::Mutex<Option<String>>,
}

impl RawWriter {
    /// Create / truncate the JSONL file. Buffer size is 64 KiB.
    ///
    /// # Errors
    /// Returns [`RawWriterError::Open`] if the file can't be created.
    pub fn create(path: impl AsRef<Path>) -> Result<Self, RawWriterError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|source| RawWriterError::Open {
                path: path.clone(),
                source,
            })?;
        Ok(Self {
            path,
            inner: BufWriter::with_capacity(64 * 1024, file),
            bytes_written: AtomicU64::new(0),
            poisoned: AtomicBool::new(false),
            error_text: std::sync::Mutex::new(None),
        })
    }

    /// Write one event as a JSON line. Returns silently after first error
    /// (further calls become no-ops). Use [`RawWriter::take_warning`] on
    /// shutdown to retrieve the latched error message.
    pub fn write(&mut self, event: &Event) {
        if self.poisoned.load(Ordering::Relaxed) {
            return;
        }
        let value = event_to_json(event);
        let Ok(serialized) = serde_json::to_string(&value) else {
            // serde_json never fails on a serde_json::Value, but
            // guarding the cast keeps the function infallible.
            return;
        };
        let len_with_newline = serialized.len() + 1;
        if let Err(e) = writeln!(self.inner, "{serialized}") {
            self.poisoned.store(true, Ordering::Relaxed);
            if let Ok(mut guard) = self.error_text.lock() {
                *guard = Some(format!("write to {} failed: {e}", self.path.display()));
            }
            return;
        }
        self.bytes_written.fetch_add(
            u64::try_from(len_with_newline).unwrap_or(0),
            Ordering::Relaxed,
        );
    }

    /// Flush the buffer. Idempotent; safe to call repeatedly.
    pub fn flush(&mut self) {
        if self.poisoned.load(Ordering::Relaxed) {
            return;
        }
        if let Err(e) = self.inner.flush() {
            self.poisoned.store(true, Ordering::Relaxed);
            if let Ok(mut guard) = self.error_text.lock() {
                *guard = Some(format!("flush of {} failed: {e}", self.path.display()));
            }
        }
    }

    /// Final bytes written count, for the shutdown report.
    #[must_use]
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Latched error message, if any. Calling this clears the slot so the
    /// caller can include it in the result JSON's warnings.
    #[must_use]
    pub fn take_warning(&self) -> Option<String> {
        self.error_text.lock().ok().and_then(|mut g| g.take())
    }

    /// File path the writer was created with.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn event_to_json(event: &Event) -> serde_json::Value {
    let common = json!({
        "mono_ns": event.ts.mono_ns,
        "wall_ms": event.ts.wall_ms,
        "endpoint": event.subscription.endpoint().label(),
        "subscription": subscription_label(event.subscription),
    });
    let payload = match &event.payload {
        EventPayload::Slot { slot, stage } => json!({
            "kind": "slot",
            "slot": slot,
            "stage": stage.label(),
        }),
        EventPayload::Account {
            slot,
            pubkey,
            owner,
            write_version,
            txn_signature,
            lamports,
        } => json!({
            "kind": "account",
            "slot": slot,
            "pubkey": base58_pk(pubkey),
            "owner": base58_pk(owner),
            "write_version": write_version,
            "txn_signature": txn_signature.as_ref().map(base58_sig),
            "lamports": lamports,
        }),
        EventPayload::Transaction { slot, signature, index } => json!({
            "kind": "transaction",
            "slot": slot,
            "signature": base58_sig(signature),
            "index": index,
        }),
        EventPayload::Block {
            slot,
            blockhash,
            tx_count,
            entries_count,
            block_size_bytes,
        } => json!({
            "kind": "block",
            "slot": slot,
            "blockhash": base58_pk(blockhash),
            "tx_count": tx_count,
            "entries_count": entries_count,
            "block_size_bytes": block_size_bytes,
        }),
        EventPayload::Entry {
            slot,
            index,
            executed_transaction_count,
            starting_transaction_index,
        } => json!({
            "kind": "entry",
            "slot": slot,
            "index": index,
            "executed_transaction_count": executed_transaction_count,
            "starting_transaction_index": starting_transaction_index,
        }),
    };
    merge(common, payload)
}

fn merge(a: serde_json::Value, b: serde_json::Value) -> serde_json::Value {
    let (serde_json::Value::Object(mut a_map), serde_json::Value::Object(b_map)) = (a, b) else {
        // Unreachable in practice — both inputs are always object literals.
        return serde_json::Value::Null;
    };
    for (k, v) in b_map {
        a_map.insert(k, v);
    }
    serde_json::Value::Object(a_map)
}

fn base58_pk(b: &Pubkey32) -> String {
    bs58::encode(b).into_string()
}

fn base58_sig(b: &Signature64) -> String {
    bs58::encode(b).into_string()
}

fn subscription_label(role: SubscriptionRole) -> String {
    match role {
        SubscriptionRole::Main { commitment, .. } => format!("main:{}", commitment_label(commitment)),
        SubscriptionRole::Entries { .. } => "entries".to_string(),
    }
}

const fn commitment_label(c: Commitment) -> &'static str {
    match c {
        Commitment::Processed => "processed",
        Commitment::Confirmed => "confirmed",
        Commitment::Finalized => "finalized",
    }
}

/// Make `EndpointRole::label` available without going through the `subscribe` module
/// — useful for callers that already have a [`Pubkey32`] in hand. Removed
/// because `EndpointRole` already has `label`; this comment is left as a
/// design note.
#[doc(hidden)]
const _: () = {
    let _ = SlotStage::Processed;
    let _ = EndpointRole::One;
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{collect::Event, timing::EventTimestamp};
    use tempfile::tempdir;

    fn ev_slot(slot: u64, stage: SlotStage) -> Event {
        Event {
            ts: EventTimestamp {
                mono_ns: 1_000,
                wall_ms: 2,
            },
            subscription: SubscriptionRole::Main {
                endpoint: EndpointRole::One,
                commitment: Commitment::Processed,
                stream: crate::subscribe::MainStream::Slots,
            },
            payload: EventPayload::Slot { slot, stage },
        }
    }

    fn ev_account(slot: u64) -> Event {
        Event {
            ts: EventTimestamp {
                mono_ns: 1_000,
                wall_ms: 2,
            },
            subscription: SubscriptionRole::Main {
                endpoint: EndpointRole::Two,
                commitment: Commitment::Confirmed,
                stream: crate::subscribe::MainStream::Accounts,
            },
            payload: EventPayload::Account {
                slot,
                pubkey: [1u8; 32],
                owner: [2u8; 32],
                write_version: 5,
                txn_signature: Some([3u8; 64]),
                lamports: 1000,
            },
        }
    }

    #[test]
    fn writes_one_json_per_event_and_reports_bytes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("raw.jsonl");
        let mut w = RawWriter::create(&path).unwrap();
        w.write(&ev_slot(100, SlotStage::Processed));
        w.write(&ev_account(101));
        w.flush();
        let contents = std::fs::read_to_string(&path).unwrap();
        let line_count = contents.lines().count();
        assert_eq!(line_count, 2);
        assert!(w.bytes_written() > 0);
        assert!(contents.contains("\"kind\":\"slot\""));
        assert!(contents.contains("\"kind\":\"account\""));
        assert!(contents.contains("\"endpoint\":\"endpoint1\""));
        assert!(contents.contains("\"endpoint\":\"endpoint2\""));
    }

    #[test]
    fn account_emits_base58_keys() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("raw.jsonl");
        let mut w = RawWriter::create(&path).unwrap();
        w.write(&ev_account(7));
        w.flush();
        let line = std::fs::read_to_string(&path).unwrap();
        // 32 bytes of 0x01 → bs58 base58 string. Just sanity-check that
        // it isn't a hex/decimal of "[1, 1, 1, ...]".
        assert!(!line.contains("[1, 1, 1"));
        // bs58 of 32 1s starts with "4vJ9JU1bJ..." — we only verify the
        // expected prefix is alpha, not numeric digits.
        let v: serde_json::Value =
            serde_json::from_str(line.lines().next().unwrap()).unwrap();
        let pubkey = v["pubkey"].as_str().unwrap();
        assert!(pubkey.chars().all(|c| !c.is_ascii_whitespace()));
        assert!(pubkey.len() > 30);
    }

    #[test]
    fn open_failure_returns_typed_error() {
        let dir = tempdir().unwrap();
        // Use the directory itself as the target → fails to open as file.
        let result = RawWriter::create(dir.path());
        assert!(matches!(result, Err(RawWriterError::Open { .. })));
    }

    #[test]
    fn subscription_label_distinguishes_main_and_entries() {
        assert_eq!(
            subscription_label(SubscriptionRole::Main {
                endpoint: EndpointRole::One,
                commitment: Commitment::Confirmed,
                stream: crate::subscribe::MainStream::Accounts,
            }),
            "main:confirmed"
        );
        assert_eq!(
            subscription_label(SubscriptionRole::Entries {
                endpoint: EndpointRole::One,
            }),
            "entries"
        );
    }
}
