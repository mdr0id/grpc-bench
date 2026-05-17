//! Assemble the §8 output JSON.
//!
//! [`RunOutput`] is the root type. It composes [`HostMetadata`],
//! [`ProtoMetadata`], a redacted config echo, the program list, the
//! `metadata` block (totals + duration), per-endpoint capture info, the
//! `comparative` block (from matchers), the `per_program_account_delay`
//! block (from the accounts matcher), the `cross_stream` block (Phase
//! 2j), and the `stability` block (Phase 2i).
//!
//! Spec compatibility: the top-level shape mirrors thorofare's so the
//! sibling `summarize.py` reads it for the fields it knows about and
//! ignores the new ones gracefully (acceptance criterion §9.10).

use std::collections::HashMap;

use serde::Serialize;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

use crate::{
    collect::AffinitySpec,
    config::{Allocator, Commitment, Config, StopCondition},
    crossstream::CrossStreamSummary,
    env::HostMetadata,
    matching::{accounts::PerProgramSummary, slots::SlotStatusSummary, LatencyDigestSummary},
    programs::ProgramEntry,
    proto::ProtoMetadata,
    stability::StabilitySummary,
    subscribe::EndpointRole,
};

/// Auto-detect Quicknode endpoints by URL substring (spec §3). When
/// neither URL matches, endpoint2 is assumed to be Quicknode.
#[must_use]
pub fn detect_qn_role(endpoint1_url: &str, endpoint2_url: Option<&str>) -> QnRoleHint {
    let ep1_qn = is_quicknode(endpoint1_url);
    let ep2_qn = endpoint2_url.is_some_and(is_quicknode);
    match (ep1_qn, ep2_qn) {
        (true, false) => QnRoleHint::Endpoint1,
        (true, true) => QnRoleHint::Both,
        // (false, true) and (false, false) both resolve to endpoint2 —
        // explicitly when detected, by spec default when neither matches.
        _ => QnRoleHint::Endpoint2,
    }
}

fn is_quicknode(url: &str) -> bool {
    let lower = url.to_ascii_lowercase();
    lower.contains("quiknode") || lower.contains("quicknode")
}

/// Where Quicknode lives in this run, used by downstream tooling to label
/// `comparative` deltas as `qn_vs_other` etc.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QnRoleHint {
    /// Endpoint1 detected as QN.
    Endpoint1,
    /// Endpoint2 detected as QN.
    Endpoint2,
    /// Both endpoints look like QN.
    Both,
}

/// Config-echo (spec §8 `config`) with x-tokens redacted.
//
// Mirrors `Config` and inherits the same independent-toggle bools.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Serialize)]
pub struct ConfigEcho {
    /// Reference endpoint URL.
    pub endpoint1: String,
    /// SUT endpoint URL. `None` when `--solo` is set.
    pub endpoint2: Option<String>,
    /// Optional entries-only URL for endpoint1.
    pub entries_endpoint1: Option<String>,
    /// Optional entries-only URL for endpoint2.
    pub entries_endpoint2: Option<String>,
    /// Stop condition.
    pub stop: StopCondition,
    /// Commitment levels subscribed to.
    pub commitments: Vec<Commitment>,
    /// `--with-blocks`.
    pub with_blocks: bool,
    /// `--with-transactions`.
    pub with_transactions: bool,
    /// Reconnect-test interval if set.
    pub reconnect_test_secs: Option<u64>,
    /// Flattened union of cores referenced by the affinity plan
    /// (backwards-compatible echo for legacy consumers).
    pub cpu_affinity: Vec<u32>,
    /// Structured affinity layout — skipped when no pin requested.
    #[serde(skip_serializing_if = "AffinitySpec::is_empty")]
    pub cpu_affinity_plan: AffinitySpec,
    /// `--realtime` requested.
    pub realtime: bool,
    /// Allocator.
    pub allocator: Allocator,
    /// Auto-detected QN role.
    pub qn_role: QnRoleHint,
    /// Whether the run was in `--solo` (single-endpoint) mode.
    pub solo: bool,
}

impl ConfigEcho {
    /// Build from a validated [`Config`]. Tokens are not included.
    #[must_use]
    pub fn from_config(c: &Config) -> Self {
        let ep2_url = c.endpoint2.as_ref().map(|e| e.url.clone());
        let qn_role = detect_qn_role(&c.endpoint1.url, ep2_url.as_deref());
        Self {
            endpoint1: c.endpoint1.url.clone(),
            endpoint2: ep2_url,
            entries_endpoint1: c.entries_endpoint1.as_ref().map(|e| e.url.clone()),
            entries_endpoint2: c.entries_endpoint2.as_ref().map(|e| e.url.clone()),
            stop: c.stop,
            commitments: c.commitments.clone(),
            with_blocks: c.with_blocks,
            with_transactions: c.with_transactions,
            reconnect_test_secs: c.reconnect_test_secs,
            cpu_affinity: c.cpu_affinity.all_cores(),
            cpu_affinity_plan: c.cpu_affinity.clone(),
            realtime: c.realtime,
            allocator: c.allocator,
            qn_role,
            solo: c.solo,
        }
    }
}

/// Top-level `metadata` block.
#[derive(Debug, Clone, Default, Serialize)]
pub struct RunMetadata {
    /// Total slot-status events on the reference endpoint, used by the
    /// `--slots` stop counter.
    pub total_slots_collected: u64,
    /// Slots seen on both endpoints (rough capture parity proxy).
    pub common_slots: u64,
    /// Wall-clock duration of the run in milliseconds.
    pub duration_ms: u64,
    /// `[endpoint1_total, endpoint2_total]` account updates.
    pub total_account_updates: [u64; 2],
    /// Transaction updates per endpoint.
    pub total_transaction_updates: [u64; 2],
    /// Block updates per endpoint.
    pub total_block_updates: [u64; 2],
    /// Entry updates per endpoint.
    pub total_entry_updates: [u64; 2],
    /// Dropped events on the endpoint1 ring (overflow).
    pub dropped_events_ep1: u64,
    /// Dropped events on the endpoint2 ring.
    pub dropped_events_ep2: u64,
}

/// Per-endpoint capture / capability info (spec §8 `endpoints`).
#[derive(Debug, Clone, Serialize)]
pub struct EndpointInfo {
    /// URL.
    pub endpoint: String,
    /// `comparison`, `sut`, or `entries-only`.
    pub role: &'static str,
    /// `yellowstone` / `richat` / `laserstream` / `entries`.
    pub plugin_type: String,
    /// Plugin version reported by `GetVersion`.
    pub plugin_version: String,
    /// TCP-level RTT in ms, averaged across the run's pings (spec §6.5).
    pub avg_ping_ms: f64,
    /// Total event count across all streams on this endpoint.
    pub total_updates: u64,
    /// Distinct slots seen.
    pub unique_slots: u64,
}

/// `comparative.*` block.
#[derive(Debug, Clone, Serialize)]
pub struct ComparativeSummary {
    /// `comparative.slot_status`.
    pub slot_status: SlotStatusSummary,
    /// `comparative.account_delay`.
    pub account_delay: LatencyDigestSummary,
    /// `comparative.transaction_delay`. `None` when `--with-transactions`
    /// was off.
    pub transaction_delay: Option<LatencyDigestSummary>,
    /// `comparative.block_delay`. `None` when `--with-blocks` was off.
    pub block_delay: Option<LatencyDigestSummary>,
}

/// §8 root output JSON. Field order in the struct matches the spec's
/// schema sample so the result is greppable.
#[derive(Debug, Clone, Serialize)]
pub struct RunOutput {
    /// Harness crate version.
    pub version: String,
    /// Static harness name (always `"grpc-bench"`).
    pub harness: &'static str,
    /// Wall-clock start, ms since epoch.
    pub run_started_wall_ms: u64,
    /// Wall-clock start, RFC 3339.
    pub run_started_iso: String,
    /// Host metadata block.
    pub host_metadata: HostMetadata,
    /// Proto-version metadata block.
    pub proto_metadata: ProtoMetadata,
    /// Config echo.
    pub config: ConfigEcho,
    /// Programs loaded from the TSV.
    pub programs: Vec<ProgramEntry>,
    /// Aggregate metadata.
    pub metadata: RunMetadata,
    /// Per-endpoint capability/capture info.
    pub endpoints: Vec<EndpointInfo>,
    /// Comparative latencies (spec §6.1).
    pub comparative: ComparativeSummary,
    /// Per-program account latency buckets (spec §6.2).
    pub per_program_account_delay: PerProgramSummary,
    /// Cross-stream ordering within each endpoint (spec §6.3).
    pub cross_stream: HashMap<&'static str, CrossStreamSummary>,
    /// Stream stability per endpoint (spec §6.4).
    pub stability: HashMap<&'static str, StabilitySummary>,
}

impl RunOutput {
    /// Compute the wall-clock RFC 3339 string for a `wall_ms` value.
    /// Exposed publicly so the bin can capture start/end timestamps with
    /// the same formatter the JSON uses.
    #[must_use]
    pub fn rfc3339_from_wall_ms(wall_ms: u64) -> String {
        // OffsetDateTime::from_unix_timestamp_nanos accepts i128.
        let nanos = i128::from(wall_ms) * 1_000_000;
        OffsetDateTime::from_unix_timestamp_nanos(nanos)
            .ok()
            .and_then(|odt| odt.format(&Rfc3339).ok())
            .unwrap_or_default()
    }

    /// Endpoint-role label for the `endpoints[].role` field. Mirrors the
    /// thorofare convention: `comparison` for endpoint1, `sut` for
    /// endpoint2, `entries-only` for an entries subscription's URL.
    #[must_use]
    pub fn role_label(role: EndpointRole) -> &'static str {
        match role {
            EndpointRole::One => "comparison",
            EndpointRole::Two => "sut",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{Cli, TomlOverlay},
        crossstream::CrossStreamSummary,
        env,
        proto,
        stability::StabilitySummary,
    };
    use std::path::PathBuf;

    fn base_cli() -> Cli {
        Cli {
            endpoint1: "https://quicknode.foo:10000".into(),
            x_token1: "tok-redact".into(),
            endpoint1_tls: false,
            endpoint2: Some("https://other.bar:10000".into()),
            x_token2: Some("tok-redact-too".into()),
            endpoint2_tls: false,
            solo: false,
            entries_endpoint1: None,
            entries_x_token1: None,
            entries_endpoint1_tls: false,
            entries_endpoint2: None,
            entries_x_token2: None,
            entries_endpoint2_tls: false,
            programs: PathBuf::from("programs.tsv"),
            slots: Some(1000),
            duration: None,
            commitment: vec![Commitment::Processed, Commitment::Confirmed],
            with_blocks: false,
            with_transactions: false,
            strict_account_key: false,
            accounts_programs_per_filter: 1,
            reconnect_test: None,
            output: PathBuf::from("out.json"),
            raw_records: None,
            config: None,
            cpu_affinity: None,
            realtime: false,
            allocator: Allocator::Jemalloc,
            log_level: "info".into(),
            solo_streams: vec![crate::subscribe::MainStream::Slots],
            max_decode_mb: 256,
            ring_capacity: crate::run::DEFAULT_RING_CAPACITY,
        }
    }

    #[test]
    fn qn_detect_endpoint1_first() {
        assert_eq!(
            detect_qn_role("https://x.quiknode.pro", Some("https://other")),
            QnRoleHint::Endpoint1
        );
    }

    #[test]
    fn qn_detect_endpoint2_first() {
        assert_eq!(
            detect_qn_role("https://other", Some("https://x.quicknode.pro")),
            QnRoleHint::Endpoint2
        );
    }

    #[test]
    fn qn_detect_both() {
        assert_eq!(
            detect_qn_role("https://x.quiknode.pro", Some("https://y.quicknode.pro")),
            QnRoleHint::Both
        );
    }

    #[test]
    fn qn_detect_neither_defaults_to_endpoint2() {
        assert_eq!(
            detect_qn_role("https://helius.x", Some("https://other.y")),
            QnRoleHint::Endpoint2
        );
    }

    #[test]
    fn qn_detect_solo_endpoint1_is_qn() {
        assert_eq!(
            detect_qn_role("https://x.quiknode.pro", None),
            QnRoleHint::Endpoint1
        );
    }

    #[test]
    fn qn_detect_solo_endpoint1_not_qn_defaults_to_endpoint2() {
        // Solo mode + non-QN ep1 → role defaults to endpoint2 (which is
        // absent); downstream consumers check `config.solo` before
        // interpreting `qn_role` for comparative labelling.
        assert_eq!(
            detect_qn_role("https://helius.x", None),
            QnRoleHint::Endpoint2
        );
    }

    #[test]
    fn config_echo_does_not_serialize_tokens() {
        let cli = base_cli();
        let cfg = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap();
        let echo = ConfigEcho::from_config(&cfg);
        let json = serde_json::to_string(&echo).unwrap();
        assert!(!json.contains("tok-redact"));
        assert!(!json.contains("tok-redact-too"));
        assert!(json.contains("quicknode.foo"));
    }

    #[test]
    fn rfc3339_known_epoch_round_trips() {
        // 2024-01-01T00:00:00Z = 1704067200000 ms (sanity-checked via
        // `date -u -d "2024-01-01 00:00:00 UTC" +%s` → 1704067200).
        let s = RunOutput::rfc3339_from_wall_ms(1_704_067_200_000);
        assert!(s.starts_with("2024-01-01T00:00:00"), "rfc3339 was {s:?}");
    }

    #[test]
    fn run_output_serializes_to_valid_json() {
        let cli = base_cli();
        let cfg = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap();
        let host = env::collect(false, false, false, AffinitySpec::default());
        let proto_meta = proto::ProtoMetadata::from_endpoints(
            &proto::evaluate(r#"{"package":"yellowstone-grpc-geyser","version":"12.3.0","proto":"12.3.0"}"#),
            &proto::evaluate(r#"{"package":"richat","version":"2.2.0","proto":"12.3.0"}"#),
        );
        let comparative = ComparativeSummary {
            slot_status: crate::matching::slots::SlotMatcher::new(
                crate::matching::EvictionPolicy::SlotWindow { window: 64 },
            )
            .summary(),
            account_delay: crate::matching::LatencyDigestSummary {
                p50: f64::NAN,
                p90: f64::NAN,
                p99: f64::NAN,
                p99_9: f64::NAN,
                matched: 0,
                ep1_faster: 0,
                ep2_faster: 0,
            },
            transaction_delay: None,
            block_delay: None,
        };
        let mut cross = HashMap::new();
        cross.insert("endpoint1", CrossStreamSummary::empty());
        cross.insert("endpoint2", CrossStreamSummary::empty());
        let mut stab = HashMap::new();
        stab.insert("endpoint1", StabilitySummary::empty());
        stab.insert("endpoint2", StabilitySummary::empty());
        let out = RunOutput {
            version: env!("CARGO_PKG_VERSION").to_string(),
            harness: "grpc-bench",
            run_started_wall_ms: 1_700_000_000_000,
            run_started_iso: RunOutput::rfc3339_from_wall_ms(1_700_000_000_000),
            host_metadata: host,
            proto_metadata: proto_meta,
            config: ConfigEcho::from_config(&cfg),
            programs: vec![],
            metadata: RunMetadata::default(),
            endpoints: vec![],
            comparative,
            per_program_account_delay: PerProgramSummary(HashMap::new()),
            cross_stream: cross,
            stability: stab,
        };
        let json = serde_json::to_value(&out).expect("serialize");
        assert_eq!(json["harness"], "grpc-bench");
        assert_eq!(json["proto_metadata"]["yellowstone_proto_crate_version"], proto::HARNESS_PROTO_CRATE_VERSION);
        // NaN must serialize to `null` per serde_json's f64::NAN handling
        // (the schema permits null for empty digests).
        assert!(json["comparative"]["account_delay"]["p50"].is_null());
    }
}
