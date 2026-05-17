//! Subscription planning and `SubscribeRequest` construction (spec §4).
//!
//! The harness opens several gRPC subscriptions per run:
//!
//! - Per endpoint × per commitment: one main `Subscribe` stream carrying
//!   slots, accounts, and (optionally) transactions and blocks.
//! - Per endpoint with `--entries-endpointN`: one entries-only `Subscribe`
//!   stream against the supplied URL. Per Phase 1 resolution, entries are
//!   delivered via the standard Yellowstone `entry` filter — there is no
//!   separate proto. The "entries endpoint" is therefore just an
//!   independent URL/credential pair for the same `Subscribe` RPC, with
//!   only the entry filter active.
//!
//! [`SubscriptionPlan::from_run_config`] enumerates these streams from a
//! [`Config`][`crate::config::Config`] plus the loaded
//! [`ProgramSet`][`crate::programs::ProgramSet`]. Each entry in the plan
//! is a [`SubscriptionSpec`] containing both the destination
//! [`EndpointSpec`] and the prebuilt
//! [`SubscribeRequest`] body, ready to hand to
//! [`yellowstone::open_subscription`].

// `SubscribeRequest` carries `HashMap<String, SubscribeRequestFilterEntry>`
// and `...FilterBlocksMeta`, both of which are zero-sized messages.
// clippy::zero_sized_map_values flags those as "use a set instead", but
// the wire shape is dictated by the proto definition — we don't get to
// pick.
#![allow(clippy::zero_sized_map_values)]

pub mod yellowstone;

use std::collections::HashMap;

use clap::ValueEnum;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel as YsCommitment, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterBlocks, SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
    SubscribeRequestFilterTransactions,
};

use crate::{
    config::{Commitment, Config, EndpointSpec},
    programs::ProgramSet,
};

/// Solana programs whose account-update rates always dominate any
/// multi-program accounts filter they share. See the
/// `receiver-multifilter-inflation` finding (2026-05-16): combining
/// these programs with anything else inflates p50 100×–500× server-side.
///
/// They are split into their own sub-subscription regardless of the
/// operator's `--accounts-programs-per-filter` setting so a relaxed
/// chunk size (e.g. 4) can still safely apply to the long tail of
/// less-busy programs without contaminating system / token measurements.
pub const KNOWN_HEAVY_PROGRAMS: &[&str] = &[
    // System program — every transaction touches it for fees / rent.
    "11111111111111111111111111111111",
    // SPL Token — the dominant token program.
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    // Token-2022 — successor token program, increasingly hot.
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
];

/// Whether the given program id is on the always-split list.
#[must_use]
pub fn is_known_heavy_program(program_id: &str) -> bool {
    KNOWN_HEAVY_PROGRAMS.contains(&program_id)
}

/// Stable filter names used by the harness. Kept as constants because the
/// match layer () keys metrics on these names when correlating
/// events across endpoints.
pub mod filter_names {
    /// Slot-status filter; spec §4.
    pub const SLOTS: &str = "slots-all";
    /// Accounts filter; spec §4.
    pub const ACCOUNTS: &str = "all-programs";
    /// Transactions filter; spec §4.
    pub const TRANSACTIONS: &str = "all-programs-tx";
    /// Blocks filter; spec §4.
    pub const BLOCKS: &str = "all-blocks";
    /// Entries filter (Phase 1: standard Yellowstone entry, not a Quicknode
    /// extension).
    pub const ENTRIES: &str = "all-entries";
}

/// Which endpoint a subscription belongs to. The summary JSON uses these
/// labels in `comparative.*` and `stability.*` blocks (the output JSON schema).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EndpointRole {
    /// `--endpoint1`. Comparison reference.
    One,
    /// `--endpoint2`. System under test.
    Two,
}

impl EndpointRole {
    /// Stable string label used in JSON keys.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::One => "endpoint1",
            Self::Two => "endpoint2",
        }
    }
}

/// Which §4 filter type a "main" subscription is carrying.
///
/// Each `Subscribe` gRPC request carries exactly one filter type. Some
/// server tiers cap the per-request filter count at 1, so the harness
/// opens a separate stream per filter type rather than bundling them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ValueEnum, serde::Serialize)]
#[clap(rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum MainStream {
    /// `slots-all` filter — slot status events.
    Slots,
    /// `all-programs` filter — account updates owned by any of the
    /// configured programs.
    Accounts,
    /// `all-programs-tx` filter — only present when `--with-transactions`
    /// is set.
    Transactions,
    /// `all-blocks` filter — only present when `--with-blocks` is set.
    Blocks,
}

impl MainStream {
    /// Stable lowercase tag used in JSON labels and log lines.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Slots => "slots",
            Self::Accounts => "accounts",
            Self::Transactions => "transactions",
            Self::Blocks => "blocks",
        }
    }
}

/// Distinguishes a main per-commitment subscription from an entries-only
/// subscription targeting a possibly-different URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionRole {
    /// Standard main subscription carrying exactly one filter type for a
    /// given `(endpoint, commitment)` pair.
    Main {
        /// Endpoint identity.
        endpoint: EndpointRole,
        /// Commitment level for this stream.
        commitment: Commitment,
        /// Which filter type this subscription is dedicated to.
        stream: MainStream,
    },
    /// Dedicated entries-only subscription (a separate URL/credential
    /// per `--entries-endpointN`). Commitment is taken from the first
    /// configured commitment because the standard entry filter is not
    /// commitment-tagged.
    Entries {
        /// Which endpoint this entries stream belongs to (uses the same
        /// `endpoint1`/`endpoint2` role label so downstream metrics share
        /// the comparison axis).
        endpoint: EndpointRole,
    },
}

impl SubscriptionRole {
    /// Whether the role represents an entries-only subscription.
    #[must_use]
    pub fn is_entries(self) -> bool {
        matches!(self, Self::Entries { .. })
    }

    /// Endpoint axis for this subscription.
    #[must_use]
    pub fn endpoint(self) -> EndpointRole {
        match self {
            Self::Main { endpoint, .. } | Self::Entries { endpoint } => endpoint,
        }
    }
}

/// One subscription the harness will open.
#[derive(Debug, Clone)]
pub struct SubscriptionSpec {
    /// What this subscription represents.
    pub role: SubscriptionRole,
    /// Destination endpoint (URL + x-token).
    pub endpoint: EndpointSpec,
    /// Prebuilt `SubscribeRequest` body. Cloned into the gRPC sink at
    /// connect time.
    pub request: SubscribeRequest,
}

/// Aggregate of all subscriptions for one run.
#[derive(Debug, Clone)]
pub struct SubscriptionPlan {
    /// Subscriptions in deterministic order: endpoint1 mains (commitment
    /// ascending), endpoint2 mains, then entries (endpoint1, endpoint2).
    /// Determinism matters because the runtime's `Barrier` uses the plan
    /// length and ordering for stagger reporting.
    pub specs: Vec<SubscriptionSpec>,
}

impl SubscriptionPlan {
    /// Build a plan from a validated [`Config`] and loaded [`ProgramSet`].
    ///
    /// Returns an empty plan only if the supplied config has zero
    /// commitments configured and no entries endpoints, which is
    /// already rejected upstream by [`Config::from_cli`]
    /// ([`crate::config::ConfigError::EmptyCommitment`]). The function
    /// itself does not fail.
    // Orchestration entry point: enumerates every subscription the run
    // will open (solo path, slot-per-endpoint, per-commitment accounts
    // chunks with KNOWN_HEAVY_PROGRAMS partition, optional tx/blocks,
    // optional entries endpoints). Splitting into smaller helpers would
    // hide the deterministic spec-ordering invariant.
    #[allow(clippy::too_many_lines)]
    #[must_use]
    pub fn from_run_config(config: &Config, programs: &ProgramSet) -> Self {
        let mut specs: Vec<SubscriptionSpec> = Vec::new();
        let program_ids = programs.program_ids();

        // In --solo mode the plan opens only the streams listed in
        // --solo-streams on endpoint1, at the first configured commitment
        // (default `processed`). Lets the operator size the filter
        // pressure to whatever the QN tier permits.
        if config.solo {
            let commitment = config.commitments.first().copied().unwrap_or(Commitment::Processed);
            // Deduplicate while preserving order so the result JSON's
            // endpoint subscription order is deterministic.
            let mut seen: std::collections::HashSet<MainStream> = std::collections::HashSet::new();
            for stream in &config.solo_streams {
                if !seen.insert(*stream) {
                    continue;
                }
                let request = build_main_request_for(commitment, &program_ids, *stream);
                specs.push(SubscriptionSpec {
                    role: SubscriptionRole::Main {
                        endpoint: EndpointRole::One,
                        commitment,
                        stream: *stream,
                    },
                    endpoint: config.endpoint1.clone(),
                    request,
                });
            }
            return Self { specs };
        }

        // Slot delivery is commitment-agnostic when
        // `filter_by_commitment=false` + `interslot_updates=true` are
        // both set: the single subscription receives all 6 stages.
        // Opening one slot subscription per commitment (a) duplicates
        // events, which `PairMatcher.observe` resolves by overwriting
        // pending state — pushing recorded timestamps later than the
        // true wire arrival — and (b) setting the top-level
        // `commitment` field on the slot request introduces a small
        // server-side ordering delay (~4ms vs thorofare measured
        // 2026-05-16). So we emit exactly one slot subscription per
        // endpoint with `commitment=None`, matching thorofare's
        // topology. The role's `commitment` label takes the first
        // configured value for downstream tag consistency.
        let slot_label_commitment = config
            .commitments
            .first()
            .copied()
            .unwrap_or(Commitment::Processed);

        for endpoint_role in [EndpointRole::One, EndpointRole::Two] {
            let endpoint_spec = match endpoint_role {
                EndpointRole::One => Some(&config.endpoint1),
                EndpointRole::Two => config.endpoint2.as_ref(),
            };
            let Some(endpoint_spec) = endpoint_spec else {
                continue; // endpoint2 absent in solo handled above; defensive here.
            };

            // Single slot subscription per endpoint, commitment=None
            // on the request body.
            specs.push(SubscriptionSpec {
                role: SubscriptionRole::Main {
                    endpoint: endpoint_role,
                    commitment: slot_label_commitment,
                    stream: MainStream::Slots,
                },
                endpoint: endpoint_spec.clone(),
                request: build_slot_request(),
            });

            // Per-commitment subscriptions for accounts and the
            // optional transactions / blocks streams.
            //
            // Accounts is chunked into sub-filters of
            // `config.accounts_programs_per_filter` programs each.
            // Measured 2026-05-16 (see Cli doc): server-side
            // multi-program filter matching adds severe per-event
            // latency to the cross-endpoint deltas at customer-scale
            // volume — 23 programs in one filter inflated system's
            // p50 by ~400× vs the same program in a single-program
            // filter. Splitting moves each sub-receiver below the
            // inflation threshold. Default `chunk_size = 1` is the
            // most accurate; raise it to reduce connection count.
            //
            // Transactions and Blocks are not chunked. Tx measured
            // cleanly at p50 ~7ms in the 23p run, so the filter
            // complexity issue is specifically the accounts filter
            // semantics, not multi-program filtering generically.
            let chunk_size = config.accounts_programs_per_filter.max(1);
            // Partition the program list into known-heavy programs (each
            // gets its own dedicated sub-subscription regardless of
            // chunk_size — see KNOWN_HEAVY_PROGRAMS) and the rest, which
            // are chunked normally. Order is preserved within each
            // partition so the result JSON's spec order stays
            // deterministic (`itertools::Itertools::partition` would
            // also work, but `Iterator::partition` is in std and
            // preserves order).
            let (heavy_programs, rest_programs): (Vec<String>, Vec<String>) = program_ids
                .iter()
                .cloned()
                .partition(|p| is_known_heavy_program(p));
            for commitment in &config.commitments {
                for heavy in &heavy_programs {
                    let single = std::slice::from_ref(heavy);
                    let request =
                        build_main_request_for(*commitment, single, MainStream::Accounts);
                    specs.push(SubscriptionSpec {
                        role: SubscriptionRole::Main {
                            endpoint: endpoint_role,
                            commitment: *commitment,
                            stream: MainStream::Accounts,
                        },
                        endpoint: endpoint_spec.clone(),
                        request,
                    });
                }
                for chunk in rest_programs.chunks(chunk_size) {
                    let request =
                        build_main_request_for(*commitment, chunk, MainStream::Accounts);
                    specs.push(SubscriptionSpec {
                        role: SubscriptionRole::Main {
                            endpoint: endpoint_role,
                            commitment: *commitment,
                            stream: MainStream::Accounts,
                        },
                        endpoint: endpoint_spec.clone(),
                        request,
                    });
                }

                let mut other_streams: Vec<MainStream> = Vec::new();
                if config.with_transactions {
                    other_streams.push(MainStream::Transactions);
                }
                if config.with_blocks {
                    other_streams.push(MainStream::Blocks);
                }
                for stream in other_streams {
                    let request = build_main_request_for(*commitment, &program_ids, stream);
                    specs.push(SubscriptionSpec {
                        role: SubscriptionRole::Main {
                            endpoint: endpoint_role,
                            commitment: *commitment,
                            stream,
                        },
                        endpoint: endpoint_spec.clone(),
                        request,
                    });
                }
            }
        }

        // Entries subscriptions are independent: they share endpoint role
        // labels with the main subscriptions but live on whatever URL
        // `--entries-endpointN` supplied. We use the first configured
        // commitment for slot-tagging coherence.
        let entries_commitment = config
            .commitments
            .first()
            .copied()
            .unwrap_or(Commitment::Processed);
        for (endpoint_role, entries_spec) in [
            (EndpointRole::One, config.entries_endpoint1.as_ref()),
            (EndpointRole::Two, config.entries_endpoint2.as_ref()),
        ] {
            if let Some(ep) = entries_spec {
                let request = build_entries_request(entries_commitment);
                specs.push(SubscriptionSpec {
                    role: SubscriptionRole::Entries {
                        endpoint: endpoint_role,
                    },
                    endpoint: ep.clone(),
                    request,
                });
            }
        }

        Self { specs }
    }
}

/// Build a `SubscribeRequest` carrying exactly one filter for the named
/// stream. Spec §4 calls for slot status, account, optional transaction,
/// and optional block filters — we emit each as its own request because
/// many provider tiers cap the per-request filter count at 1.
///
/// - `MainStream::Slots`        → `slots["slots-all"]` with
///   `filter_by_commitment = false`.
/// - `MainStream::Accounts`     → `accounts["all-programs"]` with
///   `owner = <program_ids>`.
/// - `MainStream::Transactions` → `transactions["all-programs-tx"]` with
///   `account_include = <program_ids>`, `failed = false`, `vote = false`.
/// - `MainStream::Blocks`       → `blocks["all-blocks"]` with
///   `account_include = []` (full blocks).
/// Build a slot-only `SubscribeRequest` with `commitment=None` at the
/// top level (deliberate — see the call site comment in
/// `SubscriptionPlan::from_run_config`). The slot filter sets both
/// `filter_by_commitment=false` and `interslot_updates=true` so every
/// slot stage crosses the wire.
fn build_slot_request() -> SubscribeRequest {
    let mut slots: HashMap<String, SubscribeRequestFilterSlots> = HashMap::new();
    slots.insert(
        filter_names::SLOTS.to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(false),
            interslot_updates: Some(true),
        },
    );
    SubscribeRequest {
        accounts: HashMap::new(),
        slots,
        transactions: HashMap::new(),
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
        commitment: None,
        accounts_data_slice: Vec::new(),
        ping: None,
        from_slot: None,
    }
}

fn build_main_request_for(
    commitment: Commitment,
    program_ids: &[String],
    stream: MainStream,
) -> SubscribeRequest {
    let mut accounts: HashMap<String, SubscribeRequestFilterAccounts> = HashMap::new();
    let mut slots: HashMap<String, SubscribeRequestFilterSlots> = HashMap::new();
    let mut transactions: HashMap<String, SubscribeRequestFilterTransactions> = HashMap::new();
    let mut blocks: HashMap<String, SubscribeRequestFilterBlocks> = HashMap::new();

    match stream {
        MainStream::Slots => {
            // Spec §4: `filter_by_commitment = false` so all stages
            // are visible regardless of the subscription's commitment
            // level. Additionally `interslot_updates = true` is
            // required to actually receive the pre-commitment stages
            // (FirstShredReceived, Completed, CreatedBank), which the
            // server treats as "interslot" — fired between the
            // commitment-stage events. Without both flags set the
            // server silently drops those stages, leaving grpc-bench
            // with matched=0 on first_shred/completed/created_bank
            // even though thorofare reports them; validated against
            // thorofare 2026-05-16.
            slots.insert(
                filter_names::SLOTS.to_string(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(false),
                    interslot_updates: Some(true),
                },
            );
        }
        MainStream::Accounts => {
            accounts.insert(
                filter_names::ACCOUNTS.to_string(),
                SubscribeRequestFilterAccounts {
                    account: Vec::new(),
                    owner: program_ids.to_vec(),
                    filters: Vec::new(),
                    nonempty_txn_signature: None,
                },
            );
        }
        MainStream::Transactions => {
            transactions.insert(
                filter_names::TRANSACTIONS.to_string(),
                SubscribeRequestFilterTransactions {
                    vote: Some(false),
                    failed: Some(false),
                    signature: None,
                    account_include: program_ids.to_vec(),
                    account_exclude: Vec::new(),
                    account_required: Vec::new(),
                },
            );
        }
        MainStream::Blocks => {
            blocks.insert(
                filter_names::BLOCKS.to_string(),
                SubscribeRequestFilterBlocks {
                    account_include: Vec::new(),
                    include_transactions: Some(true),
                    include_accounts: Some(false),
                    include_entries: Some(false),
                },
            );
        }
    }

    SubscribeRequest {
        accounts,
        slots,
        transactions,
        transactions_status: HashMap::new(),
        blocks,
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
        commitment: Some(to_ys_commitment(commitment) as i32),
        accounts_data_slice: Vec::new(),
        ping: None,
        from_slot: None,
    }
}

/// Build an entries-only `SubscribeRequest`. The standard
/// `SubscribeRequestFilterEntry` is empty (no per-entry filter knobs), so
/// the request body has the slot filter active for correlation purposes
/// and the entry filter present with a single key.
fn build_entries_request(commitment: Commitment) -> SubscribeRequest {
    let mut slots: HashMap<String, SubscribeRequestFilterSlots> = HashMap::with_capacity(1);
    slots.insert(
        filter_names::SLOTS.to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(false),
            interslot_updates: Some(false),
        },
    );
    let mut entry: HashMap<String, SubscribeRequestFilterEntry> = HashMap::with_capacity(1);
    entry.insert(filter_names::ENTRIES.to_string(), SubscribeRequestFilterEntry {});

    SubscribeRequest {
        accounts: HashMap::new(),
        slots,
        transactions: HashMap::new(),
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        entry,
        commitment: Some(to_ys_commitment(commitment) as i32),
        accounts_data_slice: Vec::new(),
        ping: None,
        from_slot: None,
    }
}

fn to_ys_commitment(c: Commitment) -> YsCommitment {
    match c {
        Commitment::Processed => YsCommitment::Processed,
        Commitment::Confirmed => YsCommitment::Confirmed,
        Commitment::Finalized => YsCommitment::Finalized,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Allocator, Cli, TomlOverlay};
    use crate::programs::ProgramSet;
    use std::io::Cursor;
    use std::path::PathBuf;

    fn small_program_set() -> ProgramSet {
        // Both entries are on the KNOWN_HEAVY_PROGRAMS list, so they
        // always split into their own per-commitment sub-subscription
        // regardless of `--accounts-programs-per-filter`. This matches
        // the customer's real production topology (both programs are
        // measured separately in practice).
        let src = "system\t11111111111111111111111111111111\tSystem\n\
                   spl_token\tTokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\tSPL Token\n";
        ProgramSet::from_reader(PathBuf::from("test.tsv"), Cursor::new(src)).unwrap()
    }

    /// Two non-heavy program ids, used to exercise the rest-program
    /// chunking math (heavy programs always split out and so can't
    /// exercise different chunk sizes).
    fn small_non_heavy_program_set() -> ProgramSet {
        let src = "raydium\t675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8\tRaydium AMM V4\n\
                   meteora\tLBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo\tMeteora DLMM\n";
        ProgramSet::from_reader(PathBuf::from("non_heavy.tsv"), Cursor::new(src)).unwrap()
    }

    fn base_cli() -> Cli {
        Cli {
            endpoint1: "host1:10000".into(),
            x_token1: "t1".into(),
            endpoint1_tls: false,
            endpoint2: Some("host2:10000".into()),
            x_token2: Some("t2".into()),
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
            solo_streams: vec![MainStream::Slots],
            max_decode_mb: 256,
            ring_capacity: crate::run::DEFAULT_RING_CAPACITY,
        }
    }

    #[test]
    fn slots_request_carries_only_slots_filter() {
        let r = build_main_request_for(Commitment::Processed, &["AAA".into()], MainStream::Slots);
        assert!(r.slots.contains_key(filter_names::SLOTS));
        assert!(r.accounts.is_empty());
        assert!(r.transactions.is_empty());
        assert!(r.blocks.is_empty());
        assert!(r.entry.is_empty());
    }

    #[test]
    fn accounts_request_carries_only_accounts_filter_with_owners() {
        let r =
            build_main_request_for(Commitment::Processed, &["AAA".into()], MainStream::Accounts);
        let a = r.accounts.get(filter_names::ACCOUNTS).unwrap();
        assert_eq!(a.owner, vec!["AAA"]);
        assert!(r.slots.is_empty());
        assert!(r.transactions.is_empty());
        assert!(r.blocks.is_empty());
    }

    #[test]
    fn transactions_request_includes_account_include() {
        let r = build_main_request_for(
            Commitment::Processed,
            &["AAA".into()],
            MainStream::Transactions,
        );
        let tx = r.transactions.get(filter_names::TRANSACTIONS).unwrap();
        assert_eq!(tx.account_include, vec!["AAA"]);
        assert_eq!(tx.failed, Some(false));
        assert_eq!(tx.vote, Some(false));
        assert!(r.slots.is_empty());
        assert!(r.accounts.is_empty());
        assert!(r.blocks.is_empty());
    }

    #[test]
    fn blocks_request_carries_only_blocks_filter() {
        let r = build_main_request_for(Commitment::Processed, &[], MainStream::Blocks);
        let b = r.blocks.get(filter_names::BLOCKS).unwrap();
        assert!(b.account_include.is_empty());
        assert!(r.slots.is_empty());
        assert!(r.accounts.is_empty());
        assert!(r.transactions.is_empty());
    }

    #[test]
    fn main_request_propagates_commitment() {
        let r_proc = build_main_request_for(Commitment::Processed, &[], MainStream::Slots);
        let r_conf = build_main_request_for(Commitment::Confirmed, &[], MainStream::Slots);
        let r_fin = build_main_request_for(Commitment::Finalized, &[], MainStream::Slots);
        assert_eq!(r_proc.commitment, Some(YsCommitment::Processed as i32));
        assert_eq!(r_conf.commitment, Some(YsCommitment::Confirmed as i32));
        assert_eq!(r_fin.commitment, Some(YsCommitment::Finalized as i32));
    }

    #[test]
    fn slots_filter_disables_commitment_filter_per_spec() {
        // Spec §4: both `filter_by_commitment = false` AND
        // `interslot_updates = true` are required to receive all 6
        // slot stages across the wire. Validated against thorofare
        // 2026-05-16 — without interslot_updates=true the server
        // silently drops FirstShred/Completed/CreatedBank.
        let r = build_slot_request();
        let s = r.slots.get(filter_names::SLOTS).unwrap();
        assert_eq!(s.filter_by_commitment, Some(false));
        assert_eq!(s.interslot_updates, Some(true));
    }

    #[test]
    fn slot_request_omits_top_level_commitment() {
        // Spec §6.1 + thorofare-parity 2026-05-16: the slot subscription
        // must NOT set the top-level `commitment` field on the
        // SubscribeRequest. Even with `filter_by_commitment=false` the
        // server applies a small ordering delay (~4ms p50 measured) when
        // top-level commitment is set; thorofare omits it.
        let r = build_slot_request();
        assert!(r.commitment.is_none());
    }

    #[test]
    fn entries_request_has_only_entry_and_slots_filters() {
        let r = build_entries_request(Commitment::Processed);
        assert!(r.entry.contains_key(filter_names::ENTRIES));
        assert!(r.slots.contains_key(filter_names::SLOTS));
        assert!(r.accounts.is_empty());
        assert!(r.transactions.is_empty());
        assert!(r.blocks.is_empty());
    }

    #[test]
    fn plan_has_one_spec_per_endpoint_per_commitment_per_stream() {
        let cli = base_cli();
        let config = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap();
        let plan = SubscriptionPlan::from_run_config(&config, &small_program_set());
        // With accounts_programs_per_filter=1 (default since 2026-05-16)
        // and a 2-program fixture: 2 endpoints × (1 slot subscription +
        // 2 commitments × 2 single-program accounts subs) = 10 specs.
        // Slots is commitment-agnostic (single subscription per endpoint).
        assert_eq!(plan.specs.len(), 10);
        // Every spec carries exactly one filter type.
        for spec in &plan.specs {
            let filter_count = usize::from(!spec.request.slots.is_empty())
                + usize::from(!spec.request.accounts.is_empty())
                + usize::from(!spec.request.transactions.is_empty())
                + usize::from(!spec.request.blocks.is_empty())
                + usize::from(!spec.request.entry.is_empty());
            assert_eq!(filter_count, 1, "spec {:?} had {} filters", spec.role, filter_count);
        }
    }

    #[test]
    fn plan_accounts_chunked_per_filter_size() {
        // Verify the chunking math: with chunk_size=N and P non-heavy
        // programs, we expect ceil(P / N) accounts sub-subscriptions per
        // (endpoint, commitment). Test with chunk_size=2 against 2
        // non-heavy programs → 1 chunk per commitment. Heavy programs
        // always split out independently and so can't exercise this.
        let mut cli = base_cli();
        cli.accounts_programs_per_filter = 2;
        let config = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap();
        let plan =
            SubscriptionPlan::from_run_config(&config, &small_non_heavy_program_set());
        // 2 endpoints × (1 slot + 2 commitments × 1 accounts chunk) = 6 specs.
        assert_eq!(plan.specs.len(), 6);
        // The accounts chunk should carry both program ids.
        let accounts_spec = plan
            .specs
            .iter()
            .find(|s| matches!(s.role, SubscriptionRole::Main { stream: MainStream::Accounts, .. }))
            .expect("accounts spec present");
        let owners = &accounts_spec.request.accounts.get(filter_names::ACCOUNTS).unwrap().owner;
        assert_eq!(owners.len(), 2);
    }

    #[test]
    fn plan_heavy_programs_always_split_out_under_large_chunk_size() {
        // Mix one heavy program with two non-heavy ones; even at
        // chunk_size=10 the heavy program gets its own
        // sub-subscription per (endpoint, commitment) and the two
        // non-heavy ones share a chunk.
        let src = "system\t11111111111111111111111111111111\tSystem\n\
                   raydium\t675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8\tRaydium AMM V4\n\
                   meteora\tLBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo\tMeteora DLMM\n";
        let programs =
            ProgramSet::from_reader(PathBuf::from("mixed.tsv"), Cursor::new(src)).unwrap();
        let mut cli = base_cli();
        cli.accounts_programs_per_filter = 10;
        let config = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap();
        let plan = SubscriptionPlan::from_run_config(&config, &programs);
        // For each (endpoint, commitment) pair we expect 1 heavy +
        // 1 rest chunk = 2 accounts specs.
        // 2 endpoints × (1 slot + 2 commitments × 2 accounts) = 10.
        assert_eq!(plan.specs.len(), 10);

        // The heavy sub-subscription carries only the System program;
        // the rest sub-subscription carries the two non-heavy ids.
        let mut heavy_owners_seen = false;
        let mut rest_owners_seen = false;
        for spec in &plan.specs {
            if !matches!(spec.role, SubscriptionRole::Main { stream: MainStream::Accounts, .. }) {
                continue;
            }
            let owners =
                &spec.request.accounts.get(filter_names::ACCOUNTS).unwrap().owner;
            if owners.len() == 1 && is_known_heavy_program(&owners[0]) {
                heavy_owners_seen = true;
            } else if owners.len() == 2
                && owners.iter().all(|o| !is_known_heavy_program(o))
            {
                rest_owners_seen = true;
            } else {
                panic!("unexpected accounts spec owners: {owners:?}");
            }
        }
        assert!(heavy_owners_seen, "no heavy-only sub-subscription found");
        assert!(rest_owners_seen, "no rest-only sub-subscription found");
    }

    #[test]
    fn plan_with_blocks_and_transactions_yields_more_streams() {
        let mut cli = base_cli();
        cli.with_blocks = true;
        cli.with_transactions = true;
        let config = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap();
        let plan = SubscriptionPlan::from_run_config(&config, &small_program_set());
        // With chunk_size=1, 2 programs, with_tx, with_blocks:
        // 2 endpoints × (1 slot + 2 commitments × (2 accounts + 1 tx + 1 blocks)) = 18 specs.
        assert_eq!(plan.specs.len(), 18);
    }

    #[test]
    fn plan_includes_entries_when_endpoints_configured() {
        let mut cli = base_cli();
        cli.entries_endpoint2 = Some("ent2:10000".into());
        cli.entries_x_token2 = Some("etok2".into());
        let config = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap();
        let plan = SubscriptionPlan::from_run_config(&config, &small_program_set());
        // 10 mains (chunk_size=1) + 1 entries.
        assert_eq!(plan.specs.len(), 11);
        let entries_specs: Vec<_> = plan
            .specs
            .iter()
            .filter(|s| s.role.is_entries())
            .collect();
        assert_eq!(entries_specs.len(), 1);
        assert!(
            matches!(
                entries_specs[0].role,
                SubscriptionRole::Entries {
                    endpoint: EndpointRole::Two
                }
            ),
            "entries role mismatch: {:?}",
            entries_specs[0].role
        );
        assert_eq!(entries_specs[0].endpoint.url, "ent2:10000");
    }

    #[test]
    fn plan_ordering_is_deterministic() {
        let cli = base_cli();
        let config = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap();
        let plan = SubscriptionPlan::from_run_config(&config, &small_program_set());
        // Spec 0: endpoint1, processed, slots.
        assert!(matches!(
            plan.specs[0].role,
            SubscriptionRole::Main {
                endpoint: EndpointRole::One,
                commitment: Commitment::Processed,
                stream: MainStream::Slots,
            }
        ));
        // With chunk_size=1 default and 2 programs, the ep1 accounts
        // subscriptions are: processed × 2 chunks (specs 1, 2), then
        // confirmed × 2 chunks (specs 3, 4). Then specs[5] = ep2 slots.
        assert!(matches!(
            plan.specs[1].role,
            SubscriptionRole::Main {
                endpoint: EndpointRole::One,
                commitment: Commitment::Processed,
                stream: MainStream::Accounts,
            }
        ));
        // Spec 2: still ep1, processed, accounts (the second 1-program chunk).
        assert!(matches!(
            plan.specs[2].role,
            SubscriptionRole::Main {
                endpoint: EndpointRole::One,
                commitment: Commitment::Processed,
                stream: MainStream::Accounts,
            }
        ));
        // Spec 3: ep1, confirmed, accounts (first chunk under confirmed).
        assert!(matches!(
            plan.specs[3].role,
            SubscriptionRole::Main {
                endpoint: EndpointRole::One,
                commitment: Commitment::Confirmed,
                stream: MainStream::Accounts,
            }
        ));
        // Spec 5: endpoint2, slots (after ep1 slot at 0 + ep1's 4
        // accounts subs at 1-4).
        assert!(matches!(
            plan.specs[5].role,
            SubscriptionRole::Main {
                endpoint: EndpointRole::Two,
                stream: MainStream::Slots,
                ..
            }
        ));
    }

    #[test]
    fn accounts_stream_carries_program_ids() {
        // With accounts_programs_per_filter=1 (default), each accounts
        // spec carries a single-program owner list. Union them across
        // one endpoint × one commitment to verify the full program set
        // is covered without omission.
        let cli = base_cli();
        let config = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap();
        let programs = small_program_set();
        let plan = SubscriptionPlan::from_run_config(&config, &programs);
        let mut union: Vec<String> = plan
            .specs
            .iter()
            .filter(|s| {
                matches!(
                    s.role,
                    SubscriptionRole::Main {
                        endpoint: EndpointRole::One,
                        commitment: Commitment::Processed,
                        stream: MainStream::Accounts,
                    }
                )
            })
            .flat_map(|s| {
                s.request
                    .accounts
                    .get(filter_names::ACCOUNTS)
                    .unwrap()
                    .owner
                    .clone()
            })
            .collect();
        union.sort();
        let mut expected = programs.program_ids();
        expected.sort();
        assert_eq!(union, expected);
    }
}
