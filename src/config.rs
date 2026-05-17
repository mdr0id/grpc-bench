//! Run configuration — CLI flags (spec §3) merged with optional TOML overlay.
//!
//! [`Cli`] is the raw `clap` derivation, kept narrow to keep `--help` honest.
//! [`Config`] is the validated, downstream-ready form: every field is in its
//! canonical type and every cross-flag invariant has been checked.
//!
//! The CLI is intentionally not constructed during `cargo test` of this
//! crate — [`Config::from_cli`] is the entry point used by the binary, and
//! the helpers it relies on are unit-tested directly.

use std::{
    fs,
    path::{Path, PathBuf},
};

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::collect::{AffinityParseError, AffinitySpec};

/// Commitment levels that can appear in `--commitment` (spec §3, §4).
///
/// Names match the lowercase tokens accepted on the CLI. Defaults to
/// `[Processed, Confirmed]` when the flag is omitted (spec §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Commitment {
    /// Yellowstone `processed` commitment.
    Processed,
    /// Yellowstone `confirmed` commitment.
    Confirmed,
    /// Yellowstone `finalized` commitment. Not requested by the spec default
    /// but accepted in case an operator wants it during a soak.
    Finalized,
}

impl std::fmt::Display for Commitment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Lowercase token, matching the CLI accepted form so that the
        // default printed in `--help` round-trips through `--commitment`.
        let s = match self {
            Self::Processed => "processed",
            Self::Confirmed => "confirmed",
            Self::Finalized => "finalized",
        };
        f.write_str(s)
    }
}

/// Allocator selection (spec §3 `--allocator`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Allocator {
    /// Default. Production allocator per the precision posture.
    Jemalloc,
    /// Alternative; currently treated identically to jemalloc on macOS dev
    /// builds (we don't link mimalloc by default — see [`Self::Jemalloc`]).
    Mimalloc,
    /// System allocator. Used implicitly on non-Linux dev hosts.
    System,
}

/// Raw CLI shape — what `clap` builds before validation.
///
/// This struct lives in the public API surface so the binary can derive
/// `--help` from a single source. Use [`Config::from_cli`] to validate.
//
// The TLS and feature-toggle bools are semantically independent CLI flags,
// not a state machine. Bundling them into an enum would just hide the CLI
// surface from `--help`.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Parser)]
#[command(
    name = "grpc-bench",
    version,
    about = "Solana Yellowstone gRPC comparative benchmark harness",
    long_about = LONG_ABOUT,
)]
pub struct Cli {
    /// First gRPC endpoint, treated as the comparison reference (spec §3).
    #[arg(long, value_name = "HOST:PORT")]
    pub endpoint1: String,
    /// Auth token for endpoint1 (sent as `x-token` metadata).
    #[arg(long, value_name = "TOKEN", env = "GRPC_BENCH_TOKEN1")]
    pub x_token1: String,
    /// Force TLS for endpoint1 (otherwise inferred from scheme).
    #[arg(long, default_value_t = false)]
    pub endpoint1_tls: bool,

    /// Second gRPC endpoint, treated as the system under test. Required
    /// unless `--solo` is set, in which case it is ignored.
    #[arg(long, value_name = "HOST:PORT", required_unless_present = "solo")]
    pub endpoint2: Option<String>,
    /// Auth token for endpoint2. Required unless `--solo`.
    #[arg(long, value_name = "TOKEN", env = "GRPC_BENCH_TOKEN2",
          required_unless_present = "solo")]
    pub x_token2: Option<String>,
    /// Force TLS for endpoint2.
    #[arg(long, default_value_t = false)]
    pub endpoint2_tls: bool,

    /// Single-endpoint sanity-check mode. Skips endpoint2 entirely and
    /// opens only the streams listed in `--solo-streams` on endpoint1.
    /// Useful when the target endpoint is on a tier that caps concurrent
    /// filters per token — full comparative runs need a higher tier.
    #[arg(long, default_value_t = false)]
    pub solo: bool,

    /// Which streams to open on endpoint1 in `--solo` mode. Comma-
    /// separated list of `slots`, `accounts`, `transactions`, `blocks`.
    /// Default: `slots` (1 active filter — fits a 1-filter tier).
    /// Account-update latency for the configured programs requires
    /// `accounts` (and typically `slots` for stage correlation).
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_values_t = vec![crate::subscribe::MainStream::Slots]
    )]
    pub solo_streams: Vec<crate::subscribe::MainStream>,

    /// Optional separate entries-stream endpoint for endpoint1.
    /// Per Phase 1 resolution we subscribe via the standard Yellowstone
    /// `entry` filter — see PROTO.md.
    #[arg(long, value_name = "HOST:PORT")]
    pub entries_endpoint1: Option<String>,
    /// Auth token for `--entries-endpoint1`.
    #[arg(long, value_name = "TOKEN", env = "GRPC_BENCH_ENTRIES_TOKEN1")]
    pub entries_x_token1: Option<String>,
    /// Force TLS for `--entries-endpoint1`.
    #[arg(long, default_value_t = false)]
    pub entries_endpoint1_tls: bool,

    /// Optional separate entries-stream endpoint for endpoint2.
    #[arg(long, value_name = "HOST:PORT")]
    pub entries_endpoint2: Option<String>,
    /// Auth token for `--entries-endpoint2`.
    #[arg(long, value_name = "TOKEN", env = "GRPC_BENCH_ENTRIES_TOKEN2")]
    pub entries_x_token2: Option<String>,
    /// Force TLS for `--entries-endpoint2`.
    #[arg(long, default_value_t = false)]
    pub entries_endpoint2_tls: bool,

    /// Path to the programs TSV (spec §3, §4).
    #[arg(long, value_name = "PATH")]
    pub programs: PathBuf,

    /// Terminate after this many slot-status `Processed` events on endpoint1.
    /// Mutually optional with `--duration`; if both are set, whichever fires
    /// first ends the run (spec §3).
    #[arg(long, value_name = "N")]
    pub slots: Option<u64>,

    /// Terminate after this many seconds.
    #[arg(long, value_name = "SECS")]
    pub duration: Option<u64>,

    /// Commitment levels to subscribe to. Default is both `processed` and
    /// `confirmed` per spec §3.
    #[arg(
        long,
        value_delimiter = ',',
        default_values_t = vec![Commitment::Processed, Commitment::Confirmed]
    )]
    pub commitment: Vec<Commitment>,

    /// Include `SubscribeBlocks` filter (spec §3).
    #[arg(long, default_value_t = false)]
    pub with_blocks: bool,

    /// Include `SubscribeTransactions` filter (spec §3).
    #[arg(long, default_value_t = false)]
    pub with_transactions: bool,

    /// Include `write_version` in the accounts identity tuple. Off by
    /// default —  deviation, since `write_version` is
    /// validator-local and differs across providers/dedicated nodes.
    /// Enable when comparing two endpoints backed by the same source
    /// (e.g. for thorofare-parity validation against same-provider
    /// targets); disable for cross-provider / cross-validator
    /// comparisons where it would zero out matches.
    #[arg(long, default_value_t = false)]
    pub strict_account_key: bool,

    /// Number of programs packed into each accounts sub-subscription.
    /// Lower = more accurate timing at high multi-program load (one
    /// gRPC connection per program at the extreme); higher = fewer
    /// total connections to the server.
    ///
    /// Measured 2026-05-16 on DO 32-vCPU against pump.fun, system,
    /// spl_token, token_2022 + 18 DEX programs:
    /// - 1 program / filter: 8 ms p50 (matches thorofare standalone)
    /// - 3 programs / filter: 50–350 ms p50 (filter complexity inflates timing)
    /// - 23 programs / filter: 3500 ms p50 (severe inflation; not measurement-grade)
    ///
    /// The default of 1 maximizes timing accuracy at the cost of
    /// `program_count` accounts sub-subscriptions per endpoint per
    /// commitment. Increase only if connection count becomes a
    /// server-side constraint and you've verified the inflation
    /// remains within your measurement tolerance.
    #[arg(long, value_name = "N", default_value_t = 1)]
    pub accounts_programs_per_filter: usize,

    /// Forcibly disconnect and reconnect every N seconds (spec §3, §6.4).
    #[arg(long, value_name = "SECS")]
    pub reconnect_test: Option<u64>,

    /// Output JSON path (spec §3, §8).
    #[arg(long, value_name = "PATH")]
    pub output: PathBuf,

    /// Optional raw-record JSONL path (spec §3, §8).
    #[arg(long, value_name = "PATH")]
    pub raw_records: Option<PathBuf>,

    /// Optional TOML overlay path. CLI values take precedence over TOML.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// CPU cores to pin receiver/processor/control threads to (the precision posture).
    ///
    /// Three forms are accepted:
    ///
    /// - **`auto`**: derive the layout from the host's core count
    ///   (recommended for most users). Reserves cores 0–1 for the
    ///   kernel + the highest core for the control thread, and splits
    ///   the remainder 50/50 between ep1 and ep2. Falls back to no
    ///   pinning on hosts with fewer than 6 cores.
    /// - structured per-endpoint:
    ///   `ep1=2,3,4,5:ep2=6,7,8,9:proc=10:ctrl=11` (power users who
    ///   want full control of the layout).
    /// - legacy flat list: `2,3,4,5` (ep1=2, ep2=3, proc=4, ctrl=5)
    ///   preserved for compatibility with older operator scripts.
    ///
    /// Linux-only; ignored elsewhere with a warning.
    #[arg(long, value_name = "SPEC")]
    pub cpu_affinity: Option<String>,

    /// Request `SCHED_FIFO` priority 50 on receiver threads (the precision posture).
    /// Requires `CAP_SYS_NICE` or root; fails loud if rejected.
    ///
    /// Auto-mitigation: combining `--realtime` with
    /// `--cpu-affinity proc=N` is known to wedge the coordinator on 16+
    /// vCPU rigs (RT processor pinned to one core can starve kernel
    /// bookkeeping on that core). When both are set the `proc=` pin is
    /// dropped with a warning; receivers stay pinned as configured.
    #[arg(long, default_value_t = false)]
    pub realtime: bool,

    /// Allocator selection. Default `jemalloc` on Linux; ignored elsewhere
    /// (system allocator is used and a warning is logged).
    #[arg(long, value_enum, default_value_t = Allocator::Jemalloc)]
    pub allocator: Allocator,

    /// Logging filter, in `tracing_subscriber::EnvFilter` syntax.
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// Per-message gRPC decode cap in MiB. Default 256 MiB. Solana
    /// mainnet full blocks (with `--with-blocks` and `include_transactions
    /// = true`) routinely exceed 64 MiB during busy slots; setting the
    /// default high enough to absorb worst-case blocks plus headroom
    /// avoids silent decode failures that show up downstream as ring
    /// drops or zero block matches. Lower this only for resource-
    /// constrained environments.
    #[arg(long, default_value_t = 256)]
    pub max_decode_mb: usize,

    /// Per-stream baseline ring buffer capacity (events). Default
    /// 65 536; sized to absorb several seconds of burst on a busy slot
    /// without dropping. The baseline is scaled per stream kind by
    /// [`crate::run::ring_capacity_for`]: accounts gets 4×, transactions
    /// / entries 1×, blocks ½×, slots ⅛×. Operators raise this when
    /// running tier-heavy filter sets (23 programs + `--with-blocks`)
    /// and lower it under memory pressure; the per-kind ratios stay
    /// fixed. Approximate memory cost: ~250 bytes per slot × 8–16
    /// streams per endpoint × capacity × per-kind multiplier.
    #[arg(long, default_value_t = crate::run::DEFAULT_RING_CAPACITY)]
    pub ring_capacity: usize,
}

const LONG_ABOUT: &str = "\
Open simultaneous Yellowstone gRPC subscriptions to two endpoints and \
measure per-message arrival timing with kernel precision (Linux only). \
Output JSON is compatible with thorofare's summarize.py.

Endpoint roles:
  --endpoint1  reference / comparison endpoint
  --endpoint2  system under test
Quicknode endpoints are auto-detected from the URL (substring 'quiknode' \
or 'quicknode'); if neither URL matches, endpoint2 is assumed to be \
Quicknode. The auto-detection only affects role labels in the output JSON.

Helius LaserStream note:
  This harness targets the standard Yellowstone-compatible interface only. \
For Helius, use a dedicated-node endpoint (richat-backed). Managed Helius \
LaserStream uses a custom SDK and will fail at handshake here.

Entries:
  --entries-endpoint{1,2} are optional. Per the Phase 1 design, entries \
arrive via the standard Yellowstone 'entry' filter (no separate proto). \
If --entries-endpointN is set, a dedicated stream is opened to that URL \
with only the entry filter active; otherwise no entries are subscribed.

Precision (Linux only):
  --cpu-affinity 2,3,4,5
                          legacy flat form: ep1=2, ep2=3, processor=4, \
control=5. Cores 0-1 left to the kernel.
  --cpu-affinity ep1=2,3,4,5:ep2=6,7,8,9:proc=10:ctrl=11
                          structured form: lists let each endpoint's \
receivers spread across multiple cores (one receiver per core, cycling \
beyond the list length). Useful on hosts where one endpoint's 8 \
subscriptions otherwise share a single core with `--realtime`.
  --realtime              request SCHED_FIFO 50 on receiver threads.
  --allocator jemalloc    default; warm-starts the allocator before \
subscriptions open.

On non-Linux dev hosts the binary still runs but logs a prominent warning \
that kernel timestamps, CPU pinning, SCHED_FIFO, and jemalloc are not \
active. Use Linux for credible measurements.
";

/// Validated form of the CLI + TOML overlay, ready for downstream modules.
//
// Several independent feature toggles (TLS forced, with-blocks,
// with-transactions, realtime, solo) — they're not a state machine.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Serialize)]
pub struct Config {
    /// Reference endpoint.
    pub endpoint1: EndpointSpec,
    /// System under test. `None` when `--solo` is set.
    pub endpoint2: Option<EndpointSpec>,
    /// Optional dedicated entries subscription for endpoint1.
    pub entries_endpoint1: Option<EndpointSpec>,
    /// Optional dedicated entries subscription for endpoint2.
    pub entries_endpoint2: Option<EndpointSpec>,
    /// Programs TSV path; the loader runs in the bin, not here.
    pub programs_path: PathBuf,
    /// Stop condition.
    pub stop: StopCondition,
    /// Commitments to subscribe to.
    pub commitments: Vec<Commitment>,
    /// Whether to include the `SubscribeBlocks` filter.
    pub with_blocks: bool,
    /// Whether to include the `SubscribeTransactions` filter.
    pub with_transactions: bool,
    /// Whether to include `write_version` in the accounts identity
    /// tuple ( deviation, see CLI doc on `Cli::strict_account_key`).
    pub strict_account_key: bool,
    /// Number of programs packed into each accounts sub-subscription
    /// (see [`Cli::accounts_programs_per_filter`]).
    pub accounts_programs_per_filter: usize,
    /// If set, force a disconnect + reconnect every N seconds.
    pub reconnect_test_secs: Option<u64>,
    /// Output JSON path.
    pub output: PathBuf,
    /// Optional raw JSONL output.
    pub raw_records: Option<PathBuf>,
    /// CPU affinity plan (Linux-only effect). [`AffinitySpec::is_empty`]
    /// is true when no pin was requested.
    pub cpu_affinity: AffinitySpec,
    /// Whether `--realtime` was set.
    pub realtime: bool,
    /// Allocator choice.
    pub allocator: Allocator,
    /// Logging filter expression.
    pub log_level: String,
    /// Single-endpoint sanity-check mode.
    pub solo: bool,
    /// Streams to open on endpoint1 in solo mode.
    pub solo_streams: Vec<crate::subscribe::MainStream>,
    /// Per-message gRPC decode cap in MiB.
    pub max_decode_mb: usize,
    /// Per-stream ring buffer capacity (events).
    pub ring_capacity: usize,
}

/// Per-endpoint connection settings.
#[derive(Debug, Clone, Serialize)]
pub struct EndpointSpec {
    /// Host:port string supplied on the CLI.
    pub url: String,
    /// Bearer token sent as `x-token` metadata.
    #[serde(skip_serializing)]
    pub x_token: String,
    /// Whether TLS was forced via `--*-tls`.
    pub tls_forced: bool,
}

/// Stop condition for the run (spec §3: `--slots`, `--duration`, or both).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum StopCondition {
    /// Stop after this many `Processed` slot-status events on endpoint1.
    Slots(u64),
    /// Stop after this many seconds.
    Duration(u64),
    /// Stop on whichever of the two fires first.
    Either {
        /// Slot cap.
        slots: u64,
        /// Wall-clock cap in seconds.
        duration: u64,
    },
}

/// Errors emitted by [`Config::from_cli`] and [`Config::from_cli_and_toml`].
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Neither `--slots` nor `--duration` was supplied.
    #[error("at least one of --slots or --duration must be supplied")]
    NoStopCondition,
    /// `--endpoint2`/`--x-token2` were not supplied and `--solo` wasn't set.
    #[error("--endpoint2 and --x-token2 are required unless --solo is set")]
    Endpoint2Missing,
    /// Both `--entries-endpoint*` and its `--entries-x-token*` must agree.
    #[error("--entries-endpoint{which} requires --entries-x-token{which}")]
    EntriesTokenMismatch {
        /// Endpoint index (1 or 2).
        which: u8,
    },
    /// Pair-side mismatch — token without endpoint.
    #[error("--entries-x-token{which} requires --entries-endpoint{which}")]
    EntriesEndpointMismatch {
        /// Endpoint index (1 or 2).
        which: u8,
    },
    /// `--cpu-affinity` failed to parse.
    #[error(transparent)]
    CpuAffinityParse(#[from] AffinityParseError),
    /// `--commitment` had a duplicate value.
    #[error("--commitment has duplicate value {value:?}")]
    DuplicateCommitment {
        /// The duplicated commitment token.
        value: String,
    },
    /// `--commitment` was empty (clap shouldn't produce this, but guard).
    #[error("--commitment must list at least one level")]
    EmptyCommitment,
    /// Failed to read the TOML overlay.
    #[error("failed to read TOML overlay {path}: {source}")]
    TomlRead {
        /// Path that failed to read.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// Failed to parse the TOML overlay.
    #[error("failed to parse TOML overlay {path}: {source}")]
    TomlParse {
        /// Path that failed to parse.
        path: PathBuf,
        /// Underlying TOML error.
        #[source]
        source: toml::de::Error,
    },
}

/// Optional fields that can be supplied via TOML overlay. Only fields the
/// spec explicitly allows in a config file are present. CLI values always
/// win on conflict.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TomlOverlay {
    /// Default log level if `--log-level` is not supplied.
    pub log_level: Option<String>,
    /// Default raw-records path.
    pub raw_records: Option<PathBuf>,
    /// Default CPU affinity list.
    pub cpu_affinity: Option<Vec<u32>>,
}

impl Config {
    /// Validate a parsed [`Cli`] and optionally merge a TOML overlay path
    /// from `--config`. CLI values take precedence; TOML supplies defaults
    /// for the small set of fields that make sense to put in a shared
    /// config file.
    ///
    /// # Errors
    /// Returns [`ConfigError`] on any of the validation failures listed in
    /// that type's variants.
    pub fn from_cli(cli: Cli) -> Result<Self, ConfigError> {
        let overlay = match cli.config.as_deref() {
            Some(p) => Some(load_toml_overlay(p)?),
            None => None,
        };
        Self::from_cli_and_overlay(cli, overlay.unwrap_or_default())
    }

    /// Variant of [`Self::from_cli`] that takes a preloaded TOML overlay.
    /// Used by tests so we don't touch disk.
    ///
    /// # Errors
    /// Same conditions as [`Self::from_cli`] minus the TOML read step.
    pub fn from_cli_and_overlay(cli: Cli, overlay: TomlOverlay) -> Result<Self, ConfigError> {
        let stop = match (cli.slots, cli.duration) {
            (None, None) => return Err(ConfigError::NoStopCondition),
            (Some(s), None) => StopCondition::Slots(s),
            (None, Some(d)) => StopCondition::Duration(d),
            (Some(s), Some(d)) => StopCondition::Either {
                slots: s,
                duration: d,
            },
        };

        validate_entries_pair(1, cli.entries_endpoint1.as_deref(), cli.entries_x_token1.as_deref())?;
        validate_entries_pair(2, cli.entries_endpoint2.as_deref(), cli.entries_x_token2.as_deref())?;

        if cli.commitment.is_empty() {
            return Err(ConfigError::EmptyCommitment);
        }
        for (i, c) in cli.commitment.iter().enumerate() {
            if cli.commitment[..i].contains(c) {
                return Err(ConfigError::DuplicateCommitment {
                    value: format!("{c:?}").to_lowercase(),
                });
            }
        }

        // CLI cpu_affinity is a raw string; if absent, fall back to the
        // TOML overlay's flat `Vec<u32>`. The flat form maps to the
        // spec's 4-core layout; the structured form unlocks per-endpoint
        // multi-core lists (used by `--realtime` 23p runs).
        let mut cpu_affinity = match cli.cpu_affinity {
            Some(raw) => AffinitySpec::parse(&raw)?,
            None => match overlay.cpu_affinity {
                Some(cores) => AffinitySpec::from_flat_vec(&cores)?,
                None => AffinitySpec::default(),
            },
        };

        // Safety guard: `--realtime` + `--cpu-affinity proc=N` is known
        // to wedge the coordinator on 16+ vCPU rigs (see
        // `rt_coordinator_pin_wedge`). The processor thread runs at
        // SCHED_FIFO 50 under --realtime; pinning it to one core can
        // starve out the kernel's own bookkeeping work scheduled on the
        // same core (timers, RCU callbacks) and the symptom is the
        // coordinator no longer being scheduled at all. Drop the pin
        // and let the kernel float the processor over the available
        // SCHED_OTHER cores; receivers stay pinned/RT as before.
        // Config parsing runs before `tracing_subscriber` is initialized
        // in `main`, so a `tracing::warn!` here would be silently
        // dropped. Use `eprintln!` so the operator actually sees the
        // auto-mitigation at startup.
        if cli.realtime {
            if let Some(dropped) = cpu_affinity.processor.take() {
                eprintln!(
                    "grpc-bench: WARNING --realtime + --cpu-affinity proc={dropped} \
                     has been observed to wedge the coordinator on 16+ vCPU rigs \
                     (see rt_coordinator_pin_wedge). Dropping proc= pin for safety; \
                     the processor will float on a kernel-scheduled core."
                );
            }
        }

        let raw_records = cli.raw_records.or(overlay.raw_records);
        let log_level = if cli.log_level == "info" {
            // "info" is the clap default; treat as "not specified" and let
            // TOML override.
            overlay.log_level.unwrap_or(cli.log_level)
        } else {
            cli.log_level
        };

        let entries_endpoint1 = build_optional_endpoint(
            cli.entries_endpoint1,
            cli.entries_x_token1,
            cli.entries_endpoint1_tls,
        );
        let entries_endpoint2 = build_optional_endpoint(
            cli.entries_endpoint2,
            cli.entries_x_token2,
            cli.entries_endpoint2_tls,
        );

        // endpoint2 / x_token2 are required by clap unless --solo is set,
        // so in non-solo mode we expect both to be present here. In solo
        // mode both are silently ignored even if the operator supplied
        // them (avoids spurious "you forgot --solo" surprises).
        let endpoint2 = if cli.solo {
            None
        } else {
            match (cli.endpoint2, cli.x_token2) {
                (Some(url), Some(x_token)) => Some(EndpointSpec {
                    url,
                    x_token,
                    tls_forced: cli.endpoint2_tls,
                }),
                _ => return Err(ConfigError::Endpoint2Missing),
            }
        };

        Ok(Self {
            endpoint1: EndpointSpec {
                url: cli.endpoint1,
                x_token: cli.x_token1,
                tls_forced: cli.endpoint1_tls,
            },
            endpoint2,
            entries_endpoint1,
            entries_endpoint2,
            programs_path: cli.programs,
            stop,
            commitments: cli.commitment,
            with_blocks: cli.with_blocks,
            with_transactions: cli.with_transactions,
            strict_account_key: cli.strict_account_key,
            accounts_programs_per_filter: cli.accounts_programs_per_filter.max(1),
            reconnect_test_secs: cli.reconnect_test,
            output: cli.output,
            raw_records,
            cpu_affinity,
            realtime: cli.realtime,
            allocator: cli.allocator,
            log_level,
            solo: cli.solo,
            solo_streams: cli.solo_streams,
            max_decode_mb: cli.max_decode_mb,
            ring_capacity: cli.ring_capacity,
        })
    }
}

fn build_optional_endpoint(
    url: Option<String>,
    token: Option<String>,
    tls: bool,
) -> Option<EndpointSpec> {
    match (url, token) {
        (Some(url), Some(token)) => Some(EndpointSpec {
            url,
            x_token: token,
            tls_forced: tls,
        }),
        _ => None,
    }
}

fn validate_entries_pair(
    which: u8,
    endpoint: Option<&str>,
    token: Option<&str>,
) -> Result<(), ConfigError> {
    match (endpoint, token) {
        (Some(_), None) => Err(ConfigError::EntriesTokenMismatch { which }),
        (None, Some(_)) => Err(ConfigError::EntriesEndpointMismatch { which }),
        _ => Ok(()),
    }
}

fn load_toml_overlay(path: &Path) -> Result<TomlOverlay, ConfigError> {
    let raw = fs::read_to_string(path).map_err(|source| ConfigError::TomlRead {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_str(&raw).map_err(|source| ConfigError::TomlParse {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
            solo_streams: vec![crate::subscribe::MainStream::Slots],
            max_decode_mb: 256,
            ring_capacity: crate::run::DEFAULT_RING_CAPACITY,
        }
    }

    #[test]
    fn slots_only_yields_slots_stop() {
        let cfg = Config::from_cli_and_overlay(base_cli(), TomlOverlay::default()).unwrap();
        assert!(matches!(cfg.stop, StopCondition::Slots(1000)));
    }

    #[test]
    fn duration_only_yields_duration_stop() {
        let mut cli = base_cli();
        cli.slots = None;
        cli.duration = Some(60);
        let cfg = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap();
        assert!(matches!(cfg.stop, StopCondition::Duration(60)));
    }

    #[test]
    fn both_yields_either_stop() {
        let mut cli = base_cli();
        cli.duration = Some(60);
        let cfg = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap();
        assert!(matches!(
            cfg.stop,
            StopCondition::Either {
                slots: 1000,
                duration: 60
            }
        ));
    }

    #[test]
    fn no_stop_condition_rejected() {
        let mut cli = base_cli();
        cli.slots = None;
        cli.duration = None;
        let err = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap_err();
        assert!(matches!(err, ConfigError::NoStopCondition));
    }

    #[test]
    fn entries_endpoint_without_token_rejected() {
        let mut cli = base_cli();
        cli.entries_endpoint1 = Some("host:1".into());
        let err = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap_err();
        assert!(matches!(err, ConfigError::EntriesTokenMismatch { which: 1 }));
    }

    #[test]
    fn entries_token_without_endpoint_rejected() {
        let mut cli = base_cli();
        cli.entries_x_token2 = Some("tok".into());
        let err = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap_err();
        assert!(matches!(err, ConfigError::EntriesEndpointMismatch { which: 2 }));
    }

    #[test]
    fn entries_both_supplied_builds_spec() {
        let mut cli = base_cli();
        cli.entries_endpoint1 = Some("ehost:1".into());
        cli.entries_x_token1 = Some("etok".into());
        cli.entries_endpoint1_tls = true;
        let cfg = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap();
        let e = cfg.entries_endpoint1.unwrap();
        assert_eq!(e.url, "ehost:1");
        assert_eq!(e.x_token, "etok");
        assert!(e.tls_forced);
    }

    #[test]
    fn duplicate_affinity_rejected() {
        let mut cli = base_cli();
        cli.cpu_affinity = Some("2,3,2".into());
        let err = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::CpuAffinityParse(AffinityParseError::DuplicateCore { core: 2 })
        ));
    }

    #[test]
    fn structured_affinity_parses() {
        let mut cli = base_cli();
        cli.cpu_affinity = Some("ep1=2,3,4,5:ep2=6,7,8,9:proc=10:ctrl=11".into());
        let cfg = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap();
        assert_eq!(cfg.cpu_affinity.endpoint1, vec![2, 3, 4, 5]);
        assert_eq!(cfg.cpu_affinity.endpoint2, vec![6, 7, 8, 9]);
        assert_eq!(cfg.cpu_affinity.processor, Some(10));
        assert_eq!(cfg.cpu_affinity.control, Some(11));
    }

    #[test]
    fn flat_affinity_maps_to_spec_default() {
        let mut cli = base_cli();
        cli.cpu_affinity = Some("2,3,4,5".into());
        let cfg = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap();
        assert_eq!(cfg.cpu_affinity.endpoint1, vec![2]);
        assert_eq!(cfg.cpu_affinity.endpoint2, vec![3]);
        assert_eq!(cfg.cpu_affinity.processor, Some(4));
        assert_eq!(cfg.cpu_affinity.control, Some(5));
    }

    #[test]
    fn structured_affinity_cross_role_dedup() {
        let mut cli = base_cli();
        cli.cpu_affinity = Some("ep1=2,3:ep2=3,4".into());
        let err = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::CpuAffinityParse(AffinityParseError::DuplicateCore { core: 3 })
        ));
    }

    #[test]
    fn structured_affinity_proc_rejects_list() {
        let mut cli = base_cli();
        cli.cpu_affinity = Some("ep1=2:proc=3,4".into());
        let err = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::CpuAffinityParse(AffinityParseError::SingleCoreOnly { role: "proc" })
        ));
    }

    #[test]
    fn structured_affinity_rejects_unknown_role() {
        let mut cli = base_cli();
        cli.cpu_affinity = Some("ep1=2:bogus=3".into());
        let err = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::CpuAffinityParse(AffinityParseError::UnknownRole { .. })
        ));
    }

    #[test]
    fn realtime_drops_processor_pin_with_warning() {
        // --realtime + --cpu-affinity proc=N wedges the coordinator on
        // 16+ vCPU rigs (see rt_coordinator_pin_wedge memo). Guard
        // strips the proc= pin and keeps everything else.
        let mut cli = base_cli();
        cli.realtime = true;
        cli.cpu_affinity = Some("ep1=2,3:ep2=4,5:proc=10:ctrl=11".into());
        let cfg = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap();
        assert!(cfg.realtime);
        assert!(cfg.cpu_affinity.processor.is_none(), "proc= pin should be dropped under --realtime");
        // Receivers and control pin must be preserved — only proc= is risky.
        assert_eq!(cfg.cpu_affinity.endpoint1, vec![2, 3]);
        assert_eq!(cfg.cpu_affinity.endpoint2, vec![4, 5]);
        assert_eq!(cfg.cpu_affinity.control, Some(11));
    }

    #[test]
    fn proc_pin_preserved_without_realtime() {
        // The guard fires only under --realtime; without it, proc= is
        // safe and we must keep the operator's request.
        let mut cli = base_cli();
        cli.realtime = false;
        cli.cpu_affinity = Some("ep1=2:proc=10".into());
        let cfg = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap();
        assert_eq!(cfg.cpu_affinity.processor, Some(10));
    }

    #[test]
    fn duplicate_commitment_rejected() {
        let mut cli = base_cli();
        cli.commitment = vec![Commitment::Processed, Commitment::Processed];
        let err = Config::from_cli_and_overlay(cli, TomlOverlay::default()).unwrap_err();
        assert!(matches!(err, ConfigError::DuplicateCommitment { .. }));
    }

    #[test]
    fn toml_overlay_fills_defaults_when_cli_omits() {
        let cli = base_cli();
        let overlay = TomlOverlay {
            log_level: Some("debug".into()),
            raw_records: Some(PathBuf::from("/tmp/raw.jsonl")),
            cpu_affinity: Some(vec![4, 5]),
        };
        let cfg = Config::from_cli_and_overlay(cli, overlay).unwrap();
        assert_eq!(cfg.log_level, "debug");
        assert_eq!(cfg.raw_records, Some(PathBuf::from("/tmp/raw.jsonl")));
        // Flat 2-core overlay maps per spec to ep1=4, ep2=5, no proc/ctrl.
        assert_eq!(cfg.cpu_affinity.endpoint1, vec![4]);
        assert_eq!(cfg.cpu_affinity.endpoint2, vec![5]);
        assert!(cfg.cpu_affinity.processor.is_none());
    }

    #[test]
    fn cli_overrides_toml() {
        let mut cli = base_cli();
        cli.log_level = "trace".into();
        cli.raw_records = Some(PathBuf::from("/cli/raw.jsonl"));
        cli.cpu_affinity = Some("2,3".into());
        let overlay = TomlOverlay {
            log_level: Some("debug".into()),
            raw_records: Some(PathBuf::from("/toml/raw.jsonl")),
            cpu_affinity: Some(vec![10, 11]),
        };
        let cfg = Config::from_cli_and_overlay(cli, overlay).unwrap();
        assert_eq!(cfg.log_level, "trace");
        assert_eq!(cfg.raw_records, Some(PathBuf::from("/cli/raw.jsonl")));
        assert_eq!(cfg.cpu_affinity.endpoint1, vec![2]);
        assert_eq!(cfg.cpu_affinity.endpoint2, vec![3]);
    }
}
