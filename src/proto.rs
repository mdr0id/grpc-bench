//! gRPC proto version negotiation (the proto policy).
//!
//! On startup we call `GetVersion` against every endpoint, parse the
//! returned version string (which yellowstone-grpc-geyser emits as a JSON
//! blob), and compare the reported proto version against the version of
//! [`yellowstone_grpc_proto`] this binary was compiled with. The crate
//! versions come from `Cargo.lock` via the build script (`build.rs`).
//!
//! Compatibility policy:
//!
//! The spec's the proto policy is internally inconsistent on one point — rule 3 says
//! "refuse if server proto < harness build proto", but the explicit
//! minimum baselines (`yellowstone-grpc-geyser 12.2.0+solana.3.1.13`,
//! `richat 2.1.0`) cover plugin versions whose proto can be older than
//! whatever crates.io currently publishes. Quicknode's hosted Yellowstone
//! at the documented minimum reports plugin `12.2.0` with proto `12.1.0`,
//! which the strict rule would refuse despite being the supported floor.
//!
//! Resolution: refuse only on **major** version mismatch (incompatible
//! decode) and plugin-baseline failure; warn within the same major in
//! either direction (protobuf is forward-compatible for added fields and
//! backward-compatible for known fields). The plugin baseline is now
//! checked against the `plugin_version` field, not the `proto_version`.
//!
//! - Same proto major as build → `Accept` silently or `Warn` if there's a
//!   minor / patch difference.
//! - Older proto major than build → `RefuseOlderMajor`.
//! - Newer proto major than build → `Warn` (decode of known fields still
//!   works; new wire fields are silently dropped).
//! - Plugin baseline (`yellowstone-grpc-geyser >= 12.2.0`, `richat >= 2.1.0`
//!   from the proto policy) is enforced against the **plugin version** the
//!   server reports, not the proto version → `RefuseBaseline`.
//!
//! Failure modes that are *not* a refusal — unreachable endpoints,
//! malformed version strings, missing `package` field — are reported as
//! diagnostic warnings on the resulting [`EndpointVersion`] so the run can
//! still proceed against best-effort metadata.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Resolved version of the `yellowstone-grpc-proto` crate this binary was
/// compiled against. Sourced from `Cargo.lock` by `build.rs`.
pub const HARNESS_PROTO_CRATE_VERSION: &str = env!("GRPC_BENCH_YELLOWSTONE_PROTO_VER");

/// Resolved version of the `yellowstone-grpc-client` crate.
pub const HARNESS_CLIENT_CRATE_VERSION: &str = env!("GRPC_BENCH_YELLOWSTONE_CLIENT_VER");

/// Spec the proto policy: minimum supported `yellowstone-grpc-geyser` baseline.
pub const MIN_YELLOWSTONE_GEYSER: SemVer = SemVer {
    major: 12,
    minor: 2,
    patch: 0,
};

/// Spec the proto policy: minimum supported `richat` baseline.
pub const MIN_RICHAT: SemVer = SemVer {
    major: 2,
    minor: 1,
    patch: 0,
};

/// A semver triple `MAJOR.MINOR.PATCH`. Build/pre-release metadata is
/// stripped for comparison purposes but preserved in the `raw` field on
/// [`ParsedVersion`] for the result JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SemVer {
    /// Major version.
    pub major: u32,
    /// Minor version.
    pub minor: u32,
    /// Patch version.
    pub patch: u32,
}

impl SemVer {
    /// Lexicographic comparison `(major, minor, patch)`.
    #[must_use]
    pub fn ord(self, other: SemVer) -> std::cmp::Ordering {
        (self.major, self.minor, self.patch).cmp(&(other.major, other.minor, other.patch))
    }
}

impl std::fmt::Display for SemVer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Errors returned when parsing a semver triple from a version string.
#[derive(Debug, Error)]
pub enum SemVerParseError {
    /// The string did not contain three dot-separated components.
    #[error("expected MAJOR.MINOR.PATCH triple, got {input:?}")]
    Shape {
        /// The original (pre-strip) input.
        input: String,
    },
    /// A component was not a base-10 unsigned integer.
    #[error("non-numeric semver component in {input:?}: {component:?}")]
    Component {
        /// The original (pre-strip) input.
        input: String,
        /// The offending component.
        component: String,
    },
}

/// Parse a `MAJOR.MINOR.PATCH` string into a [`SemVer`].
///
/// Build metadata (`+...`) and pre-release tags (`-...`) on the patch
/// component are stripped before parsing. The original string is returned
/// in [`SemVerParseError::Shape::input`] on failure.
///
/// # Errors
/// Returns [`SemVerParseError`] when the input doesn't look like a triple
/// or any component isn't an integer.
pub fn parse_semver(raw: &str) -> Result<SemVer, SemVerParseError> {
    let stripped = raw.trim().strip_prefix('v').unwrap_or_else(|| raw.trim());
    let mut parts = stripped.splitn(3, '.');
    let major = parts.next().ok_or_else(|| SemVerParseError::Shape {
        input: raw.to_string(),
    })?;
    let minor = parts.next().ok_or_else(|| SemVerParseError::Shape {
        input: raw.to_string(),
    })?;
    let patch_rest = parts.next().ok_or_else(|| SemVerParseError::Shape {
        input: raw.to_string(),
    })?;
    // Strip build metadata (+) and pre-release (-) on the patch component.
    let patch_str = patch_rest
        .split_once('+')
        .map_or(patch_rest, |(p, _)| p)
        .split_once('-')
        .map_or_else(
            || {
                patch_rest
                    .split_once('+')
                    .map_or(patch_rest, |(p, _)| p)
            },
            |(p, _)| p,
        );
    let major = major.parse::<u32>().map_err(|_| SemVerParseError::Component {
        input: raw.to_string(),
        component: major.to_string(),
    })?;
    let minor = minor.parse::<u32>().map_err(|_| SemVerParseError::Component {
        input: raw.to_string(),
        component: minor.to_string(),
    })?;
    let patch = patch_str
        .parse::<u32>()
        .map_err(|_| SemVerParseError::Component {
            input: raw.to_string(),
            component: patch_str.to_string(),
        })?;
    Ok(SemVer {
        major,
        minor,
        patch,
    })
}

/// Parsed form of the server's `GetVersionResponse.version` string. The
/// yellowstone-grpc-geyser server emits a JSON blob here whose shape varies
/// somewhat by plugin. The fields we care about are all optional in the
/// parser; missing fields produce a diagnostic warning rather than an
/// error so we can still surface partial information in the output JSON.
///
/// Two real-world shapes are accepted:
///
/// 1. Flat: `{"package":"...","version":"...","proto":"...","solana":"..."}`
/// 2. Nested under `version`, which is what QN's hosted Yellowstone returns:
///    `{"version":{"package":"...","version":"...","proto":"...","solana":"..."},
///       "extra":{"hostname":"..."}}`
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct VersionBlob {
    /// E.g. `"yellowstone-grpc-geyser"` or `"richat"`.
    package: Option<String>,
    /// Plugin version (often matches `proto`).
    version: Option<String>,
    /// Proto schema version. Some servers don't report it; we fall back to
    /// `version` in that case.
    proto: Option<String>,
    /// Compiled-against Solana version (informational).
    solana: Option<String>,
}

/// Outer wrapper used by Quicknode-hosted Yellowstone, where the fields
/// we care about live under a `version` key alongside an `extra` block.
#[derive(Debug, Clone, Default, Deserialize)]
struct NestedVersionBlob {
    version: VersionBlob,
}

/// Try the nested shape first, then flat. Returns `None` if the raw
/// string isn't JSON or doesn't match either shape.
fn parse_version_blob(raw: &str) -> Option<VersionBlob> {
    if let Ok(nested) = serde_json::from_str::<NestedVersionBlob>(raw) {
        // Treat the nested form as a real hit only when at least one of
        // the inner fields is populated; otherwise an empty object would
        // shadow a successful flat parse.
        let inner = nested.version;
        if inner.package.is_some()
            || inner.version.is_some()
            || inner.proto.is_some()
            || inner.solana.is_some()
        {
            return Some(inner);
        }
    }
    serde_json::from_str::<VersionBlob>(raw).ok()
}

/// Parsed and validated version info from one endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct EndpointVersion {
    /// Best-effort plugin package name. `None` if the server didn't supply
    /// one or the version string wasn't JSON. Used to apply
    /// plugin-specific baselines.
    pub package: Option<String>,
    /// Best-effort plugin version (the `version` JSON field).
    pub plugin_version: Option<String>,
    /// Best-effort proto schema version. Falls back to `plugin_version`
    /// when the server doesn't report a distinct proto field.
    pub proto_version: Option<String>,
    /// Best-effort Solana version (informational).
    pub solana_version: Option<String>,
    /// Compatibility outcome from [`negotiate`].
    pub compatibility: Compatibility,
    /// Verbatim raw `version` string from the server, for the audit trail.
    pub raw: String,
    /// Soft warnings: malformed JSON, missing fields, newer-than-build,
    /// etc.
    pub warnings: Vec<String>,
}

/// Outcome of comparing a server's reported proto version against the
/// harness's built-against version.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Compatibility {
    /// Server proto major matches harness; minor/patch within the same
    /// major. Decode is fully supported.
    Accept,
    /// Server proto is strictly newer than harness build. Decode of known
    /// fields is still valid; new fields are ignored gracefully.
    Warn {
        /// The reason text bubbled into [`EndpointVersion::warnings`].
        reason: String,
    },
    /// Refused: server proto major < harness major. We can't safely decode.
    RefuseOlderMajor {
        /// Harness major version.
        harness: u32,
        /// Server major version reported by `GetVersion`.
        server: u32,
    },
    /// Refused: server plugin failed an explicit baseline (the proto policy).
    RefuseBaseline {
        /// Plugin package name (e.g. `yellowstone-grpc-geyser`).
        package: String,
        /// Required minimum version per spec.
        minimum: SemVer,
        /// Reported version.
        reported: SemVer,
    },
    /// Server didn't return enough information to evaluate. The harness
    /// continues, but the result JSON's `proto_metadata` records the
    /// degraded check.
    Unknown {
        /// Reason text.
        reason: String,
    },
}

impl Compatibility {
    /// Whether this outcome should abort the run.
    #[must_use]
    pub fn is_refusal(&self) -> bool {
        matches!(
            self,
            Self::RefuseOlderMajor { .. } | Self::RefuseBaseline { .. }
        )
    }
}

/// Parse a server `GetVersionResponse.version` string and evaluate
/// compatibility against the harness's built-against proto version.
///
/// Diagnostic warnings (malformed JSON, missing fields, newer server) are
/// surfaced via [`EndpointVersion::warnings`]; hard incompatibilities show
/// up as [`Compatibility::Refuse*`] variants on
/// [`EndpointVersion::compatibility`].
#[must_use]
pub fn evaluate(raw: &str) -> EndpointVersion {
    let mut warnings: Vec<String> = Vec::new();
    let harness_proto = match parse_semver(HARNESS_PROTO_CRATE_VERSION) {
        Ok(v) => v,
        Err(e) => {
            // Should never happen — Cargo.lock provides this string.
            warnings.push(format!(
                "internal: failed to parse harness proto version {HARNESS_PROTO_CRATE_VERSION:?}: {e}"
            ));
            return EndpointVersion {
                package: None,
                plugin_version: None,
                proto_version: None,
                solana_version: None,
                compatibility: Compatibility::Unknown {
                    reason: "failed to parse harness's own proto crate version".to_string(),
                },
                raw: raw.to_string(),
                warnings,
            };
        }
    };

    let Some(blob) = parse_version_blob(raw) else {
        // Some servers return a plain semver string rather than JSON.
        warnings.push(format!(
            "server version string is not JSON ({}); treating as plain version label",
            short_preview(raw)
        ));
        let plain = raw.trim().trim_matches('"').to_string();
        return assemble(EndpointVersion {
            package: None,
            plugin_version: Some(plain.clone()),
            proto_version: Some(plain.clone()),
            solana_version: None,
            compatibility: classify(harness_proto, parse_semver(&plain).ok(), None, None),
            raw: raw.to_string(),
            warnings,
        });
    };

    if blob.package.is_none() {
        warnings.push(
            "server GetVersion did not include `package`; baseline check skipped".to_string(),
        );
    }

    let server_semver_str = blob.proto.clone().or_else(|| blob.version.clone());
    let server_semver = server_semver_str.as_deref().and_then(|s| match parse_semver(s) {
        Ok(v) => Some(v),
        Err(e) => {
            warnings.push(format!("could not parse server proto version: {e}"));
            None
        }
    });

    // Plugin version (the `version` field) is what the spec's baselines
    // apply to; proto version (the `proto` field, or `version` as a
    // fallback) is what dictates wire-level compatibility.
    let plugin_semver = blob.version.as_deref().and_then(|s| match parse_semver(s) {
        Ok(v) => Some(v),
        Err(e) => {
            warnings.push(format!("could not parse server plugin version: {e}"));
            None
        }
    });
    let compatibility = classify(
        harness_proto,
        server_semver,
        plugin_semver,
        blob.package.as_deref(),
    );
    if let Compatibility::Warn { reason } = &compatibility {
        warnings.push(reason.clone());
    }

    assemble(EndpointVersion {
        package: blob.package,
        plugin_version: blob.version,
        proto_version: blob.proto.or(server_semver_str),
        solana_version: blob.solana,
        compatibility,
        raw: raw.to_string(),
        warnings,
    })
}

/// Pull the `EndpointVersion` through a no-op so callers can chain without
/// rebuilding the struct literal; centralizes future field additions.
fn assemble(mut v: EndpointVersion) -> EndpointVersion {
    // De-duplicate identical warnings — `Compatibility::Warn` will surface
    // the same text the classifier returned via `reason`.
    v.warnings.sort();
    v.warnings.dedup();
    v
}

fn classify(
    harness: SemVer,
    server_proto: Option<SemVer>,
    server_plugin: Option<SemVer>,
    package: Option<&str>,
) -> Compatibility {
    let Some(server_proto_v) = server_proto else {
        return Compatibility::Unknown {
            reason: "server did not report a parseable proto version".to_string(),
        };
    };

    // Refuse on major mismatch — wire decode would silently corrupt.
    if server_proto_v.major < harness.major {
        return Compatibility::RefuseOlderMajor {
            harness: harness.major,
            server: server_proto_v.major,
        };
    }

    // Plugin-baseline check. The spec's minimum baselines are stated in
    // plugin-version terms (`yellowstone-grpc-geyser 12.2.0`,
    // `richat 2.1.0`), and the plugin's bundled proto version can lag.
    // We therefore check the **plugin** version against the baseline,
    // falling back to proto only when plugin wasn't reported.
    if let Some(pkg) = package {
        let minimum = match pkg {
            "yellowstone-grpc-geyser" => Some(MIN_YELLOWSTONE_GEYSER),
            "richat" => Some(MIN_RICHAT),
            _ => None,
        };
        if let Some(min) = minimum {
            let cmp_target = server_plugin.unwrap_or(server_proto_v);
            // Don't fire baseline refusal when the plugin major doesn't
            // match the baseline major — that's a different plugin
            // family (e.g. richat 2.x vs yellowstone 12.x) and the
            // older-major refusal above is the right gate.
            if cmp_target.major == min.major && cmp_target.ord(min) == std::cmp::Ordering::Less {
                return Compatibility::RefuseBaseline {
                    package: pkg.to_string(),
                    minimum: min,
                    reported: cmp_target,
                };
            }
        }
    }

    // Within the same major, drift in either direction is a warning. The
    // proto crate is forward-compatible for added fields (older server
    // missing fields we now read) and backward-compatible for known
    // fields (newer server adding fields we ignore).
    match server_proto_v.ord(harness) {
        std::cmp::Ordering::Equal => Compatibility::Accept,
        std::cmp::Ordering::Less => Compatibility::Warn {
            reason: format!(
                "server proto {server_proto_v} is older than harness build {harness} \
                 within the same major; continuing because protobuf is \
                 forward-compatible for added fields, but flag the version drift"
            ),
        },
        std::cmp::Ordering::Greater => Compatibility::Warn {
            reason: format!(
                "server proto {server_proto_v} is newer than harness build {harness}; \
                 continuing because protobuf is backward-compatible for known fields"
            ),
        },
    }
}

fn short_preview(s: &str) -> String {
    let trimmed = s.trim();
    if trimmed.len() <= 80 {
        format!("{trimmed:?}")
    } else {
        format!("{:?}...", &trimmed[..77])
    }
}

/// Aggregate version metadata for the entire run, ready to serialize into
/// the `proto_metadata` block of the output JSON (the output JSON schema).
#[derive(Debug, Clone, Serialize)]
pub struct ProtoMetadata {
    /// Resolved Cargo.lock version of `yellowstone-grpc-proto`.
    pub yellowstone_proto_crate_version: String,
    /// Resolved Cargo.lock version of `yellowstone-grpc-client`.
    pub yellowstone_grpc_client_crate_version: String,
    /// `GetVersion` outcome for endpoint1.
    pub endpoint1_server_plugin_version: String,
    /// `GetVersion` outcome for endpoint2.
    pub endpoint2_server_plugin_version: String,
    /// Collated warnings across endpoints — newer servers, missing fields,
    /// non-JSON version blobs.
    pub compatibility_warnings: Vec<String>,
}

impl ProtoMetadata {
    /// Construct from per-endpoint version evaluations.
    #[must_use]
    pub fn from_endpoints(
        endpoint1: &EndpointVersion,
        endpoint2: &EndpointVersion,
    ) -> Self {
        let mut warnings: Vec<String> = Vec::new();
        for (label, v) in [("endpoint1", endpoint1), ("endpoint2", endpoint2)] {
            for w in &v.warnings {
                warnings.push(format!("{label}: {w}"));
            }
        }
        Self {
            yellowstone_proto_crate_version: HARNESS_PROTO_CRATE_VERSION.to_string(),
            yellowstone_grpc_client_crate_version: HARNESS_CLIENT_CRATE_VERSION.to_string(),
            endpoint1_server_plugin_version: endpoint1
                .plugin_version
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
            endpoint2_server_plugin_version: endpoint2
                .plugin_version
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
            compatibility_warnings: warnings,
        }
    }

    /// `--solo` variant: only endpoint1 was queried.
    #[must_use]
    pub fn from_single_endpoint(endpoint1: &EndpointVersion) -> Self {
        let warnings: Vec<String> = endpoint1
            .warnings
            .iter()
            .map(|w| format!("endpoint1: {w}"))
            .collect();
        Self {
            yellowstone_proto_crate_version: HARNESS_PROTO_CRATE_VERSION.to_string(),
            yellowstone_grpc_client_crate_version: HARNESS_CLIENT_CRATE_VERSION.to_string(),
            endpoint1_server_plugin_version: endpoint1
                .plugin_version
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
            endpoint2_server_plugin_version: "n/a (--solo)".to_string(),
            compatibility_warnings: warnings,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semver_parses_plain_triple() {
        let v = parse_semver("12.3.0").unwrap();
        assert_eq!(
            v,
            SemVer {
                major: 12,
                minor: 3,
                patch: 0,
            }
        );
    }

    #[test]
    fn semver_strips_build_metadata() {
        let v = parse_semver("12.2.0+solana.3.1.13").unwrap();
        assert_eq!(v.patch, 0);
        assert_eq!(v.major, 12);
        assert_eq!(v.minor, 2);
    }

    #[test]
    fn semver_strips_v_prefix() {
        let v = parse_semver("v12.2.0+triton-ext.solana.3.1.11").unwrap();
        assert_eq!(v.major, 12);
        assert_eq!(v.minor, 2);
    }

    #[test]
    fn semver_strips_prerelease() {
        let v = parse_semver("2.1.0-rc.4").unwrap();
        assert_eq!(v.patch, 0);
    }

    #[test]
    fn semver_rejects_non_numeric() {
        let err = parse_semver("12.x.0").unwrap_err();
        assert!(matches!(err, SemVerParseError::Component { .. }));
    }

    #[test]
    fn semver_rejects_short_input() {
        let err = parse_semver("12.3").unwrap_err();
        assert!(matches!(err, SemVerParseError::Shape { .. }));
    }

    fn sv(major: u32, minor: u32, patch: u32) -> SemVer {
        SemVer {
            major,
            minor,
            patch,
        }
    }

    #[test]
    fn classify_accepts_same_version() {
        let h = sv(12, 3, 0);
        let outcome = classify(h, Some(h), Some(h), Some("yellowstone-grpc-geyser"));
        assert!(matches!(outcome, Compatibility::Accept));
    }

    #[test]
    fn classify_warns_on_newer_server() {
        let h = sv(12, 3, 0);
        let s = sv(12, 4, 0);
        let outcome = classify(h, Some(s), Some(s), Some("yellowstone-grpc-geyser"));
        assert!(matches!(outcome, Compatibility::Warn { .. }));
    }

    #[test]
    fn classify_refuses_older_major() {
        let h = sv(12, 3, 0);
        let s = sv(11, 9, 9);
        let outcome = classify(h, Some(s), Some(s), Some("yellowstone-grpc-geyser"));
        assert!(matches!(
            outcome,
            Compatibility::RefuseOlderMajor {
                harness: 12,
                server: 11
            }
        ));
    }

    #[test]
    fn classify_warns_on_older_minor_same_major() {
        // the proto policy spec contradiction resolution: within-major drift is a
        // warning, not a refusal. The protobuf wire format is
        // forward-compatible for added fields.
        let h = sv(12, 3, 0);
        let s = sv(12, 1, 0);
        let outcome = classify(h, Some(s), Some(sv(12, 2, 0)), Some("yellowstone-grpc-geyser"));
        assert!(matches!(outcome, Compatibility::Warn { .. }));
    }

    #[test]
    fn classify_refuses_baseline_when_plugin_below_minimum() {
        // Plugin version 12.1.x is below the yellowstone 12.2.0 baseline →
        // refuse, regardless of where the proto version lands.
        let h = sv(12, 3, 0);
        let proto = sv(12, 3, 0);
        let plugin = sv(12, 1, 5);
        let outcome = classify(h, Some(proto), Some(plugin), Some("yellowstone-grpc-geyser"));
        assert!(matches!(outcome, Compatibility::RefuseBaseline { .. }));
    }

    #[test]
    fn classify_accepts_baseline_plugin_with_older_proto() {
        // Real QN response: plugin 12.2.0 (at baseline), proto 12.1.0
        // (older than our build 12.3.0). Pre-fix this was refused; now
        // it warns and proceeds.
        let harness_build = sv(12, 3, 0);
        let server_proto = sv(12, 1, 0);
        let server_plugin = sv(12, 2, 0);
        let outcome = classify(
            harness_build,
            Some(server_proto),
            Some(server_plugin),
            Some("yellowstone-grpc-geyser"),
        );
        assert!(
            matches!(outcome, Compatibility::Warn { .. }),
            "expected Warn for QN's 12.2.0/12.1.0 combo, got {outcome:?}"
        );
    }

    #[test]
    fn classify_refuses_baseline_for_richat_when_below_minimum() {
        // richat at its own major is the comparison axis; 2.0.x < 2.1.0.
        let h = sv(12, 3, 0);
        // Hypothetical server reporting richat as plugin but a yellowstone-
        // compatible proto. baseline applies to plugin version.
        let proto = sv(12, 3, 0);
        let plugin = sv(2, 0, 9);
        let outcome = classify(h, Some(proto), Some(plugin), Some("richat"));
        assert!(matches!(outcome, Compatibility::RefuseBaseline { .. }));
    }

    #[test]
    fn evaluate_accepts_json_with_proto_field() {
        let raw = r#"{"package":"yellowstone-grpc-geyser","version":"12.3.0","proto":"12.3.0","solana":"3.1.13"}"#;
        let v = evaluate(raw);
        assert_eq!(v.package.as_deref(), Some("yellowstone-grpc-geyser"));
        assert_eq!(v.proto_version.as_deref(), Some("12.3.0"));
        assert!(matches!(v.compatibility, Compatibility::Accept));
    }

    #[test]
    fn evaluate_handles_qn_nested_version_blob() {
        // Real-world shape from QN-hosted Yellowstone: fields under
        // `version`, alongside an `extra` block.
        let raw = r#"{"version":{"package":"yellowstone-grpc-geyser","version":"12.2.0+solana.3.1.13","proto":"12.1.0+solana.3.1.13","solana":"3.1.13","git":"d446973","rustc":"1.86.0","buildts":"2026-04-10T21:46:38.893308195Z"},"extra":{"hostname":"yellowstone-test-host-0001"}}"#;
        let v = evaluate(raw);
        assert_eq!(v.package.as_deref(), Some("yellowstone-grpc-geyser"));
        assert_eq!(v.plugin_version.as_deref(), Some("12.2.0+solana.3.1.13"));
        assert_eq!(v.proto_version.as_deref(), Some("12.1.0+solana.3.1.13"));
        // Plugin 12.2.0 meets the yellowstone baseline; proto 12.1.0 is
        // older than the harness build (12.3.0) but same major → warn,
        // not refuse.
        assert!(
            !v.compatibility.is_refusal(),
            "expected non-refusal for QN 12.2/12.1 combo, got {:?}",
            v.compatibility
        );
        assert!(matches!(v.compatibility, Compatibility::Warn { .. }));
    }

    #[test]
    fn evaluate_falls_back_to_version_when_proto_missing() {
        let raw = r#"{"package":"yellowstone-grpc-geyser","version":"12.3.0","solana":"3.1.13"}"#;
        let v = evaluate(raw);
        assert_eq!(v.proto_version.as_deref(), Some("12.3.0"));
    }

    #[test]
    fn evaluate_handles_non_json_version_string() {
        let raw = "12.3.0";
        let v = evaluate(raw);
        assert_eq!(v.plugin_version.as_deref(), Some("12.3.0"));
        // No `package` reported, so baseline check skipped.
        assert!(matches!(v.compatibility, Compatibility::Accept));
        assert!(
            v.warnings.iter().any(|w| w.contains("not JSON")),
            "want non-JSON warning, got {:?}",
            v.warnings
        );
    }

    #[test]
    fn evaluate_refuses_plugin_below_baseline() {
        // Plugin 12.1.0 is below the yellowstone 12.2.0 baseline → refuse.
        let raw = r#"{"package":"yellowstone-grpc-geyser","version":"12.1.0","proto":"12.1.0"}"#;
        let v = evaluate(raw);
        assert!(v.compatibility.is_refusal());
    }

    #[test]
    fn proto_metadata_collates_warnings_with_endpoint_labels() {
        let e1 = evaluate("12.3.0"); // non-JSON warning
        let e2 = evaluate(r#"{"package":"yellowstone-grpc-geyser","version":"12.3.0","proto":"12.3.0"}"#);
        let m = ProtoMetadata::from_endpoints(&e1, &e2);
        assert_eq!(m.endpoint1_server_plugin_version, "12.3.0");
        assert!(m
            .compatibility_warnings
            .iter()
            .any(|w| w.starts_with("endpoint1:")));
        assert!(!m
            .compatibility_warnings
            .iter()
            .any(|w| w.starts_with("endpoint2:")));
    }

    #[test]
    fn harness_versions_are_populated_by_build_script() {
        // Sanity: the build script must have set both env vars and they
        // must be parseable as semver.
        assert!(parse_semver(HARNESS_PROTO_CRATE_VERSION).is_ok());
        assert!(parse_semver(HARNESS_CLIENT_CRATE_VERSION).is_ok());
    }
}
