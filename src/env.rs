//! Host metadata and precision-feature detection (the precision posture).
//!
//! `HostMetadata` is captured once at startup and embedded verbatim in the
//! output JSON's `host_metadata` block (the output JSON schema). It also drives the
//! prominent startup warnings the spec mandates when timing-critical
//! features are missing.
//!
//! Detection paths that read from `/proc` / `/sys` are Linux-only and
//! `#[cfg(target_os = "linux")]`-gated. On other platforms (dev hosts) the
//! corresponding fields are `None` and [`HostMetadata::warnings`] reflects
//! the degraded posture so the result JSON is honest about its provenance.

use serde::Serialize;

use crate::collect::AffinitySpec;

/// Snapshot of host conditions that affect timing precision.
#[derive(Debug, Clone, Serialize)]
pub struct HostMetadata {
    /// `uname -r` output.
    pub kernel: String,
    /// CPU brand string (`/proc/cpuinfo` `model name` on Linux,
    /// `sysctl machdep.cpu.brand_string` elsewhere).
    pub cpu_model: String,
    /// First-core cpufreq governor on Linux (`performance`, `powersave`,
    /// `schedutil`, ...). `None` if the governor file is unreadable or the
    /// platform doesn't expose it.
    pub cpu_governor: Option<String>,
    /// Transparent hugepage policy: `always`, `madvise`, `never`. `None`
    /// off-Linux.
    pub transparent_hugepage: Option<String>,
    /// Allocator name compiled into this binary.
    pub allocator: &'static str,
    /// Whether realtime scheduling was requested and accepted.
    pub realtime_priority: bool,
    /// Whether kernel timestamps (`SO_TIMESTAMPNS`) are active.
    pub kernel_timestamps: bool,
    /// Flattened union of every core referenced by the affinity plan.
    /// Retained for backwards compatibility with consumers (jq scripts,
    /// thorofare's summarizer) that read the original flat-vec shape;
    /// the structured layout lives in `cpu_affinity_plan`.
    pub cpu_affinity: Vec<u32>,
    /// Structured affinity layout — per-endpoint receiver cores plus
    /// optional processor / control pins. Skipped when no pin was
    /// requested so the JSON stays terse on `--realtime`-only runs.
    #[serde(skip_serializing_if = "AffinitySpec::is_empty")]
    pub cpu_affinity_plan: AffinitySpec,
    /// Whether NTP reports a synced clock (via `chronyc tracking` or
    /// `timedatectl status`). `None` if no NTP tool is available.
    pub ntp_synced: Option<bool>,
    /// Aggregate of precision-degradation warnings emitted at startup.
    /// Empty when everything is nominal.
    pub warnings: Vec<String>,
}

/// Build a [`HostMetadata`] for the current process.
///
/// `cli_realtime_requested` mirrors the `--realtime` CLI flag, but the
/// resulting [`HostMetadata::realtime_priority`] only reports `true` if the
/// runtime actually succeeded in setting `SCHED_FIFO`. The realtime apply
/// itself happens in [`crate::collect`] when receiver threads spawn; this
/// detector takes the outcome as input.
///
/// `kernel_timestamps_active` mirrors whether the socket-control reader
/// succeeded in enabling `SO_TIMESTAMPNS`. As with realtime, the actual
/// enable happens elsewhere; this is the report.
///
/// `cpu_affinity` is the operator's request (the parsed CLI spec), not
/// the realized affinity — the realized affinity is identical when the
/// affinity-set succeeded, which is the only path that reaches summary.
#[must_use]
pub fn collect(
    cli_realtime_requested: bool,
    realtime_applied: bool,
    kernel_timestamps_active: bool,
    cpu_affinity: AffinitySpec,
) -> HostMetadata {
    let mut warnings: Vec<String> = Vec::new();

    let kernel = read_kernel(&mut warnings);
    let cpu_model = read_cpu_model(&mut warnings);
    let cpu_governor = read_cpu_governor(&mut warnings);
    let transparent_hugepage = read_thp(&mut warnings);
    let allocator = current_allocator_name();
    let ntp_synced = read_ntp_synced(&mut warnings);

    if cli_realtime_requested && !realtime_applied {
        warnings.push(
            "SCHED_FIFO requested via --realtime but the syscall was rejected; \
             continuing with default scheduling. Run as root or grant CAP_SYS_NICE \
             for credible measurements."
                .to_string(),
        );
    }
    if !kernel_timestamps_active {
        warnings.push(
            "SO_TIMESTAMPNS unavailable — receive timestamps will be captured in \
             user space after protobuf decode. Sub-10ms deltas will include \
             scheduling jitter."
                .to_string(),
        );
    }
    if !cfg!(target_os = "linux") {
        warnings.push(format!(
            "running on {} — CPU pinning, SCHED_FIFO, jemalloc, and SO_TIMESTAMPNS \
             are all Linux-only. Use Linux for production runs.",
            std::env::consts::OS
        ));
    }
    if matches!(cpu_governor.as_deref(), Some("powersave")) {
        warnings.push(
            "cpufreq governor is `powersave` — clock frequency will fluctuate and \
             inflate p99 tail. Switch to `performance` for credible measurements."
                .to_string(),
        );
    }
    if matches!(transparent_hugepage.as_deref(), Some("always")) {
        warnings.push(
            "transparent_hugepage = `always` can introduce TLB-shootdown jitter; \
             prefer `madvise` for benchmark hosts."
                .to_string(),
        );
    }

    let cpu_affinity_flat = cpu_affinity.all_cores();
    HostMetadata {
        kernel,
        cpu_model,
        cpu_governor,
        transparent_hugepage,
        allocator,
        realtime_priority: realtime_applied,
        kernel_timestamps: kernel_timestamps_active,
        cpu_affinity: cpu_affinity_flat,
        cpu_affinity_plan: cpu_affinity,
        ntp_synced,
        warnings,
    }
}

fn current_allocator_name() -> &'static str {
    // Mirrors the cfg gates in bin/grpc-bench.rs's allocator selection.
    #[cfg(all(target_os = "linux", not(any(target_env = "musl", target_env = "ohos"))))]
    {
        "jemalloc"
    }
    #[cfg(not(all(target_os = "linux", not(any(target_env = "musl", target_env = "ohos")))))]
    {
        "system"
    }
}

#[cfg(target_os = "linux")]
fn read_kernel(_warnings: &mut Vec<String>) -> String {
    use std::process::Command;
    Command::new("uname")
        .arg("-r")
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map_or_else(
            || "unknown".to_string(),
            |out| String::from_utf8_lossy(&out.stdout).trim().to_string(),
        )
}

#[cfg(not(target_os = "linux"))]
#[allow(clippy::ptr_arg)] // Signature-parity with the Linux variant.
fn read_kernel(_warnings: &mut Vec<String>) -> String {
    // Best-effort on dev hosts so the warning JSON is still informative.
    use std::process::Command;
    Command::new("uname")
        .arg("-sr")
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map_or_else(
            || std::env::consts::OS.to_string(),
            |out| String::from_utf8_lossy(&out.stdout).trim().to_string(),
        )
}

#[cfg(target_os = "linux")]
fn read_cpu_model(warnings: &mut Vec<String>) -> String {
    match std::fs::read_to_string("/proc/cpuinfo") {
        Ok(s) => s
            .lines()
            .find_map(|l| l.strip_prefix("model name").and_then(|tail| tail.split(':').nth(1)))
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        Err(e) => {
            warnings.push(format!("failed to read /proc/cpuinfo: {e}"));
            "unknown".to_string()
        }
    }
}

#[cfg(not(target_os = "linux"))]
#[allow(clippy::ptr_arg)] // Signature-parity with the Linux variant.
fn read_cpu_model(_warnings: &mut Vec<String>) -> String {
    use std::process::Command;
    // sysinfo has CPU brand support cross-platform; use that to keep the
    // dependency footprint small without adding another crate.
    let mut sys = sysinfo::System::new();
    sys.refresh_cpu_all();
    if let Some(cpu) = sys.cpus().first() {
        let brand = cpu.brand().trim();
        if !brand.is_empty() {
            return brand.to_string();
        }
    }
    // Fallback to sysctl on darwin.
    Command::new("sysctl")
        .arg("-n")
        .arg("machdep.cpu.brand_string")
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map_or_else(
            || "unknown".to_string(),
            |out| String::from_utf8_lossy(&out.stdout).trim().to_string(),
        )
}

#[cfg(target_os = "linux")]
fn read_cpu_governor(warnings: &mut Vec<String>) -> Option<String> {
    let path = "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor";
    match std::fs::read_to_string(path) {
        Ok(s) => Some(s.trim().to_string()),
        Err(e) => {
            // `NotFound` is expected on VMs without cpufreq; don't warn there.
            if e.kind() != std::io::ErrorKind::NotFound {
                warnings.push(format!("failed to read {path}: {e}"));
            }
            None
        }
    }
}

#[cfg(not(target_os = "linux"))]
#[allow(clippy::ptr_arg)] // Signature-parity with the Linux variant.
fn read_cpu_governor(_warnings: &mut Vec<String>) -> Option<String> {
    None
}

#[cfg(target_os = "linux")]
fn read_thp(warnings: &mut Vec<String>) -> Option<String> {
    let path = "/sys/kernel/mm/transparent_hugepage/enabled";
    match std::fs::read_to_string(path) {
        Ok(s) => {
            // The file looks like: "always [madvise] never"; the bracketed
            // entry is the active policy.
            let active = s
                .split_whitespace()
                .find_map(|tok| tok.strip_prefix('[').and_then(|t| t.strip_suffix(']')))
                .map_or_else(|| s.trim().to_string(), str::to_string);
            Some(active)
        }
        Err(e) => {
            if e.kind() != std::io::ErrorKind::NotFound {
                warnings.push(format!("failed to read {path}: {e}"));
            }
            None
        }
    }
}

#[cfg(not(target_os = "linux"))]
#[allow(clippy::ptr_arg)] // Signature-parity with the Linux variant.
fn read_thp(_warnings: &mut Vec<String>) -> Option<String> {
    None
}

#[cfg(target_os = "linux")]
fn read_ntp_synced(warnings: &mut Vec<String>) -> Option<bool> {
    use std::process::Command;
    // Prefer `timedatectl` — most server distros have it.
    if let Ok(out) = Command::new("timedatectl").arg("show").output() {
        if out.status.success() {
            let s = String::from_utf8_lossy(&out.stdout);
            for line in s.lines() {
                if let Some(val) = line.strip_prefix("NTPSynchronized=") {
                    return Some(val.trim() == "yes");
                }
            }
        }
    }
    // Fallback: chronyc.
    if let Ok(out) = Command::new("chronyc").arg("tracking").output() {
        if out.status.success() {
            let s = String::from_utf8_lossy(&out.stdout);
            // "Leap status     : Normal" indicates synced.
            if s.lines().any(|l| {
                l.starts_with("Leap status") && l.split(':').nth(1).is_some_and(|v| v.trim() == "Normal")
            }) {
                return Some(true);
            }
            return Some(false);
        }
    }
    warnings.push(
        "could not query NTP sync state (timedatectl/chronyc not available); \
         field reported as unknown in host_metadata"
            .to_string(),
    );
    None
}

#[cfg(not(target_os = "linux"))]
#[allow(clippy::ptr_arg)] // Signature-parity with the Linux variant.
fn read_ntp_synced(_warnings: &mut Vec<String>) -> Option<bool> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flat_spec(cores: &[u32]) -> AffinitySpec {
        AffinitySpec::from_flat_vec(cores).expect("test cores deduped")
    }

    #[test]
    fn collect_produces_warnings_for_dev_host() {
        // On the macOS dev host the cross-platform warning is mandatory and
        // the kernel-timestamps warning fires when we report `false`.
        let m = collect(false, false, false, AffinitySpec::default());
        assert!(
            m.warnings
                .iter()
                .any(|w| w.contains("SO_TIMESTAMPNS unavailable")),
            "expected kernel_timestamps warning in {:?}",
            m.warnings
        );
        if !cfg!(target_os = "linux") {
            assert!(
                m.warnings.iter().any(|w| w.contains("Linux-only")),
                "expected non-Linux warning in {:?}",
                m.warnings
            );
        }
    }

    #[test]
    fn realtime_rejected_warning_only_when_requested() {
        let m = collect(true, false, true, AffinitySpec::default());
        assert!(m
            .warnings
            .iter()
            .any(|w| w.contains("SCHED_FIFO requested")));
        let m2 = collect(false, false, true, AffinitySpec::default());
        assert!(!m2
            .warnings
            .iter()
            .any(|w| w.contains("SCHED_FIFO requested")));
    }

    #[test]
    fn cpu_affinity_round_trips_into_metadata() {
        // Flat 4-core spec retains its union form in the legacy field
        // and surfaces the structured layout in `cpu_affinity_plan`.
        let m = collect(false, false, true, flat_spec(&[2, 3, 4, 5]));
        assert_eq!(m.cpu_affinity, vec![2, 3, 4, 5]);
        assert_eq!(m.cpu_affinity_plan.endpoint1, vec![2]);
        assert_eq!(m.cpu_affinity_plan.processor, Some(4));
    }

    #[test]
    fn structured_cpu_affinity_round_trips_plan() {
        let spec = AffinitySpec::parse("ep1=2,3:ep2=4,5:proc=6:ctrl=7").unwrap();
        let m = collect(false, false, true, spec);
        assert_eq!(m.cpu_affinity, vec![2, 3, 4, 5, 6, 7]);
        assert_eq!(m.cpu_affinity_plan.endpoint1, vec![2, 3]);
        assert_eq!(m.cpu_affinity_plan.endpoint2, vec![4, 5]);
        assert_eq!(m.cpu_affinity_plan.processor, Some(6));
        assert_eq!(m.cpu_affinity_plan.control, Some(7));
    }

    #[test]
    fn nominal_realtime_and_timestamps_emit_no_precision_warnings() {
        let m = collect(true, true, true, flat_spec(&[2, 3, 4, 5]));
        assert!(m.realtime_priority);
        assert!(m.kernel_timestamps);
        assert!(!m
            .warnings
            .iter()
            .any(|w| w.contains("SO_TIMESTAMPNS unavailable")));
        assert!(!m
            .warnings
            .iter()
            .any(|w| w.contains("SCHED_FIFO requested")));
    }
}
