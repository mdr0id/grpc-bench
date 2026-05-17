//! Timing primitives shared by every receiver task.
//!
//! Two clocks are captured per event (the precision posture):
//! - `mono_ns`: `CLOCK_MONOTONIC` nanoseconds. Used for all duration math
//!   (deltas between endpoints, inter-message gaps, reconnect TTFM).
//! - `wall_ms`: epoch milliseconds. Used only for UI timelines and
//!   reconnect-correlation; never for duration math (NTP slew would corrupt
//!   the answer).
//!
//! Two timestamp sources exist:
//! 1. **Kernel** (preferred). Via `SO_TIMESTAMPNS` on the underlying TCP
//!    socket the kernel attaches a wire-arrival timestamp to every received
//!    segment. The TCP stack passes it up through a `cmsg` on `recvmsg`.
//!    See [`kernel_ts`] for the Linux-only implementation.
//! 2. **User-space fallback**. `Instant::now()` captured immediately after
//!    the protobuf decode returns. Includes tokio scheduling jitter and
//!    allocator pressure, which is meaningful at sub-10ms deltas — the
//!    fallback must therefore be loud at startup (the precision posture).
//!
//! [`now_user_space`] always succeeds and produces the user-space pair.
//! When kernel timestamps are active, [`from_kernel_realtime_ns`] turns a
//! kernel-reported `CLOCK_REALTIME` value into both a monotonic and a wall
//! value by subtracting the realtime/monotonic offset captured at startup.

pub mod kernel_ts;

use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// A `(mono_ns, wall_ms)` pair, the canonical timestamp for an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventTimestamp {
    /// Monotonic clock value, nanoseconds since boot or arbitrary epoch.
    pub mono_ns: u64,
    /// Wall clock, milliseconds since Unix epoch. Use only for display.
    pub wall_ms: u64,
}

impl EventTimestamp {
    /// Whether this timestamp is the user-space fallback rather than a
    /// kernel-attached timestamp. Tracking which is which lets the summary
    /// JSON honestly report timing provenance.
    #[must_use]
    pub fn new(mono_ns: u64, wall_ms: u64) -> Self {
        Self { mono_ns, wall_ms }
    }
}

/// Pair the [`Instant`]-relative monotonic origin with the wall-clock
/// origin captured at the same point in time. Used by [`now_user_space`]
/// to translate `Instant` values into `mono_ns` and to bridge kernel
/// `CLOCK_REALTIME` timestamps into the monotonic axis.
#[derive(Debug, Clone, Copy)]
pub struct ClockOrigin {
    mono_origin: Instant,
    /// `mono_origin` translated to a u64 nanosecond count via a fixed,
    /// arbitrary anchor (the process-start time). The anchor cancels out
    /// in all duration arithmetic.
    mono_anchor_ns: u64,
    /// Wall-clock epoch ms captured simultaneously with `mono_origin`.
    /// Used to map a kernel `CLOCK_REALTIME` reading into the monotonic
    /// axis: `mono = mono_anchor + (kernel_realtime_ns - realtime_anchor)`.
    realtime_anchor_ns: u64,
}

impl ClockOrigin {
    /// Capture the current `(mono, wall)` pair. Call once at startup.
    #[must_use]
    pub fn capture() -> Self {
        let mono_origin = Instant::now();
        let realtime_anchor_ns = realtime_now_ns();
        Self {
            mono_origin,
            mono_anchor_ns: 0,
            realtime_anchor_ns,
        }
    }

    /// User-space "now" pair, using `Instant::now()` for mono. This is the
    /// per-event fallback when kernel timestamps are unavailable.
    #[must_use]
    pub fn now_user_space(&self) -> EventTimestamp {
        let mono_ns = self.mono_anchor_ns
            + u64::try_from(self.mono_origin.elapsed().as_nanos())
                .unwrap_or(u64::MAX);
        let wall_ms = realtime_now_ns() / 1_000_000;
        EventTimestamp { mono_ns, wall_ms }
    }

    /// Translate a kernel-reported `CLOCK_REALTIME` nanosecond reading into
    /// the monotonic axis plus wall-clock ms.
    ///
    /// `realtime_anchor_ns` is captured once at startup alongside the
    /// monotonic origin. The mono-axis value here is therefore:
    ///   `mono_anchor + (kernel_realtime_ns - realtime_anchor_ns)`
    /// — which is valid only as long as the wall clock did not slew
    /// substantially during the run. NTP slew is microseconds per second;
    /// at the 1000-slot / few-minute runs the spec describes, this is
    /// noise. For 24-hour soaks the operator should ensure NTP is configured
    /// (the `host_metadata.ntp_synced` warning fires otherwise).
    #[must_use]
    pub fn from_kernel_realtime_ns(&self, kernel_realtime_ns: u64) -> EventTimestamp {
        let delta_ns = kernel_realtime_ns.saturating_sub(self.realtime_anchor_ns);
        let mono_ns = self.mono_anchor_ns.saturating_add(delta_ns);
        let wall_ms = kernel_realtime_ns / 1_000_000;
        EventTimestamp { mono_ns, wall_ms }
    }
}

/// Read `CLOCK_REALTIME` and report nanoseconds since the Unix epoch.
#[must_use]
pub fn realtime_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn user_space_pair_is_monotonic() {
        let origin = ClockOrigin::capture();
        let a = origin.now_user_space();
        sleep(Duration::from_millis(2));
        let b = origin.now_user_space();
        assert!(b.mono_ns >= a.mono_ns, "mono should not go backward");
        assert!(b.wall_ms >= a.wall_ms, "wall should not go backward");
    }

    #[test]
    fn user_space_delta_matches_sleep_within_jitter() {
        let origin = ClockOrigin::capture();
        let a = origin.now_user_space();
        sleep(Duration::from_millis(10));
        let b = origin.now_user_space();
        let delta_ms = (b.mono_ns - a.mono_ns) / 1_000_000;
        assert!(delta_ms >= 8, "expected ~10ms, got {delta_ms}ms");
        // Generous upper bound; CI hosts can be very jittery.
        assert!(delta_ms < 200, "delta {delta_ms} ms suspiciously large");
    }

    #[test]
    fn from_kernel_realtime_round_trips_into_monotonic() {
        let origin = ClockOrigin::capture();
        // A kernel timestamp 5ms past the realtime anchor should produce a
        // mono delta of ~5ms from the anchor.
        let ts = origin.from_kernel_realtime_ns(origin.realtime_anchor_ns + 5_000_000);
        let delta_ns = ts.mono_ns.saturating_sub(origin.mono_anchor_ns);
        assert!(
            (4_500_000..=5_500_000).contains(&delta_ns),
            "expected ~5ms mono delta, got {delta_ns}ns"
        );
    }

    #[test]
    fn realtime_now_ns_is_recent() {
        let now = realtime_now_ns();
        // Sanity: > year 2024 (1.7e18 ns) and < year 2100.
        assert!(now > 1_700_000_000_000_000_000);
        assert!(now < 4_000_000_000_000_000_000);
    }
}
