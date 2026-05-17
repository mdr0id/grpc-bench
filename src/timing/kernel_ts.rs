//! Kernel-level receive timestamps (`SO_TIMESTAMPNS`, Linux-only).
//!
//! Two operations are needed to capture wire-arrival timestamps:
//!
//! 1. Set `SO_TIMESTAMPNS` (or `SO_TIMESTAMPNS_NEW` on newer kernels) on
//!    the raw socket file descriptor. From that point on, every `recvmsg`
//!    on the socket will deliver a `SCM_TIMESTAMPNS` control message
//!    containing a `struct timespec` (`CLOCK_REALTIME`) of when the first
//!    byte of the segment arrived in the kernel.
//! 2. On each `recvmsg`, walk the control buffer with `CMSG_FIRSTHDR` /
//!    `CMSG_NXTHDR` and pull the timespec out of the `SCM_TIMESTAMPNS`
//!    cmsg if present.
//!
//! Per spec §12.D this is the only module that may use `unsafe`. Each
//! unsafe block carries a `// SAFETY:` comment explaining the invariant
//! that justifies it. The crate-level `#![deny(unsafe_code)]` is relaxed
//! here with a localized `#![allow(unsafe_code)]`.
//!
//! Integration with tonic — i.e. routing decoded gRPC frames back to the
//! per-segment kernel timestamp — lives in [`crate::subscribe`]. This
//! module exposes the raw enable / read primitives only.

#![allow(unsafe_code)] // SAFETY: see per-unsafe-block comments below.

use std::io;

/// Reasons `setsockopt(SO_TIMESTAMPNS)` may fail.
#[derive(Debug, thiserror::Error)]
pub enum KernelTimestampError {
    /// The current target OS does not expose `SO_TIMESTAMPNS`.
    #[error(
        "kernel timestamps are not supported on this OS — \
         per-event timestamps will fall back to user-space Instant::now()"
    )]
    Unsupported,
    /// The `setsockopt` syscall returned an error.
    #[error("setsockopt(SO_TIMESTAMPNS) failed: {source}")]
    SetSockOpt {
        /// Underlying `errno`-derived I/O error.
        #[source]
        source: io::Error,
    },
}

/// Enable `SO_TIMESTAMPNS` on the given raw socket file descriptor.
///
/// On non-Linux targets this always returns
/// [`KernelTimestampError::Unsupported`]; callers should treat that as a
/// signal to engage the user-space fallback (and emit the spec-mandated
/// loud startup warning, which [`crate::env`] already handles).
///
/// # Errors
/// - [`KernelTimestampError::Unsupported`] off Linux.
/// - [`KernelTimestampError::SetSockOpt`] if the syscall fails (raw
///   sockets, unsupported domain, EBADF, etc.).
#[cfg(target_os = "linux")]
pub fn enable_so_timestampns(fd: std::os::fd::RawFd) -> Result<(), KernelTimestampError> {
    let on: libc::c_int = 1;
    // SAFETY: `fd` is supplied by the caller; we read it but never own it
    // (no close on failure). `&on` is a valid pointer to a c_int with
    // length `size_of::<c_int>()`, which matches what the SOL_SOCKET /
    // SO_TIMESTAMPNS option expects. The call has no aliasing
    // requirements and is safe to invoke from any thread. On error the
    // kernel returns -1 and `errno` carries the reason.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TIMESTAMPNS,
            std::ptr::addr_of!(on).cast::<libc::c_void>(),
            std::mem::size_of_val(&on) as libc::socklen_t,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(KernelTimestampError::SetSockOpt {
            source: io::Error::last_os_error(),
        })
    }
}

/// Non-Linux stub: always returns [`KernelTimestampError::Unsupported`].
/// Non-Linux stub.
///
/// # Errors
/// Always returns [`KernelTimestampError::Unsupported`].
#[cfg(not(target_os = "linux"))]
pub fn enable_so_timestampns(_fd: i32) -> Result<(), KernelTimestampError> {
    Err(KernelTimestampError::Unsupported)
}

/// Parse `SCM_TIMESTAMPNS` out of a `recvmsg` control buffer and return
/// the timestamp in nanoseconds since the Unix epoch
/// (`CLOCK_REALTIME`). `None` if the buffer contains no
/// `SCM_TIMESTAMPNS` cmsg.
///
/// Callers must pass the `msghdr` they handed to `recvmsg`, after the
/// syscall has returned. The control buffer pointer / length on the
/// `msghdr` must point at the buffer of size returned in `msg_controllen`
/// — that is, the kernel-overwritten length, not the caller-provided
/// capacity. The function is purely a parser and does no syscalls of its
/// own.
///
/// # Safety
/// - `msghdr` must have been populated by a successful `recvmsg` call.
/// - `msg_control` (the control buffer base) must point to a memory
///   region of at least `msg_controllen` bytes that the caller still owns
///   and that is properly aligned for `struct cmsghdr`.
/// - The function only reads memory; it never writes through the pointer.
#[cfg(target_os = "linux")]
pub unsafe fn read_scm_timestampns_realtime_ns(msghdr: &libc::msghdr) -> Option<u64> {
    // SAFETY: per the contract above, `msghdr` was populated by a
    // successful `recvmsg`, so `msg_control` (when `msg_controllen > 0`)
    // is a valid pointer to a buffer the kernel wrote into and the caller
    // still owns. `CMSG_FIRSTHDR` returns null when there are no
    // cmsgs, which we check before deref.
    let mut cmsg: *const libc::cmsghdr = unsafe { libc::CMSG_FIRSTHDR(msghdr) };
    while !cmsg.is_null() {
        // SAFETY: `CMSG_FIRSTHDR` / `CMSG_NXTHDR` return either null or a
        // valid pointer into the control buffer; we just checked non-null
        // above (and at loop bottom for `CMSG_NXTHDR`). The pointer is
        // aligned and points at a fully-initialized `cmsghdr` because the
        // kernel wrote it.
        let hdr = unsafe { &*cmsg };
        if hdr.cmsg_level == libc::SOL_SOCKET && hdr.cmsg_type == libc::SCM_TIMESTAMPNS {
            // SAFETY: `CMSG_DATA(cmsg)` is the kernel-defined data offset
            // inside the cmsg block; for `SCM_TIMESTAMPNS` the payload is
            // a `struct timespec` (two longs on Linux), which fits within
            // the cmsg payload area sized by the kernel. We read by
            // copying `timespec` bytes into a stack value to avoid any
            // alignment assumption on the cmsg payload.
            let data_ptr: *const libc::timespec = unsafe { libc::CMSG_DATA(cmsg) }.cast();
            let mut ts = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            // SAFETY: `data_ptr` points to at least `size_of::<timespec>()`
            // valid bytes (the kernel wrote a full struct), and `&mut ts`
            // is a unique, properly aligned destination of equal size.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    data_ptr.cast::<u8>(),
                    std::ptr::addr_of_mut!(ts).cast::<u8>(),
                    std::mem::size_of::<libc::timespec>(),
                );
            }
            let secs = u64::try_from(ts.tv_sec).unwrap_or(0);
            let nsecs = u64::try_from(ts.tv_nsec).unwrap_or(0);
            return Some(secs.saturating_mul(1_000_000_000).saturating_add(nsecs));
        }
        // SAFETY: passing the same `msghdr` and the current `cmsg`
        // pointer to `CMSG_NXTHDR`, both of which are valid per the
        // invariants above.
        cmsg = unsafe { libc::CMSG_NXTHDR(msghdr, cmsg) };
    }
    None
}

/// Non-Linux stub. Always returns `None` because no `recvmsg` control
/// path is wired here.
#[cfg(not(target_os = "linux"))]
#[must_use]
#[allow(clippy::missing_const_for_fn)] // Signature parity with Linux variant.
pub fn read_scm_timestampns_realtime_ns(_msghdr: &()) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn enable_returns_unsupported_off_linux() {
        let err = enable_so_timestampns(0).unwrap_err();
        assert!(matches!(err, KernelTimestampError::Unsupported));
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn read_returns_none_off_linux() {
        assert!(read_scm_timestampns_realtime_ns(&()).is_none());
    }
}
