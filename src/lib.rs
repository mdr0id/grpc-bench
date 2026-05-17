//! gRPC-bench — Solana Yellowstone gRPC comparative benchmark harness.
//!
//! Module layout follows spec §12.C with one rename: spec lists `match/` but
//! `match` is a Rust reserved keyword, so the module is named [`matching`]
//! instead. All other module names match the spec verbatim.
//!
//! Build target is Linux `x86_64`. Most of the precision features in §5
//! (kernel timestamps, `SCHED_FIFO`, jemalloc) are Linux-only and are
//! `#[cfg(target_os = "linux")]`-gated. On non-Linux dev hosts the harness
//! still compiles but logs a prominent startup warning that timing
//! precision is degraded; see [`env`] for the detection logic.

#![deny(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod collect;
pub mod config;
pub mod crossstream;
pub mod env;
pub mod matching;
pub mod programs;
pub mod proto;
pub mod raw;
pub mod run;
pub mod stability;
pub mod subscribe;
pub mod summary;
pub mod timing;
