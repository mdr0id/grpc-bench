//! Captures the resolved yellowstone-grpc-proto and yellowstone-grpc-client
//! versions from `Cargo.lock` and exposes them to the crate as build-time
//! environment variables. Read at runtime via `env!()` and reported in the
//! `proto_metadata` section of the output JSON (the output JSON schema, the proto policy).

use std::fs;

fn main() {
    println!("cargo:rerun-if-changed=Cargo.lock");
    println!("cargo:rerun-if-changed=build.rs");

    let lock = fs::read_to_string("Cargo.lock").expect("read Cargo.lock from package root");
    let proto = lookup_crate_version(&lock, "yellowstone-grpc-proto")
        .expect("yellowstone-grpc-proto missing from Cargo.lock");
    let client = lookup_crate_version(&lock, "yellowstone-grpc-client")
        .expect("yellowstone-grpc-client missing from Cargo.lock");

    println!("cargo:rustc-env=GRPC_BENCH_YELLOWSTONE_PROTO_VER={proto}");
    println!("cargo:rustc-env=GRPC_BENCH_YELLOWSTONE_CLIENT_VER={client}");
}

fn lookup_crate_version(lock: &str, crate_name: &str) -> Option<String> {
    // `Cargo.lock` is TOML — a sequence of `[[package]]` blocks. We don't
    // want to pull in a TOML dep just for this, so we scan in a tiny state
    // machine: any `name = "<crate>"` line, the next `version = "<v>"`
    // line is the answer.
    let want = format!("name = \"{crate_name}\"");
    let mut iter = lock.lines();
    while let Some(line) = iter.next() {
        if line.trim() != want {
            continue;
        }
        // In cargo-emitted lockfiles the version line is always immediately
        // after the name line within a `[[package]]` block, so we look only
        // at the next line. Anything else is a corrupt lockfile.
        let next = iter.next()?;
        return next
            .trim()
            .strip_prefix("version = \"")
            .and_then(|rest| rest.strip_suffix('"'))
            .map(str::to_string);
    }
    None
}
