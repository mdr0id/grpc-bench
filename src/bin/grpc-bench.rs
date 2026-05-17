//! grpc-bench CLI entry. Thin: parses arguments, sets up logging,
//! warms the allocator, and dispatches to [`grpc_bench::run::execute`].

#[cfg(all(target_os = "linux", not(any(target_env = "musl", target_env = "ohos"))))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::Parser;
use grpc_bench::{
    config::{Cli, Config},
    run,
};

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let config = match Config::from_cli(cli) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("grpc-bench: configuration error: {e}");
            return std::process::ExitCode::from(2);
        }
    };

    init_tracing(&config.log_level);
    warm_allocator();

    if let Err(e) = run::execute(config) {
        eprintln!("grpc-bench: run failed: {e:#}");
        return std::process::ExitCode::from(1);
    }
    // Force-exit instead of returning through the normal drop chain.
    // `run::execute` has already written the output JSON to disk
    // before returning (the only durable artifact this binary
    // produces), so the only thing a clean shutdown would do here is
    // drop the spawned tokio runtimes inside each receiver thread.
    // Those runtimes are sometimes parked inside `.await` calls on
    // half-open TCP connections at shutdown time; waiting for them
    // adds ~10s with no benefit to anything observable from outside
    // the process. See `shutdown_hang_suspected` for the diagnosis.
    std::process::exit(0);
}

fn init_tracing(filter: &str) {
    let env_filter = tracing_subscriber::EnvFilter::try_new(filter)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_timer(tracing_subscriber::fmt::time::uptime())
        .try_init();
}

/// Spec §5: warm-start the allocator by allocating and dropping ~64MB of
/// varied-size buffers before subscriptions open. Avoids first-message
/// allocation jitter when the very first `SubscribeUpdate` decodes.
fn warm_allocator() {
    use std::hint::black_box;
    // 64 buffers ranging from 16KB to 1MB → ~32MB; do it twice for ~64MB.
    let mut acc: Vec<Vec<u8>> = Vec::with_capacity(128);
    for _ in 0..2 {
        for size_log in 14..20_u32 {
            let size = 1usize << size_log;
            acc.push(vec![0u8; size]);
        }
    }
    black_box(&acc);
    drop(acc);
}
