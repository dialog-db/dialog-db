//! `wbg-pool` is a drop-in replacement for `wasm-bindgen-test-runner` that
//! amortizes browser startup across an entire test session. A background
//! daemon owns one headless Chrome; every runner invocation becomes a fresh
//! tab on a fresh `*.localhost` origin inside that browser, so per-test
//! process spawns (the model `cargo nextest` uses) cost tab-creation instead
//! of browser-boot, while each test still gets pristine origin-scoped
//! storage (IndexedDB, OPFS, localStorage, caches).

mod cli;
mod daemon;
mod protocol;
mod shim;
mod suite;

use anyhow::Result;

fn main() -> Result<()> {
    let args: Vec<std::ffi::OsString> = std::env::args_os().collect();

    // `wbg-pool daemon ...` runs the shared-browser daemon. Any other
    // invocation is a libtest-style runner call from a test harness
    // (cargo test / cargo nextest), where the first argument is the path
    // to a compiled wasm test binary.
    if args.get(1).map(|arg| arg == "daemon").unwrap_or(false) {
        let mut daemon_args = vec![args[0].clone()];
        daemon_args.extend_from_slice(&args[2..]);
        daemon::main(daemon_args)
    } else {
        shim::main(args)
    }
}
