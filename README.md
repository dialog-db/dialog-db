# wbg-pool

A pooled-browser test runner for [`wasm-bindgen-test`], drop-in compatible
with `wasm-bindgen-test-runner` and designed for [`cargo nextest`]'s
process-per-test execution model.

`wasm-bindgen-test-runner` starts a fresh chromedriver + headless Chrome and
re-runs `wasm-bindgen` codegen on **every invocation** — roughly six seconds
of startup before a single test executes. That is fine when one invocation
runs a whole test binary, but nextest invokes the runner **once per test**,
so a 1,400-test suite pays that startup tax 1,400 times (we watched a CI
suite spend 84 minutes running tests whose actual bodies finish in seconds).

`wbg-pool` keeps one headless Chrome alive in a background daemon and turns
each runner invocation into a **fresh tab on a fresh origin**:

- **Fast**: tab creation instead of browser boot, codegen cached per binary.
  Warm per-test overhead is ~0.2s. A 76-test suite that took 214s under the
  stock runner + nextest takes ~12s; a 1,717-test workspace whose CI test
  phase took 84 minutes finishes in ~16 (4 cores, debug builds — most of
  the remaining overhead is per-tab wasm compilation of large debug
  modules, a known future optimization).
- **Isolated**: every test runs on its own `t-<n>.localhost` origin, with
  pristine IndexedDB, OPFS, localStorage, caches and service worker
  registrations. This is *stronger* isolation than sharing a page (what
  `cargo test` does for a whole binary) and equivalent in practice to the
  stock runner's fresh-profile-per-run for storage-touching tests.
- **nextest-native**: because per-test processes are cheap again, you keep
  nextest's parallel scheduling, retries, per-test timings and partitioning.
  Concurrent invocations are served as concurrent tabs.

## Usage

Install the binary, then point your wasm target runner at it:

```toml
# .cargo/config.toml
[target.wasm32-unknown-unknown]
runner = 'wbg-pool'
```

That's it. `cargo test --target wasm32-unknown-unknown` and
`cargo nextest run --target wasm32-unknown-unknown` now run through the
pool. The first invocation spawns the daemon (and its browser) on demand;
the daemon exits on its own after five idle minutes. No workflow changes
are required for `cargo nextest --archive-file` setups either — extract and
run as usual with the runner configured.

Supported test configurations:

| `wasm_bindgen_test_configure!` | handling |
| --- | --- |
| `run_in_browser` | page main thread |
| `run_in_dedicated_worker` / `run_in_worker` | module worker |
| `run_in_shared_worker` | shared worker |
| `run_in_service_worker` | service worker (concurrent suites work — each run's origin gets its own registration) |
| node / emscripten modes | delegated to `wasm-bindgen-test-runner` |

Benchmarks (`--bench`) and coverage dumps are not supported yet; benches
should keep using the stock runner (the shim refuses rather than
misbehaving).

## How it works

```
cargo nextest ──spawns──▶ wbg-pool (shim, one per test)
                             │  POST /api/run {binary, filter, --exact, ...}
                             ▼
                      wbg-pool daemon ──── owns one headless Chrome (CDP)
                       │        │
                       │        └─ tab @ http://t-42.localhost:PORT/r/42/
                       │             └─ loads harness page → runs the test
                       │                  └─ POSTs libtest output back
                       └─ wasm-bindgen codegen, once per binary, cached
```

- The shim mirrors the stock runner's CLI. `--list` is answered locally by
  parsing the wasm exports (`__wbgt_*`), no browser involved.
- The daemon runs `wasm-bindgen` over a test binary the first time it is
  seen (exactly the `Bindgen` configuration the stock runner uses) and
  serves the output under a stable asset path.
- Each run gets a unique loopback origin — Chrome resolves any
  `*.localhost` subdomain to 127.0.0.1, and such origins are secure
  contexts, so OPFS, service workers and friends all work.
- The page (or worker harness) reports the libtest-formatted result back
  over `fetch`; the shim prints it and exits with the appropriate status,
  byte-compatible with the stock runner's output on both success and
  failure. On timeout the daemon scrapes whatever output the wedged page
  accumulated via CDP before closing the tab.

## Configuration

Everything is optional:

| Environment variable | Meaning |
| --- | --- |
| `CHROME` / `WBG_POOL_BROWSER` | Browser binary (default: first of chromium / chromium-browser / google-chrome / … on PATH) |
| `WBG_POOL_BROWSER_ARGS` | Extra whitespace-separated browser flags |
| `WBG_POOL_NO_SANDBOX` | Add `--no-sandbox` (added automatically when running as root) |
| `WBG_POOL_DIR` | Daemon rendezvous dir (default `$XDG_RUNTIME_DIR/wbg-pool` or `$TMPDIR/wbg-pool-<uid>`) |
| `WBG_POOL_URL` | Use an already-running daemon at this URL, skip discovery/spawn |
| `WBG_POOL_FALLBACK_RUNNER` | Path to `wasm-bindgen-test-runner` for delegated modes (default: from PATH) |
| `WASM_BINDGEN_TEST_TIMEOUT` | Per-invocation timeout in seconds (default 20, same as stock) |
| `WASM_BINDGEN_NO_DEBUG`, `WASM_BINDGEN_SPLIT_LINKED_MODULES`, `WASM_BINDGEN_KEEP_LLD_EXPORTS`, `WASM_BINDGEN_TEST_NO_ORIGIN_ISOLATION` | Honored with stock-runner semantics |

Daemon management is automatic, but explicit control exists:

```console
$ wbg-pool daemon --idle-timeout 600   # run in the foreground
$ wbg-pool daemon --stop               # stop a running daemon
```

## Version coupling

Like `wasm-bindgen-test-runner` itself, the daemon's codegen must match the
`wasm-bindgen` version of the crates under test. This build pins
`wasm-bindgen-cli-support 0.2.126` (compatible with `wasm-bindgen-test
0.3.76`). Use a wbg-pool build whose pin matches your `Cargo.lock`.

## Known limitations

- `--bench` and coverage collection are not implemented.
- The nested-worker `console.*` forwarding shim that the stock runner
  injects into user-spawned workers is not replicated; harness-captured
  per-test output (including panics) is unaffected.
- One daemon serves one `wasm-bindgen` version (see above).
- Unix only for now (the daemon/shim rendezvous uses Unix process
  primitives).

## Attribution

The in-page harness templates (`index.html`, the run/worker driver scripts)
are adapted from the ones `wasm-bindgen-test-runner` generates, from the
[wasm-bindgen] project (MIT OR Apache-2.0).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

[`wasm-bindgen-test`]: https://docs.rs/wasm-bindgen-test
[`cargo nextest`]: https://nexte.st
[wasm-bindgen]: https://github.com/wasm-bindgen/wasm-bindgen
