# Cross-target integration tests (native-provisioned server, client runs everywhere)

When a test needs a real service (HTTP server, S3, etc.) and the *client* under
test must run on both native and wasm, follow the provisioning pattern the S3
and UCAN access tests use. Do NOT hand-roll a `#[tokio::test]` that spawns the
server and client inline — that only runs on native.

## The three pieces

1. **A serializable address type** — what the test body receives (the endpoint
   URL, credentials, etc.). Must derive `Serialize, Deserialize` so the wasm
   runner can receive it from the native provider via an env var.

   ```rust
   #[derive(Debug, Clone, Serialize, Deserialize)]
   pub struct MyAddress { pub url: String }
   ```

2. **A `Default` settings type + a `#[dialog_common::provider]` fn** that
   provisions the service and returns `Service<Address, Provider>`. Native-only
   (the macro gates it). The `Provider` (often a `(Listener,)` tuple or a struct
   impl'ing `dialog_common::helpers::Provider`) cleans up on `stop`/drop.

   ```rust
   #[derive(Debug, Clone, Default)]
   pub struct MySettings { pub behavior: Behavior }

   #[dialog_common::provider]
   pub async fn my_server(settings: MySettings)
       -> anyhow::Result<Service<MyAddress, MyServer>> { ... }
   ```

   Put these in `src/helpers.rs` + `src/helpers/server.rs`, gated behind a
   `helpers` feature, native-only (`#[cfg(not(target_arch = "wasm32"))]`) for the
   server module.

3. **`#[dialog_common::test]` functions that take the address param**. These run
   the client on BOTH native and wasm against the provisioned endpoint. Pass
   per-test settings as attr kwargs, which override the settings `Default`:

   ```rust
   #[dialog_common::test]
   async fn happy_path(addr: MyAddress) -> anyhow::Result<()> { ... }

   #[dialog_common::test(behavior = Behavior::Redirect("...".into()))]
   async fn refuses_redirect(addr: MyAddress) -> anyhow::Result<()> { ... }
   ```

## Cargo.toml wiring

Prefer a raw `tokio::net::TcpListener` writing fixed HTTP/1.1 responses over an
HTTP-server framework when the responses are static (a 200 with a body, a 302
with `Location`, a sized/unsized body). It needs no request parsing and adds no
deps beyond `tokio`, which the provisioning already requires.

```toml
[features]
helpers = ["dep:anyhow", "dep:tokio", "dialog-common/helpers"]
integration-tests = ["helpers", "dialog-common/integration-tests"]
web-integration-tests = ["helpers", "dialog-common/web-integration-tests"]

# Native-only deps for the server. Optional so a normal build never pulls them,
# and target-gated so they never reach wasm (tokio `net` pulls `mio`, which does
# not build for wasm).
[target.'cfg(not(target_arch = "wasm32"))'.dependencies]
anyhow = { workspace = true, optional = true }
tokio = { workspace = true, features = ["rt","macros","net","sync","io-util"], optional = true }

# The tokio the TEST PROCESS uses is a native-only dev-dep for the same reason:
# the wasm client runs on wasm-bindgen-test + the browser event loop, never
# tokio net.
[target.'cfg(not(target_arch = "wasm32"))'.dev-dependencies]
tokio = { workspace = true, features = ["rt","macros","net","sync"] }

[lints.rust]
unexpected_cfgs = { level = "warn", check-cfg = [
    "cfg(dialog_test_wasm_integration)",
    "cfg(feature, values(\"web-integration-tests\", \"integration-tests\", \"helpers\"))",
] }
```

Integration test files go in `tests/`, guarded `#![cfg(feature = "helpers")]`,
and add the wasm configure line:

```rust
#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);
```

## How the address persists across the native -> wasm boundary

This is the load-bearing trick. The provider runs in a **native** process; the
wasm client runs in a **separate** browser process. The address bridges them by
serialization, not shared memory:

1. The native outer test provisions the service and gets the `Address`.
2. It `serde_json`-serializes the `Address` into the `PROVISIONED_SERVICE_ADDRESS`
   environment variable (`dialog_common::helpers::PROVISIONED_SERVICE_ADDRESS`),
   and names that variable in `WBG_POOL_PASS_ENV`.
3. It spawns the wasm test subprocess (`cargo test --target
   wasm32-unknown-unknown` for the one inner test).
4. The wasm test runner, `wbg-pool`, defines the variables `WBG_POOL_PASS_ENV`
   names on `globalThis.__wbg_pool_env` in the page or worker the test runs
   in. A wasm test in a browser has no process environment of its own.
5. The wasm inner test reads the variable from there when it runs
   (`dialog_common::helpers::address`) and `serde_json`-deserializes the
   `Address` back, then runs the client against it.

The address is read when the test **runs**. It must never be compiled into
the wasm test binary (`env!`, `option_env!`, a build script): cargo tracks
such a variable as a build input, so every test, each with its own address,
rebuilds and relinks its package's whole wasm test binary. That is what the
macro did before, and it took over a minute per test in `dialog-repository`
and more than an hour of the cross-target CI job.

Consequences to respect:

- The `Address` type **must** be `Serialize + DeserializeOwned + Clone` — it
  literally round-trips through JSON across a process boundary. This is enforced
  by the `Provisionable` trait bound.
- Put **only** what the client needs into the `Address` (endpoint URL,
  credentials). It is not a handle to the running server — the server lives in
  the other process. Server-side state (listeners, shutdown channels) belongs in
  the `Provider`, which stays native and is dropped/`stop`-ped after the test.
- The wasm client reaches the native-provisioned server over the loopback
  address the provider chose (`127.0.0.1:<port>`), so the address must be a real
  URL the browser can reach, not a symbolic handle.

## How CI runs it (verified against `flake.nix` + `.github/workflows/test.yml`)

CI runs `cargo nextest run` **workspace-wide** in the dev shell, passing the
feature flag (`testArguments` in `flake.nix`). Your crate's
`integration-tests` / `web-integration-tests` features delegate to
`dialog-common`'s, so they hook into these flags automatically — no CI config
change is needed to pick up a new integration test:

- `test:native:debug` / `test:native:release`:
  `cargo nextest run [--release] --features integration-tests`
  → provider starts natively, test body runs native, provider stops.
- `test:web:debug` / `test:web:release`:
  `cargo nextest run --workspace --exclude wbg-pool --exclude dialog-baseline --target wasm32-unknown-unknown [--release]`
  → the unit tests in a headless browser. **Every crate's test targets must
  build for wasm** here, with the `helpers` feature on.
- `test:cross:integration`:
  `cargo nextest run --features web-integration-tests`
  → native outer test starts the provider, serializes the address to an env
  var, spawns the wasm inner test which reads it back from the test runner
  and runs the client in a headless browser.
- The `cargoChecks.clippy` gate runs `clippy --all-targets --all-features -D
  warnings`, so **the `helpers` feature and its server must be clippy-clean and
  must not break the wasm build** even though the server itself is native-only.

Before pushing, reproduce them locally, in the dev shell:

```
# native (integration tests)
test:native:debug
# wasm unit tests — this is where server deps leaking to wasm bite
test:web:debug
# cross-target (native provider, wasm client); narrow it with nextest's filter
test:cross:integration -E 'package(<crate>)'
# the CI clippy gate
cargo clippy -p <crate> --all-targets --all-features -- -D warnings
```

## Pitfalls learned the hard way

- **The provider and everything it needs are native-only.** The server module,
  its settings/behavior *rendering*, and the server deps must not compile for
  wasm. Gate the server module `#[cfg(not(target_arch = "wasm32"))]` and keep
  its deps under `[target.'cfg(not(wasm))'.dependencies]`. Only the address type
  (which the wasm runner deserializes) and the client crossing.

- **Any `tokio` with `net` anywhere that applies to wasm breaks the build**
  (`mio` is unsupported on wasm). This includes a plain `[dev-dependencies]`
  tokio — move it to `[target.'cfg(not(wasm))'.dev-dependencies]`.

- **The `#[dialog_common::test(field = value)]` attribute expression is expanded
  ONLY into the native provider setup.** So a `Behavior::...` or a byte-cap
  constant referenced there is native-only. Do NOT `use`-import those types at
  the top of the cross-target test file — that drags them into the wasm build as
  dead code. Reference them by fully-qualified path inside the attribute
  instead; the test *body* should import only the address type and the client.

- The test body itself takes the address as a parameter and drives the client;
  it must be wasm-clean. Mirror `dialog-remote-s3` / `dialog-peer` tests,
  whose bodies touch only the `*Address` type.

## Reference implementations

- `rust/dialog-remote-s3/src/helpers/server.rs` — `LocalS3` + provider.
- `rust/dialog-remote-ucan-s3/src/helpers/server.rs` — composed servers.
- `rust/dialog-common/src/helpers.rs` — the `Service` / `Provider` /
  `Provisionable` doc example.
- `rust/dialog-did-web/src/helpers/` + `tests/fetch_server.rs` — a minimal HTTP
  server testing fetch hardening (redirect refusal, size cap).
