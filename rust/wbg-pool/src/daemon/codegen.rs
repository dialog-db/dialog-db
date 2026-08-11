//! Per-binary `wasm-bindgen` code generation, done once per test binary and
//! cached for the daemon's lifetime. This is the work `wasm-bindgen-test-runner`
//! repeats on every invocation.

use crate::suite::{self, Suite};
use anyhow::{Context, Result};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;

/// Stable module name for generated assets, so every binary serves
/// `wbg-test.js` + `wbg-test_bg.wasm` under its own `/a/<id>/` prefix.
pub const MODULE_NAME: &str = "wbg-test";

pub struct Binary {
    /// Cache key derived from path + mtime + size; also the asset URL prefix
    /// under which this binary's generated files are served.
    pub id: String,
    pub suite: Suite,
}

/// Fingerprints a binary so a recompiled test binary at the same path gets
/// fresh codegen.
pub fn fingerprint(path: &Path) -> Result<String> {
    let metadata =
        std::fs::metadata(path).with_context(|| format!("failed to stat {}", path.display()))?;
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    metadata.len().hash(&mut hasher);
    if let Ok(modified) = metadata.modified() {
        modified.hash(&mut hasher);
    }
    Ok(format!("{:016x}", hasher.finish()))
}

/// Runs wasm-bindgen over the test binary, mirroring the Bindgen
/// configuration of `wasm-bindgen-test-runner` 0.2.126 (browser mode).
pub fn generate(wasm_path: &Path, out_dir: &Path) -> Result<Binary> {
    let bytes = std::fs::read(wasm_path)
        .with_context(|| format!("failed to read wasm file {}", wasm_path.display()))?;
    let suite = suite::inspect(&bytes)?;

    let id = fingerprint(wasm_path)?;
    let dir = out_dir.join(&id);
    std::fs::create_dir_all(&dir)?;

    // The debug flag adds assertions and error messages to the generated JS
    // glue; it has nothing to do with the Rust profile.
    let debug = std::env::var("WASM_BINDGEN_NO_DEBUG").is_err();

    let mut bindgen = wasm_bindgen_cli_support::Bindgen::new();
    bindgen
        .web(true)
        .map_err(|error| anyhow::anyhow!(error))?
        .debug(debug)
        .input_path(wasm_path)
        .out_name(MODULE_NAME)
        .emit_start(false);
    if std::env::var("WASM_BINDGEN_SPLIT_LINKED_MODULES").is_ok() {
        bindgen.split_linked_modules(true);
    }
    if std::env::var("WASM_BINDGEN_KEEP_LLD_EXPORTS").is_ok() {
        bindgen.keep_lld_exports(true);
    }
    bindgen
        .generate(&dir)
        .with_context(|| format!("wasm-bindgen failed over {}", wasm_path.display()))?;

    Ok(Binary { id, suite })
}
