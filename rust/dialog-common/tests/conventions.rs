//! Workspace source conventions, enforced as a test.
//!
//! Async code in this workspace targets both native and
//! `wasm32-unknown-unknown`, so hand-written bounds must use
//! [`dialog_common::ConditionalSend`] / [`ConditionalSync`] (which
//! are `Send` / `Sync` on native and nothing on wasm) instead of the
//! bare marker traits. Bare `Send` / `Sync` remain correct in
//! exactly two places:
//!
//! - `dyn` bounds (`Box<dyn Future + Send>`): auto traits cannot be
//!   substituted there, so the type gets a cfg'd pair of aliases —
//!   one per target — like `ArtifactStream`.
//! - Native-only code (a `#[cfg(not(target_arch = "wasm32"))]` fn
//!   feeding `tokio::spawn`, a deliberately `Send` variant of a
//!   local/sendable pair).
//!
//! Lines using `dyn` are exempt automatically. Anything else must
//! carry a `bare-send-ok: <reason>` comment, which exempts lines
//! from the marker to the next blank line.
//!
//! This is a source-level scan rather than a clippy
//! `disallowed-types` rule because clippy attributes the `Send`
//! tokens that `async_trait` and the provider macros *expand to*
//! back to the local attribute line, flagging every macro use in the
//! workspace; the convention is about what humans write.

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::fs;
    use std::path::{Path, PathBuf};

    /// The single file allowed to name the bare traits freely: the
    /// definition site of the conditional ones.
    const DEFINITION_SITE: &str = "dialog-common/src/sync.rs";

    /// Marker comment that exempts lines up to the next blank line.
    const EXEMPT_MARKER: &str = "bare-send-ok";

    fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name == "target") {
                    continue;
                }
                rust_sources(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }

    /// Whether `line` (comments stripped) uses bare `Send` or `Sync`
    /// in bound position (preceded by `:` or `+`).
    fn uses_bare_marker(line: &str) -> bool {
        for word in ["Send", "Sync"] {
            let mut from = 0;
            while let Some(at) = line[from..].find(word) {
                let start = from + at;
                let end = start + word.len();
                from = end;
                let word_char = |c: char| c.is_ascii_alphanumeric() || c == '_';
                if line[..start].chars().next_back().is_some_and(word_char)
                    || line[end..].chars().next().is_some_and(word_char)
                {
                    continue;
                }
                let before = line[..start].trim_end();
                if before.ends_with(':') || before.ends_with('+') {
                    return true;
                }
            }
        }
        false
    }

    /// Hand-written `Send` / `Sync` bounds must be `ConditionalSend`
    /// / `ConditionalSync`; see the module doc for the rationale and
    /// the exemptions.
    #[dialog_common::test]
    async fn it_bounds_on_conditional_send_and_sync() {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("dialog-common lives under the rust/ workspace dir")
            .to_path_buf();
        let mut sources = Vec::new();
        rust_sources(&workspace, &mut sources);
        assert!(
            sources.len() > 100,
            "the scan should see the whole workspace, found {} files",
            sources.len()
        );

        let mut violations = Vec::new();
        for path in sources {
            if path.ends_with(DEFINITION_SITE) {
                continue;
            }
            let Ok(source) = fs::read_to_string(&path) else {
                continue;
            };
            let mut exempt = false;
            for (index, line) in source.lines().enumerate() {
                if line.trim().is_empty() {
                    exempt = false;
                    continue;
                }
                if line.contains(EXEMPT_MARKER) {
                    exempt = true;
                    continue;
                }
                if exempt || line.contains("dyn ") {
                    continue;
                }
                let code = line.split("//").next().unwrap_or(line);
                if uses_bare_marker(code) {
                    violations.push(format!("{}:{}: {}", path.display(), index + 1, line.trim()));
                }
            }
        }

        assert!(
            violations.is_empty(),
            "bare Send/Sync bounds found; use ConditionalSend/ConditionalSync \
             (or mark deliberate native-only/dyn uses with `{EXEMPT_MARKER}: <reason>`):\n{}",
            violations.join("\n")
        );
    }
}
