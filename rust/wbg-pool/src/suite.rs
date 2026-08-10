//! Inspection of compiled wasm test binaries: which tests they export, and
//! which environment they are configured to run in.
//!
//! The `#[wasm_bindgen_test]` macro exports each test as a wasm export named
//! `__wbgt_<modifiers>_<crate>::<module path>::<test name>`, where the
//! modifiers segment contains `$` for `#[ignore]`d tests. The
//! `wasm_bindgen_test_configure!` macro records the requested environment in
//! a `__wasm_bindgen_test_unstable` custom section. Both conventions are
//! defined by `wasm-bindgen-test` and read here the same way
//! `wasm-bindgen-test-runner` reads them.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

pub const TEST_EXPORT_PREFIX: &str = "__wbgt_";
pub const CONFIGURE_SECTION: &str = "__wasm_bindgen_test_unstable";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestMode {
    Browser,
    DedicatedWorker,
    SharedWorker,
    ServiceWorker,
    Node,
    Emscripten,
}

impl TestMode {
    pub fn describe(&self) -> &'static str {
        match self {
            TestMode::Browser => "browser",
            TestMode::DedicatedWorker => "dedicated worker",
            TestMode::SharedWorker => "shared worker",
            TestMode::ServiceWorker => "service worker",
            TestMode::Node => "node",
            TestMode::Emscripten => "emscripten",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestCase {
    /// Human-facing test name (the part after the crate name).
    pub name: String,
    /// The wasm export implementing the test.
    pub export: String,
    /// Whether the test carries `#[ignore]`.
    pub ignored: bool,
}

#[derive(Debug, Clone)]
pub struct Suite {
    /// The configured test environment; `None` when the binary carries no
    /// `wasm_bindgen_test_configure!` section, which defaults to Node.
    pub mode: Option<TestMode>,
    pub tests: Vec<TestCase>,
}

impl Suite {
    pub fn effective_mode(&self) -> TestMode {
        self.mode.unwrap_or(TestMode::Node)
    }
}

pub fn inspect(wasm: &[u8]) -> Result<Suite> {
    use wasmparser::{Parser, Payload};

    let mut tests = Vec::new();
    let mut mode = None;

    for payload in Parser::new(0).parse_all(wasm) {
        match payload.context("failed to parse wasm module")? {
            Payload::ExportSection(exports) => {
                for export in exports {
                    let export = export?;
                    let Some(rest) = export.name.strip_prefix(TEST_EXPORT_PREFIX) else {
                        continue;
                    };
                    let Some((modifiers, _)) = rest.split_once('_') else {
                        continue;
                    };
                    let Some((_, name)) = export.name.split_once("::") else {
                        continue;
                    };
                    tests.push(TestCase {
                        name: name.to_string(),
                        export: export.name.to_string(),
                        ignored: modifiers.contains('$'),
                    });
                }
            }
            Payload::CustomSection(section) if section.name() == CONFIGURE_SECTION => {
                let data = section.data();
                mode = Some(if data.contains(&0x01) {
                    TestMode::Browser
                } else if data.contains(&0x02) {
                    TestMode::DedicatedWorker
                } else if data.contains(&0x03) {
                    TestMode::SharedWorker
                } else if data.contains(&0x04) {
                    TestMode::ServiceWorker
                } else if data.contains(&0x05) {
                    TestMode::Node
                } else if data.contains(&0x06) {
                    TestMode::Emscripten
                } else {
                    bail!("invalid {CONFIGURE_SECTION} value")
                });
            }
            _ => {}
        }
    }

    Ok(Suite { mode, tests })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterArgs {
    pub filter: Option<String>,
    pub exact: bool,
    pub skip: Vec<String>,
    pub ignored: bool,
    pub include_ignored: bool,
}

pub struct Filtered {
    pub to_run: Vec<TestCase>,
    pub filtered: usize,
}

/// Applies libtest-style filtering with semantics identical to
/// `wasm-bindgen-test-runner` 0.2.126: positional filter first, then
/// `--skip`, then `--ignored` selection; every excluded test increments the
/// `filtered` count that the in-browser harness reports back.
pub fn filter(tests: &[TestCase], args: &FilterArgs) -> Filtered {
    let mut to_run = Vec::new();
    let mut filtered = 0;

    'outer: for test in tests {
        if let Some(filter) = &args.filter {
            let matches = if args.exact {
                test.name == *filter
            } else {
                test.name.contains(filter)
            };
            if !matches {
                filtered += 1;
                continue;
            }
        }

        for skip in &args.skip {
            let matches = if args.exact {
                test.name == *skip
            } else {
                test.name.contains(skip)
            };
            if matches {
                filtered += 1;
                continue 'outer;
            }
        }

        if !test.ignored && args.ignored {
            filtered += 1;
        } else {
            to_run.push(test.clone());
        }
    }

    Filtered { to_run, filtered }
}

#[cfg(test)]
mod test {
    use super::*;

    fn case(name: &str, ignored: bool) -> TestCase {
        TestCase {
            name: name.to_string(),
            export: format!("__wbgt_{}_1_crate::{name}", if ignored { "$" } else { "" }),
            ignored,
        }
    }

    fn args() -> FilterArgs {
        FilterArgs {
            filter: None,
            exact: false,
            skip: Vec::new(),
            ignored: false,
            include_ignored: false,
        }
    }

    #[test]
    fn it_runs_everything_without_filters() {
        let tests = [case("a::one", false), case("a::two", false)];
        let result = filter(&tests, &args());
        assert_eq!(result.to_run.len(), 2);
        assert_eq!(result.filtered, 0);
    }

    #[test]
    fn it_filters_by_substring_and_counts() {
        let tests = [case("a::one", false), case("a::two", false)];
        let mut a = args();
        a.filter = Some("one".to_string());
        let result = filter(&tests, &a);
        assert_eq!(result.to_run.len(), 1);
        assert_eq!(result.to_run[0].name, "a::one");
        assert_eq!(result.filtered, 1);
    }

    #[test]
    fn it_matches_exactly_when_requested() {
        let tests = [case("a::one", false), case("a::one_more", false)];
        let mut a = args();
        a.filter = Some("a::one".to_string());
        a.exact = true;
        let result = filter(&tests, &a);
        assert_eq!(result.to_run.len(), 1);
        assert_eq!(result.filtered, 1);
    }

    #[test]
    fn it_skips_and_counts() {
        let tests = [case("a::one", false), case("a::two", false)];
        let mut a = args();
        a.skip = vec!["two".to_string()];
        let result = filter(&tests, &a);
        assert_eq!(result.to_run.len(), 1);
        assert_eq!(result.filtered, 1);
    }

    #[test]
    fn it_selects_only_ignored_with_the_ignored_flag() {
        let tests = [case("a::one", false), case("a::two", true)];
        let mut a = args();
        a.ignored = true;
        let result = filter(&tests, &a);
        assert_eq!(result.to_run.len(), 1);
        assert_eq!(result.to_run[0].name, "a::two");
        assert_eq!(result.filtered, 1);
    }
}
