//! Per-run generation of the JS that drives a test suite inside the shared
//! browser. Browser-mode suites run on the page's main thread; worker-mode
//! suites get a page-side relay plus a worker-side harness script.

use crate::protocol::ENV_GLOBAL;
use crate::suite::TestMode;
use anyhow::Result;
use std::collections::BTreeMap;

const RUN_BROWSER: &str = include_str!("js/run-browser.js");
const RUN_WORKER_PAGE: &str = include_str!("js/run-worker-page.js");
const WORKER_BODY: &str = include_str!("js/worker-body.js");

const SPAWN_DEDICATED: &str = r#"
    const worker = new Worker('worker.js', { type: 'module' });
    worker.onerror = event => report(false, event.message || 'worker error');
    port = worker;
"#;

const SPAWN_SHARED: &str = r#"
    const worker = new SharedWorker('worker.js?random=' + crypto.randomUUID(), { type: 'module' });
    worker.onerror = event => report(false, event.message || 'shared worker error');
    port = worker.port;
"#;

// Each run lives on its own origin, so concurrent service worker suites
// cannot collide on a registration the way they do under the stock runner.
const SPAWN_SERVICE: &str = r#"
    const url = 'service.js?random=' + crypto.randomUUID();
    const registration = await navigator.serviceWorker.register(url, { type: 'module' });
    if (registration.installing) {
        registration.installing.addEventListener('statechange', function () {
            if (this.state === 'redundant') {
                report(false, 'service worker installation failed');
            }
        });
    }
    await new Promise(resolve => {
        navigator.serviceWorker.addEventListener('controllerchange', resolve, { once: true });
    });
    const channel = new MessageChannel();
    navigator.serviceWorker.controller.postMessage(undefined, [channel.port2]);
    port = channel.port1;
"#;

const PROLOGUE_DEDICATED: &str = "setup(self);\n";

const PROLOGUE_SHARED: &str = r#"
addEventListener('connect', event => {
    setup(event.ports[0]);
});
"#;

const PROLOGUE_SERVICE: &str = r#"
addEventListener('install', () => skipWaiting());
addEventListener('activate', event => event.waitUntil(clients.claim()));
addEventListener('message', event => {
    setup(event.ports[0]);
});
"#;

pub struct RunScripts {
    /// Served as `/r/<id>/run.js`, loaded by the page.
    pub run_js: String,
    /// Served as `/r/<id>/worker.js` and `/r/<id>/service.js` for
    /// worker-mode suites.
    pub worker_js: Option<String>,
}

pub struct RunConfig<'a> {
    pub mode: TestMode,
    pub module_js: &'a str,
    pub module_wasm: &'a str,
    pub exports: &'a [&'a str],
    pub include_ignored: bool,
    pub filtered_count: usize,
    pub nocapture: bool,
    /// Variables the shim passed, defined on [`ENV_GLOBAL`] in the scope
    /// the suite runs in.
    pub env: &'a BTreeMap<String, String>,
}

/// The statement that defines the passed variables, placed ahead of the
/// suite. JSON is a subset of a JavaScript expression but for U+2028 and
/// U+2029, and `<` is escaped so no value can close a script element.
fn env_statement(env: &BTreeMap<String, String>) -> Result<String> {
    let literal = serde_json::to_string(env)?
        .replace('<', "\\u003c")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029");
    Ok(format!(
        "globalThis.{ENV_GLOBAL} = Object.freeze({literal});"
    ))
}

pub fn generate(config: &RunConfig<'_>) -> Result<RunScripts> {
    let tests_json = serde_json::to_string(config.exports)?;
    let include_ignored = if config.include_ignored {
        "true"
    } else {
        "false"
    };
    let nocapture = if config.nocapture { "true" } else { "false" };
    let filtered = config.filtered_count.to_string();
    let env = env_statement(config.env)?;

    let scripts = match config.mode {
        TestMode::Browser => RunScripts {
            run_js: RUN_BROWSER
                .replace("{MODULE_JS}", config.module_js)
                .replace("{MODULE_WASM}", config.module_wasm)
                .replace("{INCLUDE_IGNORED}", include_ignored)
                .replace("{FILTERED_COUNT}", &filtered)
                .replace("{TESTS}", &tests_json)
                .replace("{ENV}", &env),
            worker_js: None,
        },
        TestMode::DedicatedWorker | TestMode::SharedWorker | TestMode::ServiceWorker => {
            let (spawn, prologue) = match config.mode {
                TestMode::DedicatedWorker => (SPAWN_DEDICATED, PROLOGUE_DEDICATED),
                TestMode::SharedWorker => (SPAWN_SHARED, PROLOGUE_SHARED),
                _ => (SPAWN_SERVICE, PROLOGUE_SERVICE),
            };
            RunScripts {
                run_js: RUN_WORKER_PAGE
                    .replace("{SPAWN}", spawn)
                    .replace("{TESTS}", &tests_json),
                worker_js: Some(
                    WORKER_BODY
                        .replace("{MODULE_JS}", config.module_js)
                        .replace("{MODULE_WASM}", config.module_wasm)
                        .replace("{INCLUDE_IGNORED}", include_ignored)
                        .replace("{FILTERED_COUNT}", &filtered)
                        .replace("{NOCAPTURE}", nocapture)
                        .replace("{PORT_PROLOGUE}", prologue)
                        .replace("{ENV}", &env),
                ),
            }
        }
        TestMode::Node | TestMode::Emscripten => {
            anyhow::bail!(
                "wbg-pool cannot run {} tests in a browser",
                config.mode.describe()
            )
        }
    };
    Ok(scripts)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn browser_reporting_keeps_fetch_from_before_test_code_runs() {
        let scripts = generate(&RunConfig {
            mode: TestMode::Browser,
            module_js: "/module.js",
            module_wasm: "/module.wasm",
            exports: &["__wbgt_test"],
            include_ignored: false,
            filtered_count: 0,
            nocapture: false,
            env: &BTreeMap::new(),
        })
        .unwrap();

        let capture = scripts
            .run_js
            .find("const daemonFetch = globalThis.fetch.bind(globalThis);")
            .expect("the harness should capture fetch before running tests");
        let test_run = scripts
            .run_js
            .find("const ok = await cx.run")
            .expect("the harness should run the selected tests");
        assert!(capture < test_run);
        assert!(scripts.run_js.contains("await daemonFetch('report'"));
        assert!(!scripts.run_js.contains("await fetch('report'"));
    }

    fn scripts_with(mode: TestMode, env: &BTreeMap<String, String>) -> RunScripts {
        generate(&RunConfig {
            mode,
            module_js: "/module.js",
            module_wasm: "/module.wasm",
            exports: &["__wbgt_test"],
            include_ignored: false,
            filtered_count: 0,
            nocapture: false,
            env,
        })
        .unwrap()
    }

    #[test]
    fn it_defines_passed_variables_where_a_browser_suite_runs() {
        let env = BTreeMap::from([("ADDRESS".to_string(), "{\"port\":1}".to_string())]);

        let scripts = scripts_with(TestMode::Browser, &env);

        let defined = scripts
            .run_js
            .find(r#"globalThis.__wbg_pool_env = Object.freeze({"ADDRESS":"{\"port\":1}"});"#)
            .expect("the page should define the passed variables");
        let test_run = scripts.run_js.find("const ok = await cx.run").unwrap();
        assert!(defined < test_run);
    }

    #[test]
    fn it_defines_passed_variables_where_a_worker_suite_runs() {
        let env = BTreeMap::from([("ADDRESS".to_string(), "value".to_string())]);

        for mode in [
            TestMode::DedicatedWorker,
            TestMode::SharedWorker,
            TestMode::ServiceWorker,
        ] {
            let scripts = scripts_with(mode, &env);
            let worker = scripts
                .worker_js
                .expect("a worker suite has a worker script");

            let defined = worker
                .find(r#"globalThis.__wbg_pool_env = Object.freeze({"ADDRESS":"value"});"#)
                .expect("the worker should define the passed variables");
            let test_run = worker.find("const ok = await cx.run").unwrap();
            assert!(defined < test_run);
            assert!(!scripts.run_js.contains("{ENV}"));
        }
    }

    #[test]
    fn it_defines_an_empty_environment_when_nothing_is_passed() {
        let scripts = scripts_with(TestMode::Browser, &BTreeMap::new());

        assert!(scripts
            .run_js
            .contains("globalThis.__wbg_pool_env = Object.freeze({});"));
    }

    #[test]
    fn it_keeps_a_passed_value_from_closing_the_script() {
        let env = BTreeMap::from([(
            "ADDRESS".to_string(),
            "</script>\u{2028}{TESTS}".to_string(),
        )]);

        let scripts = scripts_with(TestMode::Browser, &env);

        assert!(scripts
            .run_js
            .contains(r#"Object.freeze({"ADDRESS":"\u003c/script>\u2028{TESTS}"});"#));
    }
}
