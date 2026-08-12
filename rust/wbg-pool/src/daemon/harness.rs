//! Per-run generation of the JS that drives a test suite inside the shared
//! browser. Browser-mode suites run on the page's main thread; worker-mode
//! suites get a page-side relay plus a worker-side harness script.

use crate::suite::TestMode;
use anyhow::Result;

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

    let scripts = match config.mode {
        TestMode::Browser => RunScripts {
            run_js: RUN_BROWSER
                .replace("{MODULE_JS}", config.module_js)
                .replace("{MODULE_WASM}", config.module_wasm)
                .replace("{INCLUDE_IGNORED}", include_ignored)
                .replace("{FILTERED_COUNT}", &filtered)
                .replace("{TESTS}", &tests_json),
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
                        .replace("{PORT_PROLOGUE}", prologue),
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
