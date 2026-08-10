// Worker-side harness for worker-mode test suites, executed as a module
// inside a dedicated worker, shared worker, or service worker. Adapted from
// the worker.js that wasm-bindgen-test-runner generates (wasm-bindgen
// project, MIT/Apache-2.0), with one addition: an explicit
// `__wbgtest_done` message once the harness finishes, so the page does not
// need to scrape output to detect completion.

import {
    WasmBindgenTestContext as Context,
    __wbgtest_console_debug,
    __wbgtest_console_log,
    __wbgtest_console_info,
    __wbgtest_console_warn,
    __wbgtest_console_error,
    __wbgtest_cov_dump,
    __wbgtest_module_signature,
    default as init,
} from '{MODULE_JS}';

const nocapture = {NOCAPTURE};

function setup(port) {
    self.__wbg_test_invoke = f => f();
    self.__wbg_test_output_writeln = function (...args) {
        port.postMessage(['__wbgtest_output_append', args.map(String).join(' ') + '\n']);
    };

    const wrap = method => {
        const on_method = `on_console_${method}`;
        self.console[method] = function (...args) {
            if (nocapture) {
                self.__wbg_test_output_writeln(...args);
            }
            if (self[on_method]) {
                self[on_method](args);
            }
        };
    };
    wrap('debug');
    wrap('log');
    wrap('info');
    wrap('warn');
    wrap('error');

    port.onmessage = event => {
        runTests(port, event.data);
    };
    if (port.start) {
        port.start();
    }
}

async function runTests(port, tests) {
    try {
        const wasm = await init({ module_or_path: '{MODULE_WASM}' });

        const cx = new Context(false);
        self.on_console_debug = __wbgtest_console_debug;
        self.on_console_log = __wbgtest_console_log;
        self.on_console_info = __wbgtest_console_info;
        self.on_console_warn = __wbgtest_console_warn;
        self.on_console_error = __wbgtest_console_error;

        cx.include_ignored({INCLUDE_IGNORED});
        cx.filtered_count({FILTERED_COUNT});

        const ok = await cx.run(tests.map(name => wasm[name]));

        try {
            const coverage = __wbgtest_cov_dump();
            if (coverage !== undefined) {
                await fetch('/__wasm_bindgen/coverage', {
                    method: 'POST',
                    headers: { 'Module-Signature': String(__wbgtest_module_signature()) },
                    body: coverage,
                });
            }
        } catch (error) {
            console.warn('coverage dump failed: ' + error);
        }

        port.postMessage(['__wbgtest_done', !!ok]);
    } catch (error) {
        const detail = error && error.stack ? error.stack : String(error);
        port.postMessage(['__wbgtest_output_append', '\nharness error: ' + detail + '\n']);
        port.postMessage(['__wbgtest_done', false]);
    }
}

{PORT_PROLOGUE}
