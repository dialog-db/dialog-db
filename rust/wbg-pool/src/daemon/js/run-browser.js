// Test driver template, adapted from the run.js that
// wasm-bindgen-test-runner generates (wasm-bindgen project,
// MIT/Apache-2.0). The daemon substitutes the {UPPERCASE} placeholders per
// run. Unlike the original, this page reports its result back to the daemon
// over fetch instead of being scraped over WebDriver.

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

function grab(id) {
    const element = document.getElementById(id);
    return element ? element.textContent : '';
}

let reported = false;
async function report(ok, error) {
    if (reported) {
        return;
    }
    reported = true;
    let output = grab('output');
    if (error !== undefined) {
        const detail = error && error.stack ? error.stack : String(error);
        output += '\nharness error: ' + detail + '\n';
    }
    await fetch('report', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
            ok: !!ok,
            output,
            console_output: grab('console_output'),
        }),
    });
}

async function dumpCoverage() {
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
}

async function main() {
    document.getElementById('output').textContent = 'Loading Wasm module...\n';

    const wasm = await init({ module_or_path: '{MODULE_WASM}' });

    const cx = new Context(false);
    window.on_console_debug = __wbgtest_console_debug;
    window.on_console_log = __wbgtest_console_log;
    window.on_console_info = __wbgtest_console_info;
    window.on_console_warn = __wbgtest_console_warn;
    window.on_console_error = __wbgtest_console_error;

    cx.include_ignored({INCLUDE_IGNORED});
    cx.filtered_count({FILTERED_COUNT});

    const tests = {TESTS};
    const ok = await cx.run(tests.map(name => wasm[name]));
    await dumpCoverage();
    return ok;
}

main().then(ok => report(ok)).catch(error => report(false, error));
