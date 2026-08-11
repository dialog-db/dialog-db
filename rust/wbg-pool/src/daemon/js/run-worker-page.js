// Page-side driver for worker-mode test suites (dedicated worker, shared
// worker, service worker). The page spawns the worker, sends it the test
// list, relays harness output into #output, and reports the final verdict
// back to the daemon. Adapted from the run.js that
// wasm-bindgen-test-runner generates (wasm-bindgen project, MIT/Apache-2.0).

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

function handleHarnessMessage(event) {
    const data = event.data;
    if (!data || !Array.isArray(data) || typeof data[0] !== 'string'
        || !data[0].startsWith('__wbgtest_')) {
        return;
    }
    const method = data[0].slice(10);
    const args = data.slice(1);
    if (method === 'output_append') {
        document.getElementById('output').textContent += args[0];
    } else if (method === 'done') {
        report(args[0]);
    }
}

async function main() {
    document.getElementById('output').textContent = 'Loading Wasm module...\n';

    let port;
    {SPAWN}

    port.addEventListener('message', handleHarnessMessage);
    if (port.start) {
        port.start();
    }
    port.postMessage({TESTS});
}

main().catch(error => report(false, error));
