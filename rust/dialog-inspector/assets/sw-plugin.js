// dialog-inspector service worker plugin.
//
// Host applications import this module in their service worker to enable
// the dialog-inspector API endpoint.  The inspector WASM module is loaded
// either from the same origin (co-deployed) or from the browser extension's
// web_accessible_resources (triggered by the content script).
//
// Usage (host app's service worker):
//
//   import { initDialogInspector } from './dialog-inspector-sw-plugin.js';
//   initDialogInspector(self);
//
// The plugin registers two event listeners synchronously:
//
//   1. `fetch` — intercepts /dialog-inspector/api/* requests.  Returns 503
//      until the WASM module is loaded, then delegates to handle_request().
//
//   2. `message` — listens for { type: 'dialog-inspector-init' } messages
//      from the content script, which provides the extension ID.  The plugin
//      then dynamically imports the WASM module from the extension.
//
// The WASM module can also be loaded from the same origin by passing a URL:
//
//   initDialogInspector(self, {
//     wasmUrl: '/dialog-inspector/dialog_inspector_worker.js',
//   });

const API_PREFIX = "/dialog-inspector/api/";

let wasmModule = null;
let wasmLoading = null;

async function loadWasmFrom(url) {
  if (wasmModule) return;
  if (wasmLoading) return wasmLoading;

  wasmLoading = (async () => {
    const mod = await import(url);
    await mod.default(); // initialise wasm-bindgen
    if (mod.activate) await mod.activate();
    wasmModule = mod;
  })();

  return wasmLoading;
}

/**
 * Initialise the dialog-inspector plugin in a service worker.
 *
 * @param {ServiceWorkerGlobalScope} sw - `self` in the service worker
 * @param {Object} [options]
 * @param {string} [options.wasmUrl] - URL to load the WASM JS glue from.
 *   If omitted, the plugin waits for the extension content script to
 *   provide the extension ID via postMessage.
 */
export function initDialogInspector(sw, options) {
  // If a local WASM URL is provided, start loading immediately.
  if (options?.wasmUrl) {
    loadWasmFrom(options.wasmUrl);
  }

  // ── Fetch handler (registered synchronously) ───────────────────────

  sw.addEventListener("fetch", (event) => {
    const url = new URL(event.request.url);

    if (!url.pathname.startsWith(API_PREFIX)) return;

    event.respondWith(handleApiRequest(url));
  });

  // ── Message handler: extension provides its ID ─────────────────────

  sw.addEventListener("message", (event) => {
    const data = event.data;
    if (!data || data.type !== "dialog-inspector-init") return;

    if (data.extensionId && !wasmModule) {
      const extUrl =
        `chrome-extension://${data.extensionId}/dialog_inspector_worker.js`;
      loadWasmFrom(extUrl);
    }

    // If a wasmUrl is provided directly (for testing / local dev)
    if (data.wasmUrl && !wasmModule) {
      loadWasmFrom(data.wasmUrl);
    }
  });
}

async function handleApiRequest(url) {
  // Fast probe endpoint — always responds 200, even before WASM loads.
  const action = url.pathname.slice(API_PREFIX.length);
  if (action === "ping") {
    return new Response(
      JSON.stringify({
        kind: "pong",
        ready: wasmModule !== null,
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
  }

  // Wait for WASM to load (with a timeout).
  if (!wasmModule) {
    if (wasmLoading) {
      try {
        await Promise.race([
          wasmLoading,
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error("timeout")), 5000)
          ),
        ]);
      } catch (_) {
        // Fall through to 503
      }
    }

    if (!wasmModule) {
      return new Response(
        JSON.stringify({
          kind: "error",
          message: "Inspector WASM module not loaded yet",
        }),
        { status: 503, headers: { "Content-Type": "application/json" } }
      );
    }
  }

  try {
    const requestJson = buildRequestJson(action, url.searchParams);
    const responseJson = await wasmModule.handle_request(requestJson);

    return new Response(responseJson, {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  } catch (err) {
    return new Response(
      JSON.stringify({ kind: "error", message: String(err) }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}

function buildRequestJson(action, params) {
  switch (action) {
    case "list_databases":
      return JSON.stringify({ kind: "list_databases" });

    case "database_summary":
      return JSON.stringify({
        kind: "database_summary",
        name: params.get("name") || "",
      });

    case "query_facts":
      return JSON.stringify({
        kind: "query_facts",
        name: params.get("name") || "",
        attribute: params.get("attribute") || null,
        entity: params.get("entity") || null,
        limit: parseInt(params.get("limit") || "100", 10),
      });

    default:
      return JSON.stringify({ kind: "list_databases" });
  }
}
