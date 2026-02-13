// Service worker shell for dialog-inspector.
//
// A host application registers this file as a service worker to expose the
// inspector at its own origin.  The SW imports the companion WASM module
// (built from bin/worker.rs via wasm-pack) and delegates all
// `/dialog-inspector/api/*` requests to it.
//
// Usage (host app):
//   navigator.serviceWorker.register("/dialog-inspector-sw.js");
//
// The WASM module must be co-located at the same path prefix:
//   /dialog-inspector-sw_bg.wasm   (or whatever wasm-pack names it)

const API_PREFIX = "/dialog-inspector/api/";
const PANEL_PREFIX = "/dialog-inspector/";

let wasmReady = null; // Promise that resolves when WASM is initialised
let wasmModule = null;

async function initWasm() {
  // Import the wasm-pack generated JS glue that sits alongside this file.
  // The host must serve `dialog_inspector_worker.js` and
  // `dialog_inspector_worker_bg.wasm` at the same path as this SW.
  const mod = await import("./dialog_inspector_worker.js");
  await mod.default(); // initialise the WASM module
  await mod.activate();
  wasmModule = mod;
}

self.addEventListener("install", (_event) => {
  // Start loading WASM immediately (no need to block install on it).
  wasmReady = initWasm();
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    (wasmReady || (wasmReady = initWasm())).then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);

  if (!url.pathname.startsWith(API_PREFIX)) {
    // Not an inspector API request — let the browser handle it.
    return;
  }

  event.respondWith(handleApiRequest(url));
});

async function handleApiRequest(url) {
  try {
    // Ensure WASM is loaded before handling any request.
    await (wasmReady || (wasmReady = initWasm()));

    // Strip the API prefix to get the action path, e.g. "list_databases".
    const actionPath = url.pathname.slice(API_PREFIX.length);

    // Build a Request object from the URL.
    const requestJson = buildRequestJson(actionPath, url.searchParams);

    // Delegate to the WASM handler.
    const responseJson = await wasmModule.handle_request(requestJson);

    return new Response(responseJson, {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  } catch (err) {
    return new Response(
      JSON.stringify({ kind: "error", message: String(err) }),
      {
        status: 500,
        headers: { "Content-Type": "application/json" },
      }
    );
  }
}

// Map URL path + query params into a JSON-encoded handler::Request.
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
      return JSON.stringify({
        kind: "list_databases",
      });
  }
}
