// Content script loader for dialog-inspector.
//
// Runs in every page and performs two tasks:
//
// 1. Signals the extension ID to the host page's service worker (if one
//    exists and includes the dialog-inspector sw-plugin).  This lets the
//    SW dynamically import the inspector WASM from this extension's
//    web_accessible_resources.
//
// 2. Loads the WASM content script module which provides the IndexedDB
//    inspection backend via chrome.runtime.onMessage.  This is the
//    fallback path when the host has no SW plugin.

(async () => {
  // ── Signal extension ID to host SW ──────────────────────────────────
  //
  // If the host page has a service worker that includes
  // dialog-inspector's sw-plugin.js, it listens for this message and
  // dynamically imports the WASM module from our web_accessible_resources.
  try {
    const reg = await navigator.serviceWorker?.ready;
    if (reg?.active) {
      reg.active.postMessage({
        type: "dialog-inspector-init",
        extensionId: chrome.runtime.id,
      });
    }
  } catch (_) {
    // No service worker — that's fine, content script handles dispatch.
  }

  // ── Load content script WASM (fallback / direct dispatch) ──────────
  try {
    const src = chrome.runtime.getURL("content.js");
    const mod = await import(src);
    await mod.default();
  } catch (e) {
    // Silently ignore on pages where WASM fails to load (CSP, etc).
    // The panel will show "no databases found" in that case.
    console.debug("[dialog-inspector] content script init failed:", e);
  }
})();
