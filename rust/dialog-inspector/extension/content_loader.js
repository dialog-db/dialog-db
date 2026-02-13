// Content script loader for dialog-inspector.
//
// This runs in every page and initialises the WASM content script module
// which provides the IndexedDB inspection backend.  The WASM module
// registers a chrome.runtime.onMessage listener so the devtools panel
// can send Request messages and receive Response messages.
//
// The WASM + JS glue files (content.js, content_bg.wasm) are produced
// by wasm-bindgen during the build.  They are declared as
// web_accessible_resources in manifest.json so the content script
// (which shares the page's origin for IDB, but the extension's origin
// for asset loading) can import them.

(async () => {
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
