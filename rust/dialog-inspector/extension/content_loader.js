// Content script loader for dialog-inspector.
//
// Loads the WASM content script module which provides the IndexedDB
// inspection backend via chrome.runtime.onMessage.  The content script
// runs in the host page's origin, giving it access to the page's
// IndexedDB databases.

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
