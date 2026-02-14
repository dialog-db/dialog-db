// Minimal devtools integration.
//
// This script runs in the devtools context and creates a panel that
// hosts the Rust/WASM inspector UI. The panel HTML is the Trunk-built
// index.html (copied to the extension as panel.html during the build).
//
// In a web extension context, the panel runs with the extension's origin,
// so it can access the inspected page's IndexedDB via the devtools API
// or message passing if needed. For now the panel accesses IndexedDB
// directly (which works when the extension has appropriate permissions
// or when running as a standalone page in the same origin).

if (typeof chrome !== "undefined" && chrome.devtools && chrome.devtools.panels) {
  chrome.devtools.panels.create(
    "Dialog DB",        // Panel title shown in the devtools tab bar
    null,               // Icon path (null = default)
    "panel.html",       // The panel page (Trunk-built output, renamed)
    function (panel) {
      // Panel created. Future: wire up onShown/onHidden for lifecycle.
      console.log("Dialog Inspector panel created");
    }
  );
}
