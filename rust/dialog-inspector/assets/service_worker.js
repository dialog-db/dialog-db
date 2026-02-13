// Service worker shell for dialog-inspector.
//
// This JS file acts as a thin delegation layer. When compiled via Trunk,
// the WASM worker binary is placed alongside this file. The service worker
// imports the WASM module and delegates fetch events to it.
//
// For the initial version, this is a stub that demonstrates the architecture.
// The actual WASM worker binary (bin/worker.rs) will be added in a future
// iteration when the handler module gains async fetch support.
//
// Architecture:
//   1. Service worker intercepts fetch events
//   2. Requests matching /api/* are routed to the WASM handler
//   3. Other requests fall through to the network (or serve the panel UI)

self.addEventListener("install", (event) => {
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);

  // Only intercept API requests; let everything else pass through.
  if (url.pathname.startsWith("/api/")) {
    event.respondWith(
      new Response(
        JSON.stringify({ error: "WASM handler not yet initialized" }),
        {
          status: 503,
          headers: { "Content-Type": "application/json" },
        }
      )
    );
    return;
  }

  // For navigation requests that 404, serve the panel (SPA fallback).
  // This will be wired up once the WASM worker binary is integrated.
});
