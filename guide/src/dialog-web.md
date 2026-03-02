# Dialog on the Web

Dialog is written in Rust and compiles to WebAssembly, which means it can run in the browser. The [Tonk](https://github.com/tonk-labs/tonk) project demonstrates one approach: running Dialog inside a service worker that acts as a local database for web applications.

This chapter gives a brief overview of how that architecture works. The specifics of the web integration are still evolving, so treat this as a sketch of the approach rather than a step-by-step tutorial.

## The service worker architecture

In a typical web application, data flows through a remote server: the client sends requests, the server processes them, and the client renders the response. With Dialog in a service worker, the data lives locally:

```text
┌──────────────┐     ┌──────────────────┐     ┌──────────────┐
│  Web App UI  │────▶│  Service Worker   │────▶│  Remote Peer │
│  (React, etc)│◀────│  (Dialog + Wasm)  │◀────│  (optional)  │
└──────────────┘     └──────────────────┘     └──────────────┘
```

The service worker holds a Dialog instance with its full query engine, transaction support, and sync capabilities. The web app communicates with it through the standard `postMessage` API or through intercepted fetch requests.

The remote peer is optional. The application works fully offline because all data operations go through the local Dialog instance. When a network connection is available, the service worker can sync with remote peers using Dialog's built-in sync protocol.

## Why a service worker?

Service workers persist across page loads. Unlike in-memory state that disappears when you close a tab, a service worker (combined with persistent storage like IndexedDB or the Origin Private File System) maintains the database across sessions.

Service workers also run on a separate thread from the UI. Database operations don't block rendering, and heavy queries or sync operations happen in the background.

## Queries from the UI

From the web app's perspective, interacting with Dialog looks like sending a query description and receiving results. The notation described in the [previous chapter](./notation.md) is what makes this work: the web app describes a query using the platform-agnostic notation, the service worker's Dialog instance executes it, and the results come back as plain JSON.

This means the web app doesn't need to know about Rust types, derive macros, or the query planner. It works with the notation-level description of attributes, concepts, and patterns.

## Reactive queries

One of the more interesting possibilities with this architecture is reactive queries. Instead of polling for changes, the web app can subscribe to a query. When the underlying data changes (either from a local edit or from a sync with a remote peer), the service worker re-evaluates the query and pushes updated results to the UI.

This is similar to how systems like [Automerge](https://automerge.org/) provide change notifications, but integrated with Dialog's query engine. The UI doesn't subscribe to "changes to document X." It subscribes to "the results of this query," which might span multiple entities and include derived data from rules.

## Current state

The web integration is an active area of development. The core pieces exist: Dialog compiles to Wasm, runs in service workers, and can sync between peers. The API surface for web applications is still being refined.

If you're interested in exploring this direction, the [Tonk project](https://github.com/tonk-labs/tonk) is the best starting point.
