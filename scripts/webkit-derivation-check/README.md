# WebKit derivation check

Runs the browser half of operator derivation (see `notes/operator-derivation.md`)
against a real **WebKit** build and compares it to the value Rust computes.

CI runs wasm tests in Chromium only, so nothing in the test suite exercises
WebKit. This does, out of band:

```
npx playwright install webkit && npx playwright install-deps webkit
PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers node run.mjs webkit .
PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers node run.mjs chromium .   # cross-check
```

`page.html` reimplements the derivation in plain WebCrypto — X25519
self-agreement, then HKDF-SHA256 with the same `info` — against constants
taken from the Rust fixtures, and also drives the AES-KW archive round trip
through IndexedDB that WebKit forces. The expected values are the ones pinned
by `derivation_matches_a_known_vector` in `dialog-credentials`, so a mismatch
means the browser and Rust disagree.

## What it establishes, and what it does not

It confirms WebKit computes the same shared secret and the same derived bytes
as Rust; that the derivation is stable across repeated runs; that WebKit
cannot structured-clone an X25519 `CryptoKey` (`TypeError: Unable to
deserialize data`), which is why the wrapped archive exists; and that the
derivation still reaches the pinned value after a wrapped archive round trip
through IndexedDB.

It does **not** reproduce the Safari bug that motivated the scheme. Hedged
Ed25519 nonces come from Apple's CryptoKit/corecrypto, not from WebKit itself,
so a WebKit build on Linux signs deterministically — the harness reports this
under `[informational]`. That is a reason the old signature-based derivation
could not have been caught here either; the derivation no longer signs at all,
which is the point.
