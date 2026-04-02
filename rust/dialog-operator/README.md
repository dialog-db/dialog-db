# dialog-operator

Identity and operating environment for Dialog-DB.

This crate provides the identity layer (authority, profiles, operators) and
the operating environment (storage dispatch, remote dispatch) that sits
between raw capabilities and the repository abstraction.

## Identity Model

Three Ed25519 keypairs form a delegation chain, each identified by a `did:key`:

- **Profile**: a named identity on a device. Created on first use, persists
  for the device lifetime. Lives at a storage location.
- **Operator**: a session key derived deterministically from the profile.
  Same profile + context always yields the same key. Ephemeral, revocable.
- **Account** *(optional)*: a passkey or hardware key for cross-device
  recovery. Can be deferred.

Every capability invocation carries a delegation chain:
`subject -> profile -> operator`.

## Setup

```rust,ignore
use dialog_operator::{Authority, Profile, Operator, Remote};
use dialog_operator::storage::Storage;
use dialog_capability::Subject;

// 1. Create a storage dispatcher
let storage = Storage::temp_storage();

// 2. Open (or create) a profile at a storage location
let profile = Profile::open(Storage::profile("alice"))
    .perform(&storage)
    .await?;

// 3. Derive an operator with capability grants
let operator = profile
    .derive(b"my-app")
    .allow(Subject::any())       // powerline: full access
    .build(storage)
    .await?;
```

## Storage Locations

Storage locations are capabilities. Point them wherever you want:

```rust,ignore
Storage::profile("my-app");       // platform data dir (~/.local/share on Linux)
Storage::current("my-project");   // working directory (native only)
Storage::temp("test");            // temporary / in-memory
```

On native, these resolve to filesystem paths. On web, they resolve to
IndexedDB databases. `Storage::temp_storage()` creates a volatile
(in-memory) dispatcher for testing.

## Components

### Authority

Holds profile and operator signers. Implements `Provider<Identify>` and
`Provider<Sign>` so the capability system can resolve identity and produce
signatures.

### Profile

A named identity backed by a signing credential at a storage location.
`Profile::open(location)` loads an existing profile or creates a new one.
Profiles are the long-lived identity; operators are derived from them.

### Operator

The top-level operating environment. Composes:
- `Authority` for identity and signing
- `Storage` for DID-routed storage dispatch
- `Remote` for fork-based remote operations

Built via `profile.derive(context).allow(...).build(storage)`.

### Storage

DID-routed effect dispatcher. Each subject DID maps to a storage backend
(filesystem, IndexedDB, or volatile). Handles Load/Save/Mount for
location-addressed content and Get/Set/Delete/List for key-value stores.

### Remote

Dispatch wrapper for fork invocations targeting remote sites (e.g., S3
with UCAN authorization).
