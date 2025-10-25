# dialog-remote

Remote synchronization protocol for dialog-db.

## Overview

This crate implements revision backends for managing remote tree state in dialog-db. Backends provide storage and retrieval of revisions with compare-and-swap semantics, enabling git-like synchronization between replicas.

The sync protocol enables git-like synchronization of dialog-db trees through:
- A **Register** - holds the canonical root revision (implemented by backends)
- An **Archive** - stores immutable, hash-addressed tree nodes (handled by `dialog-storage`)

## Architecture

The core abstraction is `RevisionBackend`, which provides three operations:

```rust
#[async_trait]
pub trait RevisionBackend: ConditionalSync + Clone {
    /// Get the cached/last known revision without making a network request
    fn revision(&self, subject: &Subject) -> Result<Option<Revision>, RevisionBackendError>;

    /// Fetch the latest revision from the remote
    async fn fetch(&mut self, subject: &Subject) -> Result<Revision, RevisionBackendError>;

    /// Publish a new revision with compare-and-swap semantics
    async fn swap(
        &mut self,
        subject: &Subject,
        expected: &Revision,
        new: &Revision,
    ) -> Result<(), RevisionBackendError>;
}
```

### Key Concepts

- **Subject**: A DID (Decentralized Identifier) that owns a revision
- **Revision**: Content-addressed identifier (blake3 hash) representing tree state
- **Compare-and-Swap (CAS)**: Atomic operation to prevent race conditions when multiple writers update the same revision

## Backends

### MemoryBackend

An in-memory implementation for testing with provider/consumer pattern.

```rust
use dialog_remote::backend::{MemoryBackendProvider, RevisionBackend, Subject};
use dialog_artifacts::Revision;

// Create a provider
let provider = MemoryBackendProvider::new();
let subject = Subject::new("did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi");
let initial = Revision::new(&[1; 32]);

provider.initialize(&subject, initial.clone()).await?;

// Create multiple consumers that share the same state
let mut consumer1 = provider.connect();
let mut consumer2 = provider.connect();

// Both fetch the same revision
let rev1 = consumer1.fetch(&subject).await?;
let rev2 = consumer2.fetch(&subject).await?;
assert_eq!(rev1, rev2);

// Attempt concurrent swaps - only one will succeed
let new1 = Revision::new(&[2; 32]);
let new2 = Revision::new(&[3; 32]);

let result1 = consumer1.swap(&subject, &initial, &new1).await;
let result2 = consumer2.swap(&subject, &initial, &new2).await;

// One succeeds, one fails with RevisionMismatch
```

**Use cases:**
- Testing concurrent sync scenarios
- Single-process prototyping
- Unit tests for sync logic

### RestBackend

An HTTP-based implementation that communicates with a remote Register service.

#### Protocol

The REST backend implements the Register protocol from [sync.md](../../notes/sync.md):

**Query Revision:**
```http
HEAD /{did}
Authorization: Bearer <token>  (optional)

Response:
200 OK
ETag: <revision-hex>
```

**Update Revision (Compare-and-Swap):**
```http
PUT /{did}
If-Match: <expected-revision-hex>
Authorization: Bearer <token>  (optional)
Content-Type: application/json

{
  "iss": "<did>",
  "sub": "<did>",
  "cmd": "/state/assert",
  "args": {
    "revision": "<new-revision-hex>"
  }
}

Response:
200 OK                         (success)
412 Precondition Failed        (revision mismatch)
ETag: <actual-revision-hex>
```

#### Authentication Methods

**None** - No authentication:
```rust
use dialog_remote::backend::{RestBackend, RestBackendConfig, AuthMethod};

let config = RestBackendConfig::new("https://api.example.com/register");
let mut backend = RestBackend::new(config);
```

**Bearer Token** - Simple bearer token authentication:
```rust
use dialog_remote::backend::{RestBackend, RestBackendConfig, AuthMethod};

let config = RestBackendConfig::new("https://api.example.com/register")
    .with_auth(AuthMethod::Bearer("my-secret-token".to_string()))
    .with_timeout(60);

let mut backend = RestBackend::new(config);
```

#### Usage Example

```rust
use dialog_remote::backend::{
    RestBackend, RestBackendConfig, AuthMethod,
    RevisionBackend, Subject
};
use dialog_artifacts::Revision;

// Configure the backend
let config = RestBackendConfig::new("https://api.example.com/register")
    .with_auth(AuthMethod::Bearer("token".to_string()))
    .with_timeout(30)
    .with_header("X-Custom-Header", "value");

let mut backend = RestBackend::new(config);

// Fetch the current revision
let subject = Subject::new("did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi");
let current = backend.fetch(&subject).await?;

// Update with compare-and-swap
let new_rev = Revision::new(&[2; 32]);
backend.swap(&subject, &current, &new_rev).await?;
```

## Error Handling

```rust
pub enum RevisionBackendError {
    /// Access denied
    Unauthorized { subject: Subject, reason: String },

    /// Compare-and-swap failed (another writer updated first)
    RevisionMismatch {
        subject: Subject,
        expected: Revision,
        actual: Revision,
    },

    /// Failed to fetch
    FetchFailed { subject: Subject, reason: String },

    /// Failed to publish
    PublishFailed { subject: Subject, reason: String },

    /// Internal provider error
    ProviderError { subject: Subject, reason: String },

    /// Subject not found
    NotFound { subject: Subject },
}
```

## Sync Protocol

The synchronization process follows this flow:

1. **Fetch**: Query the Register for the latest root revision
2. **Reconcile**: Compute differential between local and remote trees
3. **Integrate**: Apply changes to create merged tree
4. **Swap**: Push the merged revision back with compare-and-swap

The `swap` operation ensures consistency when multiple replicas are updating the same remote:
- If `expected` matches the current remote revision, the swap succeeds
- If another replica updated first, `RevisionMismatch` is returned with the actual revision
- The caller can then re-fetch, re-merge, and retry

See the [full sync protocol design](../../notes/sync.md) for details.

## Design Decisions

### Trait-based Architecture

This crate uses a trait-based design to enable:
- **Extensibility**: New backend types can be added without modifying core code
- **Encapsulation**: Each implementation can have its own internal structure
- **Testing**: Easy to create mock implementations
- **Modularity**: Different backends can be enabled/disabled with feature flags

### Provider/Consumer Pattern for Memory Backend

The memory backend uses a provider/consumer pattern:
- The **provider** creates and manages the shared state
- **Consumers** are lightweight connections to that shared state
- Each consumer maintains its own cache but shares the same underlying storage

This enables testing concurrent scenarios like:
- Multiple replicas fetching the same revision
- Race conditions during concurrent swaps
- Merge conflict resolution

### Compare-and-Swap Semantics

The `swap(subject, expected, new)` API makes CAS semantics explicit:
- Caller must provide the expected revision
- No implicit state tracking in the backend
- Prevents accidental overwrites if operations fail mid-way

## Thread Safety

All backends implement `ConditionalSync` from `dialog-common` to ensure proper thread safety constraints across both native and WASM targets.

## WASM Support

All implementations are WASM-compatible:
- Uses `async_trait(?Send)` on WASM targets
- Memory backend works in WASM (with Arc<RwLock>)
- REST backend uses reqwest which supports WASM
- Tests use `wasm_bindgen_test` when compiled for WASM

## Testing

Run tests:
```bash
cargo test -p dialog-remote
```

Run with clippy:
```bash
cargo clippy -p dialog-remote -- -D warnings
```

## See Also

- [Sync Protocol Design](../../notes/sync.md) - Full protocol specification
- `dialog-artifacts` - Provides the `Revision` type
- `dialog-storage` - Archive storage backends
- `dialog-common` - Cross-platform utilities like `ConditionalSync`
