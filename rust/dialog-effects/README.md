# dialog-effects

Domain-specific capability hierarchy types for Dialog storage operations.

This crate defines the structural types (attenuations, policies, and effects) that form capability chains for storage operations. These types are used by both the storage layer (`dialog-storage`) and the credentials layer (`dialog-s3-credentials`) to build and validate capabilities.

## Capability Domains

- **storage**: Key-value storage operations (`Storage`, `Store`, `Get`, `Set`, `Delete`, `List`)
- **memory**: CAS memory cells with edition-based concurrency (`Memory`, `Space`, `Cell`, `Resolve`, `Publish`, `Retract`)
- **archive**: Content-addressed archive storage (`Archive`, `Catalog`, `Get`, `Put`)

## Usage

```rust
use dialog_effects::storage::{Storage, Store, Get};
use dialog_capability::Subject;

// Build a capability to get a value from the "index" store
let capability = Subject::from("did:key:z6Mk...")
    .attenuate(Storage)              // Domain: storage operations
    .attenuate(Store::new("index"))  // Policy: only the "index" store
    .invoke(Get::new(b"my-key"));    // Effect: get this specific key
```

## Architecture

This crate provides the capability *structure* (what operations exist and how they're scoped), while:
- `dialog-capability` provides the capability *primitives* (Subject, Attenuation, Effect traits)
- `dialog-storage` provides the capability *execution* (Provider implementations)
- `dialog-s3-credentials` provides the capability *authorization* (signing and access control)
