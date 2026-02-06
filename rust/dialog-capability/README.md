# dialog-capability

Capability-based authorization primitives for Rust.

## Overview

This crate provides a hierarchical capability system for authorization and access control. Capabilities form chains from a root `Subject` (represented by [did:key](https://w3c-ccg.github.io/did-method-key/)) through any number of constraints down to `Effect`s that perform actual operations.

## Quick Example

```rust
use dialog_capability::{Subject, Attenuation, Policy, Effect};
use serde::{Serialize, Deserialize};

// Attenuation: narrows ability (adds "/storage" to path)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Storage;
impl Attenuation for Storage {
    type Of = Subject;
}

// Policy: constrains parameters only (no path change)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Store { name: String }
impl Policy for Store {
    type Of = Storage;
}

// Effect: narrows ability (adds "/get"), and is invocable
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Get { key: Vec<u8> }
impl Effect for Get {
    type Of = Store;
    type Output = Result<Option<Vec<u8>>, std::io::Error>;
}

// Build a capability chain
let capability = Subject::from("did:key:z6MkhaXgBZD...")
    .attenuate(Storage)                        // ability: /storage
    .attenuate(Store { name: "index".into() }) // ability: /storage (unchanged)
    .invoke(Get { key: b"my-key".to_vec() });  // ability: /storage/get

// The ability is expressed as a path
assert_eq!(capability.ability(), "/storage/get");

// Extract constraint values from the chain
assert_eq!(Store::of(&capability).name, "index");
assert_eq!(Get::of(&capability).key, b"my-key");
```

## Core Concepts

### Subject

A `Subject` is the root of every capability chain - it identifies the resource (via a DID) and represents full authority: ability `/` with no policy constraints.

### Capability Space

Every `Subject` defines a capability space — the full set of operations that can be performed on that resource. The space is organized as a hierarchy of ability paths:

```text
/                        (root — full authority)
/storage                 (all storage operations)
/storage/get             (only storage get)
/storage/set             (only storage set)
/memory                  (all memory operations)
/memory/publish          (only memory publish)
```

An ability path like `/storage` includes everything beneath it — `/storage/get`, `/storage/set`, etc. The root `/` encompasses the entire capability space.

Capabilities attenuate (narrow) this space. A capability with ability `/storage` grants access to all storage operations, while `/storage/get` restricts to just reads. Policies further constrain what's permitted within a given ability by adding parameters (e.g., which store, which key) without changing the ability path itself.


### Abilities and Policies

A capability represents a set of invocable operations (effects). This set is defined by:

- **Ability**: A path like `/storage` or `/storage/get` that determines which effects are included. An ability includes all effects whose path starts with it.
- **Policies**: Parameters that constrain how effects can be invoked, without changing the ability path.

### Capability Hierarchy

Capabilities are built as chains:

```text
Subject ("did:key:z6Mk...")            -> ability: /
  |-- Attenuation (e.g., Storage)      -> ability: /storage
        |-- Policy (e.g., Store)       -> ability: /storage (unchanged)
              |-- Effect (e.g., Get)   -> ability: /storage/get
```

### Key Traits

| Trait | Constrains | Example Types |
|-------|------------|---------------|
| `Policy` | Parameters only | `Store`, `Catalog`, `Cell` |
| `Attenuation` | Ability + parameters | `Storage`, `Memory`, `Archive` |
| `Effect` | Ability + parameters, invocable | `Get`, `Set`, `Resolve` |

## Features

- `ucan` - Enable UCAN (User Controlled Authorization Networks) support with IPLD serialization
