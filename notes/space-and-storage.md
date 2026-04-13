# Space and Storage Design

## Overview

Storage is the runtime environment that routes capability effects to the correct backend providers. It uses a two-level dispatch: first by subject DID (which space), then by capability (which provider within that space).

## Core Types

### Location

A struct combining a directory kind with a name:

```rust
pub struct Location {
    pub directory: Directory,
    pub name: String,
}
```

`Directory` is an enum of logical address categories:

```rust
pub enum Directory {
    Profile,   // platform profile directory (~/Library/.../dialog/ on macOS)
    Current,   // working directory (.dialog/)
    Temp,      // temporary directory
    At(String) // custom path
}
```

Each backend resolves a `Location` to its platform-specific path. The `StorageFx` sugar creates locations:

```rust
StorageFx::profile("alice")   // Location { directory: Profile, name: "alice" }
StorageFx::temp("scratch")    // Location { directory: Temp, name: "scratch" }
```

### Space

A composed product of providers that routes capabilities to the correct backend:

```rust
#[derive(Provider)]
pub struct Space<A, M, C, D> {
    #[provide(archive::Get, archive::Put)]
    archive: A,

    #[provide(memory::Resolve, memory::Publish, memory::Retract)]
    memory: M,

    #[provide(credential::Load<Credential>, credential::Save<Credential>)]
    credential: C,

    #[provide(access::Prove<P>, access::Retain<P>)]
    certificate: D,
}
```

`#[derive(Provider)]` generates capability dispatch: `archive::Get` goes to `archive`, `memory::Resolve` goes to `memory`, etc.

### Storage

Composes a `Loader` (for space bootstrap via `storage::Load`/`storage::Create`) and a `Router` (DID-based dispatch for all other effects):

```rust
#[derive(Provider)]
pub struct Storage<S: SpaceProvider> {
    #[provide(storage::Load, storage::Create)]
    loader: Loader<S>,

    #[provide(archive::Get, archive::Put, memory::Resolve, ...)]
    router: Router<S>,
}
```

Platform defaults:
- **Native**: `NativeSpace` backed by `FileSystem` providers
- **Web**: `WebSpace` backed by `IndexedDb` providers

`Storage::default()` creates the platform-appropriate configuration.

## Mounting Flow

When `storage::Load` or `storage::Create` is performed:

1. The `Loader` resolves the `Location` to platform-specific addresses
2. Creates provider instances for each capability domain
3. Reads the credential directly from the credential provider (no DID needed yet)
4. Registers the space under its DID in the `Router`
5. Returns the `Credential` (signer or verifier)

No bootstrap DID hack. The credential is read directly from the provider before registering in the router.

## Two Levels of Dispatch

```
Effect arrives with Subject DID
  |
  v
Router looks up DID -> Space
  |
  v
Space routes by capability -> Provider
  |
  v
Provider executes the effect
```

## On-Disk Layout (FileSystem)

```
~/Library/.../dialog/alice/        <-- Location { Profile, "alice" }
  credentials/self                 <-- credential provider
  archive/                         <-- archive provider (index + blob)
  memory/                          <-- memory provider
  certificates/                    <-- certificate store
```

## On-Web Layout (IndexedDB)

```
Database: "alice"
Object stores:
  "credentials"                    <-- credential provider
  "archive"                        <-- archive provider
  "memory"                         <-- memory provider
  "certificates"                   <-- certificate store
```

## Setup Flow

```rust
let storage = Storage::default();

let profile = Profile::open("alice")
    .perform(&storage)
    .await?;

let operator = profile
    .derive(b"my-app")
    .allow(Subject::any())
    .build(storage)
    .await?;

let repo = profile.repository("contacts")
    .open()
    .perform(&operator)
    .await?;
```

`Profile::open` triggers `storage::Load` internally. `Repository::open` does the same for the repo's space. The `Operator` holds the assembled `Storage` and routes all subsequent effects through it.
