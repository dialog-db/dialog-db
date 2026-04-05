# Space and Storage Redesign

## Motivation

Profile and Repository opening currently uses a bootstrap DID hack to load credentials via capability routing before the real DID is known. The `Storage` type is a DID router (`HashMap<Did, Store>`) that requires mounting before capabilities can be dispatched.

The goal is to make `Space` the self-contained unit: one address, one credential, one composed store. `Storage` becomes a configured router that knows how to create spaces from addresses.

## Core Types

### Space

A loaded space with its credential and composed store:

```rust
pub struct Space {
    credential: Credential,
    store: ComposedStore,
}

impl Space {
    /// Load an existing space from an address.
    pub async fn load(address: impl Into<SpaceAddress>) -> Result<Space, SpaceError>;

    /// Create a new space at an address (generates credential).
    pub async fn create(address: impl Into<SpaceAddress>) -> Result<Space, SpaceError>;

    /// Load or create.
    pub async fn open(address: impl Into<SpaceAddress>) -> Result<Space, SpaceError>;

    pub fn did(&self) -> Did { self.credential.did() }
    pub fn credential(&self) -> &Credential { &self.credential }
}
```

`Space::load` opens the backend at the address, reads the credential from a well-known path, and returns the space. No DID routing needed since the store is addressed directly.

### SpaceAddress

Platform-specific address that determines the backend:

```rust
// Native
SpaceAddress::FileSystem(path)      // single directory
// Web  
SpaceAddress::IndexedDb(name)       // single IDB database + OPFS
// In-memory
SpaceAddress::Volatile(prefix)      // HashMap
```

Convenience constructors:

```rust
SpaceAddress::profile("work")    // ~/.dialog/profiles/work (native), profile/work (web)
SpaceAddress::current("myapp")   // ./.dialog/myapp (native), storage/myapp (web)
SpaceAddress::temp("scratch")    // /tmp/.dialog/scratch (native), temp/scratch (web)
```

### ComposedStore

The store backing a space. Provides archive, memory, permit, and credential effects. Composition is configurable:

```rust
pub struct ComposedStore {
    archive: ArchiveProvider,
    memory: MemoryProvider,
    permit: PermitProvider,
    credential: CredentialProvider,
}
```

Default: all backed by the same platform store (FileStore/IDB/Volatile). Custom compositions allow different backends per concern (e.g., archive on compressed storage, memory on fast cache).

### Storage (Router)

Maps DIDs to Spaces. Configured with address factories for different space types:

```rust
pub struct Storage {
    spaces: HashMap<Did, Space>,
}

impl Storage {
    pub fn new() -> Self;
    pub fn mount(&mut self, space: Space);
    // Routes effects by subject DID to the space's composed store
}
```

## Setup Flow

```rust
// 1. Open profile space (direct address, no routing needed)
let profile_space = Space::open(SpaceAddress::profile("work")).await?;
let profile = Profile::try_from(&profile_space)?;

// 2. Build operator with storage router
let mut storage = Storage::new();
storage.mount(profile_space);

let operator = profile
    .derive(b"alice")
    .network(Remote)
    .build(storage)?;

// 3. Open repos through operator (registers in storage)
let repo = Repository::open(SpaceAddress::current("contacts"))
    .perform(&operator)
    .await?;
```

## Profile and Repository

Profile and Repository become thin wrappers over Space:

```rust
impl Profile {
    pub fn try_from(space: &Space) -> Result<Profile, ProfileError> {
        match space.credential() {
            Credential::Signer(s) => Ok(Profile { credential: s.clone(), ... }),
            Credential::Verifier(_) => Err(ProfileError::Key("verifier-only")),
        }
    }
}

impl Repository {
    pub fn from(space: &Space) -> Repository {
        // Always succeeds, repos work with any credential
        Repository { credential: space.credential().clone(), ... }
    }
}
```

## Space Composition

Default composition uses a single platform backend:

```rust
Space::open(SpaceAddress::profile("work")).await?
// All effects backed by FileStore at ~/.dialog/profiles/work/
```

Custom composition for advanced use cases:

```rust
Space::builder(SpaceAddress::profile("work"))
    .archive(CompressedFileStore::new(path))
    .memory(CachedStore::new(volatile, file_backed))
    .build()
    .await?
```

## On-Disk Layout (FileSystem)

```
~/.dialog/profiles/work/
  credential          # Ed25519 keypair (multicodec)
  archive/
    {catalog}/
      {digest}        # content-addressed blobs
  memory/
    {space}/
      {cell}          # cell values (CBOR)
  permit/
    {audience}/
      {subject}/
        {issuer}.{hash}  # delegation proofs
```

## On-Web Layout (IndexedDB)

Database: `profile/work`
Object stores:
- `credential` (key: path, value: CryptoKeyPair JsValue)
- `archive` (key: `{catalog}/{digest}`, value: Uint8Array)
- `memory` (key: `{space}/{cell}`, value: Uint8Array)
- `permit` (key: `{audience}/{subject}/{issuer}.{hash}`, value: Uint8Array)

## Open Questions

- Should Space::open take a builder/config for composition, or is the default always single-backend?
- How does temp storage work? Volatile space that gets mounted but never persisted?
- Should Storage be the thing that knows about platform defaults, or should SpaceAddress constructors handle that?
- How does the operator builder interact with Storage? Currently it takes Storage and creates delegations. With this design, delegations would be stored in the profile space's permit store.
