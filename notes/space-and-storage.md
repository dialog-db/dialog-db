# Space and Storage Redesign

## Motivation

Profile and Repository opening currently uses a bootstrap DID hack to load credentials via capability routing before the real DID is known. Storage is a DID router that requires pre-mounting.

The goal: Storage holds factories and a DID router. Mounting a Location creates a Space (composed providers). Space routes capabilities to providers. Storage routes DIDs to Spaces.

## Core Types

### Location

Enum of logical address types. Each backend extracts its platform-specific address via `From` conversions:

```rust
pub enum Location {
    Profile(String),      // "alice"
    Workspace(String),    // "contacts"
    Temp(String),         // "scratch"
    Custom { fs: PathBuf, idb: String, opfs: String },
}

// Each backend resolves from Location
impl From<&Location> for FsAddress {
    fn from(loc: &Location) -> Self {
        match loc {
            Location::Profile(name) => /* ~/Library/.../dialog/{name} */,
            Location::Workspace(name) => /* $PWD/.dialog/{name} */,
            Location::Temp(name) => /* /tmp/.dialog/{name} */,
            Location::Custom { fs, .. } => FsAddress(fs.clone()),
        }
    }
}

impl From<&Location> for IdbAddress {
    fn from(loc: &Location) -> Self {
        match loc {
            Location::Profile(name) => IdbAddress(format!("{name}.profile")),
            Location::Workspace(name) => IdbAddress(name.clone()),
            Location::Temp(name) => IdbAddress(format!("temp.{name}")),
            Location::Custom { idb, .. } => IdbAddress(idb.clone()),
        }
    }
}

// Same for OpfsAddress
```

### Factory

Creates a provider from a Location. Each backend type implements this:

```rust
trait Factory {
    type Provider;
    fn create(&self, location: &Location) -> Self::Provider;
}
```

For example, an `IdbFactory` with a relative store name:

```rust
struct IdbFactory {
    store: &'static str,  // "archive/index", "memory", etc.
}

impl Factory for IdbFactory {
    type Provider = IndexedDb;
    fn create(&self, location: &Location) -> IndexedDb {
        let addr = IdbAddress::from(location);
        IndexedDb::open(addr, self.store)
    }
}
```

### Space

Product of providers created by factories. Routes capabilities to the right provider:

```rust
#[derive(Provider)]
pub struct Space<Index, Blob, Memory, Cred, Permit> {
    #[provide(archive::Get)]
    index: Index,

    #[provide(archive::Put)]
    blob: Blob,

    #[provide(memory::Resolve, memory::Publish, memory::Retract)]
    memory: Memory,

    #[provide(credential::Load, credential::Save)]
    credential: Cred,

    #[provide(access::Claim, access::Save)]
    permit: Permit,
}
```

`#[derive(Provider)]` generates capability dispatch: `archive::Get` goes to `index`, `memory::Resolve` goes to `memory`, etc.

### Storage

Holds factories (configuration) and a DID router (runtime state). Generic over factory types with platform defaults:

```rust
pub struct Storage<IF, BF, MF, CF, PF> {
    index_factory: IF,
    blob_factory: BF,
    memory_factory: MF,
    credential_factory: CF,
    permit_factory: PF,
    router: HashMap<Did, Space<IF::Provider, BF::Provider, MF::Provider, CF::Provider, PF::Provider>>,
}
```

Storage routes effects by Subject DID to the matching Space, which routes by capability to the right provider.

## Mounting

```rust
impl<IF: Factory, BF: Factory, MF: Factory, CF: Factory, PF: Factory> Storage<IF, BF, MF, CF, PF> {
    pub async fn mount(&mut self, location: &Location) -> Result<Did, Error> {
        // Create providers from factories
        let space = Space {
            index: self.index_factory.create(location),
            blob: self.blob_factory.create(location),
            memory: self.memory_factory.create(location),
            credential: self.credential_factory.create(location),
            permit: self.permit_factory.create(location),
        };

        // Read credential directly from the credential provider
        // No DID routing needed, direct provider access
        let credential = space.credential.load("credential").await?;
        let did = credential.did();

        self.router.insert(did, space);
        Ok(did)
    }
}
```

No bootstrap DID hack. The credential is read directly from the provider before registering in the router.

## Platform Defaults

```rust
#[cfg(not(target_arch = "wasm32"))]
type DefaultStorage = Storage<FsFactory, FsFactory, FsFactory, FsFactory, FsFactory>;

#[cfg(target_arch = "wasm32")]
type DefaultStorage = Storage<IdbFactory, OpfsFactory, IdbFactory, IdbFactory, IdbFactory>;

impl DefaultStorage {
    pub fn new() -> Self {
        Self {
            index_factory: FsFactory::new("archive/index"),
            blob_factory: FsFactory::new("archive/blob"),
            memory_factory: FsFactory::new("memory"),
            credential_factory: FsFactory::new("credential"),
            permit_factory: FsFactory::new("permit"),
            router: HashMap::new(),
        }
    }
}
```

Custom configuration:

```rust
let storage = Storage::new()
    .index(IdbFactory::new("archive/index"))
    .blob(OpfsFactory::new("archive/blob"))
    .memory(IdbFactory::new("memory"))
    .credential(IdbFactory::new("credential"))
    .permit(IdbFactory::new("permit"));
```

## Setup Flow

```rust
// Default storage for the platform
let mut storage = Storage::default();

// Open profile (mounts location, reads credential, registers DID)
let profile = Profile::open(Location::profile("alice"))
    .perform(&mut storage)
    .await?;

// Build operator
let operator = profile
    .derive(b"alice")
    .network(Remote)
    .build(&mut storage)?;

// Open repository
let repo = Repository::open(Location::workspace("contacts"))
    .perform(&mut storage)
    .await?;
```

Profile::open and Repository::open just call `storage.mount(location)` and wrap the result:

```rust
impl Profile {
    pub async fn open(location: Location) -> OpenProfile {
        OpenProfile { location, mode: OpenOrCreate }
    }
}

impl OpenProfile {
    pub async fn perform<S>(self, storage: &mut S) -> Result<Profile, ProfileError> {
        let did = storage.mount(&self.location).await?;
        let space = storage.get(&did);
        let credential = space.credential();
        match credential {
            Credential::Signer(s) => Ok(Profile { credential: s, did }),
            Credential::Verifier(_) => Err(ProfileError::Key("verifier-only")),
        }
    }
}
```

## Two Levels of Dispatch

```
Effect arrives with Subject DID
  |
  v
Storage routes by DID -> Space
  |
  v
Space routes by capability -> Provider
  |
  v
Provider executes the effect
```

## On-Disk Layout (FileSystem)

```
~/Library/.../dialog/alice/        <- Location::Profile("alice")
  credential/                      <- credential factory path
  archive/index/                   <- index factory path
  archive/blob/                    <- blob factory path
  memory/                          <- memory factory path
  permit/                          <- permit factory path
```

## On-Web Layout (IndexedDB + OPFS)

Database: `alice.profile`          <- IdbAddress::from(Location::Profile("alice"))
Object stores:
- `credential`                     <- credential factory store name
- `archive/index`                  <- index factory store name
- `memory`                         <- memory factory store name
- `permit`                         <- permit factory store name

OPFS:
- `/dialog/profile/alice/archive/blob/`  <- OpfsAddress::from(loc) + blob factory path
