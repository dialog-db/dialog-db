# Dialog

Dialog is a local-first database with built-in identity, replication, and delegated access control.

## Identity

Three Ed25519 keypairs, each identified by a `did:key`:

- **Profile**: a named identity on a device. Created on first use, persists for the device lifetime.
- **Operator**: a session key derived from the profile. Same profile + context always yields the same key. Ephemeral, revocable.
- **Account** *(optional)*: a passkey or hardware key for cross-device recovery. Can be deferred.

Every capability invocation carries a delegation chain: `subject -> profile -> operator`.

## Setup

```rs
let storage = Storage::temp_storage();

let profile = Profile::open(Storage::profile("alice"))
    .perform(&storage)
    .await?;

let operator = profile
    .derive(b"my-app")
    .allow(Subject::any())
    .network(Remote)
    .build(storage)
    .await?;
```

Storage locations are capabilities. Point them wherever you want:

```rs
Storage::profile("my-app");       // platform data dir
Storage::current("my-project");   // working directory (native)
Storage::temp("test");            // temporary / in-memory
```

## Repository

A repository has its own keypair, branches, and remotes. Same location always yields the same identity.

```rs
let repo = Repository::open(Storage::current("contacts"))
    .perform(&operator).await?;

let main = repo.branch("main").open().perform(&operator).await?;
```

## Writing

Data is stored as semantic triples: *the `attribute` of `entity` is `value`*.

Typed writes use `branch.transaction()`:

```rs
main.transaction()
    .assert(Name::of(alice).is("Alice"))
    .assert(Age::of(alice).is(30u32))
    .commit()
    .perform(&operator)
    .await?;
```

## Querying

Typed queries use concepts defined with derive macros:

```rs
#[derive(Concept)]
struct Employee {
    this: Entity,
    name: employee::Name,
    role: employee::Role,
}

let results: Vec<Employee> = main
    .select(Query::<Employee> {
        this: Term::var("this"),
        name: Term::var("name"),
        role: Term::var("role"),
    })
    .perform(&operator)
    .try_vec()
    .await?;
```

For queries with deductive rules:

```rs
let results: Vec<Employee> = main
    .query()
    .install(my_rule)?
    .select(Query::<Employee> { ... })
    .perform(&operator)
    .try_vec()
    .await?;
```

Raw artifact selection (with automatic remote fallback):

```rs
let artifacts = main
    .claims()
    .select(ArtifactSelector::new().the("user/name".parse()?))
    .perform(&operator)
    .await?
    .collect::<Vec<_>>()
    .await;
```

## Syncing

Register a UCAN remote and set the branch's upstream, then push and pull:

```rs
let remote_address = RemoteAddress::new(
    SiteAddress::Ucan(UcanAddress::new("https://access.example.com")),
    repo.did(),
);

repo.remote("origin")
    .create(remote_address)
    .perform(&operator).await?;

main.set_upstream(UpstreamState::Remote {
    name: RemoteName::from("origin"),
    branch: "main".into(),
    tree: NodeReference::default(),
}).perform(&operator).await?;

main.push().perform(&operator).await?;
main.pull().perform(&operator).await?;
```

When a branch has a remote upstream, queries automatically replicate missing blocks on demand.

## Collaboration

Access is shared through UCAN delegation: signed tokens forming a chain of trust.

### Alice sets up a shared repo

```rs
let repo = Repository::open(Storage::current("shared"))
    .perform(&alice_operator).await?;

let chain = repo.ownership()
    .delegate(&alice_profile)
    .perform(&alice_operator).await?;
alice_profile.save(chain).perform(&alice_operator).await?;

let remote_address = RemoteAddress::new(
    SiteAddress::Ucan(UcanAddress::new("https://access.example.com")),
    repo.did(),
);
repo.remote("origin").create(remote_address)
    .perform(&alice_operator).await?;

let main = repo.branch("main").open().perform(&alice_operator).await?;
main.set_upstream(UpstreamState::Remote {
    name: RemoteName::from("origin"),
    branch: "main".into(),
    tree: NodeReference::default(),
}).perform(&alice_operator).await?;

main.transaction()
    .assert(Name::of(alice).is("Alice"))
    .commit()
    .perform(&alice_operator)
    .await?;

main.push().perform(&alice_operator).await?;
```

### Alice invites Bob

Alice delegates repo access from her profile to Bob's. The resulting chain includes the full proof path from the repo to Bob.

```rs
let delegation = Ucan::delegate(&Subject::from(repo.did()))
    .audience(bob_profile.did())
    .issuer(alice_profile.credential().signer().clone())
    .perform(&alice_operator).await?;
```

### Bob joins

Bob saves the delegation under his profile. This is what authorizes his operator to act on Alice's repo.

```rs
bob_profile.save(delegation).perform(&bob_operator).await?;

let bob_repo = Repository::open(Storage::current("bob-copy"))
    .perform(&bob_operator).await?;

let remote_address = RemoteAddress::new(
    SiteAddress::Ucan(UcanAddress::new("https://access.example.com")),
    alice_repo_did,
);
bob_repo.remote("origin").create(remote_address)
    .perform(&bob_operator).await?;

let main = bob_repo.branch("main").open().perform(&bob_operator).await?;
main.set_upstream(UpstreamState::Remote {
    name: RemoteName::from("origin"),
    branch: "main".into(),
    tree: NodeReference::default(),
}).perform(&bob_operator).await?;

// Pull, edit, push
main.pull().perform(&bob_operator).await?;

main.transaction()
    .assert(Name::of(bob).is("Bob"))
    .commit()
    .perform(&bob_operator)
    .await?;

main.push().perform(&bob_operator).await?;
```

Alice pulls to get Bob's changes:

```rs
main.pull().perform(&operator).await?;
```

### Scoped delegation

Access can be narrowed to specific domains before delegating:

```rs
// Full ownership
let chain = repo.ownership()
    .delegate(&audience).perform(&operator).await?;

// Archive only
let chain = repo.ownership().archive()
    .delegate(&audience).perform(&operator).await?;

// Specific catalog
let chain = repo.ownership().archive().catalog("index")
    .delegate(&audience).perform(&operator).await?;

// Memory only
let chain = repo.ownership().memory()
    .delegate(&audience).perform(&operator).await?;
```
