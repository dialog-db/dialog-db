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
let storage = Storage::default();

let profile = Profile::open("alice")
    .perform(&storage)
    .await?;

let operator = profile
    .derive(b"my-app")
    .allow(Subject::any())
    .build(storage)
    .await?;
```

The operator's base directory defaults to `Directory::Current`. Override it with `.base()`:

```rs
let operator = profile
    .derive(b"my-app")
    .base(Directory::Temp)
    .allow(Subject::any())
    .build(storage)
    .await?;
```

Note: `.build(storage)` takes ownership of the storage value.

## Repository

A repository has its own keypair, branches, and remotes. Same name under the same profile always yields the same identity.

Repositories are opened through the profile, which provides the correct subject DID. The operator resolves the name against its base directory and verifies access.

```rs
let repo = profile.repository("contacts")
    .open()
    .perform(&operator)
    .await?;

let main = repo
    .branch("main")
    .open()
    .perform(&operator)
    .await?;
```

Repository modes:

- `.open()` loads existing or creates new. Returns `Repository<Credential>`.
- `.load()` loads existing, fails if not found. Returns `Repository<Credential>`.
- `.create()` creates new, fails if exists. Returns `Repository<SignerCredential>`.

Branch modes:

- `.open()` loads existing or creates new.
- `.load()` loads existing, fails if not found.

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

Register a remote and set the branch's upstream, then push and pull:

```rs
// Create remote with a UCAN access service
let origin = repo.remote("origin")
    .create(SiteAddress::Ucan(UcanAddress::new("https://access.example.com")))
    .perform(&operator).await?;

// Open remote branch and set as upstream
let remote_main = origin.branch("main").open().perform(&operator).await?;
main.set_upstream(remote_main).perform(&operator).await?;

main.push().perform(&operator).await?;
main.pull().perform(&operator).await?;
```

To point at a different repository (e.g., pulling from someone else's repo):

```rs
let origin = bob_repo.remote("origin")
    .create(SiteAddress::Ucan(UcanAddress::new("https://access.example.com")))
    .subject(alice_repo.did())  // target Alice's repo, not Bob's
    .perform(&bob_operator).await?;
```

When a branch has a remote upstream, queries automatically replicate missing blocks on demand.

## Collaboration

Access is shared through UCAN delegation: signed tokens forming a chain of trust.

The access API follows a fluent pattern: `.access().claim(&capability).delegate(audience)`.

- `repo.access()` and `profile.access()` return an `Access` handle
- `.claim()` takes anything that converts into a `Capability` (a `&repo`, `&profile`, or capability chain)
- `.delegate(audience_did)` produces a delegation chain on `.perform()`
- `.save(chain)` stores a received delegation under the profile

### Alice sets up a shared repo

```rs
let repo = alice_profile.repository("shared")
    .create()
    .perform(&alice_operator).await?;

// Repo delegates to Alice's profile
let chain = repo.access()
    .claim(&repo)
    .delegate(alice_profile.did())
    .perform(&alice_operator)
    .await?;

// Alice saves the delegation under her profile
alice_profile
    .access()
    .save(chain)
    .perform(&alice_operator)
    .await?;

let origin = repo.remote("origin")
    .create(SiteAddress::Ucan(UcanAddress::new("https://access.example.com")))
    .perform(&alice_operator).await?;

let main = repo.branch("main").open().perform(&alice_operator).await?;
let remote_main = origin.branch("main").open().perform(&alice_operator).await?;
main.set_upstream(remote_main).perform(&alice_operator).await?;

main.transaction()
    .assert(Name::of(alice).is("Alice"))
    .commit()
    .perform(&alice_operator)
    .await?;

main.push().perform(&alice_operator).await?;
```

### Alice invites Bob

Alice claims her authority over the repo and re-delegates to Bob. The resulting chain includes the full proof path from the repo subject through Alice to Bob.

```rs
let chain = alice_profile.access()
    .claim(&repo)
    .delegate(bob_profile.did())
    .perform(&alice_operator).await?;
```

Optional time bounds can constrain the delegation:

```rs
let chain = alice_profile.access()
    .claim(&repo)
    .not_before(start)
    .expires(end)
    .delegate(bob_profile.did())
    .perform(&alice_operator).await?;
```

### Bob joins

Bob saves the delegation under his profile. This is what authorizes his operator to act on Alice's repo.

```rs
bob_profile.access().save(chain).perform(&bob_operator).await?;

let bob_repo = bob_profile.repository("bob-copy")
    .open()
    .perform(&bob_operator)
    .await?;

let origin = bob_repo.remote("origin")
    .create(SiteAddress::Ucan(UcanAddress::new("https://access.example.com")))
    .subject(alice_repo_did)  // point at Alice's repo
    .perform(&bob_operator).await?;

let main = bob_repo.branch("main").open().perform(&bob_operator).await?;
let remote_main = origin.branch("main").open().perform(&bob_operator).await?;
main.set_upstream(remote_main).perform(&bob_operator).await?;

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
