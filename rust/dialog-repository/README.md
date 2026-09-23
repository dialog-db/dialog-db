# dialog-repository

A git-like interface for Dialog-DB.

Provides repositories with branches, remotes, push/pull, and merge, but for structured data instead of files. Each repository has its own identity (keypair), named branches with revision history, and remotes for replication. Information is stored as claims: `{ the, of, is, cause }` facts where `the` is the relation, `of` is the entity, `is` is the value, and `cause` is the provenance. Claims can be queried with typed concepts or deductive rules. Same name under the same profile always yields the same repository identity.

## Usage

```rust
use dialog_capability::Subject;
use dialog_effects::storage::Location;
use dialog_peer::Peer;
use dialog_repository::RepositoryExt;
use dialog_storage::Storage;

// Target-appropriate default storage: filesystem on native, IndexedDB on web.
let storage = Storage::default();

// Open (load-or-create) the peer at its location.
let alice = Peer::new()
    .storage(storage)
    .open(Location::profile("alice"))
    .await?;

// A session scoped to this application.
let session = alice
    .derive(b"my-app").await?
    .allow(Subject::any())
    .build()
    .await?;

// Open or create a repository the peer holds.
let contacts = alice
    .space("contacts")
    .open()
    .perform(&session)
    .await?;

// Work with branches.
let main = contacts
    .branch("main")
    .open()
    .perform(&session)
    .await?;

// Define a concept with typed attributes.
#[derive(Concept)]
struct Employee {
    this: Entity,
    name: employee::Name,
    role: employee::Role,
}

// Commit data.
main.transaction()
    .assert(Employee {
        this: Entity::new()?,
        name: employee::Name("Alice".into()),
        role: employee::Role("Engineer".into()),
    })
    .commit()
    .perform(&session)
    .await?;

// Query.
let results: Vec<Employee> = main
    .query()
    .select(Query::<Employee> {
        this: Term::var("this"),
        name: Term::var("name"),
        role: Term::var("role"),
    })
    .perform(&session)
    .try_vec()
    .await?;

// Add a remote and sync. `.create(...)` takes `impl Into<SiteAddress>`,
// so concrete variants like UcanAddress or S3 Address can be passed
// directly — here we point at a UCAN-gated access service in front of
// an S3 bucket.
use dialog_remote_ucan_s3::UcanAddress;

let origin = contacts
    .remote("origin")
    .create(UcanAddress::new("https://access.example.com"))
    .perform(&session)
    .await?;

let upstream = origin
    .branch("main")
    .open()
    .perform(&session)
    .await?;

main
    .set_upstream(upstream)
    .perform(&session)
    .await?;

main.push().perform(&session).await?;
main.pull().perform(&session).await?;
```
