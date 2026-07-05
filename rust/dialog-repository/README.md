# dialog-repository

A git-like interface for Dialog-DB.

Provides repositories with branches, remotes, push/pull, and merge, but for structured data instead of files. Each repository has its own identity (keypair), named branches with revision history, and remotes for replication. Information is stored as claims: `{ the, of, is, cause }` facts where `the` is the relation, `of` is the entity, `is` is the value, and `cause` is the provenance. Claims can be queried with typed concepts or deductive rules. Same name under the same profile always yields the same repository identity.

## Usage

```rust
use dialog_capability::Subject;
use dialog_operator::Profile;
use dialog_repository::RepositoryExt;
use dialog_storage::Storage;

// Target-appropriate default storage: filesystem on native, IndexedDB on web.
let storage = Storage::default();

// Open (load-or-create) the profile.
let profile = Profile::open("alice").perform(&storage).await?;

// Derive an operator scoped to this application.
let operator = profile
    .derive(b"my-app")
    .allow(Subject::any())
    .build(storage)
    .await?;

// Open or create a repository.
let contacts = profile
    .repository("contacts")
    .open()
    .perform(&operator)
    .await?;

// Work with branches.
let main = contacts
    .branch("main")
    .open()
    .perform(&operator)
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
    .perform(&operator)
    .await?;

// Query.
let results: Vec<Employee> = main
    .query()
    .select(Query::<Employee> {
        this: Term::var("this"),
        name: Term::var("name"),
        role: Term::var("role"),
    })
    .perform(&operator)
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
    .perform(&operator)
    .await?;

let upstream = origin
    .branch("main")
    .open()
    .perform(&operator)
    .await?;

main
    .set_upstream(upstream)
    .perform(&operator)
    .await?;

main.push().perform(&operator).await?;
main.pull().perform(&operator).await?;
```
