# dialog-remote-ucan-s3

UCAN-authorized remote for Dialog-DB.

Wraps S3 storage with UCAN (User Controlled Authorization Networks) for delegated access control. Instead of direct S3 credentials, requests are authorized through a UCAN access service that verifies delegation chains.

## Usage

```rust
use dialog_remote_ucan_s3::UcanAddress;

// Add a UCAN remote to a repository
let origin = repo.remote("origin")
    .create(UcanAddress::new("https://access.example.com"))
    .perform(&operator)
    .await?;

// Set upstream and sync
let main = repo.branch("main").open().perform(&operator).await?;
let remote_main = origin.branch("main").open().perform(&operator).await?;
main.set_upstream(remote_main).perform(&operator).await?;

main.push().perform(&operator).await?;
main.pull().perform(&operator).await?;
```

## Collaboration

Access is shared through UCAN delegation chains:

```rust
// Alice delegates repo access to Bob
let chain = alice_profile.access()
    .claim(&repo)
    .delegate(bob_profile.did())
    .perform(&alice_operator)
    .await?;

// Bob retains the delegation
bob_profile.access()
    .save(chain)
    .perform(&bob_operator)
    .await?;

// Bob can now push/pull through the same UCAN remote
let origin = bob_repo.remote("origin")
    .create(UcanAddress::new("https://access.example.com"))
    .subject(alice_repo.did())
    .perform(&bob_operator)
    .await?;
```
