# dialog-remote-s3

S3-compatible remote for Dialog-DB.

Provides direct S3 access with SigV4 request signing. Used for pushing and pulling repository data (archive blocks, memory cells) to S3 buckets.

## Usage

```rust
use dialog_remote_s3::{Address, S3Credentials};

// Configure S3 address with credentials from environment
let address = Address::new(
    env!("S3_ENDPOINT"),
    env!("S3_REGION"),
    env!("S3_BUCKET"),
).with_credentials(S3Credentials::new(
    env!("AWS_ACCESS_KEY_ID"),
    env!("AWS_SECRET_ACCESS_KEY"),
));

// Add as a remote on a repository
let origin = repo.remote("origin")
    .create(address)
    .perform(&operator)
    .await?;

// Set upstream and sync
let main = repo
    .branch("main")
    .open()
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

main
    .push()
    .perform(&operator)
    .await?;

main
    .pull()
    .perform(&operator)
    .await?;
```
