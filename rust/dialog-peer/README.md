# dialog-peer

Peers: the runtime capability environment for Dialog.

A **Peer** is one acting key over a storage, a network, and the branch of a
repository that holds the peer's own state, its **home**. A root peer acts
with the key its home is named by and proves from that repository's branch.
A **worker** is a peer built over a derived (or supplied) key with grants
from another peer, sharing that peer's storage and, usually, its home. Both
are the one `Peer` type; the difference is what they hold, not what they
are. See `notes/peer-and-session.md`.

## Usage

```rust,no_run
# use dialog_capability::Subject;
# use dialog_effects::storage::Location;
# use dialog_identity::{ClaimExt as _, OpenCredential};
# use dialog_peer::Peer;
# use dialog_repository::RepositoryExt as _;
# use dialog_storage::provider::storage::{Storage, VolatileSpace};
# use dialog_varsig::Principal as _;
# async fn example() -> anyhow::Result<()> {
// The credential is opened apart from the peer; here from the storage.
let storage = Storage::<VolatileSpace>::volatile();
let credential = OpenCredential::open("alice").perform(&storage).await?;

// The peer: its home is the repository the credential names.
let alice = Peer::open(credential.did())
    .credential(credential)
    .storage(storage)
    .branch("main")
    .await?;

// A worker: a derived key, allowed what the peer claims for it.
let job = alice
    .worker(b"my-app")
    .allow(Subject::any().claim(alice.credential()))
    .await?;

// Open a repository the peer holds, through the worker.
let contacts = alice.space("contacts").open().perform(&job).await?;
# let _ = contacts;
# Ok(())
# }
```

A worker needs no handle on its parent, only its credential and storage:

```rust,no_run
# use dialog_capability::Subject;
# use dialog_identity::ClaimExt as _;
# use dialog_peer::Peer;
# use dialog_varsig::Principal as _;
# async fn example(
#     credential: dialog_credentials::SignerCredential,
#     storage: dialog_storage::provider::storage::Storage<dialog_storage::provider::storage::VolatileSpace>,
# ) -> anyhow::Result<()> {
let worker = Peer::open(credential.did())
    .credential(credential.derive(b"worker").await?)
    .storage(storage)
    .ephemeral()
    .allow(Subject::any().claim(&credential))
    .await?;
# let _ = worker;
# Ok(())
# }
```

`grant` takes a claim with an expiration; `allow` is the deliberate
unbounded form. `ephemeral` drops the state branch, so the worker proves
from its grants alone and retains nothing.
