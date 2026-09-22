# dialog-peer

Peers and sessions: the runtime capability environment for Dialog.

A **Peer** is a site identified by a key that holds replicas of
repositories: a persisted signer, the storage its spaces are mounted in,
the network dispatch, and the branch of its own repository that serves as
registry and default proof source. A **Session** is a constrained peer:
one acting key (derived from the peer's, or supplied) and the grants it was
built with. The session is what every `perform` takes; the peer itself is
the unconstrained environment. See `notes/peer-and-session.md`.

## Usage

```rust,no_run
# use dialog_capability::Subject;
# use dialog_effects::storage::Location;
# use dialog_peer::Peer;
# use dialog_repository::RepositoryExt as _;
# use dialog_storage::provider::storage::{Storage, VolatileSpace};
# async fn example() -> anyhow::Result<()> {
// Open or create the peer at a location, over some storage.
let alice = Peer::new()
    .storage(Storage::<VolatileSpace>::volatile())
    .branch("main")
    .open(Location::profile("alice"))
    .await?;

// A session narrows the peer to one key and the scopes it may act on.
let job = alice
    .session(b"my-app")
    .allow(Subject::any())
    .build()
    .await?;

// Open a repository the peer holds, through the session.
let contacts = alice.space("contacts").open().perform(&job).await?;
# let _ = contacts;
# Ok(())
# }
```
