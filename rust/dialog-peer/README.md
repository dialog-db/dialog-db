# dialog-peer

Peers: the runtime capability environment for Dialog.

A **Peer** represents the owner of the replicas it opens and commits to,
over a storage and a network, with its own state in a branch of the
repository its key names. A handle's mode records which key it acts with:

- `Peer<S, Local>`, made by `Peer::new(credential)`, acts with the peer's
  own key. It can do anything the peer can, including granting authority
  and opening sessions.
- `Peer<S, Session>`, made by `peer.session(context)` or by giving the
  builder an `operator`, is the same peer acting with a separate key,
  within what the peer granted it. It keeps no copy of the peer's key, and
  every session commits under its own origin, so sessions of one peer
  never collide.

See `notes/peer-and-session.md`.

## Usage

```rust,no_run
# use dialog_capability::Subject;
# use dialog_identity::OpenCredential;
# use dialog_peer::Peer;
# use dialog_repository::RepositoryExt as _;
# use dialog_storage::provider::storage::{Storage, VolatileSpace};
# async fn example() -> anyhow::Result<()> {
// The credential is opened apart from the peer; here from the storage.
let storage = Storage::<VolatileSpace>::volatile();
let credential = OpenCredential::open("alice").perform(&storage).await?;

// The peer, acting with its own key.
let alice = Peer::new(credential).storage(storage).await?;

// A session: a derived key, allowed what the peer grants it.
let job = alice.session(b"my-app").allow(Subject::any()).await?;

// Open a repository the peer holds, through the session.
let contacts = alice.space("contacts").open().perform(&job).await?;
# let _ = contacts;
# Ok(())
# }
```

A session needs no open handle on its peer, only the peer's credential and
the storage:

```rust,no_run
# use dialog_capability::Subject;
# use dialog_peer::Peer;
# async fn example(
#     credential: dialog_credentials::SignerCredential,
#     storage: dialog_storage::provider::storage::Storage<dialog_storage::provider::storage::VolatileSpace>,
# ) -> anyhow::Result<()> {
let session = Peer::new(credential)
    .session(b"worker")
    .storage(storage)
    .ephemeral()
    .allow(Subject::any())
    .await?;
# let _ = session;
# Ok(())
# }
```

`grant` takes a claim with an expiration; `allow` is the deliberate
unbounded form. `ephemeral` drops the state branch, so the session proves
from its grants alone and retains nothing. `Peer::session_of(peer)` builds
a session when only the peer's DID is known, from an `operator` key and
certificates the peer issued.
