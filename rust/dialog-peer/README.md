# dialog-peer

Peers: the runtime capability environment for Dialog.

A **Peer** represents the owner of the replicas it opens and commits to,
over a storage and a network, with its own state in the branch it is
given, whose repository is its home. A handle's mode records which key it
acts with:

- `Peer<S, Local>`, made by `Peer::new(credential)`, acts with the peer's
  own key. It can do anything the peer can, including granting authority
  and opening sessions.
- `Peer<S, Session>`, made by `peer.session(context)`, `Peer::operator(key)`
  or by giving the builder an `operator`, is the same peer acting with a separate key,
  within what the peer granted it. It is never handed a key: not the
  peer's, and not that of a repository it loads. Every session commits
  under its own origin, so sessions of one peer never collide.

What a session does at other peers is proven from its grants. Its reads
and writes of the storage it shares with its peer are not yet: a session
is a separate signer, not yet a sandbox.

See `notes/peer-and-session.md`.

## Usage

```rust,no_run
# use dialog_capability::Subject;
# use dialog_credentials::{Ed25519Signer, SignerCredential};
# use dialog_identity::OpenCredential;
# use dialog_peer::{Allowance, Peer};
# use dialog_repository::{Repository, RepositoryExt as _};
# use dialog_storage::provider::storage::{Storage, VolatileSpace};
# use dialog_varsig::Principal as _;
# async fn example() -> anyhow::Result<()> {
// The storage belongs to a system: opening spaces in it takes the
// system's grant.
let system = SignerCredential::from(Ed25519Signer::generate().await?);
let storage = Storage::<VolatileSpace>::volatile().owned_by(system.did());

// The credential is opened apart from the peer; here from the storage.
let credential = OpenCredential::open("alice").perform(&storage).await?;

// The peer, acting with its own key, keeping its state in the main
// branch of its own repository, and granted the storage.
let alice = Peer::new(credential.clone())
    .mount(Repository::from(credential.did()).branch("main"))
    .with(storage)
    .grant(Allowance::storage(&system))
    .build()
    .await?;

// A session: a derived key, allowed what the peer grants it, keeping its
// state in the peer's branch.
let job = alice
    .session(b"my-app")
    .mount(alice.state())
    .allow(Subject::any())
    .await?;

// Open a repository the peer holds, through the session.
let contacts = alice.space("contacts").open().perform(&job).await?;
# let _ = contacts;
# Ok(())
# }
```

A session needs no open handle on its peer, only the peer's credential,
the storage and the peer's state, holding a delegation of the storage:

```rust,no_run
# use dialog_capability::Subject;
# use dialog_peer::Peer;
# async fn example(
#     credential: dialog_credentials::SignerCredential,
#     storage: dialog_storage::provider::storage::Storage<dialog_storage::provider::storage::VolatileSpace>,
#     state: dialog_repository::BranchReference,
# ) -> anyhow::Result<()> {
let session = Peer::new(credential)
    .session(b"worker")
    .with(storage)
    .mount(state)
    .allow(Subject::any())
    .await?;
# let _ = session;
# Ok(())
# }
```

`grant` takes a claim with an expiration; `allow` is the deliberate
unbounded form. A session of a peer holds the peer's grant of the storage
in memory, as the peer does. `Peer::operator(key)` builds a session when
the peer's key is not at hand, from certificates the peer issued; it
proves the storage through a delegation asserted in the state it is
given.
