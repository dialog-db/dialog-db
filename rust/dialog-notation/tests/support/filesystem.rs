//! A session on a peer kept on the filesystem, the native provider a
//! peer stores its repositories with on a machine of its own.

use std::path::Path;

use dialog_capability::Subject;
use dialog_effects::storage::{Directory, Location};
use dialog_peer::helpers::{onboard, test_grant, test_state, test_system, unique_name};
use dialog_peer::{Peer, Session};
use dialog_repository::{Branch, RepositoryExt as _};
use dialog_storage::provider::storage::{CredentialStore, NativeSpace, Storage};
use dialog_varsig::Principal as _;

/// A session on a fresh peer kept under `root`, and the `main` branch
/// of a fresh repository of it.
pub async fn branch(root: &Path) -> anyhow::Result<(Peer<NativeSpace, Session>, Branch)> {
    let base = Directory::At(root.to_string_lossy().into_owned());
    let name = unique_name("peer");
    let storage = Storage::<NativeSpace>::default().owned_by(test_system().await.did());
    let credential = dialog_peer::OpenCredential::open(name.clone())
        .at(base.clone())
        .perform(&CredentialStore::<NativeSpace>::default())
        .await?;
    let peer = Peer::new(credential.clone())
        .at(Location::new(base.clone(), name))
        .with(storage)
        .space(test_state(&credential.did()))
        .grant(test_grant().await)
        .base(base)
        .await?;
    onboard(&peer).await?;
    let session = peer
        .session(b"test")
        .space(peer.state())
        .allow(Subject::any())
        .await?;
    let repo = peer
        .space(unique_name("repo"))
        .open()
        .perform(&session)
        .await?;
    let branch = repo.branch("main").open().perform(&session).await?;
    Ok((session, branch))
}
