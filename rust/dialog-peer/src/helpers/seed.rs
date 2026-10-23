//! One number a measurement derives every identity of its run from.
//!
//! A run mints identities: the key the profile peer opens with, the keys of
//! the spaces it creates, the names it gives them, and the fresh identifier
//! a branch's first commit allocates for its line. The keys and the line
//! reach the tree (revisions name their issuer and authority, and every
//! datum is tagged with a version of the line), so runs that mint them at
//! random write differently shaped trees. A [`Seed`] derives them all from
//! one number, so the same seed writes the same tree, byte for byte.
//!
//! Each key is derived from the seed and a label, not drawn from a stream,
//! so a change elsewhere that draws one more random value does not move
//! it. A line's identifier, which the harness cannot name, comes from the
//! [seeded identifiers](Seed::entropy) of the drawing thread. Nothing that
//! generates a key reads that stream: a key the harness does not derive
//! here stays random.
//!
//! ```no_run
//! # async fn example() -> anyhow::Result<()> {
//! use dialog_peer::helpers::Seed;
//! use dialog_repository::RepositoryExt as _;
//!
//! let seed = Seed::new(7);
//! let _entropy = seed.entropy();
//! let (operator, profile) = seed.session_with_peer().await;
//! let space = profile
//!     .space(seed.name("space"))
//!     .create()
//!     .with_credential(seed.signer("space").await?)
//!     .perform(&operator)
//!     .await?;
//! # let _ = space;
//! # Ok(())
//! # }
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

use dialog_common::helpers::entropy;
use dialog_credentials::key::{ExtractableKey as _, KeyExport};
use dialog_credentials::{Ed25519Signer, Extractable, SignerCredential};
use dialog_effects::storage::Location;
use dialog_storage::provider::storage::VolatileSpace;

use super::{open_peer_as, test_storage};
use crate::{Peer, Session};

/// The context a seed's keys are derived under.
const CONTEXT: &str = "dialog-peer helpers seed v1";

/// One number every identity of a run derives from. See the
/// [module](self).
#[derive(Debug)]
pub struct Seed {
    value: u64,
    names: AtomicU64,
}

impl Seed {
    /// The seed `value`.
    pub fn new(value: u64) -> Self {
        Self {
            value,
            names: AtomicU64::new(0),
        }
    }

    /// The number this seed is.
    pub fn value(&self) -> u64 {
        self.value
    }

    /// Draw the current thread's identifiers from this seed while the
    /// guard lives: a line's identifier, which the run cannot derive by
    /// label, then replays too. Keys are never drawn from it.
    pub fn entropy(&self) -> entropy::Seeded {
        entropy::seed(self.value)
    }

    /// The 32 bytes this seed derives for `label`.
    pub fn bytes(&self, label: &str) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new_derive_key(CONTEXT);
        hasher.update(&self.value.to_le_bytes());
        hasher.update(label.as_bytes());
        *hasher.finalize().as_bytes()
    }

    /// The key this seed derives for `label`: the same key for the same
    /// seed and label, in every run.
    pub async fn signer(&self, label: &str) -> anyhow::Result<Ed25519Signer<Extractable>> {
        let seed = KeyExport::Extractable(self.bytes(label).to_vec());
        Ok(Ed25519Signer::<Extractable>::import(seed).await?)
    }

    /// [`signer`](Self::signer) as a credential.
    pub async fn credential(&self, label: &str) -> anyhow::Result<SignerCredential> {
        let signer = Ed25519Signer::import(&self.bytes(label)).await?;
        Ok(SignerCredential::from(signer))
    }

    /// A name under `prefix`, unique among the names this seed gives: the
    /// seed's counterpart of [`unique_name`](super::unique_name), the same
    /// sequence of names in every run that asks in the same order.
    pub fn name(&self, prefix: &str) -> String {
        let sequence = self.names.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{:016x}-{sequence}", self.value)
    }

    /// A session with a powerline grant on a volatile peer whose key is
    /// this seed's `profile` key: the seed's counterpart of
    /// [`test_session_with_peer`](super::test_session_with_peer).
    pub async fn session_with_peer(&self) -> (Peer<VolatileSpace, Session>, Peer<VolatileSpace>) {
        let credential = self
            .credential("profile")
            .await
            .expect("Seed::session_with_peer: a derived key imports");
        let peer = open_peer_as(
            test_storage().await,
            Location::profile(self.name("profile")),
            credential,
        )
        .await
        .expect("Seed::session_with_peer: failed to open peer");
        let worker = peer
            .session(b"test")
            .space(peer.state())
            .allow(dialog_capability::Subject::any())
            .await
            .expect("Seed::session_with_peer: failed to build worker");
        (worker, peer)
    }
}

#[cfg(test)]
mod tests {
    use super::Seed;
    use dialog_artifacts::Entity;
    use dialog_credentials::key::ExtractableKey as _;
    use dialog_credentials::{Ed25519Signer, Extractable};
    use dialog_varsig::Principal as _;

    #[dialog_common::test]
    async fn it_derives_the_same_keys_and_names_from_the_same_seed() -> anyhow::Result<()> {
        let (first, second) = (Seed::new(7), Seed::new(7));
        assert_eq!(
            first.signer("space").await?.did(),
            second.signer("space").await?.did()
        );
        assert_eq!(
            first.credential("profile").await?.did(),
            second.credential("profile").await?.did()
        );
        assert_eq!(first.name("client"), second.name("client"));
        assert_eq!(first.name("client"), second.name("client"));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_derives_distinct_keys_and_names_per_label_and_seed() -> anyhow::Result<()> {
        let seed = Seed::new(7);
        assert_ne!(
            seed.signer("space").await?.did(),
            seed.signer("profile").await?.did()
        );
        assert_ne!(
            seed.signer("space").await?.did(),
            Seed::new(8).signer("space").await?.did()
        );
        assert_ne!(seed.name("client"), seed.name("client"));
        Ok(())
    }

    /// A seed's entropy replays the identifier a line's first commit
    /// allocates.
    #[dialog_common::test]
    fn it_replays_line_identifiers_under_its_entropy() -> anyhow::Result<()> {
        let draw = || {
            let _entropy = Seed::new(7).entropy();
            anyhow::Ok((Entity::new()?, Entity::new()?))
        };
        let (first, second) = (draw()?, draw()?);
        assert_eq!(first, second);
        assert_ne!(first.0, first.1);
        Ok(())
    }

    /// A seed's entropy reaches identifiers only: under it, line
    /// identifiers replay while the key generator stays fresh on every
    /// call, as it is in production.
    #[dialog_common::test]
    async fn it_keeps_generated_keys_fresh_under_its_entropy() -> anyhow::Result<()> {
        let draw = || async {
            let _entropy = Seed::new(7).entropy();
            let key = <Ed25519Signer<Extractable>>::generate().await?.did();
            anyhow::Ok((key, Entity::new()?))
        };
        let (first, second) = (draw().await?, draw().await?);
        assert_ne!(first.0, second.0, "keys stay fresh");
        assert_eq!(first.1, second.1, "line identifiers replay");
        Ok(())
    }

    /// Without a seed's entropy on the thread, the key generator and the
    /// line identifier draw from the system: helpers compiled in seed
    /// nothing by themselves.
    #[dialog_common::test]
    async fn it_leaves_production_draws_random_without_its_entropy() -> anyhow::Result<()> {
        let _seed = Seed::new(7);
        let first = <Ed25519Signer<Extractable>>::generate().await?.did();
        let second = <Ed25519Signer<Extractable>>::generate().await?.did();
        assert_ne!(first, second);
        assert_ne!(Entity::new()?, Entity::new()?);
        Ok(())
    }
}
