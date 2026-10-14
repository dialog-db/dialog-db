//! Fs site type and Provider plumbing.
//!
//! Mirrors the shape of [`dialog_remote_s3::s3`]. The site marker is [`Fs`];
//! the site-bound fork is [`FsFork<Fx>`]. There is no on-the-wire
//! authorization for a local directory, so this crate is a thin credential
//! resolution wrapper: [`provider`] resolves the [`FsAddress`] to a registered
//! directory and delegates the capability to `dialog_storage`'s isomorphic
//! [`FileSystem`](dialog_storage::provider::FileSystem) provider.

mod address;
mod authorization;
pub mod provider;
pub mod simulation;

pub use address::FsAddress;
pub use authorization::FsAuthorization;

use std::sync::Arc;

use dialog_capability::Effect;
use dialog_capability::Fork;
use dialog_capability::Site;
use dialog_common::Flight;

/// In-flight block GETs, joined by `(vault URL, digest)`.
///
/// A block is immutable content, so every caller reading the same digest
/// from the same vault gets the same bytes — the one read that is always
/// safe to share. The vault URL scopes the join: two vaults can disagree
/// about *holding* a block (a `None` from one must never answer the
/// other), so joins never cross vaults even for equal digests.
///
/// Errors are shared as their rendering (`ArchiveError::Storage`): the
/// site is host-trusted, so a `Get` has no authorization decision to
/// preserve. Mutable reads (memory cells) deliberately do not come
/// through here.
pub(crate) type BlockGets = Flight<String, Result<Option<Arc<Vec<u8>>>, String>>;

/// Local-filesystem-backed site.
///
/// Dispatches forks — the actual I/O is performed by `dialog_storage`'s
/// [`FileSystem`](dialog_storage::provider::FileSystem) provider, to which the
/// [`Provider`](dialog_capability::Provider) impls in [`provider`] delegate.
/// The site is host-trusted: there is no on-the-wire authorization step. The
/// directory referenced by an [`FsAddress`] must be registered with the
/// provider (via [`crate::register_directory`]) before any invocation fires.
///
/// The site owns its in-flight block GETs, so readers join one another's
/// requests only through the same site (one `Network`, hence one
/// environment), and nothing outlives it. Clones share them.
#[derive(Debug, Clone, Default)]
pub struct Fs {
    gets: Arc<BlockGets>,
}

impl Fs {
    /// The in-flight block GETs shared by clones of this site.
    pub(crate) fn gets(&self) -> &BlockGets {
        &self.gets
    }
}

/// Site-owned fork wrapper for [`Fs`].
///
/// Thin newtype around [`Fork<Fs, Fx>`] that carries the site-specific
/// [`SiteFork`](dialog_capability::SiteFork) impl. For FS there are no
/// credentials to fetch — authorization is a unit marker.
pub struct FsFork<Fx: Effect>(Fork<Fs, Fx>);

impl<Fx: Effect> From<Fork<Fs, Fx>> for FsFork<Fx> {
    fn from(fork: Fork<Fs, Fx>) -> Self {
        Self(fork)
    }
}

impl Site for Fs {
    type Authorization = FsAuthorization;
    type Address = FsAddress;
    type Fork<Fx: Effect> = FsFork<Fx>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_effects::storage::{Directory, Location};

    #[dialog_common::test]
    fn it_builds_an_address() {
        let location = Location::temp("vault");
        let address = FsAddress::new(location.clone());
        assert_eq!(address.location(), &location);
    }

    #[dialog_common::test]
    fn it_roundtrips_address_through_serde() {
        let address = FsAddress::new(Location::new(Directory::At("/vault".into()), "space"));
        let json = serde_json::to_string(&address).unwrap();
        let parsed: FsAddress = serde_json::from_str(&json).unwrap();
        assert_eq!(address, parsed);
    }
}
