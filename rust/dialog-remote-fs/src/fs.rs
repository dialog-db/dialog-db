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

pub use address::FsAddress;
pub use authorization::FsAuthorization;

use dialog_capability::Effect;
use dialog_capability::Fork;
use dialog_capability::Site;

/// Local-filesystem-backed site.
///
/// Marker for fork dispatch — the actual I/O is performed by `dialog_storage`'s
/// [`FileSystem`](dialog_storage::provider::FileSystem) provider, to which the
/// [`Provider`](dialog_capability::Provider) impls in [`provider`] delegate.
/// The site is host-trusted: there is no on-the-wire authorization step. The
/// directory referenced by an [`FsAddress`] must be registered with the
/// provider (via [`crate::register_directory`]) before any invocation fires.
#[derive(Debug, Clone, Copy, Default)]
pub struct Fs;

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
