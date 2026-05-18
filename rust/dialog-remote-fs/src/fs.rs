//! Fs site type and Provider plumbing.
//!
//! Mirrors the shape of [`dialog_remote_s3::s3`]. The site marker is [`Fs`];
//! site-bound fork is [`FsFork<Fx>`]. The actual I/O lives in [`provider`],
//! and capability-to-request translation lives in
//! [`crate::request`](crate::request).

mod address;
mod authorization;
mod invocation;
mod permit;
pub mod provider;

pub use address::FsAddress;
pub use authorization::FsAuthorization;
pub use invocation::FsInvocation;
pub use permit::FsPermit;

use dialog_capability::Effect;
use dialog_capability::Fork;
use dialog_capability::Site;

/// Local-filesystem-backed site.
///
/// Marker for fork dispatch — actual I/O is performed by
/// [`Provider`](dialog_capability::Provider) impls in [`provider`]. The
/// site is host-trusted: there is no on-the-wire authorization step. The
/// directory handle referenced by an [`FsAddress`] must be registered with
/// the provider before any invocation fires.
#[derive(Debug, Clone, Copy, Default)]
pub struct Fs;

/// Site-owned fork wrapper for [`Fs`].
///
/// Thin newtype around [`Fork<Fs, Fx>`] that carries the site-specific
/// [`Authorize`](dialog_capability::SiteFork) impl. For FS there are no
/// credentials to fetch — the fork captures the request shape and seals
/// it into an [`FsAuthorization`].
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

    #[dialog_common::test]
    fn it_builds_an_address() {
        let address = FsAddress::new("did:key:zAbc");
        assert_eq!(address.id(), "did:key:zAbc");
    }

    #[dialog_common::test]
    fn it_roundtrips_address_through_serde() {
        let address = FsAddress::new("did:key:zAbc");
        let json = serde_json::to_string(&address).unwrap();
        let parsed: FsAddress = serde_json::from_str(&json).unwrap();
        assert_eq!(address, parsed);
    }
}
