//! Network provider that routes remote invocations to connections by address.
//!
//! This module uses [`#[derive(Router)]`](dialog_capability::Router) to compose
//! per-address-type routes into a single `Network` provider. Each route field
//! handles its own address type and connection caching.
//!
//! # Targeting multiple remotes
//!
//! The same capability can target different backends by pairing it with
//! different address types. `Network` routes `s3::Credentials` to direct
//! S3 connections and `ucan::Credentials` to UCAN-delegated connections:
//!
//! ```ignore
//! use dialog_effects::remote::RemoteInvocation;
//! use dialog_effects::archive::{Archive, Catalog, Get};
//! use dialog_s3_credentials::{Address, s3};
//! use dialog_storage::provider::Network;
//!
//! let network = Network::new();
//!
//! let cap = Subject::from(did)
//!     .attenuate(Archive)
//!     .attenuate(Catalog::new("index"))
//!     .invoke(Get::new(digest));
//!
//! // Route to a direct S3 backend
//! let s3_addr = s3::Credentials::public(
//!     Address::new("https://s3.amazonaws.com", "us-east-1", "my-bucket"),
//! )
//! .unwrap();
//! RemoteInvocation::new(cap.clone(), s3_addr)
//!     .perform(&network)
//!     .await;
//! ```
use dialog_capability::Router;

pub mod emulator;
pub mod route;
#[cfg(feature = "s3")]
pub mod s3;
#[cfg(feature = "ucan")]
pub mod ucan;

/// Production network provider that routes remote invocations to the
/// appropriate backend based on address type.
///
/// Each field is a route that handles a specific address type. The
/// `#[derive(Router)]` macro generates `Provider<RemoteInvocation<Fx, Addr>>`
/// implementations that forward to the matching field.
#[derive(Router)]
pub struct Network {
    #[cfg(feature = "s3")]
    s3: route::Route<s3::Credentials, s3::Connection>,
    #[cfg(feature = "ucan")]
    ucan: route::Route<ucan::Credentials, ucan::Connection>,
    #[cfg(not(any(feature = "s3", feature = "ucan")))]
    #[route(skip)]
    _marker: (),
}

impl Network {
    /// Create a new network provider.
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "s3")]
            s3: route::Route::new(),
            #[cfg(feature = "ucan")]
            ucan: route::Route::new(),
            #[cfg(not(any(feature = "s3", feature = "ucan")))]
            _marker: (),
        }
    }
}

impl Default for Network {
    fn default() -> Self {
        Self::new()
    }
}
