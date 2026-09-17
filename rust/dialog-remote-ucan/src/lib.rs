#![warn(missing_docs)]

//! A UCAN remote that proves and performs each operation in one request.
//!
//! The access service already receives a signed invocation container for
//! every remote effect. Where [`dialog_remote_ucan_s3`] redeems that
//! container for a permit and then makes a second request against the
//! object it names, this site asks the service to carry the operation
//! out in the same request: a read answers with the object's bytes, and
//! a write ships the bytes it stores inside the container, under the
//! [`payload`](dialog_ucan_core::Container::payload) key beside the
//! tokens.
//!
//! The request is the invocation the service always received, so a
//! service that does not perform operations directly notices nothing: it
//! reads the tokens, ignores the payload, and answers with a permit as it
//! always has. The site tells the two answers apart by content type and
//! completes a permit the old way, so the cost against such a service is
//! exactly what it was, one redeem and one object request, never a third.
//!
//! Addresses, authorization material and the signed invocations are the
//! ones [`dialog_remote_ucan_s3`] mints; only the exchange with the service
//! differs. Blob streams still go through permits: a blob is read and
//! written in ranges over a URL, which is what a permit is for.

mod address;
mod direct;
#[cfg(any(test, feature = "helpers"))]
pub mod helpers;
mod provider;
pub mod server;
mod site;

pub use address::UcanAddress;
pub use dialog_remote_ucan_s3::{Ucan, UcanAuthorization, UcanInvocation};
pub use direct::{ACCEPT, OBJECT_MEDIA_TYPE, PERMIT_MEDIA_TYPE};
pub use server::{Access, Answer, Refusal, Request, Response, Store, Verified};
pub use site::{UcanFork, UcanSite};
