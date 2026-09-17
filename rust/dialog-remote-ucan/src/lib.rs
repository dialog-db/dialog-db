#![warn(missing_docs)]

//! A UCAN remote that proves and performs each operation in one request.
//!
//! The access service already receives a signed invocation container for
//! every remote effect. Where [`dialog_remote_ucan_s3`] redeems that
//! container for a permit and then makes a second request against the
//! object it names, this site asks the service to carry the operation
//! out in the same request. The invocation rides in the `Authorization`
//! header, in the container spec's text serialization, and the body is
//! the operation's bytes alone: what a write stores on the way in, what
//! a read asked for on the way out. Nothing frames the bytes, so either
//! side can stream them.
//!
//! `Accept` names what the site takes back: the outcome first, a permit
//! second. A service that does not perform operations verifies the
//! invocation and answers with a permit, which the site completes the
//! way the permit flow always has, at the cost that flow always had. A
//! service that does not read the invocation from the header at all
//! answers the request as one it cannot read, and the site goes through
//! the permit flow from the start, one request the poorer. Addresses,
//! authorization material and the signed invocations are the ones
//! [`dialog_remote_ucan_s3`] mints; only the exchange with the service
//! differs.
//!
//! The server side is [`Access`]: an embedder brings a provider of the
//! effects, the layer decodes and verifies each invocation and performs
//! it with that provider.

mod address;
mod direct;
#[cfg(any(test, feature = "helpers"))]
pub mod helpers;
mod provider;
pub mod server;
mod site;
#[cfg(test)]
mod test;

pub use address::UcanAddress;
pub use dialog_remote_ucan_s3::{Ucan, UcanAuthorization, UcanInvocation};
pub use direct::{
    ACCEPT, OBJECT_MEDIA_TYPE, PERMIT_MEDIA_TYPE, SCHEME, credential, credential_container,
    is_credential,
};
pub use server::{Access, Answer, Content, Payload, Refusal, Request, Response, Store, Verified};
pub use site::{UcanFork, UcanSite};
