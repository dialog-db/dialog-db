//! Core UCAN functionality.

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod builder;
pub mod cid;
pub mod codec;
pub mod collection;
pub mod command;
pub mod container;
pub mod crypto;
pub mod delegation;
pub mod envelope;
pub mod future;
pub mod invocation;
pub mod issuer;
pub mod number;
pub mod principal;
pub mod promise;
pub mod revocation;
// pub mod receipt; TODO Reenable after first release
pub mod subject;
pub mod sync;
pub mod task;
pub mod time;
pub mod unset;
pub mod verification;

#[cfg(any(test, feature = "helpers"))]
pub mod helpers;

// Internal modules
mod ipld;
mod sealed;

pub use container::delegation::DelegationChain;
pub use container::invocation::InvocationChain;
pub use container::revocation::{
    Denial, MalformedRevocationChain, RevocationChain, RevocationError,
};
pub use container::{Container, ContainerError};
pub use delegation::{
    Delegation,
    builder::{BuildError as DelegationBuildError, DelegationBuilder},
};
pub use invocation::{
    CheckError, CheckFailed, Invalid, Invocation, InvocationPayload, Unavailable, VerifyError,
    builder::{BuildError as InvocationBuildError, InvocationBuilder},
};
pub use revocation::action::{MalformedRevocation, Revocation};
pub use revocation::builder::RevocationBuilder;
pub use revocation::{
    RevocationChecker, RevocationMatch, RevocationSelector, TolerateUnavailability,
    UnverifiedRevocations,
};
pub use verification::{Environment, Verifiable, VerificationContext};
