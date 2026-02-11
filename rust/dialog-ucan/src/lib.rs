//! Core UCAN functionality.

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod builder;
pub mod cid;
pub mod codec;
pub mod collection;
pub mod command;
pub mod crypto;
pub mod delegation;
pub mod envelope;
pub mod future;
pub mod invocation;
pub mod issuer;
pub mod number;
pub mod principal;
pub mod promise;
// pub mod receipt; TODO Reenable after first release
pub mod subject;
pub mod task;
pub mod time;
pub mod unset;

// Internal modules
mod ipld;
mod sealed;

pub use delegation::{
    Delegation,
    builder::{BuildError as DelegationBuildError, DelegationBuilder},
};
pub use invocation::{
    CheckFailed, Invocation, InvocationPayload,
    builder::{BuildError as InvocationBuildError, InvocationBuilder},
};
