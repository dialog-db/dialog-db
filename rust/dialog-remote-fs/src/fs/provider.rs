//! Provider implementations for the [`Fs`](super::Fs) site.
//!
//! By the time these run, [`authorize`](crate::fs::FsFork) has opened and
//! verified the directory and attested the resolved
//! [`FileSystem`](dialog_storage::provider::FileSystem) into the invocation. So
//! each [`Provider<ForkInvocation<Fs, Fx>>`](dialog_capability::Provider) impl
//! just delegates the capability to that provider. All filesystem I/O — layout,
//! atomic writes, CAS locking — lives in `dialog_storage`'s isomorphic provider.

pub mod archive;
pub mod memory;
