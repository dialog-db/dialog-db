//! Provider implementations for the [`Fs`](super::Fs) site.
//!
//! Each [`Provider<ForkInvocation<Fs, Fx>>`](dialog_capability::Provider) impl
//! resolves the invocation's [`FsAddress`](crate::FsAddress) to the registered
//! [`FileSystem`](dialog_storage::provider::FileSystem) provider and delegates
//! the capability to it. All filesystem I/O — layout, atomic writes, CAS
//! locking — lives in `dialog_storage`'s isomorphic provider; this crate only
//! does the credential (directory) resolution.

pub mod archive;
pub mod memory;
