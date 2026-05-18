//! Provider implementations for the [`Fs`](super::Fs) site.
//!
//! Each module provides two layers, mirroring `dialog_remote_s3`:
//! - `Provider<ForkInvocation<Fs, Fx>>` — redeems authorization (no-op for
//!   FS) and dispatches into the per-invocation execution layer
//! - `Provider<FsInvocation<Fx>>` — actual I/O against the registered
//!   directory handle (browser: FS Access API; native: `std::fs`)
//!
//! The execution layer currently returns a "not yet implemented" storage
//! error — wired-up I/O lands in a follow-up commit (see
//! `plan/fs-remote.md` in the tonk repo for phasing).

pub mod archive;
pub mod memory;
