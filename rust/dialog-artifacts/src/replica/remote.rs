//! Remote repository credentials and configuration.
//!
//! This module defines the types used to connect to remote repositories
//! for synchronization. Different remote backends are enabled via feature flags.
//!
//! # Feature Flags
//!
//! - `s3` - Enables S3-compatible storage (AWS S3, Cloudflare R2, MinIO)
//! - `ucan` - Enables UCAN-based authorization for S3 (implies `s3`)
//!
//! Without any remote features enabled, the [`RemoteCredentials`] enum is empty
//! and cannot be constructed, preventing remote configuration at compile time.

mod backend;
mod branch;
mod connection;
mod credentials;
mod repository;
mod site;
mod state;

pub use backend::*;
pub use branch::*;
pub use connection::*;
pub use credentials::*;
pub use repository::*;
pub use site::*;
pub use state::*;

pub use super::OperatingAuthority;
use super::{Operator, PlatformBackend, Revision};
use crate::PlatformStorage;

/// A named remote site identifier.
pub type Site = String;
