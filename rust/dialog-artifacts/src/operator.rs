//! Operator — an operating environment built from a Profile.
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # use dialog_artifacts::profile::Profile;
//! # use dialog_artifacts::remote::Remote;
//! # use dialog_capability::storage::Storage;
//! # use dialog_storage::provider::FileSystem;
//! #
//! # let profile = Profile::named("personal")
//! #     .open(Storage::profile())
//! #     .perform(&FileSystem)
//! #     .await?;
//! let operator = profile
//!     .operator(b"alice")
//!     .storage(FileSystem)
//!     .network(Remote)
//!     .mount(Storage::storage())
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```

mod builder;
#[cfg(test)]
mod test;

pub use builder::{MountBuilder, OperatorBuilder, OperatorError, StorageBuilder};

use crate::Credentials;
use crate::environment::Environment;
use dialog_capability::Capability;
use dialog_capability::storage::Location;
use dialog_credentials::SignerCredential;
use dialog_varsig::{Did, Principal};
use std::ops::Deref;

/// An operating environment built from a [`Profile`](crate::profile::Profile).
///
/// Holds the profile + operator credentials, mounted storage, network config,
/// and the set of allowed capabilities.
pub struct Operator<Local, Remote> {
    credential: SignerCredential,
    location: Capability<Location>,
    env: Environment<Credentials, Local, Remote>,
}

impl<Local, Remote> Operator<Local, Remote> {
    /// The profile's signing credential.
    pub fn credential(&self) -> &SignerCredential {
        &self.credential
    }

    /// The operator's DID (the ephemeral/derived session key).
    pub fn did(&self) -> Did {
        self.env.authority.did()
    }

    /// The profile's DID (the long-lived identity).
    pub fn profile_did(&self) -> Did {
        self.env.authority.profile_did()
    }

    /// The mounted storage location.
    pub fn location(&self) -> &Capability<Location> {
        &self.location
    }

    /// The inner environment.
    pub fn env(&self) -> &Environment<Credentials, Local, Remote> {
        &self.env
    }
}

impl<Local, Remote> Principal for Operator<Local, Remote> {
    fn did(&self) -> Did {
        self.env.authority.did()
    }
}

impl<Local, Remote> Deref for Operator<Local, Remote> {
    type Target = Environment<Credentials, Local, Remote>;

    fn deref(&self) -> &Self::Target {
        &self.env
    }
}
