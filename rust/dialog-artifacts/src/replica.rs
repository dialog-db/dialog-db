use dialog_storage::Blake3Hash;
use serde::{Deserialize, Serialize};

pub use super::uri::Uri;

/// Cryptographic identifier like Ed25519 public key representing
/// an principal that produced a change. We may
pub type Principal = [u8; 32];

/// Specific revision from which replica diverged.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Origin {
    /// Principal authority that produced this release.
    issuer: Principal,
    /// Version number of this release.
    version: usize,
    /// Hash of the tree root.
    checksum: Blake3Hash,
}

/// Represents a revision of the replica.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Revision {
    /// Revision from which this branch has diverged.
    origin: Option<Origin>,
    /// Principal authority that produced this release.
    issuer: Principal,
    /// Number of transactions made to the origin.
    drift: usize,
    /// Hash of the tree root.
    checksum: Blake3Hash,
}

/// Logical timestamp we can use when we produce transactions.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct DivergenceClock<'a> {
    /// Origin from which we have diverged.
    origin: &'a Principal,
    /// Version on the origin from which we have diverged.
    since: &'a usize,
    /// Replica that is diverged from the origin.
    site: &'a Principal,
    /// Number of transactions replica made since it diverged
    drift: &'a usize,
}
impl<'a> DivergenceClock<'a> {
    /// Create a new divergence clock.
    pub fn new(
        origin: &'a Principal,
        since: &'a usize,
        site: &'a Principal,
        drift: &'a usize,
    ) -> Self {
        Self {
            origin,
            since,
            site,
            drift,
        }
    }
    ///
    pub fn new_local(site: &'a Principal, drift: &'a usize) -> Self {
        Self {
            origin: site,
            since: drift,
            site,
            drift,
        }
    }
}

impl Revision {
    /// Create a logical timestamp we can use when we produce transactions.
    pub fn to_cause(&self) -> DivergenceClock<'_> {
        match &self.origin {
            Some(origin) => {
                DivergenceClock::new(&origin.issuer, &origin.version, &self.issuer, &self.drift)
            }
            None => DivergenceClock::new_local(&self.issuer, &self.drift),
        }
    }
}

/// Represents a remote from which we can fetch / pull and or push.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Remote {
    /// Principal authority representing this remote.
    audience: Principal,
    /// Address into which we can push or pull from.
    address: Uri,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Replica {
    revision: Revision,
}
