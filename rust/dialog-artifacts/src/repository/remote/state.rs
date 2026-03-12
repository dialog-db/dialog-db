use dialog_capability::Did;
use serde::{Deserialize, Serialize};

use crate::environment::Address;
use crate::repository::Site;

/// Persisted configuration for a remote site.
///
/// Stored in a memory cell keyed by the remote name, this captures
/// the site address, the issuer DID, and the credentials used to
/// authenticate operations against this remote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteState {
    /// The human-readable site name (e.g., "s3://my-bucket").
    pub site: Site,
    /// The DID of the issuer who has access to this remote.
    pub issuer: Did,
    /// The credentials used to authenticate remote operations.
    pub address: Address,
}
