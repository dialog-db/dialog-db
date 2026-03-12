use dialog_capability::Did;
use serde::{Deserialize, Serialize};

use crate::repository::Site;

/// Persisted configuration for a remote site.
///
/// Stored in a memory cell keyed by the remote name, this captures
/// the site address and the issuer DID that authenticates operations
/// against this remote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteState {
    /// The site address (e.g., S3 bucket URL or service endpoint).
    pub site: Site,
    /// The DID of the issuer who has access to this remote.
    pub issuer: Did,
}
