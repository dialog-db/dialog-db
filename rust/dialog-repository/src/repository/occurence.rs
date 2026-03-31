use dialog_capability::Did;
use serde::{Deserialize, Serialize};

use super::revision::Revision;

/// Logical timestamp used to denote dialog transactions. It takes inspiration
/// from automerge which tags lamport timestamps with origin information. It
/// takes inspiration from [Hybrid Logical Clocks (HLC)](https://sergeiturukin.com/2017/06/26/hybrid-logical-clocks.html)
/// and splits timestamp into two components `period` representing coordinated
/// component of the time and `moment` representing an uncoordinated local
/// time component. This construction allows us to capture synchronization
/// points allowing us to prioritize replicas that are actively collaborating
/// over those that are not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Occurence {
    /// DID of the site where this occurence happened.
    pub site: Did,

    /// Logical coordinated time component denoting a last synchronization
    /// cycle.
    pub period: usize,

    /// Local uncoordinated time component denoting a moment within a
    /// period at which occurrence happened.
    pub moment: usize,
}

impl From<Revision> for Occurence {
    fn from(revision: Revision) -> Self {
        Occurence {
            site: revision.issuer,
            period: revision.period,
            moment: revision.moment,
        }
    }
}
