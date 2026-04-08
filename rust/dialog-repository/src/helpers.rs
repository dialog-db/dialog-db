// Re-export operator-level helpers.
pub use dialog_operator::helpers::{
    generate_data, test_operator, test_operator_with_profile, unique_name,
};

use crate::Repository;
use crate::repository::RepositoryExt as _;
use dialog_credentials::Credential;
use dialog_operator::profile::Profile;
use dialog_storage::provider::storage::VolatileSpace;

/// Create a test repository using the given operator and profile.
pub async fn test_repo(
    operator: &dialog_operator::Operator<VolatileSpace>,
    profile: &Profile,
) -> Repository<Credential> {
    profile
        .repository(unique_name("repo"))
        .open()
        .perform(operator)
        .await
        .expect("test_repo: failed to open repository")
}
