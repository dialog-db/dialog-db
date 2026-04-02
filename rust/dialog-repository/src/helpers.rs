// Re-export operator-level helpers.
pub use dialog_operator::helpers::{
    generate_data, test_operator, test_operator_with_profile, unique_location, unique_name,
};

use crate::Operator;
use crate::repository::Repository;

/// Open a test repository against the given operator.
pub async fn test_repo(operator: &Operator) -> Repository {
    Repository::open(unique_location("repo"))
        .perform(operator)
        .await
        .unwrap()
}
