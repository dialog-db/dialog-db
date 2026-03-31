//! Query trait for polymorphic querying across different store types

mod application;
mod output;
mod source;

pub use application::*;
pub use output::*;
pub use source::*;

use crate::artifact::ArtifactStoreMut;
use dialog_common::ConditionalSend;

/// A mutable artifact store that can be used for writes in a [`Session`](crate::Session).
///
/// This is a marker trait automatically implemented for any type that
/// satisfies `ArtifactStoreMut + Clone + ConditionalSend`. It exists to
/// give a shorter name to the full bound and to decouple `Session` from
/// the specific artifact storage implementation.
pub trait Store: ArtifactStoreMut + Clone + ConditionalSend {}
/// Blanket implementation - any type that satisfies the bounds automatically implements QueryStore
impl<T> Store for T where T: ArtifactStoreMut + Clone + ConditionalSend {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::Entity;
    use crate::attribute::query::AttributeQuery;
    use crate::session::RuleRegistry;
    use crate::source::Source;
    use crate::{Term, Transaction, the};
    use anyhow::Result;
    use dialog_repository::helpers::{test_operator, test_repo};

    #[dialog_common::test]
    async fn it_queries_via_fact_selector() -> Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let mut tx = Transaction::new();
        tx.assert(the!("user/name").of(alice.clone()).is("Alice".to_string()));
        tx.assert(
            the!("user/email")
                .of(alice.clone())
                .is("alice@example.com".to_string()),
        );
        tx.assert(the!("user/name").of(bob.clone()).is("Bob".to_string()));
        branch.commit(tx.into_stream()).perform(&operator).await?;

        let alice_query = AttributeQuery::new(
            Term::from(the!("user/name")),
            alice.clone().into(),
            Term::constant("Alice".to_string()),
            Term::blank(),
            None,
        );

        let source = Source::new(&branch, &operator, RuleRegistry::new());
        let result = alice_query.perform(&source).try_vec().await;
        assert!(result.is_ok());

        let all_names_query = AttributeQuery::new(
            Term::from(the!("user/name")),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let result = all_names_query.perform(&source).try_vec().await;
        assert!(result.is_ok());

        let email_query = AttributeQuery::new(
            Term::from(the!("user/email")),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let result = email_query.perform(&source).try_vec().await;
        assert!(result.is_ok());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_succeeds_with_variables_and_constants() -> Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let variable_query = AttributeQuery::new(
            Term::from(the!("user/name")),
            Term::<Entity>::var("user"),
            Term::var("name"),
            Term::blank(),
            None,
        );

        let source = Source::new(&branch, &operator, RuleRegistry::new());
        let result = variable_query.perform(&source).try_vec().await;
        assert!(result.is_ok());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_queries_polymorphically() -> Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let mut tx = Transaction::new();
        tx.assert(the!("user/name").of(alice.clone()).is("Alice".to_string()));
        branch.commit(tx.into_stream()).perform(&operator).await?;

        let fact_selector = AttributeQuery::new(
            Term::from(the!("user/name")),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let source = Source::new(&branch, &operator, RuleRegistry::new());
        let results = fact_selector.perform(&source).try_vec().await?;
        assert_eq!(results.len(), 1);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_chains_query_operations() -> Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let mut tx = Transaction::new();
        tx.assert(the!("user/name").of(alice.clone()).is("Alice".to_string()));
        tx.assert(the!("user/role").of(alice.clone()).is("admin".to_string()));
        tx.assert(the!("user/name").of(bob.clone()).is("Bob".to_string()));
        tx.assert(the!("user/role").of(bob.clone()).is("user".to_string()));
        branch.commit(tx.into_stream()).perform(&operator).await?;

        let source = Source::new(&branch, &operator, RuleRegistry::new());
        let admin_result = AttributeQuery::new(
            Term::from(the!("user/role")),
            Term::blank(),
            Term::constant("admin".to_string()),
            Term::blank(),
            None,
        )
        .perform(&source)
        .try_vec()
        .await;
        assert!(admin_result.is_ok());

        let names_result = AttributeQuery::new(
            Term::from(the!("user/name")),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        )
        .perform(&source)
        .try_vec()
        .await;
        assert!(names_result.is_ok());

        Ok(())
    }
}
