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
    use crate::artifact::{Artifacts, Entity};
    use crate::relation::query::RelationQuery;
    use crate::{Session, Term, the};
    use anyhow::Result;
    use dialog_storage::MemoryStorageBackend;

    #[dialog_common::test]
    async fn it_queries_via_fact_selector() -> Result<()> {
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let claims = vec![
            the!("user/name").of(alice.clone()).is("Alice".to_string()),
            the!("user/email")
                .of(alice.clone())
                .is("alice@example.com".to_string()),
            the!("user/name").of(bob.clone()).is("Bob".to_string()),
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let alice_query = RelationQuery::new(
            Term::from(the!("user/name")),
            alice.clone().into(),
            Term::constant("Alice".to_string()),
            Term::blank(),
            None,
        );

        let session = Session::open(artifacts.clone());
        let result = alice_query.perform(&session).try_vec().await;
        assert!(result.is_ok());

        let all_names_query = RelationQuery::new(
            Term::from(the!("user/name")),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let session = Session::open(artifacts.clone());
        let result = all_names_query.perform(&session).try_vec().await;
        assert!(result.is_ok());

        let email_query = RelationQuery::new(
            Term::from(the!("user/email")),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let session = Session::open(artifacts);
        let result = email_query.perform(&session).try_vec().await;
        assert!(result.is_ok());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_succeeds_with_variables_and_constants() -> Result<()> {
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let variable_query = RelationQuery::new(
            Term::from(the!("user/name")),
            Term::<Entity>::var("user"),
            Term::var("name"),
            Term::blank(),
            None,
        );

        let session = Session::open(artifacts);
        let result = variable_query.perform(&session).try_vec().await;
        assert!(result.is_ok());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_queries_polymorphically() -> Result<()> {
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let claims = vec![the!("user/name").of(alice.clone()).is("Alice".to_string())];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let fact_selector = RelationQuery::new(
            Term::from(the!("user/name")),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let session = Session::open(artifacts);
        let results = fact_selector.perform(&session).try_vec().await?;
        assert_eq!(results.len(), 1);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_chains_query_operations() -> Result<()> {
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let claims = vec![
            the!("user/name").of(alice.clone()).is("Alice".to_string()),
            the!("user/role").of(alice.clone()).is("admin".to_string()),
            the!("user/name").of(bob.clone()).is("Bob".to_string()),
            the!("user/role").of(bob.clone()).is("user".to_string()),
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let session = Session::open(artifacts.clone());
        let admin_result = RelationQuery::new(
            Term::from(the!("user/role")),
            Term::blank(),
            Term::constant("admin".to_string()),
            Term::blank(),
            None,
        )
        .perform(&session)
        .try_vec()
        .await;
        assert!(admin_result.is_ok());

        let session = Session::open(artifacts);
        let names_result = RelationQuery::new(
            Term::from(the!("user/name")),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        )
        .perform(&session)
        .try_vec()
        .await;
        assert!(names_result.is_ok());

        Ok(())
    }
}
