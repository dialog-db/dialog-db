//! Querying a transaction's uncommitted writes.
//!
//! [`Transaction::query`](crate::repository::branch::Transaction::query)
//! returns a [`TransactionQuery`] that surfaces the transaction's
//! pending writes against the underlying branch — "as-if committed"
//! semantics so a caller can run normal queries mid-transaction.
//!
//! # Pending asserts and retracts
//!
//! The transaction's pending [`Changes`] flow directly into the query
//! engine through `Provider<Select> for Changes` — no in-memory tree
//! materialization. Asserts/Replaces surface as positive facts unioned
//! with the branch's stream; Retracts lift into tombstones (via
//! [`tombstones_from`]) that filter matching branch facts via
//! [`filter_tombstones`] before the merge, so a `tx.retract(x)` shadows
//! `x` in the underlying branch view without modifying the branch's
//! persistent tree.
//!
//! # Tombstone scope: source-only
//!
//! Tombstones suppress facts only in the branch source, not the
//! pending Changes overlay. So `tx.retract(X).assert(X)` correctly
//! shows `X`: the pending Changes surface `X` via Provider<Select>;
//! the branch's `X`, if any, is tombstoned but the overlay's `X`
//! passes through unmodified.
//!
//! # Auto-injected schema metadata
//!
//! `Transaction::query` resolves the operator's identity at
//! `.perform(env)` time and folds in the same schema-metadata overlay
//! that [`Branch::query`](crate::Branch::query) does (see
//! [`QueryLayer::overlay`](crate::QueryLayer::overlay)). So
//! [`Session`](crate::schema::Session) /
//! [`SessionBranch`](crate::schema::SessionBranch) / per-branch
//! [`BranchMetadata`](super::super::metadata::BranchMetadata) are
//! visible mid-transaction exactly the way they are post-commit.
//!
//! # Non-composable
//!
//! [`TransactionQuery`] intentionally has no `.with(...)` / `.join(...)`
//! chain — a transaction's view is the branch plus its own pending
//! changes, not a session you can layer more *fact sources* onto. To
//! compose more sources, commit the transaction first and use
//! [`Branch::query`](crate::Branch::query).
//!
//! Deductive-rule resolution is built into the shared [`QueryEnv`] this
//! routes through (a durable layer per branch + the overlay as a
//! transient layer), so rules resolve identically whether a concept is
//! queried mid-transaction or after commit — it's part of evaluating a
//! query, not an optional composition.

use dialog_artifacts::{Changes, DialogArtifactsError};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::memory::Resolve;
use dialog_query::query::{Application, Output};

use crate::layer::tombstones_from;
use crate::repository::branch::QueryLayer;
use crate::repository::branch::session::QueryEnv;
use crate::{Branch, RemoteSite};

/// A non-composable query handle returned by
/// [`Transaction::query`](crate::repository::branch::Transaction::query).
///
/// Holds an immutable snapshot of the transaction's pending changes
/// plus a reference to the branch. The transaction itself remains
/// open and committable.
///
/// See module docs for tombstone semantics.
pub struct TransactionQuery<'a> {
    branch: &'a Branch,
    changes: Changes,
}

impl<'a> TransactionQuery<'a> {
    pub(crate) fn new(branch: &'a Branch, changes: &Changes) -> Self {
        Self {
            branch,
            changes: changes.clone(),
        }
    }

    /// Stage a query against this transaction's view. Call
    /// [`perform`](TransactionSelectQuery::perform) to execute.
    pub fn select<Q: Application>(self, query: Q) -> TransactionSelectQuery<'a, Q> {
        TransactionSelectQuery {
            branch: self.branch,
            changes: self.changes,
            query,
        }
    }
}

/// A staged query on a [`TransactionQuery`].
pub struct TransactionSelectQuery<'a, Q> {
    branch: &'a Branch,
    changes: Changes,
    query: Q,
}

impl<'a, Q: Application> TransactionSelectQuery<'a, Q> {
    /// Execute the query, returning a stream of results.
    ///
    /// Mirrors [`SelectQuery::perform`](crate::SelectQuery::perform):
    /// resolves the operator's identity via [`Identify`], builds the
    /// per-query overlay (pending transaction changes + auto-injected
    /// schema metadata) through
    /// [`QueryLayer::overlay`](crate::QueryLayer::overlay), lifts any
    /// retracts in it into tombstones, and unions the branch stream
    /// (tombstone-filtered) with the overlay. The schema-metadata
    /// pass is what keeps `Session` / `SessionBranch` /
    /// `BranchMetadata` visible mid-transaction.
    pub fn perform<Env>(self, env: &'a Env) -> impl Output<Q::Conclusion> + 'a
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let TransactionSelectQuery {
            branch,
            changes,
            query,
        } = self;
        async_stream::try_stream! {
            let operator = Identify
                .perform(env)
                .await
                .map_err(|e| DialogArtifactsError::InvalidSignature(format!("identify: {e}")))?;

            // Route through the same QueryLayer overlay path
            // Branch::query() uses, so schema-injected metadata
            // (Session, SessionBranch, per-branch BranchMetadata)
            // surfaces alongside the transaction's pending changes.
            // `with(changes)` preserves Assert/Replace/Retract
            // polarity via `Statement for Changes`, so the user's
            // retracts stay retracts and lift into tombstones below.
            let overlay = QueryLayer::from(branch)
                .with(changes)
                .overlay(&operator);
            let tombstones = tombstones_from(&overlay);

            // A transaction query is just a single-branch `QueryEnv`.
            // Constructing the *same* env type the branch-session path
            // uses is what guarantees identical behavior — fact reads,
            // tombstones, schema metadata, and deductive-rule
            // resolution all share one implementation.
            let query_env = QueryEnv::new(vec![branch.clone()], overlay, tombstones, env);
            let results = Box::pin(query.perform(&query_env));
            for await result in results {
                yield result?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo};
    use crate::schema;
    use crate::schema::DidExt as _;
    use dialog_artifacts::Entity;
    use dialog_query::query::Output;
    use dialog_query::{Concept, Query, Term, the};

    mod people {

        /// `test/name` attribute used by the Person concept tests.
        #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
        #[domain("test")]
        pub struct Name(
            /// The person's name string.
            pub String,
        );
    }

    /// A simple concept used to exercise transaction-query semantics.
    #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    pub struct Person {
        /// The person entity.
        pub this: Entity,
        /// Their `test/name` attribute.
        pub name: people::Name,
    }

    /// `Transaction::integrate` replays an externally built [`Changes`]
    /// batch as if its instructions had been issued on the transaction
    /// directly: asserts surface, retracts tombstone, and the branch
    /// itself stays untouched until commit. This is the only pin on the
    /// integrate replay mapping (Assert/Replace -> associate, Retract ->
    /// dissociate); it died with the old transaction_query module and is
    /// ported back.
    #[dialog_common::test]
    async fn it_integrates_external_changes_into_branch_transaction() -> anyhow::Result<()> {
        use dialog_artifacts::Changes;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;

        // Seed the branch with Alice; Bob will be added via integrate.
        branch
            .transaction()
            .assert(Person {
                this: alice.clone(),
                name: people::Name("Alice".into()),
            })
            .commit()
            .perform(&operator)
            .await?;

        // Build an external Changes that adds Bob and retracts Alice's
        // name. This batch could have come from any source — a separate
        // builder, a reactor accumulator, etc.
        let mut external = Changes::new();
        external.assert(Person {
            this: bob.clone(),
            name: people::Name("Bob".into()),
        });
        external.retract(the!("test/name").of(alice.clone()).is("Alice".to_string()));

        // Integrate into a transaction and observe through tx.query().
        let tx = branch.transaction().integrate(external);

        let mut names: Vec<String> = tx
            .query()
            .select(Query::<Person> {
                this: Term::var("this"),
                name: Term::var("name"),
            })
            .perform(&operator)
            .try_vec()
            .await?
            .into_iter()
            .map(|p| p.name.0)
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec!["Bob".to_string()],
            "integrated changes must surface Bob (assert) and tombstone \
             Alice (retract); got {names:?}"
        );

        // The branch itself remains untouched until commit.
        let mut committed: Vec<String> = branch
            .query()
            .select(Query::<Person> {
                this: Term::var("this"),
                name: Term::var("name"),
            })
            .perform(&operator)
            .try_vec()
            .await?
            .into_iter()
            .map(|p| p.name.0)
            .collect();
        committed.sort();
        assert_eq!(
            committed,
            vec!["Alice".to_string()],
            "integrate must not mutate the branch before commit"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_surfaces_pending_asserts_through_transaction_query() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        let tx = branch.transaction().assert(Person {
            this: alice.clone(),
            name: people::Name("Alice".into()),
        });

        let results: Vec<Person> = tx
            .query()
            .select(Query::<Person> {
                this: alice.clone().into(),
                name: Term::var("name"),
            })
            .perform(&operator)
            .try_vec()
            .await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].this, alice);
        assert_eq!(results[0].name.0, "Alice");
        Ok(())
    }

    #[dialog_common::test]
    async fn it_tombstones_pending_retracts_through_transaction_query() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        branch
            .transaction()
            .assert(Person {
                this: alice.clone(),
                name: people::Name("Alice".into()),
            })
            .assert(Person {
                this: bob.clone(),
                name: people::Name("Bob".into()),
            })
            .commit()
            .perform(&operator)
            .await?;

        let tx = branch
            .transaction()
            .retract(the!("test/name").of(alice.clone()).is("Alice".to_string()));

        let names: Vec<String> = tx
            .query()
            .select(Query::<Person> {
                this: Term::var("this"),
                name: Term::var("name"),
            })
            .perform(&operator)
            .try_vec()
            .await?
            .into_iter()
            .map(|p| p.name.0)
            .collect();
        assert_eq!(names, vec!["Bob".to_string()]);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_keeps_value_when_retract_is_followed_by_assert() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        branch
            .transaction()
            .assert(Person {
                this: alice.clone(),
                name: people::Name("Alice".into()),
            })
            .commit()
            .perform(&operator)
            .await?;

        let stmt = the!("test/name").of(alice.clone()).is("Alice".to_string());
        let tx = branch.transaction().retract(stmt.clone()).assert(stmt);

        let names: Vec<String> = tx
            .query()
            .select(Query::<Person> {
                this: Term::var("this"),
                name: Term::var("name"),
            })
            .perform(&operator)
            .try_vec()
            .await?
            .into_iter()
            .map(|p| p.name.0)
            .collect();
        assert_eq!(names, vec!["Alice".to_string()]);
        Ok(())
    }

    /// `Transaction::query()` must surface the same auto-injected
    /// session metadata that `Branch::query()` does. The txn view is
    /// "branch + pending changes" — schema-shaped facts the branch
    /// auto-materializes (`Session`, `SessionBranch`, …) must pass
    /// through unchanged, even when the transaction is empty.
    ///
    /// Counterpart to `repository::tests::it_auto_includes_session_facts`.
    #[dialog_common::test]
    async fn it_auto_includes_session_facts_in_transaction_query() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let from_branch: Vec<schema::Session> = branch
            .query()
            .select(Query::<schema::Session> {
                this: schema::Session::entity().into(),
                profile: Term::var("profile"),
                operator: Term::var("operator"),
            })
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(from_branch.len(), 1, "Branch::query() must see the Session");

        let from_txn: Vec<schema::Session> = branch
            .transaction()
            .query()
            .select(Query::<schema::Session> {
                this: schema::Session::entity().into(),
                profile: Term::var("profile"),
                operator: Term::var("operator"),
            })
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(
            from_txn, from_branch,
            "Transaction::query() must see the same Session row as Branch::query()"
        );
        assert_eq!(from_txn[0].profile.0, profile.did().this());
        Ok(())
    }

    /// The schema-metadata overlay used by `Transaction::query()`
    /// must stay isolated to the query path: it must never leak into
    /// the transaction's committable `Changes`. So a transaction that
    /// only queries (no `.assert` / `.retract`) and then commits must
    /// land zero metadata facts on the branch tree.
    #[dialog_common::test]
    async fn it_does_not_leak_session_metadata_into_commits() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // Run a query that sees Session via the txn-query overlay,
        // then commit the (otherwise empty) transaction.
        let tx = branch.transaction();
        let seen: Vec<schema::Session> = tx
            .query()
            .select(Query::<schema::Session> {
                this: schema::Session::entity().into(),
                profile: Term::var("profile"),
                operator: Term::var("operator"),
            })
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(seen.len(), 1, "txn-query must see Session");
        tx.commit().perform(&operator).await?;

        // After commit, the branch tree must not contain any
        // `dialog.session/*` facts — those are auto-materialized at
        // query time, not persisted. Confirm by raw attribute lookup
        // through the branch's underlying claim stream.
        use dialog_query::AttributeQuery;
        use dialog_query::attribute::The;
        let profile_facts: Vec<dialog_query::Claim> = branch
            .query()
            .select(AttributeQuery::from(
                Term::<The>::from(the!("dialog.session/profile"))
                    .of(Term::<Entity>::var("e"))
                    .is(Term::<Entity>::var("v")),
            ))
            .perform(&operator)
            .try_vec()
            .await?;
        // `branch.query()` re-injects metadata at query time, so this
        // returns one row — but it's coming from the *overlay*, not
        // the persisted tree. Persisted metadata would mean two rows
        // (one from tree, one from overlay) post commit, since the
        // overlay isn't deduplicated against the tree.
        assert_eq!(
            profile_facts.len(),
            1,
            "metadata must come from the query-time overlay only, \
             not be persisted into the branch tree on commit; \
             saw {profile_facts:?}"
        );
        Ok(())
    }

    /// `dialog.session/branch` is cardinality-many and one row per
    /// branch in scope is asserted by the metadata pass. The
    /// transaction-query path must reproduce the full set.
    ///
    /// Counterpart to
    /// `repository::tests::it_auto_includes_session_branch_attribute_per_branch_in_scope`.
    #[dialog_common::test]
    async fn it_auto_includes_session_branch_in_transaction_query() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let main = repo.branch("main").open().perform(&operator).await?;

        let origin = schema::Replica::new(profile.did(), main.of().clone());
        let main_branch = schema::Branch::new(&origin, "main");

        let session_entity = schema::Session::entity();
        let rows: Vec<schema::SessionBranch> = main
            .transaction()
            .query()
            .select(Query::<schema::SessionBranch> {
                this: session_entity.into(),
                branch: Term::var("branch"),
            })
            .perform(&operator)
            .try_vec()
            .await?;
        let got: Vec<Entity> = rows.into_iter().map(|r| r.branch.0).collect();
        assert_eq!(
            got,
            vec![main_branch.this],
            "Transaction::query() must see the SessionBranch row for every in-scope branch"
        );
        Ok(())
    }
    /// The transaction view resolves the built-in derived concepts —
    /// including the recursive ancestry closure — exactly like a
    /// branch query, because both run on the same single-branch
    /// [`QueryEnv`](crate::repository::branch::session::QueryEnv).
    #[dialog_common::test]
    async fn it_resolves_derived_revision_concepts_in_a_transaction() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let mut revisions = Vec::new();
        for name in ["Alice", "Bob", "Carol"] {
            branch
                .transaction()
                .assert(the!("user/name").of(Entity::new()?).is(name.to_string()))
                .commit()
                .perform(&operator)
                .await?;
            revisions.push(branch.revision().expect("branch has a revision"));
        }
        let [first, second, third] = &revisions[..] else {
            unreachable!("three commits were made");
        };

        // An open, uncommitted transaction with its own pending write.
        let tx = branch
            .transaction()
            .assert(the!("user/name").of(Entity::new()?).is("Dave".to_string()));

        // The DAG edge projects from the tx view...
        let edges: Vec<schema::RevisionParent> = tx
            .query()
            .select(Query::<schema::RevisionParent> {
                this: third.entity().into(),
                parent: Term::var("parent"),
            })
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].parent.0, second.entity());

        // ... and so does the recursive closure: the fixpoint runs
        // through the transaction environment.
        let mut reachable: Vec<Entity> = tx
            .query()
            .select(Query::<schema::RevisionAncestor> {
                this: third.entity().into(),
                ancestor: Term::var("ancestor"),
            })
            .perform(&operator)
            .try_vec()
            .await?
            .into_iter()
            .map(|row| row.ancestor.0)
            .collect();
        reachable.sort();
        let mut expected = vec![first.entity(), second.entity()];
        expected.sort();
        assert_eq!(reachable, expected, "the head reaches both priors");

        Ok(())
    }

    /// A rule whose `dialog.rule/*` facts are pending in the transaction's
    /// change overlay resolves as a transient overlay rule: the uncommitted
    /// view derives through it without the rule (or the data) ever being
    /// committed.
    ///
    /// This stages the rule facts directly into the transaction's `Changes`
    /// (the overlay the pending-view query reads), NOT through `install_rule`,
    /// which is the durable/privileged rail. The two are distinct: the overlay
    /// is a per-view read surface, never persisted or gated.
    #[dialog_common::test]
    async fn it_resolves_rules_pending_in_the_transaction() -> anyhow::Result<()> {
        use dialog_query::rule::DeductiveRuleDescriptor;
        use dialog_query::{ConceptQuery, Parameters};

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // employee(this, name) :- person-name(this, name)
        let rule = {
            let json = serde_json::json!({
                "deduce": { "with": { "name": { "the": "org/employee-name", "as": "Text" } } },
                "when": [{
                    "assert": { "with": { "name": { "the": "org/person-name", "as": "Text" } } },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "name": { "?": { "name": "name" } }
                    }
                }]
            });
            let descriptor: DeductiveRuleDescriptor =
                serde_json::from_value(json).expect("descriptor parses");
            descriptor.compile().expect("rule compiles")
        };
        let employee = rule.conclusion().clone();

        let alice: Entity = "id:alice".parse()?;
        let tx = branch
            .transaction()
            .assert(
                the!("dialog.rule/conclusion")
                    .of(rule.this())
                    .is(employee.this()),
            )
            .assert(the!("dialog.rule/source").of(rule.this()).is(rule.encode()))
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            );

        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("name".into(), Term::var("name"));
        let rows = tx
            .query()
            .select(ConceptQuery {
                predicate: employee,
                terms,
            })
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(rows.len(), 1, "the pending rule derives the pending fact");
        assert_eq!(*rows[0].entity(), alice);

        Ok(())
    }
}
