//! WS4 integration: a real automerge-typed attribute driven end to end.
//!
//! The machinery tests in [`only`](super::only) and
//! [`resolution`](super::resolution) exercise the fold with toy formats and
//! hand-wired [`Resolution`](super::resolution::Resolution)s. This module is
//! the plan's single convergence point (`notes/automerge-integration-spec.md`
//! §5.1): it declares a genuine `#[derive(Attribute)] struct
//! Body(Recorded<TextDocument>)` and drives the *shipped* typed path —
//! diverge → fold → edit → converge — with no manual `.with_resolution(...)`.
//! The fold is sourced automatically from the attribute's format
//! ([`Scalar::resolution`](crate::types::Scalar::resolution)), so what these
//! tests prove is what an application actually gets.
//!
//! Storage is reproduced the way a replica reaches it after pull: two
//! different canonical byte values for one `(the, of)` coexist as sibling
//! claims (§2). Rather than stand up a remote to pull between (which the
//! sibling state does not require), each replica asserts both forks additively
//! — the deterministic equivalent of the post-sync state — and the two
//! replicas assert them in *opposite* orders, so any order dependence in the
//! projection would surface.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::artifact::{RecordFormat, Recorded};
use crate::attribute::Attribute;
use crate::attribute::AttributeStatement;
use crate::attribute::query::StaticAttributeQuery;
use crate::attribute::query::all::AttributeQueryAll;
use crate::query::{Application, Output};
use crate::session::RuleRegistry;
use crate::source::test::TestEnv;
use crate::{Entity, Term, Value};

use dialog_automerge::TextDocument;
use dialog_repository::helpers::{test_operator_with_profile, test_repo};

mod note {
    use crate::Attribute;
    use crate::artifact::Recorded;
    use dialog_automerge::TextDocument;

    /// A collaboratively edited note body, stored as an automerge document.
    #[derive(Attribute, Clone)]
    pub struct Body(pub Recorded<TextDocument>);
}

/// A shared base document plus two concurrently-edited forks — the divergence
/// two replicas produce editing the same document offline. The forks descend
/// from one created document, as convergence requires (crate docs on shared
/// ancestry).
fn diverged_forks() -> (TextDocument, TextDocument) {
    let mut base = TextDocument::new();
    base.splice(0, 0, "draft").unwrap();

    let mut left = base.fork();
    let mut right = base.fork();
    left.splice(0, 0, "my ").unwrap();
    right.splice(5, 0, " notes").unwrap();

    (left, right)
}

/// An *additive* record assertion (`cardinality: None` → `Assert`, not
/// `Replace`): two of these for one entity coexist as sibling claims, the
/// storage state a replica holds after pulling a concurrent edit.
fn sibling_fact(of: &Entity, fork: &TextDocument) -> AttributeStatement {
    AttributeStatement {
        the: note::Body::the(),
        of: of.clone(),
        is: Value::Record(fork.encode().expect("a fork encodes").into()),
        cause: None,
        cardinality: None,
    }
}

/// diverge → fold: every replica projects the *merged* document from the same
/// diverged siblings, and mints the identical value doing so — regardless of
/// the order the siblings entered its storage.
#[dialog_common::test]
async fn it_folds_diverged_replicas_to_one_identity() -> anyhow::Result<()> {
    let (left, right) = diverged_forks();
    let expected = Recorded::new(TextDocument::merge(&left, &right))?;
    let doc = Entity::new()?;

    // Two replicas hold the same two siblings in opposite orders.
    let mut projections = Vec::new();
    for forks in [[&left, &right], [&right, &left]] {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        for fork in forks {
            branch
                .transaction()
                .assert(sibling_fact(&doc, fork))
                .commit()
                .perform(&operator)
                .await?;
        }

        let query = StaticAttributeQuery::<note::Body> {
            of: Term::from(doc.clone()),
            is: Term::var("body"),
        };
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = Application::perform(query, &source).try_vec().await?;

        assert_eq!(results.len(), 1, "Cardinality::One yields one row per entity");
        let (of, is, _cause) = results.into_iter().next().unwrap().into_parts();
        assert_eq!(of, doc);

        // The reader never sees a fork: it sees the merge, which contains
        // both sides' edits.
        assert_eq!(is.value().realize()?.text(), "my draft notes");
        // ...and the projected value is the canonical merged record, byte for
        // byte, so the two replicas agree on identity (and tree key).
        assert_eq!(is.value(), &expected);

        projections.push(is.value().clone());
    }

    assert_eq!(
        projections[0], projections[1],
        "sibling insertion order must not change the projected identity"
    );

    Ok(())
}

/// The value-position convention for a diverged record (§6.10), through the
/// typed challenge path. While storage holds the forks, the record is
/// unqueryable *by value*: neither a stored fork nor the merged fold product
/// is a stored key, so both yield zero rows. A `Cardinality::One` write then
/// converges storage — and the written value becomes queryable, exactly as a
/// settled scalar is.
#[dialog_common::test]
async fn it_becomes_queryable_by_value_after_convergence() -> anyhow::Result<()> {
    let (left, right) = diverged_forks();
    let merged = Recorded::new(TextDocument::merge(&left, &right))?;
    let doc = Entity::new()?;

    let (operator, profile) = test_operator_with_profile().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    for fork in [&left, &right] {
        branch
            .transaction()
            .assert(sibling_fact(&doc, fork))
            .commit()
            .perform(&operator)
            .await?;
    }
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    let by_value = |form: Recorded<TextDocument>| StaticAttributeQuery::<note::Body> {
        of: Term::var("doc"),
        is: Term::from(form),
    };

    // Diverged: a stored fork is not the fold product; the fold product is not
    // a stored key. Both yield nothing.
    let fork = Application::perform(by_value(Recorded::new(left.clone())?), &source)
        .try_vec()
        .await?;
    assert_eq!(fork.len(), 0, "a stored fork is filtered by verification");

    let product = Application::perform(by_value(merged.clone()), &source)
        .try_vec()
        .await?;
    assert_eq!(
        product.len(),
        0,
        "the fold product is not a stored key until a write converges storage"
    );

    // Converge storage with a Cardinality::One write of the merged document.
    branch
        .transaction()
        .assert(note::Body::of(doc.clone()).is(merged.clone()))
        .commit()
        .perform(&operator)
        .await?;

    // Now the settled value is queryable, like any scalar.
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
    let settled = Application::perform(by_value(merged.clone()), &source)
        .try_vec()
        .await?;
    assert_eq!(settled.len(), 1, "a converged record is queryable by value");
    let (of, _is, _cause) = settled.into_iter().next().unwrap().into_parts();
    assert_eq!(of, doc);

    Ok(())
}

/// diverge → fold → converge: writing the fold product back as a
/// `Cardinality::One` assertion (§4.4) collapses storage to a single sibling.
/// Because both replicas compute the *identical* merged value from the same
/// change-set, their independent write-backs mint byte-identical claims — the
/// same tree key — so a later sync sees no divergence at all (the spec's
/// "concurrent identical write-backs collide onto the same key").
#[dialog_common::test]
async fn it_converges_storage_across_replicas() -> anyhow::Result<()> {
    let (left, right) = diverged_forks();
    let doc = Entity::new()?;

    let mut converged = Vec::new();
    for forks in [[&left, &right], [&right, &left]] {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        for fork in forks {
            branch
                .transaction()
                .assert(sibling_fact(&doc, fork))
                .commit()
                .perform(&operator)
                .await?;
        }

        // Read the folded document, then write it back unchanged as an
        // ordinary `Cardinality::One` edit — a `Replace` that supersedes every
        // different-valued sibling.
        let read = StaticAttributeQuery::<note::Body> {
            of: Term::from(doc.clone()),
            is: Term::var("body"),
        };
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = Application::perform(read, &source).try_vec().await?;
        let (_of, body, _cause) = results.into_iter().next().unwrap().into_parts();
        let merged = body.value().clone();

        branch
            .transaction()
            .assert(note::Body::of(doc.clone()).is(merged.clone()))
            .commit()
            .perform(&operator)
            .await?;

        // Storage has physically converged: the two forks are gone, one
        // merged sibling remains.
        let all = AttributeQueryAll::new(
            Term::from(note::Body::the()),
            Term::from(doc.clone()),
            Term::var("body"),
            Term::var("cause"),
        );
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let siblings = all.perform(&source).try_vec().await?;
        assert_eq!(siblings.len(), 1, "Replace collapsed storage to one sibling");
        assert_eq!(
            siblings[0].is(),
            &Value::Record(merged.record().clone()),
            "the surviving sibling is the merged document"
        );

        converged.push(merged);
    }

    // The heart of the convergence guarantee: two replicas that assembled the
    // same change-set along different sibling orders mint the *identical*
    // canonical value, and therefore collide onto one tree key.
    assert_eq!(
        converged[0].as_bytes(),
        converged[1].as_bytes(),
        "independent replicas converge on identical canonical bytes"
    );

    Ok(())
}

/// The edit leg of diverge → fold → **edit** → converge: reading the folded
/// document, splicing a new edit, and committing it as a `Cardinality::One`
/// assertion supersedes every diverged sibling in one `Replace`. The surviving
/// document carries both replicas' edits plus the local one — no forked edit is
/// lost. (Two replicas that make *different* edits re-diverge and re-converge
/// on the next sync; that is ordinary CRDT behavior, tested at the format
/// level in `dialog-automerge`.)
#[dialog_common::test]
async fn it_collapses_storage_on_edit_writeback() -> anyhow::Result<()> {
    let (left, right) = diverged_forks();
    let doc = Entity::new()?;

    let (operator, profile) = test_operator_with_profile().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    for fork in [&left, &right] {
        branch
            .transaction()
            .assert(sibling_fact(&doc, fork))
            .commit()
            .perform(&operator)
            .await?;
    }

    // Read the folded document — this is what the editor opens.
    let read = StaticAttributeQuery::<note::Body> {
        of: Term::from(doc.clone()),
        is: Term::var("body"),
    };
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
    let results = Application::perform(read, &source).try_vec().await?;
    let (_of, body, _cause) = results.into_iter().next().unwrap().into_parts();

    // Edit the merged document and write it back (a `Replace`).
    let mut editing = (*body.value().realize()?).clone();
    let end = editing.text().chars().count();
    editing.splice(end, 0, "!")?;
    let edited = Recorded::new(editing)?;

    branch
        .transaction()
        .assert(note::Body::of(doc.clone()).is(edited.clone()))
        .commit()
        .perform(&operator)
        .await?;

    let all = AttributeQueryAll::new(
        Term::from(note::Body::the()),
        Term::from(doc.clone()),
        Term::var("body"),
        Term::var("cause"),
    );
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
    let siblings = all.perform(&source).try_vec().await?;
    assert_eq!(siblings.len(), 1, "Replace collapsed storage to one sibling");
    assert_eq!(
        siblings[0].is(),
        &Value::Record(edited.record().clone()),
        "the surviving sibling is the merged-and-edited document"
    );
    assert_eq!(
        edited.realize()?.text(),
        "my draft notes!",
        "the write preserved both diverged edits plus the local one"
    );

    Ok(())
}
