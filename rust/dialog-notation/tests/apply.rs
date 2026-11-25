//! A document applied by the crate's driver, [`apply`]: one transaction,
//! its forms a sequence for what they read as for what they write.

#![allow(deprecated)]

use anyhow::Result;
use dialog_notation::{Analyzer, Applied, ApplyError, Error, apply, check};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_peer::{Peer, Session};
use dialog_repository::Branch;
use dialog_storage::provider::storage::VolatileSpace;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

const CONCEPTS: &str = r#"
    (concept (c ?t name: ?n tag: ?g)
      (xyz.c/name of: ?t is: ?n as: :text cardinality: :one)
      (xyz.c/tag of: ?t is: ?g as: :text cardinality: :many))
    (concept (n ?t v: ?v)
      (xyz.n/v of: ?t is: ?v as: :integer cardinality: :one))
    (concept (m ?t tag: ?g)
      (xyz.m/tag of: ?t is: ?g as: :text cardinality: :many))
    (concept (k ?t lvl: ?l)
      (xyz.k/lvl of: ?t is: ?l as: :text cardinality: :one))
    (concept (k/bump subject: ?t)
      (xyz.k.bump/subject of: ?c is: ?t as: :entity in: :control cardinality: :many))
    (rule (assert (k ?t lvl: "high")) (k/bump subject: ?t))"#;

/// A branch, the analyzer that read what was applied to it, and who
/// applies.
struct Document {
    operator: Peer<VolatileSpace, Session>,
    branch: Branch,
    analyzer: Analyzer,
}

impl Document {
    async fn open() -> Result<Self> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let mut document = Document {
            operator,
            branch,
            analyzer: Analyzer::new()?,
        };
        document.apply(CONCEPTS).await?;
        Ok(document)
    }

    async fn apply(&mut self, text: &str) -> Result<Applied, ApplyError> {
        let checked = check(&mut self.analyzer, text);
        apply(&self.branch, &checked, &self.operator).await
    }

    /// What `select` answers, each as text, sorted.
    async fn answers(&mut self, select: &str) -> Result<Vec<String>> {
        let mut answers: Vec<String> = self
            .apply(select)
            .await?
            .answers
            .iter()
            .map(ToString::to_string)
            .collect();
        answers.sort();
        Ok(answers)
    }
}

/// Each form of a document reads what the forms before it wrote: a
/// retract by the entity alone after an assert in the same document
/// retracts what the assert wrote as well as what was stored; a write
/// whose `when` reads a claim an earlier form asserted makes its write;
/// two writes with a `when` over one field, the second reading what the
/// first wrote, the second's `when` of two premises (a read and a sum)
/// read through a rule laid over that read alone.
#[dialog_common::test]
async fn it_reads_in_each_form_what_the_forms_before_it_wrote() -> Result<()> {
    let mut document = Document::open().await?;
    document
        .apply(r#"(assert (c x:1 name: "old" tag: "old"))"#)
        .await?;
    document
        .apply(r#"(assert (c x:1 name: "r" tag: "r")) (retract (c x:1))"#)
        .await?;
    assert_eq!(
        document
            .answers("(select (xyz.c x:1 name: ?n) (xyz.c x:1 name: ?n))")
            .await?,
        Vec::<String>::new()
    );
    assert_eq!(
        document
            .answers("(select (xyz.c x:1 tag: ?g) (xyz.c x:1 tag: ?g))")
            .await?,
        Vec::<String>::new()
    );

    document
        .apply(r#"(assert (n x:3 v: 0)) (assert (m ?t tag: "new") (when (n ?t v: 0)))"#)
        .await?;
    assert_eq!(
        document.answers("(select (m ?t tag: ?g))").await?,
        vec![r#"(xyz.m x:3 tag: "new")"#]
    );

    document
        .apply(
            "(assert (n x:3 v: (+ ?v : 1)) (when (n x:3 v: ?v)))
             (assert (n x:3 v: (+ ?v : 10)) (when (n x:3 v: ?v)))",
        )
        .await?;
    assert_eq!(
        document.answers("(select (n ?t v: ?v))").await?,
        vec!["(xyz.n x:3 v: 11)"]
    );
    Ok(())
}

/// A document is one transaction: a form that fails as it runs, after
/// forms that wrote, leaves the branch as it was, at its revision. Here
/// the last form's `when` binds a text where its write takes an integer.
#[dialog_common::test]
async fn it_writes_nothing_of_a_document_a_later_form_of_which_fails() -> Result<()> {
    let mut document = Document::open().await?;
    document.apply(r#"(assert (m x:1 tag: "seven"))"#).await?;
    let before = document.branch.revision();
    let failed = document
        .apply(
            r#"(assert (n x:2 v: 2))
               (assert (m x:2 tag: "two"))
               (assert (n ?t v: ?g) (when (m ?t tag: ?g)))"#,
        )
        .await;
    assert!(
        matches!(&failed, Err(ApplyError::Notation(Error::Refused(_)))),
        "{failed:?}"
    );
    assert_eq!(document.branch.revision(), before);
    assert_eq!(
        document.answers("(select (n ?t v: ?v))").await?,
        Vec::<String>::new()
    );
    assert_eq!(
        document.answers("(select (m ?t tag: ?g))").await?,
        vec![r#"(xyz.m x:1 tag: "seven")"#]
    );
    Ok(())
}

/// A command a document issues, and what the rules it fires write,
/// land at commit, after every form has run: a later `when` reads the
/// level the branch held, not the one the rule writes, and the command
/// itself, which holds for the transaction, as a row of its layer.
/// After the commit the rule's write is there.
#[dialog_common::test]
async fn it_lands_what_a_command_makes_at_commit() -> Result<()> {
    let mut document = Document::open().await?;
    document.apply(r#"(assert (k x:1 lvl: "low"))"#).await?;
    document
        .apply(
            r#"(assert (k/bump subject: x:1))
               (assert (xyz.seen x:9 lvl: ?l) (when (k x:1 lvl: ?l)))
               (assert (xyz.seen x:8 bumped: ?t) (when (xyz.k.bump ?c subject: ?t)))"#,
        )
        .await?;
    assert_eq!(
        document
            .answers("(select (xyz.seen x:9 lvl: ?l) (xyz.seen x:9 lvl: ?l))")
            .await?,
        vec![r#"(xyz.seen x:9 lvl: "low")"#]
    );
    assert_eq!(
        document
            .answers("(select (xyz.seen x:8 bumped: ?t) (xyz.seen x:8 bumped: ?t))")
            .await?,
        vec!["(xyz.seen x:8 bumped: x:1)"]
    );
    assert_eq!(
        document.answers("(select (k ?t lvl: ?l))").await?,
        vec![r#"(xyz.k x:1 lvl: "high")"#]
    );
    Ok(())
}

/// A select is answered after the commit, from what the whole document
/// left, wherever it stands in it.
#[dialog_common::test]
async fn it_answers_a_select_from_what_the_whole_document_left() -> Result<()> {
    let mut document = Document::open().await?;
    let applied = document
        .apply(
            r#"(assert (k x:1 lvl: "a"))
               (select (k ?t lvl: ?l))
               (assert (k x:2 lvl: "b"))"#,
        )
        .await?;
    let mut answers: Vec<String> = applied.answers.iter().map(ToString::to_string).collect();
    answers.sort();
    assert_eq!(
        answers,
        vec![r#"(xyz.k x:1 lvl: "a")"#, r#"(xyz.k x:2 lvl: "b")"#]
    );
    Ok(())
}

/// A document refused is applied not at all, and says its first
/// refusal.
#[dialog_common::test]
async fn it_applies_nothing_of_a_refused_document() -> Result<()> {
    let mut document = Document::open().await?;
    let before = document.branch.revision();
    let refused = document
        .apply(r#"(assert (k x:1 lvl: "a")) (assert (k x:2 nope: "b"))"#)
        .await;
    assert!(
        matches!(&refused, Err(ApplyError::Refused(diagnostic)) if diagnostic.refusal.is_some()),
        "{refused:?}"
    );
    assert_eq!(document.branch.revision(), before);
    assert_eq!(
        document.answers("(select (k ?t lvl: ?l))").await?,
        Vec::<String>::new()
    );
    Ok(())
}

/// A form written again in a document is a write again: each runs in
/// its place in the sequence. An assert, a retract and the same assert
/// leave the claim; the same write with a `when` twice adds twice. A
/// rule written twice is installed once, as installing it again would
/// change nothing.
#[dialog_common::test]
async fn it_runs_a_write_written_again_in_its_place() -> Result<()> {
    let mut document = Document::open().await?;
    document
        .apply("(assert (n x:1 v: 1)) (retract (n x:1 v: 1)) (assert (n x:1 v: 1))")
        .await?;
    assert_eq!(
        document.answers("(select (n x:1 v: ?v))").await?,
        vec!["(xyz.n x:1 v: 1)"]
    );
    document
        .apply(
            "(assert (n x:1 v: (+ ?v : 1)) (when (n x:1 v: ?v)))
             (assert (n x:1 v: (+ ?v : 1)) (when (n x:1 v: ?v)))",
        )
        .await?;
    assert_eq!(
        document.answers("(select (n x:1 v: ?v))").await?,
        vec!["(xyz.n x:1 v: 3)"]
    );
    let rule = r#"(rule (assert (m ?t tag: "seen")) (n ?t v: 3))"#;
    let checked = check(&mut document.analyzer, &format!("{rule} {rule}"));
    assert_eq!(checked.steps.len(), 1, "{:?}", checked.steps);
    Ok(())
}
