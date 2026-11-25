//! What a read through a transaction answers of the writes staged in
//! it, against what committing them makes true: the ground the crate's
//! driver stands on when a write's `when` reads what earlier forms of
//! its document wrote.

#![allow(deprecated)]

use anyhow::Result;
use dialog_notation::{Analyzer, Step, check};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_query::ConceptConclusion;
use dialog_query::query::Output as _;
use dialog_repository::{Branch, Transaction};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

const CONCEPT: &str = r#"(concept (r ?t o: ?o)
  (xyz.r/o of: ?t is: ?o as: :text cardinality: :one))"#;

/// The steps of `text`, refused if any form of it is.
fn steps(analyzer: &mut Analyzer, text: &str) -> Vec<Step> {
    let checked = check(analyzer, text);
    assert!(
        checked.refusals().next().is_none(),
        "{text}: {:?}",
        checked.diagnostics
    );
    checked.steps
}

/// A transaction of `steps` on `branch`.
fn staged(branch: &Branch, steps: Vec<Step>) -> Transaction<&Branch> {
    let mut transaction = branch.transaction();
    for step in steps {
        transaction = match step {
            Step::Assert(statement) => transaction.assert(statement),
            Step::Retract(statement) => transaction.retract(statement),
            Step::Deduce(rule) => transaction.assert(*rule),
            other => panic!("{other:?}"),
        };
    }
    transaction
}

/// What `read`, a `when`, answers: through a transaction that stages
/// `document` over `stored`, and on the branch once that transaction is
/// committed.
async fn compare(stored: &str, document: &str, read: &str) -> Result<(Vec<String>, Vec<String>)> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let mut analyzer = Analyzer::new()?;
    steps(&mut analyzer, CONCEPT);
    staged(&branch, steps(&mut analyzer, stored))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch.refresh(&operator).await?;

    let probe = format!("(assert (xyz.seen x:9 v: ?v) (when {read}))");
    let probed = steps(&mut analyzer, &probe);
    let [Step::Write(write)] = probed.as_slice() else {
        panic!("{probe} is no write");
    };
    let answers = |rows: &[ConceptConclusion]| -> Result<Vec<String>> {
        let mut all = rows
            .iter()
            .map(|row| {
                Ok(write
                    .statement(row)
                    .map(|statement| format!("{:?}", statement.with[0].is))?)
            })
            .collect::<Result<Vec<_>>>()?;
        all.sort();
        all.dedup();
        Ok(all)
    };

    let document = steps(&mut analyzer, document);
    assert!(
        write.query.rule.is_none(),
        "{read} reads through a rule of its own"
    );
    let transaction = staged(&branch, document.clone());
    let through: Vec<ConceptConclusion> = transaction
        .query()
        .select(write.query.select.clone())
        .perform(&operator)
        .try_vec()
        .await?;
    let through = answers(&through)?;
    drop(transaction);

    staged(&branch, document)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch.refresh(&operator).await?;
    let mut layer = branch.query();
    if let Some(rule) = &write.query.rule {
        layer = layer.with(rule.clone());
    }
    let committed: Vec<ConceptConclusion> = layer
        .select(write.query.select.clone())
        .perform(&operator)
        .try_vec()
        .await?;
    Ok((through, answers(&committed)?))
}

/// A read through a transaction answers what its commit then holds: an
/// assert over a stored claim of a `:one` field, read through the
/// concept and through the relation; a retract of a stored claim; an
/// assert and then a retract of one claim, and a retract and then an
/// assert; and a relation a rule installed in the document derives.
#[dialog_common::test]
async fn it_reads_through_a_transaction_what_its_commit_holds() -> Result<()> {
    let stored = r#"(assert (r x:1 o: "A"))"#;
    let mut differ = Vec::new();
    for (document, read) in [
        (r#"(assert (r x:1 o: "B"))"#, "(r x:1 o: ?v)"),
        (r#"(assert (r x:1 o: "B"))"#, "(xyz.r x:1 o: ?v)"),
        (r#"(retract (r x:1 o: "A"))"#, "(xyz.r x:1 o: ?v)"),
        (
            r#"(assert (r x:2 o: "A")) (assert (r x:2 o: "B"))"#,
            "(xyz.r x:2 o: ?v)",
        ),
        (
            r#"(assert (xyz.r x:1 o: "C")) (retract (xyz.r x:1 o: "C"))"#,
            "(xyz.r x:1 o: ?v)",
        ),
        (
            r#"(retract (xyz.r x:1 o: "A")) (assert (xyz.r x:1 o: "A"))"#,
            "(xyz.r x:1 o: ?v)",
        ),
        (
            r#"(rule (xyz.d ?t v: ?v) (xyz.r ?t o: ?v)) (assert (xyz.r x:1 o: "C"))"#,
            "(xyz.d x:1 v: ?v)",
        ),
    ] {
        let (through, committed) = compare(stored, document, read).await?;
        if through != committed {
            differ.push((document, read, through, committed));
        }
    }
    assert!(differ.is_empty(), "{differ:?}");
    Ok(())
}
