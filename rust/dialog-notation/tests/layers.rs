//! `in:` on a line names any layer a stack declares, or a variable: a
//! write lands in the layer it names, and a read with a variable binds
//! the layer each row is in. A write's layer variable must be bound by
//! what the write reads, and one nothing binds is refused when the
//! document is checked.

#![allow(deprecated)]

use anyhow::Result;
use dialog_notation::apply::Writer;
use dialog_notation::{Analyzer, Answer, Error, apply, check, stage};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_repository::Branch;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

const CONCEPTS: &str = r#"
    (concept (note ?t text: ?x)
      (xyz.note/text of: ?t is: ?x as: :text cardinality: :one))
    (concept (draft ?t text: ?x)
      (xyz.draft/text of: ?t is: ?x as: :text cardinality: :one in: :session))"#;

/// Every row the selects of `text` answer on `branch`, as text, sorted.
async fn answers<E: Writer>(
    analyzer: &mut Analyzer,
    branch: &Branch,
    operator: &E,
    text: &str,
) -> Result<Vec<String>> {
    let checked = check(analyzer, text);
    let applied = apply(branch, &checked, operator).await?;
    let mut rows = Vec::new();
    for form in applied.answers.forms {
        if let Answer::Rows(answered) = form.answered? {
            rows.extend(answered.iter().map(ToString::to_string));
        }
    }
    rows.sort();
    Ok(rows)
}

/// `text` staged into a transaction of `branch`'s stack and committed.
async fn staged<E: Writer>(
    analyzer: &mut Analyzer,
    branch: &Branch,
    operator: &E,
    text: &str,
) -> Result<()> {
    let stack = branch.stack().perform(operator).await?;
    let checked = check(analyzer, text);
    if let Some(refused) = checked.refusals().next() {
        anyhow::bail!("{text} was refused: {refused:?}");
    }
    let (transaction, _) = stage(stack.transaction(), &checked, operator).await?;
    transaction.commit().publish().perform(operator).await?;
    branch.refresh(operator).await?;
    Ok(())
}

/// A write `in: :session`, a line's and a concept line's, lands in the
/// stack's session layer and not in the primary: the primary's read
/// finds neither, and a read binding `in: ?layer` over the stack finds
/// each in `session:`, beside the note written with no layer, in the
/// primary, `memory:`.
async fn writes_into_a_named_layer_and_reads_the_layer_back<E: Writer>(
    branch: &Branch,
    operator: &E,
) -> Result<()> {
    let mut analyzer = Analyzer::new()?;
    answers(&mut analyzer, branch, operator, CONCEPTS).await?;

    staged(
        &mut analyzer,
        branch,
        operator,
        r#"(assert (note x:1 text: "kept"))
           (assert (xyz.note/text of: x:2 is: "for now" in: :session))
           (assert (draft x:3 text: "a draft"))"#,
    )
    .await?;

    assert_eq!(
        answers(
            &mut analyzer,
            branch,
            operator,
            "(select (xyz.note/text of: ?t is: ?x))"
        )
        .await?,
        vec![r#"(xyz.note/text x:1 "kept")"#],
        "the primary holds the note written with no layer, and not the session's"
    );
    assert_eq!(
        answers(
            &mut analyzer,
            branch,
            operator,
            "(select (xyz.draft/text of: ?t is: ?x))"
        )
        .await?,
        Vec::<String>::new(),
        "a concept line in: :session writes nothing into the primary"
    );

    // What each row was read from, recorded in the primary, where the
    // branch's select can answer it.
    staged(
        &mut analyzer,
        branch,
        operator,
        "(assert (xyz.seen/in of: ?t is: ?l) (xyz.note/text of: ?t is: ?x in: ?l))
         (assert (xyz.seen/in of: ?t is: ?l) (xyz.draft/text of: ?t is: ?x in: ?l))",
    )
    .await?;
    assert_eq!(
        answers(
            &mut analyzer,
            branch,
            operator,
            "(select (xyz.seen/in of: ?t is: ?l))"
        )
        .await?,
        vec![
            "(xyz.seen/in x:1 memory:)".to_string(),
            "(xyz.seen/in x:2 session:)".to_string(),
            "(xyz.seen/in x:3 session:)".to_string(),
        ],
        "each row's layer, bound by in: ?l"
    );
    Ok(())
}

#[dialog_common::test]
async fn it_writes_into_a_named_layer_and_reads_the_layer_back_on_a_volatile_stack() -> Result<()> {
    let (operator, peer) = test_session_with_peer().await;
    let repo = test_repo(&operator, &peer).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    writes_into_a_named_layer_and_reads_the_layer_back(&branch, &operator).await
}

/// On the filesystem provider.
///
/// Native only: the filesystem provider is the native one. In the
/// browser a peer is kept in IndexedDB and OPFS, and the web run of
/// `it_writes_into_a_named_layer_and_reads_the_layer_back_on_a_volatile_stack`
/// pins the same layers there.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn it_writes_into_a_named_layer_and_reads_the_layer_back_on_a_filesystem_stack() -> Result<()>
{
    let root = tempfile::tempdir()?;
    let (operator, branch) = support::filesystem::branch(root.path()).await?;
    writes_into_a_named_layer_and_reads_the_layer_back(&branch, &operator).await
}

#[cfg(not(target_arch = "wasm32"))]
mod support {
    pub mod filesystem;
}

/// A write whose layer is a variable nothing binds is refused when the
/// document is checked, naming the variable, for a write with no
/// premise and for one whose premises bind other variables; nothing is
/// written.
#[dialog_common::test]
async fn it_refuses_a_write_into_a_layer_nothing_binds() -> Result<()> {
    let (operator, peer) = test_session_with_peer().await;
    let repo = test_repo(&operator, &peer).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let mut analyzer = Analyzer::new()?;
    answers(&mut analyzer, &branch, &operator, CONCEPTS).await?;
    let before = branch.revision();
    for text in [
        r#"(assert (xyz.note/text of: x:1 is: "x" in: ?l))"#,
        r#"(assert (xyz.note/text of: ?t is: ?x in: ?l) (note ?t text: ?x))"#,
    ] {
        let checked = check(&mut analyzer, text);
        let refused: Vec<_> = checked.refusals().collect();
        assert!(
            matches!(
                refused.as_slice(),
                [refusal] if matches!(refusal.refusal, Some(Error::Refused(_)))
                    && refusal.message.contains("?l")
            ),
            "{text}: {refused:?}"
        );
        let stack = branch.stack().perform(&operator).await?;
        assert!(
            stage(stack.transaction(), &checked, &operator)
                .await
                .is_err()
        );
    }
    branch.refresh(&operator).await?;
    assert_eq!(branch.revision(), before, "nothing was written");
    Ok(())
}
