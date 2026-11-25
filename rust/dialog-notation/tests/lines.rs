//! What a document writes where the kernel reads a value wrapped in a
//! type, a pick, a cardinality or a layer: a line, `of:`, `is:`, `as:`,
//! `cardinality:` and `in:`, in every position (gaps row 56).

#![allow(deprecated)]

use anyhow::Result;

#[path = "support/session.rs"]
mod session;
use session::Notebook;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

async fn report(notebook: &mut Notebook, text: &str) -> Result<Vec<String>> {
    Ok(notebook.submit(text).await?.0)
}

/// A line is a write's head, at the top level, under a `when` and in a
/// rule's head, and its cardinality decides whether the write succeeds
/// the newest claim.
#[dialog_common::test]
async fn it_writes_a_domain_through_a_line_in_every_write() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(assert (xyz.n/v of: x:1 is: 3 as: :integer cardinality: :one))
               (assert (xyz.n/v of: x:1 is: 4 as: :integer cardinality: :one))"#,
        )
        .await?;
    let read = "(select (xyz.n/v of: ?x is: ?v as: :integer cardinality: :one))";
    assert_eq!(report(&mut notebook, read).await?, vec!["(xyz.n x:1 v: 4)"]);
    notebook
        .submit(
            "(assert (xyz.n/v of: ?x is: 5 as: :integer cardinality: :one)
               (when (xyz.n/v of: ?x is: 4 as: :integer cardinality: :one)))",
        )
        .await?;
    assert_eq!(report(&mut notebook, read).await?, vec!["(xyz.n x:1 v: 5)"]);
    notebook
        .submit(
            "(rule (assert (xyz.n/w of: ?x is: ?v as: :integer cardinality: :one))
               (xyz.n/v of: ?x is: ?v as: :integer cardinality: :one))
             (assert (xyz.n/v of: x:1 is: 6 as: :integer cardinality: :one))",
        )
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (xyz.n/w of: ?x is: ?v as: :integer cardinality: :one))"
        )
        .await?,
        vec!["(xyz.n x:1 w: 6)"]
    );
    Ok(())
}

/// A select's head on a domain is a line, typed as the premise is.
#[dialog_common::test]
async fn it_selects_into_a_line() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit("(assert (xyz.n/v of: x:1 is: 3 as: :integer cardinality: :one))")
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (xyz.out/v of: ?t is: ?v as: :integer)
               (xyz.n/v of: ?t is: ?v as: :integer cardinality: :one))"
        )
        .await?,
        vec!["(xyz.out x:1 v: 3)"]
    );
    Ok(())
}

/// A rule's head deriving a domain is a line, its type the concluded
/// relation's.
#[dialog_common::test]
async fn it_derives_a_domain_through_a_line() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            "(rule (xyz.out/v of: ?t is: ?v as: :integer)
               (xyz.n/v of: ?t is: ?v as: :integer cardinality: :one))
             (assert (xyz.n/v of: x:1 is: 3 as: :integer cardinality: :one))",
        )
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (xyz.out/v of: ?t is: ?v as: :integer))"
        )
        .await?,
        vec!["(xyz.out x:1 v: 3)"]
    );
    assert_eq!(
        report(
            &mut notebook,
            "(select (xyz.out/v of: ?t is: ?v as: :text))"
        )
        .await?,
        Vec::<String>::new()
    );
    Ok(())
}

/// `as:` types a derived attribute's labelled fields by name, a map of
/// label to type, where a keyword types the value under `is:`.
#[dialog_common::test]
async fn it_types_a_derived_attributes_labelled_fields() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (list ?l name: ?n)
                 (xyz.list/name of: ?l is: ?n as: :text cardinality: :one))
               (rule (list/tally ?l status: ?n count: 1 as: {:status :text}) (list ?l name: ?n))
               (assert (list list:home name: "Home"))"#,
        )
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (list/tally ?l status: ?s count: ?c as: {:status :text}))"
        )
        .await?,
        vec![r#"(list/tally list:home count: 1 status: "Home")"#]
    );
    assert_eq!(
        report(
            &mut notebook,
            "(select (list/tally ?l status: ?s count: ?c as: {:status :integer :count :integer}))"
        )
        .await?,
        Vec::<String>::new()
    );
    assert_eq!(
        report(
            &mut notebook,
            "(select (list/tally ?l status: ?s count: ?c as: {:count :integer}))"
        )
        .await?,
        vec![r#"(list/tally list:home count: 1 status: "Home")"#]
    );
    Ok(())
}

/// An elector on a derived attribute's value is written as on a line,
/// `is: (max ?n)`.
#[dialog_common::test]
async fn it_elects_a_derived_attributes_value_as_a_line_does() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            "(concept (list ?l name: ?n)
               (xyz.list/name of: ?l is: ?n as: :text cardinality: :one))
             (rule (list/size ?l ?n)
               (xyz.item/list of: ?i is: ?l cardinality: :one)
               (xyz.item/size of: ?i is: ?n cardinality: :one))
             (assert (xyz.item/list of: item:a is: list:home cardinality: :one))
             (assert (xyz.item/size of: item:a is: 3 cardinality: :one))
             (assert (xyz.item/list of: item:b is: list:home cardinality: :one))
             (assert (xyz.item/size of: item:b is: 7 cardinality: :one))",
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (list/size of: ?l is: (max ?n)))").await?,
        vec!["(list/size list:home is: 7)"]
    );
    assert_eq!(
        report(&mut notebook, "(select (list/size ?l is: (min ?n)))").await?,
        vec!["(list/size list:home is: 3)"]
    );
    Ok(())
}
