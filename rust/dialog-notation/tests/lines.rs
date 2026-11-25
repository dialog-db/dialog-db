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

/// A document writes no wrapper: a value wrapped in a type, a pick, a
/// cardinality or a layer is the kernel's, which the walker writes for a
/// line. Wherever a document writes one, in a write, a `when`, a rule's
/// head or premise, a select's head or premise, `unless` or a concept's
/// body, it is refused as `Error::Refused`, naming the line to write in
/// its place, and the document writes nothing.
#[dialog_common::test]
fn it_refuses_a_wrapper_a_document_writes_naming_the_line() -> Result<()> {
    const LIST: &str = "(concept (list ?l name: ?n)
       (xyz.list/name of: ?l is: ?n as: :text cardinality: :one))";
    for (text, says) in [
        (
            "(assert (xyz.n x:1 v: (:one (:integer 3))))",
            "(xyz.n/v of: x:1 is: 3 as: :integer cardinality: :one)",
        ),
        (
            "(assert (xyz.n ?x v: 5) (when (xyz.n ?x v: (:one (:integer 4)))))",
            "(xyz.n/v of: ?x is: 4 as: :integer cardinality: :one)",
        ),
        (
            "(rule (assert (xyz.n ?x w: (:one (:integer ?v)))) (xyz.n/v of: ?x is: ?v))",
            "(xyz.n/w of: ?x is: ?v as: :integer cardinality: :one)",
        ),
        (
            "(select (xyz.out ?t v: (:integer ?v)) (xyz.n/v of: ?t is: ?v))",
            "(xyz.out/v of: ?t is: ?v as: :integer)",
        ),
        (
            "(rule (xyz.out ?t v: (:integer ?v)) (xyz.n/v of: ?t is: ?v))",
            "(xyz.out/v of: ?t is: ?v as: :integer)",
        ),
        (
            "(select (xyz.n ?x v: (:last (:integer ?v))))",
            "(xyz.n/v of: ?x is: ?v as: :integer cardinality: :one)",
        ),
        (
            "(select (xyz.n ?x v: (:all ?v)))",
            "(xyz.n/v of: ?x is: ?v cardinality: :many)",
        ),
        (
            "(select (xyz.n ?x v: (:max ?v)))",
            "(xyz.n/v of: ?x is: (max ?v))",
        ),
        (
            "(select (xyz.go ?c n: (:control (:many ?n))))",
            "(xyz.go/n of: ?c is: ?n cardinality: :many in: :control)",
        ),
        (
            "(select (xyz.todo ?t state: (:one (:among (:entity ?s) cases: [:open :done]))))",
            "(xyz.todo/state of: ?t is: ?s as: [:open :done] cardinality: :one)",
        ),
        (
            "(select (xyz.todo ?t state: (:among ?s cases: [:open :done])))",
            "(xyz.todo/state of: ?t is: ?s as: [:open :done])",
        ),
        (
            "(select (xyz.n/v of: ?x is: (:integer ?v)))",
            "(xyz.n/v of: ?x is: ?v as: :integer)",
        ),
        (
            "(select (xyz.n ?x v: ?v) (unless (xyz.n ?x w: (:integer 3))))",
            "(xyz.n/w of: ?x is: 3 as: :integer)",
        ),
        (
            "(concept (todo ?t text: ?x) (xyz.todo ?t text: (:last (:text ?x))))",
            "(xyz.todo/text of: ?t is: ?x as: :text cardinality: :one)",
        ),
        (
            "(rule (list/tally ?l status: (:text ?n)) (list ?l name: ?n))",
            "as: {:status :text}",
        ),
        (
            "(select (list/tally ?l status: (:text ?s)))",
            "as: {:status :text}",
        ),
        ("(select (list/size ?l (:max ?n)))", "is: (max ?n)"),
        (
            "(select (list/size ?l is: (:integer ?n)))",
            "is: ?n as: :integer",
        ),
        (
            "(select (list/size ?l (:one ?n)))",
            "a derived attribute's cardinality is what its rules conclude",
        ),
    ] {
        let mut analyzer = dialog_notation::Analyzer::new()?;
        let document = format!("{LIST}\n{text}");
        let checked = dialog_notation::check(&mut analyzer, &document);
        let refusals: Vec<_> = checked.refusals().collect();
        assert_eq!(refusals.len(), 1, "{text}: {:?}", checked.diagnostics());
        assert!(
            matches!(
                refusals[0].refusal,
                Some(dialog_notation::Error::Refused(_))
            ),
            "{text}: {:?}",
            refusals[0]
        );
        assert!(
            refusals[0].message.contains(says),
            "{text}: says {:?}, not {says:?}",
            refusals[0].message
        );
        assert!(
            refusals[0].message.contains("a document writes no wrapper"),
            "{text}: {:?}",
            refusals[0].message
        );
        assert!(checked.steps().is_empty(), "{text} writes nothing");
    }
    Ok(())
}

/// `as:` on a derived attribute takes a listed type's cases as a line's
/// does, `as: [:open :done]`, and an elector ranks by the listing.
#[dialog_common::test]
async fn it_lists_a_derived_attributes_cases_as_a_line_does() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            "(concept (list ?l name: ?n)
               (xyz.list/name of: ?l is: ?n as: :text cardinality: :one))
             (rule (list/state ?l ?s)
               (list ?l name: ?n)
               (xyz.item/list of: ?i is: ?l cardinality: :one)
               (xyz.item/state of: ?i is: ?s cardinality: :one))
             (assert (list list:home name: \"Home\"))
             (assert (xyz.item/list of: item:a is: list:home cardinality: :one))
             (assert (xyz.item/state of: item:a is: :open cardinality: :one))
             (assert (xyz.item/list of: item:b is: list:home cardinality: :one))
             (assert (xyz.item/state of: item:b is: :done cardinality: :one))",
        )
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (list/state of: ?l is: (max ?s) as: [:done :open]))"
        )
        .await?,
        vec!["(list/state list:home is: :open)"]
    );
    assert_eq!(
        report(
            &mut notebook,
            "(select (list/state of: ?l is: (max ?s) as: [:open :done]))"
        )
        .await?,
        vec!["(list/state list:home is: :done)"]
    );
    assert_eq!(
        report(
            &mut notebook,
            "(select (list/state of: ?l is: ?s as: {:is [:open :done]}))"
        )
        .await?,
        vec![
            "(list/state list:home is: :done)",
            "(list/state list:home is: :open)"
        ]
    );
    Ok(())
}

/// The walker still writes every wrapper for a line, and the kernel still
/// reads each: a type, a cardinality, an elector, a listed type's cases
/// and a layer each come out of a line wrapped and compile. A kernel tree
/// built through the Rust API is no document, and compiles with them.
#[dialog_common::test]
fn it_still_reads_every_wrapper_the_walker_writes() -> Result<()> {
    for (line, wrapped) in [
        ("(xyz.n/v of: ?x is: ?v as: :integer)", "v: (:integer ?v)"),
        ("(xyz.n/v of: ?x is: ?v cardinality: :one)", "v: (:one ?v)"),
        ("(xyz.n/v of: ?x is: (max ?v))", "v: (:max ?v)"),
        (
            "(xyz.n/v of: ?x is: ?v as: [:a :b])",
            "v: (:among (:entity ?v) cases: [:a :b])",
        ),
        (
            "(xyz.n/v of: ?x is: ?v cardinality: :many in: :control)",
            "v: (:control (:many ?v))",
        ),
    ] {
        let mut analyzer = dialog_notation::Analyzer::new()?;
        let checked = dialog_notation::check(&mut analyzer, &format!("(select {line})"));
        assert!(
            checked.refusals().next().is_none(),
            "{line}: {:?}",
            checked.diagnostics()
        );
        let [tree] = checked.analyzed() else {
            panic!("{line}: one tree, {:?}", checked.analyzed());
        };
        assert!(tree.to_string().contains(wrapped), "{line}: {tree}");
        assert!(dialog_notation::compile(tree).is_ok(), "{line}: {tree}");
    }
    let built = dialog_notation::parse(
        "(rule (report (xyz.n ?x v: (:one (:integer ?v)))) (xyz.n ?x v: (:one (:integer ?v))))",
    )?
    .remove(0);
    assert!(dialog_notation::compile(&built).is_ok());
    Ok(())
}
