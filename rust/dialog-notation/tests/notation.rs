//! Documents in the notation, run on a real branch.
//!
//! Each test opens a repository, submits documents, and reads the
//! report back. Nothing here stands in for Dialog-DB: a concept
//! application is a concept query, a rule is a rule the branch stores
//! and fires, and a command asserted is a dispatched transient.
//!
//! Each test is the one writer of its branch, so it publishes with the
//! call that fails when a commit loses a race, which is deprecated for
//! `Branch::transact`.

#![allow(deprecated)]

use anyhow::Result;
use dialog_corpus::Value;
use dialog_notation::{Error, Signature, Syntax, read};
use dialog_query::concept::descriptor::ConceptStatement;
use dialog_query::query::Output as _;
use dialog_query::{Any, AttributeDescriptor, ConceptConclusion, ConceptQuery, Term};
use serde_json::json;

#[path = "../examples/notebook/session.rs"]
mod session;
use session::Notebook;

#[path = "../examples/notebook/bar.rs"]
mod bar;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// The report of a document.
async fn report(notebook: &mut Notebook, text: &str) -> Result<Vec<String>> {
    Ok(notebook.submit(text).await?.0)
}

const TODO: &str = r#"
    (concept (todo ?t list: ?l text: ?x state: ?s)
      (xyz.todo ?t list: (:last (:entity ?l))
                   text: (:last (:text ?x))
                   state: (:last (:entity ?s))))

    ; Adding a to-do, and completing one: commands, a concept each
    ; whose head names no entity, its lines on the control layer.
    (concept (todo/add todo: ?t list: ?l text: ?x)
      (xyz.todo.add/todo of: ?c is: ?t as: :entity in: :control)
      (xyz.todo.add/list of: ?c is: ?l as: :entity in: :control)
      (xyz.todo.add/text of: ?c is: ?x as: :text in: :control))
    (concept (todo/complete todo: ?t)
      (xyz.todo.complete/todo of: ?c is: ?t as: :entity in: :control))

    ; Adding a to-do opens it.
    (rule (assert (todo ?t list: ?l text: ?x state: :open))
      (todo/add todo: ?t list: ?l text: ?x))

    ; A write says every field, so one that changes the state alone
    ; writes the relation, not the concept.
    (rule (assert (xyz.todo ?t state: (:last (:entity :done))))
      (todo/complete todo: ?t))
"#;

/// A concept is defined, written through by a rule a command fires, and
/// read back through the same concept.
#[dialog_common::test]
async fn it_defines_a_concept_and_runs_a_command_through_it() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook.submit(TODO).await?;
    assert!(diagnostics.is_empty());

    notebook
        .submit(
            r#"(assert (todo/add todo: todo:1 list: list:home text: "Buy milk"))
               (assert (todo/add todo: todo:2 list: list:home text: "Call mum"))
               (assert (todo/add todo: todo:3 list: list:work text: "Ship it"))"#,
        )
        .await?;

    assert_eq!(
        report(&mut notebook, "(select (todo ?t text: ?x state: ?s ...))").await?,
        vec![
            r#"(xyz.todo todo:1 state: :open text: "Buy milk")"#,
            r#"(xyz.todo todo:2 state: :open text: "Call mum")"#,
            r#"(xyz.todo todo:3 state: :open text: "Ship it")"#,
        ]
    );

    notebook
        .submit("(assert (todo/complete todo: todo:2))")
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (todo ?t text: ?x list: list:home state: :open))"
        )
        .await?,
        vec![r#"(xyz.todo todo:1 list: list:home state: :open text: "Buy milk")"#],
        "a row lists its fields by label: a tree keeps no order of them"
    );

    // The command held for one round and is not on the branch: read
    // as any concept is, it has no rows.
    assert!(
        report(&mut notebook, "(select (todo/add todo: ?t text: ?x ...))")
            .await?
            .is_empty()
    );
    Ok(())
}

/// The order of a document's forms does not matter: here the commands
/// are issued and the rules written above the concepts they use. A
/// second document relies on what the first installed.
#[dialog_common::test]
async fn it_runs_a_document_whose_uses_stand_above_its_declarations() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (rows, diagnostics) = notebook
        .submit(
            r#"(assert (todo/add todo: todo:1 list: list:home text: "Buy milk"))
               (rule (assert (todo ?t list: ?l text: ?x state: :open))
                 (todo/add todo: ?t list: ?l text: ?x))
               (select (xyz.todo ?t text: (:last ?x)))
               (concept (todo/add todo: ?t list: ?l text: ?x)
                 (xyz.todo.add/todo of: ?c is: ?t as: :entity in: :control)
                 (xyz.todo.add/list of: ?c is: ?l as: :entity in: :control)
                 (xyz.todo.add/text of: ?c is: ?x as: :text in: :control))
               (concept (todo ?t list: ?l text: ?x state: ?s)
                 (xyz.todo/list of: ?t is: ?l as: :entity cardinality: :one)
                 (xyz.todo/text of: ?t is: ?x as: :text cardinality: :one)
                 (xyz.todo/state of: ?t is: ?s as: [:open :done] cardinality: :one))"#,
        )
        .await?;
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    assert_eq!(rows, vec![r#"(xyz.todo todo:1 text: "Buy milk")"#]);

    // The next document uses the concept, the command and the rule the
    // first one installed.
    notebook
        .submit(r#"(assert (todo/add todo: todo:2 list: list:home text: "Call mum"))"#)
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (todo ?t text: ?x state: :open ...))"
        )
        .await?,
        vec![
            r#"(xyz.todo todo:1 state: :open text: "Buy milk")"#,
            r#"(xyz.todo todo:2 state: :open text: "Call mum")"#,
        ]
    );
    Ok(())
}

/// A command's rule makes the entity of what it creates with `id`: the
/// same list and text make one to-do, on every device, and it is the
/// entity `(:id xyz.todo list: … text: …)` names.
#[dialog_common::test]
async fn it_makes_an_entity_with_id_in_a_rule() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook
        .submit(
            r#"(concept (todo ?t list: ?l text: ?x state: ?s)
                 (xyz.todo/list of: ?t is: ?l as: :entity cardinality: :one)
                 (xyz.todo/text of: ?t is: ?x as: :text cardinality: :one)
                 (xyz.todo/state of: ?t is: ?s as: [:open :done] cardinality: :one))
               (concept (todo/add list: ?l text: ?x)
                 (xyz.todo.add/list of: ?c is: ?l as: :entity in: :control)
                 (xyz.todo.add/text of: ?c is: ?x as: :text in: :control))
               (rule (assert (todo ?t list: ?l text: ?x state: :open))
                 (todo/add list: ?l text: ?x)
                 (id ?t list: ?l text: ?x))"#,
        )
        .await?;
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    for _ in 0..2 {
        notebook
            .submit(r#"(assert (todo/add list: list:home text: "Buy milk"))"#)
            .await?;
    }
    let todos = report(&mut notebook, "(select (todo ?t text: ?x ...))").await?;
    assert_eq!(todos.len(), 1, "{todos:?}");
    assert!(todos[0].starts_with("(xyz.todo id:"), "{todos:?}");

    notebook
        .submit(
            r#"(assert (xyz.todo (:id xyz.todo list: list:home text: "Buy milk") tag: (:text "shop")))"#,
        )
        .await?;
    let tagged = report(&mut notebook, "(select (xyz.todo ?t tag: ?g))").await?;
    let entity = |row: &str| row.split_whitespace().nth(1).map(str::to_string);
    assert_eq!(entity(&tagged[0]), entity(&todos[0]), "{tagged:?}");
    Ok(())
}

const COLLECTIONS: &str = "
    (concept (notebook-entry ?b block: {?p ?x})
      (xyz.tonk.notebook/?p of: ?b is: ?x as: :entity cardinality: :one by: :position))
    (concept (element-method ?e source: {?n ?s})
      (xyz.tonk.element.method/?n of: ?e is: ?s as: :text cardinality: :one by: :name))";

/// A collection's entries are its attributes: a line over one leaves
/// the key a variable in the name. Each entry is asserted by its key,
/// read a row a key, read and written by rules, and a key of the wrong
/// shape is refused. A retract of a collection is not decided.
#[dialog_common::test]
async fn it_reads_and_writes_the_entries_of_a_collection() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook
        .submit(&format!(
            r#"{COLLECTIONS}
               (assert (notebook-entry notebook:1 block: {{"N1" block:a}}))
               (assert (notebook-entry notebook:1 block: {{"N2" block:b}}))
               (assert (element-method element:x source: {{"render" "return 1"}}))
               (assert (element-method element:x source: {{"update" "return 2"}}))"#
        ))
        .await?;
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    assert_eq!(
        report(
            &mut notebook,
            "(select (notebook-entry notebook:1 block: {?p ?x}))"
        )
        .await?,
        vec![
            "(xyz.tonk.notebook notebook:1 N1: block:a)",
            "(xyz.tonk.notebook notebook:1 N2: block:b)",
        ]
    );
    assert_eq!(
        report(
            &mut notebook,
            r#"(select (element-method ?e source: {"update" ?s}))"#
        )
        .await?,
        vec![r#"(xyz.tonk.element.method element:x update: "return 2")"#]
    );
    // One entry written by its attribute, `by:` on the line, as tonk's
    // element writes its methods; and read the same way.
    notebook
        .submit(
            r#"(assert (xyz.tonk.element.method/released of: element:x is: "return 3"
                  as: :text cardinality: :one by: :name))"#,
        )
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (xyz.tonk.element.method/released of: ?e is: ?s as: :text cardinality: :one by: :name))"
        )
        .await?,
        vec![r#"(xyz.tonk.element.method element:x released: "return 3")"#]
    );
    // One entry, by its key.
    assert_eq!(
        report(
            &mut notebook,
            r#"(select (notebook-entry notebook:1 block: {"N2" ?x}))"#
        )
        .await?,
        vec!["(xyz.tonk.notebook notebook:1 N2: block:b)"]
    );

    // A rule reads every entry, its key bound, and one writes an entry
    // under a key its premises bind.
    notebook
        .submit(
            r#"(rule (xyz.placed ?x at: ?p) (notebook-entry ?b block: {?p ?x}))
               (rule (assert (notebook-entry ?b block: {?p ?x}))
                 (xyz.move ?x to: ?b at: ?p))
               (assert (xyz.move block:c to: notebook:1 at: (:text "N3")))"#,
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (xyz.placed ?x at: ?p))").await?,
        vec![
            r#"(xyz.placed block:a at: "N1")"#,
            r#"(xyz.placed block:b at: "N2")"#,
            r#"(xyz.placed block:c at: "N3")"#,
        ]
    );

    // Two entries of one sequence, their keys compared: an entry is
    // stray when another of its notebook's entries sorts first.
    notebook
        .submit(
            "(rule (xyz.stray ?x at: ?p)
               (notebook-entry ?b block: {?p ?x})
               (notebook-entry ?b block: {?q ?y})
               (< ?q : ?p))",
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (xyz.stray ?x at: ?p))").await?,
        vec![
            r#"(xyz.stray block:b at: "N2")"#,
            r#"(xyz.stray block:c at: "N3")"#,
        ]
    );

    let why = |result: Result<(Vec<String>, Vec<dialog_notation::Diagnostic>)>| {
        result.map_err(|error| error.downcast::<Error>())
    };
    let shape = notebook
        .submit(r#"(assert (notebook-entry notebook:1 block: {"first" block:d}))"#)
        .await;
    assert!(matches!(why(shape), Err(Ok(Error::Refused(said))) if said.contains("the key")),);
    // A retract of one entry, its entity, key and value written,
    // retracts that entry, and its neighbours stay. One that leaves out
    // the key or the value is not decided.
    for undecided in [
        "(retract (notebook-entry notebook:1 block: {_ block:b}))",
        "(retract (notebook-entry notebook:1))",
    ] {
        let refused = notebook.submit(undecided).await;
        assert!(
            matches!(why(refused), Err(Ok(Error::Unsupported(said))) if said.contains("leaves out the key or the value")),
            "{undecided}"
        );
    }
    notebook
        .submit(r#"(retract (notebook-entry notebook:1 block: {"N1" block:a}))"#)
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (notebook-entry notebook:1 block: {?p ?x}))"
        )
        .await?,
        vec![
            "(xyz.tonk.notebook notebook:1 N2: block:b)",
            "(xyz.tonk.notebook notebook:1 N3: block:c)",
        ]
    );
    // And in a rule: a placement takes every other entry of its block
    // out, as the notebook's rule does.
    notebook
        .submit(
            r#"(concept (block/place subject: ?b notebook: ?n key: ?k)
                 (xyz.block.place/subject of: ?c is: ?b as: :entity in: :control)
                 (xyz.block.place/notebook of: ?c is: ?n as: :entity in: :control)
                 (xyz.block.place/key of: ?c is: ?k as: :text in: :control))
               (rule (assert (notebook-entry ?n block: {?k ?b}))
                 (block/place subject: ?b notebook: ?n key: ?k))
               (rule (retract (notebook-entry ?n block: {?old ?b}))
                 (block/place subject: ?b notebook: ?n key: ?k)
                 (notebook-entry ?n block: {?old ?b})
                 (!= ?old is: ?k))"#,
        )
        .await?;
    notebook
        .submit(r#"(assert (block/place subject: block:b notebook: notebook:1 key: "N4"))"#)
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (notebook-entry notebook:1 block: {?p ?x}))"
        )
        .await?,
        vec![
            "(xyz.tonk.notebook notebook:1 N3: block:c)",
            "(xyz.tonk.notebook notebook:1 N4: block:b)",
        ]
    );
    Ok(())
}

/// A text that spans lines is kept and read back as written, newlines,
/// quotes and backslashes in it: a method's source, as tonk's elements
/// hold them.
#[dialog_common::test]
async fn it_keeps_a_text_that_spans_lines() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(assert (xyz.el el:1 source: "render() {
  return `<p class=\"a\">\\n</p>`;
}"))"#,
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (xyz.el ?e source: ?s))").await?,
        vec![
            r#"(xyz.el el:1 source: "render() {
  return `<p class=\"a\">\\n</p>`;
}")"#
        ]
    );
    Ok(())
}

/// A write at the top level with a `when` runs once, over what the
/// `when` matches as the branch stood, and is never installed: a to-do
/// added after it is not touched. Under `:one` the write succeeds the
/// claim the cell holds, as any `last` write on main does.
#[dialog_common::test]
async fn it_writes_once_what_a_when_matches() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (todo ?t text: ?x state: ?s)
                 (xyz.todo/text of: ?t is: ?x as: :text cardinality: :one)
                 (xyz.todo/state of: ?t is: ?s as: [:open :done] cardinality: :one))
               (assert (todo todo:1 text: "Call mom" state: :open))
               (assert (todo todo:2 text: "Buy milk" state: :open))"#,
        )
        .await?;
    notebook
        .submit(
            r#"(assert (xyz.todo/state of: ?t is: :done as: :entity cardinality: :one)
                 (when (todo ?t text: "Call mom" ...)))"#,
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (todo ?t text: ?x state: ?s))").await?,
        vec![
            r#"(xyz.todo todo:1 state: :done text: "Call mom")"#,
            r#"(xyz.todo todo:2 state: :open text: "Buy milk")"#,
        ]
    );
    // Every claim the cell holds: the write succeeded the one it read.
    let claims = report(&mut notebook, "(select (xyz.todo todo:1 state: ?s))").await?;
    assert_eq!(claims, vec!["(xyz.todo todo:1 state: :done)"]);

    // Never installed: a to-do the `when` would match, added later, is
    // left as it is.
    notebook
        .submit(r#"(assert (todo todo:3 text: "Call mom" state: :open))"#)
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (todo todo:3 state: ?s ...))").await?,
        vec!["(xyz.todo todo:3 state: :open)"]
    );

    // A retract with a `when`, of what it matched.
    notebook
        .submit(
            r#"(retract (xyz.todo/text of: ?t is: ?x as: :text cardinality: :one)
                 (when (todo ?t text: ?x state: :done)))"#,
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (todo ?t text: ?x ...))").await?,
        vec![
            r#"(xyz.todo todo:2 text: "Buy milk")"#,
            r#"(xyz.todo todo:3 text: "Call mom")"#,
        ]
    );

    let why = |result: Result<(Vec<String>, Vec<dialog_notation::Diagnostic>)>| {
        result.map_err(|error| error.downcast::<Error>())
    };
    let unbound = notebook
        .submit(r#"(assert (xyz.todo/tag ?t ?g) (when (todo ?t text: "Buy milk" ...)))"#)
        .await;
    assert!(
        matches!(why(unbound), Err(Ok(Error::Refused(said))) if said.contains("is bound by it, and ?g is not"))
    );
    let command = notebook
        .submit(
            r#"(concept (todo/ping todo: ?t)
                 (xyz.todo.ping/todo of: ?c is: ?t as: :entity in: :control))
               (assert (todo/ping todo: ?t) (when (todo ?t text: "Buy milk" ...)))"#,
        )
        .await;
    assert!(
        matches!(why(command), Err(Ok(Error::Unsupported(said))) if said.contains("whether a command at the top level keeps"))
    );
    Ok(())
}

/// A parameter a read leaves out matches anything, in a query and in a
/// premise, as if `...` were written: the field is still read, so an
/// entity that holds no claim of it is no instance. A write through a
/// concept says every field.
#[dialog_common::test]
async fn it_reads_a_concept_with_a_parameter_left_out() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (todo ?t text: ?x state: ?s)
                 (xyz.todo/text of: ?t is: ?x as: :text cardinality: :one)
                 (xyz.todo/state of: ?t is: ?s as: [:open :done] cardinality: :one))
               (assert (todo todo:1 text: "Call mom" state: :open))
               (assert (todo todo:2 text: "Buy milk" state: :done))
               (assert (xyz.todo/text todo:3 "No state"))
               (rule (xyz.said ?t text: ?x) (todo ?t text: ?x))"#,
        )
        .await?;
    let both = vec![
        r#"(xyz.todo todo:1 text: "Call mom")"#,
        r#"(xyz.todo todo:2 text: "Buy milk")"#,
    ];
    assert_eq!(
        report(&mut notebook, "(select (todo ?t text: ?x))").await?,
        both
    );
    assert_eq!(
        report(&mut notebook, "(select (todo ?t text: ?x ...))").await?,
        both
    );
    assert_eq!(
        report(&mut notebook, "(select (xyz.said ?t text: ?x))").await?,
        vec![
            r#"(xyz.said todo:1 text: "Call mom")"#,
            r#"(xyz.said todo:2 text: "Buy milk")"#,
        ]
    );
    let partial = notebook
        .submit(r#"(assert (todo todo:4 text: "Half"))"#)
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&partial, Err(Ok(Error::Refused(why))) if why.contains("a write says every field")),
        "{partial:?}"
    );
    Ok(())
}

/// Equality and inequality stand as premises, `(== ?a is: ?b)` and
/// `(!= ?a is: ?b)`, as the design spells them. `!=` is the absence of
/// `==`.
#[dialog_common::test]
async fn it_compares_with_equality_and_inequality() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(assert (xyz.pair pair:1 left: "a" right: "a"))
               (assert (xyz.pair pair:2 left: "a" right: "b"))
               (rule (xyz.same ?p left: ?l)
                 (xyz.pair ?p left: ?l right: ?r)
                 (== ?l is: ?r))
               (rule (xyz.other ?p left: ?l)
                 (xyz.pair ?p left: ?l right: ?r)
                 (!= ?l is: ?r))"#,
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (xyz.same ?p left: ?l))").await?,
        vec![r#"(xyz.same pair:1 left: "a")"#]
    );
    assert_eq!(
        report(&mut notebook, "(select (xyz.other ?p left: ?l))").await?,
        vec![r#"(xyz.other pair:2 left: "a")"#]
    );
    let twice = notebook
        .submit(
            "(select (xyz.pair ?p left: ?l)
               (xyz.pair ?p left: ?l right: ?r)
               (unless (!= ?l is: ?r)))",
        )
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&twice, Err(Ok(Error::Unsupported(why))) if why.contains("`!=` inside what must not hold")),
        "{twice:?}"
    );
    Ok(())
}

/// `dialog/position` makes the key of an entry between two others, so
/// an entry written between two sorts between them.
#[dialog_common::test]
async fn it_places_an_entry_between_two_with_dialog_position() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(&format!(
            r#"{COLLECTIONS}
               (concept (block/insert block: ?x notebook: ?b after: ?lo before: ?hi)
                 (xyz.block.insert/block of: ?c is: ?x as: :entity in: :control)
                 (xyz.block.insert/notebook of: ?c is: ?b as: :entity in: :control)
                 (xyz.block.insert/after of: ?c is: ?lo as: :text in: :control)
                 (xyz.block.insert/before of: ?c is: ?hi as: :text in: :control))
               (rule (assert (notebook-entry ?b block: {{?at ?x}}))
                 (block/insert block: ?x notebook: ?b after: ?lo before: ?hi)
                 (dialog/position ?x after: ?lo before: ?hi is: ?at))
               (rule (xyz.between ?x at: ?k)
                 (notebook-entry ?b block: {{?k ?x}})
                 (notebook-entry ?b block: {{?lo block:a}})
                 (notebook-entry ?b block: {{?hi block:c}})
                 (< ?lo : ?k)
                 (< ?k : ?hi))
               (assert (block/insert block: block:a notebook: notebook:1 after: "" before: ""))"#
        ))
        .await?;
    let first = report(
        &mut notebook,
        "(select (notebook-entry notebook:1 block: {?k block:a}))",
    )
    .await?;
    assert_eq!(first.len(), 1, "{first:?}");
    let key = |row: &str| {
        row.split_whitespace()
            .nth(2)
            .and_then(|key| key.strip_suffix(':'))
            .map(str::to_string)
            .unwrap_or_default()
    };
    let a = key(&first[0]);
    notebook
        .submit(&format!(
            r#"(assert (block/insert block: block:c notebook: notebook:1 after: "{a}" before: ""))"#
        ))
        .await?;
    let last = report(
        &mut notebook,
        "(select (notebook-entry notebook:1 block: {?k block:c}))",
    )
    .await?;
    let c = key(&last[0]);
    assert!(a < c, "{a} {c}");
    notebook
        .submit(&format!(
            r#"(assert (block/insert block: block:b notebook: notebook:1 after: "{a}" before: "{c}"))"#
        ))
        .await?;
    let between = report(&mut notebook, "(select (xyz.between ?x at: ?k))").await?;
    assert_eq!(between.len(), 1, "{between:?}");
    assert!(
        between[0].starts_with("(xyz.between block:b at: "),
        "{between:?}"
    );
    Ok(())
}

/// A rule reads a command's own entity with `of:` in the premise that
/// reads the command: the entity its content derived, here the entity
/// of the to-do it makes. Asserting a command with `of:` gives it no
/// entity: the definition is for a premise only.
#[dialog_common::test]
async fn it_reads_the_entity_of_the_command_that_fired() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (todo ?t list: ?l text: ?x)
                 (xyz.todo/list of: ?t is: ?l as: :entity cardinality: :one)
                 (xyz.todo/text of: ?t is: ?x as: :text cardinality: :one))
               (concept (todo/add list: ?l text: ?x)
                 (xyz.todo.add/list of: ?c is: ?l as: :entity in: :control)
                 (xyz.todo.add/text of: ?c is: ?x as: :text in: :control))
               (rule (assert (todo ?c list: ?l text: ?x))
                 (todo/add of: ?c list: ?l text: ?x))"#,
        )
        .await?;
    notebook
        .submit(r#"(assert (todo/add list: list:home text: "Buy milk"))"#)
        .await?;
    let todos = report(&mut notebook, "(select (todo ?t text: ?x))").await?;
    assert_eq!(todos.len(), 1, "{todos:?}");
    // The command's entity is what its content derives.
    notebook
        .submit(
            r#"(assert (xyz.todo (:id xyz.todo.add list: list:home text: "Buy milk") tag: (:text "made")))"#,
        )
        .await?;
    let tagged = report(&mut notebook, "(select (xyz.todo ?t tag: ?g))").await?;
    let entity = |row: &str| row.split_whitespace().nth(1).map(str::to_string);
    assert_eq!(
        entity(&tagged[0]),
        entity(&todos[0]),
        "{tagged:?} {todos:?}"
    );

    let given = notebook
        .submit(r#"(assert (todo/add of: todo:9 list: list:home text: "x"))"#)
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(matches!(&given, Err(Ok(Error::Unknown(_)))), "{given:?}");
    Ok(())
}

/// A view compiles to its plan, which nothing writes: a document that
/// holds one is written, its view left to whoever renders it. The
/// description is read from the analyzed tree.
#[dialog_common::test]
async fn it_compiles_a_view_to_its_plan_and_describes_it() -> Result<()> {
    let document = r#"(concept (todo ?t title: ?title)
                        (xyz.todo/title of: ?t is: ?title as: :text cardinality: :one))
                      (assert (todo todo:1 title: "Buy milk"))
                      (view todo :row [:li ?title] (todo ?t title: ?title))"#;
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook.submit(document).await?;
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    assert_eq!(
        report(&mut notebook, "(select (xyz.todo ?t title: ?x))").await?,
        vec![r#"(xyz.todo todo:1 title: "Buy milk")"#]
    );
    let checked = dialog_notation::check(&mut dialog_notation::Analyzer::new()?, document);
    let plans: Vec<&dialog_notation::Plan> = checked
        .steps
        .iter()
        .filter_map(|step| match step {
            dialog_notation::Step::View(plan) => Some(&**plan),
            _ => None,
        })
        .collect();
    let [plan] = plans.as_slice() else {
        panic!("one view: {:?}", checked.steps);
    };
    assert_eq!((plan.concept.as_str(), plan.name.as_str()), ("todo", "row"));
    assert_eq!(plan.layers.len(), 1);
    let analysis = dialog_notation::Analyzer::new()?.analyze(&dialog_notation::parse(document)?);
    let view = analysis
        .analyzed
        .iter()
        .find(|tree| tree.applies() == Some("view"))
        .map(dialog_notation::View::describe)
        .transpose()?
        .expect("a view");
    assert_eq!(
        view.template,
        dialog_notation::Node::Element(dialog_notation::Element {
            tag: "li".into(),
            attributes: Vec::new(),
            events: Vec::new(),
            children: vec![dialog_notation::Node::Variable("title".into())],
        })
    );
    Ok(())
}

/// What a checked document declares comes back as data: the to-do
/// example's concept and its three commands, each with the comment above
/// it, its lines with theirs, whether it is a command, and the
/// descriptor a read of it compiles to.
#[dialog_common::test]
async fn it_gives_what_a_document_declares_as_data() -> Result<()> {
    let mut analyzer = dialog_notation::Analyzer::new()?;
    let checked = dialog_notation::check(&mut analyzer, include_str!("../examples/todo.clj"));
    assert_eq!(checked.refusals().count(), 0);
    let concepts: Vec<&dialog_notation::Concept> = checked
        .declarations
        .iter()
        .filter_map(|declared| match declared {
            dialog_notation::Declaration::Concept(concept) => Some(concept),
            _ => None,
        })
        .collect();
    assert_eq!(
        concepts
            .iter()
            .map(|concept| (concept.name.as_str(), concept.command))
            .collect::<Vec<_>>(),
        vec![
            ("todo", false),
            ("todo/add", true),
            ("todo/complete", true),
            ("list/archive", true),
        ]
    );
    let todo = concepts[0];
    assert!(
        todo.description
            .as_deref()
            .is_some_and(|said| said.starts_with("Something to do, on a list.")),
        "{:?}",
        todo.description
    );
    assert_eq!(todo.entity.as_deref(), Some("t"));
    assert_eq!(
        todo.lines
            .iter()
            .map(|line| (
                line.attribute.as_str(),
                line.cardinality.as_deref(),
                line.layer.as_deref()
            ))
            .collect::<Vec<_>>(),
        vec![
            ("xyz.todo/list", Some("one"), None),
            ("xyz.todo/text", Some("one"), None),
            ("xyz.todo/state", Some("one"), None),
        ]
    );
    assert_eq!(todo.descriptors.len(), 1);
    let add = concepts[1];
    assert_eq!(
        add.description.as_deref(),
        Some("Adding a to-do, to a list.")
    );
    assert_eq!(add.entity, None);
    assert_eq!(
        add.lines
            .iter()
            .map(|line| (
                line.attribute.as_str(),
                line.cardinality.as_deref(),
                line.layer.as_deref()
            ))
            .collect::<Vec<_>>(),
        vec![
            ("xyz.todo.add/list", None, Some("control")),
            ("xyz.todo.add/text", None, Some("control")),
        ]
    );
    assert_eq!(add.descriptors.len(), 1);

    // A line's description is the comment above it. A view is declared
    // beside the concepts, though Dialog-DB's compiler refuses it.
    let checked = dialog_notation::check(
        &mut dialog_notation::Analyzer::new()?,
        "; A note.
(concept (note ?n text: ?x)
  ; What it says.
  (xyz.note/text of: ?n is: ?x as: :text))

; One note, as a row.
(view note :row [:li ?x] (note ?n text: ?x))",
    );
    let [
        dialog_notation::Declaration::Concept(note),
        dialog_notation::Declaration::View { description, view },
    ] = checked.declarations.as_slice()
    else {
        panic!("a concept and a view: {:?}", checked.declarations);
    };
    assert_eq!(note.lines[0].description.as_deref(), Some("What it says."));
    assert_eq!(note.lines[0].cardinality, None);
    assert_eq!(description.as_deref(), Some("One note, as a row."));
    assert_eq!(view.name, "row");
    Ok(())
}

/// A retract through a concept by its entity alone retracts every field
/// of the concept on that entity, at the top level and in a rule, and
/// another entity's fields stay. One that names some fields and not
/// others is refused as a write that leaves fields out.
#[dialog_common::test]
async fn it_retracts_every_field_of_a_concept_named_by_its_entity() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (notebook/block ?b notebook: ?n source: ?s)
                 (xyz.block/notebook of: ?b is: ?n as: :entity cardinality: :one)
                 (xyz.block/source of: ?b is: ?s as: :text cardinality: :one))
               (concept (block/remove subject: ?b)
                 (xyz.block.remove/subject of: ?c is: ?b as: :entity in: :control))
               (rule (retract (notebook/block ?b))
                 (block/remove subject: ?b))
               (assert (notebook/block block:1 notebook: notebook:1 source: "one"))
               (assert (notebook/block block:2 notebook: notebook:1 source: "two"))
               (assert (notebook/block block:3 notebook: notebook:1 source: "three"))"#,
        )
        .await?;
    notebook
        .submit("(retract (notebook/block block:1))")
        .await?;
    notebook
        .submit("(assert (block/remove subject: block:2))")
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (xyz.block ?b notebook: ?n))").await?,
        vec!["(xyz.block block:3 notebook: notebook:1)"]
    );
    assert_eq!(
        report(&mut notebook, "(select (xyz.block ?b source: ?s))").await?,
        vec![r#"(xyz.block block:3 source: "three")"#]
    );
    let partial = notebook
        .submit(r#"(retract (notebook/block block:3 source: "three"))"#)
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&partial, Err(Ok(Error::Refused(why))) if why.contains("a write says every field")),
        "{partial:?}"
    );
    Ok(())
}

/// A concept written as lines, an attribute a line, runs as one written
/// as a read of several fields does: written through by a rule a
/// command fires and read back. `:one` reads the newest claim and
/// `:many` every claim.
#[dialog_common::test]
async fn it_runs_a_concept_written_as_lines() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook
        .submit(
            r#"(concept (todo ?i list: ?l title: ?t state: ?s)
                 (xyz.todo/title of: ?i is: ?t as: :text cardinality: :one)
                 (xyz.todo/state of: ?i is: ?s as: [:open :done] cardinality: :one)
                 (xyz.todo/list of: ?i is: ?l as: :entity cardinality: :one))

               (concept (todo/add todo: ?i list: ?l title: ?t)
                 (xyz.todo.add/todo of: ?c is: ?i as: :entity in: :control)
                 (xyz.todo.add/list of: ?c is: ?l as: :entity in: :control)
                 (xyz.todo.add/title of: ?c is: ?t as: :text in: :control))
               (concept (todo/complete todo: ?i)
                 (xyz.todo.complete/todo of: ?c is: ?i as: :entity in: :control))

               (rule (assert (todo ?i list: ?l title: ?t state: :open))
                 (todo/add todo: ?i list: ?l title: ?t))

               (rule (assert (xyz.todo/state of: ?i is: :done as: :entity cardinality: :one))
                 (todo/complete todo: ?i))"#,
        )
        .await?;
    assert!(diagnostics.is_empty(), "{diagnostics:?}");

    notebook
        .submit(
            r#"(assert (todo/add todo: todo:1 list: list:home title: "Buy milk"))
               (assert (todo/add todo: todo:2 list: list:home title: "Call mum"))"#,
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (todo ?i title: ?t state: ?s))").await?,
        vec![
            r#"(xyz.todo todo:1 state: :open title: "Buy milk")"#,
            r#"(xyz.todo todo:2 state: :open title: "Call mum")"#,
        ]
    );

    // A second document, so a second transaction: under `:one` the
    // state it writes is the one read after it.
    notebook
        .submit("(assert (todo/complete todo: todo:2))")
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (xyz.todo/state of: ?i is: ?s as: :entity cardinality: :one))"
        )
        .await?,
        vec![
            "(xyz.todo todo:1 state: :open)",
            "(xyz.todo todo:2 state: :done)",
        ]
    );
    assert_eq!(
        report(&mut notebook, "(select (todo ?i title: ?t state: :open))").await?,
        vec![r#"(xyz.todo todo:1 state: :open title: "Buy milk")"#]
    );

    // Under `:many` a write adds a claim, and a read gives every one.
    notebook
        .submit(r#"(assert (xyz.todo/tag of: todo:1 is: "home" as: :text cardinality: :many))"#)
        .await?;
    notebook
        .submit(r#"(assert (xyz.todo/tag of: todo:1 is: "shop" as: :text cardinality: :many))"#)
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (xyz.todo/tag of: todo:1 is: ?tag as: :text cardinality: :many))"
        )
        .await?,
        vec![
            r#"(xyz.todo todo:1 tag: "home")"#,
            r#"(xyz.todo todo:1 tag: "shop")"#,
        ]
    );
    Ok(())
}

/// A derived attribute is one a rule concludes in a concept's own
/// domain: here, how many of a list's to-dos are open. One that folds
/// is read and expanded, and not run: Dialog-DB refuses a fold in a
/// rule that derives, and the document is refused with that said.
/// One that folds nothing runs, and a write of one is refused.
#[dialog_common::test]
async fn it_derives_an_attribute_in_a_concepts_domain() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (list ?l name: ?n)
                 (xyz.list/name of: ?l is: ?n as: :text cardinality: :one))
               (concept (todo ?t list: ?l state: ?s)
                 (xyz.todo/list of: ?t is: ?l as: :entity cardinality: :one)
                 (xyz.todo/state of: ?t is: ?s as: [:open :done] cardinality: :one))"#,
        )
        .await?;
    let folded = notebook
        .submit(
            "(rule (list/count-open ?l (count ?t))
               (list ?l)
               (todo ?t list: ?l state: :open))",
        )
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&folded, Err(Ok(Error::Unsupported(said))) if said.contains("is a fold in a rule that derives")),
        "{folded:?}"
    );

    // One that folds nothing is a derived relation like any other,
    // concluded and read under the one name.
    notebook
        .submit(
            r#"(rule (todo/home ?t ?n)
                 (todo ?t list: ?l)
                 (list ?l name: ?n))

               (assert (list list:home name: "Home"))
               (assert (todo todo:1 list: list:home state: :open))"#,
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (todo/home ?t ?n))").await?,
        vec![r#"(concept.todo.home todo:1 is: "Home")"#]
    );
    assert_eq!(
        report(&mut notebook, "(select (todo/home todo:1 is: ?n))").await?,
        vec![r#"(concept.todo.home todo:1 is: "Home")"#]
    );

    // A concept's domain is concluded only, and nothing of a document
    // that writes it is written.
    let written = notebook
        .submit(
            r#"(assert (list list:work name: "Work"))
               (assert (todo/home todo:1 "Away"))"#,
        )
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&written, Err(Ok(Error::Refused(why))) if why.contains("rules conclude and nothing writes")),
        "{written:?}"
    );
    assert!(
        report(&mut notebook, "(select (list ?l name: \"Work\"))")
            .await?
            .is_empty()
    );
    assert_eq!(
        report(&mut notebook, "(select (todo/home ?t ?n))").await?,
        vec![r#"(concept.todo.home todo:1 is: "Home")"#]
    );
    Ok(())
}

const COUNTER: &str = "
    (concept (counter/increment counter: ?n)
      (xyz.counter.increment/counter of: ?c is: ?n as: :entity in: :control))
    (rule (assert (xyz.counter ?c count: (:last (:natural (+ ?n : 1)))))
      (counter/increment counter: ?c)
      (xyz.counter ?c count: (:last (:natural ?n))))";

/// An attribute takes its two terms unlabeled, the entity and then the
/// value: in a write, in a rule's head and premises, and in a query,
/// where it reads as `of:` and `is:` do.
#[dialog_common::test]
async fn it_reads_and_writes_an_attribute_by_its_two_terms_unlabeled() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(assert (xyz.todo/title todo:1 "Buy milk"))
               (assert (xyz.todo/state todo:1 :open))
               (assert (xyz.todo/title todo:2 "Call mum"))
               (assert (xyz.todo/state todo:2 :done))
               (rule (xyz.open/title ?t ?x)
                 (xyz.todo/title ?t ?x)
                 (xyz.todo/state ?t :open))"#,
        )
        .await?;
    let open = vec![r#"(xyz.open todo:1 title: "Buy milk")"#];
    assert_eq!(
        report(&mut notebook, "(select (xyz.open/title ?t ?x))").await?,
        open
    );
    assert_eq!(
        report(&mut notebook, "(select (xyz.open/title of: ?t is: ?x))").await?,
        open
    );
    Ok(())
}

/// `counter/increment` is declared, and asserted it is issued: the rule
/// that reads it fires, and the count it asserts stays.
#[dialog_common::test]
async fn it_counts_with_a_command_a_rule_reads() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook
        .submit(&format!(
            "(assert (xyz.counter counter:a count: (:last (:natural 1))))
             {COUNTER}"
        ))
        .await?;
    assert!(diagnostics.is_empty(), "{diagnostics:?}");

    notebook
        .submit("(assert (counter/increment counter: counter:a))")
        .await?;
    notebook
        .submit("(assert (counter/increment counter: counter:a))")
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (xyz.counter ?c count: (:last ?n)))").await?,
        vec!["(xyz.counter counter:a count: 3)"]
    );

    // A command declared and read by no rule is issued all the same,
    // and said so.
    let (_, diagnostics) = notebook
        .submit(
            "(concept (counter/reset counter: ?n)
               (xyz.counter.reset/counter of: ?c is: ?n as: :entity in: :control))
             (assert (counter/reset counter: counter:a))",
        )
        .await?;
    assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
    assert_eq!(diagnostics[0].message, "no rule reads this command");
    // One misspelled is no command, and nothing is written.
    let misspelled = notebook
        .submit("(assert (counter/incremnt counter: counter:a))")
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&misspelled, Err(Ok(Error::Refused(why))) if why.contains("no concept is declared")),
        "{misspelled:?}"
    );
    assert_eq!(
        report(&mut notebook, "(select (xyz.counter ?c count: (:last ?n)))").await?,
        vec!["(xyz.counter counter:a count: 3)"]
    );
    Ok(())
}

/// A concept need not be about the entity its facts are of: a list's
/// items are the to-dos that name it. It reads from either side, and a
/// write through it lands on the to-do.
#[dialog_common::test]
async fn it_reads_and_writes_a_backlink_through_a_concept() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (list-item ?l todo: ?t)
                 (xyz.todo ?t list: (:last (:entity ?l))))
               (assert (list-item list:home todo: todo:1))
               (assert (list-item list:home todo: todo:2))
               (assert (list-item list:work todo: todo:3))"#,
        )
        .await?;

    assert_eq!(
        report(&mut notebook, "(select (list-item list:home todo: ?t))").await?,
        vec![
            "(xyz.todo todo:1 list: list:home)",
            "(xyz.todo todo:2 list: list:home)",
        ]
    );
    assert_eq!(
        report(&mut notebook, "(select (list-item ?l todo: todo:3))").await?,
        vec!["(xyz.todo todo:3 list: list:work)"]
    );
    Ok(())
}

/// A concept of two reads: an author has a name in one domain and an
/// address in another. Both must hold for the concept to, and its
/// absence is the absence of the two together.
#[dialog_common::test]
async fn it_joins_the_reads_a_concept_holds_and_negates_them_as_one() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (author ?p name: ?n email: ?e)
                 (io.person ?p name: (:text ?n))
                 (io.contact ?p email: (:text ?e)))
               (assert (author person:ada name: "Ada" email: "ada@example.com"))
               (assert (io.person person:bob name: (:text "Bob")))"#,
        )
        .await?;

    assert_eq!(
        report(&mut notebook, "(select (author ?p name: ?n ...))").await?,
        vec![r#"(io.person person:ada name: "Ada")"#],
        "a read the query names no field of says nothing in the report"
    );
    assert_eq!(
        report(
            &mut notebook,
            "(select (io.person ?p name: ?n)
                   (io.person ?p name: (:text ?n))
                   (unless (author ?p ...)))"
        )
        .await?,
        vec![r#"(io.person person:bob name: "Bob")"#]
    );
    // What must not hold may stand anywhere among the premises: the
    // same rule.
    assert_eq!(
        report(
            &mut notebook,
            "(select (io.person ?p name: ?n)
               (io.person ?p name: (:text ?n))
               (unless (author ?p ...)))"
        )
        .await?,
        vec![r#"(io.person person:bob name: "Bob")"#]
    );
    Ok(())
}

/// A rule with a bare head derives: nothing is stored, and the relation
/// holds while its premises do. A query of several premises keeps the
/// values of a row together.
#[dialog_common::test]
async fn it_derives_a_relation_and_reports_rows_of_several_premises() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(assert (shop.item item:tea name: (:text "Tea") price: (:natural 4)))
               (assert (shop.item item:pot name: (:text "Pot") price: (:natural 30)))
               (assert (shop.item item:cup name: (:text "Cup") price: (:natural 9)))

               ; What costs less than ten.
               (rule (shop.offer ?i name: ?n price: ?p)
                 (shop.item ?i name: (:text ?n) price: (:natural ?p))
                 (< ?p : 10))"#,
        )
        .await?;

    assert_eq!(
        report(&mut notebook, "(select (shop.offer ?i name: ?n))").await?,
        vec![
            r#"(shop.offer item:cup name: "Cup")"#,
            r#"(shop.offer item:tea name: "Tea")"#,
        ]
    );
    assert_eq!(
        report(
            &mut notebook,
            "(select (shop.pair ?a with: ?b total: (+ ?p : ?q))
                   (shop.offer ?a price: ?p)
                   (shop.offer ?b price: ?q)
                   (< ?p : ?q))"
        )
        .await?,
        vec!["(shop.pair item:tea total: 13 with: item:cup)"]
    );
    Ok(())
}

/// A warning and what the compiler refuses both say where in the text
/// they are about.
#[dialog_common::test]
async fn it_says_where_a_warning_and_a_refusal_are() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            "(concept (todo/snooze todo: ?t)
               (xyz.todo.snooze/todo of: ?c is: ?t as: :entity in: :control))",
        )
        .await?;
    let (_, diagnostics) = notebook
        .submit("(assert (xyz.todo todo:1 text: \"x\"))\n  (assert (todo/snooze todo: todo:1))")
        .await?;
    let [warning] = diagnostics.as_slice() else {
        panic!("one warning: {diagnostics:?}");
    };
    let span = warning.span.expect("the warning has a place");
    assert_eq!((span.start.line, span.start.column), (1, 10));
    assert_eq!((span.end.line, span.end.column), (1, 36));

    let refused = notebook
        .submit("\n\n  (select (xyz.a ?a n: ?n) (xyz.a ?a n: ?n) (:like ?n : \"x\"))")
        .await
        .expect_err("the compiler has no counterpart for it");
    assert!(
        refused.to_string().contains("line 3, column 3"),
        "{refused:#}"
    );
    assert!(matches!(
        refused.downcast::<Error>(),
        Ok(Error::Unsupported(_))
    ));

    // A refusal by the analyzer says where the part it is about is.
    let refused = notebook
        .submit("(rule (xyz.seen ?a n: ?n)\n  (xyz.a ?a n: ?n)\n  (frobnicate ?a))")
        .await
        .expect_err("nothing says what frobnicate is");
    assert!(
        refused.to_string().contains("line 3, column 3"),
        "{refused:#}"
    );
    assert!(matches!(refused.downcast::<Error>(), Ok(Error::Unknown(_))));
    Ok(())
}

/// What the comment above a rule says is kept as a fact about the rule,
/// so a rule is found by what it is for.
#[dialog_common::test]
async fn it_finds_a_rule_by_what_the_comment_above_it_says() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"; How a shop prices things.

               ; What costs less than ten.
               (rule (shop.offer ?i name: ?n price: ?p)
                 (shop.item ?i name: (:text ?n) price: (:natural ?p))
                 (< ?p : 10))

               (concept (shop/restock item: ?i count: ?n)
                 (shop.restock/item of: ?c is: ?i as: :entity in: :control)
                 (shop.restock/count of: ?c is: ?n as: :natural in: :control))

               ; Restocking sets the count.
               (rule (assert (shop.stock ?i count: (:last (:natural ?n))))
                 (shop/restock item: ?i count: ?n))

               (rule (shop.dear ?i name: ?n)
                 (shop.item ?i name: (:text ?n) price: (:natural ?p))
                 (> ?p : 100))"#,
        )
        .await?;
    let said = report(
        &mut notebook,
        r#"(select (syntax.meta ?rule description: ?text))"#,
    )
    .await?;
    assert_eq!(said.len(), 2, "{said:?}");
    assert!(said.iter().all(|row| row.starts_with("(syntax.meta rule:")));
    assert!(
        said.iter()
            .any(|row| row.ends_with(r#"description: "What costs less than ten.")"#))
    );
    assert!(
        said.iter()
            .any(|row| row.ends_with(r#"description: "Restocking sets the count.")"#))
    );
    Ok(())
}

/// A rule has one head. Two written as one rule are refused, and
/// nothing the document says is written, the fact beside the rule
/// included.
#[dialog_common::test]
async fn it_refuses_to_assert_and_retract_from_one_rule() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let refused = notebook
        .submit(
            r#"(assert (xyz.todo todo:1 list: list:home))
               (rule (all (assert (xyz.todo ?t list: ?to))
                          (retract (xyz.todo ?t list: ?from)))
                 (xyz.move ?t to: ?to)
                 (xyz.todo ?t list: ?from))"#,
        )
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&refused, Err(Ok(Error::Refused(why))) if why.contains("a rule for each")),
        "{refused:?}"
    );
    assert_eq!(
        report(&mut notebook, "(select (xyz.todo ?t list: ?l))").await?,
        Vec::<String>::new()
    );
    Ok(())
}

const MOVE: &str = "
    (concept (todo/move todo: ?t list: ?to)
      (xyz.todo.move/todo of: ?c is: ?t as: :entity in: :control)
      (xyz.todo.move/list of: ?c is: ?to as: :entity in: :control))";

/// Moving a to-do writes where it is and takes back where it was: two
/// rules of one head, each with the premises it needs. A tick reads
/// one state, so both see where the to-do was. A to-do on no list is
/// still put on one, since the write does not wait on what only the
/// retraction needs.
#[dialog_common::test]
async fn it_asserts_and_retracts_from_two_rules_of_one_head() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(&format!(
            "{MOVE}
             (rule (assert (xyz.todo ?t list: ?to))
               (todo/move todo: ?t list: ?to))
             (rule (retract (xyz.todo ?t list: ?from))
               (todo/move todo: ?t list: ?to)
               (xyz.todo ?t list: ?from))
             (assert (xyz.todo todo:1 list: list:home))"
        ))
        .await?;
    notebook
        .submit("(assert (todo/move todo: todo:1 list: list:work))")
        .await?;
    notebook
        .submit("(assert (todo/move todo: todo:2 list: list:work))")
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (xyz.todo ?t list: ?l))").await?,
        vec![
            "(xyz.todo todo:1 list: list:work)",
            "(xyz.todo todo:2 list: list:work)"
        ]
    );
    Ok(())
}

/// The same move with its condition given a name: one rule deduces a
/// command that carries what the condition bound, which holds in the
/// tick while the condition does, and two rules of one head read it.
/// Nothing stores the deduced command.
#[dialog_common::test]
async fn it_asserts_and_retracts_on_a_command_a_rule_deduces() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(&format!(
            "{MOVE}
             (concept (todo/moved todo: ?t to: ?to from: ?from)
               (xyz.todo.moved/todo of: ?c is: ?t as: :entity in: :control)
               (xyz.todo.moved/to of: ?c is: ?to as: :entity in: :control)
               (xyz.todo.moved/from of: ?c is: ?from as: :entity in: :control))
             (rule (todo/moved todo: ?t to: ?to from: ?from)
               (todo/move todo: ?t list: ?to)
               (xyz.todo ?t list: ?from))
             (rule (assert (xyz.todo ?t list: ?to))
               (todo/moved todo: ?t to: ?to ...))
             (rule (retract (xyz.todo ?t list: ?from))
               (todo/moved todo: ?t from: ?from ...))
             (assert (xyz.todo todo:1 list: list:home))"
        ))
        .await?;
    notebook
        .submit("(assert (todo/move todo: todo:1 list: list:work))")
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (xyz.todo ?t list: ?l))").await?,
        vec!["(xyz.todo todo:1 list: list:work)"]
    );
    assert!(
        report(&mut notebook, "(select (todo/moved todo: ?t ...))")
            .await?
            .is_empty()
    );
    Ok(())
}

/// A relation derived from itself: two rules of one head, the second
/// reading what both conclude.
#[dialog_common::test]
async fn it_derives_a_relation_from_itself() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            "(assert (fam.parent person:ann child: person:bea))
             (assert (fam.parent person:bea child: person:cal))
             (assert (fam.parent person:cal child: person:dee))

             (rule (fam.ancestor ?a descendant: ?c)
               (fam.parent ?a child: ?c))
             (rule (fam.ancestor ?a descendant: ?c)
               (fam.parent ?a child: ?b)
               (fam.ancestor ?b descendant: ?c))",
        )
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (fam.ancestor ?a descendant: person:dee))"
        )
        .await?,
        vec![
            "(fam.ancestor person:ann descendant: person:dee)",
            "(fam.ancestor person:bea descendant: person:dee)",
            "(fam.ancestor person:cal descendant: person:dee)",
        ]
    );
    Ok(())
}

/// A document that installs a rule and issues the command it reads:
/// the rule fires in the transaction that installs it.
#[dialog_common::test]
async fn it_fires_a_rule_in_the_document_that_installs_it() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (report, diagnostics) = notebook
        .submit(&format!(
            "(assert (xyz.counter counter:a count: (:last (:natural 1))))
             {COUNTER}
             (assert (counter/increment counter: counter:a))
             (select (xyz.counter ?c count: (:last ?n)))"
        ))
        .await?;
    assert!(diagnostics.is_empty());
    assert_eq!(report, vec!["(xyz.counter counter:a count: 2)"]);
    Ok(())
}

/// A rule asserts a command, which issues it, and another rule reads
/// it in the tick after: archiving a list completes what is on it.
#[dialog_common::test]
async fn it_lets_a_rule_issue_a_command_another_rule_reads() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook
        .submit(&format!(
            "{TODO}
             (concept (list/archive list: ?l)
               (xyz.list.archive/list of: ?c is: ?l as: :entity in: :control))
             (rule (assert (todo/complete todo: ?t))
               (list/archive list: ?l)
               (todo ?t list: ?l ...))"
        ))
        .await?;
    assert!(diagnostics.is_empty());
    notebook
        .submit(
            r#"(assert (todo/add todo: todo:1 list: list:home text: "Buy milk"))
               (assert (todo/add todo: todo:2 list: list:home text: "Call mum"))
               (assert (todo/add todo: todo:3 list: list:work text: "Ship it"))"#,
        )
        .await?;

    notebook
        .submit("(assert (list/archive list: list:home))")
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (todo ?t text: ?x state: :open ...))"
        )
        .await?,
        vec![r#"(xyz.todo todo:3 state: :open text: "Ship it")"#]
    );
    // Neither command is on the branch.
    assert!(
        report(&mut notebook, "(select (todo/complete todo: ?t))")
            .await?
            .is_empty()
    );
    Ok(())
}

/// An entity made from what a rule binds is the entity the same values
/// make anywhere: a rule and a write at the top level name one note.
#[dialog_common::test]
async fn it_derives_one_entity_from_the_same_values() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (note/jot text: ?x)
                 (xyz.note.jot/text of: ?c is: ?x as: :text in: :control))
               (rule (assert (xyz.note (:id xyz.note text: ?x) text: (:text ?x)))
                 (note/jot text: ?x))"#,
        )
        .await?;
    notebook.submit(r#"(assert (note/jot text: "hi"))"#).await?;
    notebook
        .submit(r#"(assert (xyz.note (:id xyz.note text: "hi") text: (:text "hi")))"#)
        .await?;
    notebook.submit(r#"(assert (note/jot text: "ho"))"#).await?;

    let notes = report(&mut notebook, "(select (xyz.note ?n text: ?x))").await?;
    assert_eq!(notes.len(), 2, "{notes:?}");
    assert!(notes.iter().all(|note| note.starts_with("(xyz.note id:")));
    Ok(())
}

/// Two commands in one document share a tick, so what they write to
/// one `last` field is concurrent. The field elects one, and which one
/// does not depend on the order the commands were written in.
#[dialog_common::test]
async fn it_elects_one_of_two_writes_of_one_round_whatever_their_order() -> Result<()> {
    let add = r#"(assert (todo/add todo: todo:1 list: list:home text: "Buy milk"))"#;
    let complete = "(assert (todo/complete todo: todo:1))";
    let mut elected = Vec::new();
    for document in [[add, complete], [complete, add]] {
        let mut notebook = Notebook::open().await?;
        notebook.submit(TODO).await?;
        notebook.submit(&document.join("\n")).await?;
        assert_eq!(
            report(&mut notebook, "(select (xyz.todo ?t state: ?s))").await?,
            vec![
                "(xyz.todo todo:1 state: :done)",
                "(xyz.todo todo:1 state: :open)",
            ],
            "both writes are in the cell"
        );
        let last = report(&mut notebook, "(select (xyz.todo ?t state: (:last ?s)))").await?;
        assert_eq!(last.len(), 1, "one of them is elected: {last:?}");
        elected.push(last);
    }
    assert_eq!(elected[0], elected[1]);
    Ok(())
}

/// Two definitions under one name, whose labels both fit what a query
/// writes: the query reports through each, and a row says which relation
/// it is of. A write says every field, so it is one definition's.
#[dialog_common::test]
async fn it_reports_through_every_definition_a_query_fits() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (todo ?t title: ?x done: ?d)
                 (xyz.todo ?t title: (:last (:text ?x)) done: (:last ?d)))
               (concept (todo ?t title: ?x pinned: ?p)
                 (xyz.note ?t title: (:last (:text ?x)) pinned: (:last ?p)))
               (assert (todo todo:1 title: "Buy milk" done: false))
               (assert (todo note:1 title: "Call Ada" pinned: true))"#,
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (todo ?t title: ?x ...))").await?,
        vec![
            r#"(xyz.note note:1 title: "Call Ada")"#,
            r#"(xyz.todo todo:1 title: "Buy milk")"#,
        ]
    );
    assert_eq!(
        report(&mut notebook, "(select (todo ?t pinned: ?p ...))").await?,
        vec!["(xyz.note note:1 pinned: true)"]
    );
    Ok(())
}

/// What a name is kept as: not a name and an entity only, but a name,
/// what is named, and the parameters it is applied with. A definition is
/// kept under the entity its signature names, with `last`, so two
/// definitions of other parameters stand side by side under one name and
/// a later one of the same parameters replaces. Each definition holds
/// its own names for the fields, whatever attributes they are of.
#[dialog_common::test]
async fn it_keeps_definitions_on_a_branch_under_their_signatures() -> Result<()> {
    let notebook = Notebook::open().await?;
    let definitions = [
        "(concept (todo ?t title: ?x) (xyz.todo ?t text: (:last (:text ?x))))",
        "(concept (todo ?t name: ?x) (xyz.todo ?t text: (:last (:text ?x))))",
        // The first, corrected: the same signature.
        "(concept (todo ?t title: ?x) (xyz.todo ?t title: (:last (:text ?x))))",
    ];
    let name: AttributeDescriptor =
        serde_json::from_value(json!({ "the": "syntax.name/is", "as": "text:" }))?;
    let definition: AttributeDescriptor =
        serde_json::from_value(json!({ "the": "syntax.name/definition", "as": "record:" }))?;

    for text in definitions {
        let syntax = Syntax::try_from(&read(text)?[0])?;
        let Syntax::Application(_, operand) = &syntax else {
            panic!("an application");
        };
        let signature = Signature::of(&operand.items[0], "concept").expect("a signature");
        notebook
            .branch
            .transaction()
            .assert(ConceptStatement {
                this: signature.entity()?,
                with: vec![
                    name.resolve(Value::String(signature.name.clone()))?,
                    definition.resolve(Value::Record(syntax.encode()?))?,
                ],
            })
            .commit()
            .publish()
            .perform(&notebook.operator)
            .await?;
    }
    notebook.branch.refresh(&notebook.operator).await?;

    // Everything named `todo`, by the name alone.
    let select: ConceptQuery = serde_json::from_value(json!({
        "assert": { "with": {
            "is": { "the": "syntax.name/is", "as": "text:" },
            "definition": { "the": "syntax.name/definition", "as": "record:" }
        } },
        "where": {
            "this": { "?": { "name": "this" } },
            "is": "todo",
            "definition": { "?": { "name": "definition" } }
        }
    }))?;
    let found: Vec<ConceptConclusion> = notebook
        .branch
        .query()
        .select(select)
        .perform(&notebook.operator)
        .try_vec()
        .await?;
    let mut kept = Vec::new();
    for conclusion in &found {
        let Value::Record(bytes) = conclusion
            .source()
            .lookup(&Term::<Any>::var("definition"))?
            .content()?
        else {
            panic!("a record");
        };
        kept.push(Syntax::decode(&bytes)?.to_string());
    }
    kept.sort();
    assert_eq!(
        kept,
        vec![
            "(concept (todo ?t name: ?x) (xyz.todo ?t text: (:last (:text ?x))))",
            "(concept (todo ?t title: ?x) (xyz.todo ?t title: (:last (:text ?x))))",
        ]
    );
    Ok(())
}

/// A struct is a value like 5: written as a field's value it is kept,
/// and comes back as the struct it was. With a variable in it, it is a
/// pattern: a query takes the kept struct apart by it. Read, a struct is
/// open: a record that holds more than it says still matches, and one
/// that lacks what it says does not.
#[dialog_common::test]
async fn it_keeps_a_struct_and_takes_it_apart_by_a_pattern() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(assert (xyz.note note:1 about: {:author "Ada" :tags ["math" "engines"] :pinned true}))
               (assert (xyz.note note:2 about: {:author "Grace"}))"#,
        )
        .await?;

    // Whole, under a variable.
    assert_eq!(
        report(&mut notebook, "(select (xyz.note ?n about: ?about))").await?,
        vec![
            r#"(xyz.note note:1 about: {:author "Ada" :pinned true :tags ["math" "engines"]})"#,
            r#"(xyz.note note:2 about: {:author "Grace"})"#,
        ]
    );
    // Taken apart. A struct that is read is open: it is any record that
    // holds what it says, so both notes have an author, and the row
    // says the whole record that was there.
    let both = vec![
        r#"(xyz.note note:1 about: {:author "Ada" :pinned true :tags ["math" "engines"]})"#,
        r#"(xyz.note note:2 about: {:author "Grace"})"#,
    ];
    assert_eq!(
        report(
            &mut notebook,
            "(select (xyz.note ?n about: {:author ?who}))"
        )
        .await?,
        both
    );
    // A trailing `...` says the same, and so does a struct with no
    // variable in it: read, it is not the value but what a record holds.
    assert_eq!(
        report(
            &mut notebook,
            "(select (xyz.note ?n about: {:author ?who ...}))"
        )
        .await?,
        both
    );
    assert_eq!(
        report(
            &mut notebook,
            r#"(select (xyz.note ?n about: {:author "Ada"}))"#
        )
        .await?,
        both[..1]
    );
    assert_eq!(
        report(
            &mut notebook,
            r#"(select (xyz.note ?n about: {:author ?who :tags [?first ?second] :pinned true}))"#
        )
        .await?,
        vec![r#"(xyz.note note:1 about: {:author "Ada" :pinned true :tags ["math" "engines"]})"#]
    );
    // With a rest it is a map of at least its entries, and the rest is
    // the others, as a struct: both notes have an author.
    assert_eq!(
        report(
            &mut notebook,
            "(select (xyz.note ?n about: {:author ?who ... ?others}))"
        )
        .await?,
        vec![
            r#"(xyz.note note:1 about: {:author "Ada" :pinned true :tags ["math" "engines"]})"#,
            r#"(xyz.note note:2 about: {:author "Grace"})"#,
        ]
    );
    // A blank takes what is there and binds nothing; the row still says
    // what was there.
    assert_eq!(
        report(
            &mut notebook,
            "(select (xyz.note ?n about: {:author _ :tags [?first _] :pinned _}))"
        )
        .await?,
        vec![r#"(xyz.note note:1 about: {:author "Ada" :pinned true :tags ["math" "engines"]})"#]
    );
    // A struct of constants is read in whatever order it is written
    // in.
    assert_eq!(
        report(
            &mut notebook,
            r#"(select (xyz.note ?n about: {:pinned true :tags ["math" "engines"] :author "Ada"}))"#
        )
        .await?
        .len(),
        1
    );

    // A query of several premises is a rule laid over the branch for
    // the query alone, struct and all: nothing of it is stored.
    notebook
        .submit(r#"(assert (xyz.person person:ada name: "Ada"))"#)
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (xyz.person ?p name: ?who)
               (xyz.note ?n about: {:author ?who :tags _ :pinned _})
               (xyz.person ?p name: ?who))",
        )
        .await?,
        vec![r#"(xyz.person person:ada name: "Ada")"#]
    );

    // A rule that holds a struct is kept on the branch like any other
    // (format 4 of a stored rule writes a pattern), and the branch
    // derives from it.
    notebook
        .submit("(rule (xyz.by ?n author: ?who) (xyz.note ?n about: {:author ?who}))")
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (xyz.by ?n author: ?who))").await?,
        vec![
            r#"(xyz.by note:1 author: "Ada")"#,
            r#"(xyz.by note:2 author: "Grace")"#,
        ]
    );
    // A query of several premises reads through the kept rule.
    notebook
        .submit(r#"(assert (xyz.person person:grace name: "Grace"))"#)
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (xyz.person ?p name: ?who)
               (xyz.by ?n author: ?who) (xyz.person ?p name: ?who))",
        )
        .await?,
        vec![
            r#"(xyz.person person:ada name: "Ada")"#,
            r#"(xyz.person person:grace name: "Grace")"#,
        ]
    );

    // A rule that writes runs when a transaction commits, so it has to
    // be on the branch. One that holds a struct is: installed, it
    // writes what the struct takes out of the notes there are, and then
    // out of a note written after it.
    notebook
        .submit(
            "(rule (assert (xyz.signed ?n by: (:last (:text ?who))))
               (xyz.note ?n about: {:author ?who}))",
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (xyz.signed ?n by: ?who))").await?,
        vec![
            r#"(xyz.signed note:1 by: "Ada")"#,
            r#"(xyz.signed note:2 by: "Grace")"#,
        ]
    );
    notebook
        .submit(r#"(assert (xyz.note note:3 about: {:author "Hedy" :pinned false}))"#)
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (xyz.signed ?n by: ?who))").await?,
        vec![
            r#"(xyz.signed note:1 by: "Ada")"#,
            r#"(xyz.signed note:2 by: "Grace")"#,
            r#"(xyz.signed note:3 by: "Hedy")"#,
        ]
    );
    Ok(())
}

/// What a name with two referents can be read as today. Written under
/// `last`, a referent written after another replaces it, and two written
/// in one round, which is what two replicas writing apart come to, both
/// stand. Read with no pick, the first is one row and the second is two.
/// So a name that was re-pointed and a name two definitions claim at
/// once are told apart by what is already stored.
#[dialog_common::test]
async fn it_tells_a_name_that_was_repointed_from_one_claimed_twice() -> Result<()> {
    let rules = "(concept (name/first name: ?n)
                   (xyz.name.first/name of: ?c is: ?n as: :entity in: :control))
                 (concept (name/second name: ?n)
                   (xyz.name.second/name of: ?c is: ?n as: :entity in: :control))
                 (rule (assert (xyz.name ?n referent: (:last (:entity concept:a))))
                   (name/first name: ?n))
                 (rule (assert (xyz.name ?n referent: (:last (:entity concept:b))))
                   (name/second name: ?n))";
    let first = "(assert (name/first name: name:todo))";
    let second = "(assert (name/second name: name:todo))";
    let every = "(select (xyz.name ?n referent: ?r))";

    let mut repointed = Notebook::open().await?;
    repointed.submit(rules).await?;
    repointed.submit(first).await?;
    repointed.submit(second).await?;
    assert_eq!(
        report(&mut repointed, every).await?,
        vec!["(xyz.name name:todo referent: concept:b)"]
    );

    let mut twice = Notebook::open().await?;
    twice.submit(rules).await?;
    twice.submit(&format!("{first}\n{second}")).await?;
    assert_eq!(
        report(&mut twice, every).await?,
        vec![
            "(xyz.name name:todo referent: concept:a)",
            "(xyz.name name:todo referent: concept:b)",
        ]
    );
    assert_eq!(
        report(&mut twice, "(select (xyz.name ?n referent: (:last ?r)))")
            .await?
            .len(),
        1
    );
    Ok(())
}

/// One call, two ways to write it: the primary with no label, and under
/// `of:`. A concept and a relation read the same through each, and a
/// write lands the same. A struct in the primary's place is the primary,
/// a value, and not the labelled parts written another way.
#[dialog_common::test]
async fn it_reads_a_call_the_same_however_its_primary_is_written() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(TODO).await?;
    notebook
        .submit(
            r#"(assert (todo/add todo: todo:1 list: list:home text: "Buy milk"))
               (assert (xyz.note text: (:text "by label") of: note:1))
               (assert (xyz.note note:2 text: (:text "by position")))"#,
        )
        .await?;

    let milk = vec![r#"(xyz.todo todo:1 text: "Buy milk")"#];
    for query in [
        "(select (todo ?t text: ?x ...))",
        "(select (todo text: ?x of: ?t ...))",
    ] {
        assert_eq!(report(&mut notebook, query).await?, milk, "{query}");
    }
    assert_eq!(
        report(&mut notebook, "(select (xyz.note of: ?n text: ?x))").await?,
        vec![
            r#"(xyz.note note:1 text: "by label")"#,
            r#"(xyz.note note:2 text: "by position")"#,
        ]
    );

    let record = notebook
        .submit(r#"(assert (xyz.note {:of note:3 :text (:text "by record")}))"#)
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(matches!(record, Err(Ok(Error::Refused(_)))), "{record:?}");

    // Beside a part with no label, `of:` would be the primary twice.
    let twice = notebook
        .submit("(select (xyz.note ?n of: ?m))")
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(matches!(twice, Err(Ok(Error::Refused(_)))));
    Ok(())
}

/// A form is held by a branch today: as a `record:` value, under the
/// entity its reference names. What comes back reads as the form that
/// went in, and names the same entity.
#[dialog_common::test]
async fn it_keeps_a_form_on_a_branch_under_the_entity_that_names_it() -> Result<()> {
    let notebook = Notebook::open().await?;
    let form = read(TODO)?.remove(0);
    let syntax = Syntax::try_from(&form)?;
    let entity = syntax.entity()?;

    let is: AttributeDescriptor =
        serde_json::from_value(json!({ "the": "syntax.form/is", "as": "record:" }))?;
    notebook
        .branch
        .transaction()
        .assert(ConceptStatement {
            this: entity.clone(),
            with: vec![is.resolve(Value::Record(syntax.encode()?))?],
        })
        .commit()
        .publish()
        .perform(&notebook.operator)
        .await?;
    notebook.branch.refresh(&notebook.operator).await?;

    let select: ConceptQuery = serde_json::from_value(json!({
        "assert": { "with": { "is": { "the": "syntax.form/is", "as": "record:" } } },
        "where": { "this": { "entity:": entity.to_string() }, "is": { "?": { "name": "is" } } }
    }))?;
    let found: Vec<ConceptConclusion> = notebook
        .branch
        .query()
        .select(select)
        .perform(&notebook.operator)
        .try_vec()
        .await?;
    assert_eq!(found.len(), 1);
    let Value::Record(bytes) = found[0]
        .source()
        .lookup(&Term::<Any>::var("is"))?
        .content()?
    else {
        panic!("a record");
    };
    let kept = Syntax::decode(&bytes)?;
    assert_eq!(kept, syntax);
    assert_eq!(kept.entity()?, entity);
    assert_eq!(read(&kept.to_string())?[0].reference()?, form.reference()?);
    Ok(())
}

/// A kept form is a composite value, so a query takes it apart by a
/// pattern like any struct: the forms that apply a name are found by
/// the shape of an application, with no code that knows what a form is.
#[dialog_common::test]
async fn it_finds_a_kept_form_by_a_pattern_over_its_tree() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let is: AttributeDescriptor =
        serde_json::from_value(json!({ "the": "syntax.form/is", "as": "record:" }))?;
    let mut entities = Vec::new();
    for text in [
        "(todo ?t)",
        "(todo ?t state: :open)",
        "(done ?t)",
        "[todo ?t]",
    ] {
        let syntax = Syntax::try_from(&read(text)?[0])?;
        entities.push(syntax.entity()?.to_string());
        notebook
            .branch
            .transaction()
            .assert(ConceptStatement {
                this: syntax.entity()?,
                with: vec![is.resolve(syntax.value()?)?],
            })
            .commit()
            .publish()
            .perform(&notebook.operator)
            .await?;
    }
    notebook.branch.refresh(&notebook.operator).await?;

    // Every application of `todo`, whatever it is applied to. A
    // reference holds its name as a symbol of no domain, written
    // `/todo`. The tuple that starts with `todo` applies nothing.
    let mut applies = report(
        &mut notebook,
        "(select (syntax.form ?f is: {:type :application
                                     :operator {:type :reference :name /todo}
                                     :operand _}))",
    )
    .await?;
    applies.sort();
    let mut expected = vec![
        format!(
            "(syntax.form {} is: {{:operand {{:items [{{:name \"t\" :type :variable}}] :terms {{}} :type :parameters}} :operator {{:name /todo :type :reference}} :type :application}})",
            entities[0]
        ),
        format!(
            "(syntax.form {} is: {{:operand {{:items [{{:name \"t\" :type :variable}}] :terms {{:state {{:type :constant :value :open}}}} :type :parameters}} :operator {{:name /todo :type :reference}} :type :application}})",
            entities[1]
        ),
    ];
    expected.sort();
    assert_eq!(applies, expected);

    // Taken further apart: the applications, by the shape of their
    // parameters, whatever name they apply. A struct that is read is
    // open, so `terms: {}` is any terms at all and not "no labelled
    // part": a read has no way to say a record holds nothing more.
    let bare = report(
        &mut notebook,
        "(select (syntax.form ?f is: {:type :application
                                     :operator {:type :reference :name ?name}
                                     :operand {:type :parameters :items ?items :terms {}}}))",
    )
    .await?;
    assert_eq!(bare.len(), 3, "{bare:?}");
    for applied in [0, 1, 2] {
        assert!(bare.iter().any(|row| row.contains(&entities[applied])));
    }

    // A tuple is no application, so it has no such shape, and it has
    // one of its own.
    let tuples = report(
        &mut notebook,
        "(select (syntax.form ?f is: {:type :tuple :items ?items}))",
    )
    .await?;
    assert_eq!(tuples.len(), 1, "{tuples:?}");
    assert!(tuples[0].contains(&entities[3]));

    // With a rest, a pattern says only what it asks about: every kept
    // form that applies a name, whatever else its nodes hold.
    let named = report(
        &mut notebook,
        "(select (syntax.form ?f is: {:operator {:name ?name ... ?reference} ... ?node}))",
    )
    .await?;
    assert_eq!(named.len(), 3, "{named:?}");

    // The parts of a node are a list, which `'( )` and `[ ]` both take
    // apart: the kept tuple of two parts.
    for pair in [
        "(select (syntax.form ?f is: {:type :tuple :items '(_ _)}))",
        "(select (syntax.form ?f is: {:type :tuple :items [_ _]}))",
    ] {
        let pairs = report(&mut notebook, pair).await?;
        assert_eq!(pairs.len(), 1, "{pair}: {pairs:?}");
        assert!(pairs[0].contains(&entities[3]));
    }
    // And by a run: every application whose first part is a variable.
    let first = report(
        &mut notebook,
        "(select (syntax.form ?f is: {:operand {:items '({:type :variable :name ?variable} ... ?more)
                                               ... ?parameters}
                                     ... ?node}))",
    )
    .await?;
    assert_eq!(first.len(), 3, "{first:?}");
    Ok(())
}

/// A list is `[1 2 3]` where a value stands, or `'(1 2 3)`, quoted as in
/// lisp so that it is not read as applying `1`. Both say the one list the
/// data model has, and either takes a kept one apart. `... ?run` after
/// its items is the ones after them, and `...` alone takes them and binds
/// nothing. A list comes back written `[ ]`, however it went in.
#[dialog_common::test]
async fn it_keeps_a_list_and_takes_it_apart_by_its_first_items() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(assert (xyz.note note:1 tags: '("math" "engines" "looms")))
               (assert (xyz.note note:2 tags: '("math")))
               (assert (xyz.note note:3 tags: ["math" "engines"]))
               (assert (xyz.note note:4 about: {:tags '("math" {:by "Ada"})}))"#,
        )
        .await?;
    let one = r#"(xyz.note note:1 tags: ["math" "engines" "looms"])"#;
    let two = r#"(xyz.note note:2 tags: ["math"])"#;
    let three = r#"(xyz.note note:3 tags: ["math" "engines"])"#;
    let sorted = |mut rows: Vec<String>| {
        rows.sort();
        rows
    };

    // Whole, under a variable: a list is printed `[ ]`, quoted or not
    // where it was written.
    assert_eq!(
        sorted(report(&mut notebook, "(select (xyz.note ?n tags: ?tags))").await?),
        vec![one, two, three]
    );
    // A list pattern with no run is the list of exactly its items, in
    // either spelling, whichever spelling wrote the list.
    assert_eq!(
        report(&mut notebook, "(select (xyz.note ?n tags: '(?only)))").await?,
        vec![two]
    );
    assert_eq!(
        report(&mut notebook, "(select (xyz.note ?n tags: [?only]))").await?,
        vec![two]
    );
    assert_eq!(
        report(&mut notebook, "(select (xyz.note ?n tags: '(?a ?b)))").await?,
        vec![three]
    );
    assert_eq!(
        report(&mut notebook, "(select (xyz.note ?n tags: [?a ?b ?c]))").await?,
        vec![one]
    );
    // With a run it is a list that starts with them. A run may take
    // nothing.
    for starts in [
        "(select (xyz.note ?n tags: '(?first ?second ... ?more)))",
        "(select (xyz.note ?n tags: [?first ?second ... ?more]))",
    ] {
        assert_eq!(
            sorted(report(&mut notebook, starts).await?),
            vec![one, three],
            "{starts}"
        );
    }
    for starts in [
        r#"(select (xyz.note ?n tags: '("math" ... ?more)))"#,
        r#"(select (xyz.note ?n tags: ["math" ...]))"#,
    ] {
        assert_eq!(
            sorted(report(&mut notebook, starts).await?),
            vec![one, two, three],
            "{starts}"
        );
    }
    // A list with no variable in it is the value, and what a row prints
    // reads back as it.
    for whole in [
        r#"(select (xyz.note ?n tags: '("math" "engines" "looms")))"#,
        r#"(select (xyz.note ?n tags: ["math" "engines" "looms"]))"#,
    ] {
        assert_eq!(report(&mut notebook, whole).await?, vec![one], "{whole}");
    }
    // Inside a struct, and a struct inside it.
    assert_eq!(
        report(
            &mut notebook,
            "(select (xyz.note ?n about: {:tags '(_ {:by ?who})}))"
        )
        .await?,
        vec![r#"(xyz.note note:4 about: {:tags ["math" {:by "Ada"}]})"#]
    );

    // A list has no labelled part.
    let labelled = notebook
        .submit("(select (xyz.note ?n tags: '(?first then: ?second)))")
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(labelled, Err(Ok(Error::Refused(_)))),
        "{labelled:?}"
    );
    // Quoted, a list may start with anything, a keyword too. Not
    // quoted, a `( )` where a value stands is refused, with what to
    // write.
    notebook
        .submit("(assert (xyz.note note:5 tags: '(:open :done)))")
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (xyz.note ?n tags: '(:open ...)))").await?,
        vec!["(xyz.note note:5 tags: [:open :done])"]
    );
    let bare = notebook
        .submit(r#"(select (xyz.note ?n tags: ("math" "engines")))"#)
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(matches!(bare, Err(Ok(Error::Refused(_)))), "{bare:?}");
    Ok(())
}

/// What a form as a record rests on: a symbol with no domain is a value
/// today, a variable or an operator spelled as one included, and it is
/// not an attribute a claim can be made under.
#[dialog_common::test]
async fn it_holds_a_symbol_with_no_domain_as_a_value_and_not_as_an_attribute() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit("(assert (xyz.form form:1 head: /todo op: /+ var: /?x))")
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (xyz.form ?f head: ?h var: ?v op: ?o))"
        )
        .await?,
        vec!["(xyz.form form:1 head: /todo op: /+ var: /?x)"]
    );

    let claim = notebook
        .submit(r#"(assert (xyz.form form:1 /text: "x"))"#)
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(matches!(claim, Err(Ok(Error::Refused(_)))));
    Ok(())
}

/// What is not allowed is refused, and what Dialog-DB cannot yet run is
/// said, not approximated.
#[dialog_common::test]
async fn it_refuses_and_says_what_is_not_supported() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(TODO).await?;
    let why = |result: Result<(Vec<String>, Vec<dialog_notation::Diagnostic>)>| {
        result.map_err(|error| error.downcast::<Error>())
    };

    // A write through a concept says every field.
    let partial = notebook
        .submit("(rule (assert (todo ?t state: :done)) (todo/complete todo: ?t))")
        .await;
    assert!(matches!(why(partial), Err(Ok(Error::Refused(_)))));

    // A variable in `unless` is one a premise binds.
    let unbound = notebook
        .submit(
            "(select (xyz.todo ?t text: ?x)
               (xyz.todo ?t text: ?x)
               (unless (xyz.todo ?t list: ?l)))",
        )
        .await;
    assert!(matches!(why(unbound), Err(Ok(Error::Refused(_)))));

    // A formula inside `unless` has nowhere to go.
    let formula = notebook
        .submit(
            "(select (xyz.counter ?c count: ?n)
               (xyz.counter ?c count: ?n)
               (unless (xyz.counter ?c count: (+ ?n : 1))))",
        )
        .await;
    assert!(matches!(why(formula), Err(Ok(Error::Unsupported(_)))));

    // What retracting a command means is not decided, and nothing of
    // the document that says it is written.
    let retracted = notebook
        .submit(
            r#"(assert (xyz.todo todo:9 text: (:text "kept")))
               (retract (todo/add todo: todo:1 list: list:home text: "Buy milk"))"#,
        )
        .await;
    assert!(
        matches!(why(retracted), Err(Ok(Error::Unsupported(said))) if said.contains("retracts a command"))
    );
    assert!(
        report(&mut notebook, "(select (xyz.todo todo:9 text: ?x))")
            .await?
            .is_empty()
    );
    Ok(())
}

/// How far tonk's notebook, written in the notation as designed, gets:
/// every form read, how many expand and compile, and how many views reach
/// a description. A view compiles to its plan when every layer's query
/// does, and each of the notebook's does. A step that moves these numbers
/// moves them here on purpose.
#[dialog_common::test]
async fn it_measures_how_far_the_notebook_gets() -> Result<()> {
    let measured = bar::Bar::of(include_str!("../sketches/notebook.clj"))?;
    let groups: Vec<String> = measured
        .groups()
        .iter()
        .map(|(said, stops)| {
            let forms: Vec<String> = stops
                .iter()
                .map(|stop| format!("{:?}", stop.form))
                .collect();
            format!("{} {said} {}", stops.len(), forms.join(" "))
        })
        .collect();
    assert_eq!(
        (
            measured.forms,
            measured.read,
            measured.expanded,
            measured.compiled,
            measured.described
        ),
        (56, 56, 53, 53, 5),
        "{groups:#?}"
    );
    // With core in front, as tonk reads a component, the concepts the
    // notebook takes from core are declared.
    let joined = bar::Bar::of_all(&[
        include_str!("../sketches/core.clj"),
        include_str!("../sketches/notebook.clj"),
    ])?;
    assert_eq!(
        (
            joined.forms,
            joined.read,
            joined.expanded,
            joined.compiled,
            joined.described
        ),
        (60, 60, 60, 60, 5)
    );
    Ok(())
}

/// How far tonk's table gets, read after core as tonk reads a component:
/// a second document, against a build fitted to the notebook. Its six
/// views compile to their plans. A step that moves these numbers moves
/// them here on purpose.
#[dialog_common::test]
async fn it_measures_how_far_the_table_gets() -> Result<()> {
    let measured = bar::Bar::of_all(&[
        include_str!("../sketches/core.clj"),
        include_str!("../sketches/table.clj"),
    ])?;
    let groups: Vec<String> = measured
        .groups()
        .iter()
        .map(|(said, stops)| {
            let forms: Vec<String> = stops
                .iter()
                .map(|stop| format!("{:?}", stop.form))
                .collect();
            format!("{} {said} {}", stops.len(), forms.join(" "))
        })
        .collect();
    assert_eq!(
        (
            measured.forms,
            measured.read,
            measured.expanded,
            measured.compiled,
            measured.described
        ),
        (54, 54, 54, 54, 6),
        "{groups:#?}"
    );
    Ok(())
}

/// The table's own forms, its views left out: what is run on a branch
/// is the rules, and nothing of a view is written.
fn the_table_without_its_views() -> Result<String> {
    let forms = dialog_notation::read(include_str!("../sketches/table.clj"))?;
    Ok(forms
        .iter()
        .filter(|form| form.head() != Some("view"))
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n"))
}

/// The table's rules, run on a branch as the sketch writes them, do what
/// table.yaml's rules do: an edit replaces a cell's content, a clear
/// retracts the content so the cell is no `table/cell` while its address
/// stays, a create makes the cell, sheet or column at the command's own
/// entity, and a rename or a resize upserts the one field.
#[dialog_common::test]
async fn it_runs_the_table_on_a_branch() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook.submit(&the_table_without_its_views()?).await?;
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let cells = "(select (table/cell ?c sheet: id:table/book/sheet1 at: ?at content: ?x))";
    assert_eq!(report(&mut notebook, cells).await?.len(), 14);

    notebook
        .submit(r#"(assert (table/edit-cell cell: id:table/book/s1!B2 content: "5"))"#)
        .await?;
    notebook
        .submit("(assert (table/clear-cell cell: id:table/book/s1!C4))")
        .await?;
    notebook
        .submit(
            r#"(assert (table/create-cell sheet: id:table/book/sheet1 at: "E1" content: "Note" time: 1))"#,
        )
        .await?;
    let after = report(&mut notebook, cells).await?;
    assert_eq!(after.len(), 14, "{after:#?}");
    assert!(after.contains(
        &r#"(xyz.tonk.table.cell id:table/book/s1!B2 at: "B2" content: "5" sheet: id:table/book/sheet1)"#
            .to_string()
    ));
    assert!(!after.iter().any(|row| row.contains("s1!C4")), "{after:#?}");
    assert!(
        after
            .iter()
            .any(|row| row.contains(r#"at: "E1" content: "Note""#)
                && !row.starts_with("(xyz.tonk.table.cell id:table/book/")),
        "{after:#?}"
    );
    assert_eq!(
        report(
            &mut notebook,
            "(select (xyz.tonk.table.cell/at id:table/book/s1!C4 ?at))"
        )
        .await?,
        vec![r#"(xyz.tonk.table.cell id:table/book/s1!C4 at: "C4")"#]
    );

    notebook
        .submit(
            r#"(assert (table/create-sheet table: id:table/book name: "Sheet2" order: "n" time: 2))"#,
        )
        .await?;
    let made = report(
        &mut notebook,
        r#"(select (table/sheet ?s table: id:table/book name: "Sheet2" order: "n"))"#,
    )
    .await?;
    let entity = |row: &str| row.split_whitespace().nth(1).map(str::to_string);
    let sheet = entity(&made[0]).expect("a sheet");
    notebook
        .submit(&format!(
            r#"(assert (table/rename-sheet sheet: {sheet} name: "Costs"))"#
        ))
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (table/sheet ?s table: id:table/book name: ?name order: ?order))"
        )
        .await?,
        vec![
            format!(r#"(xyz.tonk.table.sheet {sheet} name: "Costs" order: "n" table: id:table/book)"#),
            r#"(xyz.tonk.table.sheet id:table/book/sheet1 name: "Sheet1" order: "m" table: id:table/book)"#
                .to_string(),
        ]
    );

    notebook
        .submit(
            r#"(assert (table/create-column sheet: id:table/book/sheet1 at: "B" width: "80" time: 3))"#,
        )
        .await?;
    let columns = "(select (table/column ?k sheet: id:table/book/sheet1 at: ?at width: ?w))";
    let column = entity(&report(&mut notebook, columns).await?[0]).expect("a column");
    notebook
        .submit(&format!(
            r#"(assert (table/resize-column column: {column} width: "120"))"#
        ))
        .await?;
    assert_eq!(
        report(&mut notebook, columns).await?,
        vec![format!(
            r#"(xyz.tonk.table.column {column} at: "B" sheet: id:table/book/sheet1 width: "120")"#
        )]
    );
    Ok(())
}
