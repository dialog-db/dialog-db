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

#[path = "support/session.rs"]
mod session;
use session::Notebook;

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
      (xyz.todo.add/todo of: ?c is: ?t as: :entity in: :control cardinality: :many)
      (xyz.todo.add/list of: ?c is: ?l as: :entity in: :control cardinality: :many)
      (xyz.todo.add/text of: ?c is: ?x as: :text in: :control cardinality: :many))
    (concept (todo/complete todo: ?t)
      (xyz.todo.complete/todo of: ?c is: ?t as: :entity in: :control cardinality: :many))

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
                 (xyz.todo.add/todo of: ?c is: ?t as: :entity in: :control cardinality: :many)
                 (xyz.todo.add/list of: ?c is: ?l as: :entity in: :control cardinality: :many)
                 (xyz.todo.add/text of: ?c is: ?x as: :text in: :control cardinality: :many))
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
                 (xyz.todo.add/list of: ?c is: ?l as: :entity in: :control cardinality: :many)
                 (xyz.todo.add/text of: ?c is: ?x as: :text in: :control cardinality: :many))
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
                 (xyz.block.place/subject of: ?c is: ?b as: :entity in: :control cardinality: :many)
                 (xyz.block.place/notebook of: ?c is: ?n as: :entity in: :control cardinality: :many)
                 (xyz.block.place/key of: ?c is: ?k as: :text in: :control cardinality: :many))
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
                 (xyz.todo.ping/todo of: ?c is: ?t as: :entity in: :control cardinality: :many))
               (assert (todo/ping todo: ?t) (when (todo ?t text: "Buy milk" ...)))"#,
        )
        .await;
    assert!(
        matches!(why(command), Err(Ok(Error::Unsupported(said))) if said.contains("whether a command at the top level keeps"))
    );
    Ok(())
}

/// A read that leaves a field out, saying so with `...`, matches
/// anything there, in a select, a query and a rule's premise: the field
/// is still read, so an entity that holds no claim of it is no instance.
/// A write through a concept says every field.
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
               (rule (xyz.said ?t text: ?x) (todo ?t text: ?x ...))
               (query (todo/text ?t is: ?x) (todo ?t text: ?x ...))"#,
        )
        .await?;
    let both = vec![
        r#"(xyz.todo todo:1 text: "Call mom")"#,
        r#"(xyz.todo todo:2 text: "Buy milk")"#,
    ];
    assert_eq!(
        report(&mut notebook, "(select (todo ?t text: ?x ...))").await?,
        both
    );
    assert_eq!(
        report(&mut notebook, "(select (todo/text ?t is: ?x))").await?,
        vec![
            r#"(todo/text todo:1 is: "Call mom")"#,
            r#"(todo/text todo:2 is: "Buy milk")"#,
        ]
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
                 (xyz.block.insert/block of: ?c is: ?x as: :entity in: :control cardinality: :many)
                 (xyz.block.insert/notebook of: ?c is: ?b as: :entity in: :control cardinality: :many)
                 (xyz.block.insert/after of: ?c is: ?lo as: :text in: :control cardinality: :many)
                 (xyz.block.insert/before of: ?c is: ?hi as: :text in: :control cardinality: :many))
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
                 (xyz.todo.add/list of: ?c is: ?l as: :entity in: :control cardinality: :many)
                 (xyz.todo.add/text of: ?c is: ?x as: :text in: :control cardinality: :many))
               (rule (assert (todo ?c list: ?l text: ?x))
                 (todo/add of: ?c list: ?l text: ?x))"#,
        )
        .await?;
    notebook
        .submit(r#"(assert (todo/add list: list:home text: "Buy milk"))"#)
        .await?;
    let todos = report(&mut notebook, "(select (todo ?t text: ?x ...))").await?;
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

/// A query whose answer is markup, asked, compiles to its plan, which
/// nothing writes: a document that defines and asks one is written, its
/// view left to whoever renders it.
#[dialog_common::test]
async fn it_compiles_a_view_to_its_plan() -> Result<()> {
    let document = r#"(concept (todo ?t title: ?title)
                        (xyz.todo/title of: ?t is: ?title as: :text cardinality: :one))
                      (assert (todo todo:1 title: "Buy milk"))
                      (query (todo/row ?t is: ?row) (todo ?t title: ?title) (view ?row [:li ?title]))
                      (select (todo/row ?t is: ?row))"#;
    let mut notebook = Notebook::open().await?;
    let (rows, diagnostics) = notebook.submit(document).await?;
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    assert_eq!(rows, vec![r#"[:li "Buy milk"]"#]);
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
    assert_eq!(
        (plan.name.as_str(), plan.entity.as_str()),
        ("todo/row", "t")
    );
    assert_eq!(plan.layers.len(), 1);
    assert_eq!(
        plan.template,
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
    let checked = dialog_notation::check(&mut analyzer, include_str!("fixtures/todo.clj"));
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
            ("xyz.todo.add/list", Some("one"), Some("control")),
            ("xyz.todo.add/text", Some("one"), Some("control")),
        ]
    );
    assert_eq!(add.descriptors.len(), 1);

    // A line's description is the comment above it. A query is declared
    // beside the concepts, with whether its answer is markup.
    let checked = dialog_notation::check(
        &mut dialog_notation::Analyzer::new()?,
        "; A note.
(concept (note ?n text: ?x)
  ; What it says.
  (xyz.note/text of: ?n is: ?x as: :text cardinality: :many))

; One note, as a row.
(query (note/row ?n is: ?row) (note ?n text: ?x) (view ?row [:li ?x]))",
    );
    let [
        dialog_notation::Declaration::Concept(note),
        dialog_notation::Declaration::Query {
            description,
            name,
            markup,
        },
    ] = checked.declarations.as_slice()
    else {
        panic!("a concept and a view: {:?}", checked.declarations);
    };
    assert_eq!(note.lines[0].description.as_deref(), Some("What it says."));
    // No cardinality written: the line reads every claim.
    assert_eq!(note.lines[0].cardinality.as_deref(), Some("many"));
    assert_eq!(description.as_deref(), Some("One note, as a row."));
    assert_eq!((name.as_str(), *markup), ("note/row", true));
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
                 (xyz.block.remove/subject of: ?c is: ?b as: :entity in: :control cardinality: :many))
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

/// The to-dos the write tests below start from: two, each with all
/// three fields, and a command that names one to drop.
const DROPPABLE: &str = r#"(concept (todo ?t title: ?title state: ?s list: ?l)
      (xyz.todo/title of: ?t is: ?title as: :text cardinality: :one)
      (xyz.todo/state of: ?t is: ?s as: [:open :done] cardinality: :one)
      (xyz.todo/list of: ?t is: ?l as: :entity cardinality: :one))
    (concept (todo/drop subject: ?t)
      (xyz.todo.drop/subject of: ?c is: ?t as: :entity in: :control cardinality: :many))
    (assert (todo todo:1 title: "A" state: :open list: list:1))
    (assert (todo todo:2 title: "B" state: :done list: list:1))"#;

/// What every field of the two to-dos holds, field by field.
async fn droppable(notebook: &mut Notebook) -> Result<Vec<String>> {
    let mut held = Vec::new();
    for field in ["title", "state", "list"] {
        held.extend(report(notebook, &format!("(select (xyz.todo ?t {field}: ?x))")).await?);
    }
    Ok(held)
}

/// A write at the top level with no premise needs its entity: one whose
/// entity is a variable nothing binds is refused, whether it names
/// every field, some, or none, and nothing is retracted. By its entity
/// alone, ground, it retracts that entity's fields.
#[dialog_common::test]
async fn it_refuses_a_top_level_write_whose_entity_nothing_binds() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(DROPPABLE).await?;
    let before = droppable(&mut notebook).await?;
    assert_eq!(before.len(), 6, "{before:?}");
    for text in [
        "(retract (todo ?t state: :open))",
        "(retract (todo ?t title: ?x state: :open list: ?l))",
        "(retract (todo ?t))",
    ] {
        let refused = notebook
            .submit(text)
            .await
            .map_err(|error| error.downcast::<Error>());
        assert!(
            matches!(&refused, Err(Ok(Error::Refused(why)))
                if why == "a write with no premise needs its entity: xyz.todo"),
            "{text}: {refused:?}"
        );
        assert_eq!(droppable(&mut notebook).await?, before, "{text}");
    }
    notebook.submit("(retract (todo todo:1))").await?;
    assert_eq!(
        droppable(&mut notebook).await?,
        vec![
            r#"(xyz.todo todo:2 title: "B")"#,
            "(xyz.todo todo:2 state: :done)",
            "(xyz.todo todo:2 list: list:1)",
        ]
    );
    Ok(())
}

/// A to-do replaced through its concept in one round: a rule retracts
/// the record, every field named, and another asserts it changed. What
/// both write holds afterwards (#863): finishing leaves the title, the
/// list and `state: :done`, and a rename to the title it had leaves the
/// to-do as it was.
#[dialog_common::test]
async fn it_replaces_a_record_by_retracting_it_and_asserting_it_changed() -> Result<()> {
    let replace = r#"(concept (todo/finish subject: ?t)
          (xyz.todo.finish/subject of: ?c is: ?t as: :entity in: :control cardinality: :many))
        (concept (todo/rename subject: ?t title: ?x)
          (xyz.todo.rename/subject of: ?c is: ?t as: :entity in: :control cardinality: :many)
          (xyz.todo.rename/title of: ?c is: ?x as: :text in: :control cardinality: :many))
        (rule (retract (todo ?t title: ?title state: ?s list: ?l))
          (todo/finish subject: ?t)
          (todo ?t title: ?title state: ?s list: ?l))
        (rule (assert (todo ?t title: ?title state: :done list: ?l))
          (todo/finish subject: ?t)
          (todo ?t title: ?title state: ?s list: ?l))
        (rule (retract (todo ?t title: ?title state: ?s list: ?l))
          (todo/rename subject: ?t title: ?new)
          (todo ?t title: ?title state: ?s list: ?l))
        (rule (assert (todo ?t title: ?new state: ?s list: ?l))
          (todo/rename subject: ?t title: ?new)
          (todo ?t title: ?title state: ?s list: ?l))"#;
    let mut notebook = Notebook::open().await?;
    notebook.submit(&format!("{DROPPABLE} {replace}")).await?;
    notebook
        .submit("(assert (todo/finish subject: todo:1))")
        .await?;
    let finished = vec![
        r#"(xyz.todo todo:1 title: "A")"#,
        r#"(xyz.todo todo:2 title: "B")"#,
        "(xyz.todo todo:1 state: :done)",
        "(xyz.todo todo:2 state: :done)",
        "(xyz.todo todo:1 list: list:1)",
        "(xyz.todo todo:2 list: list:1)",
    ];
    assert_eq!(droppable(&mut notebook).await?, finished);
    notebook
        .submit(r#"(assert (todo/rename subject: todo:1 title: "A"))"#)
        .await?;
    assert_eq!(droppable(&mut notebook).await?, finished);
    notebook
        .submit(r#"(assert (todo/rename subject: todo:1 title: "C"))"#)
        .await?;
    assert_eq!(
        droppable(&mut notebook).await?,
        vec![
            r#"(xyz.todo todo:1 title: "C")"#,
            r#"(xyz.todo todo:2 title: "B")"#,
            "(xyz.todo todo:1 state: :done)",
            "(xyz.todo todo:2 state: :done)",
            "(xyz.todo todo:1 list: list:1)",
            "(xyz.todo todo:2 list: list:1)",
        ]
    );
    // A field of many claims tells whether the retract fired: retagging
    // `x` to `z` leaves `y` and `z`, where an assert alone would leave
    // `x` too; retagging `y` to `y` keeps `y`.
    let mut tagged = Notebook::open().await?;
    tagged
        .submit(
            r#"(concept (item ?t title: ?title tag: ?g)
                 (xyz.item/title of: ?t is: ?title as: :text cardinality: :one)
                 (xyz.item/tag of: ?t is: ?g as: :text cardinality: :many))
               (concept (item/retag subject: ?t from: ?from to: ?to)
                 (xyz.item.retag/subject of: ?c is: ?t as: :entity in: :control cardinality: :many)
                 (xyz.item.retag/from of: ?c is: ?from as: :text in: :control cardinality: :many)
                 (xyz.item.retag/to of: ?c is: ?to as: :text in: :control cardinality: :many))
               (rule (retract (item ?t title: ?title tag: ?from))
                 (item/retag subject: ?t from: ?from to: ?to)
                 (item ?t title: ?title tag: ?from))
               (rule (assert (item ?t title: ?title tag: ?to))
                 (item/retag subject: ?t from: ?from to: ?to)
                 (item ?t title: ?title tag: ?from))
               (assert (item item:1 title: "One" tag: "x"))
               (assert (item item:1 title: "One" tag: "y"))"#,
        )
        .await?;
    let tags = "(select (xyz.item ?t tag: ?g))";
    tagged
        .submit(r#"(assert (item/retag subject: item:1 from: "x" to: "z"))"#)
        .await?;
    let after = vec![
        r#"(xyz.item item:1 tag: "y")"#.to_string(),
        r#"(xyz.item item:1 tag: "z")"#.to_string(),
    ];
    assert_eq!(report(&mut tagged, tags).await?, after);
    assert_eq!(
        report(&mut tagged, "(select (xyz.item ?t title: ?x))").await?,
        vec![r#"(xyz.item item:1 title: "One")"#]
    );
    tagged
        .submit(r#"(assert (item/retag subject: item:1 from: "y" to: "y"))"#)
        .await?;
    assert_eq!(report(&mut tagged, tags).await?, after);
    Ok(())
}

/// One field is retracted through its attribute's relation, at the top
/// level or in a rule's conclusion, and the concept's other fields stay:
/// a retract through the concept that names some fields is refused.
#[dialog_common::test]
async fn it_retracts_one_field_through_the_attributes_relation() -> Result<()> {
    let kept = vec![
        r#"(xyz.todo todo:1 title: "A")"#,
        r#"(xyz.todo todo:2 title: "B")"#,
        "(xyz.todo todo:2 state: :done)",
        "(xyz.todo todo:1 list: list:1)",
        "(xyz.todo todo:2 list: list:1)",
    ];
    let mut top = Notebook::open().await?;
    top.submit(DROPPABLE).await?;
    top.submit("(retract (xyz.todo/state of: todo:1 is: :open))")
        .await?;
    assert_eq!(droppable(&mut top).await?, kept);

    let mut ruled = Notebook::open().await?;
    ruled
        .submit(&format!(
            "{DROPPABLE}
             (rule (retract (xyz.todo/state of: ?t is: :open))
               (todo/drop subject: ?t))"
        ))
        .await?;
    ruled.submit("(assert (todo/drop subject: todo:1))").await?;
    assert_eq!(droppable(&mut ruled).await?, kept);

    let mut narrow = Notebook::open().await?;
    narrow.submit(DROPPABLE).await?;
    let refused = narrow
        .submit(
            "(rule (retract (todo ?t state: :open))
               (todo/drop subject: ?t))",
        )
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&refused, Err(Ok(Error::Refused(why)))
            if why.contains("a write says every field") && why.contains("write the relation itself")),
        "{refused:?}"
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
                 (xyz.todo.add/todo of: ?c is: ?i as: :entity in: :control cardinality: :many)
                 (xyz.todo.add/list of: ?c is: ?l as: :entity in: :control cardinality: :many)
                 (xyz.todo.add/title of: ?c is: ?t as: :text in: :control cardinality: :many))
               (concept (todo/complete todo: ?i)
                 (xyz.todo.complete/todo of: ?c is: ?i as: :entity in: :control cardinality: :many))

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
        report(&mut notebook, "(select (todo ?i title: ?t state: ?s ...))").await?,
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
        report(
            &mut notebook,
            "(select (todo ?i title: ?t state: :open ...))"
        )
        .await?,
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

/// An elector in a concept's head chooses one of a field's claims. Over
/// a listed `as:`, `min` and `max` rank by the listing, written least
/// first: `min` is the one listed first, `max` the one listed last.
/// Under `:many` a write adds a claim and displaces none, so the read
/// gives the least or the greatest of what is held. The same elector
/// written on the line's value elects alike. A listed `as:` with no
/// elector chooses nothing: the read gives every claim.
#[dialog_common::test]
async fn it_elects_a_claim_by_the_elector_in_a_concepts_head() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook
        .submit(
            r#"(concept (account ?a status: (min ?s))
                 (xyz.account/status of: ?a is: ?s
                   as: [:suspended :active :registered :archived] cardinality: :many))
               (concept (standing ?a status: ?s)
                 (xyz.account/status of: ?a is: (min ?s)
                   as: [:suspended :active :registered :archived] cardinality: :many))
               (concept (latest ?a status: (max ?s))
                 (xyz.account/status of: ?a is: ?s
                   as: [:suspended :active :registered :archived] cardinality: :many))
               (concept (statuses ?a status: ?s)
                 (xyz.account/status of: ?a is: ?s
                   as: [:suspended :active :registered :archived] cardinality: :many))"#,
        )
        .await?;
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    // Written so that neither the first write nor the last is the least
    // or the greatest: the elector ranks by the listing, not by when.
    for status in [":active", ":suspended", ":archived", ":registered"] {
        notebook
            .submit(&format!("(assert (account acct:1 status: {status}))"))
            .await?;
    }
    for read in [
        "(select (account ?a status: ?s))",
        "(select (standing ?a status: ?s))",
    ] {
        assert_eq!(
            report(&mut notebook, read).await?,
            vec!["(xyz.account acct:1 status: :suspended)"],
            "{read}"
        );
    }
    assert_eq!(
        report(&mut notebook, "(select (latest ?a status: ?s))").await?,
        vec!["(xyz.account acct:1 status: :archived)"]
    );
    assert_eq!(
        report(&mut notebook, "(select (statuses ?a status: ?s))").await?,
        vec![
            "(xyz.account acct:1 status: :active)",
            "(xyz.account acct:1 status: :archived)",
            "(xyz.account acct:1 status: :registered)",
            "(xyz.account acct:1 status: :suspended)",
        ]
    );
    Ok(())
}

/// `top` is no elector: a document that writes it, in a concept's head,
/// on a line's value or as a pick, is refused with a message naming
/// what replaces it, `max` over the list reversed or `min` over the
/// same list, and nothing is declared
/// by it.
#[dialog_common::test]
async fn it_refuses_top_naming_min_over_the_same_list() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    for text in [
        r#"(concept (account ?a status: (top ?s))
             (xyz.account/status of: ?a is: ?s as: [:suspended :active] cardinality: :many))"#,
        r#"(concept (account ?a status: ?s)
             (xyz.account/status of: ?a is: (top ?s) as: [:suspended :active] cardinality: :many))"#,
        r#"(concept (account ?a status: ?s)
             (xyz.account ?a status: (:top (:among (:entity ?s) cases: [:suspended :active]))))"#,
    ] {
        let refused = notebook
            .submit(text)
            .await
            .map_err(|error| error.downcast::<Error>());
        assert!(
            matches!(&refused, Err(Ok(Error::Refused(why))) if why.contains("`top`") && why.contains("`max`") && why.contains("reversed") && why.contains("`min`")),
            "{text}: {refused:?}"
        );
    }
    // Nothing was declared: `account` is no concept.
    let after = notebook
        .submit("(select (account ?a status: ?s))")
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&after, Err(Ok(Error::Unknown(form))) if form == "(account ?a status: ?s)"),
        "{after:?}"
    );
    Ok(())
}

/// A concept's, a query's and a command's name is a name, `todo`, or a
/// name and a `/`, `notebook/blocks`, and has no `.` anywhere: a name
/// with a `.` is a domain, `xyz.memo`, or an attribute in one,
/// `xyz.memo/note`, read directly (the maintainer). One declared under
/// such a name is refused, written or made by a macro, saying so, and
/// nothing of the document is written: the domain and the attribute go
/// on answering for themselves, the attribute's two-term read too.
#[dialog_common::test]
async fn it_refuses_a_concept_query_or_command_whose_name_has_a_dot() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(assert (xyz.memo x:1 text: "the domain's own"))
               (assert (xyz.memo/note of: x:2 is: "the attribute's own"))"#,
        )
        .await?;
    let line = "(xyz.other/text of: ?t is: ?x as: :text cardinality: :one)";
    let mut texts: Vec<(String, &str)> = Vec::new();
    for name in ["xyz.memo", "xyz.memo/note", "a.b/c", "a/b.c"] {
        texts.push((format!("(concept ({name} ?t text: ?x) {line})"), name));
        texts.push((
            format!(
                "(rule (syntax (concept (?name ?t text: ?x) {line}))
                   (expand (memo-kind ?name)))
                 (memo-kind {name})"
            ),
            name,
        ));
        texts.push((
            format!("(query ({name} ?t is: ?x) (xyz.other ?t text: ?x))"),
            name,
        ));
        texts.push((
            format!(
                "(concept ({name} subject: ?x)
                   (xyz.go/subject of: ?c is: ?x as: :entity in: :control cardinality: :many))"
            ),
            name,
        ));
    }
    for (text, name) in &texts {
        let refused = notebook
            .submit(text)
            .await
            .map_err(|error| error.downcast::<Error>());
        assert!(
            matches!(&refused, Err(Ok(Error::Refused(why)))
                if why.contains(&format!("`{name}`")) && why.contains("has none")),
            "{text}: {refused:?}"
        );
    }
    assert_eq!(
        report(&mut notebook, "(select (xyz.memo ?t text: ?x))").await?,
        vec![r#"(xyz.memo x:1 text: "the domain's own")"#]
    );
    assert_eq!(
        report(&mut notebook, "(select (xyz.memo/note ?t ?x))").await?,
        vec![r#"(xyz.memo x:2 note: "the attribute's own")"#]
    );
    Ok(())
}

/// A name a macro's expansion declares as a concept is declared when
/// that form is expanded. A rule of the same document that reads it
/// above the use is expanded again once the use has declared it, as a
/// read above a `(concept …)` is, so the two orders answer the same: a
/// name of words joined by `/` with no `.` is never a stored attribute,
/// so the first try reads nothing else. A name no form ever declares is
/// refused once, saying what is missing, and the retries end.
#[dialog_common::test]
async fn it_reads_a_name_the_document_declares_after_it_once_declared() -> Result<()> {
    let kind = "(rule (syntax (concept (?name ?t is: ?x)
                  (xyz.memo/text of: ?t is: ?x as: :text cardinality: :one)))
                (expand (memo-kind ?name)))";
    let rule = "(rule (xyz.out ?t v: ?x) (memo/note ?t is: ?x))";
    let declare = "(memo-kind memo/note)";
    let open = || async {
        let mut notebook = Notebook::open().await?;
        notebook
            .submit(r#"(assert (xyz.memo/text x:1 is: "the concept's"))"#)
            .await?;
        anyhow::Ok(notebook)
    };
    for order in [[kind, rule, declare], [kind, declare, rule]] {
        let mut notebook = open().await?;
        notebook.submit(&order.join("\n")).await?;
        assert_eq!(
            report(&mut notebook, "(select (xyz.out ?t v: ?x))").await?,
            vec![r#"(xyz.out x:1 v: "the concept's")"#],
            "{order:?}"
        );
    }
    // Never declared: one refusal, in the author's terms, and nothing
    // written.
    let mut notebook = open().await?;
    let checked = dialog_notation::check(
        &mut dialog_notation::Analyzer::new()?,
        &[kind, rule].join("\n"),
    );
    let refusals: Vec<_> = checked.refusals().collect();
    assert_eq!(refusals.len(), 1, "{refusals:?}");
    assert!(
        refusals[0].message.contains(
            "`memo/note` names no attribute of a domain, which holds a `.`, `xyz.memo/note`, and \
             no concept `memo/note`, nor a concept `memo` it would be derived under, is declared \
             above what reads it"
        ),
        "{refusals:?}"
    );
    let refused = notebook
        .submit(&[kind, rule].join("\n"))
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&refused, Err(Ok(Error::Refused(why))) if why.contains("no concept `memo/note`")),
        "{refused:?}"
    );
    assert!(
        report(&mut notebook, "(select (xyz.out ?t v: ?x))")
            .await?
            .is_empty()
    );
    Ok(())
}

/// The same holds of an attribute named under a concept a macro's
/// expansion declares: read above the use or below it, it is the
/// concept's derived attribute, which no rule here concludes. What is
/// still refused is a read that did read another relation than the one
/// a later form makes the name: a derived attribute `list/count` of the
/// concept `list`, read before a macro's use declares a concept
/// `list/count`, saying to declare the concept above. (A `(concept …)`
/// is declared before what reads it wherever it is written.)
#[dialog_common::test]
async fn it_reads_under_a_concept_the_document_declares_after_it_as_declared_above() -> Result<()> {
    let kind = "(rule (syntax (concept (?name ?t name: ?x)
                  (xyz.list/name of: ?t is: ?x as: :text cardinality: :one)))
                (expand (list-kind ?name)))";
    let rule = "(rule (xyz.out ?t v: ?x) (list/count ?t ?x))";
    let declare = "(list-kind list)";
    for order in [[kind, rule, declare], [kind, declare, rule]] {
        let mut notebook = Notebook::open().await?;
        notebook
            .submit(&format!(
                r#"{} (assert (xyz.list/name of: list:1 is: "a"))"#,
                order.join("\n")
            ))
            .await?;
        assert!(
            report(&mut notebook, "(select (xyz.out ?t v: ?x))")
                .await?
                .is_empty(),
            "{order:?}"
        );
    }
    // A macro's use declares `list/count` a concept after the rule read
    // it as the derived attribute of `list`: the rule read another
    // relation, and is refused, saying to declare the concept above.
    let counted = "(rule (syntax (concept (?name ?t is: ?x)
                     (xyz.list/count of: ?t is: ?x as: :text cardinality: :one)))
                   (expand (count-kind ?name)))";
    let mut notebook = Notebook::open().await?;
    let refused = notebook
        .submit(&format!(
            "{kind} {declare} {counted} {rule} (count-kind list/count)"
        ))
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&refused, Err(Ok(Error::Refused(why)))
            if why.contains("`list/count`") && why.contains("declare the concept above")),
        "{refused:?}"
    );
    Ok(())
}

/// Every attribute line of a concept or a command says its cardinality:
/// one without `cardinality:` is refused at that form, saying to write
/// `cardinality: :one` or `cardinality: :many`, and nothing of the
/// document is declared. A line read in a rule's body or a select is a
/// read, which takes every claim, and needs none.
#[dialog_common::test]
async fn it_refuses_a_concept_line_that_says_no_cardinality() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    for text in [
        "(concept (memo ?t text: ?x)
           (xyz.memo/text of: ?t is: ?x as: :text))",
        "(concept (memo/add text: ?x)
           (xyz.memo.add/text of: ?c is: ?x as: :text in: :control))",
    ] {
        let refused = notebook
            .submit(text)
            .await
            .map_err(|error| error.downcast::<Error>());
        assert!(
            matches!(&refused, Err(Ok(Error::Refused(why)))
                if why.contains("cardinality: :one") && why.contains("cardinality: :many")),
            "{text}: {refused:?}"
        );
    }
    let unknown = notebook
        .submit("(select (memo ?t text: ?x))")
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&unknown, Err(Ok(Error::Unknown(_)))),
        "{unknown:?}"
    );
    notebook
        .submit(
            r#"(assert (xyz.memo x:1 text: "a"))
               (rule (xyz.out ?t v: ?x) (xyz.memo/text of: ?t is: ?x))"#,
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (xyz.out ?t v: ?x))").await?,
        vec![r#"(xyz.out x:1 v: "a")"#]
    );
    Ok(())
}

/// A derived attribute's relation is named by a hash, which no person
/// is shown: an answer asked once, an answer kept standing, and what a
/// check of the fixtures says all name it as it is written.
#[dialog_common::test]
async fn it_shows_no_hash_of_a_derived_attribute() -> Result<()> {
    let concepts = r#"
        (concept (list ?l name: ?n)
          (xyz.list/name of: ?l is: ?n as: :text cardinality: :one))
        (rule (list/label ?l ?n) (list ?l name: ?n))"#;
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(&format!(
            r#"{concepts} (assert (list list:home name: "Home"))"#
        ))
        .await?;
    let asked = report(&mut notebook, "(select (list/label ?l ?n))").await?;
    assert_eq!(asked, vec![r#"(list/label list:home is: "Home")"#]);
    // Kept standing, through a query that reads it.
    let checked = dialog_notation::check(
        &mut dialog_notation::Analyzer::new()?,
        &format!(
            "{concepts}
             (query (list/card ?l is: ?card) (list/label ?l ?n) (is ?card {{:label ?n}}))
             (select (list/card ?l is: ?card))"
        ),
    );
    let selection = checked
        .steps
        .iter()
        .find_map(|step| match step {
            dialog_notation::Step::Select(selection) => Some((**selection).clone()),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("a selection: {:?}", checked.diagnostics))?;
    let live = dialog_notation::live::Live::new(notebook.branch.clone());
    let mut standing = live.standing(selection)?;
    let kept = standing.poll(&notebook.operator).await?.unwrap_or_default();
    let kept: Vec<String> = kept.iter().map(ToString::to_string).collect();
    assert_eq!(kept, vec![r#"(list/card list:home is: {:label "Home"})"#]);
    let mut said = Vec::new();
    for fixture in [
        include_str!("fixtures/todo.clj"),
        include_str!("fixtures/notebook.clj"),
        include_str!("fixtures/table.clj"),
    ] {
        let checked = dialog_notation::check(
            &mut dialog_notation::Analyzer::new()?,
            &format!("{}\n{fixture}", include_str!("fixtures/core.clj")),
        );
        said.extend(
            checked
                .diagnostics
                .iter()
                .map(|diagnostic| diagnostic.message.clone()),
        );
    }
    // What is refused says something: a fold in a derived attribute's
    // head, and the notebook read without core, which refuses three
    // uses of what core declares.
    for text in [
        format!("{concepts} (rule (list/count-open ?l (count ?t)) (xyz.todo ?t list: ?l))"),
        include_str!("fixtures/notebook.clj").to_string(),
    ] {
        let checked = dialog_notation::check(&mut dialog_notation::Analyzer::new()?, &text);
        said.extend(
            checked
                .diagnostics
                .iter()
                .map(|diagnostic| diagnostic.message.clone()),
        );
    }
    assert!(
        said.iter().any(|said| said.contains("list/count-open")),
        "{said:?}"
    );
    assert!(said.len() > 1, "{said:?}");
    let hashed: Vec<&String> = said
        .iter()
        .chain(&kept)
        .filter(|said| said.contains("dialog.n.c."))
        .collect();
    assert!(hashed.is_empty(), "{hashed:?}");
    Ok(())
}

/// A concept whose body reads another concept is a concept, whatever
/// its name: under a name with a `/` whose first part is a declared
/// concept it is read through its body, not as a derived attribute that
/// nothing concludes, and so is one whose field is an entry with a
/// constant key. In a document of its own or beside its parent alike,
/// and above the concept it reads or below it. One whose name has no
/// `/` is a concept too: an attribute is derived under it.
#[dialog_common::test]
async fn it_reads_a_concept_over_another_concept_under_a_name_with_a_slash() -> Result<()> {
    let alt = r#"(concept (alt ?d v: ?v)
           (xyz.alt/v of: ?d is: ?v as: :text cardinality: :one))
         (concept (doc ?d title: ?t)
           (xyz.doc.meta/title of: ?d is: ?t as: :text cardinality: :one))
         (assert (alt doc:1 v: "x"))
         (assert (doc doc:1 title: "T"))"#;
    let plain = "(concept (doc/plain ?d part: ?v) (alt ?d v: ?v))";
    let keyed = r#"(concept (doc/parts ?d part: {"a" ?v}) (alt ?d v: ?v))"#;
    for together in [false, true, true] {
        let mut notebook = Notebook::open().await?;
        if together {
            // Above what it reads or below it alike.
            notebook.submit(&format!("{alt} {plain} {keyed}")).await?;
            notebook = Notebook::open().await?;
            notebook.submit(&format!("{plain} {keyed} {alt}")).await?;
        } else {
            notebook.submit(alt).await?;
            notebook.submit(plain).await?;
            notebook.submit(keyed).await?;
        }
        assert_eq!(
            report(&mut notebook, "(select (doc/plain ?d part: ?p))").await?,
            vec![r#"(xyz.alt doc:1 v: "x")"#],
            "together: {together}"
        );
        assert_eq!(
            report(&mut notebook, r#"(select (doc/parts ?d part: {"a" ?w}))"#).await?,
            vec![r#"(xyz.alt doc:1 v: "x")"#],
            "together: {together}"
        );
    }
    // A concept with no `/` over another, in either order, and an
    // attribute derived under it, in the same document and in a later one.
    let plain = "(concept (plain ?d part: ?v) (alt ?d v: ?v))";
    let derived = "(rule (plain/len ?d ?v) (plain ?d part: ?v))";
    for (first, second) in [(alt, plain), (plain, alt)] {
        let mut notebook = Notebook::open().await?;
        notebook
            .submit(&format!("{first} {second} {derived}"))
            .await?;
        assert_eq!(
            report(&mut notebook, "(select (plain/len ?d ?v))").await?,
            vec![r#"(plain/len doc:1 is: "x")"#]
        );
        let mut later = Notebook::open().await?;
        later.submit(&format!("{first} {second}")).await?;
        later.submit(derived).await?;
        assert_eq!(
            report(&mut later, "(select (plain/len ?d ?v))").await?,
            vec![r#"(plain/len doc:1 is: "x")"#]
        );
    }
    Ok(())
}

/// A refusal names what the author wrote: their form and their
/// variables, never one the compiler made or a name in the notation's
/// own namespace. A head whose field Dialog-DB finds unbound, and a
/// select that names no field, are each said so.
#[dialog_common::test]
async fn it_says_a_refusal_in_the_authors_terms() -> Result<()> {
    let alt = r#"(concept (alt ?d v: ?v)
           (xyz.alt/v of: ?d is: ?v as: :text cardinality: :one))
         (concept (doc ?d title: ?t)
           (xyz.doc.meta/title of: ?d is: ?t as: :text cardinality: :one))
         (concept (doc/plain ?d part: ?v) (alt ?d v: ?v))"#;
    for (text, says) in [
        (
            r#"(rule (doc/parts ?d part: {"a" ?v}) (alt ?d v: ?v))"#,
            "part:",
        ),
        ("(select (doc/plain ?d ...))", "(select (doc/plain ?d ...))"),
    ] {
        let mut notebook = Notebook::open().await?;
        notebook.submit(alt).await?;
        let refused = notebook.submitted(text).await;
        let said = match &refused {
            Err(error) => format!("{error:#}"),
            Ok(done) => panic!("{text} was not refused: {done:?}"),
        };
        assert!(said.contains(says), "{text}: {said}");
        assert!(session::leaks(&said).is_empty(), "{text}: {said}");
    }
    Ok(())
}

/// A label is a name like any other: one under the notation's namespace
/// is `Reserved`, in a rule's head, a premise and a select, so no
/// document concludes or reads a derived attribute by its hash. A label
/// is a field of the read's own domain: one that names an attribute of
/// another domain, `io.contact/email:`, is refused, saying to read that
/// domain by itself. Nothing is written by a refusal.
#[dialog_common::test]
async fn it_refuses_a_label_the_notation_names_or_one_of_another_domain() -> Result<()> {
    let list = r#"(concept (list ?l name: ?n)
          (xyz.list/name of: ?l is: ?n as: :text cardinality: :one))
        (rule (list/label ?l ?n) (list ?l name: ?n))
        (assert (list list:1 name: "real"))"#;
    let mut notebook = Notebook::open().await?;
    notebook.submit(list).await?;
    let hashed = report(&mut notebook, "(select (list/label ?l ?n))").await?;
    assert_eq!(hashed, vec![r#"(list/label list:1 is: "real")"#]);
    for (text, name) in [
        (
            r#"(rule (xyz.f ?l dialog.n.c.abcdefghijklmnopqrstuvwxyz/is: "forged") (xyz.list ?l name: ?n))"#,
            "dialog.n.c.abcdefghijklmnopqrstuvwxyz/is:",
        ),
        (
            r#"(rule (xyz.f ?l dialog.n/x: "forged") (xyz.list ?l name: ?n))"#,
            "dialog.n/x:",
        ),
        (
            "(select (xyz.foo ?e dialog.n.c.abcdefghijklmnopqrstuvwxyz/is: ?v))",
            "dialog.n.c.abcdefghijklmnopqrstuvwxyz/is:",
        ),
        (
            r#"(assert (xyz.foo foo:1 dialog.n.report.q/x: "forged"))"#,
            "dialog.n.report.q/x:",
        ),
    ] {
        let refused = notebook
            .submit(text)
            .await
            .map_err(|error| error.downcast::<Error>());
        assert!(
            matches!(&refused, Err(Ok(Error::Reserved(said))) if said == name),
            "{text}: {refused:?}"
        );
    }
    assert_eq!(
        report(&mut notebook, "(select (list/label ?l ?n))").await?,
        hashed
    );
    for text in [
        "(select (io.person ?p io.contact/email: ?e))",
        "(rule (xyz.out ?p a.b/c: ?e) (xyz.in ?p e: ?e))",
        "(select (list ?l list/label: ?x ...))",
    ] {
        let refused = notebook
            .submit(text)
            .await
            .map_err(|error| error.downcast::<Error>());
        assert!(
            matches!(&refused, Err(Ok(Error::Refused(why))) if why.contains("a label is a field of")),
            "{text}: {refused:?}"
        );
    }
    Ok(())
}

/// A line of a concept says its cardinality whatever else the body
/// holds: beside a read of another concept, a line that says none is
/// refused as one alone is. A body that is a read of a domain, not a
/// line, says no cardinality and reads every claim: it is accepted, as
/// the requirement is on lines.
#[dialog_common::test]
async fn it_refuses_a_line_with_no_cardinality_whatever_the_body_holds() -> Result<()> {
    let alt = "(concept (alt ?t text: ?x)
          (xyz.alt/text of: ?t is: ?x as: :text cardinality: :one))";
    for mixed in [
        "(concept (mix ?t text: ?x v: ?v) (alt ?t text: ?x) (xyz.b/v of: ?t is: ?v))",
        "(concept (mix ?t text: ?x v: ?v) (xyz.b/v of: ?t is: ?v) (alt ?t text: ?x))",
    ] {
        let mut notebook = Notebook::open().await?;
        let refused = notebook
            .submit(&format!("{alt} {mixed}"))
            .await
            .map_err(|error| error.downcast::<Error>());
        assert!(
            matches!(&refused, Err(Ok(Error::Refused(why))) if why.contains("says no cardinality")),
            "{mixed}: {refused:?}"
        );
    }
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (m ?t text: ?x) (xyz.m ?t text: ?x))
               (assert (xyz.m/text of: m:1 is: "a"))
               (assert (xyz.m/text of: m:1 is: "b"))"#,
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (m ?t text: ?x))").await?,
        vec![r#"(xyz.m m:1 text: "a")"#, r#"(xyz.m m:1 text: "b")"#]
    );
    Ok(())
}

/// A concept a macro's use declares is read by a query below it, as by
/// a select, and by one above it, which is expanded again once the use
/// has declared it: the two answer the same.
#[dialog_common::test]
async fn it_reads_a_concept_a_macro_declares_above_a_query() -> Result<()> {
    let kind = "(rule (syntax (concept (?name ?t is: ?x)
           (xyz.memo/text of: ?t is: ?x as: :text cardinality: :one)))
         (expand (memo-kind ?name)))";
    let query = "(query (memo/card ?t is: ?c) (memo/note ?t is: ?x) (is ?c {:text ?x}))";
    let declare = "(memo-kind memo/note)";
    for order in [[kind, declare, query], [kind, query, declare]] {
        let mut notebook = Notebook::open().await?;
        notebook
            .submit(&format!(
                r#"{} (assert (xyz.memo/text of: memo:1 is: "hello"))"#,
                order.join("\n")
            ))
            .await?;
        assert_eq!(
            report(&mut notebook, "(select (memo/card ?t is: ?c))").await?,
            vec![r#"(memo/card memo:1 is: {:text "hello"})"#],
            "{order:?}"
        );
    }
    // A select in the same place, as before.
    let mut selected = Notebook::open().await?;
    selected
        .submit(&format!(
            r#"{kind}
               (memo-kind memo/note)
               (assert (xyz.memo/text of: memo:1 is: "hello"))"#
        ))
        .await?;
    assert_eq!(
        report(&mut selected, "(select (memo/note ?t is: ?x))").await?,
        vec![r#"(xyz.memo memo:1 text: "hello")"#]
    );
    Ok(())
}

/// A name rules installed in an earlier document conclude or read as a
/// derived attribute is not declared as a concept later: the installed
/// rules would keep the derived relation while the same text written now
/// would read the stored one. The declaration is refused, naming where
/// the name was used, and the derived attribute still answers.
#[dialog_common::test]
async fn it_refuses_a_concept_over_a_name_installed_rules_derive() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (todo ?t title: ?x)
                 (xyz.todo/title of: ?t is: ?x as: :text cardinality: :one))
               (rule (todo/tag ?t ?g) (todo ?t title: ?g))
               (rule (xyz.seen ?t v: ?g) (todo/tag ?t ?g))
               (assert (todo todo:1 title: "milk"))"#,
        )
        .await?;
    let before = report(&mut notebook, "(select (xyz.seen ?t v: ?g))").await?;
    assert_eq!(before, vec![r#"(xyz.seen todo:1 v: "milk")"#]);
    for declared in [
        "(concept (todo/tag ?t is: ?g)
           (xyz.todo/tag of: ?t is: ?g as: :text cardinality: :many))",
        "(concept (todo/tag ?t tag: ?g)
           (xyz.todo/tag of: ?t is: ?g as: :text cardinality: :many))",
    ] {
        let refused = notebook
            .submit(declared)
            .await
            .map_err(|error| error.downcast::<Error>());
        assert!(
            matches!(&refused, Err(Ok(Error::Refused(why)))
                if why.contains("todo/tag") && why.contains("(rule (todo/tag ?t ?g)")),
            "{declared}: {refused:?}"
        );
    }
    assert_eq!(
        report(&mut notebook, "(select (xyz.seen ?t v: ?g))").await?,
        before
    );
    Ok(())
}

/// A head whose variable no premise binds is refused naming the
/// variable as the author wrote it and the field it stands in, not the
/// rule's identity or the field Dialog-DB names it by. What `compile`
/// refuses, called directly, names a derived attribute as it is written.
#[dialog_common::test]
async fn it_says_an_unbound_head_variable_as_the_author_wrote_it() -> Result<()> {
    let list = "(concept (list ?l name: ?n)
          (xyz.list/name of: ?l is: ?n as: :text cardinality: :one))";
    for text in [
        "(rule (xyz.out ?t v: ?z) (xyz.in ?t w: ?w))".to_string(),
        "(rule (assert (xyz.out ?t v: ?z)) (xyz.in ?t w: ?w))".to_string(),
        format!("{list} (rule (list/x ?l ?z) (list ?l name: ?n))"),
    ] {
        let mut notebook = Notebook::open().await?;
        let said = match notebook.submitted(&text).await {
            Err(error) => format!("{error:#}"),
            Ok(done) => panic!("{text} was not refused: {done:?}"),
        };
        assert!(said.contains("?z"), "{text}: {said}");
        assert!(session::leaks(&said).is_empty(), "{text}: {said}");
    }
    let text = format!("{list} (rule (list/count-open ?l (count ?t)) (xyz.todo ?t list: ?l))");
    let analysis = dialog_notation::Analyzer::new()?.analyze(&dialog_notation::parse(&text)?);
    let refused: Vec<String> = analysis
        .analyzed
        .iter()
        .filter_map(|tree| dialog_notation::compile(tree).err())
        .map(|error| error.to_string())
        .collect();
    assert!(!refused.is_empty(), "nothing refused");
    for said in refused {
        assert!(said.contains("list/count-open"), "{said}");
        assert!(session::leaks(&said).is_empty(), "{said}");
    }
    Ok(())
}

/// Concepts whose bodies read each other round cannot expand to
/// anything finite: one that reads itself, and whichever is written
/// first of two or of three, the
/// document is refused as a cycle naming every concept on it, and
/// nothing is declared.
#[dialog_common::test]
async fn it_refuses_concepts_that_read_each_other_round_in_any_order() -> Result<()> {
    let two = [
        "(concept (a ?t v: ?x) (b ?t v: ?x))",
        "(concept (b ?t v: ?x) (a ?t v: ?x))",
    ];
    let three = [
        "(concept (p ?t v: ?x) (q ?t v: ?x))",
        "(concept (q ?t v: ?x) (r ?t v: ?x))",
        "(concept (r ?t v: ?x) (p ?t v: ?x))",
    ];
    for (forms, names) in [
        (vec!["(concept (s ?t v: ?x) (s ?t v: ?x))"], vec!["(s "]),
        (vec![two[0], two[1]], vec!["(a ", "(b "]),
        (vec![two[1], two[0]], vec!["(a ", "(b "]),
        (
            vec![three[0], three[1], three[2]],
            vec!["(p ", "(q ", "(r "],
        ),
        (
            vec![three[2], three[1], three[0]],
            vec!["(p ", "(q ", "(r "],
        ),
    ] {
        let mut notebook = Notebook::open().await?;
        let text = forms.join("\n");
        let refused = notebook
            .submit(&text)
            .await
            .map_err(|error| error.downcast::<Error>());
        assert!(
            matches!(&refused, Err(Ok(Error::Cycle(form, through)))
                if names.iter().all(|name| form.contains(name) || through.iter().any(|part| part.contains(name)))),
            "{text}: {refused:?}"
        );
        // Nothing was declared: a later read of either is no concept.
        let read = format!("(select {}?t v: ?x))", names[0]);
        let unknown = notebook
            .submit(&read)
            .await
            .map_err(|error| error.downcast::<Error>());
        assert!(
            matches!(&unknown, Err(Ok(Error::Unknown(_)))),
            "{unknown:?}"
        );
    }
    Ok(())
}

/// What a refused form read as a derived attribute was never installed:
/// checked form by form, a form that reads `todo/tag` so and is refused
/// leaves nothing behind, and a later document declares `todo/tag` a
/// concept.
#[dialog_common::test]
async fn it_keeps_no_derived_use_of_a_form_that_was_refused() -> Result<()> {
    let mut analyzer = dialog_notation::Analyzer::new()?;
    let checked = dialog_notation::check_forms(
        &mut analyzer,
        "(concept (todo ?t title: ?x)
           (xyz.todo/title of: ?t is: ?x as: :text cardinality: :one))
         (rule (xyz.seen ?t v: ?g) (todo/tag ?t ?g) (frobnicate ?t))",
    );
    assert_eq!(checked.refusals().count(), 1, "{:?}", checked.diagnostics);
    let declared = dialog_notation::check(
        &mut analyzer,
        "(concept (todo/tag ?t is: ?g)
           (xyz.todo/tag of: ?t is: ?g as: :text cardinality: :many))",
    );
    assert!(
        declared.refusals().next().is_none(),
        "{:?}",
        declared.diagnostics
    );
    Ok(())
}

/// A derived attribute is named under the concept before its last `/`:
/// with concepts `nb` and `nb/blocks`, `nb/blocks/count` is the field
/// `count` of `nb/blocks`, and no field `blocks/count` of `nb`. With no
/// concept `nb/blocks` it is no derived attribute, and is refused.
#[dialog_common::test]
async fn it_names_a_derived_attribute_under_the_concept_before_its_last_slash() -> Result<()> {
    let nb = "(concept (nb ?l name: ?n)
          (xyz.nb/name of: ?l is: ?n as: :text cardinality: :one))";
    let blocks = "(concept (nb/blocks ?l name: ?n)
          (xyz.nb.blocks/name of: ?l is: ?n as: :text cardinality: :one))";
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(&format!(
            r#"{nb} {blocks}
               (rule (nb/blocks/count ?l ?n) (nb/blocks ?l name: ?n))
               (rule (nb/other ?l ?n) (nb ?l name: ?n))
               (assert (nb/blocks b:1 name: "blocks"))
               (assert (nb b:1 name: "nb"))"#
        ))
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (nb/blocks/count ?l ?n))").await?,
        vec![r#"(nb/blocks/count b:1 is: "blocks")"#]
    );
    let checked = dialog_notation::check(
        &mut dialog_notation::Analyzer::new()?,
        &format!("{nb} (rule (nb/blocks/count ?l ?n) (nb ?l name: ?n))"),
    );
    assert!(
        checked.refusals().next().is_some(),
        "{:?}",
        checked.diagnostics
    );
    Ok(())
}

/// A name the notation writes itself is refused as `Reserved` at every
/// public way in, read into a tree, checked whole or form by form, and
/// said as the document wrote it.
#[dialog_common::test]
async fn it_refuses_a_name_the_notation_writes_at_every_way_in() -> Result<()> {
    use dialog_notation::{Analyzer, Syntax, check, check_forms, read};
    for name in [
        "dialog.n.c/list/label",
        "dialog.n.select",
        "dialog.n.c.abcdefghijklmnopqrstuvwxyz",
    ] {
        let text = format!("(select ({name} ?l is: ?n))");
        for form in read(&text)? {
            assert!(
                matches!(Syntax::try_from(&form), Err(Error::Reserved(said)) if said == name),
                "{text}"
            );
        }
        for checked in [
            check(&mut Analyzer::new()?, &text),
            check_forms(&mut Analyzer::new()?, &text),
        ] {
            let refused: Vec<_> = checked.refusals().collect();
            assert!(
                refused.iter().any(|refused| refused.message.contains(name)
                    && matches!(&refused.refusal, Some(Error::Reserved(said)) if said == name)),
                "{text}: {refused:?}"
            );
            assert!(checked.steps.is_empty(), "{text}");
        }
    }
    Ok(())
}

/// A line, an elector's line and a call at the top level name an
/// attribute of a domain: one that names none, an undeclared `memo/note`,
/// a reserved `dialog.n.c/a/b` or a malformed `a.b/c/d`, is refused
/// naming it as written, and none is read as a stored attribute.
#[dialog_common::test]
async fn it_refuses_a_line_that_names_no_attribute_of_a_domain() -> Result<()> {
    for name in ["memo/note", "dialog.n.c/a/b", "a.b/c/d"] {
        for text in [
            format!("(concept (m ?t v: ?x) ({name} of: ?t is: ?x as: :text cardinality: :one))"),
            format!(
                "(concept (m ?t v: (min ?x)) ({name} of: ?t is: ?x as: [:a :b] cardinality: :many))"
            ),
            format!(
                "(concept (m ?t v: ?x) ({name} of: ?t is: (min ?x) as: [:a :b] cardinality: :many))"
            ),
            format!("({name} x:1 \"a\")"),
            format!("(select ({name} ?t ?x))"),
        ] {
            let mut notebook = Notebook::open().await?;
            let said = match notebook.submitted(&text).await {
                Err(error) => format!("{error:#}"),
                Ok(done) => panic!("{text} was not refused: {done:?}"),
            };
            assert!(said.contains(name), "{text}: {said}");
        }
    }
    Ok(())
}

/// A name is a name: empty, ending or beginning with a `/`, with two
/// `/` together, with space in it or a variable's `?`, it declares no
/// concept, query, command or derived attribute, and is refused naming
/// it.
#[dialog_common::test]
async fn it_declares_nothing_under_a_name_that_is_no_name() -> Result<()> {
    let line = "(xyz.a/v of: ?t is: ?x as: :text cardinality: :one)";
    for name in ["a/", "/a", "a//b", "a/b/"] {
        for text in [
            format!("(concept ({name} ?t v: ?x) {line})"),
            format!("(query ({name} ?t is: ?x) (xyz.a ?t v: ?x))"),
            format!(
                "(concept ({name} subject: ?x)
                   (xyz.go/subject of: ?c is: ?x as: :entity in: :control cardinality: :many))"
            ),
            format!("(concept (a ?t v: ?x) {line}) (rule ({name} ?t ?x) (a ?t v: ?x))"),
        ] {
            let mut notebook = Notebook::open().await?;
            let said = match notebook.submitted(&text).await {
                Err(error) => format!("{error:#}"),
                Ok(done) => panic!("{text} was not refused: {done:?}"),
            };
            assert!(said.contains(name), "{text}: {said}");
        }
    }
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
               (list ?l ...)
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
                 (todo ?t list: ?l ...)
                 (list ?l name: ?n))

               (assert (list list:home name: "Home"))
               (assert (todo todo:1 list: list:home state: :open))"#,
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (todo/home ?t ?n))").await?,
        vec![r#"(todo/home todo:1 is: "Home")"#]
    );
    assert_eq!(
        report(&mut notebook, "(select (todo/home todo:1 is: ?n))").await?,
        vec![r#"(todo/home todo:1 is: "Home")"#]
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
        vec![r#"(todo/home todo:1 is: "Home")"#]
    );
    Ok(())
}

const COUNTER: &str = "
    (concept (counter/increment counter: ?n)
      (xyz.counter.increment/counter of: ?c is: ?n as: :entity in: :control cardinality: :many))
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
               (xyz.counter.reset/counter of: ?c is: ?n as: :entity in: :control cardinality: :many))
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
        matches!(&misspelled, Err(Ok(Error::Refused(why)))
            if why.contains("no concept `counter/incremnt`, nor a concept `counter`")),
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
    // What must not hold may stand anywhere among the premises: before
    // the read it excludes from, it is the same rule.
    assert_eq!(
        report(
            &mut notebook,
            "(select (io.person ?p name: ?n)
               (unless (author ?p ...))
               (io.person ?p name: (:text ?n)))"
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
               (xyz.todo.snooze/todo of: ?c is: ?t as: :entity in: :control cardinality: :many))",
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
                 (shop.restock/item of: ?c is: ?i as: :entity in: :control cardinality: :many)
                 (shop.restock/count of: ?c is: ?n as: :natural in: :control cardinality: :many))

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
      (xyz.todo.move/todo of: ?c is: ?t as: :entity in: :control cardinality: :many)
      (xyz.todo.move/list of: ?c is: ?to as: :entity in: :control cardinality: :many))";

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
               (xyz.todo.moved/todo of: ?c is: ?t as: :entity in: :control cardinality: :many)
               (xyz.todo.moved/to of: ?c is: ?to as: :entity in: :control cardinality: :many)
               (xyz.todo.moved/from of: ?c is: ?from as: :entity in: :control cardinality: :many))
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
               (xyz.list.archive/list of: ?c is: ?l as: :entity in: :control cardinality: :many))
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
                 (xyz.note.jot/text of: ?c is: ?x as: :text in: :control cardinality: :many))
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
                   (xyz.name.first/name of: ?c is: ?n as: :entity in: :control cardinality: :many))
                 (concept (name/second name: ?n)
                   (xyz.name.second/name of: ?c is: ?n as: :entity in: :control cardinality: :many))
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
        serde_json::from_value(json!({ "the": "syntax.form/is", "as": "composite:map" }))?;
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

/// The table's own forms, its views left out: what is run on a branch
/// is the rules, and nothing of a view is written.
fn the_table_without_its_views() -> Result<String> {
    let forms = dialog_notation::read(include_str!("fixtures/table.clj"))?;
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

/// The entity `id` makes in a rule of a number a read gives is the one
/// `(:id …)` names of the same number written. A written `1` is a natural,
/// stored and read as one whatever the attribute's `as:`; and a `1`
/// another writer stores as a signed integer names the same entity too,
/// since `entity/derive` derives from the value's reference, which is
/// one for the two `1`s.
#[dialog_common::test]
async fn it_names_one_entity_of_a_number_read_and_written() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook
        .submit(
            r#"(concept (todo ?t list: ?l n: ?n)
                 (xyz.todo/list of: ?t is: ?l as: :entity cardinality: :one)
                 (xyz.todo/n of: ?t is: ?n as: :integer cardinality: :one))
               (concept (todo/add list: ?l n: ?n)
                 (xyz.todo.add/list of: ?c is: ?l as: :entity in: :control cardinality: :many)
                 (xyz.todo.add/n of: ?c is: ?n as: :integer in: :control cardinality: :many))
               (rule (assert (todo ?t list: ?l n: ?n))
                 (todo/add list: ?l n: ?n)
                 (id ?t list: ?l n: ?n))"#,
        )
        .await?;
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    notebook
        .submit(r#"(assert (todo/add list: list:home n: 1))"#)
        .await?;
    let made = report(&mut notebook, "(select (todo ?t n: ?n ...))").await?;
    assert_eq!(made.len(), 1, "{made:?}");
    notebook
        .submit(r#"(assert (xyz.todo (:id xyz.todo list: list:home n: 1) tag: (:text "shop")))"#)
        .await?;
    let tagged = report(&mut notebook, "(select (xyz.todo ?t tag: ?g))").await?;
    let entity = |row: &str| row.split_whitespace().nth(1).map(str::to_string);
    assert_eq!(entity(&tagged[0]), entity(&made[0]), "{made:?} {tagged:?}");

    use dialog_query::formula::entity::derive;
    let domain = dialog_corpus::Value::String("xyz.todo".into());
    assert_eq!(
        derive(&domain, &dialog_corpus::Value::UnsignedInt(1)),
        derive(&domain, &dialog_corpus::Value::SignedInt(1)),
        "a natural 1 and a signed 1 derive one entity"
    );
    Ok(())
}

/// A formula in a select's head over two reads is the formula of each
/// row's own values: four pairs, each with its one total. Its columns
/// were reported as fields of the entity each, and read back crossed:
/// eight rows, each pair with both totals (tonk's tour, T1).
#[dialog_common::test]
async fn it_reports_a_formula_in_the_head_of_each_rows_own_values() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook
        .submit(
            r#"(concept (estimate ?t hours: ?h)
                 (tour.todo/hours of: ?t is: ?h as: :natural cardinality: :one))
               (assert (estimate todo:milk hours: 1))
               (assert (estimate todo:mum hours: 1))
               (assert (estimate todo:deck hours: 3))
               (assert (estimate todo:seeds hours: 2))"#,
        )
        .await?;
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let rows = report(
        &mut notebook,
        "(select (tour.morning ?a with: ?b total: (+ ?p : ?q))
           (estimate ?a hours: ?p)
           (estimate ?b hours: ?q)
           (< ?p : ?q)
           (<= (+ ?p : ?q) : 4))",
    )
    .await?;
    assert_eq!(
        rows,
        [
            "(tour.morning todo:milk total: 3 with: todo:seeds)",
            "(tour.morning todo:milk total: 4 with: todo:deck)",
            "(tour.morning todo:mum total: 3 with: todo:seeds)",
            "(tour.morning todo:mum total: 4 with: todo:deck)",
        ]
    );
    Ok(())
}

/// What the notebook's commands write, on a branch: an edit writes the
/// block's source, a move leaves the block under its new key alone, a
/// placement too, a removal takes the block's entries and its own
/// facts, and an insert at the tail makes a block with the source, kept
/// in the notebook's sequence.
#[dialog_common::test]
async fn it_runs_the_notebooks_commands() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook
        .submit(&format!(
            "{}\n{}",
            include_str!("fixtures/core.clj"),
            include_str!("fixtures/notebook.clj")
        ))
        .await?;
    assert!(
        diagnostics
            .iter()
            .all(|diagnostic| diagnostic.refusal.is_none()),
        "{diagnostics:?}"
    );
    let scratch = "id:notebook/scratch";
    let keys = |block: &str| format!("(select (notebook/blocks {scratch} block: {{?k {block}}}))");

    notebook
        .submit(&format!(
            r#"(assert (block/edit subject: {scratch}/2 source: "edited" notebook: {scratch}))"#
        ))
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            &format!("(select (notebook/block {scratch}/2 notebook: ?n source: ?s))")
        )
        .await?,
        [format!(
            r#"(xyz.tonk.notebook.block {scratch}/2 notebook: {scratch} source: "edited")"#
        )]
    );

    notebook
        .submit(&format!(
            r#"(assert (block/move subject: {scratch}/3 notebook: {scratch} from: "N9" key: "N7"))"#
        ))
        .await?;
    let moved = report(&mut notebook, &keys(&format!("{scratch}/3"))).await?;
    assert_eq!(moved.len(), 1, "{moved:?}");
    assert!(moved[0].contains(" N7: "), "{moved:?}");

    notebook
        .submit(&format!(
            r#"(assert (block/place subject: {scratch}/3 notebook: {scratch} key: "N8"))"#
        ))
        .await?;
    let placed = report(&mut notebook, &keys(&format!("{scratch}/3"))).await?;
    assert_eq!(placed.len(), 1, "{placed:?}");
    assert!(placed[0].contains(" N8: "), "{placed:?}");

    notebook
        .submit(&format!(
            r#"(assert (block/remove subject: {scratch}/2 notebook: {scratch} key: "N5"))"#
        ))
        .await?;
    assert!(
        report(&mut notebook, &keys(&format!("{scratch}/2")))
            .await?
            .is_empty()
    );
    assert!(
        report(
            &mut notebook,
            &format!("(select (notebook/block {scratch}/2 notebook: ?n source: ?s))")
        )
        .await?
        .is_empty()
    );

    notebook
        .submit(&format!(
            r#"(assert (block/insert notebook: {scratch} source: "tail" next: case:none prev: {scratch}/3))"#
        ))
        .await?;
    let made = report(
        &mut notebook,
        &format!(r#"(select (notebook/block ?b notebook: {scratch} source: "tail"))"#),
    )
    .await?;
    assert_eq!(made.len(), 1, "{made:?}");
    let block = made[0]
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("a block"))?
        .to_string();
    assert_eq!(
        report(&mut notebook, &keys(&block)).await?.len(),
        1,
        "the inserted block is in the sequence"
    );
    Ok(())
}

/// A derived attribute concludes the fields its labels name (gaps row
/// 48, the maintainer's word, 2026-11-25): a head may write `is:` beside
/// another label, and a read of both answers both. Its two terms
/// unlabeled are sugar for `of:` and `is:`, and either spelling reads
/// what the other concluded. A read of a label no rule concludes is
/// accepted and answers nothing. A tree built by hand with labels
/// compiles as the document's does.
#[dialog_common::test]
async fn it_concludes_and_reads_a_derived_attribute_with_labels() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (list ?l name: ?n)
                 (xyz.list/name of: ?l is: ?n as: :text cardinality: :one))
               (rule (list/label ?l is: ?n other: ?n) (list ?l name: ?n))
               (rule (list/title of: ?l is: ?n) (list ?l name: ?n))
               (rule (list/heading ?l ?n) (list ?l name: ?n))
               (assert (list list:home name: "Home"))"#,
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (list/label ?l is: ?n other: ?o))").await?,
        vec![r#"(list/label list:home is: "Home" other: "Home")"#]
    );
    assert_eq!(
        report(&mut notebook, "(select (list/label ?l wat: ?q))").await?,
        Vec::<String>::new()
    );
    for read in [
        "(select (list/title ?l ?n))",
        "(select (list/title of: ?l is: ?n))",
    ] {
        assert_eq!(
            report(&mut notebook, read).await?,
            vec![r#"(list/title list:home is: "Home")"#],
            "{read}"
        );
    }
    assert_eq!(
        report(&mut notebook, "(select (list/heading of: ?l is: ?n))").await?,
        vec![r#"(list/heading list:home is: "Home")"#]
    );
    let tree = dialog_notation::parse(
        "(rule (dialog.n.c/list/tag ?l is: ?n other: ?n) (xyz.list ?l name: ?n))",
    )?
    .remove(0);
    let compiled = dialog_notation::compile(&tree);
    assert!(compiled.is_ok(), "{compiled:?}");
    Ok(())
}

/// A pin, not a decision (gaps row 47): what Dialog-DB refuses of an
/// attribute's domain and field beyond what the notation tells apart is
/// refused when it is compiled, as the whole form the author wrote, at
/// its place in the document, with Dialog-DB's reason. The notation does
/// not mirror these rules; this is the list of what is refused today and
/// with what text.
#[dialog_common::test]
fn it_pins_what_dialog_db_refuses_of_an_attribute_today() -> Result<()> {
    for (text, said) in [
        (
            "(select (Xyz.t ?x name: ?n))",
            "invalid relation \"Xyz.t/name\": domain must start with a lowercase letter",
        ),
        (
            "(select (xyz_q.t ?x name: ?n))",
            "invalid relation \"xyz_q.t/name\": domain must contain only lowercase letters, \
             digits, hyphens, or dots",
        ),
        (
            "(select (xyz.t ?x Name: ?n))",
            "invalid relation \"xyz.t/Name\": name must start with a lowercase letter",
        ),
        (
            "(select (xyz.t ?x 9name: ?n))",
            "invalid relation \"xyz.t/9name\": name must start with a lowercase letter",
        ),
        (
            "(select (xyz.t ?x na_me: ?n))",
            "invalid relation \"xyz.t/na_me\": name must contain only lowercase letters, digits, \
             or hyphens",
        ),
        (
            "(select (xyz.t ?x caf\u{e9}: ?n))",
            "invalid relation \"xyz.t/caf\u{e9}\": name must contain only lowercase letters, \
             digits, or hyphens",
        ),
        (
            "(select (xyz.t ?x name-: ?n))",
            "invalid relation \"xyz.t/name-\": name must not end with a hyphen",
        ),
        (
            "(assert (xyz.t x:1 na_me: 1))",
            "invalid relation \"xyz.t/na_me\": name must contain only lowercase letters, digits, \
             or hyphens",
        ),
    ] {
        let checked = dialog_notation::check(&mut dialog_notation::Analyzer::new()?, text);
        let written = read(text)?;
        assert_eq!(
            checked.diagnostics.len(),
            1,
            "{text}: {:?}",
            checked.diagnostics
        );
        let refused = &checked.diagnostics[0];
        assert_eq!(refused.message, said, "{text}");
        assert_eq!(
            refused.form.as_ref().map(ToString::to_string),
            Some(written[0].to_string()),
            "{text}"
        );
        assert_eq!(
            refused.path,
            vec![dialog_notation::Segment::Item(0)],
            "{text}"
        );
    }
    Ok(())
}

/// At the limits of what Dialog-DB keeps, a document runs end to end: a
/// concept named in 63 bytes is declared, written and read; an attribute
/// of 64 bytes, and a domain whose read makes one of 64, are written and
/// read. One byte more of each is refused (`it_refuses_a_name_dialog_db_
/// cannot_keep_in_the_authors_terms`, `src/expand.rs`).
#[dialog_common::test]
async fn it_runs_a_document_at_the_limits_of_a_name() -> Result<()> {
    let concept = "c".repeat(63);
    let attribute = format!("xyz.{}/name", "a".repeat(55));
    let domain = format!("xyz.{}", "d".repeat(54));
    assert_eq!(
        (concept.len(), attribute.len(), domain.len() + "/name".len()),
        (63, 64, 63)
    );
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(&format!(
            r#"(concept ({concept} ?x name: ?n)
                 ({attribute} of: ?x is: ?n as: :text cardinality: :one))
               (assert ({concept} t:1 name: "kept"))
               (assert ({domain} t:2 name: "also"))"#
        ))
        .await?;
    // The concept is read through its line, and said as the line's
    // domain.
    let kept = format!(
        r#"({} t:1 name: "kept")"#,
        attribute.split_once('/').unwrap().0
    );
    assert_eq!(
        report(&mut notebook, &format!("(select ({concept} ?x name: ?n))")).await?,
        vec![kept.clone()]
    );
    assert_eq!(
        report(&mut notebook, &format!("(select ({attribute} ?x ?n))")).await?,
        vec![kept]
    );
    assert_eq!(
        report(&mut notebook, &format!("(select ({domain} ?x name: ?n))")).await?,
        vec![format!(r#"({domain} t:2 name: "also")"#)]
    );
    Ok(())
}

/// Every claim of `xyz.p`'s fields on `entity`, read from the relation.
async fn claims_of(notebook: &mut Notebook, entity: &str) -> Result<Vec<String>> {
    let mut all = Vec::new();
    for field in ["n: ?v", "tag: ?v", "score: ?v"] {
        let read = format!("(select (xyz.p {entity} {field}) (xyz.p {entity} {field}))");
        all.extend(report(notebook, &read).await?);
    }
    Ok(all)
}

/// A retract through a concept by its entity alone retracts every claim
/// of the concept's fields on that entity, not only the one a read under
/// `cardinality: :one` or an elector picks: two claims of each field, as
/// two documents or two replicas make them, are all gone, read from the
/// relation itself, for a field under `:one`, one under `:many` and one
/// under `max`, at the top level and in a rule a command fires. Another
/// entity's claims stay, and a retract of an entity that holds nothing
/// writes nothing.
#[dialog_common::test]
async fn it_retracts_every_claim_of_a_field_by_its_entity() -> Result<()> {
    let concept = r#"(concept (p ?t n: ?n tag: ?g score: (max ?s))
          (xyz.p/n of: ?t is: ?n as: :text cardinality: :one)
          (xyz.p/tag of: ?t is: ?g as: :text cardinality: :many)
          (xyz.p/score of: ?t is: ?s as: :integer cardinality: :many))
        (concept (p/drop subject: ?t)
          (xyz.p.drop/subject of: ?c is: ?t as: :entity in: :control cardinality: :many))
        (rule (retract (p ?t)) (p/drop subject: ?t))"#;
    for retract in ["(retract (p x:1))", "(assert (p/drop subject: x:1))"] {
        let mut notebook = Notebook::open().await?;
        notebook.submit(concept).await?;
        for (n, g, s) in [("a", "red", 1), ("b", "blue", 2)] {
            notebook
                .submit(&format!(
                    r#"(assert (xyz.p x:1 n: "{n}" tag: "{g}" score: {s}))"#
                ))
                .await?;
        }
        notebook
            .submit(r#"(assert (xyz.p x:2 n: "kept" tag: "kept" score: 9))"#)
            .await?;
        assert_eq!(claims_of(&mut notebook, "x:1").await?.len(), 6);
        notebook.submit(retract).await?;
        assert_eq!(
            claims_of(&mut notebook, "x:1").await?,
            Vec::<String>::new(),
            "{retract}"
        );
        assert_eq!(claims_of(&mut notebook, "x:2").await?.len(), 3, "{retract}");
    }
    // An entity that holds nothing: no commit is made.
    let mut notebook = Notebook::open().await?;
    notebook.submit(concept).await?;
    let before = notebook.branch.revision();
    notebook.submit("(retract (p x:9))").await?;
    assert_eq!(notebook.branch.revision(), before);
    Ok(())
}

/// The claims `entity` holds of each of `fields`, read from each
/// attribute's relation, a `(domain, field)` each.
async fn claims_in(
    notebook: &mut Notebook,
    entity: &str,
    fields: &[(&str, &str)],
) -> Result<Vec<String>> {
    let mut all = Vec::new();
    for (domain, field) in fields {
        let read =
            format!("(select ({domain} {entity} {field}: ?v) ({domain} {entity} {field}: ?v))");
        all.extend(report(notebook, &read).await?);
    }
    Ok(all)
}

/// A retract through a concept by its entity alone retracts every claim
/// of each field the entity holds, whichever of the concept's fields it
/// holds: each field's claims are retracted apart from the others', so
/// a field the entity holds nothing of leaves the rest to go. Every
/// subset of the fields, none included, with one claim and with two of
/// each field held, for a concept in one domain (a field under `:one`
/// and two under `:many`) and one over two domains, at the top level and
/// in a rule a command fires; read after each from the relations
/// themselves: the entity holds nothing of the concept, and another
/// entity holding every field keeps every claim.
#[dialog_common::test]
async fn it_retracts_by_its_entity_whichever_fields_it_holds() -> Result<()> {
    let concepts: [(&str, &[(&str, &str)]); 2] = [
        (
            r#"(concept (p ?t a: ?a b: ?b c: ?c)
                 (xyz.p/a of: ?t is: ?a as: :text cardinality: :one)
                 (xyz.p/b of: ?t is: ?b as: :text cardinality: :many)
                 (xyz.p/c of: ?t is: ?c as: :text cardinality: :many))"#,
            &[("xyz.p", "a"), ("xyz.p", "b"), ("xyz.p", "c")],
        ),
        (
            r#"(concept (p ?t a: ?a b: ?b)
                 (xyz.p/a of: ?t is: ?a as: :text cardinality: :one)
                 (abc.q/b of: ?t is: ?b as: :text cardinality: :many))"#,
            &[("xyz.p", "a"), ("abc.q", "b")],
        ),
    ];
    let command = r#"(concept (p/drop subject: ?t)
          (xyz.p.drop/subject of: ?c is: ?t as: :entity in: :control cardinality: :many))
        (rule (retract (p ?t)) (p/drop subject: ?t))"#;
    for (concept, fields) in concepts {
        for held in 0..(1u32 << fields.len()) {
            for claims in [1, 2] {
                for retract in ["(retract (p x:1))", "(assert (p/drop subject: x:1))"] {
                    let case = format!("{concept} holding {held:b}, {claims} each, {retract}");
                    let mut notebook = Notebook::open().await?;
                    notebook.submit(concept).await?;
                    notebook.submit(command).await?;
                    for (at, (domain, field)) in fields.iter().enumerate() {
                        for claim in 0..claims {
                            if held & (1 << at) != 0 {
                                notebook
                                    .submit(&format!(
                                        r#"(assert ({domain} x:1 {field}: "{claim}"))"#
                                    ))
                                    .await?;
                            }
                        }
                        notebook
                            .submit(&format!(r#"(assert ({domain} x:2 {field}: "kept"))"#))
                            .await?;
                    }
                    assert_eq!(
                        claims_in(&mut notebook, "x:1", fields).await?.len(),
                        held.count_ones() as usize * claims,
                        "{case}"
                    );
                    notebook.submit(retract).await?;
                    assert_eq!(
                        claims_in(&mut notebook, "x:1", fields).await?,
                        Vec::<String>::new(),
                        "{case}"
                    );
                    assert_eq!(
                        claims_in(&mut notebook, "x:2", fields).await?.len(),
                        fields.len(),
                        "{case}"
                    );
                }
            }
        }
    }
    Ok(())
}

/// A write through a concept's `cardinality: :one` field succeeds the
/// newest claim, as Dialog-DB's does, and does not clear the cell (gaps
/// row 50, decided): the other claims stay, as Dialog-DB keeps two
/// replicas' writes, and a retract that names a value retracts that
/// claim. So with claims "a" and "b", an assert of "c" through the
/// concept leaves "a" and "c", and a retract of "c" leaves "a", which a
/// read through the concept answers.
#[dialog_common::test]
async fn it_succeeds_the_newest_claim_through_a_cardinality_one_field() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (p ?t n: ?n)
                 (xyz.p/n of: ?t is: ?n as: :text cardinality: :one))"#,
        )
        .await?;
    let direct = "(select (xyz.p ?t n: ?n) (xyz.p ?t n: ?n))";
    let concept = "(select (p ?t n: ?n))";
    for (write, directly, through) in [
        (
            r#"(assert (xyz.p x:1 n: "a"))"#,
            vec![r#"(xyz.p x:1 n: "a")"#],
            vec![r#"(xyz.p x:1 n: "a")"#],
        ),
        (
            r#"(assert (xyz.p x:1 n: "b"))"#,
            vec![r#"(xyz.p x:1 n: "a")"#, r#"(xyz.p x:1 n: "b")"#],
            vec![r#"(xyz.p x:1 n: "b")"#],
        ),
        (
            r#"(assert (p x:1 n: "c"))"#,
            vec![r#"(xyz.p x:1 n: "a")"#, r#"(xyz.p x:1 n: "c")"#],
            vec![r#"(xyz.p x:1 n: "c")"#],
        ),
        (
            r#"(retract (p x:1 n: "c"))"#,
            vec![r#"(xyz.p x:1 n: "a")"#],
            vec![r#"(xyz.p x:1 n: "a")"#],
        ),
    ] {
        notebook.submit(write).await?;
        assert_eq!(report(&mut notebook, direct).await?, directly, "{write}");
        assert_eq!(report(&mut notebook, concept).await?, through, "{write}");
    }
    Ok(())
}

/// The attributes under `dialog.` the notation refuses to write are the
/// ones Dialog-DB refuses to commit, on a real branch, in both
/// directions: each attribute is written once through the notation and
/// once as a fact committed past it, and the two refuse alike. The
/// notation's list is a copy of the engine's (`writes_into_dialog` in
/// `compile.rs`, from dialog-corpus `tree.rs`), and this is what fails
/// when the engine's list moves.
#[dialog_common::test]
async fn it_refuses_the_writes_into_dialog_that_dialog_db_refuses() -> Result<()> {
    use dialog_corpus::{Cardinality, Changes, Fact, Instruction};
    for (domain, field) in [
        ("dialog.rule", "x"),
        ("dialog.concept", "x"),
        ("dialog.layer", "x"),
        ("dialog.command", "x"),
        ("dialog.provider", "x"),
        ("dialog.branch", "revision"),
        ("dialog.branch", "tree"),
        ("dialog.meta", "x"),
        ("dialog.anything", "v"),
        ("dialog.branch", "other"),
        ("dialog.rulex", "y"),
        ("dialog.db", "revision"),
    ] {
        let attribute = format!("{domain}/{field}");
        let notebook = Notebook::open().await?;
        let fact = Fact {
            the: attribute.parse()?,
            of: "x:1".parse()?,
            is: Value::String("v".into()),
            cause: None,
        };
        let committed = notebook
            .branch
            .transaction()
            .integrate(
                [Instruction::Assert(fact, Cardinality::Many)]
                    .into_iter()
                    .collect::<Changes>(),
            )
            .commit()
            .publish()
            .perform(&notebook.operator)
            .await;
        let engine =
            matches!(&committed, Err(error) if format!("{error:?}").contains("ReservedRelation"));
        let checked = dialog_notation::check(
            &mut dialog_notation::Analyzer::new()?,
            &format!(r#"(assert ({domain} x:1 {field}: "v"))"#),
        );
        let refusals: Vec<_> = checked
            .refusals()
            .map(|said| said.message.clone())
            .collect();
        assert_eq!(
            !refusals.is_empty(),
            engine,
            "{attribute}: {refusals:?} against {committed:?}"
        );
        if engine {
            assert_eq!(refusals.len(), 1, "{attribute}: {refusals:?}");
            assert!(
                refusals[0].contains(&format!("`{attribute}`"))
                    && refusals[0].contains("Dialog-DB's own"),
                "{attribute}: {refusals:?}"
            );
        }
    }
    Ok(())
}

/// A write into the namespace Dialog-DB keeps is refused in a rule's
/// head and by a retract as by an assert, and a read of one is taken:
/// an embedder reads `dialog.rule/invalid`.
#[dialog_common::test]
fn it_refuses_a_rule_that_writes_into_dialog_and_reads_from_it() -> Result<()> {
    for text in [
        r#"(rule (assert (dialog.meta ?t description: ?n)) (xyz.t ?t n: ?n))"#,
        r#"(retract (dialog.anything x:1 v: 1))"#,
        r#"(rule (retract (dialog.meta ?t description: ?n)) (xyz.t ?t n: ?n))"#,
    ] {
        let checked = dialog_notation::check(&mut dialog_notation::Analyzer::new()?, text);
        assert_eq!(
            checked.refusals().count(),
            1,
            "{text}: {:?}",
            checked.diagnostics
        );
    }
    let read = dialog_notation::check(
        &mut dialog_notation::Analyzer::new()?,
        "(select (dialog.rule ?r invalid: ?why))",
    );
    assert!(read.refusals().next().is_none(), "{:?}", read.diagnostics);
    Ok(())
}

/// A pin, not a decision (gaps row 51): a document may write the facts
/// Dialog-DB lets an application write under `dialog.`, a rule's among
/// them, past what the notation checks when it installs a rule itself.
/// Written by hand, `dialog.rule/source` is text, not the bytes of a
/// rule's body (bytes have no written form, row 42), and its index
/// entries name an attribute: the facts are kept and read back, the
/// rule is not listed as invalid, and it runs nothing, so a write and a
/// read of the attribute it names answer as before.
#[dialog_common::test]
async fn it_pins_a_rule_written_by_hand() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit("(concept (p ?t n: ?n) (xyz.p/n of: ?t is: ?n as: :text cardinality: :one))")
        .await?;
    notebook.submit(r#"(assert (xyz.p x:1 n: "a"))"#).await?;
    notebook
        .submit(
            r#"(assert (dialog.rule rule:1 source: "not a rule"
                 derives: on:xyz.p/n reads: on:xyz.p/n on: on:xyz.p/n))"#,
        )
        .await?;
    notebook.submit(r#"(assert (xyz.p x:2 n: "b"))"#).await?;
    assert_eq!(
        report(&mut notebook, "(select (dialog.rule ?r derives: ?s))").await?,
        vec!["(dialog.rule rule:1 derives: on:xyz.p/n)"]
    );
    assert!(
        report(&mut notebook, "(select (dialog.rule ?r invalid: ?w))")
            .await?
            .is_empty()
    );
    let both = vec![r#"(xyz.p x:1 n: "a")"#, r#"(xyz.p x:2 n: "b")"#];
    assert_eq!(report(&mut notebook, "(select (p ?t n: ?n))").await?, both);
    assert_eq!(
        report(&mut notebook, "(select (xyz.p ?t n: ?n))").await?,
        both
    );
    Ok(())
}

/// A typed read of a derived attribute reads it as a typed read of a
/// stored attribute does (#866), written as a line is, `as:` the type of
/// the value under `is:`: with `list/label` derived from a text, a read
/// of it as an integer or an entity answers nothing, as text it
/// answers, and a join through a typed read of it answers as the same
/// join through the stored attribute's line. The kernel's spelling, the
/// value wrapped in its type, `(:text ?n)`, reads the same. A derived
/// attribute takes no `cardinality:`, which is what its rules conclude,
/// and `as:` with no `is:` to type is refused.
#[dialog_common::test]
async fn it_reads_a_derived_attribute_through_a_type_as_a_stored_one() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (list ?l name: ?n)
                 (xyz.list/name of: ?l is: ?n as: :text cardinality: :one))
               (rule (list/label ?l ?n) (list ?l name: ?n))
               (assert (list list:home name: "Home"))"#,
        )
        .await?;
    let home = vec![r#"(list/label list:home is: "Home")"#];
    for (read, answers) in [
        ("(select (list/label of: ?l is: ?n as: :integer))", vec![]),
        ("(select (list/label of: ?l is: ?n as: :entity))", vec![]),
        (
            "(select (list/label of: ?l is: ?n as: :text))",
            home.clone(),
        ),
        ("(select (list/label ?l is: ?n as: :text))", home.clone()),
        ("(select (list/label ?l (:text ?n)))", home.clone()),
        ("(select (list/label ?l (:integer ?n)))", vec![]),
    ] {
        assert_eq!(report(&mut notebook, read).await?, answers, "{read}");
    }
    for kind in [":integer", ":entity", ":text"] {
        let derived = report(
            &mut notebook,
            &format!("(select (xyz.out ?l v: ?n) (list/label of: ?l is: ?n as: {kind}))"),
        )
        .await?;
        let stored = report(
            &mut notebook,
            &format!("(select (xyz.out ?l v: ?n) (xyz.list/name of: ?l is: ?n as: {kind}))"),
        )
        .await?;
        assert_eq!(derived, stored, "{kind}");
    }
    for (read, says) in [
        (
            "(select (list/label of: ?l is: ?n as: :text cardinality: :one))",
            "cardinality:",
        ),
        ("(select (list/label of: ?l other: ?n as: :text))", "as:"),
    ] {
        let refused = notebook
            .submit(read)
            .await
            .map_err(|error| error.downcast::<Error>());
        assert!(
            matches!(&refused, Err(Ok(Error::Refused(why))) if why.contains(says)),
            "{read}: {refused:?}"
        );
    }
    Ok(())
}

/// A case a listed type does not list is refused wherever it is written
/// as a constant, naming the listing: an assert at the top level, a
/// write with a `when`, and a rule's head, one that writes and one that
/// issues a command; nothing is written. A listed case is written as
/// before, and a retract of an unlisted case is not refused, so a claim
/// written before can be taken back. Through a variable the case is not
/// known until the rule runs, and Dialog-DB writes what the variable was
/// bound to: a command carrying `:huge` into a rule that asserts it
/// stores `:huge` under the listed type. That is pinned here as it is.
#[dialog_common::test]
async fn it_refuses_a_case_the_listing_does_not_list() -> Result<()> {
    let concept = r#"(concept (k ?t lvl: ?l)
          (xyz.k/lvl of: ?t is: ?l as: [:low :mid :high] cardinality: :one))
        (concept (k/set subject: ?t to: ?l)
          (xyz.k.set/subject of: ?c is: ?t as: :entity in: :control cardinality: :many)
          (xyz.k.set/to of: ?c is: ?l as: [:low :mid :high] in: :control cardinality: :many))
        (concept (k/pass subject: ?t to: ?l)
          (xyz.k.pass/subject of: ?c is: ?t as: :entity in: :control cardinality: :many)
          (xyz.k.pass/to of: ?c is: ?l as: :entity in: :control cardinality: :many))
        (concept (k/bump subject: ?t)
          (xyz.k.bump/subject of: ?c is: ?t as: :entity in: :control cardinality: :many))
        (rule (assert (k ?t lvl: ?l)) (k/pass subject: ?t to: ?l))"#;
    let lvl = "(select (xyz.k x:1 lvl: ?v) (xyz.k x:1 lvl: ?v))";
    for text in [
        "(assert (k x:1 lvl: :huge))",
        "(assert (k ?t lvl: :huge) (when (k ?t lvl: :low)))",
        "(rule (assert (k ?t lvl: :huge)) (k/bump subject: ?t))",
        "(assert (k/set subject: x:1 to: :huge))",
        "(rule (assert (k/set subject: ?t to: :huge)) (k/bump subject: ?t))",
    ] {
        let mut notebook = Notebook::open().await?;
        notebook.submit(concept).await?;
        notebook.submit("(assert (k x:1 lvl: :low))").await?;
        let refused = notebook
            .submit(text)
            .await
            .map_err(|error| error.downcast::<Error>());
        assert!(
            matches!(&refused, Err(Ok(Error::Refused(why))) if why.contains("`:huge`") && why.contains("[:low :mid :high]")),
            "{text}: {refused:?}"
        );
        notebook.submit("(assert (k/bump subject: x:1))").await?;
        assert_eq!(
            report(&mut notebook, lvl).await?,
            vec!["(xyz.k x:1 lvl: :low)"],
            "{text}"
        );
    }
    let mut notebook = Notebook::open().await?;
    notebook.submit(concept).await?;
    notebook.submit("(assert (k x:1 lvl: :mid))").await?;
    assert_eq!(
        report(&mut notebook, lvl).await?,
        vec!["(xyz.k x:1 lvl: :mid)"]
    );
    // Written before through a variable, as Dialog-DB writes it.
    notebook
        .submit("(assert (k/pass subject: x:1 to: :huge))")
        .await?;
    assert_eq!(
        report(&mut notebook, lvl).await?,
        vec!["(xyz.k x:1 lvl: :huge)"]
    );
    // And taken back.
    notebook.submit("(retract (k x:1 lvl: :huge))").await?;
    assert_eq!(report(&mut notebook, lvl).await?, Vec::<String>::new());
    Ok(())
}

/// A formula in the head of a write with a `when` is computed from what
/// the `when` matched, as the same head in a rule is: each match writes
/// its value, also a formula inside another and in two fields at once.
#[dialog_common::test]
async fn it_computes_a_formula_in_the_head_of_a_write_with_a_when() -> Result<()> {
    let concept = r#"(concept (n ?t v: ?v w: ?w)
          (xyz.n/v of: ?t is: ?v as: :integer cardinality: :one)
          (xyz.n/w of: ?t is: ?w as: :integer cardinality: :one))"#;
    let read = "(select (n ?t v: ?v w: ?w))";
    for (write, after) in [
        (
            "(assert (n ?t v: (+ ?v : 1) w: ?w) (when (n ?t v: ?v w: ?w)))",
            ["(xyz.n x:1 v: 2 w: 10)", "(xyz.n x:2 v: 6 w: 20)"],
        ),
        (
            "(assert (n ?t v: (* (+ ?v : 1) : 3) w: (- ?w : ?v)) (when (n ?t v: ?v w: ?w)))",
            ["(xyz.n x:1 v: 6 w: 9)", "(xyz.n x:2 v: 18 w: 15)"],
        ),
    ] {
        let mut notebook = Notebook::open().await?;
        notebook.submit(concept).await?;
        notebook
            .submit("(assert (n x:1 v: 1 w: 10)) (assert (n x:2 v: 5 w: 20))")
            .await?;
        notebook.submit(write).await?;
        assert_eq!(report(&mut notebook, read).await?, after, "{write}");
    }
    Ok(())
}

/// A read of a concept takes no pick, `(:last …)`, `(:all …)`, `(:max …)`
/// or `(:min …)`, as it takes no type or cardinality: the concept says
/// its field's cardinality, which replaced picks, and elects a claim by
/// the elector in its head. Each is refused naming the field and the
/// pick, saying to write it bare. A read of the domain itself still
/// takes one.
#[dialog_common::test]
async fn it_refuses_a_pick_on_a_concept_read() -> Result<()> {
    let concept = r#"(concept (n ?t v: ?v w: ?w)
          (xyz.n/v of: ?t is: ?v as: :integer cardinality: :one)
          (xyz.n/w of: ?t is: ?w as: :integer cardinality: :many))"#;
    for (read, pick) in [
        ("(select (n ?t v: (:last ?v) w: ?w))", "last"),
        ("(select (n ?t v: ?v w: (:all ?w)))", "all"),
        ("(select (n ?t v: ?v w: (:max ?w)))", "max"),
        ("(select (n ?t v: ?v w: (:min ?w)))", "min"),
    ] {
        let checked = dialog_notation::check(
            &mut dialog_notation::Analyzer::new()?,
            &format!("{concept} {read}"),
        );
        let refused: Vec<_> = checked
            .refusals()
            .map(|said| said.message.clone())
            .collect();
        assert!(
            refused.len() == 1
                && refused[0].contains(&format!("(:{pick} …)"))
                && refused[0].contains("bare"),
            "{read}: {refused:?}"
        );
    }
    let checked = dialog_notation::check(
        &mut dialog_notation::Analyzer::new()?,
        &format!("{concept} (select (xyz.n ?t w: (:max ?w)) (xyz.n ?t w: (:max ?w)))"),
    );
    assert!(
        checked.refusals().next().is_none(),
        "{:?}",
        checked.diagnostics
    );
    Ok(())
}
