//! Queries, the second layer, asked on a real branch.
//!
//! A query reads rows from concepts and rules, nests, and collects: the
//! whole of a field is `(collect ?x)` on the term of a read, one row with
//! a map, `{}` for none. Asking is `(select …)`. Each test is a claim of
//! design/macros/design-queries.md, with the answers its tables give.

#![allow(deprecated)]

use anyhow::Result;
use dialog_notation::Error;

#[path = "../examples/notebook/session.rs"]
mod session;
use session::Notebook;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// The report of a document.
async fn report(notebook: &mut Notebook, text: &str) -> Result<Vec<String>> {
    Ok(notebook.submit(text).await?.0)
}

/// Why a document was refused: the notation's own error.
async fn refused(notebook: &mut Notebook, text: &str) -> Error {
    match notebook.submit(text).await {
        Ok(done) => panic!("{text} was not refused: {done:?}"),
        Err(error) => match error.downcast::<Error>() {
            Ok(error) => error,
            Err(other) => panic!("{text} failed otherwise: {other}"),
        },
    }
}

/// "Buy milk" is tagged `errand` and `home`. "Call mum" has no tag.
const TODOS: &str = r#"
    (concept (todo ?t title: ?title)
      (xyz.todo/title of: ?t is: ?title as: :text cardinality: :one))
    (concept (todo/tag ?t tag: ?g)
      (xyz.todo/tag of: ?t is: ?g as: :text cardinality: :many))

    (assert (todo todo:1 title: "Buy milk"))
    (assert (todo/tag todo:1 tag: "errand"))
    (assert (todo/tag todo:1 tag: "home"))
    (assert (todo todo:2 title: "Call mum"))
"#;

/// The cards of the to-dos, each with the whole of its tags.
const CARD: &str = r#"
    (query (todo/card ?t is: ?card)
      (todo ?t title: ?title)
      (todo/tag ?t tag: (collect ?tags))
      (is ?card {:title ?title :tags ?tags}))
"#;

async fn todos() -> Result<Notebook> {
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook.submit(TODOS).await?;
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    Ok(notebook)
}

/// All required: a row for each way every condition holds, so two for
/// "Buy milk" and none for "Call mum".
#[dialog_common::test]
async fn it_answers_a_row_for_each_way_its_conditions_hold() -> Result<()> {
    let mut notebook = todos().await?;
    notebook
        .submit(
            "(query (todo/tagged ?t is: ?pair)
               (todo ?t title: ?title)
               (todo/tag ?t tag: ?g)
               (is ?pair {:title ?title :tag ?g}))",
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (todo/tagged ?t is: ?pair))").await?,
        vec![
            r#"(todo/tagged todo:1 is: {:title "Buy milk" :tag "errand"})"#,
            r#"(todo/tagged todo:1 is: {:title "Buy milk" :tag "home"})"#,
        ]
    );
    Ok(())
}

/// The whole of a field: one row a to-do, its tags a set, and the empty
/// set, `{}`, for a to-do with none. Collecting never keeps the row.
#[dialog_common::test]
async fn it_collects_the_whole_of_a_field_as_a_set() -> Result<()> {
    let mut notebook = todos().await?;
    notebook.submit(CARD).await?;
    assert_eq!(
        report(&mut notebook, "(select (todo/card ?t is: ?card))").await?,
        vec![
            r#"(todo/card todo:1 is: {:title "Buy milk" :tags {: "errand" : "home"}})"#,
            r#"(todo/card todo:2 is: {:title "Call mum" :tags {}})"#,
        ]
    );
    // A field passed bound is a parameter.
    assert_eq!(
        report(&mut notebook, "(select (todo/card todo:2 is: ?card))").await?,
        vec![r#"(todo/card todo:2 is: {:title "Call mum" :tags {}})"#]
    );
    Ok(())
}

/// The folds a selection asks Dialog-DB for.
fn folds(text: &str) -> Result<usize> {
    let analysis = dialog_notation::Analyzer::new()?.analyze(&dialog_notation::parse(text)?);
    for tree in &analysis.analyzed {
        if let [dialog_notation::Step::Select(selection)] =
            dialog_notation::compile(tree)?.as_slice()
        {
            return Ok(selection.folds.len());
        }
    }
    Err(anyhow::anyhow!("no selection: {:?}", analysis.diagnostics))
}

/// A read that collects one variable, whose shared terms the conditions
/// every answer needs bind, is folded by Dialog-DB, with `distinct` and
/// `count-distinct`: the card's tags are one fold, asked for every card
/// or for one. A read that collects entries is put together here.
#[dialog_common::test]
fn it_asks_dialog_db_to_fold_a_collected_read() -> Result<()> {
    assert_eq!(
        folds(&format!("{TODOS}{CARD}(select (todo/card ?t is: ?card))"))?,
        1
    );
    assert_eq!(
        folds(&format!(
            "{TODOS}{CARD}(select (todo/card todo:2 is: ?card))"
        ))?,
        1
    );
    let outline = "(query (notebook/outline ?n is: ?outline)
           (notebook/named ?n title: ?title)
           (notebook/blocks ?n block: (collect ?blocks))
           (is ?outline {:title ?title :blocks ?blocks}))
         (select (notebook/outline ?n is: ?outline))";
    assert_eq!(folds(&format!("{NOTEBOOK}{outline}"))?, 0);
    Ok(())
}

/// A fold is a function of what was collected: `count` of the tags is 2
/// and 0. A function other than `count` is refused.
#[dialog_common::test]
async fn it_counts_what_was_collected() -> Result<()> {
    let mut notebook = todos().await?;
    notebook
        .submit(
            "(query (todo/counted ?t is: ?card)
               (todo ?t title: ?title)
               (todo/tag ?t tag: (collect ?tags))
               (is ?card {:title ?title :tags (count ?tags)}))",
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (todo/counted ?t is: ?card))").await?,
        vec![
            r#"(todo/counted todo:1 is: {:title "Buy milk" :tags 2})"#,
            r#"(todo/counted todo:2 is: {:title "Call mum" :tags 0})"#,
        ]
    );
    let summed = refused(
        &mut notebook,
        "(query (todo/summed ?t is: ?n)
           (todo ?t title: ?title)
           (todo/tag ?t tag: (collect ?tags))
           (is ?n (sum ?tags)))",
    )
    .await;
    assert!(
        matches!(&summed, Error::Unsupported(why) if why.contains("`(count ?x)`")),
        "{summed:?}"
    );
    Ok(())
}

/// A notebook, its title and its blocks, each block's source. "Untitled"
/// has no block, and block `c` has no source.
const NOTEBOOK: &str = r##"
    (concept (notebook/named ?n title: ?title)
      (xyz.tonk.notebook/title of: ?n is: ?title as: :text cardinality: :one))
    (concept (notebook/blocks ?n block: {?k ?b})
      (xyz.tonk.notebook/?k of: ?n is: ?b as: :entity cardinality: :many by: :position))
    (concept (notebook/block ?b source: ?s)
      (xyz.tonk.notebook.block/source of: ?b is: ?s as: :text cardinality: :one))

    (assert (notebook/named notebook:1 title: "Field notes"))
    (assert (notebook/blocks notebook:1 block: {"N1" block:a}))
    (assert (notebook/blocks notebook:1 block: {"N2" block:b}))
    (assert (notebook/blocks notebook:1 block: {"N3" block:c}))
    (assert (notebook/block block:a source: "# Monday"))
    (assert (notebook/block block:b source: "Rained."))
    (assert (notebook/named notebook:2 title: "Untitled"))
"##;

async fn notebooks() -> Result<Notebook> {
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook.submit(NOTEBOOK).await?;
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    Ok(notebook)
}

/// The whole of a keyed collection is a map of key to value, and `{}`
/// for a notebook with no block.
#[dialog_common::test]
async fn it_collects_a_keyed_collection_as_a_map() -> Result<()> {
    let mut notebook = notebooks().await?;
    notebook
        .submit(
            "(query (notebook/outline ?n is: ?outline)
               (notebook/named ?n title: ?title)
               (notebook/blocks ?n block: (collect ?blocks))
               (is ?outline {:title ?title :blocks ?blocks}))",
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (notebook/outline ?n is: ?outline))").await?,
        vec![
            r#"(notebook/outline notebook:1 is: {:title "Field notes" :blocks {"N1" block:a "N2" block:b "N3" block:c}})"#,
            r#"(notebook/outline notebook:2 is: {:title "Untitled" :blocks {}})"#,
        ]
    );
    Ok(())
}

/// A query whose answer is an entry, collected by another, gives a map
/// again: each block's source by its position. A block with no source is
/// no answer of the item, so it is absent from the map.
#[dialog_common::test]
async fn it_collects_a_query_whose_answer_is_an_entry_as_a_map() -> Result<()> {
    let mut notebook = notebooks().await?;
    notebook
        .submit(
            "(query (notebook/source ?n is: {?k ?s})
               (notebook/blocks ?n block: {?k ?b})
               (notebook/block ?b source: ?s))
             (query (notebook/outline ?n is: ?outline)
               (notebook/named ?n title: ?title)
               (notebook/source ?n is: (collect ?sources))
               (is ?outline {:title ?title :blocks ?sources}))",
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (notebook/source ?n is: ?entry))").await?,
        vec![
            r##"(notebook/source notebook:1 is: {"N1" "# Monday"})"##,
            r#"(notebook/source notebook:1 is: {"N2" "Rained."})"#,
        ]
    );
    assert_eq!(
        report(&mut notebook, "(select (notebook/outline ?n is: ?outline))").await?,
        vec![
            r##"(notebook/outline notebook:1 is: {:title "Field notes" :blocks {"N1" "# Monday" "N2" "Rained."}})"##,
            r#"(notebook/outline notebook:2 is: {:title "Untitled" :blocks {}})"#,
        ]
    );
    Ok(())
}

/// `collect` in the entity position: the to-dos of a list that are open,
/// a set of entities, and `{}` for a list with none.
#[dialog_common::test]
async fn it_collects_in_the_entity_position() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (list ?l name: ?name)
                 (xyz.list/name of: ?l is: ?name as: :text cardinality: :one))
               (concept (task ?t list: ?l state: ?s)
                 (xyz.task/list of: ?t is: ?l as: :entity cardinality: :one)
                 (xyz.task/state of: ?t is: ?s as: [:open :done] cardinality: :one))
               (query (list/open ?l is: ?open)
                 (list ?l name: ?name)
                 (task (collect ?open) list: ?l state: :open))

               (assert (list list:home name: "Home"))
               (assert (list list:work name: "Work"))
               (assert (task task:1 list: list:home state: :open))
               (assert (task task:2 list: list:home state: :done))
               (assert (task task:3 list: list:home state: :open))
               (assert (task task:4 list: list:work state: :done))"#,
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (list/open ?l is: ?open))").await?,
        vec![
            "(list/open list:home is: {: task:1 : task:3})",
            "(list/open list:work is: {})",
        ]
    );
    Ok(())
}

/// A query of no entity that only collects is an index: it answers once,
/// with what there is, and once with `{}` when there is nothing. It may
/// collect the answers of another query too.
#[dialog_common::test]
async fn it_answers_an_index_once_whatever_there_is() -> Result<()> {
    let index = r#"
        (query (todo/titles is: ?titles)
          (todo _ title: (collect ?titles)))
        (query (todo/deck is: ?deck)
          (todo/card _ is: (collect ?deck)))
    "#;
    let mut notebook = todos().await?;
    notebook.submit(&format!("{CARD} {index}")).await?;
    assert_eq!(
        report(&mut notebook, "(select (todo/titles is: ?titles))").await?,
        vec![r#"(todo/titles is: {: "Buy milk" : "Call mum"})"#]
    );
    assert_eq!(
        report(&mut notebook, "(select (todo/deck is: ?deck))").await?,
        vec![
            r#"(todo/deck is: {: {:title "Buy milk" :tags {: "errand" : "home"}} : {:title "Call mum" :tags {}}})"#
        ]
    );

    let mut empty = Notebook::open().await?;
    let declared: String = TODOS
        .lines()
        .filter(|line| !line.trim_start().starts_with("(assert"))
        .collect::<Vec<_>>()
        .join("\n");
    empty.submit(&format!("{declared} {CARD} {index}")).await?;
    assert_eq!(
        report(&mut empty, "(select (todo/titles is: ?titles))").await?,
        vec!["(todo/titles is: {})"]
    );
    assert_eq!(
        report(&mut empty, "(select (todo/deck is: ?deck))").await?,
        vec!["(todo/deck is: {})"]
    );
    Ok(())
}

/// A query of an entity whose body only collects is refused: nothing
/// says which entities it answers for. Nothing of it is kept.
#[dialog_common::test]
async fn it_refuses_a_query_of_an_entity_that_only_collects() -> Result<()> {
    let mut notebook = todos().await?;
    let error = refused(
        &mut notebook,
        "(query (todo/bag ?t is: ?tags)
           (todo/tag ?t tag: (collect ?tags)))",
    )
    .await;
    assert!(
        matches!(&error, Error::Refused(why) if why.contains("only collects")),
        "{error:?}"
    );
    // Nothing of it is kept: asked, `todo/bag` is read as an attribute
    // derived under the concept `todo`, which no rule derives.
    assert!(
        report(&mut notebook, "(select (todo/bag ?t is: ?tags))")
            .await?
            .is_empty()
    );
    Ok(())
}

/// In a read that collects every other term is bound by a condition that
/// does not collect, a constant, or `_`.
#[dialog_common::test]
async fn it_refuses_an_unbound_term_in_a_collecting_read() -> Result<()> {
    let mut notebook = todos().await?;
    let error = refused(
        &mut notebook,
        "(query (todo/loose ?t is: ?tags)
           (todo ?t title: ?title)
           (todo/tag ?u tag: (collect ?tags)))",
    )
    .await;
    assert!(
        matches!(&error, Error::Refused(why) if why.contains("collects") && why.contains("?u")),
        "{error:?}"
    );
    Ok(())
}

/// `collect` stands in a query and nowhere else: not in a rule, not in a
/// concept, and the whole of a field is never written.
#[dialog_common::test]
async fn it_refuses_collect_outside_a_query() -> Result<()> {
    let mut notebook = notebooks().await?;
    let in_rule = refused(
        &mut notebook,
        "(rule (xyz.tagged ?t tags: ?tags)
           (todo/tag ?t tag: (collect ?tags)))",
    )
    .await;
    assert!(
        matches!(&in_rule, Error::Refused(why)
            if why.contains("stands in a rule") && why.contains("could feed itself")),
        "{in_rule:?}"
    );
    let in_concept = refused(
        &mut notebook,
        "(concept (todo/tags ?t tags: ?tags)
           (xyz.todo/tag of: ?t is: (collect ?tags) as: :text cardinality: :many))",
    )
    .await;
    assert!(
        matches!(&in_concept, Error::Refused(why)
            if why.contains("stands in a concept") && why.contains("no fact")),
        "{in_concept:?}"
    );
    let whole = refused(
        &mut notebook,
        "(assert (notebook/blocks notebook:1 block: (collect ?blocks)))",
    )
    .await;
    assert!(
        matches!(&whole, Error::Refused(why)
            if why.contains("writes the whole of a field") && why.contains("one entry")),
        "{whole:?}"
    );
    let in_head = refused(
        &mut notebook,
        "(rule (assert (notebook/blocks ?n block: (collect ?blocks)))
           (notebook/named ?n title: \"x\"))",
    )
    .await;
    assert!(
        matches!(&in_head, Error::Refused(why) if why.contains("writes the whole of a field")),
        "{in_head:?}"
    );
    // Nothing of a refused document is written.
    assert_eq!(
        report(
            &mut notebook,
            "(select (notebook/blocks notebook:1 block: {?k ?b}))"
        )
        .await?
        .len(),
        3
    );
    Ok(())
}

/// A rule and a concept give rows, and never read a query.
#[dialog_common::test]
async fn it_refuses_a_rule_or_a_concept_that_applies_a_query() -> Result<()> {
    let mut notebook = todos().await?;
    notebook.submit(CARD).await?;
    let in_rule = refused(
        &mut notebook,
        "(rule (xyz.seen ?t card: ?card)
           (todo/card ?t is: ?card))",
    )
    .await;
    assert!(
        matches!(&in_rule, Error::Refused(why)
            if why.contains("applies the query todo/card") && why.contains("never by a rule")),
        "{in_rule:?}"
    );
    let in_concept = refused(
        &mut notebook,
        "(concept (todo/carded ?t card: ?card)
           (todo/card ?t is: ?card))",
    )
    .await;
    assert!(
        matches!(&in_concept, Error::Refused(why) if why.contains("applies the query todo/card")),
        "{in_concept:?}"
    );
    Ok(())
}

/// A query that calls itself, directly, is refused.
#[dialog_common::test]
async fn it_refuses_a_query_that_calls_itself() -> Result<()> {
    let mut notebook = todos().await?;
    let error = refused(
        &mut notebook,
        "(query (todo/loop ?t is: ?x)
           (todo ?t title: ?title)
           (todo/loop ?t is: (collect ?x)))",
    )
    .await;
    assert!(
        matches!(&error, Error::Refused(why) if why.contains("calls itself")),
        "{error:?}"
    );
    Ok(())
}

/// Asking is `(select …)`. `(query …)` written the old way, as a
/// question, is refused with what to write.
#[dialog_common::test]
async fn it_refuses_a_query_written_as_a_question() -> Result<()> {
    let mut notebook = todos().await?;
    for asked in [
        "(query (todo ?t title: ?title))",
        "(query (todo ?t title: ?title) (todo/tag ?t tag: ?g))",
        "(query (xyz.todo/title of: ?t is: ?title))",
    ] {
        let error = refused(&mut notebook, asked).await;
        assert!(
            matches!(&error, Error::Refused(why) if why.contains("asking is written `(select …)`")),
            "{asked}: {error:?}"
        );
    }
    assert_eq!(
        report(&mut notebook, "(select (todo ?t title: ?title))").await?,
        vec![
            r#"(xyz.todo todo:1 title: "Buy milk")"#,
            r#"(xyz.todo todo:2 title: "Call mum")"#,
        ]
    );
    Ok(())
}

/// A collection's field is one entry, its key and its value. Read one at
/// a time it is a row an entry, and no row for none; a write goes
/// through one entry, and so does a retract.
#[dialog_common::test]
async fn it_reads_and_writes_a_collection_an_entry_at_a_time() -> Result<()> {
    let mut notebook = notebooks().await?;
    let blocks = "(select (notebook/blocks notebook:1 block: {?k ?b}))";
    assert_eq!(
        report(&mut notebook, blocks).await?,
        vec![
            "(xyz.tonk.notebook notebook:1 N1: block:a)",
            "(xyz.tonk.notebook notebook:1 N2: block:b)",
            "(xyz.tonk.notebook notebook:1 N3: block:c)",
        ]
    );
    assert!(
        report(
            &mut notebook,
            "(select (notebook/blocks notebook:2 block: {?k ?b}))"
        )
        .await?
        .is_empty()
    );
    // The key as a constant reads one entry.
    assert_eq!(
        report(
            &mut notebook,
            r#"(select (notebook/blocks notebook:1 block: {"N2" ?b}))"#
        )
        .await?,
        vec!["(xyz.tonk.notebook notebook:1 N2: block:b)"]
    );
    notebook
        .submit(r#"(assert (notebook/blocks notebook:2 block: {"N1" block:d}))"#)
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(select (notebook/blocks notebook:2 block: {?k ?b}))"
        )
        .await?,
        vec!["(xyz.tonk.notebook notebook:2 N1: block:d)"]
    );
    notebook
        .submit(r#"(retract (notebook/blocks notebook:1 block: {"N2" block:b}))"#)
        .await?;
    assert_eq!(
        report(&mut notebook, blocks).await?,
        vec![
            "(xyz.tonk.notebook notebook:1 N1: block:a)",
            "(xyz.tonk.notebook notebook:1 N3: block:c)",
        ]
    );
    // The key and the value as two fields of the head is refused.
    let apart = refused(
        &mut notebook,
        "(concept (notebook/apart ?n key: ?k block: ?b)
           (xyz.tonk.notebook/?k of: ?n is: ?b as: :entity cardinality: :many by: :position))",
    )
    .await;
    assert!(
        matches!(&apart, Error::Refused(why) if why.contains("one entry")),
        "{apart:?}"
    );
    Ok(())
}

/// A set, `{: 1 : 2}`, is stored in a record as a list: its members in
/// the order of their values, each once. What is stored reads back as
/// that list.
#[dialog_common::test]
async fn it_stores_a_set_in_a_record_as_a_list() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(r#"(assert (xyz.thing thing:1 about: {:tags {: "home" : "errand" : "home"}}))"#)
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (xyz.thing ?x about: ?about))").await?,
        vec![r#"(xyz.thing thing:1 about: {:tags ["errand" "home"]})"#]
    );
    Ok(())
}
