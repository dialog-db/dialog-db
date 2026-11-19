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
/// or for one. A read that collects entries is folded too, each entry
/// the record of the row it was read from.
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
    assert_eq!(folds(&format!("{NOTEBOOK}{outline}"))?, 1);
    Ok(())
}

/// The selection a document compiles to.
fn selection(text: &str) -> Result<dialog_notation::Selection> {
    let analysis = dialog_notation::Analyzer::new()?.analyze(&dialog_notation::parse(text)?);
    for tree in &analysis.analyzed {
        if let [dialog_notation::Step::Select(selection)] =
            dialog_notation::compile(tree)?.as_slice()
        {
            return Ok((**selection).clone());
        }
    }
    Err(anyhow::anyhow!("no selection: {:?}", analysis.diagnostics))
}

/// The card's tags, the fold of two concept queries whose rules are laid
/// on the branch's session overlay, kept by a standing fold: after a tag
/// added, a tag taken away and a to-do added, what it has reported is
/// what a select of the fold gives, and neither query was asked again
/// whole.
#[dialog_common::test]
async fn it_keeps_a_collected_read_folded_under_a_subscription() -> Result<()> {
    use dialog_query::query::Output as _;
    let mut notebook = todos().await?;
    let selection = selection(&format!("{TODOS}{CARD}(select (todo/card ?t is: ?card))"))?;
    let folded = &selection.folds[0];
    for rule in &folded.rules {
        notebook.branch.overlay().assert(rule.clone())?;
    }
    let mut standing = notebook.branch.subscribe_fold(folded.fold.clone());
    let mut answer = Vec::new();
    for write in [
        None,
        Some(r#"(assert (todo/tag todo:2 tag: "call"))"#),
        Some(r#"(retract (todo/tag todo:1 tag: "home"))"#),
        Some(r#"(assert (todo todo:3 title: "Water plants"))"#),
    ] {
        if let Some(write) = write {
            let (_, diagnostics) = notebook.submit(write).await?;
            assert!(diagnostics.is_empty(), "{diagnostics:?}");
        }
        let delta = standing
            .poll(&notebook.operator)
            .await?
            .expect("each write moves a card");
        for row in &delta.retracted {
            let at = answer.iter().position(|held| held == row).expect("held");
            answer.swap_remove(at);
        }
        answer.extend(delta.asserted);
        let selected = notebook
            .branch
            .query()
            .select(folded.fold.clone())
            .perform(&notebook.operator)
            .try_vec()
            .await?;
        let rows = |rows: &[dialog_query::FoldConclusion]| {
            let mut rows: Vec<String> = rows
                .iter()
                .map(|row| format!("{:?}", folded.values(row)))
                .collect();
            rows.sort();
            rows
        };
        assert_eq!(rows(&answer), rows(&selected), "after {write:?}");
    }
    assert_eq!(answer.len(), 3);
    assert_eq!(standing.recomputes(), 2);
    Ok(())
}

/// What a tag added to one of `n` to-dos costs the card's standing fold
/// once it has been polled: how many times its inner subscriptions were
/// evaluated whole again, and how many tree nodes the poll read.
async fn tag_added(n: usize) -> Result<(usize, u64)> {
    let mut notebook = Notebook::open().await?;
    let mut seed = String::from(TODOS);
    for i in 0..n {
        seed.push_str(&format!(
            "\n(assert (todo todo:n{i} title: \"Task {i}\"))\
             \n(assert (todo/tag todo:n{i} tag: \"a{}\"))\
             \n(assert (todo/tag todo:n{i} tag: \"b{}\"))",
            i % 7,
            i % 5
        ));
    }
    let (_, diagnostics) = notebook.submit(&seed).await?;
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let selection = selection(&format!("{TODOS}{CARD}(select (todo/card ?t is: ?card))"))?;
    let folded = &selection.folds[0];
    for rule in &folded.rules {
        notebook.branch.overlay().assert(rule.clone())?;
    }
    let env = dialog_repository::helpers::Interposing::new(notebook.operator.clone());
    let mut standing = notebook.branch.subscribe_fold(folded.fold.clone());
    standing.poll(&env).await?;
    let recomputed = standing.recomputes();
    notebook
        .submit(&format!(
            r#"(assert (todo/tag todo:n{} tag: "new"))"#,
            n / 2
        ))
        .await?;
    let reads = env.reads();
    let delta = standing.poll(&env).await?.expect("the tag moves a card");
    assert_eq!((delta.asserted.len(), delta.retracted.len()), (1, 1));
    Ok((standing.recomputes() - recomputed, env.reads() - reads))
}

/// A tag added to one to-do is maintained for that to-do alone: the
/// fold's inner subscriptions are not evaluated whole again, and the
/// poll reads a few nodes, as many among 200 to-dos as among 20 (about
/// 20 each; reported under an entity made of the row's values, as they
/// were, the poll read 88 and 94, and its time grew with the to-dos). Each
/// inner query reports its rows under the entity they are about, the
/// to-do, so the subscription re-derives the one to-do whose fact moved.
#[dialog_common::test]
async fn it_maintains_a_folded_read_for_the_entity_that_changed() -> Result<()> {
    let (few, few_reads) = tag_added(20).await?;
    let (many, many_reads) = tag_added(200).await?;
    assert_eq!((few, many), (0, 0), "an inner subscription recomputed");
    assert!(
        few_reads <= 40 && many_reads <= 40,
        "node reads grew: {few_reads} at 20, {many_reads} at 200"
    );
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

/// The outline: a notebook's title and the whole of its blocks, a map.
const OUTLINE: &str = "(query (notebook/outline ?n is: ?outline)
       (notebook/named ?n title: ?title)
       (notebook/blocks ?n block: (collect ?blocks))
       (is ?outline {:title ?title :blocks ?blocks :count (count ?blocks)}))";

/// The answers of a selection, its rows asked once and its folds kept
/// standing: each fold polled after its rules are laid, its rows those a
/// standing fold reports, so a map Dialog-DB folds is the same selected
/// and maintained.
async fn standing(
    notebook: &Notebook,
    selection: &dialog_notation::Selection,
) -> Result<Vec<String>> {
    use dialog_query::query::Output as _;
    let mut results = Vec::new();
    for query in &selection.queries {
        let mut layer = notebook.branch.query();
        if let Some(rule) = &query.rule {
            layer = layer.with(rule.clone());
        }
        let rows: Vec<dialog_query::ConceptConclusion> = layer
            .select(query.select.clone())
            .perform(&notebook.operator)
            .try_vec()
            .await?;
        results.push(
            rows.iter()
                .map(|row| query.values(row))
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    let mut folded = Vec::new();
    for fold in &selection.folds {
        for rule in &fold.rules {
            notebook.branch.overlay().assert(rule.clone())?;
        }
        let mut kept = notebook.branch.subscribe_fold(fold.fold.clone());
        let rows = match kept.poll(&notebook.operator).await? {
            Some(delta) => delta.asserted,
            None => Vec::new(),
        };
        folded.push(rows.iter().map(|row| fold.values(row)).collect());
    }
    Ok(selection
        .answer(&results, &folded)?
        .iter()
        .map(ToString::to_string)
        .collect())
}

/// A collected entry `{?k ?v}` is folded by Dialog-DB as the record of
/// the row it was read from, so its key stays beside its value: two
/// blocks of one source under two keys are two entries, and two blocks
/// under one key are one, the first in the order of their values, as
/// what is put together in memory keeps. It is counted by its keys.
#[dialog_common::test]
async fn it_folds_a_collected_entry_keeping_its_key_beside_its_value() -> Result<()> {
    let mut notebook = notebooks().await?;
    let (_, diagnostics) = notebook
        .submit(
            r#"(assert (notebook/named notebook:3 title: "Shared"))
               (assert (notebook/blocks notebook:3 block: {"N1" block:a}))
               (assert (notebook/blocks notebook:3 block: {"N2" block:a}))
               (assert (notebook/named notebook:4 title: "Keyed"))
               (assert (notebook/blocks notebook:4 block: {"N1" block:b}))
               (assert (notebook/blocks notebook:4 block: {"N1" block:a}))"#,
        )
        .await?;
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let asked = format!("{NOTEBOOK}{OUTLINE}(select (notebook/outline ?n is: ?outline))");
    let selection = selection(&asked)?;
    assert_eq!(selection.folds.len(), 1);
    let expected = vec![
        r#"(notebook/outline notebook:1 is: {:title "Field notes" :blocks {"N1" block:a "N2" block:b "N3" block:c} :count 3})"#,
        r#"(notebook/outline notebook:2 is: {:title "Untitled" :blocks {} :count 0})"#,
        r#"(notebook/outline notebook:3 is: {:title "Shared" :blocks {"N1" block:a "N2" block:a} :count 2})"#,
        r#"(notebook/outline notebook:4 is: {:title "Keyed" :blocks {"N1" block:a} :count 1})"#,
    ];
    assert_eq!(standing(&notebook, &selection).await?, expected);
    notebook.submit(OUTLINE).await?;
    assert_eq!(
        report(&mut notebook, "(select (notebook/outline ?n is: ?outline))").await?,
        expected
    );
    Ok(())
}

/// Items whose tags are read through two definitions of `item/tag`, one
/// of the tag attribute and one of the label attribute.
const LABELLED: &str = r#"
    (concept (item ?t title: ?title)
      (xyz.item/title of: ?t is: ?title as: :text cardinality: :one))
    (concept (item/tag ?t tag: ?g)
      (xyz.item/tag of: ?t is: ?g as: :text cardinality: :many))
    (concept (item/tag ?t tag: ?g source: ?s)
      (xyz.item/label of: ?t is: ?g as: :text cardinality: :many)
      (xyz.item/source of: ?t is: ?s as: :text cardinality: :one))
    (query (item/card ?t is: ?card)
      (item ?t title: ?title)
      (item/tag ?t tag: (collect ?tags) ...)
      (is ?card {:title ?title :tags ?tags}))"#;

/// A collected read through two definitions is one query: each
/// definition derives the one relation of the part, so Dialog-DB folds
/// the tags of both.
#[dialog_common::test]
async fn it_folds_a_collected_read_through_several_definitions() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook
        .submit(&format!(
            r#"{LABELLED}
               (assert (item item:1 title: "One"))
               (assert (item/tag item:1 tag: "a"))
               (assert (item/tag item:1 tag: "b" source: "s"))
               (assert (item item:2 title: "Two"))"#
        ))
        .await?;
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let selection = selection(&format!("{LABELLED}(select (item/card ?t is: ?card))"))?;
    assert_eq!(
        selection.queries.len(),
        3,
        "the title, and the tags through each definition"
    );
    assert_eq!(selection.folds.len(), 1);
    let expected = vec![
        r#"(item/card item:1 is: {:title "One" :tags {: "a" : "b"}})"#,
        r#"(item/card item:2 is: {:title "Two" :tags {}})"#,
    ];
    assert_eq!(standing(&notebook, &selection).await?, expected);
    assert_eq!(
        report(&mut notebook, "(select (item/card ?t is: ?card))").await?,
        expected
    );
    Ok(())
}

/// A collected call is put together in memory: what a query answers may
/// itself be collected, and a query is not a relation Dialog-DB can
/// fold. The fallback answers it.
#[dialog_common::test]
fn it_puts_a_collected_call_together_in_memory() -> Result<()> {
    let chips = "(query (todo/chip ?t tag: ?g is: ?chip)
           (todo/tag ?t tag: ?g)
           (is ?chip {:tag ?g}))
         (query (todo/chips ?t is: ?card)
           (todo ?t title: ?title)
           (todo/chip ?t is: (collect ?chips))
           (is ?card {:title ?title :chips ?chips}))
         (select (todo/chips ?t is: ?card))";
    assert_eq!(folds(&format!("{TODOS}{chips}"))?, 0);
    Ok(())
}

/// A collected read that shares a term with the rest beside its entity,
/// and has more than one column beside the entity, is put together in
/// memory: its rows are reported as a record under the entity, and a
/// field of one row could be read beside a field of another, so a fold
/// could not group by the term the read shares. The fallback answers.
#[dialog_common::test]
async fn it_puts_a_read_grouped_beside_its_entity_together_in_memory() -> Result<()> {
    let noted = r#"
        (concept (todo/note ?t day: ?d text: ?x)
          (xyz.todo.note/day of: ?t is: ?d as: :text cardinality: :many)
          (xyz.todo.note/text of: ?t is: ?x as: :text cardinality: :many))
        (query (todo/day ?t is: ?card)
          (todo ?t title: ?title)
          (todo/note ?t day: ?d)
          (todo/note ?t day: ?d text: (collect ?texts))
          (is ?card {:title ?title :day ?d :texts ?texts}))"#;
    assert_eq!(
        folds(&format!("{TODOS}{noted}(select (todo/day ?t is: ?card))"))?,
        0
    );
    let mut notebook = todos().await?;
    notebook.submit(noted).await?;
    notebook
        .submit(r#"(assert (todo/note todo:1 day: "mon" text: "early"))"#)
        .await?;
    assert_eq!(
        report(&mut notebook, "(select (todo/day ?t is: ?card))").await?,
        vec![r#"(todo/day todo:1 is: {:title "Buy milk" :day "mon" :texts {: "early"}})"#]
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
