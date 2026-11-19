//! A query whose answer is markup, maintained on a real branch: each
//! layer of its plan a query of its own, the rows that come and go fed to
//! a maintainer, and the patches it gives checked against the tree
//! rendered afresh.
//!
//! A layer is driven one of two ways. Subscribed: `Branch::subscribe`
//! over the layer's query, polled after each commit. Asked: the query
//! asked again after each commit, and the rows that came and went taken
//! as the difference of the two answers. The maintainer sees only rows
//! that came and went either way. A layer's query has a rule of its own;
//! `Branch::subscribe` takes no rule beside the query, so the rule is
//! asserted into the branch's session overlay, where a subscription finds
//! it and from which nothing is committed.

#![allow(deprecated)]

use anyhow::Result;
use dialog_corpus::Value;
use dialog_notation::view::tree::Slot;
use dialog_notation::{
    Analyzer, Change, Content, Error, Maintainer, Patch, Plan, Query, Rendered, Row, Shown, Step,
    compile, parse, render,
};
use dialog_query::query::Output as _;
use dialog_query::{ConceptConclusion, ConceptQuery};
use dialog_repository::Subscription;

#[path = "../examples/notebook/session.rs"]
mod session;
use session::Notebook;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// A list with a name, and its to-dos, each with a title and a state.
const CONCEPTS: &str = r#"
    (concept (list ?l name: ?name)
      (xyz.list/name of: ?l is: ?name as: :text cardinality: :one))
    (concept (todo ?t list: ?l title: ?title state: ?s)
      (xyz.todo/list of: ?t is: ?l as: :entity cardinality: :one)
      (xyz.todo/title of: ?t is: ?title as: :text cardinality: :one)
      (xyz.todo/state of: ?t is: ?s as: [:open :done] cardinality: :one))"#;

/// The card of a list: its name, and an item a to-do, each a query of
/// its own, collected.
const CARD: &str = r#"
    (query (todo/item ?t list: ?l is: ?item)
      (todo ?t list: ?l title: ?title state: ?s)
      (view ?item [:li {:class ?s} ?title]))
    (query (list/card ?l is: ?card)
      (list ?l name: ?name)
      (todo/item _ list: ?l is: (collect ?items))
      (view ?card
        [:article
          [:h2 ?name]
          [:ul ?items]]))"#;

/// The plan `asked` compiles to, after `documents`.
fn plan_of(documents: &[&str], asked: &str) -> Result<Plan> {
    let text = format!("{}\n{asked}", documents.join("\n"));
    let analysis = Analyzer::new()?.analyze(&parse(&text)?);
    let refused: Vec<String> = analysis
        .diagnostics
        .iter()
        .filter_map(|diagnostic| diagnostic.refusal.as_ref().map(ToString::to_string))
        .collect();
    assert!(refused.is_empty(), "{refused:?}");
    for tree in &analysis.analyzed {
        if let [Step::View(plan)] = compile(tree)?.as_slice() {
            return Ok((**plan).clone());
        }
    }
    Err(anyhow::anyhow!(
        "{asked} is no query whose answer is markup"
    ))
}

/// Why `asked`, after `documents`, is refused.
fn refusal(documents: &[&str], asked: &str) -> Error {
    let text = format!("{}\n{asked}", documents.join("\n"));
    let analysis = Analyzer::new()
        .expect("an analyzer")
        .analyze(&parse(&text).expect("forms"));
    if let Some(error) = analysis
        .diagnostics
        .iter()
        .find_map(|diagnostic| diagnostic.refusal.clone())
    {
        return error;
    }
    for tree in &analysis.analyzed {
        if let Err(error) = compile(tree) {
            return error;
        }
    }
    panic!("{asked} was not refused")
}

fn entity(uri: &str) -> Value {
    let [tree] = parse(uri).expect("an entity").try_into().expect("one");
    match tree {
        dialog_notation::Syntax::Constant(value) => value,
        other => panic!("{other} is no entity"),
    }
}

/// How a layer's rows are taken from the branch.
#[derive(Clone, Copy, Debug, PartialEq)]
enum How {
    Subscribed,
    Asked,
}

enum Feed {
    Subscribed(Box<Subscription<ConceptQuery>>),
    Asked(Vec<Row>),
}

/// A plan's layers, each fed from the branch, and its maintainer. A
/// layer with no condition has no query and no feed.
struct Driven {
    plan: Plan,
    entity: Option<Value>,
    queries: Vec<Option<Query>>,
    feeds: Vec<Option<Feed>>,
    maintainer: Maintainer,
    tree: Rendered,
}

/// Every row of a layer's query, as the branch stands.
async fn ask(
    notebook: &Notebook,
    plan: &Plan,
    layer: usize,
    query: &Query,
    entity: Option<&Value>,
) -> Result<Vec<Row>> {
    let mut asked = notebook.branch.query();
    if let Some(rule) = &query.rule {
        asked = asked.with(rule.clone());
    }
    let found: Vec<ConceptConclusion> = asked
        .select(query.select.clone())
        .perform(&notebook.operator)
        .try_vec()
        .await?;
    let mut rows = found
        .iter()
        .map(|conclusion| plan.row(layer, query, conclusion, entity))
        .collect::<Result<Vec<_>, _>>()?;
    rows.sort_by_key(|row| format!("{row:?}"));
    Ok(rows)
}

impl Driven {
    async fn new(
        notebook: &Notebook,
        plan: Plan,
        entity: impl Into<Option<Value>>,
        how: How,
    ) -> Result<Driven> {
        let entity = entity.into();
        let mut queries = Vec::new();
        let mut feeds = Vec::new();
        for layer in 0..plan.layers.len() {
            if plan.layers[layer].conditions.is_empty() {
                queries.push(None);
                feeds.push(None);
                continue;
            }
            let query = plan.query(layer, entity.as_ref())?;
            feeds.push(Some(match how {
                How::Subscribed => {
                    if let Some(rule) = &query.rule {
                        notebook.branch.overlay().assert(rule.clone())?;
                    }
                    Feed::Subscribed(Box::new(notebook.branch.subscribe(query.select.clone())))
                }
                How::Asked => Feed::Asked(Vec::new()),
            }));
            queries.push(Some(query));
        }
        Ok(Driven {
            maintainer: Maintainer::new(plan.clone(), entity.clone()),
            plan,
            entity,
            queries,
            feeds,
            tree: Rendered::default(),
        })
    }

    /// For each subscribed layer, how many times it was evaluated whole.
    fn evaluations(&self) -> Vec<usize> {
        self.feeds
            .iter()
            .flatten()
            .filter_map(|feed| match feed {
                Feed::Subscribed(subscription) => Some(subscription.recomputes()),
                Feed::Asked(_) => None,
            })
            .collect()
    }

    /// The rows that came and went since the last step, fed to the
    /// maintainer as one change; the patches it gives, checked. The first
    /// step also gives what stands with no row.
    async fn step(&mut self, notebook: &Notebook) -> Result<Vec<Patch>> {
        let first = self.tree == Rendered::default();
        let mut changes = Vec::new();
        for (layer, feed) in self.feeds.iter_mut().enumerate() {
            let (Some(feed), Some(query)) = (feed, &self.queries[layer]) else {
                continue;
            };
            let entity = self.entity.as_ref();
            let (arrived, left) = match feed {
                Feed::Subscribed(subscription) => {
                    match subscription.poll(&notebook.operator).await? {
                        Some(delta) => (
                            delta
                                .asserted
                                .iter()
                                .map(|row| self.plan.row(layer, query, row, entity))
                                .collect::<Result<Vec<_>, _>>()?,
                            delta
                                .retracted
                                .iter()
                                .map(|row| self.plan.row(layer, query, row, entity))
                                .collect::<Result<Vec<_>, _>>()?,
                        ),
                        None => (Vec::new(), Vec::new()),
                    }
                }
                Feed::Asked(last) => {
                    let now = ask(notebook, &self.plan, layer, query, entity).await?;
                    let arrived = now
                        .iter()
                        .filter(|row| !last.contains(row))
                        .cloned()
                        .collect();
                    let left = last
                        .iter()
                        .filter(|row| !now.contains(row))
                        .cloned()
                        .collect();
                    *last = now;
                    (arrived, left)
                }
            };
            changes.push(Change {
                layer,
                arrived,
                left,
            });
        }
        let mut patches = Vec::new();
        if first {
            // What stands with no row is there before any row arrives.
            for (index, shown) in self.maintainer.tree().roots.into_iter().enumerate() {
                patches.push(Patch::Insert {
                    parent: Vec::new(),
                    slot: 0,
                    index,
                    instance: shown,
                });
            }
            for patch in &patches {
                self.tree.apply(patch).map_err(anyhow::Error::msg)?;
            }
        }
        let more = self.maintainer.apply(&changes);
        for patch in &more {
            self.tree.apply(patch).map_err(anyhow::Error::msg)?;
        }
        patches.extend(more);
        let maintained = self.maintainer.tree();
        assert_eq!(self.tree, maintained, "the patches make the tree");
        let mut every = Vec::new();
        for (layer, query) in self.queries.iter().enumerate() {
            every.push(match query {
                Some(query) => {
                    ask(notebook, &self.plan, layer, query, self.entity.as_ref()).await?
                }
                None => Vec::new(),
            });
        }
        assert_eq!(
            maintained,
            render(&self.plan, self.entity.as_ref(), &every),
            "the tree maintained is the tree rendered afresh"
        );
        Ok(patches)
    }
}

/// Write a document to the branch.
async fn write(notebook: &mut Notebook, text: &str) -> Result<()> {
    let (_, diagnostics) = notebook.submit(text).await?;
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    Ok(())
}

fn card() -> dialog_notation::Address {
    vec![(0, vec![entity("list:home")])]
}

fn heading() -> dialog_notation::Address {
    let mut at = card();
    at.push((0, Vec::new()));
    at
}

fn items() -> dialog_notation::Address {
    let mut at = card();
    at.push((1, Vec::new()));
    at
}

fn item(t: &str) -> dialog_notation::Address {
    let mut at = items();
    at.push((0, vec![entity(t)]));
    at
}

fn element(tag: &str, attributes: &[(&str, &str)], slots: Vec<Slot>) -> Content {
    Content::Element {
        tag: tag.into(),
        attributes: attributes
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect(),
        events: Vec::new(),
        slots,
    }
}

fn shown(key: Vec<Value>, content: Content) -> Shown {
    Shown { key, content }
}

fn li(t: &str, title: &str, state: &str) -> Shown {
    shown(
        vec![entity(t)],
        element("li", &[("class", state)], vec![Slot::Text(title.into())]),
    )
}

/// The to-do card through every change, driven `how`: each change a
/// patch of the one instance it is behind.
async fn the_card_through_its_changes(how: How) -> Result<Vec<Vec<Patch>>> {
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, CONCEPTS).await?;
    write(&mut notebook, r#"(assert (list list:home name: "Home"))"#).await?;
    let plan = plan_of(&[CONCEPTS, CARD], "(select (list/card ?l is: ?card))")?;
    let mut card_of = Driven::new(&notebook, plan, entity("list:home"), how).await?;
    let mut log = Vec::new();

    let first = card_of.step(&notebook).await?;
    assert_eq!(
        first,
        [Patch::Insert {
            parent: Vec::new(),
            slot: 0,
            index: 0,
            instance: shown(
                vec![entity("list:home")],
                element(
                    "article",
                    &[],
                    vec![
                        Slot::Instances(vec![shown(
                            Vec::new(),
                            element("h2", &[], vec![Slot::Text("Home".into())])
                        )]),
                        Slot::Instances(vec![shown(
                            Vec::new(),
                            element("ul", &[], vec![Slot::Instances(Vec::new())])
                        )]),
                    ]
                )
            ),
        }]
    );
    log.push(first);

    write(
        &mut notebook,
        r#"(assert (todo todo:1 list: list:home title: "Buy milk" state: :open))"#,
    )
    .await?;
    let added = card_of.step(&notebook).await?;
    // A keyword's value is the entity of its name: `:open` is `open:`.
    assert_eq!(
        added,
        [Patch::Insert {
            parent: items(),
            slot: 0,
            index: 0,
            instance: li("todo:1", "Buy milk", "open:"),
        }]
    );
    log.push(added);

    write(&mut notebook, r#"(assert (list list:home name: "Chores"))"#).await?;
    let renamed = card_of.step(&notebook).await?;
    assert_eq!(
        renamed,
        [Patch::SetText {
            at: heading(),
            slot: 0,
            text: "Chores".into(),
        }]
    );
    log.push(renamed);

    write(
        &mut notebook,
        r#"(assert (todo todo:1 list: list:home title: "Buy oat milk" state: :open))"#,
    )
    .await?;
    let retitled = card_of.step(&notebook).await?;
    assert_eq!(
        retitled,
        [Patch::SetText {
            at: item("todo:1"),
            slot: 0,
            text: "Buy oat milk".into(),
        }]
    );
    log.push(retitled);

    write(
        &mut notebook,
        r#"(assert (todo todo:1 list: list:home title: "Buy oat milk" state: :done))"#,
    )
    .await?;
    let completed = card_of.step(&notebook).await?;
    assert_eq!(
        completed,
        [Patch::SetAttribute {
            at: item("todo:1"),
            name: "class".into(),
            value: "done:".into(),
        }]
    );
    log.push(completed);

    write(
        &mut notebook,
        r#"(assert (todo todo:2 list: list:home title: "Call mum" state: :open))"#,
    )
    .await?;
    let second = card_of.step(&notebook).await?;
    assert_eq!(
        second,
        [Patch::Insert {
            parent: items(),
            slot: 0,
            index: 1,
            instance: li("todo:2", "Call mum", "open:"),
        }]
    );
    log.push(second);

    write(
        &mut notebook,
        r#"(retract (todo todo:2 list: list:home title: "Call mum" state: :open))"#,
    )
    .await?;
    let removed = card_of.step(&notebook).await?;
    assert_eq!(removed, [Patch::Remove { at: item("todo:2") }]);
    log.push(removed);

    write(
        &mut notebook,
        r#"(retract (todo todo:1 list: list:home title: "Buy oat milk" state: :done))"#,
    )
    .await?;
    let emptied = card_of.step(&notebook).await?;
    assert_eq!(emptied, [Patch::Remove { at: item("todo:1") }]);
    log.push(emptied);
    let [root] = card_of.tree.roots.as_slice() else {
        panic!("the card stays: {:?}", card_of.tree);
    };
    let Content::Element { tag, slots, .. } = &root.content;
    assert_eq!(tag, "article");
    assert_eq!(slots.len(), 2, "its heading and its list");

    // A write that touches no layer gives no patch.
    write(&mut notebook, r#"(assert (list list:work name: "Work"))"#).await?;
    assert_eq!(card_of.step(&notebook).await?, []);
    if how == How::Subscribed {
        // Each layer evaluated whole once, at its first poll; every change
        // after was maintained from the change.
        assert_eq!(card_of.evaluations(), [1, 1]);
    }
    Ok(log)
}

/// Each layer a subscription: adding a to-do is one insert, renaming the
/// list one text, retitling a to-do one text, completing it one
/// attribute, removing it one remove, and emptying the list keeps the
/// card. After every step the tree maintained is the tree rendered
/// afresh.
#[dialog_common::test]
async fn it_maintains_the_card_from_subscriptions() -> Result<()> {
    the_card_through_its_changes(How::Subscribed).await?;
    Ok(())
}

/// The same changes with every layer asked again after each commit give
/// the same patches.
#[dialog_common::test]
async fn it_maintains_the_card_by_asking_again_alike() -> Result<()> {
    let subscribed = the_card_through_its_changes(How::Subscribed).await?;
    let asked = the_card_through_its_changes(How::Asked).await?;
    assert_eq!(subscribed, asked);
    Ok(())
}

/// To-dos with a title, and tags of many values.
const TAGGED: &str = r#"
    (concept (task ?t title: ?title)
      (xyz.task/title of: ?t is: ?title as: :text cardinality: :one))
    (concept (task/tag ?t tag: ?g)
      (xyz.task/tag of: ?t is: ?g as: :text cardinality: :many))

    ; A chip is built from a tag, so it is a query of its own.
    (query (task/chip ?t tag: ?g is: ?chip)
      (task/tag ?t tag: ?g)
      (view ?chip [:span {:class "tag"} ?g]))

    ; The row takes all its chips: none is no chip, and the row stays.
    (query (task/row ?t is: ?row)
      (task ?t title: ?title)
      (task/chip ?t is: (collect ?chips))
      (view ?row [:li ?title ?chips]))

    ; The tag's read required: a row for each way it holds.
    (query (task/flat ?t is: ?row)
      (task ?t title: ?title)
      (task/tag ?t tag: ?g)
      (view ?row [:li ?title [:span {:class "tag"} ?g]]))"#;

fn chip(tag: &str) -> Shown {
    shown(
        vec![Value::String(tag.into())],
        element("span", &[("class", "tag")], vec![Slot::Text(tag.into())]),
    )
}

fn row(t: &str, title: &str, chips: Vec<Shown>) -> Shown {
    shown(
        vec![entity(t)],
        element(
            "li",
            &[],
            vec![Slot::Text(title.into()), Slot::Instances(chips)],
        ),
    )
}

/// The to-do row with its chips collected: "Buy milk" with two, "Call
/// mum" with its `li` and none. A tag added to "Call mum" is one insert
/// of a `span`; a to-do renamed is one text; a tag removed is one remove.
#[dialog_common::test]
async fn it_maintains_a_row_whose_chips_are_collected() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    write(
        &mut notebook,
        &dialog_notation::read(TAGGED)?
            .iter()
            .filter(|form| form.head() == Some("concept"))
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .await?;
    write(
        &mut notebook,
        r#"(assert (task task:1 title: "Buy milk"))
           (assert (task/tag task:1 tag: "errand"))
           (assert (task/tag task:1 tag: "home"))
           (assert (task task:2 title: "Call mum"))"#,
    )
    .await?;
    let plan = plan_of(&[TAGGED], "(select (task/row ?t is: ?row))")?;
    let mut rows = Driven::new(&notebook, plan, None, How::Subscribed).await?;
    rows.step(&notebook).await?;
    assert_eq!(
        rows.tree.roots,
        [
            row("task:1", "Buy milk", vec![chip("errand"), chip("home")]),
            row("task:2", "Call mum", Vec::new()),
        ]
    );
    assert_eq!(
        rows.tree.hiccup(),
        r#"[:li "Buy milk" [:span {:class "tag"} "errand"] [:span {:class "tag"} "home"]] [:li "Call mum"]"#
    );

    write(&mut notebook, r#"(assert (task/tag task:2 tag: "phone"))"#).await?;
    assert_eq!(
        rows.step(&notebook).await?,
        [Patch::Insert {
            parent: vec![(0, vec![entity("task:2")])],
            slot: 1,
            index: 0,
            instance: chip("phone"),
        }]
    );
    write(
        &mut notebook,
        r#"(assert (task task:1 title: "Buy oat milk"))"#,
    )
    .await?;
    assert_eq!(
        rows.step(&notebook).await?,
        [Patch::SetText {
            at: vec![(0, vec![entity("task:1")])],
            slot: 0,
            text: "Buy oat milk".into(),
        }]
    );
    write(&mut notebook, r#"(retract (task/tag task:1 tag: "home"))"#).await?;
    assert_eq!(
        rows.step(&notebook).await?,
        [Patch::Remove {
            at: vec![
                (0, vec![entity("task:1")]),
                (1, vec![Value::String("home".into())])
            ],
        }]
    );

    // The tag's read required instead: "Call mum" has no tag, so no `li`.
    let mut flat = Notebook::open().await?;
    write(
        &mut flat,
        &dialog_notation::read(TAGGED)?
            .iter()
            .filter(|form| form.head() == Some("concept"))
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .await?;
    write(
        &mut flat,
        r#"(assert (task task:1 title: "Buy milk"))
           (assert (task/tag task:1 tag: "errand"))
           (assert (task task:2 title: "Call mum"))"#,
    )
    .await?;
    let plan = plan_of(&[TAGGED], "(select (task/flat ?t is: ?row))")?;
    let mut required = Driven::new(&flat, plan, None, How::Subscribed).await?;
    required.step(&flat).await?;
    assert_eq!(
        required.tree.hiccup(),
        r#"[:li "Buy milk" [:span {:class "tag"} "errand"]]"#,
        "no `li` for Call mum"
    );
    Ok(())
}

/// The list card with a count of what is open: `0 open` and an empty
/// `ul` for a list with nothing on it. Adding a to-do is one insert under
/// the `ul` and the count's text set.
#[dialog_common::test]
async fn it_counts_what_a_card_collects() -> Result<()> {
    let counted = r#"
        (query (todo/item ?t list: ?l is: ?item)
          (todo ?t list: ?l title: ?title state: ?s)
          (view ?item [:li {:class ?s} ?title]))
        (query (list/summary ?l is: ?card)
          (list ?l name: ?name)
          (todo (collect ?open) list: ?l state: :open)
          (todo/item _ list: ?l is: (collect ?items))
          (is ?count (count ?open))
          (view ?card
            [:article
              [:h2 ?name]
              [:p ?count " open"]
              [:ul ?items]]))"#;
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, CONCEPTS).await?;
    write(&mut notebook, r#"(assert (list list:home name: "Home"))"#).await?;
    let plan = plan_of(&[CONCEPTS, counted], "(select (list/summary ?l is: ?card))")?;
    let mut card_of = Driven::new(&notebook, plan, entity("list:home"), How::Subscribed).await?;
    card_of.step(&notebook).await?;
    assert_eq!(
        card_of.tree.hiccup(),
        r#"[:article [:h2 "Home"] [:p "0" " open"] [:ul]]"#
    );
    write(
        &mut notebook,
        r#"(assert (todo todo:1 list: list:home title: "Buy milk" state: :open))"#,
    )
    .await?;
    let mut patches = card_of.step(&notebook).await?;
    patches.sort_by_key(|patch| format!("{patch:?}"));
    let mut count = card();
    count.push((1, Vec::new()));
    let mut list = card();
    list.push((2, Vec::new()));
    assert_eq!(
        patches,
        [
            Patch::Insert {
                parent: list,
                slot: 0,
                index: 0,
                instance: li("todo:1", "Buy milk", "open:"),
            },
            Patch::SetText {
                at: count,
                slot: 0,
                text: "1".into(),
            },
        ]
    );
    assert_eq!(
        card_of.tree.hiccup(),
        r#"[:article [:h2 "Home"] [:p "1" " open"] [:ul [:li {:class "open:"} "Buy milk"]]]"#
    );
    Ok(())
}

/// The forms of a document, its writes kept or not.
fn forms_of(text: &str, writes: bool) -> Result<String> {
    let forms = dialog_notation::read(text)?;
    Ok(forms
        .iter()
        .filter(|form| writes || !matches!(form.head(), Some("assert" | "retract")))
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n"))
}

/// The concepts of a document alone.
fn concepts_of(text: &str) -> Result<String> {
    Ok(dialog_notation::read(text)?
        .iter()
        .filter(|form| form.head() == Some("concept"))
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n"))
}

const CORE: &str = include_str!("../sketches/core.clj");
const NOTEBOOK: &str = include_str!("../sketches/notebook.clj");
const NOTEBOOK_VIEWS: &str = include_str!("../sketches/notebook-views.clj");
const TABLE: &str = include_str!("../sketches/table.clj");
const TABLE_VIEWS: &str = include_str!("../sketches/table-views.clj");

/// A branch with core and a sketch's concepts on it.
async fn sketched(sketch: &str) -> Result<Notebook> {
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, CORE).await?;
    write(&mut notebook, &concepts_of(sketch)?).await?;
    Ok(notebook)
}

/// The notebook's page: whole for a notebook with no block. A block
/// placed before the first is one insert at index 0 of the order, and the
/// order follows the positions.
#[dialog_common::test]
async fn it_maintains_the_notebook_page_by_position() -> Result<()> {
    let mut notebook = sketched(NOTEBOOK).await?;
    write(
        &mut notebook,
        r#"(assert (notebook/named notebook:1 title: "Plan"))"#,
    )
    .await?;
    let sketch = forms_of(NOTEBOOK, false)?;
    let plan = plan_of(
        &[CORE, &sketch, NOTEBOOK_VIEWS],
        "(select (notebook/ui ?n is: ?ui))",
    )?;
    let mut page = Driven::new(&notebook, plan, entity("notebook:1"), How::Subscribed).await?;
    page.step(&notebook).await?;
    let empty = r#"[:notebook-save {:style "display: block;"} [:tonk-notebook {:data-subject "notebook:1" :data-notebook "notebook:1" :title "Plan" :style "display: block;"} [:div {:class "notebook-data" :hidden ""} [:div {:class "notebook-order"}] [:div {:class "notebook-block-rows"}]]]]"#;
    assert_eq!(page.tree.hiccup(), empty, "the whole page, with no block");

    write(
        &mut notebook,
        r#"(assert (notebook/blocks notebook:1 block: {"N5" block:b}))
           (assert (notebook/block block:b notebook: notebook:1 source: "Second"))"#,
    )
    .await?;
    page.step(&notebook).await?;
    write(
        &mut notebook,
        r#"(assert (notebook/blocks notebook:1 block: {"N1" block:a}))"#,
    )
    .await?;
    let order: dialog_notation::Address = vec![
        (0, vec![entity("notebook:1")]),
        (0, Vec::new()),
        (0, Vec::new()),
        (0, Vec::new()),
    ];
    assert_eq!(
        page.step(&notebook).await?,
        [Patch::Insert {
            parent: order,
            slot: 0,
            index: 0,
            instance: shown(
                vec![Value::String("N1".into())],
                element(
                    "span",
                    &[
                        ("class", "notebook-order__item"),
                        ("data-block", "block:a"),
                        ("data-key", "N1")
                    ],
                    Vec::new()
                )
            ),
        }]
    );
    assert!(
        page.tree.hiccup().contains(
            r#"[:div {:class "notebook-order"} [:span {:class "notebook-order__item" :data-block "block:a" :data-key "N1"}] [:span {:class "notebook-order__item" :data-block "block:b" :data-key "N5"}]]"#
        ),
        "{}",
        page.tree.hiccup()
    );
    Ok(())
}

/// An index, a query of no entity that only collects: one container with
/// nothing in it when there is nothing, and a notebook added is one insert
/// inside it, not a second container.
#[dialog_common::test]
async fn it_maintains_an_index_as_one_container() -> Result<()> {
    let mut notebook = sketched(NOTEBOOK).await?;
    let sketch = forms_of(NOTEBOOK, false)?;
    let plan = plan_of(
        &[CORE, &sketch, NOTEBOOK_VIEWS],
        "(select (notebook/directory is: ?directory))",
    )?;
    let mut index = Driven::new(&notebook, plan, None, How::Subscribed).await?;
    index.step(&notebook).await?;
    assert_eq!(index.tree.hiccup(), r#"[:div {:class "notebook-index"}]"#);
    write(
        &mut notebook,
        r#"(assert (notebook/named notebook:1 title: "Plan"))"#,
    )
    .await?;
    let patches = index.step(&notebook).await?;
    assert!(
        matches!(patches.as_slice(), [Patch::Insert { parent, slot: 0, index: 0, .. }]
            if *parent == vec![(0, Vec::new())]),
        "{patches:?}"
    );
    assert_eq!(
        index.tree.hiccup(),
        r#"[:div {:class "notebook-index"} [:a {:class "notebook-index__item" :href "/notebook/notebook:1"} [:span {:class "notebook-index__title"} "Plan"]]]"#
    );
    assert_eq!(index.tree.roots.len(), 1, "one container");
    Ok(())
}

/// Every query of the sketches whose answer is markup, laid out: each
/// place that has a layer, and what tells its instances apart.
#[dialog_common::test]
async fn it_lays_out_every_view_of_the_sketches() -> Result<()> {
    let notebook_sketch = forms_of(NOTEBOOK, true)?;
    let table_sketch = forms_of(TABLE, true)?;
    let mut said = Vec::new();
    for (documents, asked) in [
        (
            [CORE, notebook_sketch.as_str(), NOTEBOOK_VIEWS],
            "(select (notebook/ui ?n is: ?ui))",
        ),
        (
            [CORE, notebook_sketch.as_str(), NOTEBOOK_VIEWS],
            "(select (notebook/directory is: ?d))",
        ),
        (
            [CORE, notebook_sketch.as_str(), NOTEBOOK_VIEWS],
            "(select (notebook/index-page ?site is: ?page))",
        ),
        (
            [CORE, notebook_sketch.as_str(), NOTEBOOK_VIEWS],
            "(select (notebook/page ?site is: ?page))",
        ),
        (
            [CORE, table_sketch.as_str(), TABLE_VIEWS],
            "(select (table/ui ?t is: ?ui))",
        ),
        (
            [CORE, table_sketch.as_str(), TABLE_VIEWS],
            "(select (table/directory is: ?d))",
        ),
    ] {
        let plan = plan_of(&documents, asked)?;
        let layers: Vec<String> = plan
            .layers
            .iter()
            .map(|layer| {
                let place = &plan.elements[layer.element];
                let tag = match &place.placed {
                    dialog_notation::Placed::Element(tag) => tag.clone(),
                    dialog_notation::Placed::Count(count) => format!("count {count}"),
                };
                format!("{tag} by [{}]", place.key.join(" "))
            })
            .collect();
        said.push(format!("{}: {}", plan.name, layers.join(", ")));
    }
    assert_eq!(
        said,
        [
            "notebook/ui: notebook-save by [n], span by [q1.k], div by [q2.b]",
            "notebook/directory: div by [], a by [q1.n]",
            "notebook/index-page: tonk-notebook-index by [site], a by [q2.n]",
            "notebook/page: div by [site], notebook-save by [], span by [q2.k], div by [q3.b]",
            "table/ui: tonk-table by [t], div by [q1.s], div by [q2.c], div by [q3.k], div by \
             [q4.r]",
            "table/directory: div by [], tonk-table by [q1.t], div by [q2.s], div by [q3.c], div \
             by [q4.k], div by [q5.r]",
        ]
    );
    Ok(())
}

/// The table's page and directory, restated: with no table the
/// directory stands empty; with the sketch's seeds it holds the one
/// workbook's page, and that page holds its sheet and its cells; no
/// column or row is sized, so their sets stand empty.
#[dialog_common::test]
async fn it_renders_the_tables_page_and_directory() -> Result<()> {
    let mut notebook = sketched(TABLE).await?;
    let sketch = forms_of(TABLE, false)?;
    let directory = plan_of(
        &[CORE, &sketch, TABLE_VIEWS],
        "(select (table/directory is: ?d))",
    )?;
    let page = plan_of(
        &[CORE, &sketch, TABLE_VIEWS],
        "(select (table/ui ?t is: ?ui))",
    )?;
    let mut all = Driven::new(&notebook, directory, None, How::Subscribed).await?;
    all.step(&notebook).await?;
    assert_eq!(all.tree.hiccup(), r#"[:div {:class "table-directory"}]"#);
    let seeds: String = dialog_notation::read(TABLE)?
        .iter()
        .filter(|form| matches!(form.head(), Some("assert" | "retract")))
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    write(&mut notebook, &seeds).await?;
    all.step(&notebook).await?;
    let mut one = Driven::new(&notebook, page, entity("id:table/book"), How::Subscribed).await?;
    one.step(&notebook).await?;
    let written = one.tree.hiccup();
    assert!(
        written.starts_with(r#"[:tonk-table {:subject "id:table/book"}"#),
        "{written}"
    );
    for (rows, count) in [
        ("table-sheet-row", 1),
        ("table-cell-row", 14),
        ("table-column-row", 0),
        ("table-rowsize-row", 0),
    ] {
        assert_eq!(
            written.matches(&format!("{{:class \"{rows}\"")).count(),
            count,
            "{rows}: {written}"
        );
    }
    let [workbook] = all.tree.roots.as_slice() else {
        panic!("one directory: {:?}", all.tree);
    };
    assert_eq!(
        Rendered {
            roots: vec![workbook.clone()]
        }
        .hiccup(),
        format!(r#"[:div {{:class "table-directory"}} {written}]"#)
    );
    Ok(())
}

/// A query named under a declared concept and defined by no document is
/// shown as its source: the concept applied to the entity with its
/// fields, narrowed by what the call gives. It follows a field.
#[dialog_common::test]
async fn it_shows_a_query_not_found_as_its_source() -> Result<()> {
    let board = r#"
        (query (list/board ?l is: ?board)
          (list ?l name: ?name)
          (todo/missing _ list: ?l is: (collect ?items))
          (view ?board [:article [:h2 ?name] ?items]))"#;
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, CONCEPTS).await?;
    write(
        &mut notebook,
        r#"(assert (list list:home name: "Home"))
           (assert (todo todo:1 list: list:home title: "Buy milk" state: :open))"#,
    )
    .await?;
    let plan = plan_of(&[CONCEPTS, board], "(select (list/board ?l is: ?board))")?;
    let mut shown_of = Driven::new(&notebook, plan, entity("list:home"), How::Subscribed).await?;
    shown_of.step(&notebook).await?;
    assert_eq!(
        shown_of.tree.hiccup(),
        r#"[:article [:h2 "Home"] [:view-source "(todo todo:1 list: list:home state: :open title: \"Buy milk\")"]]"#
    );
    write(
        &mut notebook,
        r#"(assert (todo todo:1 list: list:home title: "Buy oat milk" state: :open))"#,
    )
    .await?;
    let patches = shown_of.step(&notebook).await?;
    assert!(
        matches!(patches.as_slice(), [Patch::SetText { text, .. }]
            if text == r#"(todo todo:1 list: list:home state: :open title: "Buy oat milk")"#),
        "{patches:?}"
    );
    Ok(())
}

/// A query whose answer is markup, asked with `select`, is rendered once
/// by the session.
#[dialog_common::test]
async fn it_renders_a_view_asked_once() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, CONCEPTS).await?;
    write(&mut notebook, CARD).await?;
    write(
        &mut notebook,
        r#"(assert (list list:home name: "Home"))
           (assert (todo todo:1 list: list:home title: "Buy milk" state: :open))"#,
    )
    .await?;
    let (report, _) = notebook
        .submit("(select (list/card list:home is: ?card))")
        .await?;
    assert_eq!(
        report,
        [r#"[:article [:h2 "Home"] [:ul [:li {:class "open:"} "Buy milk"]]]"#]
    );
    Ok(())
}

/// What a view may not say is refused where it is analyzed: the view
/// form as it was, a view called or nested in a template, a variable no
/// condition binds, an event of no declared command or of other fields,
/// `false` as an attribute, and the whole of a read put in markup.
#[dialog_common::test]
async fn it_refuses_what_a_view_may_not_say() -> Result<()> {
    let command = r#"
        (concept (todo/toggle todo: ?t)
          (xyz.todo.toggle/todo of: ?c is: ?t as: :entity in: :control))"#;
    for (text, says) in [
        (
            "(view todo :row [:li ?title] (todo ?t title: ?title))",
            "a view is a query whose answer is markup",
        ),
        (
            "(query (list/x ?l is: ?x) (list ?l name: ?name) (view ?x [:ul (view todo :item)]))",
            "a view is a query whose answer is markup, and one in a template is that query \
             collected",
        ),
        (
            "(query (list/x ?l is: ?x) (list ?l name: ?name) (view ?x [:ul {:view :row :of ?l}]))",
            "a view nested by reference is a query collected",
        ),
        (
            "(query (list/x ?l is: ?x) (list ?l name: ?name) (view ?x [:h2 ?other]))",
            "holds ?other, which no condition of the query binds",
        ),
        (
            "(query (list/x ?l is: ?x) (list ?l name: ?name)
               (view ?x [:h2 {:on/click (todo/nothing todo: ?l)} ?name]))",
            "which is no command declared",
        ),
        (
            "(query (list/x ?l is: ?x) (list ?l name: ?name)
               (view ?x [:h2 {:on/click (todo/toggle todo: ?l done: true)} ?name]))",
            "an event gives every field of its command and no other",
        ),
        (
            "(query (list/x ?l is: ?x) (list ?l name: ?name) (view ?x [:h2 {:hidden false} ?name]))",
            "an attribute that is not there is left out",
        ),
    ] {
        let error = refusal(&[CONCEPTS, command], text);
        assert!(
            matches!(&error, Error::Refused(why) if why.contains(says)),
            "{text}: {error:?}"
        );
    }
    let whole = refusal(
        &[CONCEPTS],
        "(query (list/x ?l is: ?x) (list ?l name: ?name)
           (todo (collect ?all) list: ?l)
           (view ?x [:ul ?all]))
         (select (list/x ?l is: ?x))",
    );
    assert!(
        matches!(&whole, Error::Unsupported(why) if why.contains("the whole of a read")),
        "{whole:?}"
    );
    Ok(())
}
