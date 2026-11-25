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
use dialog_notation::view::Slot;
use dialog_notation::{
    Analyzer, Change, Content, Error, Maintainer, Patch, Plan, Query, Rendered, Row, Shown, Step,
    compile, parse, render,
};
use dialog_query::query::Output as _;
use dialog_query::{ConceptConclusion, ConceptQuery};
use dialog_repository::Subscription;

#[path = "support/session.rs"]
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
      (todo/item _ list: ?l is: (distinct ?items))
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
    let told = |error: Error| {
        // What a view's author is told names only what they wrote.
        session::spoken(&text, &error.to_string());
        error
    };
    if let Some(error) = analysis
        .diagnostics
        .iter()
        .find_map(|diagnostic| diagnostic.refusal.clone())
    {
        return told(error);
    }
    for tree in &analysis.analyzed {
        if let Err(error) = compile(tree) {
            return told(error);
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
            instance: li("todo:1", "Buy milk", "open"),
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
            value: "done".into(),
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
            instance: li("todo:2", "Call mum", "open"),
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
    let log = the_card_through_its_changes(How::Subscribed).await?;
    let kinds: Vec<Vec<&str>> = log
        .iter()
        .map(|step| step.iter().map(kind).collect())
        .collect();
    // The card inserted; a to-do added; the list renamed; a to-do
    // retitled; one completed; another added; each removed. One patch a
    // step, each checked whole, and the tree against a fresh render, in
    // the steps themselves.
    assert_eq!(
        kinds,
        [
            ["insert"],
            ["insert"],
            ["text"],
            ["text"],
            ["attribute"],
            ["insert"],
            ["remove"],
            ["remove"],
        ]
    );
    Ok(())
}

/// What a patch does, by its kind.
fn kind(patch: &Patch) -> &'static str {
    match patch {
        Patch::Insert { .. } => "insert",
        Patch::Remove { .. } => "remove",
        Patch::SetText { .. } => "text",
        Patch::SetAttribute { .. } => "attribute",
        Patch::SetEvent { .. } => "event",
    }
}

/// The same changes with every layer asked again after each commit give
/// the same patches.
#[dialog_common::test]
async fn it_maintains_the_card_by_asking_again_alike() -> Result<()> {
    let subscribed = the_card_through_its_changes(How::Subscribed).await?;
    let asked = the_card_through_its_changes(How::Asked).await?;
    assert!(
        !subscribed.is_empty(),
        "the changes give patches to compare"
    );
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
      (task/chip ?t is: (distinct ?chips))
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

/// The rows with their chips collected, kept by the crate's driver,
/// `Live::watch`, beside the subscriptions driven by this file: through
/// an item added, a member added to its collected part, a member
/// removed, the last member removed and the item removed, the driver
/// gives the patches driven here give. The last chip removed leaves the
/// row, with no chip; the row removed is one remove.
#[dialog_common::test]
async fn it_drives_a_view_with_a_collected_part_as_the_layers_do() -> Result<()> {
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
           (assert (task/tag task:1 tag: "errand"))"#,
    )
    .await?;
    let plan = plan_of(&[TAGGED], "(select (task/row ?t is: ?row))")?;
    let mut here = Driven::new(&notebook, plan.clone(), None, How::Subscribed).await?;
    let live = dialog_notation::live::Live::new(notebook.branch.clone());
    let mut watched = live.watch(plan, None)?;
    assert_eq!(
        watched.poll(&notebook.operator).await?,
        here.step(&notebook).await?
    );
    let three = || vec![(0, vec![entity("task:3")])];
    let steps: Vec<(&str, Vec<Patch>)> = vec![
        (
            r#"(assert (task task:3 title: "Water plants"))"#,
            vec![Patch::Insert {
                parent: Vec::new(),
                slot: 0,
                index: 1,
                instance: row("task:3", "Water plants", Vec::new()),
            }],
        ),
        (
            r#"(assert (task/tag task:3 tag: "home"))"#,
            vec![Patch::Insert {
                parent: three(),
                slot: 1,
                index: 0,
                instance: chip("home"),
            }],
        ),
        (
            r#"(assert (task/tag task:3 tag: "garden"))"#,
            vec![Patch::Insert {
                parent: three(),
                slot: 1,
                index: 0,
                instance: chip("garden"),
            }],
        ),
        (
            r#"(retract (task/tag task:3 tag: "garden"))"#,
            vec![Patch::Remove {
                at: vec![
                    (0, vec![entity("task:3")]),
                    (1, vec![Value::String("garden".into())]),
                ],
            }],
        ),
        (
            r#"(retract (task/tag task:3 tag: "home"))"#,
            vec![Patch::Remove {
                at: vec![
                    (0, vec![entity("task:3")]),
                    (1, vec![Value::String("home".into())]),
                ],
            }],
        ),
        (
            r#"(retract (task task:3 title: "Water plants"))"#,
            vec![Patch::Remove { at: three() }],
        ),
    ];
    for (change, expected) in steps {
        write(&mut notebook, change).await?;
        let driven = watched.poll(&notebook.operator).await?;
        assert_eq!(driven, here.step(&notebook).await?, "after {change}");
        assert_eq!(driven, expected, "after {change}");
        if change.contains(r#"tag: "home""#) && change.starts_with("(retract") {
            assert_eq!(
                watched.tree().roots,
                [
                    row("task:1", "Buy milk", vec![chip("errand")]),
                    row("task:3", "Water plants", Vec::new()),
                ],
                "the last chip removed leaves the row, with none"
            );
        }
    }
    assert_eq!(watched.tree(), here.tree);
    Ok(())
}

/// A board of lists, its lists each a card of rows, each row with its
/// chips: a view nested two levels below its root.
const BOARD: &str = r#"
    (concept (task ?t list: ?l title: ?title)
      (xyz.task/list of: ?t is: ?l as: :entity cardinality: :one)
      (xyz.task/title of: ?t is: ?title as: :text cardinality: :one))
    (concept (task/tag ?t tag: ?g)
      (xyz.task/tag of: ?t is: ?g as: :text cardinality: :many))
    (concept (shelf ?l name: ?name)
      (xyz.shelf/name of: ?l is: ?name as: :text cardinality: :one))

    (query (task/chip ?t tag: ?g is: ?chip)
      (task/tag ?t tag: ?g)
      (view ?chip [:span {:class "tag"} ?g]))
    (query (task/line ?t list: ?l is: ?line)
      (task ?t list: ?l title: ?title)
      (task/chip ?t is: (distinct ?chips))
      (view ?line [:li ?title ?chips]))
    (query (shelf/card ?l is: ?card)
      (shelf ?l name: ?name)
      (task/line _ list: ?l is: (distinct ?lines))
      (view ?card [:section [:h2 ?name] [:ul ?lines]]))"#;

/// A view two levels deep is kept by patches at every level: a chip
/// added to a row of a card is one insert under that row, a row renamed
/// is one text, a row added is one insert under its card, and a chip
/// removed is one remove. The crate's driver gives the patches the
/// subscriptions driven here give, and every step's tree is the tree
/// rendered afresh.
#[dialog_common::test]
async fn it_maintains_a_view_nested_two_levels_deep() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    write(
        &mut notebook,
        &dialog_notation::read(BOARD)?
            .iter()
            .filter(|form| form.head() == Some("concept"))
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .await?;
    write(
        &mut notebook,
        r#"(assert (shelf list:home name: "Home"))
           (assert (task task:1 list: list:home title: "Buy milk"))
           (assert (task/tag task:1 tag: "errand"))"#,
    )
    .await?;
    let plan = plan_of(&[BOARD], "(select (shelf/card ?l is: ?card))")?;
    let mut here = Driven::new(&notebook, plan.clone(), None, How::Subscribed).await?;
    let live = dialog_notation::live::Live::new(notebook.branch.clone());
    let mut watched = live.watch(plan, None)?;
    assert_eq!(
        watched.poll(&notebook.operator).await?,
        here.step(&notebook).await?
    );
    assert_eq!(
        here.tree.hiccup(),
        r#"[:section [:h2 "Home"] [:ul [:li "Buy milk" [:span {:class "tag"} "errand"]]]]"#
    );
    let task = |t: &str| item(t);
    let steps: Vec<(&str, Vec<Patch>)> = vec![
        (
            r#"(assert (task/tag task:1 tag: "shop"))"#,
            vec![Patch::Insert {
                parent: task("task:1"),
                slot: 1,
                index: 1,
                instance: chip("shop"),
            }],
        ),
        (
            r#"(assert (task task:1 list: list:home title: "Buy oat milk"))"#,
            vec![Patch::SetText {
                at: task("task:1"),
                slot: 0,
                text: "Buy oat milk".into(),
            }],
        ),
        (
            r#"(assert (task task:2 list: list:home title: "Call mum"))"#,
            vec![Patch::Insert {
                parent: items(),
                slot: 0,
                index: 1,
                instance: row("task:2", "Call mum", Vec::new()),
            }],
        ),
        (
            r#"(retract (task/tag task:1 tag: "errand"))"#,
            vec![Patch::Remove {
                at: {
                    let mut at = task("task:1");
                    at.push((1, vec![Value::String("errand".into())]));
                    at
                },
            }],
        ),
    ];
    for (change, expected) in steps {
        write(&mut notebook, change).await?;
        let driven = watched.poll(&notebook.operator).await?;
        assert_eq!(driven, here.step(&notebook).await?, "after {change}");
        assert_eq!(driven, expected, "after {change}");
    }
    assert_eq!(
        here.tree.hiccup(),
        r#"[:section [:h2 "Home"] [:ul [:li "Buy oat milk" [:span {:class "tag"} "shop"]] [:li "Call mum"]]]"#
    );
    assert_eq!(watched.tree(), here.tree);
    Ok(())
}

/// The list card with a count of what is open, and an item a to-do.
const SUMMARY: &str = r#"
    (query (todo/item ?t list: ?l is: ?item)
      (todo ?t list: ?l title: ?title state: ?s)
      (view ?item [:li {:class ?s} ?title]))
    (query (list/summary ?l is: ?card)
      (list ?l name: ?name)
      (todo (distinct ?open) list: ?l state: :open ...)
      (todo/item _ list: ?l is: (distinct ?items))
      (is ?count (count ?open))
      (view ?card
        [:article
          [:h2 ?name]
          [:p ?count " open"]
          [:ul ?items]]))"#;

/// The list card with a count of what is open: `0 open` and an empty
/// `ul` for a list with nothing on it. Adding a to-do is one insert under
/// the `ul` and the count's text set.
#[dialog_common::test]
async fn it_counts_what_a_card_collects() -> Result<()> {
    let counted = SUMMARY;
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
                instance: li("todo:1", "Buy milk", "open"),
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
        r#"[:article [:h2 "Home"] [:p "1" " open"] [:ul [:li {:class "open"} "Buy milk"]]]"#
    );
    Ok(())
}

/// The card's count kept by the driver, `Live::watch`, on a fold of
/// Dialog-DB's, beside the layers driven by this file, whose count the
/// maintainer counts from the rows of its layer: through a member added,
/// a member removed, and the last member removed, the driver gives the
/// patches driven here give. The last removed leaves the card, counting
/// 0. The driver's maintainer is given no row of the counted layer, and
/// a list with nothing open counts 0 from its first render.
#[dialog_common::test]
async fn it_keeps_a_views_count_on_a_fold() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, CONCEPTS).await?;
    write(
        &mut notebook,
        r#"(assert (list list:home name: "Home"))
           (assert (list list:work name: "Work"))
           (assert (todo todo:1 list: list:home title: "Buy milk" state: :open))"#,
    )
    .await?;
    let plan = plan_of(&[CONCEPTS, SUMMARY], "(select (list/summary ?l is: ?card))")?;
    let counted = plan.elements[plan.counts[0].place].layer;
    let mut here = Driven::new(&notebook, plan.clone(), None, How::Subscribed).await?;
    let live = dialog_notation::live::Live::new(notebook.branch.clone());
    let mut watched = live.watch(plan, None)?;
    assert_eq!(watched.folded(), [counted], "the count is on a fold");
    assert_eq!(
        watched.poll(&notebook.operator).await?,
        here.step(&notebook).await?
    );
    assert_eq!(
        watched.tree().hiccup(),
        r#"[:article [:h2 "Home"] [:p "1" " open"] [:ul [:li {:class "open"} "Buy milk"]]] [:article [:h2 "Work"] [:p "0" " open"] [:ul]]"#
    );
    let mut count = card();
    count.push((1, Vec::new()));
    let text = |text: &str| Patch::SetText {
        at: count.clone(),
        slot: 0,
        text: text.into(),
    };
    let steps: Vec<(&str, Vec<Patch>)> = vec![
        (
            r#"(assert (todo todo:2 list: list:home title: "Call mum" state: :open))"#,
            vec![
                text("2"),
                Patch::Insert {
                    parent: items_of_summary(),
                    slot: 0,
                    index: 1,
                    instance: li("todo:2", "Call mum", "open"),
                },
            ],
        ),
        (
            r#"(retract (todo todo:2 list: list:home title: "Call mum" state: :open))"#,
            vec![
                text("1"),
                Patch::Remove {
                    at: item_of_summary("todo:2"),
                },
            ],
        ),
        (
            r#"(retract (todo todo:1 list: list:home title: "Buy milk" state: :open))"#,
            vec![
                text("0"),
                Patch::Remove {
                    at: item_of_summary("todo:1"),
                },
            ],
        ),
    ];
    for (change, expected) in steps {
        write(&mut notebook, change).await?;
        let mut driven = watched.poll(&notebook.operator).await?;
        assert_eq!(driven, here.step(&notebook).await?, "after {change}");
        driven.sort_by_key(|patch| format!("{patch:?}"));
        let mut expected = expected;
        expected.sort_by_key(|patch| format!("{patch:?}"));
        assert_eq!(driven, expected, "after {change}");
        assert_eq!(
            watched.maintainer().rows(counted),
            0,
            "the maintainer holds no row of the counted layer"
        );
    }
    assert_eq!(
        watched.tree().hiccup(),
        r#"[:article [:h2 "Home"] [:p "0" " open"] [:ul]] [:article [:h2 "Work"] [:p "0" " open"] [:ul]]"#,
        "the last removed leaves the card, counting 0"
    );
    assert_eq!(watched.tree(), here.tree);
    Ok(())
}

/// Where the summary card's items stand.
fn items_of_summary() -> dialog_notation::Address {
    let mut at = card();
    at.push((2, Vec::new()));
    at
}

/// Where an item of the summary card stands.
fn item_of_summary(t: &str) -> dialog_notation::Address {
    let mut at = items_of_summary();
    at.push((0, vec![entity(t)]));
    at
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

const CORE: &str = include_str!("fixtures/core.clj");
const NOTEBOOK: &str = include_str!("fixtures/notebook.clj");
const NOTEBOOK_VIEWS: &str = include_str!("fixtures/notebook-views.clj");
const TABLE: &str = include_str!("fixtures/table.clj");
const TABLE_VIEWS: &str = include_str!("fixtures/table-views.clj");

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
            "notebook/ui: notebook-save by [n], span by [[q1]k], div by [[q2]b]",
            "notebook/directory: div by [], a by [[q1]n]",
            "notebook/index-page: tonk-notebook-index by [site], a by [[q2]n]",
            "notebook/page: div by [site], notebook-save by [], span by [[q2]k], div by [[q3]b]",
            "table/ui: tonk-table by [t], div by [[q1]s], div by [[q2]c], div by [[q3]k], div by \
             [[q4]r]",
            "table/directory: div by [], tonk-table by [[q1]t], div by [[q2]s], div by [[q3]c], div \
             by [[q4]k], div by [[q5]r]",
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

/// An attribute written as a list is its parts joined, as text children
/// are: fixed text and a value of the row, one after the other.
#[dialog_common::test]
async fn it_joins_the_parts_of_an_attribute_written_as_a_list() -> Result<()> {
    let link = r##"
        (query (list/link ?l is: ?link)
          (list ?l name: ?name)
          (view ?link [:a {:href ["/lists/" ?name "#top"] :class "list"} ?name]))"##;
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, CONCEPTS).await?;
    write(&mut notebook, r#"(assert (list list:home name: "home"))"#).await?;
    let plan = plan_of(&[CONCEPTS, link], "(select (list/link ?l is: ?link))")?;
    let mut shown = Driven::new(&notebook, plan, entity("list:home"), How::Subscribed).await?;
    shown.step(&notebook).await?;
    assert_eq!(
        shown.tree.hiccup(),
        r##"[:a {:href "/lists/home#top" :class "list"} "home"]"##
    );
    Ok(())
}

/// The source of a query not found names each field of the concept
/// apart from an entry's key, whatever the fields are labelled: a field
/// labelled `key` beside an entry is shown with its own value.
#[dialog_common::test]
async fn it_shows_a_field_labelled_key_beside_an_entry_apart_from_its_key() -> Result<()> {
    let concepts = r#"
        (concept (room ?r name: ?n)
          (xyz.room/name of: ?r is: ?n as: :text cardinality: :one))
        (concept (shelf ?s room: ?r key: ?k item: {?i ?b})
          (xyz.shelf/room of: ?s is: ?r as: :entity cardinality: :one)
          (xyz.shelf/key of: ?s is: ?k as: :text cardinality: :one)
          (xyz.shelf.item/?i of: ?s is: ?b as: :entity cardinality: :many by: :position))"#;
    let board = r#"
        (query (room/board ?r is: ?board)
          (room ?r name: ?name)
          (shelf/missing _ room: ?r is: (distinct ?shelves))
          (view ?board [:article [:h2 ?name] ?shelves]))"#;
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, concepts).await?;
    write(
        &mut notebook,
        r#"(assert (room room:1 name: "Den"))
           (assert (shelf shelf:1 room: room:1 key: "K1" item: {"A" book:a}))"#,
    )
    .await?;
    let plan = plan_of(&[concepts, board], "(select (room/board ?r is: ?board))")?;
    let mut shown = Driven::new(&notebook, plan, entity("room:1"), How::Subscribed).await?;
    shown.step(&notebook).await?;
    assert_eq!(
        shown.tree.hiccup(),
        r#"[:article [:h2 "Den"] [:view-source "(shelf shelf:1 item: {\"A\" book:a} key: \"K1\" room: room:1)"]]"#
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
          (todo/missing _ list: ?l is: (distinct ?items))
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
        [r#"[:article [:h2 "Home"] [:ul [:li {:class "open"} "Buy milk"]]]"#]
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
          (xyz.todo.toggle/todo of: ?c is: ?t as: :entity in: :control cardinality: :many))"#;
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
           (todo (distinct ?all) list: ?l ...)
           (view ?x [:ul ?all]))
         (select (list/x ?l is: ?x))",
    );
    assert!(
        matches!(&whole, Error::Unsupported(why) if why.contains("the whole of a read")),
        "{whole:?}"
    );
    Ok(())
}

/// How far tonk's notebook and table, written in the notation, get,
/// form by form: how many forms are read, how many are refused, and how
/// many compile to a step. Alone, the notebook's three uses of what core
/// declares (two routes and an intent) are what nothing gives a meaning;
/// with core in front, as tonk reads a component, nothing is refused. A
/// change that moves these numbers moves them here on purpose.
#[test]
fn it_measures_how_far_the_notebook_and_the_table_get() -> Result<()> {
    let measure = |documents: &[&str]| -> Result<(usize, usize, usize)> {
        let text = documents.join("\n");
        let checked = dialog_notation::check_forms(&mut Analyzer::new()?, &text);
        for refused in checked.refusals() {
            session::spoken(&text, &refused.message);
        }
        let forms = parse(&text)?.len();
        let compiled: std::collections::BTreeSet<usize> =
            checked.step_forms().iter().copied().collect();
        Ok((forms, checked.refusals().count(), compiled.len()))
    };
    let core = include_str!("fixtures/core.clj");
    let notebook = include_str!("fixtures/notebook.clj");
    let notebook_views = include_str!("fixtures/notebook-views.clj");
    let table = include_str!("fixtures/table.clj");
    let table_views = include_str!("fixtures/table-views.clj");
    assert_eq!(measure(&[notebook, notebook_views])?, (58, 3, 33));
    assert_eq!(measure(&[core, notebook, notebook_views])?, (62, 0, 36));
    assert_eq!(measure(&[core, table, table_views])?, (54, 0, 25));
    Ok(())
}
