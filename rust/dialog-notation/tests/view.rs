//! A view maintained on a real branch: each layer of its plan a query of
//! its own, the rows that come and go fed to a maintainer, and the
//! patches it gives checked against the tree rendered afresh.
//!
//! A layer is driven one of two ways. Subscribed: `Branch::subscribe`
//! over the layer's query, polled after each commit. Asked: the query
//! asked again after each commit, and the rows that came and went taken
//! as the difference of the two answers. The maintainer sees only rows
//! that came and went either way. A layer whose query is more than one
//! read has a rule of its own; `Branch::subscribe` takes no rule beside
//! the query, so the rule is asserted into the branch's session overlay,
//! where a subscription finds it and from which nothing is committed.

#![allow(deprecated)]

use anyhow::Result;
use dialog_corpus::Value;
use dialog_notation::view::tree::Slot;
use dialog_notation::{
    Analyzer, Change, Content, Maintainer, Patch, Plan, Query, Rendered, Row, Shown, View, parse,
    render,
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

/// The card of a list: its name, and an item for each of its to-dos.
const CARD: &str = r#"
    (view list :card
      [:article
        [:h2 ?name]
        [:ul [:li {:class ?s} ?title]]]
      (list ?l name: ?name)
      (todo ?t list: ?l title: ?title state: ?s))"#;

/// The view `name` of the views written after the concepts.
fn described(views: &str, name: &str, concept: &str) -> Result<View> {
    let analysis = Analyzer::new()?.analyze(&parse(&format!("{CONCEPTS}\n{views}"))?);
    let refused: Vec<String> = analysis
        .diagnostics
        .iter()
        .filter_map(|diagnostic| diagnostic.refusal.as_ref().map(ToString::to_string))
        .collect();
    assert!(refused.is_empty(), "{refused:?}");
    analysis
        .analyzed
        .iter()
        .filter(|tree| tree.applies() == Some("view"))
        .map(View::describe)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .find(|view| view.name == name && view.concept == concept)
        .ok_or_else(|| anyhow::anyhow!("no view {concept} :{name}"))
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

/// A view's layers, each fed from the branch, and its maintainer.
struct Driven {
    plan: Plan,
    entity: Value,
    queries: Vec<Query>,
    feeds: Vec<Feed>,
    maintainer: Maintainer,
    tree: Rendered,
}

/// Every row of a layer's query, as the branch stands.
async fn ask(notebook: &Notebook, plan: &Plan, query: &Query, entity: &Value) -> Result<Vec<Row>> {
    let mut layer = notebook.branch.query();
    if let Some(rule) = &query.rule {
        layer = layer.with(rule.clone());
    }
    let found: Vec<ConceptConclusion> = layer
        .select(query.select.clone())
        .perform(&notebook.operator)
        .try_vec()
        .await?;
    let mut rows = found
        .iter()
        .map(|conclusion| plan.row(query, conclusion, Some(entity)))
        .collect::<Result<Vec<_>, _>>()?;
    rows.sort_by_key(|row| format!("{row:?}"));
    Ok(rows)
}

impl Driven {
    async fn new(notebook: &Notebook, plan: Plan, entity: Value, how: How) -> Result<Driven> {
        let mut queries = Vec::new();
        let mut feeds = Vec::new();
        for layer in 0..plan.layers.len() {
            let query = plan.query(layer, Some(&entity))?;
            feeds.push(match how {
                How::Subscribed => {
                    if let Some(rule) = &query.rule {
                        notebook.branch.overlay().assert(rule.clone())?;
                    }
                    Feed::Subscribed(Box::new(notebook.branch.subscribe(query.select.clone())))
                }
                How::Asked => Feed::Asked(Vec::new()),
            });
            queries.push(query);
        }
        Ok(Driven {
            maintainer: Maintainer::new(plan.clone(), Some(entity.clone())),
            plan,
            entity,
            queries,
            feeds,
            tree: Rendered::default(),
        })
    }

    /// For each subscribed layer, how many times it was evaluated whole
    /// and how many polls were maintained from the change alone.
    fn evaluations(&self) -> Vec<(usize, usize)> {
        self.feeds
            .iter()
            .filter_map(|feed| match feed {
                Feed::Subscribed(subscription) => {
                    Some((subscription.recomputes(), subscription.maintenances()))
                }
                Feed::Asked(_) => None,
            })
            .collect()
    }

    /// The rows that came and went since the last step, fed to the
    /// maintainer as one change; the patches it gives, checked.
    async fn step(&mut self, notebook: &Notebook) -> Result<Vec<Patch>> {
        let mut changes = Vec::new();
        for (layer, feed) in self.feeds.iter_mut().enumerate() {
            let query = &self.queries[layer];
            let (arrived, left) = match feed {
                Feed::Subscribed(subscription) => {
                    match subscription.poll(&notebook.operator).await? {
                        Some(delta) => (
                            delta
                                .asserted
                                .iter()
                                .map(|row| self.plan.row(query, row, Some(&self.entity)))
                                .collect::<Result<Vec<_>, _>>()?,
                            delta
                                .retracted
                                .iter()
                                .map(|row| self.plan.row(query, row, Some(&self.entity)))
                                .collect::<Result<Vec<_>, _>>()?,
                        ),
                        None => (Vec::new(), Vec::new()),
                    }
                }
                Feed::Asked(last) => {
                    let now = ask(notebook, &self.plan, query, &self.entity).await?;
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
        let patches = self.maintainer.apply(&changes);
        for patch in &patches {
            self.tree.apply(patch).map_err(anyhow::Error::msg)?;
        }
        let maintained = self.maintainer.tree();
        assert_eq!(self.tree, maintained, "the patches make the tree");
        let mut every = Vec::new();
        for query in &self.queries {
            every.push(ask(notebook, &self.plan, query, &self.entity).await?);
        }
        assert_eq!(
            maintained,
            render(&self.plan, Some(&self.entity), &every),
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

fn li(t: &str, title: &str, state: &str) -> Shown {
    Shown {
        key: vec![entity(t)],
        content: Content::Element {
            tag: "li".into(),
            attributes: vec![("class".into(), state.into())],
            events: Vec::new(),
            slots: vec![Slot::Text(title.into())],
        },
    }
}

/// The to-do card through every change, driven `how`: each change a
/// patch of the one instance it is behind.
async fn the_card_through_its_changes(how: How) -> Result<Vec<Vec<Patch>>> {
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, CONCEPTS).await?;
    write(&mut notebook, r#"(assert (list list:home name: "Home"))"#).await?;
    let plan = Plan::of(&described(CARD, "card", "list")?)?;
    let mut card_of = Driven::new(&notebook, plan, entity("list:home"), how).await?;
    let mut log = Vec::new();

    let first = card_of.step(&notebook).await?;
    let element = |tag: &str, slots| Content::Element {
        tag: tag.into(),
        attributes: Vec::new(),
        events: Vec::new(),
        slots,
    };
    let shown = |key, content| Shown { key, content };
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
                    vec![
                        Slot::Instances(vec![shown(
                            Vec::new(),
                            element("h2", vec![Slot::Text("Home".into())])
                        )]),
                        Slot::Instances(vec![shown(
                            Vec::new(),
                            element("ul", vec![Slot::Instances(Vec::new())])
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
    let Content::Element { tag, slots, .. } = &root.content else {
        panic!("an article");
    };
    assert_eq!(tag, "article");
    assert_eq!(slots.len(), 2, "its heading and its list");

    // A write that touches no layer gives no patch.
    write(&mut notebook, r#"(assert (list list:work name: "Work"))"#).await?;
    assert_eq!(card_of.step(&notebook).await?, []);
    if how == How::Subscribed {
        // Each layer evaluated whole once, at its first poll; every
        // change after was maintained from the change: the heading at
        // the rename, the items at each of their six writes, and the
        // root once, a write in the range it read (the list's name, which
        // it reads as a blank) that changed no row of it.
        assert_eq!(card_of.evaluations(), [(1, 1), (1, 1), (1, 6)]);
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

/// The reading in which a view is one query over all its conditions
/// renders nothing for a list with no to-dos; the layered card renders
/// the card, its list empty.
#[dialog_common::test]
async fn it_renders_no_card_for_an_empty_list_from_one_query() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, CONCEPTS).await?;
    write(&mut notebook, r#"(assert (list list:home name: "Home"))"#).await?;
    let view = described(CARD, "card", "list")?;

    let mut one = Driven::new(
        &notebook,
        Plan::single(&view)?,
        entity("list:home"),
        How::Subscribed,
    )
    .await?;
    assert_eq!(one.step(&notebook).await?, []);
    assert_eq!(one.tree, Rendered::default());

    let mut layered = Driven::new(
        &notebook,
        Plan::of(&view)?,
        entity("list:home"),
        How::Subscribed,
    )
    .await?;
    layered.step(&notebook).await?;
    assert_eq!(layered.tree.roots.len(), 1);

    // With a to-do, the one query has its row and renders the card.
    write(
        &mut notebook,
        r#"(assert (todo todo:1 list: list:home title: "Buy milk" state: :open))"#,
    )
    .await?;
    let patches = one.step(&notebook).await?;
    assert!(
        matches!(patches.as_slice(), [Patch::Insert { parent, .. }] if parent.is_empty()),
        "{patches:?}"
    );
    Ok(())
}
