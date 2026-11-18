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

/// A view's layers, each fed from the branch, and its maintainer. A
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
    query: &Query,
    entity: Option<&Value>,
) -> Result<Vec<Row>> {
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
        .map(|conclusion| plan.row(query, conclusion, entity))
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

    /// For each subscribed layer, how many times it was evaluated whole
    /// and how many polls were maintained from the change alone.
    fn evaluations(&self) -> Vec<(usize, usize)> {
        self.feeds
            .iter()
            .flatten()
            .filter_map(|feed| match feed {
                Feed::Subscribed(subscription) => {
                    Some((subscription.recomputes(), subscription.maintenances()))
                }
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
                                .map(|row| self.plan.row(query, row, entity))
                                .collect::<Result<Vec<_>, _>>()?,
                            delta
                                .retracted
                                .iter()
                                .map(|row| self.plan.row(query, row, entity))
                                .collect::<Result<Vec<_>, _>>()?,
                        ),
                        None => (Vec::new(), Vec::new()),
                    }
                }
                Feed::Asked(last) => {
                    let now = ask(notebook, &self.plan, query, entity).await?;
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
        for query in &self.queries {
            every.push(match query {
                Some(query) => ask(notebook, &self.plan, query, self.entity.as_ref()).await?,
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

/// The card with each item a nested view, the to-do's own row.
const NESTED: &str = r#"
    (view todo :row [:li {:class ?s} ?title]
      (todo ?t title: ?title state: ?s))
    (view list :card
      [:article
        [:h2 ?name]
        [:ul {:view :row :of ?t}]]
      (list ?l name: ?name)
      (todo ?t list: ?l))"#;

/// A view and the views nested in it, each mounted where its reference's
/// instance is and maintained as a view of its own, with its own
/// subscriptions.
struct Host {
    outer: Driven,
    inner: Plan,
    mounted: Vec<(dialog_notation::Address, Driven)>,
}

/// Every nested reference in an instance, by its address.
fn mounts(
    at: dialog_notation::Address,
    shown: &Shown,
    into: &mut Vec<(dialog_notation::Address, dialog_notation::Mount)>,
) {
    match &shown.content {
        Content::View(mount) => into.push((at, mount.clone())),
        Content::Element { slots, .. } => {
            for (slot, inside) in slots.iter().enumerate() {
                if let Slot::Instances(list) = inside {
                    for child in list {
                        let mut below = at.clone();
                        below.push((slot, child.key.clone()));
                        mounts(below, child, into);
                    }
                }
            }
        }
    }
}

impl Host {
    /// The patches of a step: the outer view's with no mount, then each
    /// nested view's with the address it is mounted at.
    async fn step(
        &mut self,
        notebook: &Notebook,
    ) -> Result<Vec<(dialog_notation::Address, Patch)>> {
        let mut said = Vec::new();
        let outer = self.outer.step(notebook).await?;
        let mut arrived = Vec::new();
        for patch in &outer {
            match patch {
                Patch::Insert {
                    parent,
                    slot,
                    instance,
                    ..
                } => {
                    let mut at = parent.clone();
                    at.push((*slot, instance.key.clone()));
                    if parent.is_empty() {
                        at = vec![(0, instance.key.clone())];
                    }
                    mounts(at, instance, &mut arrived);
                }
                Patch::Remove { at } => {
                    self.mounted.retain(|(mounted, _)| !mounted.starts_with(at))
                }
                _ => {}
            }
            said.push((Vec::new(), patch.clone()));
        }
        for (at, driven) in &mut self.mounted {
            for patch in driven.step(notebook).await? {
                said.push((at.clone(), patch));
            }
        }
        for (at, mount) in arrived {
            let entity = mount.of.clone().expect("a row of a to-do");
            let mut driven =
                Driven::new(notebook, self.inner.clone(), entity, How::Subscribed).await?;
            for patch in driven.step(notebook).await? {
                said.push((at.clone(), patch));
            }
            self.mounted.push((at, driven));
        }
        Ok(said)
    }
}

/// The card's items are references to each to-do's row, and each row is
/// a view of its own, mounted where its reference is, with its own
/// subscriptions: adding a to-do inserts the reference into the card and
/// the row into the mount; retitling it changes one text of the row and
/// nothing of the card; removing it removes the reference, and the row
/// with it.
#[dialog_common::test]
async fn it_maintains_a_nested_view_as_a_view_of_its_own() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, CONCEPTS).await?;
    write(&mut notebook, r#"(assert (list list:home name: "Home"))"#).await?;
    let card = Plan::of(&described(NESTED, "card", "list")?)?;
    let row = Plan::of(&described(NESTED, "row", "todo")?)?;
    assert_eq!(card.elements[3].key, ["t"], "a reference a to-do");
    let mut host = Host {
        outer: Driven::new(&notebook, card, entity("list:home"), How::Subscribed).await?,
        inner: row,
        mounted: Vec::new(),
    };
    let first = host.step(&notebook).await?;
    assert!(
        matches!(first.as_slice(), [(mount, Patch::Insert { .. })] if mount.is_empty()),
        "{first:?}"
    );

    write(
        &mut notebook,
        r#"(assert (todo todo:1 list: list:home title: "Buy milk" state: :open))"#,
    )
    .await?;
    let reference = || {
        let mut at = items();
        at.push((0, vec![entity("todo:1")]));
        at
    };
    let root = || vec![(0, vec![entity("todo:1")])];
    assert_eq!(
        host.step(&notebook).await?,
        [
            (
                Vec::new(),
                Patch::Insert {
                    parent: items(),
                    slot: 0,
                    index: 0,
                    instance: Shown {
                        key: vec![entity("todo:1")],
                        content: Content::View(dialog_notation::Mount {
                            view: "row".into(),
                            model: None,
                            of: Some(entity("todo:1")),
                            entries: Vec::new(),
                            given: Vec::new(),
                        }),
                    },
                }
            ),
            (
                reference(),
                Patch::Insert {
                    parent: Vec::new(),
                    slot: 0,
                    index: 0,
                    instance: li("todo:1", "Buy milk", "open:"),
                }
            ),
        ]
    );
    assert_eq!(host.mounted.len(), 1);

    write(
        &mut notebook,
        r#"(assert (todo todo:1 list: list:home title: "Buy oat milk" state: :open))"#,
    )
    .await?;
    assert_eq!(
        host.step(&notebook).await?,
        [(
            reference(),
            Patch::SetText {
                at: root(),
                slot: 0,
                text: "Buy oat milk".into(),
            }
        )],
        "the row changes and the card does not"
    );

    write(
        &mut notebook,
        r#"(retract (todo todo:1 list: list:home title: "Buy oat milk" state: :open))"#,
    )
    .await?;
    let removed = host.step(&notebook).await?;
    assert_eq!(
        removed,
        [(Vec::new(), Patch::Remove { at: reference() })],
        "the reference leaves and its row with it"
    );
    assert!(host.mounted.is_empty());
    Ok(())
}

/// Every view of documents read in order as one, described.
fn views_of(texts: &[&str]) -> Result<Vec<View>> {
    let parsed = dialog_notation::parse_document(&texts.join("\n\n"));
    let mut expander = dialog_notation::Expander::new()?;
    let (kernel, _) = expander.check(parsed.trees);
    Ok(kernel
        .iter()
        .filter(|tree| tree.applies() == Some("view"))
        .filter_map(|tree| View::describe(tree).ok())
        .collect())
}

/// A plan, said in a line a view: each layer, the element that takes
/// it, why each condition is there, its row and whether its query
/// compiles; each element told apart by a key; and what surprised the
/// rule.
fn summary(plan: &Plan) -> String {
    let tag = |at: usize| match &plan.elements[at].placed {
        dialog_notation::Placed::Element(tag) => tag.clone(),
        dialog_notation::Placed::View(nested) => format!("{{:view :{}}}", nested.view),
        dialog_notation::Placed::Call(call) => format!("(view {} :{})", call.concept, call.view),
    };
    let layers: Vec<String> = plan
        .layers
        .iter()
        .enumerate()
        .map(|(index, layer)| {
            let whys: Vec<String> = layer
                .conditions
                .iter()
                .map(|condition| format!("{:?}", condition.why))
                .collect();
            let compiles = match plan.query(index, None) {
                _ if layer.conditions.is_empty() => "stands".to_string(),
                Ok(_) => "compiles".to_string(),
                Err(error) => format!("refused: {error}"),
            };
            format!(
                "{} [{}] row {} {compiles}",
                tag(layer.element),
                whys.join(" "),
                layer.row.join(" ")
            )
        })
        .collect();
    let keys: Vec<String> = plan
        .elements
        .iter()
        .enumerate()
        .filter(|(_, place)| !place.key.is_empty())
        .map(|(at, place)| format!("{} by {}", tag(at), place.key.join(" ")))
        .collect();
    let cases: Vec<String> = plan
        .cases
        .iter()
        .map(|case| match case {
            dialog_notation::Case::Shared {
                condition,
                at,
                below,
            } => format!(
                "shared {} at {} below {}",
                condition.applies().unwrap_or_default(),
                tag(*at),
                below
                    .iter()
                    .map(|at| tag(*at))
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
            dialog_notation::Case::Unneeded { condition } => {
                format!("unneeded {}", condition.applies().unwrap_or_default())
            }
            dialog_notation::Case::Unbound {
                element,
                variable,
                event,
            } => format!("unbound ?{variable} at {} event {event}", tag(*element)),
            dialog_notation::Case::Unjoined { layer, variable } => {
                format!("unjoined ?{variable} in layer {layer}")
            }
            dialog_notation::Case::Other { variable } => format!("other ?{variable}"),
            dialog_notation::Case::Flat { element, variable } => {
                format!("flat ?{variable} at {}", tag(*element))
            }
        })
        .collect();
    format!(
        "{} :{} | {} | {} | {}",
        plan.concept,
        plan.name,
        layers.join(" ; "),
        keys.join(", "),
        cases.join(", ")
    )
}

/// The layering of every view of tonk's notebook and table, read after
/// core as tonk reads a component: every condition lands on an element
/// and every layer's query compiles. What the rule says of them, pinned:
/// the notebook's `:ui` gives the whole of its one condition to the
/// element that shows the title, its rows one a block; a `:directory`
/// view's root is one an instance, where tonk renders one index holding
/// a row an instance; the index route's site is shared at the root by
/// the two children that show its branch; and the table's name, which no
/// element shows, is the root's.
#[dialog_common::test]
async fn it_lays_out_every_view_of_the_sketches() -> Result<()> {
    let notebook: Vec<String> = views_of(&[
        include_str!("../sketches/core.clj"),
        include_str!("../sketches/notebook.clj"),
    ])?
    .iter()
    .map(|view| Ok(summary(&Plan::of(view)?)))
    .collect::<Result<_>>()?;
    assert_eq!(
        notebook,
        [
            "notebook :ui | notebook-save [Instance] row n compiles ; tonk-notebook \
             [Binds(\"title\")] row n title k b compiles | notebook-save by n, span by k b | ",
            "notebook :directory | div [Instance] row n compiles ; span [Binds(\"title\")] row n \
             title compiles | div by n | ",
            "notebook/block :directory | div [Instance] row b compiles ; div [Binds(\"n\")] row b \
             n s compiles | div by b | ",
            "notebook/index-route :ui | tonk-notebook-index [Instance Shared(\"branch\")] row site \
             branch repo compiles | tonk-notebook-index by site | shared xyz.tonk.site at \
             tonk-notebook-index below notebook-save {:view :directory}",
            "notebook/route :ui | div [Instance] row site compiles ; {:view :ui} \
             [Binds(\"branch\")] row site entity branch repo compiles | div by site | ",
        ]
    );
    let table: Vec<String> = views_of(&[
        include_str!("../sketches/core.clj"),
        include_str!("../sketches/table.clj"),
    ])?
    .iter()
    .map(|view| Ok(summary(&Plan::of(view)?)))
    .collect::<Result<_>>()?;
    assert_eq!(
        table,
        [
            "table/sheet :directory | div [Instance] row s compiles ; div [Binds(\"name\")] row s \
             table name order compiles | div by s | ",
            "table/cell :directory | div [Instance] row c compiles ; div [Binds(\"at\")] row c \
             sheet at content compiles | div by c | ",
            "table/column :directory | div [Instance] row k compiles ; div [Binds(\"at\")] row k \
             sheet at width compiles | div by k | ",
            "table/row :directory | div [Instance] row r compiles ; div [Binds(\"at\")] row r \
             sheet at height compiles | div by r | ",
            "table :ui | tonk-table [Instance Unneeded] row t compiles | tonk-table by t | \
             unneeded xyz.tonk.table",
            "table :directory | div [Instance Unneeded] row t compiles | div by t | unneeded \
             xyz.tonk.table",
        ]
    );
    Ok(())
}

/// A view and every view it calls, each mounted where its call stands:
/// one maintainer a call, with its own subscriptions, rendering every
/// match of the called view narrowed by the call, or for the call's
/// entity. A view that no document holds is shown as its source.
struct Caller {
    views: Vec<View>,
    mounted: Vec<(Vec<dialog_notation::Address>, Driven)>,
}

impl Caller {
    async fn open(
        notebook: &Notebook,
        views: Vec<View>,
        top: (&str, &str),
        entity: Option<Value>,
    ) -> Result<Caller> {
        let view = views
            .iter()
            .find(|view| (view.concept.as_str(), view.name.as_str()) == top)
            .ok_or_else(|| anyhow::anyhow!("no view {top:?}"))?;
        let driven = Driven::new(notebook, Plan::repeating(view)?, entity, How::Subscribed).await?;
        Ok(Caller {
            views,
            mounted: vec![(Vec::new(), driven)],
        })
    }

    /// The patches of a step, each with the calls it is mounted under.
    async fn step(
        &mut self,
        notebook: &Notebook,
    ) -> Result<Vec<(Vec<dialog_notation::Address>, Patch)>> {
        let mut said = Vec::new();
        let mut at = 0;
        while at < self.mounted.len() {
            let path = self.mounted[at].0.clone();
            let patches = self.mounted[at].1.step(notebook).await?;
            let mut arrived = Vec::new();
            let mut gone: Vec<dialog_notation::Address> = Vec::new();
            for patch in &patches {
                match patch {
                    Patch::Insert {
                        parent,
                        slot,
                        instance,
                        ..
                    } => {
                        let mut address = parent.clone();
                        address.push((*slot, instance.key.clone()));
                        mounts(address, instance, &mut arrived);
                    }
                    Patch::Remove { at } => gone.push(at.clone()),
                    Patch::SetView { at, mount } => {
                        gone.push(at.clone());
                        arrived.push((at.clone(), mount.clone()));
                    }
                    _ => {}
                }
                said.push((path.clone(), patch.clone()));
            }
            // What went takes every call mounted under it.
            let depth = path.len();
            self.mounted.retain(|(mounted, _)| {
                !(mounted.len() > depth
                    && mounted[..depth] == path[..]
                    && gone.iter().any(|at| mounted[depth].starts_with(at)))
            });
            let plan = self.mounted[at].1.plan.clone();
            for (address, mount) in arrived {
                let slots: Vec<usize> = address.iter().map(|(slot, _)| *slot).collect();
                let Some(dialog_notation::Placed::Call(call)) = plan
                    .place_at(&slots)
                    .map(|place| plan.elements[place].placed.clone())
                else {
                    continue;
                };
                let called = match self
                    .views
                    .iter()
                    .find(|view| view.concept == call.concept && view.name == call.view)
                {
                    Some(view) => Plan::called(view, &call, &mount.given)?,
                    None => Plan::source(&call, &mount.given)?,
                };
                let driven =
                    Driven::new(notebook, called, mount.of.clone(), How::Subscribed).await?;
                let mut below = path.clone();
                below.push(address);
                self.mounted.push((below, driven));
            }
            at += 1;
        }
        Ok(said)
    }

    /// The whole tree, each call replaced by what it mounts, written as
    /// Hiccup.
    fn written(&self) -> String {
        let top = &self.mounted[0].1.tree;
        top.roots
            .iter()
            .map(|root| self.hiccup(&[], vec![(0, root.key.clone())], root))
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn hiccup(
        &self,
        path: &[dialog_notation::Address],
        at: dialog_notation::Address,
        shown: &Shown,
    ) -> String {
        match &shown.content {
            Content::View(_) => {
                let mut below = path.to_vec();
                below.push(at);
                match self.mounted.iter().find(|(mounted, _)| *mounted == below) {
                    Some((_, driven)) => driven
                        .tree
                        .roots
                        .iter()
                        .map(|root| self.hiccup(&below, vec![(0, root.key.clone())], root))
                        .collect::<Vec<_>>()
                        .join(" "),
                    None => String::new(),
                }
            }
            Content::Element {
                tag,
                attributes,
                slots,
                ..
            } => {
                let mut out = format!("[:{tag}");
                if !attributes.is_empty() {
                    let written: Vec<String> = attributes
                        .iter()
                        .map(|(name, value)| format!(":{name} {value:?}"))
                        .collect();
                    out.push_str(&format!(" {{{}}}", written.join(" ")));
                }
                for (index, inside) in slots.iter().enumerate() {
                    match inside {
                        Slot::Text(text) => out.push_str(&format!(" {text:?}")),
                        Slot::Instances(list) => {
                            for child in list {
                                let mut below = at.clone();
                                below.push((index, child.key.clone()));
                                let written = self.hiccup(path, below, child);
                                if !written.is_empty() {
                                    out.push(' ');
                                    out.push_str(&written);
                                }
                            }
                        }
                    }
                }
                out.push(']');
                out
            }
        }
    }
}

/// The views of a document written after the concepts, described.
fn views_in(concepts: &str, views: &str) -> Result<Vec<View>> {
    let analysis = Analyzer::new()?.analyze(&parse(&format!("{concepts}\n{views}"))?);
    let refused: Vec<String> = analysis
        .diagnostics
        .iter()
        .filter_map(|diagnostic| diagnostic.refusal.as_ref().map(ToString::to_string))
        .collect();
    assert!(refused.is_empty(), "{refused:?}");
    Ok(analysis
        .analyzed
        .iter()
        .filter(|tree| tree.applies() == Some("view"))
        .map(View::describe)
        .collect::<Result<Vec<_>, _>>()?)
}

/// A task with one title and tags of many values.
const TASKS: &str = r#"
    (concept (task ?t title: ?title tag: ?tag)
      (xyz.task/title of: ?t is: ?title as: :text cardinality: :one)
      (xyz.task/tag of: ?t is: ?tag as: :text cardinality: :many))"#;

const LISTS: &str = r#"
    (view list :directory
      [:ul {:class "lists"} [:li {:subject ?l} ?name]]
      (list ?l name: ?name))
    (view list :unmarked
      [:ul {:class "lists"} [:li ?name]]
      (list ?l name: ?name))"#;

const ROWS: &str = r#"
    (view task :row
      [:li ?title [:span {:class "tag"} ?tag]]
      (task ?t title: ?title tag: ?tag))
    (view task :moved
      [:li [:p ?title] [:span {:class "tag"} ?tag]]
      (task ?t title: ?title tag: ?tag))"#;

fn element(tag: &str, attributes: &[(&str, &str)], slots: Vec<Slot>) -> Content {
    Content::Element {
        tag: tag.into(),
        attributes: attributes
            .iter()
            .map(|(name, value)| ((*name).into(), (*value).into()))
            .collect(),
        events: Vec::new(),
        slots,
    }
}

fn shown(key: Vec<Value>, content: Content) -> Shown {
    Shown { key, content }
}

/// By tonk's rule, the element that holds the entity's variable alone in
/// an attribute, `{:subject ?l}`, repeats once a list, and what is
/// outside it stands once: with no list, the `ul`, empty. Without the
/// marker the whole template repeats, a `ul` a list, and with no list
/// nothing is there.
#[dialog_common::test]
async fn it_repeats_the_marked_element_and_keeps_what_is_outside() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, CONCEPTS).await?;
    let views = views_in(CONCEPTS, LISTS)?;
    let mut marked = Caller::open(&notebook, views.clone(), ("list", "directory"), None).await?;
    let mut unmarked = Caller::open(&notebook, views, ("list", "unmarked"), None).await?;

    assert_eq!(
        marked.step(&notebook).await?,
        [(
            Vec::new(),
            Patch::Insert {
                parent: Vec::new(),
                slot: 0,
                index: 0,
                instance: shown(
                    Vec::new(),
                    element(
                        "ul",
                        &[("class", "lists")],
                        vec![Slot::Instances(Vec::new())]
                    )
                ),
            }
        )]
    );
    assert_eq!(marked.written(), r#"[:ul {:class "lists"}]"#);
    assert_eq!(unmarked.step(&notebook).await?, []);
    assert_eq!(unmarked.written(), "");

    write(&mut notebook, r#"(assert (list list:home name: "Home"))"#).await?;
    let li = |uri: &str, name: &str| {
        shown(
            vec![entity(uri)],
            element("li", &[("subject", uri)], vec![Slot::Text(name.into())]),
        )
    };
    assert_eq!(
        marked.step(&notebook).await?,
        [(
            Vec::new(),
            Patch::Insert {
                parent: vec![(0, Vec::new())],
                slot: 0,
                index: 0,
                instance: li("list:home", "Home"),
            }
        )]
    );
    let ul = |uri: &str, name: &str| {
        shown(
            vec![entity(uri)],
            element(
                "ul",
                &[("class", "lists")],
                vec![Slot::Instances(vec![shown(
                    Vec::new(),
                    element("li", &[], vec![Slot::Text(name.into())]),
                )])],
            ),
        )
    };
    assert_eq!(
        unmarked.step(&notebook).await?,
        [(
            Vec::new(),
            Patch::Insert {
                parent: Vec::new(),
                slot: 0,
                index: 0,
                instance: ul("list:home", "Home"),
            }
        )],
        "with no marker the whole template repeats"
    );

    write(&mut notebook, r#"(assert (list list:work name: "Work"))"#).await?;
    assert_eq!(
        marked.step(&notebook).await?,
        [(
            Vec::new(),
            Patch::Insert {
                parent: vec![(0, Vec::new())],
                slot: 0,
                index: 1,
                instance: li("list:work", "Work"),
            }
        )]
    );
    assert_eq!(
        marked.written(),
        r#"[:ul {:class "lists"} [:li {:subject "list:home"} "Home"] [:li {:subject "list:work"} "Work"]]"#
    );
    unmarked.step(&notebook).await?;
    assert_eq!(
        unmarked.written(),
        r#"[:ul {:class "lists"} [:li "Home"]] [:ul {:class "lists"} [:li "Work"]]"#
    );
    Ok(())
}

/// A field of many values, as the concept declares it, is cloned at the
/// element that shows it, once a value, within the one match of the
/// task. A task with no tag is no `task`: the concept requires every
/// field, so nothing is shown. Moving the one-valued title into another
/// element changes nothing of what repeats.
#[dialog_common::test]
async fn it_clones_the_element_that_shows_a_field_of_many_values() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, TASKS).await?;
    let views = views_in(TASKS, ROWS)?;
    let keys = |name: &str| -> Result<Vec<(String, Vec<String>)>> {
        let view = views.iter().find(|view| view.name == name).expect("a view");
        Ok(Plan::repeating(view)?
            .elements
            .iter()
            .filter(|place| !place.key.is_empty())
            .map(|place| match &place.placed {
                dialog_notation::Placed::Element(tag) => (tag.clone(), place.key.clone()),
                _ => (String::new(), place.key.clone()),
            })
            .collect())
    };
    let repeats = vec![
        ("li".to_string(), vec!["t".to_string()]),
        ("span".into(), vec!["tag".into()]),
    ];
    assert_eq!(keys("row")?, repeats);
    assert_eq!(keys("moved")?, repeats, "the title moved, the same repeats");

    let mut row = Caller::open(
        &notebook,
        views.clone(),
        ("task", "row"),
        Some(entity("task:1")),
    )
    .await?;
    let mut moved =
        Caller::open(&notebook, views, ("task", "moved"), Some(entity("task:1"))).await?;
    assert_eq!(row.step(&notebook).await?, []);
    moved.step(&notebook).await?;

    write(
        &mut notebook,
        r#"(assert (xyz.task/title task:1 "Buy milk"))"#,
    )
    .await?;
    assert_eq!(row.step(&notebook).await?, [], "no tag: no task");
    assert_eq!(moved.step(&notebook).await?, []);

    let tag = |name: &str| {
        shown(
            vec![Value::String(name.into())],
            element("span", &[("class", "tag")], vec![Slot::Text(name.into())]),
        )
    };
    write(&mut notebook, r#"(assert (xyz.task/tag task:1 "shop"))"#).await?;
    assert_eq!(
        row.step(&notebook).await?,
        [(
            Vec::new(),
            Patch::Insert {
                parent: Vec::new(),
                slot: 0,
                index: 0,
                instance: shown(
                    vec![entity("task:1")],
                    element(
                        "li",
                        &[],
                        vec![
                            Slot::Text("Buy milk".into()),
                            Slot::Instances(vec![tag("shop")])
                        ]
                    )
                ),
            }
        )]
    );
    moved.step(&notebook).await?;
    assert_eq!(
        moved.written(),
        r#"[:li [:p "Buy milk"] [:span {:class "tag"} "shop"]]"#
    );

    write(&mut notebook, r#"(assert (xyz.task/tag task:1 "home"))"#).await?;
    let added = [(
        Vec::new(),
        Patch::Insert {
            parent: vec![(0, vec![entity("task:1")])],
            slot: 1,
            index: 0,
            instance: tag("home"),
        },
    )];
    assert_eq!(
        row.step(&notebook).await?,
        added,
        "a second tag is one span"
    );
    assert_eq!(
        moved.step(&notebook).await?,
        added,
        "and the same with the title moved"
    );
    assert_eq!(
        row.written(),
        r#"[:li "Buy milk" [:span {:class "tag"} "home"] [:span {:class "tag"} "shop"]]"#
    );

    write(&mut notebook, r#"(retract (xyz.task/tag task:1 "shop"))"#).await?;
    let removed = [(
        Vec::new(),
        Patch::Remove {
            at: vec![
                (0, vec![entity("task:1")]),
                (1, vec![Value::String("shop".into())]),
            ],
        },
    )];
    assert_eq!(row.step(&notebook).await?, removed);
    assert_eq!(moved.step(&notebook).await?, removed);
    Ok(())
}

const CALLS: &str = r#"
    (view list :lists [:section (view list :card)])
    (view list :card
      [:article [:h2 ?name] [:ul (view todo :item list: ?l state: :open)]]
      (list ?l name: ?name))
    (view todo :item [:li {:subject ?t} ?title] (todo ?t title: ?title ...))
    (view list :board [:article (view todo :chip list: ?l)] (list ?l name: ?name))"#;

/// A view calls another, `(view todo :item list: ?l state: :open)`:
/// each call is mounted with subscriptions of its own, rendering every
/// to-do its labels narrow to. The lists stand with no list; a list
/// added mounts its card, the card its items. Completing a to-do takes it
/// out of the open items. A call of a view no document holds shows each
/// to-do as its source, and the text follows a field.
#[dialog_common::test]
async fn it_mounts_a_called_view_narrowed_and_shows_one_not_found_as_source() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, CONCEPTS).await?;
    let views = views_in(CONCEPTS, CALLS)?;
    let mut lists = Caller::open(&notebook, views.clone(), ("list", "lists"), None).await?;
    let mut board = Caller::open(
        &notebook,
        views,
        ("list", "board"),
        Some(entity("list:home")),
    )
    .await?;
    let first = lists.step(&notebook).await?;
    assert!(
        matches!(first.as_slice(), [(path, Patch::Insert { parent, .. })] if path.is_empty() && parent.is_empty()),
        "{first:?}"
    );
    assert_eq!(lists.written(), "[:section]");
    assert_eq!(board.step(&notebook).await?, []);

    write(&mut notebook, r#"(assert (list list:home name: "Home"))"#).await?;
    let card = vec![(0, Vec::new()), (0, Vec::new())];
    let added = lists.step(&notebook).await?;
    assert!(
        matches!(added.as_slice(), [(path, Patch::Insert { parent, index: 0, .. })] if *path == [card.clone()] && parent.is_empty()),
        "{added:?}"
    );
    assert_eq!(
        lists.written(),
        r#"[:section [:article [:h2 "Home"] [:ul]]]"#
    );
    board.step(&notebook).await?;
    assert_eq!(board.written(), "[:article]");

    write(
        &mut notebook,
        r#"(assert (todo todo:1 list: list:home title: "Buy milk" state: :open))"#,
    )
    .await?;
    let items = vec![
        card.clone(),
        vec![
            (0, vec![entity("list:home")]),
            (1, Vec::new()),
            (0, Vec::new()),
        ],
    ];
    assert_eq!(
        lists.step(&notebook).await?,
        [(
            items.clone(),
            Patch::Insert {
                parent: Vec::new(),
                slot: 0,
                index: 0,
                instance: shown(
                    vec![entity("todo:1")],
                    element(
                        "li",
                        &[("subject", "todo:1")],
                        vec![Slot::Text("Buy milk".into())]
                    )
                ),
            }
        )]
    );
    let chips = vec![vec![(0, vec![entity("list:home")]), (0, Vec::new())]];
    let source = |state: &str, title: &str| {
        format!("(todo todo:1 list: list:home state: {state} title: \"{title}\")")
    };
    assert_eq!(
        board.step(&notebook).await?,
        [(
            chips.clone(),
            Patch::Insert {
                parent: Vec::new(),
                slot: 0,
                index: 0,
                instance: shown(
                    vec![entity("todo:1")],
                    element(
                        dialog_notation::view::plan::SOURCE_TAG,
                        &[],
                        vec![Slot::Text(source(":open", "Buy milk"))]
                    )
                ),
            }
        )]
    );

    write(
        &mut notebook,
        r#"(assert (todo todo:1 list: list:home title: "Buy oat milk" state: :open))"#,
    )
    .await?;
    let item = vec![(0, vec![entity("todo:1")])];
    assert_eq!(
        lists.step(&notebook).await?,
        [(
            items.clone(),
            Patch::SetText {
                at: item.clone(),
                slot: 0,
                text: "Buy oat milk".into(),
            }
        )]
    );
    assert_eq!(
        board.step(&notebook).await?,
        [(
            chips.clone(),
            Patch::SetText {
                at: item.clone(),
                slot: 0,
                text: source(":open", "Buy oat milk"),
            }
        )]
    );

    write(
        &mut notebook,
        r#"(assert (todo todo:1 list: list:home title: "Buy oat milk" state: :done))"#,
    )
    .await?;
    assert_eq!(
        lists.step(&notebook).await?,
        [(items.clone(), Patch::Remove { at: item.clone() })],
        "a completed to-do is no open item"
    );
    assert_eq!(
        lists.written(),
        r#"[:section [:article [:h2 "Home"] [:ul]]]"#
    );
    assert_eq!(
        board.step(&notebook).await?,
        [(
            chips.clone(),
            Patch::SetText {
                at: item.clone(),
                slot: 0,
                text: source(":done", "Buy oat milk"),
            }
        )]
    );

    write(
        &mut notebook,
        r#"(assert (todo todo:2 list: list:home title: "Call mum" state: :open))"#,
    )
    .await?;
    lists.step(&notebook).await?;
    assert_eq!(
        lists.written(),
        r#"[:section [:article [:h2 "Home"] [:ul [:li {:subject "todo:2"} "Call mum"]]]]"#
    );
    write(
        &mut notebook,
        r#"(retract (todo todo:2 list: list:home title: "Call mum" state: :open))"#,
    )
    .await?;
    assert_eq!(
        lists.step(&notebook).await?,
        [(
            items,
            Patch::Remove {
                at: vec![(0, vec![entity("todo:2")])]
            }
        )]
    );
    Ok(())
}

/// The forms of a document that are not views, its writes kept or not.
fn forms_of(text: &str, writes: bool) -> Result<String> {
    let forms = dialog_notation::read(text)?;
    Ok(forms
        .iter()
        .filter(|form| form.head() != Some("view"))
        .filter(|form| writes || !matches!(form.head(), Some("assert" | "retract")))
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n"))
}

/// What a sketch's restated views render on a branch: each view's
/// repeating element and its axes, then the trees with nothing to show
/// and after the writes.
async fn rendered(
    sketch: &str,
    views: &str,
    tops: &[(&str, &str, Option<&str>)],
    data: &[&str],
) -> Result<(Vec<String>, Vec<String>, Vec<String>)> {
    let core = include_str!("../sketches/core.clj");
    let described = views_of(&[core, &forms_of(sketch, true)?, views])?;
    let plans: Vec<String> = described
        .iter()
        .map(|view| {
            let plan = Plan::repeating(view)?;
            let tag = |at: usize| match &plan.elements[at].placed {
                dialog_notation::Placed::Element(tag) => tag.clone(),
                dialog_notation::Placed::View(nested) => format!("{{:view :{}}}", nested.view),
                dialog_notation::Placed::Call(call) => {
                    format!("(view {} :{})", call.concept, call.view)
                }
            };
            let repeats: Vec<String> = plan
                .elements
                .iter()
                .enumerate()
                .filter(|(_, place)| !place.key.is_empty())
                .map(|(at, place)| format!("{} by {}", tag(at), place.key.join(" ")))
                .collect();
            Ok(format!(
                "{} :{} repeats {}{}",
                plan.concept,
                plan.name,
                if repeats.is_empty() {
                    "nothing".to_string()
                } else {
                    repeats.join(", ")
                },
                if plan.cases.is_empty() {
                    String::new()
                } else {
                    format!(" | {:?}", plan.cases)
                }
            ))
        })
        .collect::<Result<_>>()?;
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, core).await?;
    // The concepts alone: what the views read. The sketch's rules are
    // not what is rendered.
    let concepts: String = dialog_notation::read(sketch)?
        .iter()
        .filter(|form| form.head() == Some("concept"))
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    write(&mut notebook, &concepts).await?;
    let mut callers = Vec::new();
    for (concept, name, entity_of) in tops {
        callers.push(
            Caller::open(
                &notebook,
                described.clone(),
                (concept, name),
                entity_of.map(entity),
            )
            .await?,
        );
    }
    let mut empty = Vec::new();
    for caller in &mut callers {
        caller.step(&notebook).await?;
        empty.push(caller.written());
    }
    let writes: String = dialog_notation::read(sketch)?
        .iter()
        .filter(|form| matches!(form.head(), Some("assert" | "retract")))
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    write(&mut notebook, &writes).await?;
    for text in data {
        write(&mut notebook, text).await?;
    }
    let mut full = Vec::new();
    for caller in &mut callers {
        caller.step(&notebook).await?;
        full.push(caller.written());
    }
    Ok((plans, empty, full))
}

/// The notebook's and the table's views restated with calls and
/// markers, read after core and the forms of their documents that are not
/// views, rendered on a branch by tonk's rule: what repeats in each, what
/// each shows with nothing to show, and with the sketches' own seeds and a
/// notebook of two blocks.
#[dialog_common::test]
async fn it_renders_the_restated_views_with_nothing_and_with_a_little() -> Result<()> {
    let (plans, empty, full) = rendered(
        include_str!("../sketches/notebook.clj"),
        include_str!("../sketches/notebook-views.clj"),
        &[
            ("notebook", "ui", Some("notebook:1")),
            ("notebook", "directory", None),
            ("notebook/block", "directory", None),
            ("notebook/index-route", "ui", Some("site:1")),
            ("notebook/route", "ui", Some("site:2")),
        ],
        &[
            r#"(assert (notebook notebook:1 title: "Plan" key: "N1" block: block:a))"#,
            r#"(assert (notebook notebook:1 title: "Plan" key: "N2" block: block:b))"#,
            r#"(assert (notebook/block block:a notebook: notebook:1 source: "Hi"))"#,
            r#"(assert (notebook/block block:b notebook: notebook:1 source: "Text"))"#,
            r#"(assert (notebook/index-route site:1 repo: "r" branch: "main"))"#,
            r#"(assert (notebook/route site:2 entity: notebook:1 repo: "r" branch: "main"))"#,
        ],
    )
    .await?;
    assert_eq!(
        plans,
        [
            "notebook :ui repeats tonk-notebook by n, span by k b",
            "notebook :directory repeats div by n",
            "notebook/block :directory repeats div by b",
            "notebook/index-route :ui repeats tonk-notebook-index by site",
            "notebook/route :ui repeats div by site",
        ]
    );
    assert_eq!(
        empty,
        [
            r#"[:notebook-save {:style "display: block;"}]"#,
            "",
            r#"[:div {:class "notebook-block-rows"}]"#,
            "",
            "",
        ]
    );
    let index = r#"[:div {:class "notebook-index"} [:a {:class "notebook-index__item" :href "/notebook/id:notebook/scratch"} [:span {:class "notebook-index__title"} "Scratch"]]] [:div {:class "notebook-index"} [:a {:class "notebook-index__item" :href "/notebook/notebook:1"} [:span {:class "notebook-index__title"} "Plan"]]]"#;
    assert_eq!(full[1], index, "one index a notebook, as tonk renders it");
    let order = r#"[:div {:class "notebook-order"} [:span {:class "notebook-order__item" :data-block "block:a" :data-key "N1"}] [:span {:class "notebook-order__item" :data-block "block:b" :data-key "N2"}]]"#;
    assert!(full[0].contains(order), "{}", full[0]);
    assert!(full[4].starts_with(r#"[:div {:class "notebook-space"} [:notebook-save"#));
    assert!(full[3].ends_with(&format!("{index}]")), "{}", full[3]);

    let (plans, empty, full) = rendered(
        include_str!("../sketches/table.clj"),
        include_str!("../sketches/table-views.clj"),
        &[
            ("table/sheet", "directory", None),
            ("table/cell", "directory", None),
            ("table/column", "directory", None),
            ("table/row", "directory", None),
            ("table", "ui", Some("id:table/book")),
            ("table", "directory", None),
        ],
        &[],
    )
    .await?;
    assert_eq!(
        plans,
        [
            "table/sheet :directory repeats div by s",
            "table/cell :directory repeats div by c",
            "table/column :directory repeats div by k",
            "table/row :directory repeats div by r",
            "table :ui repeats tonk-table by t",
            "table :directory repeats div by t",
        ]
    );
    assert_eq!(
        empty,
        [
            r#"[:div {:class "table-sheet-rows"}]"#,
            r#"[:div {:class "table-cell-rows"}]"#,
            r#"[:div {:class "table-column-rows"}]"#,
            r#"[:div {:class "table-rowsize-rows"}]"#,
            "",
            "",
        ]
    );
    assert_eq!(
        full[0],
        r#"[:div {:class "table-sheet-rows"} [:div {:class "table-sheet-row" :subject "id:table/book/sheet1" :data-table "id:table/book" :data-name "Sheet1" :data-order "m"}]]"#
    );
    assert_eq!(full[1].matches("table-cell-row\"").count(), 14);
    assert!(full[5].starts_with(
        r#"[:div {:class "table-directory"} [:tonk-table {:subject "id:table/book"}"#
    ));
    Ok(())
}

impl Caller {
    /// A view mounted under a plan of the caller's choosing.
    async fn with(
        notebook: &Notebook,
        views: Vec<View>,
        plan: Plan,
        entity: Option<Value>,
    ) -> Result<Caller> {
        let driven = Driven::new(notebook, plan, entity, How::Subscribed).await?;
        Ok(Caller {
            views,
            mounted: vec![(Vec::new(), driven)],
        })
    }
}

/// A view under tonk's rule and under the layered plan, side by side:
/// for each reading, the tree before any write and after each, and the
/// patches each write gave.
async fn side_by_side(
    concepts: &str,
    views: &str,
    view: (&str, &str),
    entity_of: Option<&str>,
    writes: &[&str],
) -> Result<Vec<(&'static str, Vec<String>, Vec<Vec<Patch>>)>> {
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, concepts).await?;
    let described = views_in(concepts, views)?;
    let chosen = described
        .iter()
        .find(|described| (described.concept.as_str(), described.name.as_str()) == view)
        .expect("the view")
        .clone();
    let mut readings = Vec::new();
    for (label, plan) in [
        ("tonk", Plan::repeating(&chosen)?),
        ("layered", Plan::of(&chosen)?),
    ] {
        readings.push((
            label,
            Caller::with(&notebook, described.clone(), plan, entity_of.map(entity)).await?,
            Vec::new(),
            Vec::new(),
        ));
    }
    for (_, caller, trees, patches) in &mut readings {
        let said = caller.step(&notebook).await?;
        trees.push(caller.written());
        patches.push(said.into_iter().map(|(_, patch)| patch).collect());
    }
    for text in writes {
        write(&mut notebook, text).await?;
        for (_, caller, trees, patches) in &mut readings {
            let said = caller.step(&notebook).await?;
            trees.push(caller.written());
            patches.push(said.into_iter().map(|(_, patch)| patch).collect());
        }
    }
    Ok(readings
        .into_iter()
        .map(|(label, _, trees, patches)| (label, trees, patches))
        .collect())
}

const INDEX: &str = r#"
    (concept (notebook ?n title: ?title)
      (xyz.nb/title of: ?n is: ?title as: :text cardinality: :one))"#;

const INDEXES: &str = r#"
    (view notebook :index
      [:div {:class "notebook-index"} [:a {:href ["/notebook/" ?n]} [:span ?title]]]
      (notebook ?n title: ?title))
    (view notebook :marked
      [:div {:class "notebook-index"}
        [:a {:subject ?n :href ["/notebook/" ?n]} [:span ?title]]]
      (notebook ?n title: ?title))"#;

const BOOKS: &str = r#"
    (concept (book ?n key: ?k block: ?b)
      (xyz.book/?k of: ?n is: ?b as: :entity cardinality: :one by: :position))"#;

const ORDER: &str = r#"
    (view book :order
      [:div {:class "order"} [:span {:data-block ?b :data-key ?k}]]
      (book ?n key: ?k block: ?b))"#;

/// A patch said short: what it does, to what, and how deep.
fn said(patch: &Patch) -> String {
    let tag = |shown: &Shown| match &shown.content {
        Content::Element { tag, .. } => tag.clone(),
        Content::View(mount) => format!("(view :{})", mount.view),
    };
    match patch {
        Patch::Insert {
            parent, instance, ..
        } => format!("insert {} under {}", tag(instance), parent.len()),
        Patch::Remove { at } => format!("remove at {}", at.len()),
        Patch::SetText { text, .. } => format!("text {text:?}"),
        Patch::SetAttribute { name, value, .. } => format!(":{name} {value:?}"),
        Patch::SetEvent { .. } => "event".into(),
        Patch::SetView { .. } => "view".into(),
    }
}

/// One case side by side: for each reading, each step's tree and what
/// its patches did.
async fn compared(
    concepts: &str,
    views: &str,
    view: (&str, &str),
    entity_of: Option<&str>,
    writes: &[&str],
) -> Result<Vec<(String, Vec<String>)>> {
    Ok(side_by_side(concepts, views, view, entity_of, writes)
        .await?
        .into_iter()
        .map(|(label, trees, patches)| {
            let steps = trees
                .iter()
                .zip(&patches)
                .map(|(tree, patches)| {
                    let did: Vec<String> = patches.iter().map(said).collect();
                    format!("{tree} | {}", did.join(", "))
                })
                .collect();
            (label.to_string(), steps)
        })
        .collect())
}

fn reading(label: &str, steps: &[&str]) -> (String, Vec<String>) {
    (
        label.to_string(),
        steps.iter().map(ToString::to_string).collect(),
    )
}

/// tonk's rule, the default, beside the layered plan, on four cases:
/// the notebook index with no notebook, one and two, with and without the
/// marker; the list card whose conditions read a second entity; the
/// to-do row with no tag, one and two, and with its title moved; and the
/// blocks of a notebook by position. Each step's tree, and what the
/// write before it patched.
#[dialog_common::test]
async fn it_compares_tonks_rule_with_the_layered_plan() -> Result<()> {
    let notebooks = [
        r#"(assert (notebook notebook:1 title: "Plan"))"#,
        r#"(assert (notebook notebook:2 title: "Trip"))"#,
    ];
    let index =
        r#"[:div {:class "notebook-index"} [:a {:href "/notebook/notebook:1"} [:span "Plan"]]]"#;
    let index2 =
        r#"[:div {:class "notebook-index"} [:a {:href "/notebook/notebook:2"} [:span "Trip"]]]"#;
    let unmarked = [
        " | ".to_string(),
        format!("{index} | insert div under 0"),
        format!("{index} {index2} | insert div under 0"),
    ];
    assert_eq!(
        compared(INDEX, INDEXES, ("notebook", "index"), None, &notebooks).await?,
        [
            ("tonk".to_string(), unmarked.to_vec()),
            ("layered".to_string(), unmarked.to_vec()),
        ],
        "with no marker both repeat the whole index a notebook"
    );
    let a = |n: &str, title: &str| {
        format!(
            r#"[:a {{:subject "notebook:{n}" :href "/notebook/notebook:{n}"}} [:span "{title}"]]"#
        )
    };
    let div = |inside: &str| format!(r#"[:div {{:class "notebook-index"}}{inside}]"#);
    assert_eq!(
        compared(INDEX, INDEXES, ("notebook", "marked"), None, &notebooks).await?,
        [
            (
                "tonk".to_string(),
                vec![
                    format!("{} | insert div under 0", div("")),
                    format!(
                        "{} | insert a under 1",
                        div(&format!(" {}", a("1", "Plan")))
                    ),
                    format!(
                        "{} | insert a under 1",
                        div(&format!(" {} {}", a("1", "Plan"), a("2", "Trip")))
                    ),
                ]
            ),
            (
                "layered".to_string(),
                vec![
                    " | ".to_string(),
                    format!(
                        "{} | insert div under 0",
                        div(&format!(" {}", a("1", "Plan")))
                    ),
                    format!(
                        "{} {} | insert div under 0",
                        div(&format!(" {}", a("1", "Plan"))),
                        div(&format!(" {}", a("2", "Trip")))
                    ),
                ]
            ),
        ],
        "the marker: tonk keeps the index and repeats the link; the layered plan ignores it"
    );

    let todos = [
        r#"(assert (list list:home name: "Home"))"#,
        r#"(assert (todo todo:1 list: list:home title: "Buy milk" state: :open))"#,
        r#"(assert (todo todo:2 list: list:home title: "Call mum" state: :open))"#,
    ];
    let li = |title: &str| format!(r#"[:li {{:class "open:"}} "{title}"]"#);
    assert_eq!(
        compared(CONCEPTS, CARD, ("list", "card"), Some("list:home"), &todos).await?,
        [
            reading(
                "tonk",
                &[
                    " | ",
                    " | ",
                    &format!(
                        r#"[:article [:h2 "Home"] [:ul {}]] | insert article under 0"#,
                        li("Buy milk")
                    ),
                    &format!(
                        r#"[:article [:h2 "Home"] [:ul {} {}]] | insert li under 2"#,
                        li("Buy milk"),
                        li("Call mum")
                    ),
                ]
            ),
            reading(
                "layered",
                &[
                    " | ",
                    r#"[:article [:h2 "Home"] [:ul]] | insert article under 0"#,
                    &format!(
                        r#"[:article [:h2 "Home"] [:ul {}]] | insert li under 2"#,
                        li("Buy milk")
                    ),
                    &format!(
                        r#"[:article [:h2 "Home"] [:ul {} {}]] | insert li under 2"#,
                        li("Buy milk"),
                        li("Call mum")
                    ),
                ]
            ),
        ],
        "a second entity: one query has no card for a list with no to-do"
    );

    let tags = [
        r#"(assert (xyz.task/title task:1 "Buy milk"))"#,
        r#"(assert (xyz.task/tag task:1 "shop"))"#,
        r#"(assert (xyz.task/tag task:1 "home"))"#,
    ];
    let span = |tag: &str| format!(r#"[:span {{:class "tag"}} "{tag}"]"#);
    let row = [
        " | ".to_string(),
        " | ".to_string(),
        format!(r#"[:li "Buy milk" {}] | insert li under 0"#, span("shop")),
        format!(
            r#"[:li "Buy milk" {} {}] | insert span under 1"#,
            span("home"),
            span("shop")
        ),
    ];
    assert_eq!(
        compared(TASKS, ROWS, ("task", "row"), Some("task:1"), &tags).await?,
        [
            ("tonk".to_string(), row.to_vec()),
            ("layered".to_string(), row.to_vec())
        ],
        "a field of many values: both clone the span, and a task with no tag is none"
    );
    let moved = [
        " | ".to_string(),
        " | ".to_string(),
        format!(
            r#"[:li [:p "Buy milk"] {}] | insert li under 0"#,
            span("shop")
        ),
        format!(
            r#"[:li [:p "Buy milk"] {} {}] | insert span under 1"#,
            span("home"),
            span("shop")
        ),
    ];
    assert_eq!(
        compared(TASKS, ROWS, ("task", "moved"), Some("task:1"), &tags).await?,
        [
            ("tonk".to_string(), moved.to_vec()),
            ("layered".to_string(), moved.to_vec())
        ],
        "the title moved: the same repeats"
    );

    let entries = [
        r#"(assert (book book:1 key: "N2" block: block:b))"#,
        r#"(assert (book book:1 key: "N1" block: block:a))"#,
    ];
    let entry = |block: &str, key: &str| {
        format!(r#"[:span {{:data-block "block:{block}" :data-key "{key}"}}]"#)
    };
    let order = [
        " | ".to_string(),
        format!(
            r#"[:div {{:class "order"}} {}] | insert div under 0"#,
            entry("b", "N2")
        ),
        format!(
            r#"[:div {{:class "order"}} {} {}] | insert span under 1"#,
            entry("a", "N1"),
            entry("b", "N2")
        ),
    ];
    assert_eq!(
        compared(BOOKS, ORDER, ("book", "order"), Some("book:1"), &entries).await?,
        [
            ("tonk".to_string(), order.to_vec()),
            ("layered".to_string(), order.to_vec())
        ],
        "entries by position: an entry before the first is one span, first"
    );
    Ok(())
}
