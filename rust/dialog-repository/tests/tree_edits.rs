#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use dialog_corpus::{Entity, Value};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_query::Fold;
use dialog_query::reduce::{Aggregator, Reduce, ReduceEntry};
use dialog_query::{
    AttributeQuery, ConceptConclusion, ConceptQuery, MapPattern, Output as _, Parameters, Term, the,
};

use dialog_repository::{
    Delta, EditedTree, ProjectedNode, TreeEdit, TreeEditError, TreeId, TreePosition,
    TreeProjection, TreeSource,
};

#[derive(Clone, Debug, PartialEq, Eq)]
enum Content {
    Notebook,
    Card(String),
    Tag(String),
}

fn id(text: impl AsRef<str>) -> TreeId {
    text.as_ref().into()
}

fn node(name: &str, parent: Option<&str>, order: &str, value: Content) -> ProjectedNode<Content> {
    ProjectedNode {
        id: id(name),
        parent: parent.map(id),
        order: order.as_bytes().to_vec(),
        value,
    }
}

fn apply<V>(tree: &mut EditedTree<V>, edits: Vec<TreeEdit<V>>) -> Result<(), TreeEditError> {
    for edit in edits {
        tree.apply(edit)?;
    }
    Ok(())
}

#[dialog_common::test]
fn it_moves_before_an_existing_sibling_using_only_node_ids() -> anyhow::Result<()> {
    let a = node("a", None, "b", Content::Card("A".into()));
    let b = node("b", None, "a", Content::Card("B".into()));
    let child = node("tag", Some("a"), "", Content::Tag("home".into()));
    let mut projection = TreeProjection::default();
    let mut receiver = EditedTree::default();
    apply(
        &mut receiver,
        projection.apply(Delta {
            asserted: vec![a.clone(), b, child],
            retracted: vec![],
        })?,
    )?;
    assert_eq!(receiver.children(None).collect::<Vec<_>>(), ["b", "a"]);
    let edits = projection.apply(Delta {
        asserted: vec![ProjectedNode {
            order: vec![],
            ..a.clone()
        }],
        retracted: vec![a],
    })?;
    assert_eq!(
        edits,
        [TreeEdit::Move {
            id: id("a"),
            to: TreePosition {
                parent: None,
                before: Some(id("b"))
            },
        }]
    );
    apply(&mut receiver, edits)?;
    assert_eq!(receiver.children(None).collect::<Vec<_>>(), ["a", "b"]);
    assert_eq!(receiver.children(Some("a")).collect::<Vec<_>>(), ["tag"]);
    Ok(())
}

#[dialog_common::test]
fn it_reparents_an_existing_subtree_and_keeps_its_children() -> anyhow::Result<()> {
    let a = node("a", None, "", Content::Notebook);
    let x = node("x", Some("a"), "", Content::Card("X".into()));
    let b = node("b", Some("x"), "", Content::Notebook);
    let c = node("c", Some("b"), "a", Content::Card("C".into()));
    let tag = node("tag", Some("c"), "", Content::Tag("urgent".into()));
    let mut projection = TreeProjection::default();
    let mut receiver = EditedTree::default();
    apply(
        &mut receiver,
        projection.apply(Delta {
            asserted: vec![tag.clone(), c.clone(), b.clone(), x.clone(), a.clone()],
            retracted: vec![],
        })?,
    )?;
    // The new direct parent c is unchanged. Its ancestor b must move
    // out of a's subtree before a can become a child of c.
    let edits = projection.apply(Delta {
        asserted: vec![
            ProjectedNode {
                parent: Some(id("c")),
                ..a.clone()
            },
            ProjectedNode {
                parent: None,
                ..b.clone()
            },
        ],
        retracted: vec![a, b],
    })?;
    assert!(matches!(edits.as_slice(), [
        TreeEdit::Move { id: first, .. }, TreeEdit::Move { id: second, .. }
    ] if &**first == "b" && &**second == "a"));
    apply(&mut receiver, edits)?;
    assert_eq!(receiver.children(None).collect::<Vec<_>>(), ["b"]);
    assert_eq!(
        receiver.children(Some("c")).collect::<Vec<_>>(),
        ["a", "tag"]
    );
    assert_eq!(receiver.children(Some("a")).collect::<Vec<_>>(), ["x"]);
    assert_eq!(receiver.value("tag"), Some(&tag.value));
    let invalid = projection
        .apply(Delta {
            retracted: vec![c.clone()],
            asserted: vec![ProjectedNode {
                parent: Some(id("x")),
                ..c
            }],
        })
        .expect_err("cycle");
    assert_eq!(invalid.reason, "parent cycle");
    Ok(())
}

#[dialog_common::test]
fn it_consolidates_contributions_and_removes_a_subtree_once() -> anyhow::Result<()> {
    let root = node("root", None, "", Content::Notebook);
    let card = node("card", Some("root"), "", Content::Card("A".into()));
    let tag = node("tag", Some("card"), "", Content::Tag("urgent".into()));
    let mut projection = TreeProjection::default();
    let mut receiver = EditedTree::default();
    apply(
        &mut receiver,
        projection.apply(Delta {
            asserted: vec![tag.clone(), card.clone(), root.clone(), tag.clone()],
            retracted: vec![],
        })?,
    )?;
    assert!(
        projection
            .apply(Delta {
                asserted: vec![],
                retracted: vec![tag.clone()],
            })?
            .is_empty()
    );
    assert!(
        projection
            .apply(Delta {
                asserted: vec![],
                retracted: vec![root.clone()],
            })
            .is_err(),
        "a surviving child cannot lose its parent"
    );
    let edits = projection.apply(Delta {
        asserted: vec![],
        retracted: vec![tag, card, root],
    })?;
    assert_eq!(edits, [TreeEdit::Remove { id: id("root") }]);
    apply(&mut receiver, edits)?;
    assert!(receiver.children(None).next().is_none());
    assert!(receiver.value("tag").is_none());
    Ok(())
}

fn query(fields: &[(&str, &str, &str, &str)], subject: &str) -> anyhow::Result<ConceptQuery> {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var(subject));
    let mut attributes = serde_json::Map::new();
    for (field, relation, kind, cardinality) in fields {
        attributes.insert(
            (*field).into(),
            serde_json::json!({
                "the": relation, "as": kind, "cardinality": cardinality
            }),
        );
        terms.insert((*field).into(), Term::var(*field));
    }
    Ok(ConceptQuery {
        predicate: serde_json::from_value(serde_json::json!({"with": attributes}))?,
        terms,
    })
}

fn card_query() -> anyhow::Result<ConceptQuery> {
    query(
        &[
            ("notebook", "todo/notebook", "entity:", "one"),
            ("title", "todo/title", "text:", "one"),
        ],
        "todo",
    )
}

fn sources(
    visits: Arc<[AtomicUsize; 3]>,
) -> anyhow::Result<[TreeSource<ConceptQuery, Content>; 3]> {
    let notebooks = query(&[("name", "notebook/title", "text:", "one")], "notebook")?;
    let cards = card_query()?;
    let tags = query(
        &[
            ("notebook", "todo/notebook", "entity:", "one"),
            ("title", "todo/title", "text:", "one"),
            ("tag", "todo/tag", "text:", "many"),
        ],
        "todo",
    )?;
    let count = |at: usize| {
        let visits = visits.clone();
        move || {
            visits[at].fetch_add(1, Ordering::Relaxed);
        }
    };
    let (count_notebook, count_card, count_tag) = (count(0_usize), count(1), count(2));
    Ok([
        TreeSource::new(notebooks, move |row: &ConceptConclusion, out| {
            count_notebook();
            out.push(node(row.entity().as_str(), None, "", Content::Notebook));
            Ok(())
        }),
        TreeSource::new(cards, move |row: &ConceptConclusion, out| {
            count_card();
            let parent: Entity = row.get("notebook")?;
            let title: String = row.get("title")?;
            let order = title.as_bytes().to_vec();
            out.push(ProjectedNode {
                id: id(row.entity().as_str()),
                parent: Some(id(parent.as_str())),
                order,
                value: Content::Card(title),
            });
            Ok(())
        }),
        TreeSource::new(tags, move |row: &ConceptConclusion, out| {
            count_tag();
            let todo = row.entity().as_str();
            let tag: String = row.get("tag")?;
            out.push(ProjectedNode {
                id: id(format!("{todo}/tag/{tag}")),
                parent: Some(id(todo)),
                order: tag.as_bytes().to_vec(),
                value: Content::Tag(tag),
            });
            Ok(())
        }),
    ])
}

fn nested_fold() -> anyhow::Result<Fold<AttributeQuery, Fold<ConceptQuery, AttributeQuery>>> {
    let text = |value: &str| Value::String(value.into());
    let cards = Fold::new(
        card_query()?,
        AttributeQuery::new(
            the!("todo/tag").into(),
            Term::<Entity>::var("todo"),
            Term::var("tag"),
            Term::blank(),
            None,
        ),
        Reduce::new(
            vec!["todo".into()],
            vec![
                ReduceEntry::new("page", Aggregator::Min, Term::var("notebook")),
                ReduceEntry::new("title", Aggregator::Min, Term::var("title")),
                ReduceEntry::new("tags", Aggregator::Distinct, Term::var("tag")),
            ],
        ),
    )
    .with_projection(
        "card",
        MapPattern::new([
            (text("id"), Term::var("todo").into()),
            (text("title"), Term::var("title").into()),
            (text("tags"), Term::var("tags").into()),
        ])?,
    );
    Ok(Fold::new(
        AttributeQuery::new(
            the!("notebook/title").into(),
            Term::<Entity>::var("page"),
            Term::blank(),
            Term::blank(),
            None,
        ),
        cards,
        Reduce::new(
            vec!["page".into()],
            vec![ReduceEntry::new(
                "cards",
                Aggregator::Distinct,
                Term::var("card"),
            )],
        ),
    ))
}

#[dialog_common::test]
async fn it_delivers_one_tag_edit_in_a_large_notebook() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let home: Entity = "id:home".parse()?;
    let work: Entity = "id:work".parse()?;
    let first: Entity = "id:todo-0000".parse()?;
    const CARDS: usize = 512;
    branch
        .transact(|tx| {
            let (home, work, first) = (&home, &work, &first);
            async move {
                let mut tx = tx
                    .assert(
                        the!("notebook/title")
                            .of(home.clone())
                            .is("Home".to_string()),
                    )
                    .assert(
                        the!("notebook/title")
                            .of(work.clone())
                            .is("Work".to_string()),
                    );
                for n in 0..CARDS {
                    let todo: Entity = format!("id:todo-{n:04}").parse()?;
                    tx = tx
                        .assert(the!("todo/notebook").of(todo.clone()).is(home.clone()))
                        .assert(the!("todo/title").of(todo).is(format!("Card {n:04}")));
                }
                anyhow::Ok(tx.assert(the!("todo/tag").of(first.clone()).is("home".to_string())))
            }
        })
        .perform(&operator)
        .await?;

    let visits = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
    let mut stream = branch.subscribe_tree(sources(visits.clone())?);
    let mut receiver = EditedTree::default();
    let mut folded = branch.subscribe_nested_fold(nested_fold()?);
    fn assert_send<T: dialog_common::ConditionalSend>(_: T) {}
    assert_send(stream.poll(&operator));
    apply(&mut receiver, stream.poll(&operator).await?)?;
    folded.poll(&operator).await?;
    assert_eq!(receiver.children(Some(home.as_str())).count(), CARDS);

    let before = visits.each_ref().map(|count| count.load(Ordering::Relaxed));
    let recomputes = stream.recomputes();
    branch
        .transact(|tx| {
            let tag = the!("todo/tag").of(first.clone()).is("urgent".to_string());
            async move { anyhow::Ok(tx.assert(tag)) }
        })
        .perform(&operator)
        .await?;
    let edits = stream.poll(&operator).await?;
    assert_eq!(
        edits,
        [TreeEdit::Insert {
            id: id("id:todo-0000/tag/urgent"),
            at: TreePosition {
                parent: Some(id(first.as_str())),
                before: None
            },
            value: Content::Tag("urgent".into()),
        }]
    );
    let after = visits.each_ref().map(|count| count.load(Ordering::Relaxed));
    assert_eq!(after, [before[0], before[1], before[2] + 1]);
    assert_eq!(stream.recomputes(), recomputes);
    apply(&mut receiver, edits)?;

    let replaced = folded
        .poll(&operator)
        .await?
        .expect("whole notebook replaced");
    assert_eq!(replaced.retracted.len(), 1);
    let Value::Composite(cards) = replaced.asserted[0].get("cards").expect("folded cards") else {
        anyhow::bail!("expected collected cards");
    };
    assert_eq!(cards.len(), CARDS);
    let from_fold: BTreeMap<_, _> = cards
        .items()
        .map(|card| {
            let Value::Composite(card) = card else {
                anyhow::bail!("expected card")
            };
            let Some(Value::Entity(todo)) = card.first(&Value::String("id".into())) else {
                anyhow::bail!("expected card ID")
            };
            let Some(Value::String(title)) = card.first(&Value::String("title".into())) else {
                anyhow::bail!("expected title")
            };
            let Some(Value::Composite(tags)) = card.first(&Value::String("tags".into())) else {
                anyhow::bail!("expected tags")
            };
            Ok((
                todo.to_string(),
                (
                    Content::Card(title),
                    tags.items()
                        .map(|tag| match tag {
                            Value::String(tag) => Content::Tag(tag),
                            _ => panic!("expected a tag"),
                        })
                        .collect::<Vec<_>>(),
                ),
            ))
        })
        .collect::<anyhow::Result<_>>()?;
    let from_edits: BTreeMap<_, _> = receiver
        .children(Some(home.as_str()))
        .map(|todo| {
            (
                todo.to_string(),
                (
                    receiver.value(todo).unwrap().clone(),
                    receiver
                        .children(Some(todo))
                        .map(|tag| receiver.value(tag).unwrap().clone())
                        .collect::<Vec<_>>(),
                ),
            )
        })
        .collect();
    assert_eq!(from_edits, from_fold);

    let fresh = branch
        .query()
        .select(nested_fold()?)
        .perform(&operator)
        .try_vec()
        .await?;
    assert!(fresh.contains(&replaced.asserted[0]));

    #[cfg(not(target_arch = "wasm32"))]
    {
        use std::time::Instant;

        let mut trees = Vec::new();
        let mut folds = Vec::new();
        for step in 0..16 {
            branch
                .transact(|tx| {
                    let tag = the!("todo/tag").of(first.clone()).is("bench".to_string());
                    async move {
                        anyhow::Ok(if step % 2 == 0 {
                            tx.assert(tag)
                        } else {
                            tx.retract(tag)
                        })
                    }
                })
                .perform(&operator)
                .await?;
            let (edits, baseline) = if step % 2 == 0 {
                let start = Instant::now();
                let edits = stream.poll(&operator).await?;
                trees.push(start.elapsed());
                let start = Instant::now();
                let baseline = folded.poll(&operator).await?;
                folds.push(start.elapsed());
                (edits, baseline)
            } else {
                let start = Instant::now();
                let baseline = folded.poll(&operator).await?;
                folds.push(start.elapsed());
                let start = Instant::now();
                let edits = stream.poll(&operator).await?;
                trees.push(start.elapsed());
                (edits, baseline)
            };
            assert_eq!(edits.len(), 1);
            let baseline = baseline.expect("changed folded answer");
            assert_eq!((baseline.asserted.len(), baseline.retracted.len()), (1, 1));
            apply(&mut receiver, edits)?;
        }
        trees.sort_unstable();
        folds.sort_unstable();
        eprintln!(
            "{CARDS} cards, 16 single-tag changes, median poll: edits {:?}, nested fold {:?}",
            trees[8], folds[8],
        );
    }

    branch
        .transact(|tx| {
            let remove = the!("todo/notebook").of(first.clone()).is(home.clone());
            let add = the!("todo/notebook").of(first.clone()).is(work.clone());
            async move { anyhow::Ok(tx.retract(remove).assert(add)) }
        })
        .perform(&operator)
        .await?;
    let edits = stream.poll(&operator).await?;
    assert_eq!(
        edits,
        [TreeEdit::Move {
            id: id(first.as_str()),
            to: TreePosition {
                parent: Some(id(work.as_str())),
                before: None
            },
        }]
    );
    apply(&mut receiver, edits)?;
    assert_eq!(
        receiver.children(Some(work.as_str())).collect::<Vec<_>>(),
        [first.as_str()]
    );
    assert_eq!(receiver.children(Some(first.as_str())).count(), 2);
    Ok(())
}
