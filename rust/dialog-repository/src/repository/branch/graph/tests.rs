use super::*;
use crate::helpers::test_repo;
use dialog_peer::helpers::test_session_with_peer;
use dialog_query::graph::{GraphEdge, GraphSchema, GraphShape, check_bag};
use dialog_query::{Parameters, Term, the};
use serde_json::json;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

fn entity(name: &str) -> Entity {
    format!("id:{name}").parse().expect("test entity")
}

fn subject(term: Term<dialog_query::Any>) -> Parameters {
    let mut terms = Parameters::new();
    terms.insert("this".into(), term);
    terms
}

fn person_query() -> GraphQuery {
    let person: ConceptDescriptor = serde_json::from_value(json!({"with": {
        "name": {"the": "person/name", "as": "text:"},
        "manager": {"the": "person/manager", "as": "entity:", "optional": true}
    }}))
    .unwrap();
    GraphQuery {
        root: ConceptQuery {
            predicate: person.clone(),
            terms: subject(Term::constant(entity("alice"))),
        },
        shape: "person".into(),
        schema: GraphSchema {
            shapes: BTreeMap::from([(
                "person".into(),
                GraphShape {
                    concept: person,
                    edges: BTreeMap::from([(
                        "manager".into(),
                        GraphEdge {
                            target: "person".into(),
                            matching: BTreeMap::new(),
                            shaping: BTreeMap::new(),
                        },
                    )]),
                },
            )]),
        },
    }
}

#[dialog_common::test]
async fn graph_maintains_deep_renames_closes_and_cuts_cycles() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .transaction()
        .assert(
            the!("person/name")
                .of(entity("alice"))
                .is("Alice".to_string()),
        )
        .assert(the!("person/name").of(entity("bob")).is("Bob".to_string()))
        .assert(
            the!("person/name")
                .of(entity("carol"))
                .is("Carol".to_string()),
        )
        .assert(the!("person/manager").of(entity("alice")).is(entity("bob")))
        .assert(the!("person/manager").of(entity("bob")).is(entity("carol")))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let mut graph = branch.subscribe_graph(person_query())?;
    graph.poll(&operator).await?.expect("initial delta");
    assert_eq!(graph.results().nodes.len(), 3);
    let initial = graph.recomputes();
    assert_eq!(graph.retained_subscriptions(), 7);
    branch
        .transaction()
        .retract(
            the!("person/name")
                .of(entity("carol"))
                .is("Carol".to_string()),
        )
        .assert(
            the!("person/name")
                .of(entity("carol"))
                .is("Caroline".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = graph.poll(&operator).await?.expect("rename");
    assert_eq!(delta.fields.len(), 1);
    assert_eq!(delta.fields[0].entity, entity("carol"));
    assert!(delta.roots_added.is_empty() && delta.roots_removed.is_empty());
    assert_eq!(
        graph.recomputes(),
        initial,
        "deep rename is entity-local maintenance"
    );
    branch
        .transaction()
        .assert(
            the!("person/manager")
                .of(entity("carol"))
                .is(entity("alice")),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    graph.poll(&operator).await?.expect("closing cycle");
    assert_eq!(graph.results().nodes.len(), 3);
    assert_eq!(
        graph.recomputes(),
        initial,
        "closing cycle adds no evaluator recursion"
    );
    // Make Bob/Carol a disconnected cycle: it must not keep itself alive.
    branch
        .transaction()
        .retract(
            the!("person/manager")
                .of(entity("carol"))
                .is(entity("alice")),
        )
        .assert(the!("person/manager").of(entity("carol")).is(entity("bob")))
        .retract(the!("person/manager").of(entity("alice")).is(entity("bob")))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = graph.poll(&operator).await?.expect("cut");
    assert_eq!(graph.results().nodes.len(), 1);
    assert_eq!(delta.nodes_removed.len(), 2);
    assert_eq!(graph.recomputes(), initial);
    assert_eq!(graph.retained_subscriptions(), 3);
    assert!(graph.poll(&operator).await?.is_none());
    Ok(())
}

fn issue_query(required: bool) -> GraphQuery {
    let issue: ConceptDescriptor = serde_json::from_value(json!({"with": {
        "title": {"the": "issue/title", "as": "text:"},
        "assignee": {"the": "issue/assignee", "as": "entity:", "optional": !required},
        "comment": {"the": "issue/comment", "as": "entity:", "pick": "all:", "optional": true}
    }}))
    .unwrap();
    let person: ConceptDescriptor = serde_json::from_value(json!({"with": {
        "name": {"the": "person/name", "as": "text:"}
    }}))
    .unwrap();
    let comment: ConceptDescriptor = serde_json::from_value(json!({"with": {
        "body": {"the": "comment/body", "as": "text:"},
        "author": {"the": "comment/author", "as": "entity:"}
    }}))
    .unwrap();
    let edge = |target: &str| GraphEdge {
        target: target.into(),
        matching: BTreeMap::new(),
        shaping: BTreeMap::new(),
    };
    GraphQuery {
        root: ConceptQuery {
            predicate: issue.clone(),
            terms: subject(Term::var("issue")),
        },
        shape: "issue".into(),
        schema: GraphSchema {
            shapes: BTreeMap::from([
                (
                    "issue".into(),
                    GraphShape {
                        concept: issue,
                        edges: BTreeMap::from([
                            ("assignee".into(), edge("person")),
                            ("comment".into(), edge("comment")),
                        ]),
                    },
                ),
                (
                    "person".into(),
                    GraphShape {
                        concept: person,
                        edges: BTreeMap::new(),
                    },
                ),
                (
                    "comment".into(),
                    GraphShape {
                        concept: comment,
                        edges: BTreeMap::from([("author".into(), edge("person"))]),
                    },
                ),
            ]),
        },
    }
}

#[dialog_common::test]
async fn graph_keeps_incomplete_nested_bags_and_empty_many_without_dropping_parents()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .transaction()
        .assert(
            the!("issue/title")
                .of(entity("issue1"))
                .is("One".to_string()),
        )
        .assert(
            the!("issue/title")
                .of(entity("issue2"))
                .is("Two".to_string()),
        )
        .assert(
            the!("issue/assignee")
                .of(entity("issue1"))
                .is(entity("alice")),
        )
        .assert(
            the!("issue/assignee")
                .of(entity("issue2"))
                .is(entity("missing")),
        )
        .assert(
            the!("person/name")
                .of(entity("alice"))
                .is("Alice".to_string()),
        )
        .assert(the!("issue/comment").of(entity("issue1")).is(entity("c1")))
        .assert(the!("issue/comment").of(entity("issue1")).is(entity("c2")))
        .assert(the!("comment/body").of(entity("c1")).is("Yes".to_string()))
        .assert(the!("comment/author").of(entity("c1")).is(entity("alice")))
        .assert(
            the!("comment/body")
                .of(entity("c2"))
                .is("Incomplete".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let query = issue_query(false);
    let mut graph = branch.subscribe_graph(query.clone())?;
    graph.poll(&operator).await?;
    assert_eq!(graph.results().roots.len(), 2);
    assert_eq!(
        graph.results().nodes[&entity("issue2")]["issue"]["comment"],
        Vec::<Value>::new()
    );
    let bag = &graph.results().nodes[&entity("c2")]["comment"];
    assert!(!bag.contains_key("author"));
    let issues = check_bag(&query.schema.shapes["comment"].concept, bag);
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].field, "author");
    assert_eq!(
        graph.results().nodes[&entity("alice")].len(),
        1,
        "shared author normalized once"
    );
    let before = graph.recomputes();
    branch
        .transaction()
        .retract(
            the!("person/name")
                .of(entity("alice"))
                .is("Alice".to_string()),
        )
        .assert(
            the!("person/name")
                .of(entity("alice"))
                .is("Alicia".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = graph.poll(&operator).await?.expect("shared rename");
    assert_eq!(delta.fields.len(), 1);
    assert_eq!(graph.recomputes(), before);
    let required = branch
        .query()
        .graph(issue_query(true))
        .perform(&operator)
        .await?;
    assert_eq!(
        required.roots,
        vec![entity("issue1")],
        "same check excludes a nonconforming required root target"
    );
    assert_eq!(
        branch.query().graph(query).perform(&operator).await?,
        *graph.results()
    );
    Ok(())
}

#[dialog_common::test]
async fn graph_constraints_distinguish_matching_and_shaping_and_read_transaction_writes()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let transaction = branch
        .transaction()
        .assert(
            the!("issue/title")
                .of(entity("issue1"))
                .is("One".to_string()),
        )
        .assert(
            the!("issue/assignee")
                .of(entity("issue1"))
                .is(entity("alice")),
        )
        .assert(
            the!("person/name")
                .of(entity("alice"))
                .is("Alice".to_string()),
        );
    let mut query = issue_query(false);
    query
        .schema
        .shapes
        .get_mut("issue")
        .unwrap()
        .edges
        .get_mut("assignee")
        .unwrap()
        .shaping
        .insert("name".into(), Value::String("Bob".into()));
    let result = transaction
        .query()
        .graph(query.clone())
        .perform(&operator)
        .await?;
    assert_eq!(
        result.roots,
        vec![entity("issue1")],
        "shaping never removes root"
    );
    assert_eq!(result.nodes.len(), 1);
    let edge = query
        .schema
        .shapes
        .get_mut("issue")
        .unwrap()
        .edges
        .get_mut("assignee")
        .unwrap();
    edge.shaping.clear();
    edge.matching
        .insert("name".into(), Value::String("Bob".into()));
    assert!(
        transaction
            .query()
            .graph(query.clone())
            .perform(&operator)
            .await?
            .roots
            .is_empty()
    );
    query
        .schema
        .shapes
        .get_mut("issue")
        .unwrap()
        .edges
        .get_mut("assignee")
        .unwrap()
        .matching
        .insert("name".into(), Value::String("Alice".into()));
    assert_eq!(
        transaction
            .query()
            .graph(query.clone())
            .perform(&operator)
            .await?
            .roots
            .len(),
        1
    );
    assert!(
        branch
            .query()
            .graph(query)
            .perform(&operator)
            .await?
            .roots
            .is_empty(),
        "pending facts are transaction-local"
    );
    Ok(())
}

#[dialog_common::test]
async fn graph_reads_derived_fields_and_newly_reachable_entities() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let query = issue_query(true);
    let descriptor: dialog_query::DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": query.schema.shapes["person"].concept,
        "when": [{"assert": {"with": {"name": {"the": "person/stated-name", "as": "text:"}}},
                  "where": {"this": {"?": {"name": "this"}}, "name": {"?": {"name": "name"}}}}]
    }))?;
    branch
        .transaction()
        .assert(descriptor.compile()?)
        .assert(
            the!("issue/title")
                .of(entity("issue1"))
                .is("One".to_string()),
        )
        .assert(
            the!("issue/assignee")
                .of(entity("issue1"))
                .is(entity("alice")),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let mut graph = branch.subscribe_graph(query)?;
    graph.poll(&operator).await?;
    assert!(graph.results().roots.is_empty());
    // The previously empty target read stays demanded even though it rejected
    // the root: adding the derived name must bring the root into the graph.
    branch
        .transaction()
        .assert(
            the!("person/stated-name")
                .of(entity("alice"))
                .is("Alice".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    graph
        .poll(&operator)
        .await?
        .expect("newly conforming target");
    assert_eq!(graph.results().roots, vec![entity("issue1")]);
    assert_eq!(
        graph.results().nodes[&entity("alice")]["person"]["name"],
        vec![Value::String("Alice".into())]
    );
    branch
        .transaction()
        .retract(
            the!("issue/assignee")
                .of(entity("issue1"))
                .is(entity("alice")),
        )
        .assert(
            the!("issue/assignee")
                .of(entity("issue1"))
                .is(entity("bob")),
        )
        .assert(
            the!("person/stated-name")
                .of(entity("bob"))
                .is("Bob".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = graph.poll(&operator).await?.expect("edge move");
    assert!(delta.nodes_added.contains_key(&entity("bob")));
    assert!(delta.nodes_removed.contains_key(&entity("alice")));
    Ok(())
}

#[dialog_common::test]
async fn graph_shaping_constraints_are_in_the_recorded_scan_demand() -> anyhow::Result<()> {
    use dialog_artifacts::{Artifact, EntityKey, ValueKey};
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .transaction()
        .assert(
            the!("issue/title")
                .of(entity("issue1"))
                .is("One".to_string()),
        )
        .assert(
            the!("issue/assignee")
                .of(entity("issue1"))
                .is(entity("alice")),
        )
        .assert(
            the!("person/name")
                .of(entity("alice"))
                .is("Alice".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let mut query = issue_query(false);
    // All-pick equality can go directly into the scan. Choosing picks must
    // still elect their winner before comparing it, as ordinary queries do.
    query.schema.shapes.get_mut("person").unwrap().concept =
        serde_json::from_value(json!({"with": {
            "name": {"the": "person/name", "as": "text:", "pick": "all:"}
        }}))?;
    query
        .schema
        .shapes
        .get_mut("issue")
        .unwrap()
        .edges
        .get_mut("assignee")
        .unwrap()
        .shaping
        .insert("name".into(), Value::String("Bob".into()));
    let mut graph = branch.subscribe_graph(query)?;
    graph.poll(&operator).await?;
    let probe = graph
        .fields
        .iter()
        .find(|((_, shape, field, constraint), _)| {
            shape == "person" && field == "name" && constraint != "null"
        })
        .expect("constrained probe")
        .1;
    let demand = probe.demand();
    let manifest = demand.manifest().expect("recorded manifest");
    let alice = Artifact {
        the: "person/name".parse()?,
        of: entity("alice"),
        is: Value::String("Alice".into()),
        cause: None,
    };
    let bob = Artifact {
        is: Value::String("Bob".into()),
        ..alice.clone()
    };
    assert!(!demand.covers(&ValueKey::from_artifact(&alice, &manifest).into_key()));
    assert!(!demand.covers(&EntityKey::from_artifact(&alice, &manifest).into_key()));
    assert!(
        demand.covers(&ValueKey::from_artifact(&bob, &manifest).into_key())
            || demand.covers(&EntityKey::from_artifact(&bob, &manifest).into_key())
    );
    Ok(())
}
