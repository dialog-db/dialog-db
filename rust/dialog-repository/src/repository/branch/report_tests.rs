//! A query reports the rules it left out, as it runs.
//!
//! A branch holds a rule deriving `doc/tag` from `doc/label`, and bodies
//! another writer stored under the same index that do not compile here.
//! A query of `doc/tag` answers through the rule that compiles and
//! reports each one that does not, with why, before its first row; a
//! query that reaches none of them reports nothing. Run on the volatile
//! and the filesystem providers.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::Result;
use dialog_corpus::{Changes, Entity, Update as _, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, PeerSpace, Session};
use dialog_query::rule::DeductiveRuleDescriptor;
use dialog_query::types::Any;
use dialog_query::{ConceptDescriptor, ConceptQuery, DeductiveRule, Parameters, Term};
use futures_util::StreamExt as _;
use serde_json::json;

#[cfg(not(target_arch = "wasm32"))]
use crate::helpers::filesystem_session_with_peer;
use crate::helpers::{arrive, test_repo};
use crate::{Branch, Entry, RuleError};

/// `doc/tag(x) := label :- doc/label(x) = label`.
fn tag_rule() -> Result<DeductiveRule> {
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": { "with": { "tag": { "the": "doc/tag", "as": "text:" } } },
        "when": [{
            "assert": { "with": { "label": { "the": "doc/label", "as": "text:" } } },
            "where": {
                "this": { "?": { "name": "this" } },
                "label": { "?": { "name": "tag" } }
            }
        }]
    }))?;
    Ok(descriptor.compile()?)
}

/// `doc/tag` of `?this`, read as text.
fn tags() -> Result<ConceptQuery> {
    concept_query("doc/tag", "tag")
}

/// `doc/title` of `?this`, read as text: a relation no rule derives.
fn titles() -> Result<ConceptQuery> {
    concept_query("doc/title", "title")
}

fn concept_query(the: &str, name: &str) -> Result<ConceptQuery> {
    let predicate: ConceptDescriptor = serde_json::from_value(json!({ "with": {
        name: { "the": the, "as": "text:" }
    }}))?;
    let mut terms = Parameters::new();
    terms.insert("this".to_string(), Term::<Any>::var("this"));
    terms.insert(name.to_string(), Term::<Any>::var(name));
    Ok(ConceptQuery { predicate, terms })
}

/// The rule index entity of `doc/tag`.
fn on_tag() -> Entity {
    "on:doc/tag".parse().expect("an index entity")
}

/// The rule bodies another writer stored under the index of `doc/tag`
/// that do not compile here.
fn broken() -> Entity {
    "rule:broken".parse().expect("a rule entity")
}

/// A branch holding three documents labelled, the rule deriving their
/// tags, and a body under the same index that is no rule.
async fn seeded<S: PeerSpace>(session: &Peer<S, Session>, peer: &Peer<S>) -> Result<Branch> {
    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    let mut changes = Changes::new();
    for (doc, label) in [("doc:1", "draft"), ("doc:2", "final"), ("doc:3", "later")] {
        changes.associate(
            "doc/label".parse()?,
            doc.parse()?,
            Value::String(label.into()),
            dialog_corpus::Cardinality::Many,
        );
    }
    changes.associate(
        "doc/title".parse()?,
        "doc:1".parse()?,
        Value::String("Notes".into()),
        dialog_corpus::Cardinality::Many,
    );
    branch
        .transaction()
        .assert(tag_rule()?)
        .assert(changes)
        .commit()
        .publish()
        .perform(session)
        .await?;
    // A commit refuses a body that is no rule, so the broken one arrives
    // as another writer's data does.
    let mut foreign = Changes::new();
    foreign.associate(
        "dialog.rule/derives".parse()?,
        broken(),
        Value::Entity(on_tag()),
        dialog_corpus::Cardinality::Many,
    );
    foreign.associate(
        "dialog.rule/source".parse()?,
        broken(),
        Value::Bytes(vec![0xff, 0x00]),
        dialog_corpus::Cardinality::Many,
    );
    arrive(&branch, session, foreign).await?;
    branch.refresh(session).await?;
    Ok(branch)
}

/// Whether `entries` reports the broken body, left out of `doc/tag`
/// as undecodable.
fn reports_broken(entries: &[Entry]) -> bool {
    entries.iter().any(|entry| {
        matches!(
            entry,
            Entry::RuleSkipped { rule, concludes, reason: RuleError::Undecodable { .. } }
                if *rule == broken() && *concludes == on_tag()
        )
    })
}

/// A query of `doc/tag` reports nothing before its rows are first
/// polled, since it has not run; it reports the body it left out by its
/// first row, and answers through the rule that compiles; the report
/// reads the same between rows and after the last.
async fn reported_before_the_first_row<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    let branch = seeded(session, peer).await?;
    let layer = branch.query();
    let mut answer = Box::pin(layer.select(tags()?).perform(session));
    let report = answer.report();
    assert!(
        report.is_empty(),
        "read before the rows are first polled, the query has not run: {:?}",
        report.entries()
    );
    let first = answer.next().await.transpose()?;
    assert!(first.is_some(), "the rule that compiles derives");
    let entries = report.entries();
    assert!(
        reports_broken(&entries),
        "reported by the time the first row arrives: {entries:?}"
    );
    assert_eq!(entries.len(), 1, "{entries:?}");
    let mut rows = 1;
    while let Some(row) = answer.next().await {
        row?;
        rows += 1;
        assert_eq!(report.entries(), entries, "nothing else is left out");
    }
    assert_eq!(rows, 3, "the three documents' tags");
    assert_eq!(report.entries(), entries);
    Ok(())
}

/// A query of a relation no rule derives reaches none of the rules, and
/// reports nothing.
async fn not_reported_out_of_reach<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    let branch = seeded(session, peer).await?;
    let (rows, report) = branch
        .query()
        .select(titles()?)
        .perform(session)
        .try_report()
        .await?;
    assert_eq!(rows.len(), 1);
    assert!(report.is_empty(), "{:?}", report.entries());
    Ok(())
}

/// The report's own stream delivers the entry; dropping the query after
/// its first row loses nothing reported, and ends the report's stream.
/// A second query, served the cached rule set, reports the entry again.
async fn streamed_and_kept<S: PeerSpace>(session: &Peer<S, Session>, peer: &Peer<S>) -> Result<()> {
    let branch = seeded(session, peer).await?;
    let layer = branch.query();
    let mut answer = Box::pin(layer.select(tags()?).perform(session));
    let report = answer.report();
    let mut entries = report.stream();
    answer.next().await.transpose()?;
    drop(answer);
    let streamed: Vec<Entry> = entries.by_ref().collect().await;
    assert!(reports_broken(&streamed), "{streamed:?}");
    assert_eq!(
        streamed,
        report.entries(),
        "dropping the query loses nothing"
    );

    let (rows, again) = branch
        .query()
        .select(tags()?)
        .perform(session)
        .try_report()
        .await?;
    assert_eq!(rows.len(), 3);
    assert!(
        reports_broken(&again.entries()),
        "the cached rule set reports what it left out: {:?}",
        again.entries()
    );
    Ok(())
}

/// A transaction's own query reports what it left out as a branch
/// query does.
async fn transaction_query_reports<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    let branch = seeded(session, peer).await?;
    let transaction = branch.transaction();
    let (rows, report) = transaction
        .query()
        .select(tags()?)
        .perform(session)
        .try_report()
        .await?;
    assert_eq!(rows.len(), 3);
    assert!(reports_broken(&report.entries()), "{:?}", report.entries());
    Ok(())
}

/// A rule set aside for closing a cycle is reported as a cycle.
async fn cycle_reported<S: PeerSpace>(session: &Peer<S, Session>, peer: &Peer<S>) -> Result<()> {
    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    // `test/p(x) := base :- test/base(x) = base, unless test/q(x) = true`.
    let negating: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": { "with": { "p": { "the": "test/p", "as": "boolean:" } } },
        "when": [{
            "assert": { "with": { "base": { "the": "test/base", "as": "boolean:" } } },
            "where": { "this": { "?": { "name": "this" } }, "base": { "?": { "name": "p" } } }
        }],
        "unless": [{
            "assert": { "with": { "q": { "the": "test/q", "as": "boolean:" } } },
            "where": { "this": { "?": { "name": "this" } }, "q": true }
        }]
    }))?;
    // `test/q(x) := p :- test/p(x) = p`.
    let closing: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": { "with": { "q": { "the": "test/q", "as": "boolean:" } } },
        "when": [{
            "assert": { "with": { "p": { "the": "test/p", "as": "boolean:" } } },
            "where": { "this": { "?": { "name": "this" } }, "p": { "?": { "name": "q" } } }
        }]
    }))?;
    let closing = closing.compile()?;
    for rule in [negating.compile()?, closing.clone()] {
        branch
            .transaction()
            .assert(rule)
            .commit()
            .publish()
            .perform(session)
            .await?;
    }
    branch
        .transaction()
        .assert(
            dialog_query::the!("test/base")
                .of("id:alice".parse::<Entity>()?)
                .is(true),
        )
        .commit()
        .publish()
        .perform(session)
        .await?;
    branch.refresh(session).await?;

    let predicate: ConceptDescriptor = serde_json::from_value(json!({ "with": {
        "p": { "the": "test/p", "as": "boolean:" }
    }}))?;
    let mut terms = Parameters::new();
    terms.insert("this".to_string(), Term::<Any>::var("this"));
    terms.insert("p".to_string(), Term::<Any>::var("p"));
    let (_, report) = branch
        .query()
        .select(ConceptQuery { predicate, terms })
        .perform(session)
        .try_report()
        .await?;
    let entries = report.entries();
    assert!(
        entries.iter().any(|entry| matches!(
            entry,
            Entry::RuleSkipped { rule, reason: RuleError::Cycle { .. }, .. }
                if *rule == closing.this()
        )),
        "{entries:?}"
    );
    Ok(())
}

/// `doc/<head>(x) := value :- doc/<body>(x) = value`: a rule whose
/// premise is a concept another rule may derive.
fn relay(head: &str, body: &str) -> Result<DeductiveRule> {
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": { "with": { "value": { "the": format!("doc/{head}"), "as": "text:" } } },
        "when": [{
            "assert": { "with": { "read": { "the": format!("doc/{body}"), "as": "text:" } } },
            "where": {
                "this": { "?": { "name": "this" } },
                "read": { "?": { "name": "value" } }
            }
        }]
    }))?;
    Ok(descriptor.compile()?)
}

/// A body under the index of `doc/other`, a relation the chain never
/// reads, that is no rule.
fn unrelated() -> Entity {
    "rule:unrelated".parse().expect("a rule entity")
}

/// The seeded branch, with `doc/badge` relayed from `doc/tag` and
/// `doc/medal` from `doc/badge`, and a body that is no rule under the
/// index of `doc/other`.
async fn chained<S: PeerSpace>(session: &Peer<S, Session>, peer: &Peer<S>) -> Result<Branch> {
    let branch = seeded(session, peer).await?;
    branch
        .transaction()
        .assert(relay("badge", "tag")?)
        .assert(relay("medal", "badge")?)
        .commit()
        .publish()
        .perform(session)
        .await?;
    let mut changes = Changes::new();
    changes.associate(
        "dialog.rule/derives".parse()?,
        unrelated(),
        Value::Entity("on:doc/other".parse()?),
        dialog_corpus::Cardinality::Many,
    );
    changes.associate(
        "dialog.rule/source".parse()?,
        unrelated(),
        Value::Bytes(vec![0xfe, 0x01]),
        dialog_corpus::Cardinality::Many,
    );
    arrive(&branch, session, changes).await?;
    branch.refresh(session).await?;
    Ok(branch)
}

/// A query of `doc/badge`, whose rule reads `doc/tag`, and of
/// `doc/medal`, two levels up, reports the body left out of `doc/tag`,
/// on the first run and on a second one served the cached rule sets;
/// the body under `doc/other`, which neither reaches, is not reported.
async fn reported_when_reached_indirectly<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    let branch = chained(session, peer).await?;
    for (relation, levels) in [("badge", "one level"), ("medal", "two levels")] {
        for run in ["first run", "cached run"] {
            let (rows, report) = branch
                .query()
                .select(concept_query(&format!("doc/{relation}"), "value")?)
                .perform(session)
                .try_report()
                .await?;
            let entries = report.entries();
            assert_eq!(rows.len(), 3, "{levels}, {run}: the three documents");
            assert!(
                reports_broken(&entries),
                "{levels}, {run}: the body left out of doc/tag is reported: {entries:?}"
            );
            assert!(
                !entries.iter().any(|entry| matches!(
                    entry,
                    Entry::RuleSkipped { rule, .. } if *rule == unrelated()
                )),
                "{levels}, {run}: a body no concept of the query reads is not reported: {entries:?}"
            );
            assert_eq!(entries.len(), 1, "{levels}, {run}: {entries:?}");
        }
    }
    Ok(())
}

/// Every case, on one provider.
macro_rules! cases {
    ($provider:ident, $root:ident = $make:expr, $session:expr) => {
        mod $provider {
            use super::*;

            #[dialog_common::test]
            async fn it_reports_a_skipped_rule_before_the_first_row() -> Result<()> {
                let $root = $make;
                let (session, peer) = $session;
                reported_before_the_first_row(&session, &peer).await
            }

            #[dialog_common::test]
            async fn it_reports_nothing_a_query_does_not_reach() -> Result<()> {
                let $root = $make;
                let (session, peer) = $session;
                not_reported_out_of_reach(&session, &peer).await
            }

            #[dialog_common::test]
            async fn it_streams_the_report_and_keeps_it_when_the_query_is_dropped() -> Result<()> {
                let $root = $make;
                let (session, peer) = $session;
                streamed_and_kept(&session, &peer).await
            }

            #[dialog_common::test]
            async fn it_reports_what_a_transaction_query_left_out() -> Result<()> {
                let $root = $make;
                let (session, peer) = $session;
                transaction_query_reports(&session, &peer).await
            }

            #[dialog_common::test]
            async fn it_reports_a_rule_set_aside_for_a_cycle() -> Result<()> {
                let $root = $make;
                let (session, peer) = $session;
                cycle_reported(&session, &peer).await
            }

            #[dialog_common::test]
            async fn it_reports_a_skipped_rule_the_query_reaches_indirectly() -> Result<()> {
                let $root = $make;
                let (session, peer) = $session;
                reported_when_reached_indirectly(&session, &peer).await
            }
        }
    };
}

// The volatile provider needs no root.
cases!(volatile, _root = (), test_session_with_peer().await);

// Native only: the filesystem provider is the native one. In the browser
// a branch is kept in IndexedDB and OPFS, and the web run of the
// `volatile` cases pins the same scenarios there.
#[cfg(not(target_arch = "wasm32"))]
cases!(
    filesystem,
    root = tempfile::tempdir()?,
    filesystem_session_with_peer(root.path()).await?
);
