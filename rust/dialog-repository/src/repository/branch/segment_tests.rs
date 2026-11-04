//! A segment: one window of a sorted list, as a rule whose body reads a model
//! and compares its order key with constant bounds. These tests read the shape
//! that a virtualized display reads, and count the rows that each one takes
//! from the store.

use anyhow::Result;
use dialog_corpus::Entity;
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, Session};
use dialog_query::rule::DeductiveRuleDescriptor;
use dialog_query::{
    ConceptConclusion, ConceptDescriptor, ConceptQuery, DeductiveRule, Parameters, Term,
};
use dialog_storage::provider::storage::VolatileSpace;
use serde_json::{Map, Value as Json, json};

use crate::Branch;
use crate::helpers::test_repo;

type Operator = Peer<VolatileSpace, Session>;

/// The fields of a message, each read once per entity under `last`.
const FIELDS: [(&str, &str); 5] = [
    ("channel", "entity:"),
    ("number", "natural:"),
    ("author", "text:"),
    ("time", "natural:"),
    ("text", "text:"),
];

/// The first `count` fields of a message, as a concept of `namespace`.
fn fields(namespace: &str, count: usize) -> Json {
    let mut with = Map::new();
    for (name, kind) in FIELDS.iter().take(count) {
        with.insert(
            (*name).to_string(),
            json!({ "the": format!("{namespace}/{name}"), "as": kind }),
        );
    }
    json!({ "with": with })
}

/// One premise that binds a variable for each of the first `count` fields.
fn message_premise(count: usize) -> Json {
    let mut terms = Map::new();
    terms.insert("this".to_string(), json!({ "?": { "name": "this" } }));
    for (name, _) in FIELDS.iter().take(count) {
        terms.insert((*name).to_string(), json!({ "?": { "name": name } }));
    }
    json!({ "assert": fields("chat.message", count), "where": terms })
}

/// The bounds of a segment, and its filter by channel.
#[derive(Clone, Copy)]
struct Segment<'a> {
    channel: Option<&'a str>,
    lower: Option<u64>,
    upper: Option<u64>,
    fields: usize,
}

impl Segment<'_> {
    /// The rule of the segment, as a renderer writes it.
    fn rule(&self) -> Result<DeductiveRule> {
        let mut when = vec![message_premise(self.fields)];
        if let Some(channel) = self.channel {
            when.push(json!({
                "assert": "==",
                "where": {
                    "this": { "?": { "name": "channel" } },
                    "is": { "entity:": channel }
                }
            }));
        }
        if let Some(lower) = self.lower {
            when.push(json!({
                "assert": ">=",
                "where": { "of": { "?": { "name": "number" } }, "with": lower }
            }));
        }
        if let Some(upper) = self.upper {
            when.push(json!({
                "assert": "<",
                "where": { "of": { "?": { "name": "number" } }, "with": upper }
            }));
        }
        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
            "deduce": fields("test.segment", self.fields),
            "when": when,
        }))?;
        Ok(descriptor.compile()?)
    }

    /// The query of the head of the rule.
    fn query(&self) -> Result<ConceptQuery> {
        let predicate: ConceptDescriptor =
            serde_json::from_value(fields("test.segment", self.fields))?;
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        for (name, _) in FIELDS.iter().take(self.fields) {
            terms.insert((*name).into(), Term::var(*name));
        }
        Ok(ConceptQuery { predicate, terms })
    }
}

/// The entity of a message.
fn message(channel: usize, number: u64) -> Result<Entity> {
    Ok(format!("message:{channel}-{number}").parse()?)
}

/// The entity of a channel.
fn channel(channel: usize) -> String {
    format!("channel:{channel}")
}

/// `channels` channels of `count` messages each, numbered from 0 in each.
async fn chat(branch: &Branch, operator: &Operator, channels: usize, count: u64) -> Result<()> {
    for at in 0..channels {
        let of: Entity = channel(at).parse()?;
        let mut transaction = branch.transaction();
        for number in 0..count {
            let this = message(at, number)?;
            transaction = transaction
                .assert(
                    dialog_query::the!("chat.message/channel")
                        .of(this.clone())
                        .is(of.clone()),
                )
                .assert(
                    dialog_query::the!("chat.message/number")
                        .of(this.clone())
                        .is(number),
                )
                .assert(
                    dialog_query::the!("chat.message/author")
                        .of(this.clone())
                        .is("ana".to_string()),
                )
                .assert(
                    dialog_query::the!("chat.message/time")
                        .of(this.clone())
                        .is(number),
                )
                .assert(
                    dialog_query::the!("chat.message/text")
                        .of(this)
                        .is("hello".to_string()),
                );
        }
        transaction.commit().publish().perform(operator).await?;
    }
    Ok(())
}

/// What a first read of a segment reads: its rows, and how many rows of the store
/// it took to find them.
async fn first_read(
    branch: &Branch,
    operator: &Operator,
    segment: Segment<'_>,
) -> Result<(Vec<ConceptConclusion>, usize)> {
    let mut subscription = branch
        .query()
        .with(segment.rule()?)
        .subscribe(segment.query()?);
    let delta = subscription.poll(operator).await?.expect("the first read");
    Ok((delta.asserted, subscription.rows_read()))
}

/// The numbers of `rows`, sorted.
fn numbers(rows: &[ConceptConclusion]) -> Result<Vec<u64>> {
    let mut numbers = rows
        .iter()
        .map(|row| row.get::<u64>("number"))
        .collect::<Result<Vec<_>, _>>()?;
    numbers.sort_unstable();
    Ok(numbers)
}

/// A new branch of a chat.
async fn opened(channels: usize, count: u64) -> Result<(Branch, Operator)> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    chat(&branch, &operator, channels, count).await?;
    Ok((branch, operator))
}

/// The rule of a segment holds comparisons. Its body runs once whatever the
/// head names, and each row of its range comes back with each of its fields.
/// The reads grow with the fields of the head, one read of each field for each
/// message of the channel, and not with their square.
#[dialog_common::test]
async fn it_evaluates_the_body_of_a_segment_once_for_all_its_fields() -> Result<()> {
    let (branch, operator) = opened(5, 200).await?;
    let general = channel(0);
    let segment = |fields| Segment {
        channel: Some(&general),
        lower: Some(150),
        upper: Some(170),
        fields,
    };
    let (three, three_read) = first_read(&branch, &operator, segment(3)).await?;
    let (five, five_read) = first_read(&branch, &operator, segment(5)).await?;
    assert_eq!(numbers(&three)?, (150..170).collect::<Vec<_>>());
    assert_eq!(numbers(&five)?, (150..170).collect::<Vec<_>>());
    for row in &five {
        assert_eq!(row.get::<String>("author")?, "ana");
        assert_eq!(row.get::<String>("text")?, "hello");
        assert_eq!(row.get::<u64>("time")?, row.get::<u64>("number")?);
    }
    assert!(
        five_read <= 6 * 200,
        "one evaluation reads each of 5 fields and checks one winner for each message, got {five_read}"
    );
    assert!(
        three_read <= 4 * 200,
        "one evaluation reads each of 3 fields and checks one winner for each message, got {three_read}"
    );
    Ok(())
}

/// The fast path and the slow path answer alike. A stored fact of an attribute
/// of the head turns the fast path off, so the same segment reads each field
/// through the rule, one evaluation for each field.
#[dialog_common::test]
async fn it_answers_a_segment_as_one_evaluation_for_each_field_does() -> Result<()> {
    let (branch, operator) = opened(5, 200).await?;
    let general = channel(0);
    let segment = Segment {
        channel: Some(&general),
        lower: Some(150),
        upper: Some(170),
        fields: 5,
    };
    let (fast, fast_read) = first_read(&branch, &operator, segment).await?;

    // A row with a value of one field alone is no instance of the head: the
    // answer stays the same, and each attribute now has a stored source.
    let lone: Entity = "other:lone".parse()?;
    branch
        .transaction()
        .assert(
            dialog_query::the!("test.segment/text")
                .of(lone)
                .is("alone".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let (slow, slow_read) = first_read(&branch, &operator, segment).await?;

    assert_eq!(numbers(&fast)?, numbers(&slow)?);
    assert!(
        fast_read * 3 < slow_read,
        "the slow path reads the body for each field: {fast_read} against {slow_read}"
    );
    Ok(())
}
