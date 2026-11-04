//! A segment: one window of a sorted list, as a rule whose body reads a model
//! and compares its order key with constant bounds. These tests read the shape
//! that a virtualized display reads, and count the rows that each one takes
//! from the store.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use dialog_artifacts::{Entity, Value, ValueDataType};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, Session};
use dialog_query::attribute::AttributeStatement;
use dialog_query::formula::number::Numeric;
use dialog_query::rule::DeductiveRuleDescriptor;
use dialog_query::type_system::Type as Kind;
use dialog_query::{
    Cardinality, ConceptConclusion, ConceptDescriptor, ConceptQuery, DeductiveRule, Parameters,
    Term,
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

/// A segment with no filter starts from the range of its order key, and not from
/// each row of each attribute of the model.
#[dialog_common::test]
async fn it_scans_the_range_of_the_order_key_when_a_segment_has_no_filter() -> Result<()> {
    let (branch, operator) = opened(5, 200).await?;
    let segment = Segment {
        channel: None,
        lower: Some(150),
        upper: Some(170),
        fields: 3,
    };
    let (rows, read) = first_read(&branch, &operator, segment).await?;
    let mut expected: Vec<u64> = (0..5).flat_map(|_| 150..170).collect();
    expected.sort_unstable();
    assert_eq!(numbers(&rows)?, expected);
    assert!(
        read <= 4 * 100,
        "the 100 rows of the range, a winner for each, and 2 more fields for each, got {read}"
    );
    Ok(())
}

/// One more claim of `number` for `this`, beside the claims of its cell.
async fn claim(branch: &Branch, operator: &Operator, this: &Entity, number: u64) -> Result<()> {
    branch
        .transaction()
        .assert(AttributeStatement {
            the: dialog_query::the!("chat.message/number"),
            of: this.clone(),
            is: number.into(),
            cause: None,
            cardinality: Some(Cardinality::Many),
            pick: None,
        })
        .commit()
        .publish()
        .perform(operator)
        .await?;
    Ok(())
}

/// A range finds claims, and a field under `last` reads the winner of the cell. A
/// claim in the range that lost its cell does not match, and a winner in the range
/// matches whatever else the cell holds.
#[dialog_common::test]
async fn it_reads_the_winner_of_a_cell_through_a_range() -> Result<()> {
    let (branch, operator) = opened(1, 3).await?;
    let lost = message(0, 0)?;
    let won = message(0, 1)?;
    claim(&branch, &operator, &lost, 160).await?;
    claim(&branch, &operator, &lost, 5).await?;
    claim(&branch, &operator, &won, 5).await?;
    claim(&branch, &operator, &won, 160).await?;
    let segment = Segment {
        channel: None,
        lower: Some(150),
        upper: Some(170),
        fields: 3,
    };
    let (rows, _) = first_read(&branch, &operator, segment).await?;
    let entities: Vec<Entity> = rows.iter().map(|row| row.entity().clone()).collect();
    assert_eq!(entities, vec![won]);
    assert_eq!(numbers(&rows)?, vec![160]);
    Ok(())
}

/// The rule of a segment whose body reads `number` as `field`, with the premise
/// and the head that a test names.
fn rule_over(premise: Json, lower: u64, upper: u64) -> Result<DeductiveRule> {
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": { "with": { "number": { "the": "test.segment/number", "as": "natural:" } } },
        "when": [
            premise,
            { "assert": ">=", "where": { "of": { "?": { "name": "number" } }, "with": lower } },
            { "assert": "<", "where": { "of": { "?": { "name": "number" } }, "with": upper } }
        ],
    }))?;
    Ok(descriptor.compile()?)
}

/// The query of the head of [`rule_over`].
fn query_over() -> Result<ConceptQuery> {
    let predicate: ConceptDescriptor = serde_json::from_value(json!({
        "with": { "number": { "the": "test.segment/number", "as": "natural:" } }
    }))?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("number".into(), Term::var("number"));
    Ok(ConceptQuery { predicate, terms })
}

/// A pick that elects by value reads every claim of a cell to choose one, so a
/// range must not reach its scan: `max` of 5 and 160 is 160, and a scan of `[0, 10)`
/// alone would choose 5.
#[dialog_common::test]
async fn it_leaves_a_field_under_max_to_the_comparison() -> Result<()> {
    let (branch, operator) = opened(1, 1).await?;
    let this = message(0, 0)?;
    claim(&branch, &operator, &this, 5).await?;
    claim(&branch, &operator, &this, 160).await?;
    let premise = json!({
        "assert": { "with": {
            "number": { "the": "chat.message/number", "as": "natural:", "pick": "max:" }
        }},
        "where": {
            "this": { "?": { "name": "this" } },
            "number": { "?": { "name": "number" } }
        }
    });
    let inside = branch
        .query()
        .with(rule_over(premise.clone(), 150, 170)?)
        .subscribe(query_over()?);
    let outside = branch
        .query()
        .with(rule_over(premise, 1, 10)?)
        .subscribe(query_over()?);
    for (mut subscription, expected) in [(inside, vec![160]), (outside, vec![])] {
        let delta = subscription.poll(&operator).await?.expect("the first read");
        assert_eq!(numbers(&delta.asserted)?, expected);
    }
    Ok(())
}

/// A field that a rule derives has no scan to take the bound. The comparison still
/// filters each row, so the answer stays the same.
#[dialog_common::test]
async fn it_filters_an_order_key_that_a_rule_derives() -> Result<()> {
    let (branch, operator) = opened(1, 20).await?;
    let derive: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": { "with": { "rank": { "the": "chat.derived/rank", "as": "natural:" } } },
        "when": [{
            "assert": { "with": { "number": { "the": "chat.message/number", "as": "natural:" } } },
            "where": {
                "this": { "?": { "name": "this" } },
                "number": { "?": { "name": "rank" } }
            }
        }],
    }))?;
    let premise = json!({
        "assert": { "with": { "rank": { "the": "chat.derived/rank", "as": "natural:" } } },
        "where": {
            "this": { "?": { "name": "this" } },
            "rank": { "?": { "name": "number" } }
        }
    });
    let mut subscription = branch
        .query()
        .with(derive.compile()?)
        .with(rule_over(premise, 5, 8)?)
        .subscribe(query_over()?);
    let delta = subscription.poll(&operator).await?.expect("the first read");
    assert_eq!(numbers(&delta.asserted)?, vec![5, 6, 7]);
    Ok(())
}

/// A read through a range keeps a live query correct: a row that enters the range,
/// a row that leaves it, and a row that a new message adds each reach the query as
/// one change, and a new message outside the range reaches it as none.
#[dialog_common::test]
async fn it_keeps_a_live_query_correct_over_a_range() -> Result<()> {
    let (branch, operator) = opened(1, 20).await?;
    let segment = Segment {
        channel: None,
        lower: Some(5),
        upper: Some(8),
        fields: 3,
    };
    let mut subscription = branch
        .query()
        .with(segment.rule()?)
        .subscribe(segment.query()?);
    let first = subscription.poll(&operator).await?.expect("the first read");
    assert_eq!(numbers(&first.asserted)?, vec![5, 6, 7]);

    // Message 6 leaves the range, and message 10 enters it.
    let write = |of: u64, to: u64| -> Result<_> {
        Ok(dialog_query::the!("chat.message/number")
            .of(message(0, of)?)
            .is(to))
    };
    branch
        .transaction()
        .assert(write(6, 60)?)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = subscription.poll(&operator).await?.expect("a row left");
    assert_eq!(numbers(&delta.retracted)?, vec![6]);
    assert!(delta.asserted.is_empty());

    branch
        .transaction()
        .assert(write(10, 6)?)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = subscription.poll(&operator).await?.expect("a row entered");
    assert_eq!(numbers(&delta.asserted)?, vec![6]);
    assert!(delta.retracted.is_empty());

    // A new message in the range, and one outside it.
    let added = |number: u64| -> Result<_> {
        let this: Entity = format!("message:new-{number}").parse()?;
        let of: Entity = channel(0).parse()?;
        Ok(branch
            .transaction()
            .assert(
                dialog_query::the!("chat.message/channel")
                    .of(this.clone())
                    .is(of),
            )
            .assert(
                dialog_query::the!("chat.message/number")
                    .of(this.clone())
                    .is(number),
            )
            .assert(
                dialog_query::the!("chat.message/author")
                    .of(this)
                    .is("ana".to_string()),
            ))
    };
    added(7)?.commit().publish().perform(&operator).await?;
    let delta = subscription.poll(&operator).await?.expect("a new row");
    assert_eq!(numbers(&delta.asserted)?, vec![7]);

    let read = subscription.rows_read();
    added(100)?.commit().publish().perform(&operator).await?;
    assert!(subscription.poll(&operator).await?.is_none());
    assert_eq!(subscription.rows_read(), read, "the new row reads nothing");
    Ok(())
}

/// A scan of an optional field that finds nothing yields a row with no value, so a
/// range must not reach it. A row whose optional order key stands outside the range
/// and a row with no order key both fall to the comparison.
#[dialog_common::test]
async fn it_leaves_an_optional_order_key_to_the_comparison() -> Result<()> {
    let (branch, operator) = opened(1, 20).await?;
    let lone: Entity = "message:lone".parse()?;
    branch
        .transaction()
        .assert(
            dialog_query::the!("chat.message/author")
                .of(lone)
                .is("ana".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let premise = json!({
        "assert": { "with": {
            "author": { "the": "chat.message/author", "as": "text:" },
            "number": { "the": "chat.message/number", "as": "natural:", "optional": true }
        }},
        "where": {
            "this": { "?": { "name": "this" } },
            "author": { "?": { "name": "author" } },
            "number": { "?": { "name": "number" } }
        }
    });
    let mut subscription = branch
        .query()
        .with(rule_over(premise, 5, 8)?)
        .subscribe(query_over()?);
    let delta = subscription.poll(&operator).await?.expect("the first read");
    assert_eq!(numbers(&delta.asserted)?, vec![5, 6, 7]);
    Ok(())
}

/// The rule and the query of a head of one field, `number`, that reads a message
/// with the kind `kind` under `comparisons`.
fn number_rule(
    kind: Option<&str>,
    comparisons: &[(&str, Json)],
) -> Result<(DeductiveRule, ConceptQuery)> {
    let field = |the: &str| match kind {
        Some(kind) => json!({ "the": the, "as": kind }),
        None => json!({ "the": the }),
    };
    let number = json!({ "?": { "name": "number" } });
    let mut when = vec![json!({
        "assert": { "with": { "number": field("chat.message/number") } },
        "where": { "this": { "?": { "name": "this" } }, "number": number.clone() }
    })];
    for (operator, literal) in comparisons {
        when.push(json!({
            "assert": operator,
            "where": { "of": number.clone(), "with": literal }
        }));
    }
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": { "with": { "number": field("test.segment/number") } },
        "when": when,
    }))?;
    let predicate: ConceptDescriptor =
        serde_json::from_value(json!({ "with": { "number": field("test.segment/number") } }))?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("number".into(), Term::var("number"));
    Ok((descriptor.compile()?, ConceptQuery { predicate, terms }))
}

/// What a live query of one number field answers first, in a fixed text form.
async fn number_rows(
    branch: &Branch,
    operator: &Operator,
    (rule, query): (DeductiveRule, ConceptQuery),
) -> Result<Vec<String>> {
    let mut subscription = branch.query().with(rule).subscribe(query);
    let delta = subscription.poll(operator).await?.expect("the first read");
    let mut rows: Vec<String> = delta
        .asserted
        .iter()
        .map(|row| format!("{} {:?}", row.entity(), row.get::<Numeric>("number")))
        .collect();
    rows.sort();
    Ok(rows)
}

/// One more claim of `is` for the number of `this`, of any type.
async fn number_claim(branch: &Branch, operator: &Operator, this: &str, is: Value) -> Result<()> {
    branch
        .transaction()
        .assert(AttributeStatement {
            the: dialog_query::the!("chat.message/number"),
            of: this.parse()?,
            is,
            cause: None,
            cardinality: Some(Cardinality::Many),
            pick: None,
        })
        .commit()
        .publish()
        .perform(operator)
        .await?;
    Ok(())
}

/// A kind that rounds can carry a value across a bound, because the range applies
/// to the stored number and the comparison sees what the kind reads. `float:32`
/// reads 0.1 above 0.1000000001, and `float:` reads the natural 2^53 + 1 as 2^53.
/// Each row stays, as with no range.
#[dialog_common::test]
async fn it_keeps_a_row_that_a_kind_rounds_across_a_bound() -> Result<()> {
    let (branch, operator) = opened(0, 0).await?;
    number_claim(&branch, &operator, "message:a", Value::Float(0.1)).await?;
    let all = number_rows(&branch, &operator, number_rule(Some("float:32"), &[])?).await?;
    let ranged = number_rows(
        &branch,
        &operator,
        number_rule(Some("float:32"), &[(">=", json!(0.1000000001))])?,
    )
    .await?;
    assert_eq!(all.len(), 1);
    assert_eq!(ranged, all);

    let (branch, operator) = opened(0, 0).await?;
    number_claim(
        &branch,
        &operator,
        "message:a",
        Value::UnsignedInt(9_007_199_254_740_993),
    )
    .await?;
    let all = number_rows(&branch, &operator, number_rule(Some("float:"), &[])?).await?;
    let ranged = number_rows(
        &branch,
        &operator,
        number_rule(Some("float:"), &[("<=", json!(9_007_199_254_740_992.0f64))])?,
    )
    .await?;
    assert_eq!(all.len(), 1);
    assert_eq!(ranged, all);
    Ok(())
}

/// A claim found by a range counts only if it is the winner of its cell. With no
/// kind to read it through, a winner of 5.9 is not the 5 that lost to it, so
/// `<= 5` answers nothing.
#[dialog_common::test]
async fn it_does_not_answer_a_claim_that_only_reads_like_the_winner() -> Result<()> {
    let (branch, operator) = opened(0, 0).await?;
    number_claim(&branch, &operator, "message:a", Value::UnsignedInt(5)).await?;
    number_claim(&branch, &operator, "message:a", Value::Float(5.9)).await?;
    let all = number_rows(&branch, &operator, number_rule(None, &[])?).await?;
    assert_eq!(all.len(), 1);
    let ranged = number_rows(&branch, &operator, number_rule(None, &[("<=", json!(5))])?).await?;
    assert_eq!(ranged, Vec::<String>::new());
    Ok(())
}

/// A natural field reads 160 and 160.5 alike, so a cell of both is one row, as with
/// no range, though the range finds both claims.
#[dialog_common::test]
async fn it_answers_a_cell_once_when_a_range_finds_it_twice() -> Result<()> {
    let (branch, operator) = opened(0, 0).await?;
    number_claim(&branch, &operator, "message:a", Value::UnsignedInt(160)).await?;
    number_claim(&branch, &operator, "message:a", Value::Float(160.5)).await?;
    let all = number_rows(&branch, &operator, number_rule(Some("natural:"), &[])?).await?;
    let ranged = number_rows(
        &branch,
        &operator,
        number_rule(Some("natural:"), &[(">=", json!(150))])?,
    )
    .await?;
    assert_eq!(all.len(), 1);
    assert_eq!(ranged, all);
    Ok(())
}

/// Bounds that cross read no key. A live query of them answers nothing, and a later
/// write reaches it as nothing.
#[dialog_common::test]
async fn it_answers_nothing_when_the_bounds_of_a_live_query_cross() -> Result<()> {
    let (branch, operator) = opened(0, 0).await?;
    number_claim(&branch, &operator, "message:a", Value::UnsignedInt(5)).await?;
    for bounds in [
        vec![(">=", json!(5)), ("<", json!(5))],
        vec![(">=", json!(10)), ("<", json!(1))],
        vec![(">", json!(5)), ("<=", json!(5))],
    ] {
        let (rule, query) = number_rule(Some("natural:"), &bounds)?;
        let mut subscription = branch.query().with(rule).subscribe(query);
        let first = subscription.poll(&operator).await?.expect("the first read");
        assert!(first.asserted.is_empty(), "{bounds:?}");
        number_claim(&branch, &operator, "message:b", Value::UnsignedInt(7)).await?;
        assert!(subscription.poll(&operator).await?.is_none(), "{bounds:?}");
    }
    Ok(())
}

/// A tree that keys each number type in its own band cannot bound one range across
/// them, and a natural or a float field reads each band. Each row stays, on a tree
/// that a migration has not reached.
#[dialog_common::test]
async fn it_reads_each_band_of_a_tree_that_is_not_migrated() -> Result<()> {
    let (branch, operator) = opened(0, 0).await?;
    number_claim(&branch, &operator, "message:a", Value::UnsignedInt(160)).await?;
    number_claim(&branch, &operator, "message:b", Value::Float(160.5)).await?;
    number_claim(&branch, &operator, "message:c", Value::SignedInt(161)).await?;
    branch.rekey_storage(0).perform(&operator).await?;
    // A comparison with a natural literal reads an integer field as natural, with
    // or without a range, so this compares the rows and not how each is typed.
    let entities = |rows: Vec<String>| -> Vec<String> {
        rows.iter()
            .map(|row| row.split(' ').next().unwrap_or_default().to_string())
            .collect()
    };
    for kind in ["natural:", "integer:", "float:"] {
        let all = number_rows(&branch, &operator, number_rule(Some(kind), &[])?).await?;
        let ranged = number_rows(
            &branch,
            &operator,
            number_rule(Some(kind), &[(">=", json!(150))])?,
        )
        .await?;
        assert_eq!(all.len(), 3, "{kind}");
        assert_eq!(entities(ranged), entities(all), "{kind}");
    }
    Ok(())
}

/// A small generator, so that a random run replays exactly.
struct Random(u64);

impl Random {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % bound
    }
}

/// A row of a segment in a fixed text form.
fn row_text(row: &ConceptConclusion) -> String {
    format!(
        "{} {:?} {:?} {:?}",
        row.entity(),
        row.get::<Entity>("channel").ok().map(|of| of.to_string()),
        row.get::<Numeric>("number").ok(),
        row.get::<String>("author").ok()
    )
}

/// The rows of `segment`, from a read with no comparison and no filter, kept here by
/// the same test that the segment states.
async fn expected_rows(
    branch: &Branch,
    operator: &Operator,
    segment: Segment<'_>,
) -> Result<Vec<String>> {
    let whole = Segment {
        channel: None,
        lower: None,
        upper: None,
        fields: 3,
    };
    let (rows, _) = first_read(branch, operator, whole).await?;
    let of = segment
        .channel
        .map(|channel| channel.parse::<Entity>())
        .transpose()?;
    let mut kept: Vec<String> = rows
        .iter()
        .filter(|row| {
            let number = row.get::<u64>("number").expect("a natural field");
            of.as_ref()
                .is_none_or(|of| row.get::<Entity>("channel").ok().as_ref() == Some(of))
                && segment.lower.is_none_or(|lower| number >= lower)
                && segment.upper.is_none_or(|upper| number < upper)
        })
        .map(row_text)
        .collect();
    kept.sort();
    Ok(kept)
}

/// Random writes of the fields of messages, as claims, retractions, and writes that
/// succeed a cell. After each write, the live query and a fresh read of each segment
/// answer what a read with no range answers.
#[dialog_common::test]
async fn it_answers_as_a_read_with_no_range_under_random_writes() -> Result<()> {
    for seed in [1, 2, 11, 12] {
        let floats = seed > 10;
        let (branch, operator) = opened(3, 12).await?;
        let general = channel(0);
        let segments = [
            (Some(general.as_str()), Some(4), Some(6)),
            (Some(general.as_str()), Some(0), Some(40)),
            (None, Some(4), Some(6)),
            (Some(general.as_str()), Some(9), None),
            (Some(general.as_str()), None, Some(3)),
        ]
        .map(|(channel, lower, upper)| Segment {
            channel,
            lower,
            upper,
            fields: 3,
        });
        let mut live = Vec::new();
        for segment in &segments {
            let mut subscription = branch
                .query()
                .with(segment.rule()?)
                .subscribe(segment.query()?);
            let first = subscription.poll(&operator).await?.expect("the first read");
            let held: BTreeSet<String> = first.asserted.iter().map(row_text).collect();
            live.push((subscription, held));
        }
        let mut random = Random(seed);
        let mut claims: BTreeMap<(String, Entity), Vec<Value>> = BTreeMap::new();
        for step in 0..30 {
            let this = message(random.below(3) as usize, random.below(14))?;
            let (the, is): (&str, Value) = match random.below(4) {
                0 | 1 => {
                    let number = random.below(20);
                    if floats && random.below(3) == 0 {
                        ("chat.message/number", Value::Float(number as f64 + 0.5))
                    } else {
                        ("chat.message/number", Value::UnsignedInt(number.into()))
                    }
                }
                2 => (
                    "chat.message/channel",
                    Value::Entity(channel(random.below(3) as usize).parse()?),
                ),
                _ => (
                    "chat.message/author",
                    Value::String(format!("a{}", random.below(2))),
                ),
            };
            let statement = |is: Value, cardinality| AttributeStatement {
                the: the.parse().expect("an attribute"),
                of: this.clone(),
                is,
                cause: None,
                cardinality: Some(cardinality),
                pick: None,
            };
            let key = (the.to_string(), this.clone());
            let held = claims.entry(key).or_default();
            let transaction = branch.transaction();
            let transaction = match random.below(4) {
                0 if !held.is_empty() => {
                    let at = random.below(held.len() as u64) as usize;
                    transaction.retract(statement(held.remove(at), Cardinality::Many))
                }
                1 => {
                    *held = vec![is.clone()];
                    transaction.assert(statement(is, Cardinality::One))
                }
                _ => {
                    held.push(is.clone());
                    transaction.assert(statement(is, Cardinality::Many))
                }
            };
            transaction.commit().publish().perform(&operator).await?;
            for (index, segment) in segments.iter().enumerate() {
                let (subscription, rows) = &mut live[index];
                if let Some(delta) = subscription.poll(&operator).await? {
                    for row in &delta.retracted {
                        assert!(rows.remove(&row_text(row)), "seed {seed} step {step}");
                    }
                    rows.extend(delta.asserted.iter().map(row_text));
                }
                let expected = expected_rows(&branch, &operator, *segment).await?;
                let (fresh, _) = first_read(&branch, &operator, *segment).await?;
                let mut fresh: Vec<String> = fresh.iter().map(row_text).collect();
                fresh.sort();
                assert_eq!(
                    fresh, expected,
                    "seed {seed} step {step} segment {index}: fresh"
                );
                let held: Vec<String> = rows.iter().cloned().collect();
                assert_eq!(
                    held, expected,
                    "seed {seed} step {step} segment {index}: live"
                );
            }
        }
    }
    Ok(())
}

/// The channels of the tests that compare a channel with a range.
const CHANNELS: usize = 20;

/// A narrow range starts the plan: the range `[180, 184)` holds 4 rows of each
/// channel, 80 in all, and a channel holds 200. A wide range does not: `[0, 160)`
/// holds 3,200 rows, so the plan starts from the channel. Each plan answers the rows
/// of the one channel in the range.
#[dialog_common::test]
async fn it_starts_a_segment_from_whichever_scan_is_narrowest() -> Result<()> {
    let (branch, operator) = opened(CHANNELS, 200).await?;
    let general = channel(0);
    let narrow = Segment {
        channel: Some(&general),
        lower: Some(180),
        upper: Some(184),
        fields: 5,
    };
    let (rows, read) = first_read(&branch, &operator, narrow).await?;
    assert_eq!(numbers(&rows)?, (180..184).collect::<Vec<_>>());
    assert!(
        read <= 3 * 80 + 3 * 4,
        "80 entries, 80 winners, 80 channels, and 3 fields of 4 rows, got {read}"
    );

    let wide = Segment {
        channel: Some(&general),
        lower: Some(0),
        upper: Some(160),
        fields: 5,
    };
    let (rows, read) = first_read(&branch, &operator, wide).await?;
    assert_eq!(numbers(&rows)?, (0..160).collect::<Vec<_>>());
    assert!(
        read <= 6 * 200,
        "the rows of the channel, and not the 3,200 of the range, got {read}"
    );
    Ok(())
}

/// The chat at its full size: a segment of 99 rows, over 100,000 messages in 20
/// channels. Not part of the regular suite, because its fixture writes 500,000 facts:
/// `cargo test -p dialog-repository --lib it_reads_a_segment_of_a_large_chat -- --ignored --nocapture`.
#[dialog_common::test]
#[ignore]
async fn it_reads_a_segment_of_a_large_chat() -> Result<()> {
    let (branch, operator) = opened(CHANNELS, 5_000).await?;
    let general = channel(0);
    let segment = Segment {
        channel: Some(&general),
        lower: Some(4_901),
        upper: Some(5_000),
        fields: 5,
    };
    let (rows, read) = first_read(&branch, &operator, segment).await?;
    assert_eq!(numbers(&rows)?, (4_901..5_000).collect::<Vec<_>>());
    println!("a segment of 99 rows reads {read} rows of the store");
    assert!(
        read <= 3 * 1_980 + 3 * 99,
        "1,980 entries, as many winners and channels, and 3 fields of 99 rows, got {read}"
    );
    Ok(())
}

/// A live query keeps the cover of the plan that it took. A new message outside the
/// range of a segment below the top is outside that cover, so the query reads nothing.
#[dialog_common::test]
async fn it_does_no_work_for_a_new_message_outside_the_range_of_a_segment() -> Result<()> {
    let (branch, operator) = opened(CHANNELS, 200).await?;
    let general = channel(0);
    let segment = Segment {
        channel: Some(&general),
        lower: Some(180),
        upper: Some(184),
        fields: 5,
    };
    let mut subscription = branch
        .query()
        .with(segment.rule()?)
        .subscribe(segment.query()?);
    subscription.poll(&operator).await?.expect("the first read");
    let read = subscription.rows_read();
    let of: Entity = general.parse()?;
    let this: Entity = "message:new".parse()?;
    branch
        .transaction()
        .assert(
            dialog_query::the!("chat.message/channel")
                .of(this.clone())
                .is(of),
        )
        .assert(
            dialog_query::the!("chat.message/number")
                .of(this.clone())
                .is(600u64),
        )
        .assert(
            dialog_query::the!("chat.message/author")
                .of(this.clone())
                .is("ana".to_string()),
        )
        .assert(
            dialog_query::the!("chat.message/time")
                .of(this.clone())
                .is(600u64),
        )
        .assert(
            dialog_query::the!("chat.message/text")
                .of(this)
                .is("hello".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    assert!(subscription.poll(&operator).await?.is_none());
    assert_eq!(subscription.rows_read(), read);
    Ok(())
}

/// The top segment has no upper bound, so a new message of another channel with a
/// greater number lands in its cover. The query reads that one entity, and sends
/// nothing, and a new message of its own channel reaches it as one row.
#[dialog_common::test]
async fn it_reads_one_entity_for_a_new_message_of_another_channel_in_the_top_segment() -> Result<()>
{
    let (branch, operator) = opened(CHANNELS, 200).await?;
    let general = channel(0);
    let segment = Segment {
        channel: Some(&general),
        lower: Some(195),
        upper: None,
        fields: 5,
    };
    let mut subscription = branch
        .query()
        .with(segment.rule()?)
        .subscribe(segment.query()?);
    let first = subscription.poll(&operator).await?.expect("the first read");
    assert_eq!(numbers(&first.asserted)?, (195..200).collect::<Vec<_>>());
    let post = |at: usize, name: &str, number: u64| -> Result<_> {
        let of: Entity = channel(at).parse()?;
        let this: Entity = format!("message:{name}").parse()?;
        Ok(branch
            .transaction()
            .assert(
                dialog_query::the!("chat.message/channel")
                    .of(this.clone())
                    .is(of),
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
            ))
    };
    let read = subscription.rows_read();
    post(1, "other", 600)?
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    if let Some(delta) = subscription.poll(&operator).await? {
        assert!(delta.asserted.is_empty() && delta.retracted.is_empty());
    }
    let cost = subscription.rows_read() - read;
    assert!(cost <= 12, "the cells of one entity, got {cost}");

    post(0, "own", 601)?
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = subscription.poll(&operator).await?.expect("a new row");
    assert_eq!(numbers(&delta.asserted)?, vec![601]);
    Ok(())
}

/// A sized kind reads 5.5 as 5, and `<= 5` keeps what it reads, though the stored
/// number stands above the bound. The row stays, as with no range.
#[dialog_common::test]
async fn it_keeps_a_row_that_a_sized_kind_reads_into_a_range() -> Result<()> {
    let (branch, operator) = opened(0, 0).await?;
    number_claim(&branch, &operator, "message:a", Value::Float(5.5)).await?;
    let all = number_rows(&branch, &operator, number_rule(Some("natural:16"), &[])?).await?;
    assert_eq!(all.len(), 1);
    let ranged = number_rows(
        &branch,
        &operator,
        number_rule(Some("natural:16"), &[("<=", json!(5))])?,
    )
    .await?;
    assert_eq!(ranged, all);
    Ok(())
}

/// An interval on a term of a query, with no comparison in a rule, is a statement of
/// a type. It filters nothing, with or without a range path to take.
#[dialog_common::test]
async fn it_leaves_an_interval_on_a_query_term_as_a_statement_of_a_type() -> Result<()> {
    let (branch, operator) = opened(0, 0).await?;
    number_claim(&branch, &operator, "message:a", Value::UnsignedInt(5)).await?;
    number_claim(&branch, &operator, "message:b", Value::UnsignedInt(160)).await?;
    let kind = Kind::from(ValueDataType::UnsignedInt)
        .with_interval(&Value::UnsignedInt(150), true, true)
        .expect("a natural takes a bound");
    let predicate: ConceptDescriptor = serde_json::from_value(
        json!({ "with": { "number": { "the": "chat.message/number", "as": "natural:" } } }),
    )?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("number".into(), Term::var("number").with_kind(kind));
    let query = ConceptQuery { predicate, terms };
    let mut subscription = branch.query().subscribe(query);
    let first = subscription.poll(&operator).await?.expect("the first read");
    assert_eq!(first.asserted.len(), 2);
    Ok(())
}

/// A comparison under `unless` excludes the rows it holds of, so it must not narrow a
/// scan to them.
#[dialog_common::test]
async fn it_does_not_narrow_a_scan_by_a_negated_comparison() -> Result<()> {
    let (branch, operator) = opened(0, 0).await?;
    number_claim(&branch, &operator, "message:a", Value::UnsignedInt(5)).await?;
    number_claim(&branch, &operator, "message:b", Value::UnsignedInt(160)).await?;
    let field = |the: &str| json!({ "the": the, "as": "natural:" });
    let number = json!({ "?": { "name": "number" } });
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": { "with": { "number": field("test.segment/number") } },
        "when": [{
            "assert": { "with": { "number": field("chat.message/number") } },
            "where": { "this": { "?": { "name": "this" } }, "number": number.clone() }
        }],
        "unless": [{ "assert": ">=", "where": { "of": number, "with": 150 } }],
    }))?;
    let predicate: ConceptDescriptor =
        serde_json::from_value(json!({ "with": { "number": field("test.segment/number") } }))?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("number".into(), Term::var("number"));
    let rows = number_rows(
        &branch,
        &operator,
        (descriptor.compile()?, ConceptQuery { predicate, terms }),
    )
    .await?;
    assert_eq!(rows, vec!["message:a Ok(UnsignedInt(5))".to_string()]);
    Ok(())
}

/// A statement of `is` for the attribute `the` of `this`, under every pick.
fn session_fact(the: &str, this: &str, is: Value) -> Result<AttributeStatement> {
    Ok(AttributeStatement {
        the: the.parse().expect("an attribute"),
        of: this.parse()?,
        is,
        cause: None,
        cardinality: Some(Cardinality::Many),
        pick: None,
    })
}

/// A range of the value index spans every attribute, because the value leads its key.
/// A session store checks each row against the selector, as a tree does, so a fact of
/// another attribute in the range answers nothing, for a number and for a text.
#[dialog_common::test]
async fn it_does_not_answer_a_session_fact_of_another_attribute_in_a_range() -> Result<()> {
    let (branch, operator) = opened(0, 0).await?;
    let overlay = branch.overlay();
    overlay.assert(session_fact(
        "chat.message/number",
        "message:a",
        Value::UnsignedInt(1),
    )?)?;
    overlay.assert(session_fact(
        "chat.other/weight",
        "message:b",
        Value::UnsignedInt(6),
    )?)?;
    let rows = number_rows(
        &branch,
        &operator,
        number_rule(Some("natural:"), &[(">=", json!(5)), ("<", json!(7))])?,
    )
    .await?;
    assert_eq!(rows, Vec::<String>::new());

    let (branch, operator) = opened(0, 0).await?;
    let overlay = branch.overlay();
    overlay.assert(session_fact(
        "chat.message/number",
        "message:a",
        Value::String("zzz".into()),
    )?)?;
    overlay.assert(session_fact(
        "chat.other/name",
        "message:b",
        Value::String("b".into()),
    )?)?;
    let (rule, query) = number_rule(Some("text:"), &[(">=", json!("a")), ("<", json!("c"))])?;
    let mut subscription = branch.query().with(rule).subscribe(query);
    let first = subscription.poll(&operator).await?.expect("the first read");
    assert!(first.asserted.is_empty());
    Ok(())
}

/// Bounds that cross hold no key, in a session store as in a tree.
#[dialog_common::test]
async fn it_answers_nothing_when_bounds_cross_over_a_session_store() -> Result<()> {
    let (branch, operator) = opened(0, 0).await?;
    branch.overlay().assert(session_fact(
        "chat.message/number",
        "message:a",
        Value::UnsignedInt(5),
    )?)?;
    let rows = number_rows(
        &branch,
        &operator,
        number_rule(Some("natural:"), &[(">=", json!(10)), ("<", json!(1))])?,
    )
    .await?;
    assert_eq!(rows, Vec::<String>::new());
    Ok(())
}

/// On a tree with a band for each number type, a constant number is an exact key to a
/// scan and a lens to a read of a cell, so which scan starts could decide whether a row
/// answers. The choice of a start keeps the fixed order there, so a row does not depend
/// on how many rows of other channels stand in the range.
#[dialog_common::test]
async fn it_keeps_the_fixed_order_on_a_tree_that_is_not_migrated() -> Result<()> {
    let lens_row_answers = |others: u64| async move {
        let (branch, operator) = opened(0, 0).await?;
        let lens: Entity = "message:lens".parse()?;
        branch
            .transaction()
            .assert(
                dialog_query::the!("chat.message/channel")
                    .of(lens.clone())
                    .is(1.5f64),
            )
            .assert(dialog_query::the!("chat.message/number").of(lens).is(5u64))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let mut transaction = branch.transaction();
        for at in 0..300u64 {
            let this: Entity = format!("message:one-{at}").parse()?;
            transaction = transaction
                .assert(
                    dialog_query::the!("chat.message/channel")
                        .of(this.clone())
                        .is(1u64),
                )
                .assert(dialog_query::the!("chat.message/number").of(this).is(5u64));
        }
        for at in 0..others {
            let this: Entity = format!("message:other-{at}").parse()?;
            transaction = transaction
                .assert(
                    dialog_query::the!("chat.message/channel")
                        .of(this.clone())
                        .is(2u64),
                )
                .assert(
                    dialog_query::the!("chat.message/number")
                        .of(this)
                        .is(5u64 + at % 3),
                );
        }
        transaction.commit().publish().perform(&operator).await?;
        branch.rekey_storage(0).perform(&operator).await?;

        let field = |the: &str| json!({ "the": the, "as": "natural:" });
        let variable = |name: &str| json!({ "?": { "name": name } });
        let head = json!({ "with": {
            "channel": field("test.segment/channel"),
            "number": field("test.segment/number")
        }});
        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
            "deduce": head.clone(),
            "when": [
                {
                    "assert": { "with": {
                        "channel": field("chat.message/channel"),
                        "number": field("chat.message/number")
                    }},
                    "where": {
                        "this": variable("this"),
                        "channel": variable("channel"),
                        "number": variable("number")
                    }
                },
                { "assert": "==", "where": { "this": variable("channel"), "is": 1 } },
                { "assert": ">=", "where": { "of": variable("number"), "with": 5 } },
                { "assert": "<", "where": { "of": variable("number"), "with": 6 } }
            ],
        }))?;
        let predicate: ConceptDescriptor = serde_json::from_value(head)?;
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("channel".into(), Term::var("channel"));
        terms.insert("number".into(), Term::var("number"));
        let query = ConceptQuery { predicate, terms };
        let mut subscription = branch.query().with(descriptor.compile()?).subscribe(query);
        let first = subscription.poll(&operator).await?.expect("the first read");
        anyhow::Ok(
            first
                .asserted
                .iter()
                .any(|row| row.entity().to_string() == "message:lens"),
        )
    };
    assert_eq!(lens_row_answers(0).await?, lens_row_answers(300).await?);
    Ok(())
}
