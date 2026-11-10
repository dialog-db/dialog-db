//! A log: the messages of a channel as a keyed collection of the channel,
//! where the key of each entry holds the order of its message. These tests
//! read a window of a log as a rule does: the entries of one collection
//! between two keys, each joined to its message. They count the rows that
//! each read takes from the store.
//!
//! The entity index sorts by entity, then attribute, then value, and the
//! attribute of an entry is its domain, a `/`, and its key. So the entries of
//! one log sit side by side in key order, and a comparison on the key reads
//! the entries in its range alone (see `ConceptQuery::key_bound`).

use anyhow::Result;
use dialog_corpus::position::{self, Bias, Position};
use dialog_corpus::{Entity, Relation as CorpusRelation};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, Session};
use dialog_query::attribute::Relation;
use dialog_query::rule::DeductiveRuleDescriptor;
use dialog_query::{ConceptConclusion, ConceptQuery, DeductiveRule, Output as _, Parameters, Term};
use dialog_storage::provider::storage::VolatileSpace;
use serde_json::{Map, Value as Json, json};

use crate::Branch;
use crate::helpers::test_repo;

type Operator = Peer<VolatileSpace, Session>;

/// The fields of a message, each read once per entity under `last`.
const FIELDS: [(&str, &str); 3] = [
    ("channel", "entity:"),
    ("number", "natural:"),
    ("text", "text:"),
];

/// The fields of a message, as a concept of `namespace`.
fn fields(namespace: &str) -> Json {
    let mut with = Map::new();
    for (name, kind) in FIELDS {
        with.insert(
            name.to_string(),
            json!({ "the": format!("{namespace}/{name}"), "as": kind }),
        );
    }
    json!({ "with": with })
}

/// How a log keys its entries.
#[derive(Clone, Copy, PartialEq)]
enum Keyed {
    /// A symbol: the letter `n` and the number, padded so that the keys
    /// order as the numbers do.
    Dictionary,
    /// A position, as `dialog/position` derives one for each append.
    Sequence,
}

impl Keyed {
    fn name(self) -> &'static str {
        match self {
            Keyed::Dictionary => "dictionary",
            Keyed::Sequence => "sequence",
        }
    }
}

/// The keys of the entries of one log, in the order of their numbers.
struct Keys(Vec<String>);

impl Keys {
    fn of(keyed: Keyed, count: u64) -> Result<Keys> {
        let mut keys = Vec::new();
        let mut last: Option<Position> = None;
        for number in 0..count {
            keys.push(match keyed {
                Keyed::Dictionary => format!("n{number:05}"),
                Keyed::Sequence => {
                    let bias = Bias::derive(format!("message:{number}").as_bytes());
                    let next = match &last {
                        Some(last) => position::insert(&bias, last..),
                        None => position::insert(&bias, ..),
                    };
                    let next = next.map_err(|error| anyhow::anyhow!("{error}"))?;
                    last = Some(next.clone());
                    next.as_str().to_string()
                }
            });
        }
        Ok(Keys(keys))
    }

    fn at(&self, number: u64) -> &str {
        &self.0[number as usize]
    }
}

/// The concept of a channel that a window reads: its log alone.
fn log(keyed: Keyed) -> Json {
    json!({ "with": { "log": {
        "the": { "domain": "chat.channel.log", "keyed": keyed.name() },
        "as": "entity:",
        "pick": "all:",
    } } })
}

/// One side of a window: a key, and whether the window holds it.
#[derive(Clone, Copy)]
struct Bound<'a> {
    key: &'a str,
    inclusive: bool,
}

/// A window of the log of a channel: its bounds on the key, and its channel.
#[derive(Clone, Copy)]
struct Window<'a> {
    keyed: Keyed,
    channel: Option<&'a str>,
    lower: Option<Bound<'a>>,
    upper: Option<Bound<'a>>,
}

impl Window<'_> {
    /// The rule of the window, as a renderer writes the rule of a segment:
    /// each entry of the log between the bounds, joined to its message.
    fn rule(&self) -> Result<DeductiveRule> {
        let channel = match self.channel {
            Some(channel) => json!({ "entity:": channel }),
            None => json!({ "?": { "name": "of" } }),
        };
        let mut when = vec![json!({
            "assert": log(self.keyed),
            "where": {
                "this": channel,
                "log": {
                    "the": { "?": { "name": "key" } },
                    "is": { "?": { "name": "this" } }
                }
            }
        })];
        for (bound, lower) in [(self.lower, true), (self.upper, false)] {
            let Some(bound) = bound else { continue };
            let assert = match (lower, bound.inclusive) {
                (true, true) => ">=",
                (true, false) => ">",
                (false, true) => "<=",
                (false, false) => "<",
            };
            when.push(json!({
                "assert": assert,
                "where": { "of": { "?": { "name": "key" } }, "with": { "text:": bound.key } }
            }));
        }
        let mut terms = Map::new();
        terms.insert("this".to_string(), json!({ "?": { "name": "this" } }));
        for (name, _) in FIELDS {
            terms.insert(name.to_string(), json!({ "?": { "name": name } }));
        }
        when.push(json!({ "assert": fields("chat.message"), "where": terms }));
        let deduce = fields("test.window");
        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
            "deduce": deduce,
            "when": when,
        }))?;
        Ok(descriptor.compile()?)
    }

    /// The query of the head of the rule.
    fn query(&self) -> Result<ConceptQuery> {
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        for (name, _) in FIELDS {
            terms.insert(name.into(), Term::var(name));
        }
        Ok(ConceptQuery {
            predicate: serde_json::from_value(fields("test.window"))?,
            terms,
        })
    }
}

/// The entity of a channel.
fn channel(at: usize) -> String {
    format!("channel:{at}")
}

/// The facts of one message of a channel, and its entry in the log of that
/// channel under `key`.
fn post<L: crate::Line>(
    mut transaction: crate::Transaction<L>,
    at: usize,
    name: &str,
    number: u64,
    key: &str,
) -> Result<crate::Transaction<L>> {
    let of: Entity = channel(at).parse()?;
    let this: Entity = format!("message:{at}-{name}").parse()?;
    let entry = Relation::from(CorpusRelation::try_from(format!("chat.channel.log/{key}"))?);
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
            dialog_query::the!("chat.message/text")
                .of(this.clone())
                .is("hello".to_string()),
        )
        .assert(entry.of(of).is(this));
    Ok(transaction)
}

/// A new branch of a chat: one channel for each count of `counts`, with that
/// many messages, each with its entry in the log of its channel. A channel
/// also has a name, which is a fact of the channel outside its log.
async fn opened(keyed: Keyed, counts: &[u64]) -> Result<(Branch, Operator, Keys)> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let most = counts.iter().copied().max().unwrap_or(0);
    let keys = Keys::of(keyed, most)?;
    for (at, count) in counts.iter().enumerate() {
        let of: Entity = channel(at).parse()?;
        branch
            .transaction()
            .assert(
                dialog_query::the!("chat.channel/name")
                    .of(of)
                    .is(format!("channel {at}")),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        // A few thousand messages for each commit keeps a commit small.
        for start in (0..*count).step_by(2_000) {
            let mut transaction = branch.transaction();
            for number in start..(*count).min(start + 2_000) {
                transaction = post(
                    transaction,
                    at,
                    &number.to_string(),
                    number,
                    keys.at(number),
                )?;
            }
            transaction.commit().publish().perform(&operator).await?;
        }
    }
    Ok((branch, operator, keys))
}

/// What a first read of a window reads: its rows, and how many rows of the
/// store it took to find them.
async fn first_read(
    branch: &Branch,
    operator: &Operator,
    window: Window<'_>,
) -> Result<(Vec<ConceptConclusion>, usize)> {
    let mut subscription = branch
        .query()
        .with(window.rule()?)
        .subscribe(window.query()?);
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

/// The channel and the number of each of `rows`, sorted.
fn places(rows: &[ConceptConclusion]) -> Result<Vec<(String, u64)>> {
    let mut places = rows
        .iter()
        .map(|row| {
            Ok((
                row.get::<Entity>("channel")?.to_string(),
                row.get::<u64>("number")?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    places.sort();
    Ok(places)
}

fn at_least(key: &str) -> Option<Bound<'_>> {
    Some(Bound {
        key,
        inclusive: true,
    })
}

fn below(key: &str) -> Option<Bound<'_>> {
    Some(Bound {
        key,
        inclusive: false,
    })
}

/// The most rows that a read of a window of `rows` messages takes from the
/// store. The head of the rule holds each field of a message, and its body
/// reads an `all:` collection, which can yield several rows for one entity.
/// So the query reads each attribute of the head through the rule on its own:
/// one evaluation of the body for each field, and each evaluation reads one
/// entry and each field of each message. The few rows more are the entry at
/// an exclusive bound, once for each evaluation.
fn budget(rows: usize) -> usize {
    FIELDS.len() * (1 + FIELDS.len()) * rows + 2 * FIELDS.len()
}

/// A window of 99 messages of a log of 10,000 reads the entries of the window
/// and the fields of their messages. Its cost follows the window, and not the
/// log: with no narrowing, the scan of the log alone reads 10,000 entries.
#[dialog_common::test]
async fn it_reads_a_window_of_a_log_by_its_keys() -> Result<()> {
    let (branch, operator, keys) = opened(Keyed::Dictionary, &[10_000, 300, 300]).await?;
    let general = channel(0);
    let window = Window {
        keyed: Keyed::Dictionary,
        channel: Some(&general),
        lower: at_least(keys.at(4_901)),
        upper: below(keys.at(5_000)),
    };
    let (rows, read) = first_read(&branch, &operator, window).await?;
    assert_eq!(numbers(&rows)?, (4_901..5_000).collect::<Vec<_>>());
    assert!(
        rows.iter()
            .all(|row| row.get::<Entity>("channel").unwrap().to_string() == general)
    );
    println!("a window of 99 of 10,000 entries reads {read} rows of the store");
    assert!(
        read <= budget(99),
        "a window of 99 rows reads about its rows, and not the 10,000 entries of the log, got {read}"
    );
    Ok(())
}

/// Each form of a window answers the rows that its comparisons keep: a lower
/// bound alone, an upper bound alone, both, an exclusive and an inclusive form
/// of each, and a range that holds no entry. Each read stays near its rows.
#[dialog_common::test]
async fn it_answers_each_form_of_a_window_as_its_comparisons_do() -> Result<()> {
    let (branch, operator, keys) = opened(Keyed::Dictionary, &[400, 100]).await?;
    let general = channel(0);
    let window = |lower, upper| Window {
        keyed: Keyed::Dictionary,
        channel: Some(&general),
        lower,
        upper,
    };
    let strict = |key| {
        Some(Bound {
            key,
            inclusive: false,
        })
    };
    let closed = |key| {
        Some(Bound {
            key,
            inclusive: true,
        })
    };
    for (lower, upper, expected) in [
        (closed(keys.at(390)), None, 390..400),
        (strict(keys.at(390)), None, 391..400),
        (None, strict(keys.at(12)), 0..12),
        (None, closed(keys.at(12)), 0..13),
        (closed(keys.at(100)), strict(keys.at(120)), 100..120),
        (strict(keys.at(100)), closed(keys.at(120)), 101..121),
        (closed(keys.at(50)), strict(keys.at(50)), 0..0),
        (closed(keys.at(60)), strict(keys.at(20)), 0..0),
        (closed("n99000"), None, 0..0),
    ] {
        let expected: Vec<u64> = expected.collect();
        let (rows, read) = first_read(&branch, &operator, window(lower, upper)).await?;
        assert_eq!(numbers(&rows)?, expected);
        assert!(
            read <= budget(expected.len()),
            "a window of {} rows read {read}",
            expected.len()
        );
    }

    // No bound reads the whole log, as it did.
    let (rows, _) = first_read(&branch, &operator, window(None, None)).await?;
    assert_eq!(numbers(&rows)?, (0..400).collect::<Vec<_>>());
    Ok(())
}

/// A sequence keys its entries by position. A position holds digits of its
/// member after its whole part, and the positions of a log still order as its
/// appends do, so a window between two positions reads as one between two
/// symbols does.
#[dialog_common::test]
async fn it_reads_a_window_of_a_sequence() -> Result<()> {
    let (branch, operator, keys) = opened(Keyed::Sequence, &[600, 50]).await?;
    assert!(
        keys.0.windows(2).all(|pair| pair[0] < pair[1]),
        "the positions of the appends ascend"
    );
    let general = channel(0);
    let window = Window {
        keyed: Keyed::Sequence,
        channel: Some(&general),
        lower: at_least(keys.at(500)),
        upper: below(keys.at(540)),
    };
    let (rows, read) = first_read(&branch, &operator, window).await?;
    assert_eq!(numbers(&rows)?, (500..540).collect::<Vec<_>>());
    assert!(read <= budget(40), "a window of 40 rows read {read}");
    Ok(())
}

/// With the channel free, a window answers the rows of each log between its
/// keys. The plan of the rule is free to start from the messages here, and it
/// does, so this test holds the answer and no count of the rows read.
#[dialog_common::test]
async fn it_reads_a_window_of_each_log_with_the_channel_free() -> Result<()> {
    let (branch, operator, keys) = opened(Keyed::Dictionary, &[300, 300, 150]).await?;
    let window = Window {
        keyed: Keyed::Dictionary,
        channel: None,
        lower: at_least(keys.at(148)),
        upper: below(keys.at(152)),
    };
    let (rows, _) = first_read(&branch, &operator, window).await?;
    let mut expected = Vec::new();
    for (at, count) in [300u64, 300, 150].into_iter().enumerate() {
        for number in 148..152 {
            if number < count {
                expected.push((channel(at), number));
            }
        }
    }
    expected.sort();
    assert_eq!(places(&rows)?, expected);
    Ok(())
}

/// A live query of a window keeps the range of its keys as what it demands.
/// A new entry outside the range leaves it at rest, with no row read. A new
/// entry inside the range reaches it as one row, and so does an entry that
/// leaves.
#[dialog_common::test]
async fn it_keeps_a_live_window_at_rest_for_an_entry_outside_its_keys() -> Result<()> {
    let (branch, operator, keys) = opened(Keyed::Dictionary, &[200, 200]).await?;
    let general = channel(0);
    let window = Window {
        keyed: Keyed::Dictionary,
        channel: Some(&general),
        lower: at_least(keys.at(100)),
        upper: below("n00104"),
    };
    let mut subscription = branch
        .query()
        .with(window.rule()?)
        .subscribe(window.query()?);
    let first = subscription.poll(&operator).await?.expect("the first read");
    assert_eq!(numbers(&first.asserted)?, vec![100, 101, 102, 103]);

    // A new message at the end of the same log, and one in another log
    // under a key of the window.
    let read = subscription.rows_read();
    let recomputes = subscription.recomputes();
    post(branch.transaction(), 0, "late", 200, "n00200")?
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    assert!(subscription.poll(&operator).await?.is_none());
    post(branch.transaction(), 1, "other", 900, "n00102")?
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    assert!(subscription.poll(&operator).await?.is_none());
    assert_eq!(
        subscription.rows_read(),
        read,
        "an entry outside the window reads nothing"
    );
    assert_eq!(subscription.recomputes(), recomputes);

    // A second message under a key of the window: two devices that wrote
    // the same number.
    post(branch.transaction(), 0, "twin", 102, "n00102")?
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = subscription.poll(&operator).await?.expect("a new row");
    assert_eq!(numbers(&delta.asserted)?, vec![102]);
    assert!(delta.retracted.is_empty());

    // An entry that leaves the log leaves the window.
    let of: Entity = general.parse()?;
    let gone: Entity = "message:0-101".parse()?;
    let entry = Relation::from(CorpusRelation::try_from(
        "chat.channel.log/n00101".to_string(),
    )?);
    branch
        .transaction()
        .retract(entry.of(of).is(gone))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = subscription.poll(&operator).await?.expect("a row left");
    assert_eq!(numbers(&delta.retracted)?, vec![101]);
    assert!(delta.asserted.is_empty());
    Ok(())
}

/// A read of a window merges the tree with the session overlay of its
/// snapshot. An entry that the overlay holds shows when its key is in the
/// window, and stays out when it is not, as it would once committed.
#[dialog_common::test]
async fn it_reads_a_window_through_a_session_overlay() -> Result<()> {
    let (branch, operator, keys) = opened(Keyed::Dictionary, &[50]).await?;
    let general = channel(0);
    let window = Window {
        keyed: Keyed::Dictionary,
        channel: Some(&general),
        lower: at_least(keys.at(40)),
        upper: below("n00060"),
    };
    let snapshot = branch.snapshot().expect("a snapshot");
    let of: Entity = general.parse()?;
    for (name, number, key) in [("inside", 55u64, "n00055"), ("outside", 70, "n00070")] {
        let this: Entity = format!("message:0-{name}").parse()?;
        let entry = Relation::from(CorpusRelation::try_from(format!("chat.channel.log/{key}"))?);
        let overlay = snapshot.overlay();
        overlay.assert(
            dialog_query::the!("chat.message/channel")
                .of(this.clone())
                .is(of.clone()),
        )?;
        overlay.assert(
            dialog_query::the!("chat.message/number")
                .of(this.clone())
                .is(number),
        )?;
        overlay.assert(
            dialog_query::the!("chat.message/text")
                .of(this.clone())
                .is("hello".to_string()),
        )?;
        overlay.assert(entry.of(of.clone()).is(this))?;
    }
    let rows = snapshot
        .query()
        .with(window.rule()?)
        .select(window.query()?)
        .perform(&operator)
        .try_vec()
        .await?;
    let mut expected: Vec<u64> = (40..50).collect();
    expected.push(55);
    assert_eq!(numbers(&rows)?, expected);
    Ok(())
}
