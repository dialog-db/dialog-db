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
use dialog_corpus::{Entity, Relation as CorpusRelation, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, Session};
use dialog_query::attribute::{AttributeStatement, Relation};
use dialog_query::rule::DeductiveRuleDescriptor;
use dialog_query::{
    Cardinality, ConceptConclusion, ConceptQuery, DeductiveRule, Output as _, Parameters, Term,
};
use dialog_storage::provider::storage::VolatileSpace;
use serde_json::{Map, Value as Json, json};

use super::Subscription;
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
    /// Whether the head of the rule also holds the key of each entry.
    key_in_head: bool,
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
        let deduce = self.head();
        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
            "deduce": deduce,
            "when": when,
        }))?;
        Ok(descriptor.compile()?)
    }

    /// The head of the rule: each field of a message, and the key of its
    /// entry when the window asks for it.
    fn head(&self) -> Json {
        let mut head = fields("test.window");
        if self.key_in_head {
            head["with"]["key"] =
                json!({ "the": "test.window/key", "as": "text:", "pick": "all:" });
        }
        head
    }

    /// The query of the head of the rule.
    fn query(&self) -> Result<ConceptQuery> {
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        for (name, _) in FIELDS {
            terms.insert(name.into(), Term::var(name));
        }
        if self.key_in_head {
            terms.insert("key".into(), Term::var("key"));
        }
        Ok(ConceptQuery {
            predicate: serde_json::from_value(self.head())?,
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
/// store: one entry of the log and one row of each field for each message.
/// Each field of the head follows from the message, so the body of the rule
/// is evaluated once for all of them (see `DeductiveRule::head_determined`).
/// The two rows more are slack for an entry at an exclusive bound.
fn budget(rows: usize) -> usize {
    (1 + FIELDS.len()) * rows + 2
}

/// A window of 99 messages of a log of 10,000 reads the entries of the window
/// and the fields of their messages. Its cost follows the window, and not the
/// log: with no narrowing, the scan of the log alone reads 10,000 entries.
#[dialog_common::test]
async fn it_reads_a_window_of_a_log_by_its_keys() -> Result<()> {
    let (branch, operator, keys) = opened(Keyed::Dictionary, &[10_000, 300, 300]).await?;
    let general = channel(0);
    let window = Window {
        key_in_head: false,
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
        key_in_head: false,
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
        key_in_head: false,
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
        key_in_head: false,
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
        key_in_head: false,
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
        key_in_head: false,
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

/// A second entry of the log that holds a message of the channel `at`, under
/// `key`.
async fn held_again(
    branch: &Branch,
    operator: &Operator,
    at: usize,
    name: &str,
    key: &str,
) -> Result<()> {
    let of: Entity = channel(at).parse()?;
    let this: Entity = format!("message:{at}-{name}").parse()?;
    let entry = Relation::from(CorpusRelation::try_from(format!("chat.channel.log/{key}"))?);
    branch
        .transaction()
        .assert(entry.of(of).is(this))
        .commit()
        .publish()
        .perform(operator)
        .await?;
    Ok(())
}

/// The body of the rule of a window gives one row for each entry, so a message
/// that two entries hold comes from it twice. Each field of the head follows
/// from the message, so the two rows are one row of the head, and the window
/// answers it once. The answer equals what one evaluation for each field
/// gives: a stored fact of an attribute of the head turns the fast path off,
/// and the same window then reads each field through the rule on its own.
#[dialog_common::test]
async fn it_answers_a_window_once_for_a_message_that_two_entries_hold() -> Result<()> {
    let (branch, operator, keys) = opened(Keyed::Dictionary, &[400, 100]).await?;
    held_again(&branch, &operator, 0, "150", "n00155x").await?;
    let general = channel(0);
    let window = Window {
        key_in_head: false,
        keyed: Keyed::Dictionary,
        channel: Some(&general),
        lower: at_least(keys.at(140)),
        upper: below(keys.at(160)),
    };
    let (fast, fast_read) = first_read(&branch, &operator, window).await?;
    assert_eq!(numbers(&fast)?, (140..160).collect::<Vec<_>>());
    assert!(
        fast_read <= budget(21),
        "20 messages from 21 entries read {fast_read}"
    );

    // A row with a value of one field alone is no instance of the head: the
    // answer stays the same, and each attribute now has a stored source.
    let lone: Entity = "other:lone".parse()?;
    branch
        .transaction()
        .assert(
            dialog_query::the!("test.window/text")
                .of(lone)
                .is("alone".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let (slow, slow_read) = first_read(&branch, &operator, window).await?;
    assert_eq!(numbers(&slow)?, numbers(&fast)?);
    assert!(
        fast_read * 2 < slow_read,
        "the slow path reads the body for each field: {fast_read} against {slow_read}"
    );
    Ok(())
}

/// A head that holds the key of each entry is not determined by its entity:
/// a message that two entries hold has two keys. Each other field of the head
/// follows from the message, so the body is evaluated once for all the fields,
/// and the window answers the message once for each of its keys. The answer
/// equals what one evaluation for each field gives: a stored fact of an
/// attribute of the head turns the fast path off for the comparison.
#[dialog_common::test]
async fn it_answers_each_key_of_a_message_when_the_head_holds_the_key() -> Result<()> {
    let (branch, operator, keys) = opened(Keyed::Dictionary, &[400]).await?;
    held_again(&branch, &operator, 0, "150", "n00155x").await?;
    let general = channel(0);
    let window = Window {
        key_in_head: true,
        keyed: Keyed::Dictionary,
        channel: Some(&general),
        lower: at_least(keys.at(140)),
        upper: below(keys.at(160)),
    };
    let (rows, read) = first_read(&branch, &operator, window).await?;
    let mut expected: Vec<u64> = (140..160).collect();
    expected.push(150);
    expected.sort_unstable();
    assert_eq!(numbers(&rows)?, expected);
    let mut held: Vec<String> = rows
        .iter()
        .filter(|row| row.get::<u64>("number").unwrap() == 150)
        .map(|row| row.get::<String>("key").unwrap())
        .collect();
    held.sort();
    assert_eq!(held, vec!["n00150".to_string(), "n00155x".to_string()]);
    assert!(
        read <= budget(21),
        "21 entries of 20 messages read one entry and each field of each: {read}"
    );

    lone_field(&branch, &operator).await?;
    let (slow, slow_read) = first_read(&branch, &operator, window).await?;
    assert_eq!(entries(&slow)?, entries(&rows)?);
    assert!(
        read * 2 < slow_read,
        "the slow path reads the body for each field: {read} against {slow_read}"
    );
    Ok(())
}

/// A stored fact of one field of the head of a window, on an entity that no
/// entry holds. It is no instance of the head, so the answer of a window stays
/// the same. Each attribute of the head then has a stored source, which turns
/// the fast path of a rule off: the window reads each field through the rule
/// on its own.
async fn lone_field(branch: &Branch, operator: &Operator) -> Result<()> {
    let lone: Entity = "other:lone".parse()?;
    branch
        .transaction()
        .assert(
            dialog_query::the!("test.window/text")
                .of(lone)
                .is("alone".to_string()),
        )
        .commit()
        .publish()
        .perform(operator)
        .await?;
    Ok(())
}

/// Each of `rows` as a line of its entity, its key, and each field, sorted.
fn entries(rows: &[ConceptConclusion]) -> Result<Vec<String>> {
    let mut lines = rows
        .iter()
        .map(|row| {
            Ok(format!(
                "{} key={} channel={} number={} text={}",
                row.entity(),
                row.get::<String>("key")?,
                row.get::<Entity>("channel")?,
                row.get::<u64>("number")?,
                row.get::<String>("text")?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    lines.sort();
    Ok(lines)
}

/// The five fields of a message of a chat, each read once per entity under
/// `last`: those of [`FIELDS`], and an author and a time.
const MESSAGE: [(&str, &str); 5] = [
    ("author", "entity:"),
    ("channel", "entity:"),
    ("number", "natural:"),
    ("text", "text:"),
    ("time", "natural:"),
];

/// The rule of a segment of a list of a collection: each entry of the log of
/// `channel` from the key `lower` up to the key `upper`, which it leaves out,
/// joined to its message of five fields. The head holds the key of each entry
/// beside the five fields.
fn segment(channel: &str, lower: &str, upper: &str) -> Result<(DeductiveRule, ConceptQuery)> {
    let mut with = Map::new();
    let mut read = Map::new();
    let mut terms = Map::new();
    let mut parameters = Parameters::new();
    terms.insert("this".to_string(), json!({ "?": { "name": "this" } }));
    parameters.insert("this".into(), Term::var("this"));
    for (name, kind) in MESSAGE {
        with.insert(
            name.to_string(),
            json!({ "the": format!("test.segment/{name}"), "as": kind }),
        );
        read.insert(
            name.to_string(),
            json!({ "the": format!("chat.message/{name}"), "as": kind }),
        );
        terms.insert(name.to_string(), json!({ "?": { "name": name } }));
        parameters.insert(name.into(), Term::var(name));
    }
    with.insert(
        "key".to_string(),
        json!({ "the": "test.segment/key", "as": "text:", "pick": "all:" }),
    );
    parameters.insert("key".into(), Term::var("key"));
    let head = json!({ "with": with });
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": head,
        "when": [
            {
                "assert": log(Keyed::Dictionary),
                "where": {
                    "this": { "entity:": channel },
                    "log": {
                        "the": { "?": { "name": "key" } },
                        "is": { "?": { "name": "this" } }
                    }
                }
            },
            {
                "assert": ">=",
                "where": { "of": { "?": { "name": "key" } }, "with": { "text:": lower } }
            },
            {
                "assert": "<",
                "where": { "of": { "?": { "name": "key" } }, "with": { "text:": upper } }
            },
            { "assert": { "with": read }, "where": terms },
        ]
    }))?;
    let query = ConceptQuery {
        predicate: serde_json::from_value(head)?,
        terms: parameters,
    };
    Ok((descriptor.compile()?, query))
}

/// A segment of 20 entries of a log, whose head holds the key of each entry
/// beside the five fields of a message, reads one entry and one row for each
/// field of each message: 6 rows for each entry. With one evaluation of the
/// body for each field of the head, it read 36.
#[dialog_common::test]
async fn it_reads_one_entry_and_each_field_of_a_message_when_the_head_holds_the_key() -> Result<()>
{
    let (branch, operator, keys) = opened(Keyed::Dictionary, &[200]).await?;
    let of: Entity = channel(0).parse()?;
    let author: Entity = "person:ana".parse()?;
    let mut transaction = branch.transaction();
    for number in 0..200u64 {
        let this: Entity = format!("message:0-{number}").parse()?;
        transaction = transaction
            .assert(
                dialog_query::the!("chat.message/author")
                    .of(this.clone())
                    .is(author.clone()),
            )
            .assert(
                dialog_query::the!("chat.message/time")
                    .of(this)
                    .is(1_700_000_000_000 + number),
            );
    }
    transaction.commit().publish().perform(&operator).await?;

    let (rule, query) = segment(&channel(0), keys.at(100), keys.at(120))?;
    let mut subscription = branch.query().with(rule).subscribe(query);
    let first = subscription.poll(&operator).await?.expect("the first read");
    assert_eq!(numbers(&first.asserted)?, (100..120).collect::<Vec<_>>());
    for row in &first.asserted {
        let number = row.get::<u64>("number")?;
        assert_eq!(row.get::<String>("key")?, keys.at(number));
        assert_eq!(row.get::<Entity>("author")?, author);
        assert_eq!(row.get::<Entity>("channel")?, of);
        assert_eq!(row.get::<u64>("time")?, 1_700_000_000_000 + number);
    }
    let read = subscription.rows_read();
    println!("a segment of 20 entries with the key in its head reads {read} rows");
    assert!(
        read <= (1 + MESSAGE.len()) * 20 + 2,
        "one entry and 5 fields for each of 20 entries, got {read}"
    );
    Ok(())
}

/// A live window whose head holds the key keeps the rows that a first read
/// gives, with the fast path and with the slow path. A second entry of a
/// message of the window adds a row with its own key, the first entry of that
/// message leaves, a new message comes inside the window and another outside
/// it, and a field of a message of the window changes. After each change, the
/// rows that the live query holds are the rows of a first read, and they are
/// the same rows on both paths.
#[dialog_common::test]
async fn it_keeps_a_live_window_that_holds_the_key_as_a_first_read_gives_it() -> Result<()> {
    let fast = live_keyed_window(false).await?;
    let slow = live_keyed_window(true).await?;
    assert_eq!(fast, slow);
    Ok(())
}

/// The rows of a live window whose head holds the key after each change of
/// [`it_keeps_a_live_window_that_holds_the_key_as_a_first_read_gives_it`],
/// each checked against a first read. With `slow`, a stored fact of a field of
/// the head turns the fast path off first.
async fn live_keyed_window(slow: bool) -> Result<Vec<Vec<String>>> {
    let (branch, operator, keys) = opened(Keyed::Dictionary, &[200]).await?;
    if slow {
        lone_field(&branch, &operator).await?;
    }
    let general = channel(0);
    let window = Window {
        key_in_head: true,
        keyed: Keyed::Dictionary,
        channel: Some(&general),
        lower: at_least(keys.at(140)),
        upper: below(keys.at(160)),
    };
    let mut subscription = branch
        .query()
        .with(window.rule()?)
        .subscribe(window.query()?);
    subscription.poll(&operator).await?.expect("the first read");
    let mut seen = vec![entries(subscription.results())?];

    let of: Entity = general.parse()?;
    let held: Entity = "message:0-150".parse()?;
    let edited: Entity = "message:0-145".parse()?;
    let entry = |key: &str| -> Result<Relation> {
        Ok(Relation::from(CorpusRelation::try_from(format!(
            "chat.channel.log/{key}"
        ))?))
    };
    // Each step opens its transaction when it runs: a transaction commits
    // against the head it opened at, so one opened before the step ahead of
    // it published would be refused.
    type Step<'s> = Box<
        dyn Fn(
                crate::Transaction<&'s crate::Branch>,
            ) -> Result<crate::Transaction<&'s crate::Branch>>
            + 's,
    >;
    let steps: Vec<Step<'_>> = vec![
        Box::new(|transaction| {
            Ok(transaction.assert(entry("n00155x")?.of(of.clone()).is(held.clone())))
        }),
        Box::new(|transaction| {
            Ok(transaction.retract(entry("n00150")?.of(of.clone()).is(held.clone())))
        }),
        Box::new(|transaction| post(transaction, 0, "new", 900, "n00151x")),
        Box::new(|transaction| post(transaction, 0, "late", 901, "n00170x")),
        Box::new(|transaction| {
            Ok(transaction
                .retract(
                    dialog_query::the!("chat.message/text")
                        .of(edited.clone())
                        .is("hello".to_string()),
                )
                .assert(
                    dialog_query::the!("chat.message/text")
                        .of(edited.clone())
                        .is("edited".to_string()),
                ))
        }),
    ];
    for step in steps {
        step(branch.transaction())?
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        subscription.poll(&operator).await?;
        let live = entries(subscription.results())?;
        let (fresh, _) = first_read(&branch, &operator, window).await?;
        assert_eq!(live, entries(&fresh)?, "the live query holds a first read");
        seen.push(live);
    }
    let last = seen.last().expect("a step");
    let holds = |key: &str, number: u64| {
        last.iter().any(|row| {
            row.contains(&format!(" key={key} ")) && row.contains(&format!(" number={number} "))
        })
    };
    assert!(holds("n00155x", 150), "the second entry of 150 stays");
    assert!(!holds("n00150", 150), "the first entry of 150 left");
    assert!(holds("n00151x", 900), "the new message inside the window");
    assert!(!holds("n00170x", 901), "the new message outside the window");
    assert!(
        last.iter()
            .any(|row| row.contains(" number=145 text=edited"))
    );
    Ok(seen)
}

/// The rule of a window of the log of `channel` from the key `lower` up to the
/// key `upper`, which it leaves out, whose head holds the key of each entry
/// beside the fields of its message. The body also joins each message to the
/// entries of the log of `other` that hold it, and the head names no field of
/// those entries. So the body gives one row for each pair of an entry of the
/// window and an entry of `other` that hold one message.
fn window_twice(
    channel: &str,
    other: &str,
    lower: &str,
    upper: &str,
) -> Result<(DeductiveRule, ConceptQuery)> {
    let window = Window {
        key_in_head: true,
        keyed: Keyed::Dictionary,
        channel: Some(channel),
        lower: at_least(lower),
        upper: below(upper),
    };
    let head = window.head();
    let mut terms = Map::new();
    terms.insert("this".to_string(), json!({ "?": { "name": "this" } }));
    for (name, _) in FIELDS {
        terms.insert(name.to_string(), json!({ "?": { "name": name } }));
    }
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": head,
        "when": [
            {
                "assert": log(Keyed::Dictionary),
                "where": {
                    "this": { "entity:": channel },
                    "log": {
                        "the": { "?": { "name": "key" } },
                        "is": { "?": { "name": "this" } }
                    }
                }
            },
            {
                "assert": log(Keyed::Dictionary),
                "where": {
                    "this": { "entity:": other },
                    "log": {
                        "the": { "?": { "name": "elsewhere" } },
                        "is": { "?": { "name": "this" } }
                    }
                }
            },
            {
                "assert": ">=",
                "where": { "of": { "?": { "name": "key" } }, "with": { "text:": lower } }
            },
            {
                "assert": "<",
                "where": { "of": { "?": { "name": "key" } }, "with": { "text:": upper } }
            },
            { "assert": fields("chat.message"), "where": terms },
        ]
    }))?;
    Ok((descriptor.compile()?, window.query()?))
}

/// An entry of a window whose head holds the key gives one row, though the
/// body derives it twice. Each message of the window has two entries in the
/// log of a second channel, so the body gives two rows for each entry of the
/// window, with the same key and the same fields. The window answers one row
/// for each entry. When one of the two entries of the second channel leaves,
/// the row stays, and when the other leaves too, the row leaves. After each
/// change, the live query holds the rows of a first read.
#[dialog_common::test]
async fn it_answers_an_entry_that_the_body_derives_twice_once_when_the_head_holds_the_key()
-> Result<()> {
    let (branch, operator, keys) = opened(Keyed::Dictionary, &[200, 0]).await?;
    let other: Entity = channel(1).parse()?;
    let entry = |key: &str| -> Result<Relation> {
        Ok(Relation::from(CorpusRelation::try_from(format!(
            "chat.channel.log/{key}"
        ))?))
    };
    let mut transaction = branch.transaction();
    for number in 100..110u64 {
        let message: Entity = format!("message:0-{number}").parse()?;
        transaction = transaction
            .assert(
                entry(&format!("x{number}a"))?
                    .of(other.clone())
                    .is(message.clone()),
            )
            .assert(entry(&format!("x{number}b"))?.of(other.clone()).is(message));
    }
    transaction.commit().publish().perform(&operator).await?;

    let (rule, query) = window_twice(&channel(0), &channel(1), keys.at(100), keys.at(110))?;
    assert_eq!(
        rule.head_keyed(),
        Some("key"),
        "the rule takes the path of one evaluation for each entry"
    );
    let mut subscription = branch.query().with(rule.clone()).subscribe(query.clone());
    let first = subscription.poll(&operator).await?.expect("the first read");
    assert_eq!(
        numbers(&first.asserted)?,
        (100..110).collect::<Vec<_>>(),
        "one row for each entry"
    );
    for row in &first.asserted {
        assert_eq!(
            row.get::<String>("key")?,
            keys.at(row.get::<u64>("number")?)
        );
    }

    let message: Entity = "message:0-105".parse()?;
    for (key, stays) in [("x105a", true), ("x105b", false)] {
        branch
            .transaction()
            .retract(entry(key)?.of(other.clone()).is(message.clone()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        subscription.poll(&operator).await?;
        let mut fresh = branch.query().with(rule.clone()).subscribe(query.clone());
        let fresh = fresh.poll(&operator).await?.expect("the first read");
        assert_eq!(
            entries(subscription.results())?,
            entries(&fresh.asserted)?,
            "the live query holds a first read"
        );
        let expected: Vec<u64> = (100..110)
            .filter(|number| stays || *number != 105)
            .collect();
        assert_eq!(
            numbers(subscription.results())?,
            expected,
            "after the leave of {key}"
        );
    }
    Ok(())
}

/// A live window keeps a message for as long as one entry of its range holds
/// it. Two entries hold message 150. The window answers it once, keeps it when
/// one of the two entries leaves, and drops it when the other leaves too.
#[dialog_common::test]
async fn it_keeps_a_row_of_a_live_window_while_one_entry_still_holds_it() -> Result<()> {
    let (branch, operator, keys) = opened(Keyed::Dictionary, &[400]).await?;
    held_again(&branch, &operator, 0, "150", "n00155x").await?;
    let general = channel(0);
    let window = Window {
        key_in_head: false,
        keyed: Keyed::Dictionary,
        channel: Some(&general),
        lower: at_least(keys.at(140)),
        upper: below(keys.at(160)),
    };
    let mut subscription = branch
        .query()
        .with(window.rule()?)
        .subscribe(window.query()?);
    let first = subscription.poll(&operator).await?.expect("the first read");
    assert_eq!(numbers(&first.asserted)?, (140..160).collect::<Vec<_>>());

    let of: Entity = general.parse()?;
    let held: Entity = "message:0-150".parse()?;
    let leave = |key: &str| -> Result<_> {
        let entry = Relation::from(CorpusRelation::try_from(format!("chat.channel.log/{key}"))?);
        Ok(branch
            .transaction()
            .retract(entry.of(of.clone()).is(held.clone())))
    };
    leave("n00150")?
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    if let Some(delta) = subscription.poll(&operator).await? {
        assert!(
            delta.asserted.is_empty() && delta.retracted.is_empty(),
            "the other entry still holds the message"
        );
    }
    assert_eq!(subscription.results().len(), 20);

    leave("n00155x")?
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = subscription.poll(&operator).await?.expect("the row left");
    assert_eq!(numbers(&delta.retracted)?, vec![150]);
    assert_eq!(subscription.results().len(), 19);
    Ok(())
}

/// A new entry costs a live window the same, whatever the window holds. The
/// live query binds the new entry into the read of the log, with its key and
/// its message, so the one message is the one row that it derives again. Two
/// windows of one log, one that holds 100 rows and one that holds 2, read the
/// same rows for one new message.
#[dialog_common::test]
async fn it_reads_the_same_rows_for_a_new_entry_whatever_a_live_window_holds() -> Result<()> {
    let (branch, operator, keys) = opened(Keyed::Dictionary, &[400, 50]).await?;
    let general = channel(0);
    let from = |number: u64| Window {
        key_in_head: false,
        keyed: Keyed::Dictionary,
        channel: Some(&general),
        lower: at_least(keys.at(number)),
        upper: None,
    };
    let (wide, narrow) = (from(300), from(398));
    let mut holds_100 = branch.query().with(wide.rule()?).subscribe(wide.query()?);
    let mut holds_2 = branch
        .query()
        .with(narrow.rule()?)
        .subscribe(narrow.query()?);
    let first = holds_100.poll(&operator).await?.expect("the first read");
    assert_eq!(first.asserted.len(), 100);
    let first = holds_2.poll(&operator).await?.expect("the first read");
    assert_eq!(first.asserted.len(), 2);

    let (before_100, before_2) = (holds_100.rows_read(), holds_2.rows_read());
    post(branch.transaction(), 0, "new", 400, "n00400")?
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = holds_100.poll(&operator).await?.expect("a new row");
    assert_eq!(numbers(&delta.asserted)?, vec![400]);
    let delta = holds_2.poll(&operator).await?.expect("a new row");
    assert_eq!(numbers(&delta.asserted)?, vec![400]);
    let (cost_100, cost_2) = (
        holds_100.rows_read() - before_100,
        holds_2.rows_read() - before_2,
    );
    println!("a new entry costs a window of 100 rows {cost_100} rows, and one of 2 rows {cost_2}");
    assert_eq!(
        cost_100, cost_2,
        "the cost of a new entry does not follow the rows that the window holds"
    );
    assert!(
        cost_100 <= 12 * (1 + FIELDS.len()),
        "a new entry reads the rows of one message a few times, got {cost_100}"
    );
    Ok(())
}

/// A rule can derive the entries of a collection. Then a row of the
/// collection can change with no change to a stored entry, so a live window
/// finds what a change affects through the rules, as for any concept. Here a
/// rule derives an entry of the log from a pin, and a new pin reaches the
/// window as a new row.
#[dialog_common::test]
async fn it_keeps_a_live_window_correct_over_a_log_that_a_rule_derives() -> Result<()> {
    let (branch, operator, keys) = opened(Keyed::Dictionary, &[200]).await?;
    let general = channel(0);
    let window = Window {
        key_in_head: false,
        keyed: Keyed::Dictionary,
        channel: Some(&general),
        lower: at_least(keys.at(190)),
        upper: None,
    };
    let pinned: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": log(Keyed::Dictionary),
        "when": [{
            "assert": { "with": {
                "of": { "the": "chat.pin/of", "as": "entity:" },
                "message": { "the": "chat.pin/message", "as": "entity:" },
                "key": { "the": "chat.pin/key", "as": "text:" }
            } },
            "where": {
                "this": { "?": { "name": "pin" } },
                "of": { "?": { "name": "this" } },
                "message": { "?": { "name": "log" } },
                "key": { "?": { "name": "log/key" } }
            }
        }]
    }))?;
    let mut subscription = branch
        .query()
        .with(window.rule()?)
        .with(pinned.compile()?)
        .subscribe(window.query()?);
    let first = subscription.poll(&operator).await?.expect("the first read");
    assert_eq!(numbers(&first.asserted)?, (190..200).collect::<Vec<_>>());

    // Message 5 is below the window. A pin gives it a second place, inside.
    let of: Entity = general.parse()?;
    let pin: Entity = "pin:1".parse()?;
    let message: Entity = "message:0-5".parse()?;
    branch
        .transaction()
        .assert(dialog_query::the!("chat.pin/of").of(pin.clone()).is(of))
        .assert(
            dialog_query::the!("chat.pin/message")
                .of(pin.clone())
                .is(message),
        )
        .assert(
            dialog_query::the!("chat.pin/key")
                .of(pin)
                .is("n00195x".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = subscription.poll(&operator).await?.expect("the pinned row");
    assert_eq!(numbers(&delta.asserted)?, vec![5]);
    assert!(delta.retracted.is_empty());
    Ok(())
}

/// A fact under a key of the log whose value is not an entity is no entry of
/// the log, because the field reads entities. A live query passes over such a
/// fact when it looks for the rows that a change affects, as a scan of the
/// log does. Here a rule reads the messages that no entry of the log holds.
/// One commit takes message 100 out of the log, and writes a text under a key
/// of the log. The live query answers message 100, as a first read does.
#[dialog_common::test]
async fn it_passes_over_a_changed_fact_that_is_no_entry_of_the_log() -> Result<()> {
    let (branch, operator, _) = opened(Keyed::Dictionary, &[120]).await?;
    let general = channel(0);
    let mut terms = Map::new();
    terms.insert("this".to_string(), json!({ "?": { "name": "this" } }));
    for (name, _) in FIELDS {
        terms.insert(name.to_string(), json!({ "?": { "name": name } }));
    }
    let outside: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": fields("test.window"),
        "when": [
            { "assert": fields("chat.message"), "where": terms },
            { "assert": "==", "where": {
                "this": { "?": { "name": "channel" } }, "is": { "entity:": general }
            } },
            { "assert": ">=", "where": { "of": { "?": { "name": "number" } }, "with": 95 } }
        ],
        "unless": [{
            "assert": log(Keyed::Dictionary),
            "where": {
                "this": { "entity:": general },
                "log": { "the": { "?": {} }, "is": { "?": { "name": "this" } } }
            }
        }]
    }))?;
    let window = Window {
        key_in_head: false,
        keyed: Keyed::Dictionary,
        channel: Some(&general),
        lower: None,
        upper: None,
    };
    let mut subscription = branch
        .query()
        .with(outside.compile()?)
        .subscribe(window.query()?);
    if let Some(first) = subscription.poll(&operator).await? {
        assert!(first.asserted.is_empty(), "each message has an entry");
    }

    let of: Entity = general.parse()?;
    let gone: Entity = "message:0-100".parse()?;
    let entry = |key: &str| -> Result<Relation> {
        Ok(Relation::from(CorpusRelation::try_from(format!(
            "chat.channel.log/{key}"
        ))?))
    };
    branch
        .transaction()
        .retract(entry("n00100")?.of(of.clone()).is(gone))
        .assert(
            entry("n00900")?
                .of(of.clone())
                .is("not an entity".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = subscription
        .poll(&operator)
        .await?
        .expect("the message that left the log");
    assert_eq!(numbers(&delta.asserted)?, vec![100]);
    assert!(delta.retracted.is_empty());

    // The text leaves again, and no row changes.
    branch
        .transaction()
        .retract(entry("n00900")?.of(of).is("not an entity".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    if let Some(delta) = subscription.poll(&operator).await? {
        assert!(delta.asserted.is_empty() && delta.retracted.is_empty());
    }
    assert_eq!(subscription.results().len(), 1);
    Ok(())
}

/// A collection of numbers, as a concept: one dictionary field of `kind`, or
/// a field with no type for an empty `kind`.
fn scores(kind: &str) -> Json {
    let mut field = json!({
        "the": { "domain": "test.scores", "keyed": "dictionary" },
        "pick": "all:",
    });
    if !kind.is_empty() {
        field["as"] = json!(kind);
    }
    json!({ "with": { "scores": field } })
}

/// A new branch with nothing in it.
async fn empty() -> Result<(Branch, Operator)> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    Ok((branch, operator))
}

/// The attribute of the entry of `test.scores` under `key`.
fn score(key: &str) -> Result<Relation> {
    Ok(Relation::from(CorpusRelation::try_from(format!(
        "test.scores/{key}"
    ))?))
}

/// A live query over each score of each holder: the rule reads the collection
/// of `kind`, and its head holds the score. Gives the subscription after its
/// first poll.
async fn scored(
    branch: &Branch,
    operator: &Operator,
    kind: &str,
) -> Result<Subscription<ConceptQuery>> {
    let head = json!({ "with": {
        "score": { "the": "test.board/score", "as": kind, "pick": "all:" },
    } });
    let rule: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": head,
        "when": [{
            "assert": scores(kind),
            "where": {
                "this": { "?": { "name": "this" } },
                "scores": { "the": { "?": { "name": "key" } }, "is": { "?": { "name": "score" } } }
            }
        }]
    }))?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("score".into(), Term::var("score"));
    let query = ConceptQuery {
        predicate: serde_json::from_value(head)?,
        terms,
    };
    let mut subscription = branch.query().with(rule.compile()?).subscribe(query);
    if let Some(first) = subscription.poll(operator).await? {
        assert!(first.asserted.is_empty(), "the branch holds no score");
    }
    Ok(subscription)
}

/// A field of integers reads a natural as an integer. A scan of the collection
/// gives a row for an entry that holds the natural 7, so a live query binds
/// that entry too, as the field reads it.
#[dialog_common::test]
async fn it_binds_a_changed_entry_of_a_number_as_its_field_reads_it() -> Result<()> {
    let (branch, operator) = empty().await?;
    let mut subscription = scored(&branch, &operator, "integer:").await?;
    let board: Entity = "board:1".parse()?;
    branch
        .transaction()
        .assert(score("a")?.of(board.clone()).is(7u64))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = subscription.poll(&operator).await?.expect("the score");
    assert_eq!(delta.asserted.len(), 1);
    assert_eq!(delta.asserted[0].get::<i64>("score")?, 7);

    branch
        .transaction()
        .assert(score("b")?.of(board).is(-3i64))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = subscription.poll(&operator).await?.expect("a second score");
    assert_eq!(delta.asserted.len(), 1);
    assert_eq!(subscription.results().len(), 2);
    Ok(())
}

/// A field of naturals reads 9.5 as 9. The row of that entry comes when the
/// entry is written, and it leaves when the entry is retracted.
#[dialog_common::test]
async fn it_retracts_the_row_of_an_entry_that_its_field_read_as_another_number() -> Result<()> {
    let (branch, operator) = empty().await?;
    let mut subscription = scored(&branch, &operator, "natural:").await?;
    let board: Entity = "board:1".parse()?;
    branch
        .transaction()
        .assert(score("a")?.of(board.clone()).is(9.5f64))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = subscription.poll(&operator).await?.expect("the score");
    assert_eq!(delta.asserted.len(), 1);
    assert_eq!(delta.asserted[0].get::<u64>("score")?, 9);

    branch
        .transaction()
        .retract(score("a")?.of(board).is(9.5f64))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = subscription.poll(&operator).await?.expect("the score left");
    assert_eq!(delta.retracted.len(), 1);
    assert!(subscription.results().is_empty());
    Ok(())
}

/// The value of a changed entry joins the other premises as the field reads
/// it. An item has the rank 7, and a rule gives each item whose rank is a
/// score of one board. An entry of 7.5 in a field of naturals reads as 7, so
/// the item shows.
#[dialog_common::test]
async fn it_joins_a_changed_entry_by_the_value_that_its_field_reads() -> Result<()> {
    let (branch, operator) = empty().await?;
    let board: Entity = "board:1".parse()?;
    let item: Entity = "item:1".parse()?;
    branch
        .transaction()
        .assert(
            dialog_query::the!("test.item/rank")
                .of(item.clone())
                .is(7u64),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let ranked = json!({ "with": { "rank": { "the": "test.ranked/rank", "as": "natural:" } } });
    let rule: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": ranked,
        "when": [
            {
                "assert": scores("natural:"),
                "where": {
                    "this": { "entity:": "board:1" },
                    "scores": { "the": { "?": { "name": "key" } }, "is": { "?": { "name": "rank" } } }
                }
            },
            {
                "assert": { "with": { "rank": { "the": "test.item/rank", "as": "natural:" } } },
                "where": { "this": { "?": { "name": "this" } }, "rank": { "?": { "name": "rank" } } }
            }
        ]
    }))?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("rank".into(), Term::var("rank"));
    let query = ConceptQuery {
        predicate: serde_json::from_value(ranked)?,
        terms,
    };
    let mut subscription = branch.query().with(rule.compile()?).subscribe(query);
    if let Some(first) = subscription.poll(&operator).await? {
        assert!(first.asserted.is_empty(), "the board holds no score");
    }

    branch
        .transaction()
        .assert(score("a")?.of(board).is(7.5f64))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = subscription
        .poll(&operator)
        .await?
        .expect("the ranked item");
    assert_eq!(delta.asserted.len(), 1);
    assert_eq!(delta.asserted[0].entity(), &item);
    Ok(())
}

/// The fact that `of` holds the value `is` under the attribute `the`, where
/// the test names the value with its type.
fn fact(the: &str, of: &Entity, is: Value) -> Result<AttributeStatement> {
    Ok(AttributeStatement {
        the: the.parse()?,
        of: of.clone(),
        is,
        cause: None,
        cardinality: Some(Cardinality::Many),
        pick: None,
    })
}

/// A query of each field of `head`.
fn each_field(head: &Json) -> Result<ConceptQuery> {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    for name in head["with"].as_object().expect("the fields").keys() {
        terms.insert(name.clone(), Term::var(name.clone()));
    }
    Ok(ConceptQuery {
        predicate: serde_json::from_value(head.clone())?,
        terms,
    })
}

/// Polls `live`, and compares the rows that it holds with the rows that a
/// new query of the same rule reads. Gives the number of rows.
async fn rows_of_both(
    branch: &Branch,
    operator: &Operator,
    rule: &DeductiveRule,
    head: &Json,
    live: &mut Subscription<ConceptQuery>,
) -> Result<usize> {
    live.poll(operator).await?;
    let mut new = branch
        .query()
        .with(rule.clone())
        .subscribe(each_field(head)?);
    let read = new
        .poll(operator)
        .await?
        .map_or(Vec::new(), |delta| delta.asserted);
    let held = live.results();
    assert_eq!(held.len(), read.len(), "the live query and a first read");
    assert!(held.iter().all(|row| read.contains(row)));
    Ok(held.len())
}

/// A rule that gives each holder whose collection of `kind` holds the number
/// 7, with the name of the holder. The 7 is a constant in the place of the
/// value of an entry.
fn holder_of_seven(kind: &str) -> Result<(DeductiveRule, Json)> {
    holder_with(scores(kind), "scores", json!({ "?": {} }), json!(7))
}

/// A rule that gives each holder whose `collection`, a concept of the one
/// field `field`, holds an entry of `value` under `key`, with the name of the
/// holder. The key and the value are each a constant of the rule or a blank.
fn holder_with(
    collection: Json,
    field: &str,
    key: Json,
    value: Json,
) -> Result<(DeductiveRule, Json)> {
    let head = json!({ "with": { "name": { "the": "test.flagged/name", "as": "text:" } } });
    let rule: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": head,
        "when": [
            {
                "assert": collection,
                "where": {
                    "this": { "?": { "name": "this" } },
                    field: { "the": key, "is": value }
                }
            },
            {
                "assert": { "with": { "name": { "the": "test.board/name", "as": "text:" } } },
                "where": { "this": { "?": { "name": "this" } }, "name": { "?": { "name": "name" } } }
            }
        ]
    }))?;
    Ok((rule.compile()?, head))
}

/// A branch that holds one board with a name, and no score.
async fn board() -> Result<(Branch, Operator, Entity)> {
    let (branch, operator) = empty().await?;
    let board: Entity = "board:1".parse()?;
    branch
        .transaction()
        .assert(
            dialog_query::the!("test.board/name")
                .of(board.clone())
                .is("board".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    Ok((branch, operator, board))
}

/// A scan does not compare a number by equality. It reads each stored number
/// through the type of the field, and the constant of a rule as that type. So
/// in a field of floats, an entry that holds the natural 7 answers a rule
/// that names 7. A live query finds that entry as a first read does.
#[dialog_common::test]
async fn it_finds_a_changed_entry_of_a_number_by_a_constant_of_another_number_type() -> Result<()> {
    let (branch, operator, board) = board().await?;
    let (rule, head) = holder_of_seven("float:")?;
    let mut live = branch
        .query()
        .with(rule.clone())
        .subscribe(each_field(&head)?);
    assert_eq!(
        rows_of_both(&branch, &operator, &rule, &head, &mut live).await?,
        0
    );

    branch
        .transaction()
        .assert(score("a")?.of(board.clone()).is(7u64))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    assert_eq!(
        rows_of_both(&branch, &operator, &rule, &head, &mut live).await?,
        1
    );
    assert_eq!(live.results()[0].entity(), &board);

    branch
        .transaction()
        .retract(score("a")?.of(board).is(7u64))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    assert_eq!(
        rows_of_both(&branch, &operator, &rule, &head, &mut live).await?,
        0
    );
    Ok(())
}

/// In a field with no type, a scan reads each stored number through the type
/// of the constant of the rule. So an entry of 7.5 answers a rule that names
/// 7, as an entry of 7 does. The row of the holder stays while one of the two
/// entries is there, and it leaves with the last.
#[dialog_common::test]
async fn it_keeps_the_row_of_a_holder_while_an_entry_still_answers_the_constant() -> Result<()> {
    let (branch, operator, board) = board().await?;
    let (rule, head) = holder_of_seven("")?;
    let mut live = branch
        .query()
        .with(rule.clone())
        .subscribe(each_field(&head)?);
    live.poll(&operator).await?;

    let mut held = Vec::new();
    for (exists, key, number) in [
        (true, "a", Value::UnsignedInt(7)),
        (true, "c", Value::Float(7.5)),
        (false, "a", Value::UnsignedInt(7)),
        (false, "c", Value::Float(7.5)),
    ] {
        let entry = fact(&format!("test.scores/{key}"), &board, number)?;
        let transaction = branch.transaction();
        let transaction = if exists {
            transaction.assert(entry)
        } else {
            transaction.retract(entry)
        };
        transaction.commit().publish().perform(&operator).await?;
        held.push(rows_of_both(&branch, &operator, &rule, &head, &mut live).await?);
    }
    assert_eq!(held, [1, 1, 1, 0]);
    Ok(())
}

/// A number joins the number of another premise as a scan compares the two:
/// the number that a read is bound with reads each stored number through its
/// own type. An item of the rank 7 is held by an entry of 7.5, and an item of
/// the rank 7.5 is not held by an entry of 7. A rule gives each item whose
/// rank no entry of one board holds. After each change of an entry, the live
/// query holds the items that a first read gives.
#[dialog_common::test]
async fn it_joins_a_changed_entry_of_a_number_as_a_first_read_does() -> Result<()> {
    let (branch, operator, board) = board().await?;
    let mut transaction = branch.transaction();
    for (item, rank) in [
        ("item:1", Value::UnsignedInt(7)),
        ("item:2", Value::UnsignedInt(8)),
        ("item:3", Value::SignedInt(-3)),
        ("item:4", Value::Float(7.5)),
    ] {
        transaction = transaction.assert(fact("test.item/rank", &item.parse()?, rank)?);
    }
    transaction.commit().publish().perform(&operator).await?;

    let head = json!({ "with": { "rank": { "the": "test.ranked/rank" } } });
    let rule: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": head,
        "when": [{
            "assert": { "with": { "rank": { "the": "test.item/rank" } } },
            "where": { "this": { "?": { "name": "this" } }, "rank": { "?": { "name": "rank" } } }
        }],
        "unless": [{
            "assert": scores(""),
            "where": {
                "this": { "entity:": "board:1" },
                "scores": { "the": { "?": {} }, "is": { "?": { "name": "rank" } } }
            }
        }]
    }))?;
    let rule = rule.compile()?;
    let mut live = branch
        .query()
        .with(rule.clone())
        .subscribe(each_field(&head)?);
    assert_eq!(
        rows_of_both(&branch, &operator, &rule, &head, &mut live).await?,
        4
    );

    let mut held = Vec::new();
    for (exists, key, number) in [
        (true, "a", Value::UnsignedInt(7)),
        (true, "c", Value::Float(7.5)),
        (false, "a", Value::UnsignedInt(7)),
        (false, "c", Value::Float(7.5)),
    ] {
        let entry = fact(&format!("test.scores/{key}"), &board, number)?;
        let transaction = branch.transaction();
        let transaction = if exists {
            transaction.assert(entry)
        } else {
            transaction.retract(entry)
        };
        transaction.commit().publish().perform(&operator).await?;
        held.push(rows_of_both(&branch, &operator, &rule, &head, &mut live).await?);
    }
    assert_eq!(held, [3, 2, 2, 4]);
    Ok(())
}

/// A collection under `pick`, a pick that elects, as a concept: one dictionary
/// field of `kind`. A live query reads such a collection as it reads each
/// other concept, by the entity of a changed claim, because a claim can take
/// the place of another claim that a premise read.
fn settings(kind: &str, pick: &str) -> Json {
    json!({ "with": { "settings": {
        "the": { "domain": "test.settings", "keyed": "dictionary" },
        "as": kind,
        "pick": pick,
    } } })
}

/// Writes or retracts one claim of the collection of [`settings`], under
/// `key`. A claim is an append, so an older claim of the key stays in the
/// store beside it.
async fn claim(
    branch: &Branch,
    operator: &Operator,
    board: &Entity,
    exists: bool,
    key: &str,
    value: Value,
) -> Result<()> {
    let claim = fact(&format!("test.settings/{key}"), board, value)?;
    let transaction = branch.transaction();
    let transaction = if exists {
        transaction.assert(claim)
    } else {
        transaction.retract(claim)
    };
    transaction.commit().publish().perform(operator).await?;
    Ok(())
}

/// Under `last:`, a scan gives one claim of each key of a holder, and a newer
/// claim takes that place whatever its own value. A rule names the mode
/// "dark". The holder shows while "dark" is the claim that a scan gives.
/// It leaves when a claim of "light" takes that place, though the rule does
/// not match "light", and it returns when that claim leaves.
#[dialog_common::test]
async fn it_follows_a_claim_that_takes_the_place_of_the_claim_that_a_rule_names() -> Result<()> {
    let (branch, operator, board) = board().await?;
    let (rule, head) = holder_with(
        settings("text:", "last:"),
        "settings",
        json!({ "?": {} }),
        json!("dark"),
    )?;
    let mut live = branch
        .query()
        .with(rule.clone())
        .subscribe(each_field(&head)?);
    live.poll(&operator).await?;

    let mut held = Vec::new();
    for (exists, mode) in [
        (true, "dark"),
        (true, "light"),
        (false, "light"),
        (true, "light"),
        (false, "dark"),
    ] {
        let mode = Value::String(mode.to_owned());
        claim(&branch, &operator, &board, exists, "mode", mode).await?;
        held.push(rows_of_both(&branch, &operator, &rule, &head, &mut live).await?);
    }
    assert_eq!(held, [1, 0, 1, 0, 0]);
    Ok(())
}

/// A claim that the field does not read takes the place of a claim too. A
/// field of naturals under `last:` holds 7 under one key, and a rule gives
/// each number of each holder. A claim of an entity arrives under that key. A
/// scan then gives no row for the key, so the live query drops the row of 7,
/// and the row returns when the claim of the entity leaves.
#[dialog_common::test]
async fn it_follows_a_claim_that_its_field_does_not_read() -> Result<()> {
    let (branch, operator, board) = board().await?;
    let head = json!({ "with": {
        "setting": { "the": "test.board/setting", "as": "natural:", "pick": "all:" },
    } });
    let rule: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": head,
        "when": [{
            "assert": settings("natural:", "last:"),
            "where": {
                "this": { "?": { "name": "this" } },
                "settings": { "the": { "?": { "name": "key" } }, "is": { "?": { "name": "setting" } } }
            }
        }]
    }))?;
    let rule = rule.compile()?;
    let mut live = branch
        .query()
        .with(rule.clone())
        .subscribe(each_field(&head)?);
    live.poll(&operator).await?;

    let thing = Value::Entity("thing:1".parse()?);
    let mut held = Vec::new();
    for (exists, value) in [
        (true, Value::UnsignedInt(7)),
        (true, thing.clone()),
        (false, thing),
    ] {
        claim(&branch, &operator, &board, exists, "level", value).await?;
        held.push(rows_of_both(&branch, &operator, &rule, &head, &mut live).await?);
    }
    assert_eq!(held, [1, 0, 1]);
    Ok(())
}

/// Under `max:`, a read elects among each claim of a holder, and compares the
/// key after that. A rule gives each holder with an entry under the key
/// `level`. The holder shows while its greatest claim is under `level`, and it
/// leaves when a greater claim arrives under another key, though the rule does
/// not name that key.
#[dialog_common::test]
async fn it_follows_a_claim_of_another_key_that_a_pick_elects() -> Result<()> {
    let (branch, operator, board) = board().await?;
    let (rule, head) = holder_with(
        settings("natural:", "max:"),
        "settings",
        json!("level"),
        json!({ "?": {} }),
    )?;
    let mut live = branch
        .query()
        .with(rule.clone())
        .subscribe(each_field(&head)?);
    live.poll(&operator).await?;

    let mut held = Vec::new();
    for (key, number) in [("level", 7), ("limit", 9)] {
        let number = Value::UnsignedInt(number);
        claim(&branch, &operator, &board, true, key, number).await?;
        held.push(rows_of_both(&branch, &operator, &rule, &head, &mut live).await?);
    }
    assert_eq!(held, [1, 0]);
    Ok(())
}

/// A rule can read any entry of a collection: the key and the value of its
/// entry are both blank. Such an entry is written as an empty object, and the
/// live query reads the rule back from that form. The holder shows while it
/// holds an entry of any key and any value.
#[dialog_common::test]
async fn it_reads_a_rule_whose_entry_has_a_blank_key_and_a_blank_value() -> Result<()> {
    let (branch, operator, board) = board().await?;
    let (rule, head) = holder_with(scores(""), "scores", json!({ "?": {} }), json!({ "?": {} }))?;
    let mut live = branch
        .query()
        .with(rule.clone())
        .subscribe(each_field(&head)?);
    assert_eq!(
        rows_of_both(&branch, &operator, &rule, &head, &mut live).await?,
        0
    );

    let mut held = Vec::new();
    for (exists, key, value) in [
        (true, "a", Value::UnsignedInt(7)),
        (true, "b", Value::String("seven".into())),
        (false, "a", Value::UnsignedInt(7)),
        (false, "b", Value::String("seven".into())),
    ] {
        let entry = fact(&format!("test.scores/{key}"), &board, value)?;
        let transaction = branch.transaction();
        let transaction = if exists {
            transaction.assert(entry)
        } else {
            transaction.retract(entry)
        };
        transaction.commit().publish().perform(&operator).await?;
        held.push(rows_of_both(&branch, &operator, &rule, &head, &mut live).await?);
    }
    assert_eq!(held, [1, 1, 1, 0]);
    Ok(())
}
