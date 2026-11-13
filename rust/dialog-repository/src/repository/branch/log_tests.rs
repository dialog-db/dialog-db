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

/// How a query of a range of a log whose head holds the key binds that key.
#[derive(Clone, Copy, Debug)]
enum KeyBinding {
    /// The query leaves the key free.
    Free,
    /// The query binds the key to the constant `n00150`.
    Constant,
    /// A rule reads the range with the key bound to a variable, which an
    /// earlier premise of its body binds to `n00150`.
    Variable,
}

/// A query of a range of a log whose head holds the key can bind that key, to a
/// constant or to a variable that an earlier premise binds. The query then asks
/// for the rows of one key. Each path of evaluation gives the same rows for it
/// as for a free key: the path of one evaluation for each entry, and the path
/// of one evaluation for each field of the head. A stored fact of a field of
/// the head turns the second path on, and the test runs it with a stored fact
/// of the text and with a stored fact of the key. The rows are the same with
/// the holder of the log bound and with the holder free, on a first read and in
/// a live query after each change.
#[dialog_common::test]
async fn it_answers_a_bound_key_on_each_path_when_the_head_holds_the_key() -> Result<()> {
    for holder_bound in [true, false] {
        for key in [KeyBinding::Constant, KeyBinding::Variable, KeyBinding::Free] {
            let fast = rows_of_a_bound_key(None, holder_bound, key).await?;
            for lone in ["text", "key"] {
                let slow = rows_of_a_bound_key(Some(lone), holder_bound, key).await?;
                assert_eq!(
                    fast, slow,
                    "holder bound: {holder_bound}, key: {key:?}, a stored {lone}"
                );
            }
            let counts: Vec<usize> = fast.iter().map(Vec::len).collect();
            let expected = match (key, holder_bound) {
                (KeyBinding::Free, true) => [20, 19, 20, 20, 21],
                (KeyBinding::Free, false) => [40, 39, 40, 40, 41],
                (_, true) => [1, 0, 1, 1, 1],
                (_, false) => [2, 1, 2, 2, 2],
            };
            assert_eq!(
                counts, expected,
                "holder bound: {holder_bound}, key: {key:?}"
            );
        }
    }
    Ok(())
}

/// The rows of a live query of a range of the log of the first holder, or of
/// the log of each holder when `holder_bound` is false, whose head holds the
/// key, as the query binds the key. The rows are those of the first read and
/// those after each of four changes: the leave of the entry under the key
/// `n00150` of the first log, a new entry under that key, a change of the text
/// of the item of that entry, and a new entry under another key of the range.
/// After each change the live query must hold the rows of a first read. With a
/// `lone` field, a stored fact of that field of the head, on an entity that no
/// entry holds, turns the fast path off first.
async fn rows_of_a_bound_key(
    lone: Option<&str>,
    holder_bound: bool,
    key: KeyBinding,
) -> Result<Vec<Vec<String>>> {
    let (branch, operator, keys) = opened(Keyed::Dictionary, &[200, 200]).await?;
    let chosen = "n00150".to_string();
    let mut setup = branch.transaction().assert(
        dialog_query::the!("test.choice/key")
            .of("choice:one".parse::<Entity>()?)
            .is(chosen.clone()),
    );
    if let Some(field) = lone {
        let the = Relation::from(CorpusRelation::try_from(format!("test.window/{field}"))?);
        setup = setup.assert(
            the.of("other:lone".parse::<Entity>()?)
                .is("alone".to_string()),
        );
    }
    setup.commit().publish().perform(&operator).await?;

    let general = channel(0);
    let window = Window {
        key_in_head: true,
        keyed: Keyed::Dictionary,
        channel: holder_bound.then_some(general.as_str()),
        lower: at_least(keys.at(140)),
        upper: below(keys.at(160)),
    };
    let rule = window.rule()?;
    let mut query = window.query()?;
    // The rule of a choice reads the range under the key that the stored
    // choice names, and its head holds what the range holds.
    let mut head = fields("test.chosen");
    head["with"]["key"] = json!({ "the": "test.chosen/key", "as": "text:", "pick": "all:" });
    let mut terms = Map::new();
    terms.insert("this".to_string(), json!({ "?": { "name": "this" } }));
    terms.insert("key".to_string(), json!({ "?": { "name": "key" } }));
    for (name, _) in FIELDS {
        terms.insert(name.to_string(), json!({ "?": { "name": name } }));
    }
    let choice: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": head.clone(),
        "when": [
            {
                "assert": { "with": { "key": { "the": "test.choice/key", "as": "text:" } } },
                "where": { "this": { "entity:": "choice:one" }, "key": { "?": { "name": "key" } } }
            },
            { "assert": window.head(), "where": terms },
        ]
    }))?;
    let choice = choice.compile()?;
    match key {
        KeyBinding::Free => {}
        KeyBinding::Constant => {
            query.terms.insert("key".into(), Term::constant(chosen));
        }
        KeyBinding::Variable => {
            query.predicate = serde_json::from_value(head)?;
        }
    }
    let subscribe = || {
        branch
            .query()
            .with(rule.clone())
            .with(choice.clone())
            .subscribe(query.clone())
    };
    let mut subscription = subscribe();
    subscription.poll(&operator).await?.expect("the first read");
    let mut seen = vec![entries(subscription.results())?];

    let of: Entity = general.parse()?;
    let held: Entity = "message:0-150".parse()?;
    let new: Entity = "message:0-new".parse()?;
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
            Ok(transaction.retract(entry("n00150")?.of(of.clone()).is(held.clone())))
        }),
        Box::new(|transaction| post(transaction, 0, "new", 900, "n00150")),
        Box::new(|transaction| {
            Ok(transaction
                .retract(
                    dialog_query::the!("chat.message/text")
                        .of(new.clone())
                        .is("hello".to_string()),
                )
                .assert(
                    dialog_query::the!("chat.message/text")
                        .of(new.clone())
                        .is("edited".to_string()),
                ))
        }),
        Box::new(|transaction| post(transaction, 0, "other", 901, "n00151x")),
    ];
    for step in steps {
        step(branch.transaction())?
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        subscription.poll(&operator).await?;
        let live = entries(subscription.results())?;
        let mut fresh = subscribe();
        let fresh = fresh.poll(&operator).await?.expect("the first read");
        assert_eq!(
            live,
            entries(&fresh.asserted)?,
            "the live query holds a first read"
        );
        seen.push(live);
    }
    Ok(seen)
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

/// A scan reads an exact number as the range of the numbers that its type
/// reads as it. A rule gives each holder of 7 in a collection of naturals, and
/// the field reads 7.5 as 7. So a second board with an entry of 7.5 is a
/// holder of 7. The live query watches each write that the scan reads, so it
/// sees that entry as a first read does.
#[dialog_common::test]
async fn it_sees_a_new_entry_that_its_field_reads_as_the_number_of_the_rule() -> Result<()> {
    let (branch, operator, board) = board().await?;
    let other: Entity = "board:2".parse()?;
    branch
        .transaction()
        .assert(
            dialog_query::the!("test.board/name")
                .of(other.clone())
                .is("other".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let (rule, head) = holder_of_seven("natural:")?;
    let mut live = branch
        .query()
        .with(rule.clone())
        .subscribe(each_field(&head)?);
    live.poll(&operator).await?;

    let mut held = Vec::new();
    for (exists, holder, number) in [
        (true, &board, Value::UnsignedInt(7)),
        (true, &other, Value::Float(7.5)),
        (false, &other, Value::Float(7.5)),
        (false, &board, Value::UnsignedInt(7)),
    ] {
        let entry = fact("test.scores/a", holder, number)?;
        let transaction = branch.transaction();
        let transaction = if exists {
            transaction.assert(entry)
        } else {
            transaction.retract(entry)
        };
        transaction.commit().publish().perform(&operator).await?;
        held.push(rows_of_both(&branch, &operator, &rule, &head, &mut live).await?);
    }
    assert_eq!(held, [1, 2, 1, 0]);
    Ok(())
}

/// Under `max:`, `min:`, and `top:`, a read elects one claim among each claim
/// of a holder, and compares the key after that. A rule gives each item whose
/// name is the key of the claim that board:1 elects. Item x shows while the
/// claim under `x` wins. It leaves when a claim under `y` wins, and item y
/// shows. It shows again when the claim under `y` leaves, though no claim of
/// `x` changed. After each change the live query holds the rows of a first
/// read.
#[dialog_common::test]
async fn it_follows_the_claim_that_a_pick_elects_for_a_key_that_another_premise_binds() -> Result<()>
{
    let entity = |name: &str| Value::Entity(name.parse().expect("an entity"));
    for (kind, pick, first, second) in [
        (
            json!("natural:"),
            "max:",
            Value::UnsignedInt(7),
            Value::UnsignedInt(9),
        ),
        (
            json!("natural:"),
            "min:",
            Value::UnsignedInt(7),
            Value::UnsignedInt(3),
        ),
        (
            json!(["rank:gold", "rank:silver"]),
            "top:",
            entity("rank:silver"),
            entity("rank:gold"),
        ),
    ] {
        let (branch, operator, board) = board().await?;
        let mut transaction = branch.transaction();
        for (item, name) in [("item:x", "x"), ("item:y", "y")] {
            transaction = transaction.assert(fact(
                "test.item/name",
                &item.parse()?,
                Value::String(name.into()),
            )?);
        }
        transaction.commit().publish().perform(&operator).await?;

        let head = json!({ "with": { "name": { "the": "test.named/name", "as": "text:" } } });
        let rule: DeductiveRuleDescriptor = serde_json::from_value(json!({
            "deduce": head,
            "when": [
                {
                    "assert": { "with": { "name": { "the": "test.item/name", "as": "text:" } } },
                    "where": { "this": { "?": { "name": "this" } }, "name": { "?": { "name": "name" } } }
                },
                {
                    "assert": { "with": { "settings": {
                        "the": { "domain": "test.settings", "keyed": "dictionary" },
                        "as": kind,
                        "pick": pick,
                    } } },
                    "where": {
                        "this": { "entity:": "board:1" },
                        "settings": { "the": { "?": { "name": "name" } }, "is": { "?": {} } }
                    }
                }
            ]
        }))?;
        let rule = rule.compile()?;
        let mut live = branch
            .query()
            .with(rule.clone())
            .subscribe(each_field(&head)?);
        live.poll(&operator).await?;

        let mut held = Vec::new();
        for (exists, key, value) in [
            (true, "x", &first),
            (true, "y", &second),
            (false, "y", &second),
            (true, "y", &second),
            (false, "x", &first),
            (true, "x", &first),
            (false, "y", &second),
        ] {
            claim(&branch, &operator, &board, exists, key, value.clone()).await?;
            rows_of_both(&branch, &operator, &rule, &head, &mut live).await?;
            let names: Vec<String> = live
                .results()
                .iter()
                .map(|row| row.entity().to_string())
                .collect();
            held.push(names);
        }
        assert_eq!(
            held,
            [
                vec!["item:x"],
                vec!["item:y"],
                vec!["item:x"],
                vec!["item:y"],
                vec!["item:y"],
                vec!["item:y"],
                vec!["item:x"],
            ],
            "under {pick}"
        );
    }
    Ok(())
}

/// Under `max:`, `min:`, and `top:`, a read of a field of one attribute
/// elects one claim among each claim of the entity. A rule gives each item
/// whose rank is the level that board:1 elects. Item 1 shows while the first
/// claim wins. It leaves when a second claim wins, and item 2 shows. It shows
/// again when the second claim leaves, though the first claim did not change.
/// After each change the live query holds the rows of a first read, for a
/// concept of the level alone and for a concept of the level and the name of
/// the board.
#[dialog_common::test]
async fn it_follows_the_claim_that_a_pick_elects_for_a_field_that_another_premise_joins()
-> Result<()> {
    let entity = |name: &str| Value::Entity(name.parse().expect("an entity"));
    for (kind, rank, pick, first, second) in [
        (
            json!("natural:"),
            "natural:",
            "max:",
            Value::UnsignedInt(7),
            Value::UnsignedInt(9),
        ),
        (
            json!("natural:"),
            "natural:",
            "min:",
            Value::UnsignedInt(7),
            Value::UnsignedInt(3),
        ),
        (
            json!(["rank:gold", "rank:silver"]),
            "entity:",
            "top:",
            entity("rank:silver"),
            entity("rank:gold"),
        ),
    ] {
        for named in [false, true] {
            let (branch, operator, board) = board().await?;
            let mut transaction = branch.transaction();
            for (item, value) in [("item:1", &first), ("item:2", &second)] {
                transaction =
                    transaction.assert(fact("test.item/rank", &item.parse()?, value.clone())?);
            }
            transaction.commit().publish().perform(&operator).await?;

            let mut level = json!({ "with": { "level": {
                "the": "test.board/level",
                "as": kind,
                "pick": pick,
            } } });
            let mut terms = json!({
                "this": { "entity:": "board:1" },
                "level": { "?": { "name": "rank" } }
            });
            if named {
                level["with"]["name"] = json!({ "the": "test.board/name", "as": "text:" });
                terms["name"] = json!({ "?": {} });
            }
            let head = json!({ "with": { "rank": { "the": "test.ranked/rank", "as": rank } } });
            let rule: DeductiveRuleDescriptor = serde_json::from_value(json!({
                "deduce": head,
                "when": [
                    {
                        "assert": { "with": { "rank": { "the": "test.item/rank", "as": rank } } },
                        "where": { "this": { "?": { "name": "this" } }, "rank": { "?": { "name": "rank" } } }
                    },
                    { "assert": level, "where": terms }
                ]
            }))?;
            let rule = rule.compile()?;
            let mut live = branch
                .query()
                .with(rule.clone())
                .subscribe(each_field(&head)?);
            live.poll(&operator).await?;

            let mut held = Vec::new();
            for (exists, value) in [
                (true, &first),
                (true, &second),
                (false, &second),
                (true, &second),
                (false, &first),
                (true, &first),
                (false, &second),
            ] {
                let level = fact("test.board/level", &board, value.clone())?;
                let transaction = branch.transaction();
                let transaction = if exists {
                    transaction.assert(level)
                } else {
                    transaction.retract(level)
                };
                transaction.commit().publish().perform(&operator).await?;
                rows_of_both(&branch, &operator, &rule, &head, &mut live).await?;
                let items: Vec<String> = live
                    .results()
                    .iter()
                    .map(|row| row.entity().to_string())
                    .collect();
                held.push(items);
            }
            assert_eq!(
                held,
                [
                    vec!["item:1"],
                    vec!["item:2"],
                    vec!["item:1"],
                    vec!["item:2"],
                    vec!["item:2"],
                    vec!["item:2"],
                    vec!["item:1"],
                ],
                "under {pick}, named {named}"
            );
        }
    }
    Ok(())
}

/// A read of a collection with its holder free reads the entries in the
/// order of their keys, and not in the order of the holders. A rule gives
/// each holder of 7 with its name, and the two boards hold 7 under the keys
/// `c` and `a`. A first read gives both boards, as the live query does.
#[dialog_common::test]
async fn it_reads_each_holder_of_a_collection_that_another_premise_joins() -> Result<()> {
    let (branch, operator, board) = board().await?;
    let other: Entity = "board:2".parse()?;
    branch
        .transaction()
        .assert(
            dialog_query::the!("test.board/name")
                .of(other.clone())
                .is("other".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let (rule, head) = holder_of_seven("natural:")?;
    let mut live = branch
        .query()
        .with(rule.clone())
        .subscribe(each_field(&head)?);
    live.poll(&operator).await?;

    let mut held = Vec::new();
    for (holder, key) in [(&board, "c"), (&other, "a")] {
        branch
            .transaction()
            .assert(fact(
                &format!("test.scores/{key}"),
                holder,
                Value::UnsignedInt(7),
            )?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        held.push(rows_of_both(&branch, &operator, &rule, &head, &mut live).await?);
    }
    assert_eq!(held, [1, 2]);
    Ok(())
}

/// A join of two numbers depends on the read that starts it: a read bound
/// with the natural 7 finds a stored 7.5, and a read bound with 7.5 does not
/// find a stored 7. A rule gives each item whose rank is the value of an entry
/// of board:1, and the items have the ranks 7, 8, -3, and 7.5. A live query
/// that starts from an item could give other rows than a first read, so it
/// reads such a rule again in full. The steps write and take back entries of
/// numbers of each type, in commits of one change and of two. After each step
/// the live query holds the rows of a first read, for a collection and a rank
/// with no type under `all:` and under `last:`, and for a collection and a
/// rank of `float:32` under `last:`.
#[dialog_common::test]
async fn it_holds_the_rows_of_a_first_read_where_a_join_meets_numbers_of_two_types() -> Result<()> {
    let one = |exists: bool, key: &'static str, value: Value| vec![(exists, key, value)];
    let steps: Vec<Vec<(bool, &str, Value)>> = vec![
        one(true, "a", Value::UnsignedInt(7)),
        one(true, "c", Value::Float(7.5)),
        one(false, "a", Value::UnsignedInt(7)),
        one(false, "c", Value::Float(7.5)),
        one(true, "b", Value::Float(8.0)),
        one(true, "n", Value::SignedInt(-3)),
        one(false, "b", Value::Float(8.0)),
        one(false, "n", Value::SignedInt(-3)),
        vec![
            (true, "x", Value::UnsignedInt(7)),
            (true, "x", Value::Float(7.5)),
        ],
        one(false, "x", Value::UnsignedInt(7)),
        vec![
            (true, "x", Value::UnsignedInt(7)),
            (false, "x", Value::Float(7.5)),
        ],
        vec![
            (false, "x", Value::UnsignedInt(7)),
            (true, "y", Value::UnsignedInt(7)),
        ],
        vec![
            (false, "y", Value::UnsignedInt(7)),
            (true, "y", Value::Float(7.9)),
        ],
        one(false, "y", Value::Float(7.9)),
    ];
    for (kind, pick, expected) in [
        ("", "all:", [2, 2, 1, 0, 1, 2, 1, 0, 2, 1, 2, 2, 0, 0]),
        (
            "float:32",
            "last:",
            [1, 2, 1, 0, 1, 2, 1, 0, 1, 1, 1, 1, 0, 0],
        ),
        ("", "last:", [2, 2, 1, 0, 1, 2, 1, 0, 1, 1, 2, 2, 0, 0]),
    ] {
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

        let typed = |the: &str| {
            let mut field = json!({ "the": the });
            if !kind.is_empty() {
                field["as"] = json!(kind);
            }
            field
        };
        let mut collection = typed("test.scores");
        collection["the"] = json!({ "domain": "test.scores", "keyed": "dictionary" });
        collection["pick"] = json!(pick);
        let head = json!({ "with": { "rank": typed("test.ranked/rank") } });
        let rule: DeductiveRuleDescriptor = serde_json::from_value(json!({
            "deduce": head,
            "when": [
                {
                    "assert": { "with": { "scores": collection } },
                    "where": {
                        "this": { "entity:": "board:1" },
                        "scores": { "the": { "?": { "name": "key" } }, "is": { "?": { "name": "rank" } } }
                    }
                },
                {
                    "assert": { "with": { "rank": typed("test.item/rank") } },
                    "where": { "this": { "?": { "name": "this" } }, "rank": { "?": { "name": "rank" } } }
                }
            ]
        }))?;
        let rule = rule.compile()?;
        let mut live = branch
            .query()
            .with(rule.clone())
            .subscribe(each_field(&head)?);
        live.poll(&operator).await?;

        let mut held = Vec::new();
        for changes in &steps {
            let mut transaction = branch.transaction();
            for (exists, key, value) in changes {
                let entry = fact(&format!("test.scores/{key}"), &board, value.clone())?;
                transaction = if *exists {
                    transaction.assert(entry)
                } else {
                    transaction.retract(entry)
                };
            }
            transaction.commit().publish().perform(&operator).await?;
            held.push(rows_of_both(&branch, &operator, &rule, &head, &mut live).await?);
        }
        assert_eq!(held, expected, "a field of {kind:?} under {pick}");
    }
    Ok(())
}

/// A log with no type can hold an entry of text. A rule reads each message
/// that the log of channel 0 holds, takes the message as the entity of
/// another premise, and gives the topic of the channel of the message. An
/// entry of text holds no message, so it changes no row: the live query polls
/// without a fault, and holds the rows of a first read.
#[dialog_common::test]
async fn it_passes_over_an_entry_that_is_no_entity_where_a_premise_takes_it_as_one() -> Result<()> {
    let (branch, operator, _) = opened(Keyed::Dictionary, &[3]).await?;
    let general = channel(0);
    let of: Entity = general.parse()?;
    branch
        .transaction()
        .assert(
            dialog_query::the!("chat.channel/topic")
                .of(of.clone())
                .is("zero".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let head = json!({ "with": { "topic": { "the": "test.active/topic", "as": "text:" } } });
    let rule: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": head,
        "when": [
            {
                "assert": { "with": { "log": {
                    "the": { "domain": "chat.channel.log", "keyed": "dictionary" },
                    "pick": "all:",
                } } },
                "where": {
                    "this": { "entity:": general },
                    "log": { "the": { "?": { "name": "key" } }, "is": { "?": { "name": "member" } } }
                }
            },
            {
                "assert": { "with": { "channel": { "the": "chat.message/channel", "as": "entity:" } } },
                "where": { "this": { "?": { "name": "member" } }, "channel": { "?": { "name": "this" } } }
            },
            {
                "assert": { "with": { "topic": { "the": "chat.channel/topic", "as": "text:" } } },
                "where": { "this": { "?": { "name": "this" } }, "topic": { "?": { "name": "topic" } } }
            }
        ]
    }))?;
    let rule = rule.compile()?;
    let mut live = branch
        .query()
        .with(rule.clone())
        .subscribe(each_field(&head)?);
    assert_eq!(
        rows_of_both(&branch, &operator, &rule, &head, &mut live).await?,
        1
    );

    let text = fact(
        "chat.channel.log/n00900",
        &of,
        Value::String("not an entity".into()),
    )?;
    branch
        .transaction()
        .assert(text.clone())
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    assert_eq!(
        rows_of_both(&branch, &operator, &rule, &head, &mut live).await?,
        1
    );
    branch
        .transaction()
        .retract(text)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    assert_eq!(
        rows_of_both(&branch, &operator, &rule, &head, &mut live).await?,
        1
    );
    Ok(())
}

/// A collection with no type can hold an entry of any value. A rule gives
/// each member of the collection of a board unless the member is hidden, and
/// only the negated premise takes the member as its entity. A negated premise
/// holds where its fact is absent, so an entry of text or of a number is a
/// member too. The steps write an entry of an entity, of text, and of a
/// number, and take back the last two. After each step the live query holds
/// the rows of a first read.
#[dialog_common::test]
async fn it_reads_an_entry_that_is_no_entity_where_only_a_negated_premise_takes_it_as_one()
-> Result<()> {
    let (branch, operator, board) = board().await?;
    let head = json!({ "with": { "member": { "the": "test.kept/member" } } });
    let rule: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": head,
        "when": [
            {
                "assert": { "with": { "scores": {
                    "the": { "domain": "test.scores", "keyed": "dictionary" },
                    "pick": "all:",
                } } },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "scores": { "the": { "?": { "name": "key" } }, "is": { "?": { "name": "member" } } }
                }
            }
        ],
        "unless": [
            {
                "assert": { "with": { "hidden": { "the": "test.msg/hidden", "as": "text:" } } },
                "where": { "this": { "?": { "name": "member" } }, "hidden": { "?": {} } }
            }
        ]
    }))?;
    let rule = rule.compile()?;
    let mut live = branch
        .query()
        .with(rule.clone())
        .subscribe(each_field(&head)?);
    live.poll(&operator).await?;

    let steps = [
        (true, "e", Value::Entity("item:1".parse()?)),
        (true, "t", Value::String("text".into())),
        (true, "n", Value::UnsignedInt(7)),
        (false, "t", Value::String("text".into())),
        (false, "n", Value::UnsignedInt(7)),
    ];
    for (exists, key, value) in steps {
        let entry = fact(&format!("test.scores/{key}"), &board, value)?;
        let transaction = branch.transaction();
        let transaction = if exists {
            transaction.assert(entry)
        } else {
            transaction.retract(entry)
        };
        transaction.commit().publish().perform(&operator).await?;
        assert_eq!(
            rows_of_both(&branch, &operator, &rule, &head, &mut live).await?,
            1
        );
    }
    Ok(())
}

/// Polls `live`, and compares the rows that it holds with the rows that a
/// new query of the same `rules` reads. Gives the number of rows.
async fn rows_of_rules(
    branch: &Branch,
    operator: &Operator,
    rules: &[DeductiveRule],
    head: &Json,
    live: &mut Subscription<ConceptQuery>,
) -> Result<usize> {
    live.poll(operator).await?;
    let mut query = branch.query();
    for rule in rules {
        query = query.with(rule.clone());
    }
    let read = query
        .subscribe(each_field(head)?)
        .poll(operator)
        .await?
        .map_or(Vec::new(), |delta| delta.asserted);
    let held = live.results();
    assert_eq!(held.len(), read.len(), "the live query and a first read");
    assert!(held.iter().all(|row| read.contains(row)));
    Ok(held.len())
}

/// The rows of a live query of `head` under `rules`, which join the entries
/// of the collection `test.scores` of board:1 with the ranks of items, after
/// each step of a list. The items have the ranks 7, 8, -3, and 7.5 at the
/// start. With `items` the collection holds 7 and 7.5, and the steps write
/// and take back the ranks of items. Without it, the steps write and take back
/// entries of numbers of each type, in commits of one change and of two.
/// After each step the live query holds the rows of a first read.
async fn ranked_rows(rules: &[DeductiveRule], head: &Json, items: bool) -> Result<Vec<usize>> {
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
    if items {
        transaction = transaction
            .assert(fact("test.scores/a", &board, Value::UnsignedInt(7))?)
            .assert(fact("test.scores/c", &board, Value::Float(7.5))?);
    }
    transaction.commit().publish().perform(&operator).await?;
    let mut query = branch.query();
    for rule in rules {
        query = query.with(rule.clone());
    }
    let mut live = query.subscribe(each_field(head)?);
    live.poll(&operator).await?;

    let one = |exists: bool, key: &'static str, value: Value| vec![(exists, key, value)];
    let steps: Vec<Vec<(bool, &str, Value)>> = if items {
        vec![
            one(true, "item:5", Value::Float(7.5)),
            one(true, "item:6", Value::UnsignedInt(7)),
            one(false, "item:1", Value::UnsignedInt(7)),
            one(false, "item:4", Value::Float(7.5)),
            one(true, "item:7", Value::SignedInt(7)),
            one(false, "item:6", Value::UnsignedInt(7)),
            one(true, "item:1", Value::Float(7.0)),
        ]
    } else {
        vec![
            one(true, "a", Value::UnsignedInt(7)),
            one(true, "c", Value::Float(7.5)),
            one(false, "a", Value::UnsignedInt(7)),
            one(false, "c", Value::Float(7.5)),
            one(true, "b", Value::Float(8.0)),
            one(true, "n", Value::SignedInt(-3)),
            one(false, "b", Value::Float(8.0)),
            one(false, "n", Value::SignedInt(-3)),
            vec![
                (true, "x", Value::UnsignedInt(7)),
                (true, "x", Value::Float(7.5)),
            ],
            one(false, "x", Value::UnsignedInt(7)),
            vec![
                (true, "x", Value::UnsignedInt(7)),
                (false, "x", Value::Float(7.5)),
            ],
            vec![
                (false, "x", Value::UnsignedInt(7)),
                (true, "y", Value::UnsignedInt(7)),
            ],
            vec![
                (false, "y", Value::UnsignedInt(7)),
                (true, "y", Value::Float(7.9)),
            ],
            one(false, "y", Value::Float(7.9)),
        ]
    };
    let mut held = Vec::new();
    for changes in steps {
        let mut transaction = branch.transaction();
        for (exists, name, value) in changes {
            let change = if items {
                fact("test.item/rank", &name.parse()?, value)?
            } else {
                fact(&format!("test.scores/{name}"), &board, value)?
            };
            transaction = if exists {
                transaction.assert(change)
            } else {
                transaction.retract(change)
            };
        }
        transaction.commit().publish().perform(&operator).await?;
        held.push(rows_of_rules(&branch, &operator, rules, head, &mut live).await?);
    }
    Ok(held)
}

/// A rule can join two numbers through a premise that binds one variable from
/// another: an equality (`==`), or `coalesce`. A rule gives each item whose
/// rank is the value of an entry of board:1, where the entry and the rank are
/// two variables that such a premise links. Neither has a type, so the join
/// depends on the side it starts from, as a join through one variable does.
/// After each step the live query holds the rows of a first read, under
/// `all:` and under `last:`.
#[dialog_common::test]
async fn it_holds_the_rows_of_a_first_read_where_a_join_of_two_numbers_passes_through_a_link()
-> Result<()> {
    let head = json!({ "with": { "rank": { "the": "test.ranked/rank" } } });
    let links = [
        json!({ "assert": "==", "where": {
            "this": { "?": { "name": "value" } },
            "is": { "?": { "name": "rank" } }
        } }),
        json!({ "assert": "coalesce", "where": {
            "source": { "?": { "name": "value" } },
            "fallback": 0,
            "is": { "?": { "name": "rank" } }
        } }),
    ];
    for link in links {
        for pick in ["all:", "last:"] {
            let rule: DeductiveRuleDescriptor = serde_json::from_value(json!({
                "deduce": head,
                "when": [
                    {
                        "assert": { "with": { "scores": {
                            "the": { "domain": "test.scores", "keyed": "dictionary" },
                            "pick": pick,
                        } } },
                        "where": {
                            "this": { "entity:": "board:1" },
                            "scores": { "the": { "?": { "name": "key" } }, "is": { "?": { "name": "value" } } }
                        }
                    },
                    link,
                    {
                        "assert": { "with": { "rank": { "the": "test.item/rank" } } },
                        "where": { "this": { "?": { "name": "this" } }, "rank": { "?": { "name": "rank" } } }
                    }
                ]
            }))?;
            let rules = [rule.compile()?];
            let entries = match pick {
                "all:" => [2, 2, 1, 0, 1, 2, 1, 0, 2, 1, 2, 2, 0, 0],
                _ => [2, 2, 1, 0, 1, 2, 1, 0, 1, 1, 2, 2, 0, 0],
            };
            let what = format!("{} under {pick}", link["assert"]);
            assert_eq!(ranked_rows(&rules, &head, false).await?, entries, "{what}");
            assert_eq!(
                ranked_rows(&rules, &head, true).await?,
                [3, 4, 3, 2, 3, 2, 3],
                "{what}, as the ranks change"
            );
        }
    }
    Ok(())
}

/// A change of one fact: whether it is asserted, its attribute, its entity,
/// and its value.
type Write = (bool, &'static str, &'static str, Value);

/// Commits `writes` to `branch` in one transaction.
async fn commit_writes(branch: &Branch, operator: &Operator, writes: &[Write]) -> Result<()> {
    let mut transaction = branch.transaction();
    for (exists, the, of, value) in writes {
        let change = fact(the, &of.parse()?, value.clone())?;
        transaction = if *exists {
            transaction.assert(change)
        } else {
            transaction.retract(change)
        };
    }
    transaction.commit().publish().perform(operator).await?;
    Ok(())
}

/// Opens a live query of `head` under `rule` over the facts of `setup`, and
/// then commits each step. After each step the live query holds the rows of
/// a first read. Gives how many times the live query evaluated its rule in
/// full, the first poll included.
async fn evaluations_in_full(
    rule: &DeductiveRule,
    head: &Json,
    setup: &[Write],
    steps: &[Vec<Write>],
) -> Result<usize> {
    let (branch, operator) = empty().await?;
    commit_writes(&branch, &operator, setup).await?;
    let mut live = branch
        .query()
        .with(rule.clone())
        .subscribe(each_field(head)?);
    live.poll(&operator).await?;
    for step in steps {
        commit_writes(&branch, &operator, step).await?;
        rows_of_both(&branch, &operator, rule, head, &mut live).await?;
    }
    Ok(live.recomputes())
}

/// The chat of Sapling keeps its live queries on the incremental path. The
/// model of a message is typed as Sapling writes it (`MESSAGE` in
/// `tests/support/chat_log.rs` of Sapling). A segment of the log of a
/// channel joins each entry of the log, from a lower key up to an upper key,
/// to the message that it holds, as the renderer writes it
/// (`collectionBody`). A list of the model reads each message, with or
/// without a lower bound on its time (`segmentBody`). Messages arrive, a text
/// changes, and an entry leaves the log. After each step each live query
/// holds the rows of a first read, and it evaluated its rule in full once, at
/// its first poll.
#[dialog_common::test]
async fn it_keeps_each_live_query_of_the_chat_on_the_incremental_path() -> Result<()> {
    let variable = |name: &str| json!({ "?": { "name": name } });
    let message = json!({ "with": {
        "channel": { "the": "chat.message/channel", "as": "Entity", "cardinality": "one" },
        "author": { "the": "chat.message/author", "as": "Entity", "cardinality": "one" },
        "time": { "the": "chat.message/time", "as": "UnsignedInteger", "cardinality": "one" },
        "text": { "the": "chat.message/text", "as": "Text", "cardinality": "one" },
    } });
    let log = json!({ "with": { "log": {
        "the": { "domain": "chat.channel.log", "keyed": "sequence" },
        "as": "entity:",
        "pick": "all:",
    } } });
    let posted = |message: &'static str, entry: &'static str, time: u64| -> Result<Vec<Write>> {
        let entity = |name: &str| -> Result<Value> { Ok(Value::Entity(name.parse()?)) };
        Ok(vec![
            (true, "chat.message/channel", message, entity("channel:0")?),
            (true, "chat.message/author", message, entity("person:1")?),
            (
                true,
                "chat.message/time",
                message,
                Value::UnsignedInt(time.into()),
            ),
            (
                true,
                "chat.message/text",
                message,
                Value::String(format!("hi {time}")),
            ),
            (true, entry, "channel:0", entity(message)?),
        ])
    };
    let setup = posted("message:1", "chat.channel.log/b0", 1)?;
    let steps = vec![
        posted("message:2", "chat.channel.log/b1", 2)?,
        posted("message:3", "chat.channel.log/a5", 3)?,
        vec![(
            true,
            "chat.message/text",
            "message:1",
            Value::String("edited".into()),
        )],
        vec![(
            false,
            "chat.channel.log/b1",
            "channel:0",
            Value::Entity("message:2".parse()?),
        )],
    ];
    // The head and the read of the model: each field of a message under
    // `sapling.segment/`, with the key of an entry beside them for a segment
    // of the log.
    let model = |key: bool| {
        let mut head = Map::new();
        if key {
            head.insert(
                "key".into(),
                json!({ "the": "sapling.segment/key", "as": "text:", "pick": "all:" }),
            );
        }
        let mut fields = Map::new();
        fields.insert("this".into(), variable("this"));
        for (field, descriptor) in message["with"].as_object().expect("the fields") {
            head.insert(
                field.clone(),
                json!({ "the": format!("sapling.segment/{field}"), "as": descriptor["as"] }),
            );
            fields.insert(field.clone(), variable(field));
        }
        (
            json!({ "with": head }),
            json!({ "assert": message, "where": fields }),
        )
    };
    for (lower, upper) in [
        (None, None),
        (Some("a0"), None),
        (None, Some("b1")),
        (Some("a0"), Some("b1")),
    ] {
        let (head, read) = model(true);
        let mut when = vec![json!({
            "assert": log,
            "where": {
                "this": { "entity:": "channel:0" },
                "log": { "the": variable("key"), "is": variable("this") },
            },
        })];
        for (assert, key) in [(">=", lower), ("<", upper)] {
            if let Some(key) = key {
                when.push(json!({
                    "assert": assert,
                    "where": { "of": variable("key"), "with": { "text:": key } },
                }));
            }
        }
        when.push(read);
        let rule: DeductiveRuleDescriptor =
            serde_json::from_value(json!({ "deduce": head, "when": when }))?;
        assert_eq!(
            evaluations_in_full(&rule.compile()?, &head, &setup, &steps).await?,
            1,
            "a segment of the log from {lower:?} up to {upper:?}"
        );
    }
    for lower in [None, Some(2)] {
        let (head, read) = model(false);
        let mut when = vec![read];
        if let Some(lower) = lower {
            when.push(
                json!({ "assert": ">=", "where": { "of": variable("time"), "with": lower } }),
            );
        }
        let rule: DeductiveRuleDescriptor =
            serde_json::from_value(json!({ "deduce": head, "when": when }))?;
        assert_eq!(
            evaluations_in_full(&rule.compile()?, &head, &setup, &steps).await?,
            1,
            "a list of the model with the time at least {lower:?}"
        );
    }
    Ok(())
}

/// A list of a model with a filter on one field, as the renderer writes it
/// (`segmentBody`): each entity of the model, with its fields, whose `score`
/// equals `constant`, and whose name is at least `a`. With `owner`, the rule
/// reads the model of the item that an owner names instead, so a change to an
/// item reaches the owner through a join.
fn filtered(kind: &str, constant: Json, owner: bool) -> Result<(DeductiveRule, Json)> {
    let variable = |name: &str| json!({ "?": { "name": name } });
    let model = json!({ "with": {
        "name": { "the": "rv.m/name", "as": "Text", "cardinality": "one" },
        "score": { "the": "rv.m/score", "as": kind, "cardinality": "one" },
    } });
    let mut head = Map::new();
    if owner {
        head.insert(
            "item".into(),
            json!({ "the": "rv.out/item", "as": "Entity" }),
        );
    }
    for (field, descriptor) in model["with"].as_object().expect("the fields") {
        head.insert(
            field.clone(),
            json!({ "the": format!("rv.out/{field}"), "as": descriptor["as"] }),
        );
    }
    let item = if owner { "item" } else { "this" };
    let mut when = vec![
        json!({ "assert": model, "where": {
            "this": variable(item), "name": variable("name"), "score": variable("score"),
        } }),
        json!({ "assert": "==", "where": { "this": variable("score"), "is": constant } }),
        json!({ "assert": ">=", "where": { "of": variable("name"), "with": { "text:": "a" } } }),
    ];
    if owner {
        when.push(json!({
            "assert": { "with": { "item": { "the": "rv.o/item", "as": "Entity" } } },
            "where": { "this": variable("this"), "item": variable("item") },
        }));
    }
    let head = json!({ "with": head });
    let rule: DeductiveRuleDescriptor =
        serde_json::from_value(json!({ "deduce": head, "when": when }))?;
    Ok((rule.compile()?, head))
}

/// The renderer writes a filter of a number as bare digits, so `?score == 3`
/// on a field of integers compares the field with the natural 3. A natural
/// and an integer read each number alike where both see it, so the filter
/// finds the same items from either side, and the live query keeps its
/// incremental path. A filter of a float on a field of integers, or of a
/// natural on a field of floats, reads a fraction in two ways, so that live
/// query evaluates its rule in full at each poll. Items arrive with scores of
/// each number type, and scores change and leave. After each step the live
/// query holds the rows of a first read, for a list of the model and for a
/// list that reaches the model through an owner.
#[dialog_common::test]
async fn it_keeps_a_filter_of_a_whole_number_on_the_incremental_path() -> Result<()> {
    let item = |name: &'static str, n: &'static str, score: Value| {
        vec![
            (true, "rv.m/name", name, Value::String(n.into())),
            (true, "rv.m/score", name, score),
        ]
    };
    let mut setup = item("item:1", "a1", Value::SignedInt(3));
    setup.extend(item("item:2", "a2", Value::SignedInt(-3)));
    for (owner, item) in [
        ("owner:1", "item:1"),
        ("owner:2", "item:2"),
        ("owner:3", "item:3"),
        ("owner:4", "item:4"),
        ("owner:5", "item:5"),
        ("owner:6", "item:6"),
    ] {
        setup.push((true, "rv.o/item", owner, Value::Entity(item.parse()?)));
    }
    let steps = vec![
        item("item:3", "a3", Value::SignedInt(3)),
        item("item:4", "a4", Value::UnsignedInt(3)),
        item("item:5", "a5", Value::Float(3.0)),
        item("item:6", "a6", Value::Float(3.5)),
        vec![(false, "rv.m/score", "item:1", Value::SignedInt(3))],
        vec![(true, "rv.m/name", "item:3", Value::String("b3".into()))],
        vec![(true, "rv.m/score", "item:2", Value::SignedInt(3))],
        vec![(false, "rv.m/score", "item:4", Value::UnsignedInt(3))],
        vec![(true, "rv.m/score", "item:4", Value::SignedInt(-3))],
        vec![(true, "rv.m/score", "item:1", Value::Float(3.5))],
    ];
    let float = |text: &str| -> Result<Json> { Ok(serde_json::from_str(text)?) };
    for owner in [false, true] {
        for (kind, constant, incremental) in [
            ("SignedInteger", json!(3), true),
            ("SignedInteger", json!(-3), true),
            ("UnsignedInteger", json!(3), true),
            ("Float", float("3.0")?, true),
            ("SignedInteger", float("3.0")?, false),
            ("UnsignedInteger", float("3.0")?, false),
            ("Float", json!(3), false),
        ] {
            let what = format!("{kind} == {constant}, through an owner: {owner}");
            let (rule, head) = filtered(kind, constant, owner)?;
            let full = evaluations_in_full(&rule, &head, &setup, &steps).await?;
            if incremental {
                assert_eq!(full, 1, "{what}");
            } else {
                assert!(full > 1, "{what}");
            }
        }
    }
    Ok(())
}

/// A constant that is no number is never equal to a number. A filter of text
/// on a field with no type, whose items hold text and numbers, keeps its
/// incremental path, and after each step the live query holds the rows of a
/// first read.
#[dialog_common::test]
async fn it_keeps_a_filter_of_text_on_a_field_with_no_type_on_the_incremental_path() -> Result<()> {
    let variable = |name: &str| json!({ "?": { "name": name } });
    let head = json!({ "with": { "tag": { "the": "rv.out/tag" } } });
    let rule: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": head,
        "when": [
            {
                "assert": { "with": { "tag": { "the": "rv.m/tag" } } },
                "where": { "this": variable("this"), "tag": variable("tag") },
            },
            { "assert": "==", "where": { "this": variable("tag"), "is": "x" } },
        ]
    }))?;
    let tag = |exists: bool, item: &'static str, value: Value| (exists, "rv.m/tag", item, value);
    let setup = vec![tag(true, "item:1", Value::String("x".into()))];
    let steps = vec![
        vec![tag(true, "item:2", Value::UnsignedInt(3))],
        vec![tag(true, "item:3", Value::String("x".into()))],
        vec![tag(true, "item:4", Value::Float(3.5))],
        vec![tag(false, "item:1", Value::String("x".into()))],
        vec![tag(true, "item:2", Value::String("x".into()))],
    ];
    assert_eq!(
        evaluations_in_full(&rule.compile()?, &head, &setup, &steps).await?,
        1
    );
    Ok(())
}
