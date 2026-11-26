//! A window of a sorted list, moved: the read a virtualized display makes
//! when it scrolls. A list of `size` messages, each with a position and a
//! body, and a window of [`WIDTH`] positions over it. One form writes the
//! window's bounds as constants of the rule of each segment, as a display
//! that opens one live query per segment does. The other reads the bounds
//! from the facts of a window entity, so the rule joins them, as a display
//! that moves one window by a command does.
//!
//! Each reports the rows that the subscription's fact scans gave to its
//! evaluations in the measured phase, as the counter `rows.read`: what the
//! move cost in reads, whatever the instructions of a read come to.

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use dialog_corpus::{Cardinality, Value};
use dialog_query::{ConceptDescriptor, ConceptQuery, DeductiveRule};
use dialog_repository::Subscription;

use super::{Outcome, Prepared};
use crate::env::{Env, assert_all, concept, entity, fact, query, rule, text, var};

/// How many positions a window holds.
const WIDTH: usize = 50;

/// The fields of a row, in its head and in the message it reads.
const FIELDS: [(&str, &str); 2] = [("at", "integer:"), ("body", "text:")];

/// The first window of each scenario starts here, and the move goes to
/// the window after it.
fn start(size: usize) -> usize {
    size / 2
}

/// A concept of `namespace` with the fields of a row.
fn fields(namespace: &str) -> serde_json::Value {
    let mut with = serde_json::Map::new();
    for (name, kind) in FIELDS {
        with.insert(
            name.to_string(),
            serde_json::json!({ "the": format!("{namespace}/{name}"), "as": kind }),
        );
    }
    serde_json::json!({ "with": with })
}

/// The premise that reads a message with each field of a row.
fn message() -> serde_json::Value {
    serde_json::json!({
        "assert": fields("message"),
        "where": { "this": var("this"), "at": var("at"), "body": var("body") }
    })
}

/// The comparisons that keep a position of `[lower, upper)`.
fn within(lower: serde_json::Value, upper: serde_json::Value) -> [serde_json::Value; 2] {
    [
        serde_json::json!({ "assert": ">=", "where": { "of": var("at"), "with": lower } }),
        serde_json::json!({ "assert": "<", "where": { "of": var("at"), "with": upper } }),
    ]
}

/// The head of the rows of `namespace`, and the query of each of them.
fn rows(namespace: &str) -> (ConceptDescriptor, ConceptQuery) {
    let head = concept(fields(namespace));
    let query = query(&head, &["at", "body"], None);
    (head, query)
}

/// The rule of the segment `[lower, lower + WIDTH)`, its bounds constants,
/// with a head of its own.
fn segment(lower: usize) -> (DeductiveRule, ConceptQuery) {
    let namespace = format!("window.segment.{lower}");
    let [above, below] = within(
        serde_json::json!(lower as i64),
        serde_json::json!((lower + WIDTH) as i64),
    );
    let rule = rule(serde_json::json!({
        "deduce": fields(&namespace),
        "when": [message(), above, below]
    }));
    (rule, rows(&namespace).1)
}

/// The rule of the rows a window entity holds: its bounds read from the
/// window's facts, then compared with each position.
fn paged() -> (DeductiveRule, ConceptQuery) {
    let [above, below] = within(var("lower"), var("upper"));
    let rule = rule(serde_json::json!({
        "deduce": fields("window.page"),
        "when": [
            {
                "assert": { "with": {
                    "lower": { "the": "window/lower", "as": "integer:" },
                    "upper": { "the": "window/upper", "as": "integer:" }
                }},
                "where": {
                    "this": { "entity:": window().to_string() },
                    "lower": var("lower"),
                    "upper": var("upper")
                }
            },
            message(),
            above,
            below
        ]
    }));
    (rule, rows("window.page").1)
}

/// The entity of the window.
fn window() -> dialog_query::Entity {
    entity("window", 0)
}

/// The facts of the window at `[lower, lower + WIDTH)`.
fn bounds(lower: usize) -> [dialog_corpus::Fact; 2] {
    [
        fact(&window(), "window/lower", Value::SignedInt(lower as i128)),
        fact(
            &window(),
            "window/upper",
            Value::SignedInt((lower + WIDTH) as i128),
        ),
    ]
}

/// An env holding `size` messages, position `index` for the `index`th.
async fn list(size: usize) -> Result<Env> {
    let mut env = Env::open().await?;
    let facts = (0..size).flat_map(|index| {
        let this = entity("message", index);
        [
            fact(&this, "message/at", Value::SignedInt(index as i128)),
            fact(&this, "message/body", text(format!("message {index}"))),
        ]
    });
    env.commit(assert_all(facts, Cardinality::One)).await?;
    env.reopen().await?;
    Ok(env)
}

/// What a poll of `subscription` asserted and retracted, and the rows its
/// evaluations read since `before`, counted into the env.
fn outcome(env: &Env, asserted: usize, retracted: usize, read: usize) -> Outcome {
    env.count("rows.read", read as u64);
    Outcome::from([
        ("asserted".into(), asserted as u64),
        ("retracted".into(), retracted as u64),
    ])
}

struct Constant {
    env: Env,
    query: Option<ConceptQuery>,
}

impl Prepared for Constant {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let query = self
                .query
                .take()
                .ok_or_else(|| anyhow::anyhow!("the measured phase runs once"))?;
            let mut subscription = self.env.branch.subscribe(query);
            let delta = subscription.poll(&self.env.operator).await?;
            let asserted = delta.map(|delta| delta.asserted.len()).unwrap_or(0);
            Ok(outcome(&self.env, asserted, 0, subscription.rows_read()))
        })
    }
}

/// `window-constant`: `size` messages and the rules of two segments of
/// [`WIDTH`] positions, the second just past the first, each with its
/// bounds as constants. The first segment's subscription is polled in
/// setup; the measured phase opens the second and polls it once, as a
/// display that scrolls one segment on does.
pub async fn constant(size: usize) -> Result<Box<dyn Prepared>> {
    let env = list(size).await?;
    let (first, shown) = segment(start(size));
    let (next, query) = segment(start(size) + WIDTH);
    env.install(&[first, next]).await?;
    let mut showing = env.branch.subscribe(shown);
    let initial = showing.poll(&env.operator).await?;
    anyhow::ensure!(
        initial.is_some_and(|delta| delta.asserted.len() == WIDTH),
        "the first segment answers its {WIDTH} rows"
    );
    Ok(Box::new(Constant {
        env,
        query: Some(query),
    }))
}

struct Joined {
    env: Env,
    subscription: Subscription<ConceptQuery>,
    size: usize,
}

impl Prepared for Joined {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let before = self.subscription.rows_read();
            self.env
                .commit(assert_all(
                    bounds(start(self.size) + WIDTH),
                    Cardinality::One,
                ))
                .await?;
            let delta = self.subscription.poll(&self.env.operator).await?;
            let (asserted, retracted) = delta
                .as_ref()
                .map(|delta| (delta.asserted.len(), delta.retracted.len()))
                .unwrap_or((0, 0));
            let read = self.subscription.rows_read().saturating_sub(before);
            Ok(outcome(&self.env, asserted, retracted, read))
        })
    }
}

/// `window-joined`: `size` messages, a window entity whose facts hold its
/// bounds, and the rule of the rows the window holds, which joins those
/// bounds. The subscription is polled in setup; the measured phase moves
/// the window one width on, by a commit of its bounds, and polls once.
pub async fn joined(size: usize) -> Result<Box<dyn Prepared>> {
    let env = list(size).await?;
    let (paged, query) = paged();
    env.install(&[paged]).await?;
    env.commit(assert_all(bounds(start(size)), Cardinality::One))
        .await?;
    let mut subscription = env.branch.subscribe(query);
    let initial = subscription.poll(&env.operator).await?;
    anyhow::ensure!(
        initial.is_some_and(|delta| delta.asserted.len() == WIDTH),
        "the window answers its {WIDTH} rows"
    );
    Ok(Box::new(Joined {
        env,
        subscription,
        size,
    }))
}
