//! The scenarios: one per path an application drives through the
//! engine. Each prepares its environment (seeding, rules, warm-up) off
//! the measured phase, then runs the phase the report is about.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use dialog_artifacts::Pick;
use dialog_query::ConceptDescriptor;

use crate::env::{Env, assert_all, concept, entity, fact, text};

mod commit;
mod number;
mod read;
mod replica;
mod rule;
mod stack;
mod subscribe;
mod transaction;

/// What a measured phase produced, by name: rows read, facts committed.
pub type Outcome = BTreeMap<String, u64>;

/// A scenario after its setup, ready to run its measured phase once.
pub trait Prepared {
    /// The environment the scenario runs in.
    fn env(&self) -> &Env;

    /// Run the measured phase.
    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>>;
}

type Prepare = fn(usize) -> Pin<Box<dyn Future<Output = Result<Box<dyn Prepared>>>>>;

/// A scenario in the catalog: its name, default size and what it measures.
pub struct Spec {
    /// The name `perf run` takes and the report carries.
    pub name: &'static str,
    /// The size the baseline is recorded at.
    pub size: usize,
    /// What the measured phase is, in a line.
    pub about: &'static str,
    /// Whether the phase's instruction count varies between runs of one
    /// binary by more than the gate allows, so the gate holds it to its
    /// counters alone. Each such scenario says why in the catalog.
    pub volatile: bool,
    /// Instruction growth, in percent, allowed on top of the tolerance
    /// when this scenario is compared against a sweep that ran on a
    /// different CPU model or feature set: the spread across CPU models
    /// past the spread within one, from eight identical CI sweeps (#653).
    /// Runtime CPU dispatch makes the difference; zero where it is under
    /// 0.2%. Rough: five runs on one model and one on each of three others.
    pub cross_cpu_pct: f64,
    prepare: Prepare,
}

impl Spec {
    /// Run the scenario's setup at `size`.
    pub async fn prepare(&self, size: usize) -> Result<Box<dyn Prepared>> {
        (self.prepare)(size).await
    }
}

/// Every scenario, in the order a sweep runs them.
pub fn catalog() -> Vec<Spec> {
    vec![
        Spec {
            name: "commit-batch",
            size: 1000,
            about: "one transaction asserting two facts per entity under `all`, committed and published",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(commit::batch(size)),
        },
        Spec {
            name: "commit-rows",
            size: 100,
            about: "one commit per entity: the per-commit cost of records, signing and publication",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(commit::rows(size)),
        },
        Spec {
            name: "commit-hot-key",
            size: 100,
            about: "one commit per entity, each also re-asserting one standing fact: the per-commit cost of a key every commit rewrites",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(commit::hot_key(size)),
        },
        Spec {
            name: "commit-succeed",
            size: 500,
            about: "one transaction rewriting every entity's name under `last`: each write elects and retracts a standing claim",
            // Its commit's tree write varies between runs of one binary
            // (1.60 G to 1.85 G instructions, every counter equal), in
            // the delete path's forced merges. Ten runs on one machine
            // spread 8% to 12% with identities seeded or not, and with
            // std's hash keys fixed: not identities, not hash order.
            // Gated on counters.
            volatile: true,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(commit::succeed(size)),
        },
        Spec {
            name: "commit-derived",
            size: 200,
            about: "one transaction writing under `last` to a relation a rule also derives: the commit settles each cell through the rule",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(commit::derived(size)),
        },
        Spec {
            name: "transaction-reads",
            size: 300,
            about: "a transaction that reads the cell it just wrote, for every entity, then commits: the library-install shape",
            // Once volatile (263 M to 394 M instructions across runs of
            // one binary). Ten runs of one binary now hold within 0.3%,
            // every counter equal, so its instructions are gated again.
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(transaction::reads(size)),
        },
        Spec {
            name: "first-query",
            size: 200,
            about: "one point read on a replica that adopted, by root, a head `size` one-commit origins deep: the first query of a device that faults blocks in",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(replica::first_query(size)),
        },
        Spec {
            name: "read-point",
            size: 2000,
            about: "two hundred single-entity reads of one attribute over a seeded branch",
            volatile: false,
            cross_cpu_pct: 0.9,
            prepare: |size| Box::pin(read::point(size)),
        },
        Spec {
            name: "read-scan",
            size: 2000,
            about: "one concept read over a single attribute, every entity",
            volatile: false,
            cross_cpu_pct: 0.2,
            prepare: |size| Box::pin(read::scan(size)),
        },
        Spec {
            name: "query-join",
            size: 2000,
            about: "one two-attribute concept read: the planner's join",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(read::join(size)),
        },
        Spec {
            name: "rule-scan",
            size: 2000,
            about: "one read of a concept a committed rule derives from two stored attributes, every entity",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(rule::scan(size)),
        },
        Spec {
            name: "rule-point",
            size: 2000,
            about: "two hundred single-entity reads of a concept a committed rule derives",
            volatile: false,
            cross_cpu_pct: 2.4,
            prepare: |size| Box::pin(rule::point(size)),
        },
        Spec {
            name: "rule-recursive",
            size: 20,
            about: "one read of a transitive closure over `size` chains of depth 25, derived by a base and a recursive rule",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(rule::recursive(size)),
        },
        Spec {
            name: "rule-ranked",
            size: 1000,
            about: "one read of a ranked field four rules each state a case for, elected best first, every entity",
            volatile: false,
            cross_cpu_pct: 0.5,
            prepare: |size| Box::pin(rule::ranked(size)),
        },
        Spec {
            name: "rule-install",
            size: 100,
            about: "one transaction committing `size` rules, then the first read of the last one: discovery and hydration cold",
            // Once volatile (221 M to 234 M instructions across runs of
            // one binary). Ten runs of one binary now hold within 0.4%,
            // every counter equal, so its instructions are gated again.
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(rule::install(size)),
        },
        Spec {
            name: "rule-overlay",
            size: 2000,
            about: "two hundred single-entity reads of a concept a rule derives, each read carrying the rule in its own overlay",
            volatile: false,
            cross_cpu_pct: 0.3,
            prepare: |size| Box::pin(rule::overlay(size)),
        },
        Spec {
            name: "rule-session",
            size: 2000,
            about: "two hundred rounds of a session-overlay fact that is no rule, then a single-entity read of a concept a committed rule derives",
            volatile: false,
            cross_cpu_pct: 0.3,
            prepare: |size| Box::pin(rule::session(size)),
        },
        Spec {
            name: "rule-dispatch",
            size: 500,
            about: "one transaction asserting a fact per entity that a chain of three inductive rules relays: a round of induction per link",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(commit::dispatch(size)),
        },
        Spec {
            name: "number-commit",
            size: 1000,
            about: "one transaction asserting a timestamp, a price, a count and a size per item: how numbers key",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(number::commit(size)),
        },
        Spec {
            name: "number-point",
            size: 2000,
            about: "two hundred single-item reads of a float price",
            volatile: false,
            cross_cpu_pct: 0.4,
            prepare: |size| Box::pin(number::point(size)),
        },
        Spec {
            name: "number-scan",
            size: 2000,
            about: "one read of every item's float price: a number decoded per row",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(number::scan(size)),
        },
        Spec {
            name: "number-range-narrow",
            size: 2000,
            about: "twenty reads of the ten items in a window of integer timestamps, the window pushed into the scan",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(number::narrow(size)),
        },
        Spec {
            name: "number-range-wide",
            size: 2000,
            about: "one read of every item priced at 500 or more, about half, the bound pushed into the scan",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(number::wide(size)),
        },
        Spec {
            name: "number-range-mixed",
            size: 2000,
            about: "one read of the tenth of items with the largest size, a relation holding naturals, integers and floats",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(number::mixed(size)),
        },
        Spec {
            name: "subscribe-repoll",
            size: 1000,
            about: "a commit that moves ten entities to a better-ranked case, then the subscription's poll",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(subscribe::repoll(size)),
        },
    ]
}

/// Scenarios outside the gate: a sweep runs them only when `--only`
/// names them, and `compare` never sees them unless a sweep wrote them.
/// The gate fails a scenario with no baseline file, and recording one
/// is the maintainer's call, so a scenario that measures a question
/// rather than a path the gate should hold waits here until it is
/// promoted into [`catalog`] with its baseline.
pub fn ungated() -> Vec<Spec> {
    vec![
        Spec {
            name: "stack-event",
            size: 2000,
            about: "two hundred stack commits of one transient click each, which an inductive rule turns into a count under `last` on an ephemeral layer: nothing durable",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(stack::event(size)),
        },
        Spec {
            name: "stack-cursor",
            size: 2000,
            about: "two hundred stack commits each moving one cursor under `last` on an ephemeral layer, then one read of it through the stack",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(stack::cursor_moves(size)),
        },
        Spec {
            name: "stack-frame",
            size: 2000,
            about: "twenty stack commits of a hundred transient clicks each on distinct counters, which an inductive rule counts on an ephemeral layer",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(stack::frame(size)),
        },
        Spec {
            name: "stack-subscribe",
            size: 2000,
            about: "`stack-cursor` with a subscription to every cursor over the stack, polled after every commit",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(stack::subscribe(size)),
        },
        Spec {
            name: "branch-cursor",
            size: 2000,
            about: "`stack-cursor` without a stack: two hundred cursor moves under `last` into the branch's session store, then one read",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(stack::branch_cursor(size)),
        },
        Spec {
            name: "branch-subscribe",
            size: 2000,
            about: "`stack-subscribe` without a stack: a branch subscription to every cursor, polled after each move into the session store",
            volatile: false,
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(stack::branch_subscribe(size)),
        },
    ]
}

/// The scenario named `name`, gated or not.
pub fn spec(name: &str) -> Option<Spec> {
    catalog()
        .into_iter()
        .chain(ungated())
        .find(|spec| spec.name == name)
}

/// The `stuff` population: `stuff/name` and `stuff/role` per entity.
fn stuff() -> ConceptDescriptor {
    concept(serde_json::json!({ "with": {
        "name": { "the": "stuff/name", "as": "text:" },
        "role": { "the": "stuff/role", "as": "text:" }
    }}))
}

/// The `stuff/name` field alone.
fn stuff_name() -> ConceptDescriptor {
    concept(serde_json::json!({ "with": {
        "name": { "the": "stuff/name", "as": "text:" }
    }}))
}

/// Commit `count` stuff entities, their two facts under `policy`.
async fn seed_stuff(env: &Env, count: usize, policy: Pick) -> Result<()> {
    let facts = (0..count).flat_map(|index| {
        let this = entity("stuff", index);
        [
            fact(&this, "stuff/name", text(format!("name-{index}"))),
            fact(&this, "stuff/role", text(format!("role-{}", index % 8))),
        ]
    });
    env.commit(assert_all(facts, policy)).await
}
