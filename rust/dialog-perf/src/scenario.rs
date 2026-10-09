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
mod read;
mod rule;
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
            prepare: |size| Box::pin(commit::batch(size)),
        },
        Spec {
            name: "commit-rows",
            size: 100,
            about: "one commit per entity: the per-commit cost of records, signing and publication",
            volatile: false,
            prepare: |size| Box::pin(commit::rows(size)),
        },
        Spec {
            name: "commit-succeed",
            size: 500,
            about: "one transaction rewriting every entity's name under `last`: each write elects and retracts a standing claim",
            // Like `transaction-reads`, its commit's tree write varies
            // between runs of one binary (1.60 G to 1.85 G instructions,
            // every counter equal), in the delete path's forced merges,
            // on the PR head as on this branch. Gated on counters.
            volatile: true,
            prepare: |size| Box::pin(commit::succeed(size)),
        },
        Spec {
            name: "commit-derived",
            size: 200,
            about: "one transaction writing under `last` to a relation a rule also derives: the commit settles each cell through the rule",
            volatile: false,
            prepare: |size| Box::pin(commit::derived(size)),
        },
        Spec {
            name: "transaction-reads",
            size: 300,
            about: "a transaction that reads the cell it just wrote, for every entity, then commits: the library-install shape",
            // The commit's tree write splits nodes in some runs and not
            // others (263 M to 394 M instructions across runs of one
            // binary, every counter equal): four of its blocks come out
            // eight bytes shorter in some runs, and the seal writes the
            // delta's blocks in hash order. Gated on counters until the
            // varying bytes are found.
            volatile: true,
            prepare: |size| Box::pin(transaction::reads(size)),
        },
        Spec {
            name: "read-point",
            size: 2000,
            about: "two hundred single-entity reads of one attribute over a seeded branch",
            volatile: false,
            prepare: |size| Box::pin(read::point(size)),
        },
        Spec {
            name: "read-scan",
            size: 2000,
            about: "one concept read over a single attribute, every entity",
            volatile: false,
            prepare: |size| Box::pin(read::scan(size)),
        },
        Spec {
            name: "query-join",
            size: 2000,
            about: "one two-attribute concept read: the planner's join",
            volatile: false,
            prepare: |size| Box::pin(read::join(size)),
        },
        Spec {
            name: "rule-scan",
            size: 2000,
            about: "one read of a concept a committed rule derives from two stored attributes, every entity",
            volatile: false,
            prepare: |size| Box::pin(rule::scan(size)),
        },
        Spec {
            name: "rule-point",
            size: 2000,
            about: "two hundred single-entity reads of a concept a committed rule derives",
            volatile: false,
            prepare: |size| Box::pin(rule::point(size)),
        },
        Spec {
            name: "rule-recursive",
            size: 20,
            about: "one read of a transitive closure over `size` chains of depth 25, derived by a base and a recursive rule",
            volatile: false,
            prepare: |size| Box::pin(rule::recursive(size)),
        },
        Spec {
            name: "rule-ranked",
            size: 1000,
            about: "one read of a ranked field four rules each state a case for, elected best first, every entity",
            volatile: false,
            prepare: |size| Box::pin(rule::ranked(size)),
        },
        Spec {
            name: "rule-install",
            size: 100,
            about: "one transaction committing `size` rules, then the first read of the last one: discovery and hydration cold",
            volatile: false,
            prepare: |size| Box::pin(rule::install(size)),
        },
        Spec {
            name: "subscribe-repoll",
            size: 1000,
            about: "a commit that moves ten entities to a better-ranked case, then the subscription's poll",
            volatile: false,
            prepare: |size| Box::pin(subscribe::repoll(size)),
        },
    ]
}

/// The scenario named `name`.
pub fn spec(name: &str) -> Option<Spec> {
    catalog().into_iter().find(|spec| spec.name == name)
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
