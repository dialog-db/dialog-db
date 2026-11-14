//! The scenarios: one per path an application drives through the
//! engine. Each prepares its environment (seeding, rules, warm-up) off
//! the measured phase, then runs the phase the report is about.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use dialog_corpus::Pick;
use dialog_query::ConceptDescriptor;

use crate::env::{Env, assert_all, concept, entity, fact, text};

mod commit;
mod export;
mod member;
mod number;
mod read;
mod replica;
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
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(commit::batch(size)),
        },
        Spec {
            name: "commit-rows",
            size: 100,
            about: "one commit per entity: the per-commit cost of records, signing and publication",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(commit::rows(size)),
        },
        Spec {
            name: "commit-hot-key",
            size: 100,
            about: "one commit per entity, each also re-asserting one standing fact: the per-commit cost of a key every commit rewrites",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(commit::hot_key(size)),
        },
        Spec {
            name: "commit-succeed",
            size: 500,
            about: "one transaction rewriting every entity's name under `last`: each write elects and retracts a standing claim",
            // Its total varies most between runs of one binary (8%,
            // every counter equal): a buffer that grows to about a
            // megabyte is copied by `realloc` or not with the heap's
            // layout. Outside the allocator it repeats to 0.04%.
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(commit::succeed(size)),
        },
        Spec {
            name: "commit-derived",
            size: 200,
            about: "one transaction writing under `last` to a relation a rule also derives: the commit settles each cell through the rule",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(commit::derived(size)),
        },
        Spec {
            name: "transaction-reads",
            size: 300,
            about: "a transaction that reads the cell it just wrote, for every entity, then commits: the library-install shape",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(transaction::reads(size)),
        },
        Spec {
            name: "first-query",
            size: 200,
            about: "one point read on a replica that adopted, by root, a head `size` one-commit origins deep: the first query of a device that faults blocks in",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(replica::first_query(size)),
        },
        Spec {
            name: "read-point",
            size: 2000,
            about: "two hundred single-entity reads of one attribute over a seeded branch",
            cross_cpu_pct: 0.9,
            prepare: |size| Box::pin(read::point(size)),
        },
        Spec {
            name: "read-scan",
            size: 2000,
            about: "one concept read over a single attribute, every entity",
            cross_cpu_pct: 0.2,
            prepare: |size| Box::pin(read::scan(size)),
        },
        Spec {
            name: "query-join",
            size: 2000,
            about: "one two-attribute concept read: the planner's join",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(read::join(size)),
        },
        Spec {
            name: "query-member",
            size: 2000,
            about: "on a member that tracks a remote and holds every block, one cold read of every entity's name and role, then the names of one role: the reads that warm ahead of demand",
            // CI sweeps of #767 (instructions, then `_int_malloc`
            // inclusive where it was read):
            //   387f06b  Xeon Platinum 8370C  662b464ced6baa9c  115,986,589  8,068,511
            //   18d5a61  Xeon Platinum 8370C  662b464ced6baa9c  115,829,922  7,974,892
            //   6587a02  EPYC 9V45            060ba73ad866d3a8  115,729,243  7,921,637
            //   d152a84  EPYC 7763            d14bb80d302b2adf  116,264,748  8,118,501
            //   da530de  EPYC 9V74            1551aebb00c12037  116,225,137
            //   4557565  EPYC 9V74            1551aebb00c12037  116,485,183
            // 0.46% across the first four, one code state, less 0.135%
            // within the 8370C pair, rounded as #653 rounds. The last two
            // took main (#761, #763, #782) between them, so their spread
            // mixes code with CPU; across all six it is 0.65%. The
            // baseline file is the 4557565 sweep. blake3, verifying the 1.5 MB its 32
            // cold loads read (12% of the phase), retired 13,672,977 on
            // every model; the allocator moved by up to 197 K.
            cross_cpu_pct: 0.3,
            prepare: |size| Box::pin(member::view(size)),
        },
        Spec {
            name: "rule-scan",
            size: 2000,
            about: "one read of a concept a committed rule derives from two stored attributes, every entity",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(rule::scan(size)),
        },
        Spec {
            name: "rule-point",
            size: 2000,
            about: "two hundred single-entity reads of a concept a committed rule derives",
            cross_cpu_pct: 2.4,
            prepare: |size| Box::pin(rule::point(size)),
        },
        Spec {
            name: "rule-recursive",
            size: 20,
            about: "one read of a transitive closure over `size` chains of depth 25, derived by a base and a recursive rule",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(rule::recursive(size)),
        },
        Spec {
            name: "rule-ranked",
            size: 1000,
            about: "one read of a ranked field four rules each state a case for, elected best first, every entity",
            cross_cpu_pct: 0.5,
            prepare: |size| Box::pin(rule::ranked(size)),
        },
        Spec {
            name: "rule-install",
            size: 100,
            about: "one transaction committing `size` rules, then the first read of the last one: discovery and hydration cold",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(rule::install(size)),
        },
        Spec {
            name: "rule-overlay",
            size: 2000,
            about: "two hundred single-entity reads of a concept a rule derives, each read carrying the rule in its own overlay",
            cross_cpu_pct: 0.3,
            prepare: |size| Box::pin(rule::overlay(size)),
        },
        Spec {
            name: "rule-session",
            size: 2000,
            about: "two hundred rounds of a session-overlay fact that is no rule, then a single-entity read of a concept a committed rule derives",
            cross_cpu_pct: 0.3,
            prepare: |size| Box::pin(rule::session(size)),
        },
        Spec {
            name: "rule-dispatch",
            size: 500,
            about: "one transaction asserting a fact per entity that a chain of three inductive rules relays: a round of induction per link",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(commit::dispatch(size)),
        },
        Spec {
            name: "number-commit",
            size: 1000,
            about: "one transaction asserting a timestamp, a price, a count and a size per item: how numbers key",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(number::commit(size)),
        },
        Spec {
            name: "number-point",
            size: 2000,
            about: "two hundred single-item reads of a float price",
            cross_cpu_pct: 0.4,
            prepare: |size| Box::pin(number::point(size)),
        },
        Spec {
            name: "number-scan",
            size: 2000,
            about: "one read of every item's float price: a number decoded per row",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(number::scan(size)),
        },
        Spec {
            name: "number-range-narrow",
            size: 2000,
            about: "twenty reads of the ten items in a window of integer timestamps, the window pushed into the scan",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(number::narrow(size)),
        },
        Spec {
            name: "number-range-wide",
            size: 2000,
            about: "one read of every item priced at 500 or more, about half, the bound pushed into the scan",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(number::wide(size)),
        },
        Spec {
            name: "number-range-mixed",
            size: 2000,
            about: "one read of the tenth of items with the largest size, a relation holding naturals, integers and floats",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(number::mixed(size)),
        },
        Spec {
            name: "subscribe-repoll",
            size: 1000,
            about: "a commit that moves ten entities to a better-ranked case, then the subscription's poll",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(subscribe::repoll(size)),
        },
        Spec {
            name: "subscribe-fanout",
            size: 200,
            about: "a commit adding one note, then the poll of a subscription per note and one to the list",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(subscribe::fanout(size)),
        },
        Spec {
            name: "subscribe-pulled",
            size: 200,
            about: "a device's pull of a note another device committed, then the poll of its subscription per note and one to the list",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(subscribe::pulled(size)),
        },
        Spec {
            name: "read-sibling",
            size: 2000,
            about: "two hundred single-entity reads through a second branch of the repository at the same head: the nodes one branch read answer another's",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(read::sibling(size)),
        },
        Spec {
            name: "export-walk",
            size: 2000,
            about: "one snapshot export streaming every block of a tree the branch's own commit left warm",
            cross_cpu_pct: 0.0,
            prepare: |size| Box::pin(export::walk(size)),
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
