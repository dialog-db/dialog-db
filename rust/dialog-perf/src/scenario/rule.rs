//! Reads answered by rules: a derived concept scanned and probed, a
//! recursive closure, a ranked field several rules state cases for, the
//! first read after installing many rules, reads carrying their rule in
//! their own overlay, and reads after session writes that change no rule.

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use dialog_artifacts::{Artifact, Entity, Pick, Value};
use dialog_query::{ConceptDescriptor, DeductiveRule};

use dialog_query::query::Output as _;
use dialog_query::{ConceptConclusion, the};

use super::read::{PROBES, probe, read};
use super::{Outcome, Prepared, seed_stuff};
use crate::env::{Env, assert_all, concept, entity, fact, query, rule, text, var};

/// `member`, derived from `stuff`: title from the name, level from the role.
fn member() -> ConceptDescriptor {
    concept(serde_json::json!({ "with": {
        "title": { "the": "member/title", "as": "text:" },
        "level": { "the": "member/level", "as": "text:" }
    }}))
}

fn member_rule() -> DeductiveRule {
    rule(serde_json::json!({
        "deduce": { "with": {
            "title": { "the": "member/title", "as": "text:" },
            "level": { "the": "member/level", "as": "text:" }
        }},
        "when": [{
            "assert": { "with": {
                "name": { "the": "stuff/name", "as": "text:" },
                "role": { "the": "stuff/role", "as": "text:" }
            }},
            "where": { "this": var("this"), "name": var("title"), "role": var("level") }
        }]
    }))
}

struct Scan {
    env: Env,
}

impl Prepared for Scan {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let rows = read(&self.env, &member(), &["title", "level"], None).await?;
            Ok(Outcome::from([("rows".into(), rows.len() as u64)]))
        })
    }
}

/// `rule-scan`: the member rule and `size` stuff entities, then one read
/// of every member.
pub async fn scan(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    env.install(&[member_rule()]).await?;
    seed_stuff(&env, size, Pick::Last).await?;
    env.reopen().await?;
    Ok(Box::new(Scan { env }))
}

struct Point {
    env: Env,
    size: usize,
}

impl Prepared for Point {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let rows = probe(&self.env, &member(), &["title", "level"], self.size).await?;
            Ok(Outcome::from([("rows".into(), rows)]))
        })
    }
}

/// `rule-point`: the member rule and `size` stuff entities, then two
/// hundred single-entity reads of a member.
pub async fn point(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    env.install(&[member_rule()]).await?;
    seed_stuff(&env, size, Pick::Last).await?;
    env.reopen().await?;
    Ok(Box::new(Point { env, size }))
}

/// The depth of every chain in `rule-recursive`.
const DEPTH: usize = 25;

fn ancestor() -> ConceptDescriptor {
    concept(serde_json::json!({ "with": {
        "ancestor": { "the": "family/ancestor", "as": "entity:", "pick": "all:" }
    }}))
}

fn ancestor_rules() -> [DeductiveRule; 2] {
    let parent = serde_json::json!({ "with": {
        "parent": { "the": "family/parent", "as": "entity:" }
    }});
    let head = serde_json::json!({ "with": {
        "ancestor": { "the": "family/ancestor", "as": "entity:", "pick": "all:" }
    }});
    let base = rule(serde_json::json!({
        "deduce": head,
        "when": [{
            "assert": parent,
            "where": { "this": var("this"), "parent": var("ancestor") }
        }]
    }));
    let step = rule(serde_json::json!({
        "deduce": head,
        "when": [
            {
                "assert": parent,
                "where": { "this": var("this"), "parent": var("parent") }
            },
            {
                "assert": head,
                "where": { "this": var("parent"), "ancestor": var("ancestor") }
            }
        ]
    }));
    [base, step]
}

struct Recursive {
    env: Env,
}

impl Prepared for Recursive {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let rows = read(&self.env, &ancestor(), &["ancestor"], None).await?;
            Ok(Outcome::from([("rows".into(), rows.len() as u64)]))
        })
    }
}

/// `rule-recursive`: the base and recursive ancestor rules over `size`
/// chains of `DEPTH` parent links, then one read of every ancestor pair.
pub async fn recursive(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    env.install(&ancestor_rules()).await?;
    let facts = (0..size).flat_map(|chain| {
        (1..DEPTH).map(move |depth| {
            let child = entity(&format!("chain-{chain}"), depth);
            let parent = entity(&format!("chain-{chain}"), depth - 1);
            fact(&child, "family/parent", Value::Entity(parent))
        })
    });
    env.commit(assert_all(facts, Pick::All)).await?;
    env.reopen().await?;
    Ok(Box::new(Recursive { env }))
}

/// The cases of `account/status`, best first.
pub const CASES: [&str; 4] = [
    "case:active",
    "case:registered",
    "case:invited",
    "case:onboarding",
];

/// `status`, read under `top` over `CASES`.
pub fn status() -> ConceptDescriptor {
    concept(serde_json::json!({ "with": {
        "status": { "the": "account/status", "as": CASES }
    }}))
}

/// One rule per case: an account with the case's flag has that status;
/// every account with an email is at least onboarding.
pub fn status_rules() -> Vec<DeductiveRule> {
    let flags = [
        ("case:active", "account/active"),
        ("case:registered", "account/registered"),
        ("case:invited", "account/invited"),
        ("case:onboarding", "account/email"),
    ];
    flags
        .into_iter()
        .map(|(case, flag)| {
            rule(serde_json::json!({
                "deduce": { "with": { "status": { "the": "account/status", "as": CASES } } },
                "when": [
                    {
                        "assert": { "with": { "flag": { "the": flag, "as": "text:" } } },
                        "where": { "this": var("this"), "flag": var("flag") }
                    },
                    { "assert": "==", "where": { "this": var("status"), "is": case } }
                ]
            }))
        })
        .collect()
}

/// The flags account `index` carries: every fourth account is active,
/// every second registered, three in four invited, all have an email.
pub fn account_facts(index: usize) -> Vec<Artifact> {
    let this = entity("account", index);
    let mut facts = vec![fact(&this, "account/email", text(format!("{index}@mail")))];
    if index % 4 < 3 {
        facts.push(fact(&this, "account/invited", text("yes")));
    }
    if index % 4 < 2 {
        facts.push(fact(&this, "account/registered", text("yes")));
    }
    if index.is_multiple_of(4) {
        facts.push(fact(&this, "account/active", text("yes")));
    }
    facts
}

/// The status rules installed and `size` accounts committed.
pub async fn accounts(size: usize) -> Result<Env> {
    let mut env = Env::open().await?;
    env.install(&status_rules()).await?;
    let facts = (0..size).flat_map(account_facts);
    env.commit(assert_all(facts, Pick::Last)).await?;
    env.reopen().await?;
    Ok(env)
}

struct Ranked {
    env: Env,
}

impl Prepared for Ranked {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let rows = read(&self.env, &status(), &["status"], None).await?;
            let mut outcome = Outcome::from([("rows".into(), rows.len() as u64)]);
            // A case is an entity (`case:active`), the domain the ranked
            // list types the field as.
            for row in &rows {
                let status = row
                    .get::<Entity>("status")
                    .map(|case| case.to_string())
                    .unwrap_or_else(|_| "unreadable".into());
                *outcome.entry(status).or_insert(0) += 1;
            }
            Ok(outcome)
        })
    }
}

/// `rule-ranked`: four status rules over `size` accounts, then one read
/// of every account's status.
pub async fn ranked(size: usize) -> Result<Box<dyn Prepared>> {
    let env = accounts(size).await?;
    Ok(Box::new(Ranked { env }))
}

struct Install {
    env: Env,
    size: usize,
}

impl Prepared for Install {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let rules: Vec<DeductiveRule> = (0..self.size)
                .map(|index| {
                    rule(serde_json::json!({
                        "deduce": { "with": {
                            "name": { "the": format!("derived-{index}/name"), "as": "text:" }
                        }},
                        "when": [{
                            "assert": { "with": { "name": { "the": "stuff/name", "as": "text:" } } },
                            "where": { "this": var("this"), "name": var("name") }
                        }]
                    }))
                })
                .collect();
            self.env.install(&rules).await?;
            self.env.reopen().await?;
            let last = concept(serde_json::json!({ "with": {
                "name": { "the": format!("derived-{}/name", self.size - 1), "as": "text:" }
            }}));
            let rows = read(&self.env, &last, &["name"], None).await?;
            Ok(Outcome::from([
                ("rules".into(), self.size as u64),
                ("rows".into(), rows.len() as u64),
            ]))
        })
    }
}

/// `rule-install`: a hundred stuff entities, then one transaction of
/// `size` rules and the first read of the last one.
pub async fn install(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    seed_stuff(&env, 100, Pick::Last).await?;
    env.reopen().await?;
    Ok(Box::new(Install { env, size }))
}

struct Overlay {
    env: Env,
    size: usize,
}

impl Prepared for Overlay {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let step = (self.size / PROBES).max(1);
            let mut rows = 0u64;
            for probe in 0..PROBES {
                let this = entity("stuff", (probe * step) % self.size);
                let found: Vec<ConceptConclusion> = self
                    .env
                    .branch
                    .query()
                    .with(member_rule())
                    .select(query(&member(), &["title", "level"], Some(&this)))
                    .perform(&self.env.operator)
                    .try_vec()
                    .await?;
                rows += found.len() as u64;
            }
            Ok(Outcome::from([("rows".into(), rows)]))
        })
    }
}

/// `rule-overlay`: `size` stuff entities and no rule committed, then two
/// hundred single-entity reads of a member, each carrying the member rule
/// in its own overlay, the way an application hands its rules to every
/// query.
pub async fn overlay(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    seed_stuff(&env, size, Pick::Last).await?;
    env.reopen().await?;
    Ok(Box::new(Overlay { env, size }))
}

struct Session {
    env: Env,
    size: usize,
}

impl Prepared for Session {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let step = (self.size / PROBES).max(1);
            let presence: Entity = "id:presence".parse()?;
            let mut rows = 0u64;
            for probe in 0..PROBES {
                let seen = the!("session/seen")
                    .of(presence.clone())
                    .is(format!("probe-{probe}"));
                self.env.branch.overlay().assert(seen)?;
                let this = entity("stuff", (probe * step) % self.size);
                rows += read(&self.env, &member(), &["title", "level"], Some(&this))
                    .await?
                    .len() as u64;
            }
            Ok(Outcome::from([("rows".into(), rows)]))
        })
    }
}

/// `rule-session`: the member rule and `size` stuff entities, then two
/// hundred rounds of a session-overlay fact that is no rule followed by a
/// single-entity read of a member, the way a reactive application writes
/// presence between reads.
pub async fn session(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    env.install(&[member_rule()]).await?;
    seed_stuff(&env, size, Pick::Last).await?;
    env.reopen().await?;
    Ok(Box::new(Session { env, size }))
}
