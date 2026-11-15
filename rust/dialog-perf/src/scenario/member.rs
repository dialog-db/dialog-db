//! A member's view read: a replica branch that tracks a remote and has
//! pulled everything, so its reads may warm ahead of demand, though every
//! block they touch is already local.

// The scenario commits through the one-shot publish, as the others here do:
// what the gate measures stays what its baseline recorded, and moving it to
// `Branch::transact` is the maintainer's call.
#![allow(deprecated)]

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use dialog_capability::Did;
use dialog_corpus::{Pick, Value};
use dialog_query::query::Output as _;
use dialog_query::{ConceptQuery, InductiveRule, Parameters, Term};
use dialog_repository::{Branch, Download, contact};

use super::{Outcome, Prepared, stuff};
use crate::env::{Env, assert_all, entity, fact, text, var};

/// How many entities each seeding command transaction carries.
const BATCH: usize = 20;

/// How many roles the population cycles through, as `seed_stuff` does.
const ROLES: usize = 8;

/// The role the filtered read pins.
const PINNED: usize = 3;

/// The inductive rule that records a `cmd.seed` command as the entity's
/// name and role. A command carries all three of the premise's fields,
/// so the rule is seeded once per field: the shape of an application's
/// commands, and what decides how many ops each commit's heads write.
fn record() -> Result<InductiveRule> {
    Ok(serde_json::from_value(serde_json::json!({
        "assert!": {
            "with": {
                "name": { "the": "stuff/name", "as": "text:" },
                "role": { "the": "stuff/role", "as": "text:" }
            }
        },
        "when": [{
            "assert": {
                "with": {
                    "target": { "the": "cmd.seed/target", "as": "entity:" },
                    "name": { "the": "cmd.seed/name", "as": "text:" },
                    "role": { "the": "cmd.seed/role", "as": "text:" }
                }
            },
            "where": { "target": var("this"), "name": var("name"), "role": var("role") }
        }]
    }))?)
}

/// Every entity's name and role.
fn every() -> ConceptQuery {
    crate::env::query(&stuff(), &["name", "role"], None)
}

/// The names of the entities holding one role: a pinned scan joined to a
/// probe per row, the join a filtered view runs.
fn of_role(role: usize) -> ConceptQuery {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("name".into(), Term::var("name"));
    terms.insert("role".into(), Term::constant(format!("role-{role}")));
    ConceptQuery {
        predicate: stuff(),
        terms,
    }
}

struct Member {
    env: Env,
    member: Branch,
}

impl Prepared for Member {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let rows = self
                .member
                .select(every())
                .perform(&self.env.operator)
                .try_vec()
                .await?
                .len() as u64;
            let filtered = self
                .member
                .select(of_role(PINNED))
                .perform(&self.env.operator)
                .try_vec()
                .await?
                .len() as u64;
            Ok(Outcome::from([
                ("rows".into(), rows),
                ("filtered".into(), filtered),
            ]))
        })
    }
}

/// `query-member`: `size` entities recorded by an inductive rule from
/// commands, `BATCH` per commit; a member branch pulls the result and
/// tracks a remote it never needs to reach, and the repository's caches
/// are let go of. The phase reads every entity's name and role, then the
/// names of one role.
pub async fn view(size: usize) -> Result<Box<dyn Prepared>> {
    let env = Env::open().await?;
    let mut transaction = env.branch.transaction();
    transaction = transaction.assert(record()?);
    transaction
        .commit()
        .publish()
        .perform(&env.operator)
        .await?;
    for start in (0..size).step_by(BATCH) {
        let commands = (start..(start + BATCH).min(size)).flat_map(|index| {
            let command = entity("cmd", index);
            [
                fact(
                    &command,
                    "cmd.seed/target",
                    Value::Entity(entity("stuff", index)),
                ),
                fact(&command, "cmd.seed/name", text(format!("name-{index}"))),
                fact(
                    &command,
                    "cmd.seed/role",
                    text(format!("role-{}", index % ROLES)),
                ),
            ]
        });
        env.branch
            .transaction()
            .dispatch(assert_all(commands, Pick::All))
            .commit()
            .publish()
            .perform(&env.operator)
            .await?;
    }

    let member = env.branch("member").await?;
    member
        .pull(Download::MinimalRanges)
        .from(&env.branch)
        .perform(&env.operator)
        .await?;

    // Track a remote. Nothing here or in the phase reaches it: every
    // block the member's reads touch is local, and that is the case
    // the phase measures. Tracking it is what has reads warm ahead.
    let site = dialog_repository::SiteAddress::S3(
        dialog_remote_s3::Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("perf-origin")
            .build()?,
    );
    let origin: Did = "did:web:perf-origin.example".parse()?;
    contact(&origin)
        .add_address(site)
        .name("origin")
        .perform(&env.operator)
        .await?;
    let remote = contact(&origin)
        .connect()
        .repository(env.branch.of().clone())
        .open()
        .perform(&env.operator)
        .await?
        .branch("main")
        .open()
        .perform(&env.operator)
        .await?;
    member.set_upstream(remote).perform(&env.operator).await?;
    let member = env.branch("member").await?;
    env.forget();
    Ok(Box::new(Member { env, member }))
}
