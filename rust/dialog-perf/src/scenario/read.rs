//! The read path over stored facts: point reads, a scan, and a join.

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use dialog_corpus::{Entity, Pick};
use dialog_query::query::Output as _;
use dialog_query::{ConceptConclusion, ConceptDescriptor};
use dialog_repository::Branch;

use super::{Outcome, Prepared, seed_stuff, stuff, stuff_name};
use crate::env::{Env, entity, query};

/// How many point reads a point scenario makes.
pub const PROBES: usize = 200;

/// Read `descriptor` for every `free` field, `of` one entity or all.
pub async fn read(
    env: &Env,
    descriptor: &ConceptDescriptor,
    free: &[&str],
    of: Option<&Entity>,
) -> Result<Vec<ConceptConclusion>> {
    Ok(env
        .branch
        .select(query(descriptor, free, of))
        .perform(&env.operator)
        .try_vec()
        .await?)
}

/// Point reads of `descriptor` over `PROBES` entities of the `stuff`
/// population spread across `size`, counting the rows.
pub async fn probe(
    env: &Env,
    descriptor: &ConceptDescriptor,
    free: &[&str],
    size: usize,
) -> Result<u64> {
    let step = (size / PROBES).max(1);
    let mut rows = 0u64;
    for probe in 0..PROBES {
        let this = entity("stuff", (probe * step) % size);
        rows += read(env, descriptor, free, Some(&this)).await?.len() as u64;
    }
    Ok(rows)
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
            let rows = probe(&self.env, &stuff_name(), &["name"], self.size).await?;
            Ok(Outcome::from([("rows".into(), rows)]))
        })
    }
}

/// `read-point`: `size` entities, then `PROBES` single-entity reads.
pub async fn point(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    seed_stuff(&env, size, Pick::Last).await?;
    env.reopen().await?;
    Ok(Box::new(Point { env, size }))
}

struct Sibling {
    env: Env,
    sibling: Branch,
    size: usize,
}

impl Prepared for Sibling {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            // The sibling holds the same tree by root. Its nodes are the
            // repository's, warm from the scenario branch's commit, so a
            // read through the sibling touches the archive for nothing.
            let step = (self.size / PROBES).max(1);
            let mut rows = 0u64;
            for probe in 0..PROBES {
                let this = entity("stuff", (probe * step) % self.size);
                rows += self
                    .sibling
                    .select(query(&stuff_name(), &["name"], Some(&this)))
                    .perform(&self.env.operator)
                    .try_vec()
                    .await?
                    .len() as u64;
            }
            Ok(Outcome::from([("rows".into(), rows)]))
        })
    }
}

/// `read-sibling`: `size` entities committed on the scenario branch, a
/// second branch of the repository pulled to the same head, then `PROBES`
/// single-entity reads through the second branch.
pub async fn sibling(size: usize) -> Result<Box<dyn Prepared>> {
    let env = Env::open().await?;
    seed_stuff(&env, size, Pick::Last).await?;
    let sibling = env.branch("sibling").await?;
    sibling
        .pull(dialog_repository::Download::MinimalRanges)
        .from(&env.branch)
        .perform(&env.operator)
        .await?;
    Ok(Box::new(Sibling { env, sibling, size }))
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
            let rows = read(&self.env, &stuff_name(), &["name"], None).await?;
            Ok(Outcome::from([("rows".into(), rows.len() as u64)]))
        })
    }
}

/// `read-scan`: `size` entities, then one read of every name.
pub async fn scan(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    seed_stuff(&env, size, Pick::Last).await?;
    env.reopen().await?;
    Ok(Box::new(Scan { env }))
}

struct Join {
    env: Env,
}

impl Prepared for Join {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let rows = read(&self.env, &stuff(), &["name", "role"], None).await?;
            Ok(Outcome::from([("rows".into(), rows.len() as u64)]))
        })
    }
}

/// `query-join`: `size` entities, then one read of name and role.
pub async fn join(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    seed_stuff(&env, size, Pick::Last).await?;
    env.reopen().await?;
    Ok(Box::new(Join { env }))
}

/// Tonk's issue concept (`v2/issue`): three required fields and ten
/// optional ones, which a list read left-merges beside the required
/// scans instead of probing once per entity.
fn issue() -> ConceptDescriptor {
    crate::env::concept(serde_json::json!({ "with": {
        "ident": { "the": "bug/ident", "as": "text:" },
        "title": { "the": "bug/title", "as": "text:" },
        "status": { "the": "bug/status", "as": "text:" },
        "assignee": { "the": "bug/assignee", "as": "text:", "optional": true },
        "created": { "the": "bug/created-at", "as": "text:", "optional": true },
        "creator": { "the": "bug/creator", "as": "entity:", "optional": true },
        "creator-name": { "the": "bug/creator-name", "as": "text:", "optional": true },
        "detail": { "the": "bug/detail", "as": "text:", "optional": true },
        "due": { "the": "bug/due-date", "as": "text:", "optional": true },
        "effort": { "the": "bug/effort", "as": "natural:", "optional": true },
        "ordering": { "the": "bug/ordering", "as": "float:", "optional": true },
        "priority": { "the": "bug/priority", "as": "text:", "optional": true },
        "project": { "the": "bug/project", "as": "entity:", "optional": true }
    }}))
}

/// Every field of [`issue`].
const ISSUE_FIELDS: [&str; 13] = [
    "ident",
    "title",
    "status",
    "assignee",
    "created",
    "creator",
    "creator-name",
    "detail",
    "due",
    "effort",
    "ordering",
    "priority",
    "project",
];

/// Commit `count` issues, each optional field present on the share of
/// them tonk-team's issues hold it (as `tonk_bench`'s dataset does), every
/// value a function of the issue's index.
async fn seed_issues(env: &Env, count: usize) -> Result<()> {
    use crate::env::{fact, text};
    use dialog_corpus::Value;
    const WORDS: [&str; 8] = [
        "sync", "replica", "query", "page", "commit", "branch", "merge", "leaf",
    ];
    const STATUSES: [&str; 4] = ["todo", "doing", "review", "done"];
    let people: Vec<Entity> = (0..6).map(|n| entity("person", n)).collect();
    let projects: Vec<Entity> = (0..22).map(|n| entity("project", n)).collect();
    // Present on `percent` of the issues, spread by a stride.
    let on = |index: usize, salt: usize, percent: usize| (index * 37 + salt * 11) % 100 < percent;
    let mut facts = Vec::new();
    for index in 0..count {
        let this = entity("bug", index);
        let title: Vec<&str> = (0..5).map(|w| WORDS[(index * 7 + w * 3) % 8]).collect();
        let title = title.join(" ");
        facts.push(fact(&this, "bug/ident", text(format!("TONK-{index}"))));
        facts.push(fact(&this, "bug/title", text(title.clone())));
        facts.push(fact(&this, "bug/status", text(STATUSES[index % 4])));
        if on(index, 1, 96) {
            facts.push(fact(
                &this,
                "bug/assignee",
                text(format!("person-{}", index % 6)),
            ));
        }
        facts.push(fact(
            &this,
            "bug/created-at",
            text(format!("2026-10-{:02}T10:00:00Z", index % 28 + 1)),
        ));
        facts.push(fact(
            &this,
            "bug/creator",
            Value::Entity(people[index % 6].clone()),
        ));
        facts.push(fact(
            &this,
            "bug/creator-name",
            text(format!("Person {}", index % 6)),
        ));
        if on(index, 2, 50) {
            facts.push(fact(&this, "bug/detail", text(format!("{title} {title}"))));
        }
        if on(index, 3, 10) {
            facts.push(fact(
                &this,
                "bug/due-date",
                text(format!("2026-12-{:02}", index % 28 + 1)),
            ));
        }
        if on(index, 4, 30) {
            facts.push(fact(
                &this,
                "bug/effort",
                Value::UnsignedInt((index % 5) as u128),
            ));
        }
        facts.push(fact(
            &this,
            "bug/ordering",
            Value::Float(index as f64 * 0.5),
        ));
        if on(index, 5, 60) {
            facts.push(fact(
                &this,
                "bug/priority",
                text(["low", "medium", "high"][index % 3]),
            ));
        }
        if on(index, 6, 39) {
            facts.push(fact(
                &this,
                "bug/project",
                Value::Entity(projects[index % 22].clone()),
            ));
        }
    }
    env.commit(crate::env::assert_all(facts, Pick::Last)).await
}

struct Optional {
    env: Env,
}

impl Prepared for Optional {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let rows = read(&self.env, &issue(), &ISSUE_FIELDS, None).await?;
            Ok(Outcome::from([("rows".into(), rows.len() as u64)]))
        })
    }
}

/// `concept-optional`: `size` issues of tonk's issue concept, then one
/// read of every field of every issue.
pub async fn optional(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    seed_issues(&env, size).await?;
    env.reopen().await?;
    Ok(Box::new(Optional { env }))
}
