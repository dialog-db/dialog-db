//! A tonk-shaped space and a week of its members' writes, generated from a
//! seed through the space's own commands.
//!
//! Everything is a function of the seed, the scale and the export it is
//! shaped like: two runs of the generator write the same file. Records are
//! made the way tonk's pages make them: an issue by `v2/issue-create`, its
//! labels, project, assignee and status by the `v2/issue-*` commands (each
//! also leaving a change in the issue's activity), a comment by
//! `v2/issue-post-comment`, a memo by `memo/post`. People, their member
//! profiles and projects have no command in the space; they are asserted.

use std::collections::{BTreeMap, HashMap};

use dialog_artifacts::Value;
use rand::{Rng as _, SeedableRng as _};
use rand_chacha::ChaCha8Rng;

pub use crate::dataset::Shape;
use crate::dataset::{Change, Line, Op, Phase, member_entity};

/// The members a generated dataset has write to the space.
pub const MEMBERS: u32 = 6;

/// How many commands the measured week holds.
pub const WEEK: usize = 300;
/// How many of each kind of command the steady phase sends, unless
/// `DIALOG_DATASET_STEADY` says otherwise (for a dataset to profile
/// commands with).
pub const STEADY: usize = 20;

/// The concepts a dataset carries, by name, as descriptor JSON.
pub type Concepts = BTreeMap<String, serde_json::Value>;

/// The attribute field `field` of concept `concept` is stored under.
fn attribute<'a>(concepts: &'a Concepts, concept: &str, field: &str) -> anyhow::Result<&'a str> {
    concepts
        .get(concept)
        .and_then(|descriptor| descriptor["with"][field]["the"].as_str())
        .ok_or_else(|| anyhow::anyhow!("the space has no {concept}.{field}"))
}

/// Every cell's values at the end of `lines`' commits, by attribute and
/// entity.
pub fn live(lines: &[Line]) -> HashMap<(String, String), Vec<Value>> {
    let mut cells: HashMap<(String, String), Vec<Value>> = HashMap::new();
    for line in lines {
        let Line::Commit { changes, .. } = line else {
            continue;
        };
        for change in changes {
            let cell = cells
                .entry((change.the.clone(), change.of.clone()))
                .or_default();
            match change.op {
                Op::Add => {
                    if !cell.contains(&change.is) {
                        cell.push(change.is.clone());
                    }
                }
                Op::Replace => {
                    cell.clear();
                    cell.push(change.is.clone());
                }
                Op::Retract => cell.retain(|value| *value != change.is),
            }
        }
    }
    cells.retain(|_, values| !values.is_empty());
    cells
}

/// The entities holding a value of `the`, and how many values in all.
fn holders(cells: &HashMap<(String, String), Vec<Value>>, the: &str) -> (Vec<String>, usize) {
    let mut entities = Vec::new();
    let mut values = 0;
    for ((attribute, of), held) in cells {
        if attribute == the {
            entities.push(of.clone());
            values += held.len();
        }
    }
    entities.sort();
    (entities, values)
}

/// What an exported space holds that its writes build on.
#[derive(Default)]
pub struct Existing {
    /// Issues, with the number of their ident.
    pub issues: Vec<(String, u64)>,
    /// Labels.
    pub labels: Vec<String>,
    /// Projects.
    pub projects: Vec<String>,
    /// Memos.
    pub memos: Vec<String>,
    /// Comments, by issue.
    pub comments: Vec<(String, String)>,
}

/// The shape of the space `lines` export, and what it holds.
pub fn shape(lines: &[Line], concepts: &Concepts) -> anyhow::Result<(Shape, Existing)> {
    let cells = live(lines);
    let ident = attribute(concepts, "v2/issue", "ident")?;
    let mut issues: Vec<(String, u64)> = Vec::new();
    for ((attribute, of), values) in &cells {
        if attribute == ident {
            let number = match values.last() {
                Some(Value::String(text)) => text
                    .rsplit('-')
                    .next()
                    .and_then(|number| number.parse().ok())
                    .unwrap_or(0),
                _ => 0,
            };
            issues.push((of.clone(), number));
        }
    }
    issues.sort();
    let (labels, _) = holders(&cells, attribute(concepts, "v2/label", "name")?);
    let (projects, _) = holders(&cells, attribute(concepts, "v2/project", "title")?);
    let (_, label_links) = holders(&cells, attribute(concepts, "v2/issue-labels", "label")?);
    let (in_project, _) = holders(&cells, attribute(concepts, "v2/issue", "project")?);
    let (assigned, _) = holders(&cells, attribute(concepts, "v2/issue", "assignee")?);
    let kind = attribute(concepts, "v2/activity", "kind")?;
    let on_issue = attribute(concepts, "v2/activity", "issue")?;
    let mut comments = Vec::new();
    for ((attribute, of), values) in &cells {
        if attribute == kind
            && values.last() == Some(&Value::String("comment".into()))
            && let Some(Value::Entity(issue)) = cells
                .get(&(on_issue.to_string(), of.clone()))
                .and_then(|values| values.last())
        {
            comments.push((issue.to_string(), of.clone()));
        }
    }
    comments.sort();
    let (memos, _) = holders(&cells, attribute(concepts, "memo/full", "on")?);
    let (_, mentions) = holders(&cells, attribute(concepts, "memo/full", "mention")?);
    let count = issues.len().max(1) as f64;
    let shape = Shape {
        issues: issues.len(),
        labels: labels.len(),
        projects: projects.len(),
        labels_per_issue: label_links as f64 / count,
        in_project: in_project.len() as f64 / count,
        assigned: assigned.len() as f64 / count,
        comments_per_issue: comments.len() as f64 / count,
        memos: memos.len(),
        mentions_per_memo: mentions as f64 / memos.len().max(1) as f64,
    };
    let existing = Existing {
        issues,
        labels,
        projects,
        memos,
        comments,
    };
    Ok((shape, existing))
}

/// What a generator has made so far, for later records to link to.
struct Made {
    labels: Vec<String>,
    projects: Vec<String>,
    issues: Vec<String>,
    memos: Vec<String>,
    comments: Vec<(String, String)>,
    next_ident: u64,
}

/// Generates records and commands over a space's concepts.
pub struct Generator<'a> {
    rng: ChaCha8Rng,
    concepts: &'a Concepts,
    made: Made,
    /// Milliseconds since the epoch: each command a few minutes after the
    /// last.
    clock: u64,
    counter: u64,
    /// The member sending the next commands, when a round says whose
    /// turn it is.
    turn: Option<u32>,
}

const STATUSES: [&str; 4] = ["todo", "progress", "review", "done"];
const PRIORITIES: [&str; 4] = ["low", "medium", "high", "urgent"];
const WORDS: [&str; 16] = [
    "sync", "replica", "query", "rule", "view", "memo", "issue", "label", "space", "branch",
    "commit", "invite", "member", "archive", "index", "page",
];

impl<'a> Generator<'a> {
    /// A generator over `concepts` from `seed`, whose members are the
    /// dataset's people.
    pub fn new(concepts: &'a Concepts, seed: u64) -> Self {
        Self {
            rng: ChaCha8Rng::seed_from_u64(seed),
            concepts,
            made: Made {
                labels: Vec::new(),
                projects: Vec::new(),
                issues: Vec::new(),
                memos: Vec::new(),
                comments: Vec::new(),
                next_ident: 1,
            },
            // 2026-11-01T00:00:00Z.
            clock: 1_793_491_200_000,
            counter: 0,
            turn: None,
        }
    }

    /// Builds on what an exported space already holds.
    pub fn building_on(mut self, existing: &Existing) -> Self {
        self.made.issues = existing.issues.iter().map(|(id, _)| id.clone()).collect();
        self.made.next_ident = existing.issues.iter().map(|(_, n)| *n).max().unwrap_or(0) + 1;
        self.made.labels = existing.labels.clone();
        self.made.projects = existing.projects.clone();
        self.made.memos = existing.memos.clone();
        self.made.comments = existing.comments.clone();
        self
    }

    fn tick(&mut self) -> u64 {
        self.clock += self.rng.gen_range(30_000..600_000);
        self.clock
    }

    fn at(&self) -> String {
        let seconds = self.clock / 1000;
        let (days, rest) = (seconds / 86_400, seconds % 86_400);
        // Days since 1970-01-01 to a civil date (Howard Hinnant's).
        let z = days as i64 + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z - era * 146_097;
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = doy - (153 * mp + 2) / 5 + 1;
        let month = if mp < 10 { mp + 3 } else { mp - 9 };
        let year = yoe + era * 400 + i64::from(month <= 2);
        format!(
            "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
            rest / 3600,
            rest % 3600 / 60,
            rest % 60,
            self.clock % 1000
        )
    }

    /// A fresh entity, `urn:uuid:` from the seed.
    fn uuid(&mut self) -> String {
        self.counter += 1;
        let high: u64 = self.rng.r#gen();
        format!(
            "urn:uuid:{:08x}-{:04x}-4{:03x}-8{:03x}-{:012x}",
            high >> 32,
            (high >> 16) & 0xffff,
            high & 0xfff,
            self.counter & 0xfff,
            self.counter
        )
    }

    fn words(&mut self, count: usize) -> String {
        (0..count)
            .map(|_| WORDS[self.rng.gen_range(0..WORDS.len())])
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn pick<'b>(&mut self, from: &'b [String]) -> Option<&'b String> {
        (!from.is_empty()).then(|| &from[self.rng.gen_range(0..from.len())])
    }

    /// The facts of a record of `concept` about `of`: a claim per field,
    /// replacing under a single pick, added under `all:`.
    fn record(
        &self,
        concept: &str,
        of: &str,
        fields: &[(&str, Value)],
    ) -> anyhow::Result<Vec<Change>> {
        let descriptor = &self.concepts[concept]["with"];
        fields
            .iter()
            .map(|(field, value)| {
                let the = descriptor[*field]["the"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("the space has no {concept}.{field}"))?;
                let op = if descriptor[*field]["pick"].as_str() == Some("all:") {
                    Op::Add
                } else {
                    Op::Replace
                };
                Ok(Change {
                    op,
                    the: the.to_string(),
                    of: of.to_string(),
                    is: value.clone(),
                })
            })
            .collect()
    }

    /// Command `name` from member `author`, its fields `fields`, on a
    /// fresh event entity.
    fn command(
        &mut self,
        phase: Phase,
        name: &str,
        author: u32,
        fields: &[(&str, Value)],
    ) -> anyhow::Result<Line> {
        let event = self.uuid();
        Ok(Line::Command {
            phase,
            name: name.to_string(),
            author,
            changes: self.record(name, &event, fields)?,
            durable: Vec::new(),
        })
    }

    fn entity(text: &str) -> anyhow::Result<Value> {
        Ok(Value::Entity(text.parse()?))
    }

    /// The members' people and profiles, committed as records: a person
    /// per member, linked to their profile, which carries their name.
    pub fn members(&mut self) -> anyhow::Result<Line> {
        let mut changes = Vec::new();
        for member in 0..MEMBERS {
            let profile = member_entity(member);
            let name = format!("Member {member}");
            let person = self.uuid();
            changes.extend(self.record(
                "v2/person",
                &person,
                &[
                    ("name", Value::String(name.clone())),
                    ("role", Value::String("Engineer".into())),
                    ("active", Value::Boolean(true)),
                    ("email", Value::String(String::new())),
                    ("account", Self::entity(&profile)?),
                ],
            )?);
            changes.extend(self.record(
                "v2/profile-account",
                &profile,
                &[
                    ("name", Value::String(name)),
                    ("account", Self::entity(&profile)?),
                ],
            )?);
        }
        Ok(Line::Commit {
            phase: Phase::Setup,
            changes,
        })
    }

    /// `count` projects, committed as records.
    pub fn projects(&mut self, count: usize) -> anyhow::Result<Line> {
        let mut changes = Vec::new();
        for _ in 0..count {
            let project = self.uuid();
            let title = self.words(3);
            let description = self.words(24);
            let created = self.tick() as f64;
            let target = self.at();
            changes.extend(self.record(
                "project",
                &project,
                &[
                    ("title", Value::String(title)),
                    ("status", Value::String("active".into())),
                    ("description", Value::String(description)),
                    ("created", Value::Float(created)),
                    ("target", Value::String(target)),
                ],
            )?);
            self.made.projects.push(project);
        }
        Ok(Line::Commit {
            phase: Phase::Setup,
            changes,
        })
    }

    fn author(&mut self) -> u32 {
        match self.turn {
            Some(member) => member,
            None => self.rng.gen_range(0..MEMBERS),
        }
    }

    /// `v2/label-create`.
    pub fn label(&mut self, phase: Phase) -> anyhow::Result<Line> {
        let id = self.uuid();
        let time = self.tick();
        let name = self.words(1);
        let author = self.author();
        let line = self.command(
            phase,
            "v2/label-create",
            author,
            &[
                ("id", Self::entity(&id)?),
                ("name", Value::String(name)),
                ("color", Value::String(format!("#{:06x}", time & 0xff_ffff))),
                ("time", Value::UnsignedInt(time.into())),
            ],
        )?;
        self.made.labels.push(id);
        Ok(line)
    }

    /// `v2/issue-create`.
    pub fn issue(&mut self, phase: Phase) -> anyhow::Result<(Line, String)> {
        let author = self.author();
        let id = self.uuid();
        let time = self.tick();
        let ident = self.made.next_ident;
        self.made.next_ident += 1;
        let title = self.words(6);
        let priority = PRIORITIES[self.rng.gen_range(0..PRIORITIES.len())];
        let line = self.command(
            phase,
            "v2/issue-create",
            author,
            &[
                ("id", Self::entity(&id)?),
                ("ident", Value::String(format!("BUG-{ident}"))),
                ("title", Value::String(title)),
                ("status", Value::String("todo".into())),
                ("priority", Value::String(priority.into())),
                ("ordering", Value::Float(ident as f64)),
                ("created", Value::String(self.at())),
                ("creator", Self::entity(&member_entity(author))?),
                ("time", Value::UnsignedInt(time.into())),
            ],
        )?;
        self.made.issues.push(id.clone());
        Ok((line, id))
    }

    /// One of the `v2/issue-*` commands that change an issue and leave the
    /// change in its activity: `which` is `add-label`, `set-project`,
    /// `set-assignee` or `set-status`.
    pub fn change(&mut self, phase: Phase, which: &str, issue: &str) -> anyhow::Result<Line> {
        let author = self.author();
        let time = self.tick();
        let change_id = self.uuid();
        let (field, value, text) = match which {
            "add-label" => {
                let label = self.pick(&self.made.labels.clone()).cloned();
                let label = label.ok_or_else(|| anyhow::anyhow!("no label to add"))?;
                ("label", Self::entity(&label)?, "added a label".to_string())
            }
            "set-project" => {
                let project = self.pick(&self.made.projects.clone()).cloned();
                let project = project.ok_or_else(|| anyhow::anyhow!("no project to set"))?;
                (
                    "value",
                    Self::entity(&project)?,
                    "set the project".to_string(),
                )
            }
            "set-assignee" => {
                let person = self.rng.gen_range(0..MEMBERS);
                (
                    "value",
                    Value::String(member_entity(person)),
                    "set the assignee".to_string(),
                )
            }
            _ => {
                let status = STATUSES[self.rng.gen_range(0..STATUSES.len())];
                (
                    "value",
                    Value::String(status.into()),
                    format!("set status to {status}"),
                )
            }
        };
        let name = format!("v2/issue-{which}");
        self.command(
            phase,
            &name,
            author,
            &[
                ("issue", Self::entity(issue)?),
                (field, value),
                ("change", Value::String(text)),
                ("change-id", Self::entity(&change_id)?),
                ("author", Self::entity(&member_entity(author))?),
                ("author-name", Value::String(format!("Member {author}"))),
                ("at", Value::String(self.at())),
                ("time", Value::UnsignedInt(time.into())),
            ],
        )
    }

    /// `v2/issue-post-comment`, or `v2/issue-post-reply` under an earlier
    /// comment on the same issue.
    pub fn comment(&mut self, phase: Phase, issue: &str, reply: bool) -> anyhow::Result<Line> {
        let author = self.author();
        let time = self.tick();
        let id = self.uuid();
        let body = self.words(12);
        let parent = if reply {
            let on_issue: Vec<String> = self
                .made
                .comments
                .iter()
                .filter(|(of, _)| of == issue)
                .map(|(_, comment)| comment.clone())
                .collect();
            self.pick(&on_issue).cloned()
        } else {
            None
        };
        let mut fields = vec![
            ("id", Self::entity(&id)?),
            ("issue", Self::entity(issue)?),
            ("body", Value::String(body)),
            ("author", Self::entity(&member_entity(author))?),
            ("author-name", Value::String(format!("Member {author}"))),
            ("nonce", Value::String(id.clone())),
            ("at", Value::String(self.at())),
            ("time", Value::UnsignedInt(time.into())),
        ];
        let name = match parent {
            Some(parent) => {
                fields.push(("parent", Value::String(parent)));
                "v2/issue-post-reply"
            }
            None => "v2/issue-post-comment",
        };
        let line = self.command(phase, name, author, &fields)?;
        self.made.comments.push((issue.to_string(), id));
        Ok(line)
    }

    /// `memo/post` on an issue or an earlier memo, with its mentions
    /// asserted beside it.
    pub fn memo(&mut self, phase: Phase, mentions: f64) -> anyhow::Result<Line> {
        let author = self.author();
        let time = self.tick();
        let id = format!("note:memo-{}", self.uuid().trim_start_matches("urn:uuid:"));
        let on = if self.rng.gen_bool(0.5) {
            self.pick(&self.made.memos.clone()).cloned()
        } else {
            None
        };
        let on = match on.or_else(|| self.pick(&self.made.issues.clone()).cloned()) {
            Some(on) => on,
            None => anyhow::bail!("nothing for a memo to be on"),
        };
        let body = self.words(16);
        let mut line = self.command(
            phase,
            "memo/post",
            author,
            &[
                ("id", Self::entity(&id)?),
                ("on", Self::entity(&on)?),
                ("body", Value::String(body)),
                ("at", Value::String(self.at())),
                ("time", Value::Float(time as f64)),
            ],
        )?;
        let count = poisson(&mut self.rng, mentions);
        let mut durable = Vec::new();
        for _ in 0..count {
            let person = member_entity(self.rng.gen_range(0..MEMBERS));
            durable.extend(self.record(
                "memo/mention",
                &id,
                &[("mention", Self::entity(&person)?)],
            )?);
        }
        if let Line::Command { durable: slot, .. } = &mut line {
            *slot = durable;
        }
        self.made.memos.push(id);
        Ok(line)
    }

    /// A space shaped like `shape` at `scale`, built through its commands.
    pub fn space(&mut self, shape: &Shape, scale: usize) -> anyhow::Result<Vec<Line>> {
        let mut lines = vec![self.members()?, self.projects(shape.projects * scale)?];
        for _ in 0..shape.labels * scale {
            lines.push(self.label(Phase::Setup)?);
        }
        for _ in 0..shape.issues * scale {
            let (line, issue) = self.issue(Phase::Setup)?;
            lines.push(line);
            for _ in 0..poisson(&mut self.rng, shape.labels_per_issue) {
                lines.push(self.change(Phase::Setup, "add-label", &issue)?);
            }
            if self.rng.gen_bool(shape.in_project.min(1.0)) {
                lines.push(self.change(Phase::Setup, "set-project", &issue)?);
            }
            if self.rng.gen_bool(shape.assigned.min(1.0)) {
                lines.push(self.change(Phase::Setup, "set-assignee", &issue)?);
            }
            if self.rng.gen_bool(0.5) {
                lines.push(self.change(Phase::Setup, "set-status", &issue)?);
            }
            for _ in 0..poisson(&mut self.rng, shape.comments_per_issue) {
                let reply = self.rng.gen_bool(0.3);
                lines.push(self.comment(Phase::Setup, &issue, reply)?);
            }
        }
        for _ in 0..shape.memos * scale {
            lines.push(self.memo(Phase::Setup, shape.mentions_per_memo)?);
        }
        Ok(lines)
    }

    /// After the week, the first member's commands in a row: [`STEADY`]
    /// of each kind, back to back, with no sync between.
    pub fn steady(&mut self, shape: &Shape) -> anyhow::Result<Vec<Line>> {
        let mut lines = Vec::new();
        let count = std::env::var("DIALOG_DATASET_STEADY")
            .ok()
            .and_then(|count| count.parse().ok())
            .unwrap_or(STEADY);
        self.turn = Some(0);
        for kind in [
            "issue-create",
            "set-status",
            "add-label",
            "set-assignee",
            "comment",
            "memo",
        ] {
            for _ in 0..count {
                let issue = self
                    .pick(&self.made.issues.clone())
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("no issue to act on"))?;
                lines.push(match kind {
                    "issue-create" => self.issue(Phase::Steady)?.0,
                    "comment" => self.comment(Phase::Steady, &issue, false)?,
                    "memo" => self.memo(Phase::Steady, shape.mentions_per_memo.max(0.5))?,
                    change => self.change(Phase::Steady, change, &issue)?,
                });
            }
        }
        self.turn = None;
        Ok(lines)
    }

    /// A week of the members' writes: rounds in which each member sends
    /// one to three commands, then everyone syncs. About [`WEEK`] commands.
    pub fn week(&mut self, shape: &Shape) -> anyhow::Result<Vec<Line>> {
        let mut lines = Vec::new();
        let mut sent = 0;
        while sent < WEEK {
            for member in 0..MEMBERS {
                self.turn = Some(member);
                for _ in 0..self.rng.gen_range(1..=3) {
                    let issue = self.pick(&self.made.issues.clone()).cloned();
                    let roll = self.rng.gen_range(0..100);
                    let line = match (roll, issue) {
                        (0..10, _) | (_, None) => self.issue(Phase::Writes)?.0,
                        (10..30, Some(issue)) => {
                            self.change(Phase::Writes, "set-status", &issue)?
                        }
                        (30..42, Some(issue)) => self.change(Phase::Writes, "add-label", &issue)?,
                        (42..50, Some(issue)) => {
                            self.change(Phase::Writes, "set-assignee", &issue)?
                        }
                        (50..55, Some(issue)) => {
                            self.change(Phase::Writes, "set-project", &issue)?
                        }
                        (55..85, Some(issue)) => {
                            let reply = self.rng.gen_bool(0.3);
                            self.comment(Phase::Writes, &issue, reply)?
                        }
                        _ => self.memo(Phase::Writes, shape.mentions_per_memo.max(0.5))?,
                    };
                    lines.push(line);
                    sent += 1;
                }
            }
            lines.push(Line::Sync);
        }
        self.turn = None;
        Ok(lines)
    }
}

/// A Poisson draw with mean `mean`.
fn poisson(rng: &mut ChaCha8Rng, mean: f64) -> usize {
    let limit = (-mean).exp();
    let mut product: f64 = rng.r#gen();
    let mut count = 0;
    while product > limit {
        product *= rng.r#gen::<f64>();
        count += 1;
    }
    count
}
