//! The write path against a model of what it should leave: every
//! sequence of a few writes, enumerated, applied to a real branch by
//! the crate's driver and read back from the relations themselves,
//! claim by claim. Each sequence is applied two ways, a document for
//! each write and one document of every write, and each way is held to
//! the model exactly.

#![allow(deprecated)]

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;

#[path = "support/session.rs"]
mod session;
use session::Notebook;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// A concept with a field under `:one`, one under `:many`, and one under
/// `:one` in a second domain, and two commands whose rules write it.
const CONCEPT: &str = r#"(concept (r ?t o: ?o m: ?m s: ?s)
  (xyz.r/o of: ?t is: ?o as: :text cardinality: :one)
  (xyz.r/m of: ?t is: ?m as: :text cardinality: :many)
  (abc.r/s of: ?t is: ?s as: :text cardinality: :one))
(concept (r/drop subject: ?t)
  (xyz.r.drop/subject of: ?c is: ?t as: :entity in: :control cardinality: :many))
(concept (r/set subject: ?t to: ?v)
  (xyz.r.set/subject of: ?c is: ?t as: :entity in: :control cardinality: :many)
  (xyz.r.set/to of: ?c is: ?v as: :text in: :control cardinality: :many))
(rule (retract (r ?t)) (r/drop subject: ?t))
(rule (assert (r ?t o: ?v m: ?v s: ?v)) (r/set subject: ?t to: ?v))"#;

/// Each field: its domain, its name, and whether it is under `:one`.
const FIELDS: [(&str, &str, bool); 3] = [
    ("xyz.r", "o", true),
    ("xyz.r", "m", false),
    ("abc.r", "s", true),
];

/// One write, of one entity.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Op {
    /// The whole record through the concept, each field holding the
    /// value.
    Assert(&'static str),
    /// The whole record through the concept, each field holding the
    /// value, retracted.
    Retract(&'static str),
    /// A retract through the concept by the entity alone.
    Drop,
    /// `o: "A"` asserted through the attribute's relation.
    Put,
    /// `o: "A"` retracted through the attribute's relation.
    Take,
    /// The command whose rule retracts the record by its entity alone.
    Dropping,
    /// The command whose rule asserts the record with each field `"A"`.
    Setting,
}

impl Op {
    fn commands(self) -> bool {
        matches!(self, Op::Dropping | Op::Setting)
    }
}

/// The writes at the top level.
const WRITES: [Op; 7] = [
    Op::Assert("A"),
    Op::Assert("B"),
    Op::Retract("A"),
    Op::Retract("B"),
    Op::Drop,
    Op::Put,
    Op::Take,
];

/// The writes a rule makes, a command issued.
const COMMANDS: [Op; 2] = [Op::Dropping, Op::Setting];

/// The form of `op` on the entity `x:{entity}`.
fn form(entity: u32, op: Op) -> String {
    let record = |value: &str| format!(r#"(r x:{entity} o: "{value}" m: "{value}" s: "{value}")"#);
    match op {
        Op::Assert(value) => format!("(assert {})", record(value)),
        Op::Retract(value) => format!("(retract {})", record(value)),
        Op::Drop => format!("(retract (r x:{entity}))"),
        Op::Put => format!(r#"(assert (xyz.r x:{entity} o: "A"))"#),
        Op::Take => format!(r#"(retract (xyz.r x:{entity} o: "A"))"#),
        Op::Dropping => format!("(assert (r/drop subject: x:{entity}))"),
        Op::Setting => format!(r#"(assert (r/set subject: x:{entity} to: "A"))"#),
    }
}

/// A cell: which of the sequence's two entities, and which field.
type Cell = (usize, usize);

/// A change a transaction makes to a cell, as Dialog-DB is handed it.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Change {
    /// Asserted, under `:one` when `true`.
    Assert(&'static str, bool),
    Retract(&'static str),
}

/// What a branch holds: each cell's claims, oldest first.
#[derive(Clone, Default, PartialEq, Debug)]
struct State(BTreeMap<Cell, Vec<&'static str>>);

impl State {
    /// `changes`, one transaction's, applied in order. An assert under
    /// `:one` succeeds the cell's newest claim and keeps the others; one
    /// under `:many` adds; a retract removes that claim.
    fn apply(&mut self, changes: &[(Cell, Change)]) {
        for (cell, change) in changes {
            let claims = self.0.entry(*cell).or_default();
            match change {
                Change::Assert(value, one) => {
                    if *one {
                        claims.pop();
                    }
                    if !claims.contains(value) {
                        claims.push(value);
                    }
                }
                Change::Retract(value) => claims.retain(|claim| claim != value),
            }
        }
        self.0.retain(|_, claims| !claims.is_empty());
    }

    /// Every claim, as a read of its relation says it, of the two
    /// entities named `entities`.
    fn claims(&self, entities: [u32; 2]) -> BTreeSet<String> {
        self.0
            .iter()
            .flat_map(|((which, at), claims)| {
                let (domain, field, _) = FIELDS[*at];
                let entity = entities[*which];
                claims
                    .iter()
                    .map(move |value| format!(r#"({domain} x:{entity} {field}: "{value}")"#))
            })
            .collect()
    }
}

/// The changes `op` on `which` makes over `view`, what the transaction
/// reads.
fn changes(view: &State, which: usize, op: Op) -> Vec<(Cell, Change)> {
    let mut made = Vec::new();
    for (at, (_, _, one)) in FIELDS.iter().enumerate() {
        let cell = (which, at);
        match op {
            Op::Assert(value) => made.push((cell, Change::Assert(value, *one))),
            Op::Retract(value) => made.push((cell, Change::Retract(value))),
            Op::Drop | Op::Dropping => {
                for value in view.0.get(&cell).into_iter().flatten() {
                    made.push((cell, Change::Retract(value)));
                }
            }
            Op::Put if at == 0 => made.push((cell, Change::Assert("A", false))),
            Op::Take if at == 0 => made.push((cell, Change::Retract("A"))),
            Op::Setting => made.push((cell, Change::Assert("A", *one))),
            Op::Put | Op::Take => {}
        }
    }
    made
}

/// What each write in a document of its own leaves.
fn apart(sequence: &[(usize, Op)]) -> State {
    let mut state = State::default();
    for (which, op) in sequence {
        let made = changes(&state, *which, *op);
        state.apply(&made);
    }
    state
}

/// What every write in one document leaves, by the driver's rule: the
/// forms run in order, each reading what the ones before it staged; the
/// commands land at commit, after every form, their rules' writes one
/// round read from what the forms left.
fn together(sequence: &[(usize, Op)]) -> State {
    let staged = |made: &[(Cell, Change)]| {
        let mut view = State::default();
        view.apply(made);
        view
    };
    let mut made = Vec::new();
    for (which, op) in sequence.iter().filter(|(_, op)| !op.commands()) {
        let view = staged(&made);
        made.extend(changes(&view, *which, *op));
    }
    let mut state = staged(&made);
    // The round the commands fire in: each rule reads what the forms
    // left, and a claim one rule retracts and another asserts is held.
    // Under `:one` a rule's write succeeds the cell's newest claim, one
    // a form of the same document wrote too (#872).
    let mut round = Vec::new();
    for which in 0..2 {
        let issued = |command: Op| sequence.contains(&(which, command));
        let mut retracted = if issued(Op::Dropping) {
            changes(&state, which, Op::Dropping)
        } else {
            Vec::new()
        };
        if issued(Op::Setting) {
            let set = changes(&state, which, Op::Setting);
            retracted.retain(|(cell, change)| {
                !set.iter().any(|(at, set)| {
                    at == cell
                        && matches!((change, set), (Change::Retract(a), Change::Assert(b, _)) if a == b)
                })
            });
            round.extend(retracted);
            round.extend(set);
        } else {
            round.extend(retracted);
        }
    }
    state.apply(&round);
    state
}

/// Every sequence of `length` writes or fewer of `alphabet` over the two
/// entities, one of each pair that differ only by which entity is which:
/// the first write is of the first entity.
fn sequences(alphabet: &[Op], length: usize) -> Vec<Vec<(usize, Op)>> {
    let symbols: Vec<(usize, Op)> = [0, 1]
        .into_iter()
        .flat_map(|which| alphabet.iter().map(move |op| (which, *op)))
        .collect();
    let mut all = Vec::new();
    let mut last: Vec<Vec<(usize, Op)>> = vec![Vec::new()];
    for _ in 0..length {
        let mut next = Vec::new();
        for sequence in &last {
            for symbol in &symbols {
                if sequence.is_empty() && symbol.0 != 0 {
                    continue;
                }
                let mut longer = sequence.clone();
                longer.push(*symbol);
                next.push(longer);
            }
        }
        all.extend(next.iter().cloned());
        last = next;
    }
    all
}

/// The writes of `sequence` as forms, `entities` the two it is of.
fn forms(sequence: &[(usize, Op)], entities: [u32; 2]) -> Vec<String> {
    sequence
        .iter()
        .map(|(which, op)| form(entities[*which], *op))
        .collect()
}

/// Every claim the branch holds of the concept's attributes, read from
/// each attribute's relation, by entity.
async fn held(notebook: &mut Notebook) -> Result<BTreeMap<String, BTreeSet<String>>> {
    let mut all: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (domain, field, _) in FIELDS {
        let read = format!("(select ({domain} ?e {field}: ?v) ({domain} ?e {field}: ?v))");
        for claim in notebook.submit(&read).await?.0 {
            let entity = claim
                .split_whitespace()
                .nth(1)
                .unwrap_or_default()
                .to_string();
            all.entry(entity).or_default().insert(claim);
        }
    }
    Ok(all)
}

/// A sequence that left what the model did not.
#[derive(Debug)]
#[allow(dead_code)]
struct Differs {
    forms: Vec<String>,
    model: BTreeSet<String>,
    branch: BTreeSet<String>,
}

/// Each sequence of `alphabet` up to `length` applied both ways on one
/// branch, each on entities of its own, and held to the model, saying
/// the shortest that differs; and how many sequences the command rule
/// makes leave one thing as a document each and another as one
/// document.
async fn hold(alphabet: &[Op], length: usize) -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(CONCEPT).await?;
    let all = sequences(alphabet, length);
    let entities = |at: usize, way: u32| {
        let base = (at as u32) * 4 + way * 2 + 1;
        [base, base + 1]
    };
    for (at, sequence) in all.iter().enumerate() {
        for form in forms(sequence, entities(at, 0)) {
            notebook.submit(&form).await?;
        }
        notebook
            .submit(&forms(sequence, entities(at, 1)).join("\n"))
            .await?;
    }
    let held = held(&mut notebook).await?;
    let of = |pair: [u32; 2]| -> BTreeSet<String> {
        pair.iter()
            .flat_map(|entity| {
                held.get(&format!("x:{entity}"))
                    .cloned()
                    .unwrap_or_default()
            })
            .collect()
    };
    let (mut apart_differ, mut together_differ) = (Vec::new(), Vec::new());
    let mut at_commit = 0;
    for (at, sequence) in all.iter().enumerate() {
        let pair = entities(at, 0);
        let model = apart(sequence).claims(pair);
        let branch = of(pair);
        if branch != model {
            apart_differ.push(Differs {
                forms: forms(sequence, pair),
                model,
                branch,
            });
        }
        let pair = entities(at, 1);
        let model = together(sequence).claims(pair);
        if model != apart(sequence).claims(pair) {
            at_commit += 1;
        }
        let branch = of(pair);
        if branch != model {
            together_differ.push(Differs {
                forms: forms(sequence, pair),
                model,
                branch,
            });
        }
    }
    apart_differ.sort_by_key(|differs| differs.forms.len());
    together_differ.sort_by_key(|differs| differs.forms.len());
    println!(
        "{} sequences up to {length} of {} writes: {at_commit} leave apart what they do not \
         together, by the command rule",
        all.len(),
        alphabet.len()
    );
    assert!(
        apart_differ.is_empty(),
        "{} sequences, a document each write, differ; the shortest: {:#?}",
        apart_differ.len(),
        apart_differ.first()
    );
    assert!(
        together_differ.is_empty(),
        "{} sequences, one document, differ; the shortest: {:#?}",
        together_differ.len(),
        together_differ.first()
    );
    Ok(())
}

/// Every sequence of a few writes, at the top level and through the
/// commands' rules.
#[dialog_common::test]
async fn it_leaves_what_the_model_of_writes_and_commands_leaves() -> Result<()> {
    let alphabet: Vec<Op> = WRITES.iter().chain(COMMANDS.iter()).copied().collect();
    hold(&alphabet, LENGTH_WITH_COMMANDS).await
}

/// Every sequence of a few more writes at the top level.
#[dialog_common::test]
async fn it_leaves_what_the_model_of_writes_leaves() -> Result<()> {
    hold(&WRITES, LENGTH).await
}

const LENGTH_WITH_COMMANDS: usize = 3;
const LENGTH: usize = 3;
