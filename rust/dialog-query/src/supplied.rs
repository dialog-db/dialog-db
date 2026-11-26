//! Formulas the engine declares and a crate of dialog's own supplies.
//!
//! The formulas and resolvers the engine knows are a closed set, and
//! each is on every replica: a query that asks one means the same
//! wherever it runs, whoever embeds dialog. Two more are declared here,
//! by name and by their cells, and their code is not the engine's:
//!
//! | declared | of | gives a row for |
//! |---|---|---|
//! | the formula `notation/read` | `text` | each form read: `at`, `tree`, and its range |
//! | the resolver `notation/analyze` | `form` | each thing said of the form: `severity`, `code`, `message`, `path` |
//!
//! Their implementation is supplied by the crate that has it, the
//! notation, to the environment a query runs in ([`supply_read`](crate::supplied::supply_read),
//! [`supply_analyze`](crate::supplied::supply_analyze)), which holds it
//! ([`Holds`](dialog_common::Holds)), and a premise asking
//! one finds it there when it is evaluated. A row it computes is held to
//! the cells declared here before it is written: a value for a cell not
//! declared as an output ([`EvaluationError::UndeclaredOutput`](crate::error::EvaluationError::UndeclaredOutput)), an
//! output left out ([`EvaluationError::MissingOutput`](crate::error::EvaluationError::MissingOutput)) or a value its
//! cell's type does not admit ([`EvaluationError::MistypedOutput`](crate::error::EvaluationError::MistypedOutput))
//! fails the query, naming the formula and the cell. A premise asking
//! one where the environment holds no implementation fails the query,
//! naming it ([`EvaluationError::Unsupplied`](crate::error::EvaluationError::Unsupplied)), before any row is read:
//! never a query that answers nothing.
//!
//! What is locked and what is not:
//!
//! - **Locked:** the names and the cells are the engine's. No crate adds
//!   a formula: a premise names one of these or one of the built-ins,
//!   and nothing else deserializes.
//! - **Not locked:** [`Holds`](dialog_common::Holds) is open, so any code holding the
//!   environment can supply an implementation under these names. That
//!   is not offered as an extension point and not stable; it is how the
//!   notation's code reaches the engine without the engine depending on
//!   it.
//!
//! **No rule holds one, for now.** A built-in formula is on every
//! replica. One whose implementation comes from another crate is only
//! where that crate is linked, and what it does would become part of
//! what a stored rule means. So a rule holding either is refused when it
//! is compiled ([`TypeError::SuppliedInRule`](crate::error::TypeError::SuppliedInRule)), and they are asked in a
//! [`PremiseQuery`](crate::query::PremiseQuery) alone. Opening rules to
//! them later is that one refusal removed, once two things are true: what
//! the implementation does is versioned in what a replica stores, as a
//! rule's body format is, so a stored rule says which reading it means;
//! and a replica whose environment holds no implementation judges such a
//! rule when the rule set is assembled, as a rule it does not evaluate
//! (`RuleError`, the quarantine every rule a replica cannot compile
//! goes to), not as an [`EvaluationError`](crate::error::EvaluationError) when a query asks it.

use std::fmt;
use std::sync::{Arc, LazyLock};

use dialog_capability::Provider;
use dialog_common::{ConditionalSend, ConditionalSync, Holds};
use dialog_corpus::selector::Constrained;
use dialog_corpus::{Fact, FactViewStream as _, Select, Selector};
use futures_util::TryStreamExt as _;
#[cfg(not(target_arch = "wasm32"))]
use futures_util::future::BoxFuture;
#[cfg(target_arch = "wasm32")]
use futures_util::future::LocalBoxFuture;
use serde::{Deserialize, Serialize};

use crate::corpus::Type as ValueType;
use crate::error::{EvaluationError, TypeError};
use crate::formula::bindings::Bindings;
use crate::formula::cell::Cells;
use crate::formula::query::{FormulaQuery, PARAM_COST};
use crate::resolver::rows;
use crate::scope::Scope;
use crate::selection::{Match, Selection};
use crate::term::Term;
use crate::type_system::Type as Kind;
use crate::types::Any;
use crate::{Binding, Parameters, Value, try_stream};

/// The formula that reads a text's forms.
pub const READ: &str = "notation/read";

/// The resolver that says what is refused of a form, and what is said
/// of it without refusing.
pub const ANALYZE: &str = "notation/analyze";

/// One row an implementation gives: the value of each output cell, by
/// the cell's name.
pub type Output = Vec<(&'static str, Value)>;

/// What [`NotationAnalyze::analyze`] gives: the rows, once it has read
/// what it needs. `Send` where the target has threads.
#[cfg(not(target_arch = "wasm32"))]
pub type Selected<'s> = BoxFuture<'s, Result<Vec<Output>, EvaluationError>>;
/// What [`NotationAnalyze::analyze`] gives: the rows, once it has read
/// what it needs.
#[cfg(target_arch = "wasm32")]
pub type Selected<'s> = LocalBoxFuture<'s, Result<Vec<Output>, EvaluationError>>;

/// What [`Facts::select`] gives: the facts a selector matches. `Send`
/// where the target has threads.
#[cfg(not(target_arch = "wasm32"))]
pub type Matched<'s> = BoxFuture<'s, Result<Vec<Fact>, EvaluationError>>;
/// What [`Facts::select`] gives: the facts a selector matches.
#[cfg(target_arch = "wasm32")]
pub type Matched<'s> = LocalBoxFuture<'s, Result<Vec<Fact>, EvaluationError>>;

/// The facts of the revision a query reads, as an implementation reads
/// them: through the query's own environment, so what it reads is the
/// query's demand, as a scan's is, and a subscription is evaluated again
/// when what it read changes.
pub trait Facts: ConditionalSync {
    /// The facts `selector` matches.
    fn select(&self, selector: Selector<Constrained>) -> Matched<'_>;
}

/// The implementation of `notation/read`: the rows for a text, one a
/// form, each of the cells declared for it.
pub trait NotationRead: fmt::Debug + ConditionalSend + ConditionalSync {
    /// The forms of `text`. The same text gives the same rows.
    fn read(&self, text: &str) -> Result<Vec<Output>, EvaluationError>;
}

/// The implementation of `notation/analyze`: the rows for a form, one a
/// thing said of it, each of the cells declared for it. It may read the
/// facts it needs, the definitions a form uses, through `facts`.
pub trait NotationAnalyze: fmt::Debug + ConditionalSend + ConditionalSync {
    /// What is said of `form`, read against `facts`.
    fn analyze<'s>(&'s self, form: &'s Value, facts: &'s dyn Facts) -> Selected<'s>;
}

/// The key the environment holds a reader under.
const READER: &str = "dialog-query/supplied/notation/read";

/// The key the environment holds an analyzer under.
const ANALYZER: &str = "dialog-query/supplied/notation/analyze";

struct Reader(Arc<dyn NotationRead>);

struct Analyzer(Arc<dyn NotationAnalyze>);

/// Hold `implementation` in `env` as what `notation/read` asks, in place
/// of any held before.
pub fn supply_read(env: &(impl Holds + ?Sized), implementation: Arc<dyn NotationRead>) {
    env.hold(READER.into(), Arc::new(Reader(implementation)));
}

/// Hold `implementation` in `env` as what `notation/analyze` asks, in
/// place of any held before.
pub fn supply_analyze(env: &(impl Holds + ?Sized), implementation: Arc<dyn NotationAnalyze>) {
    env.hold(ANALYZER.into(), Arc::new(Analyzer(implementation)));
}

/// The reader `env` holds, if one.
pub(crate) fn reader(env: &(impl Holds + ?Sized)) -> Option<Arc<dyn NotationRead>> {
    let held = env.held(READER)?;
    held.downcast_ref::<Reader>().map(|reader| reader.0.clone())
}

/// The analyzer `env` holds, if one.
pub(crate) fn analyzer(env: &(impl Holds + ?Sized)) -> Option<Arc<dyn NotationAnalyze>> {
    let held = env.held(ANALYZER)?;
    held.downcast_ref::<Analyzer>()
        .map(|analyzer| analyzer.0.clone())
}

/// The cells of a range, beside a formula's own.
const RANGE: [(&str, &str); 4] = [
    ("start-line", "the line it starts on, from zero"),
    (
        "start-character",
        "the UTF-16 unit it starts at on that line, from zero",
    ),
    ("end-line", "the line it ends on, from zero"),
    (
        "end-character",
        "the UTF-16 unit just past its end on that line, from zero",
    ),
];

fn natural() -> Option<Kind> {
    Some(Kind::from(ValueType::UnsignedInt))
}

fn text() -> Option<Kind> {
    Some(Kind::from(ValueType::String))
}

pub(crate) static READ_CELLS: LazyLock<Cells> = LazyLock::new(|| {
    Cells::define(|builder| {
        builder.cell("text", text()).the("the text").required();
        builder
            .cell("at", natural())
            .the("the form's place among the forms read, from zero")
            .output(0);
        builder.cell("tree", None).the("the form as data").output(0);
        for (cell, about) in RANGE {
            builder.cell(cell, natural()).the(about).output(0);
        }
    })
});

const READ_OUTPUTS: [&str; 6] = [
    "at",
    "tree",
    "start-line",
    "start-character",
    "end-line",
    "end-character",
];

pub(crate) static ANALYZE_CELLS: LazyLock<Cells> = LazyLock::new(|| {
    Cells::define(|builder| {
        builder
            .cell("form", None)
            .the("the form as data")
            .required();
        builder
            .cell("severity", text())
            .the("\"error\" for a refusal, \"warning\" for what is said without refusing")
            .output(0);
        builder
            .cell("code", text())
            .the("the kind of what is said, as a word")
            .output(0);
        builder
            .cell("message", text())
            .the("what is said")
            .output(0);
        builder
            .cell("path", None)
            .the("the way to the part of the form it is said of")
            .output(0);
    })
});

const ANALYZE_OUTPUTS: [&str; 4] = ["severity", "code", "message", "path"];

/// `rows`, each held to the outputs and the cells declared for
/// `formula`: a value for a cell not declared as an output, an output
/// left out, or a value its cell's type does not admit fails the query.
fn checked(
    formula: &'static str,
    outputs: &[&'static str],
    cells: &Cells,
    rows: Vec<Output>,
) -> Result<Vec<Output>, EvaluationError> {
    for row in &rows {
        for (cell, value) in row {
            if !outputs.contains(cell) {
                return Err(EvaluationError::UndeclaredOutput {
                    formula: formula.into(),
                    cell: (*cell).into(),
                });
            }
            if let Some(kind) = cells.get(cell).and_then(|cell| cell.content_type().clone())
                && !kind.admits(value)
            {
                return Err(EvaluationError::MistypedOutput {
                    formula: formula.into(),
                    cell: (*cell).into(),
                    expected: Box::new(kind),
                    actual: value.data_type(),
                });
            }
        }
        if let Some(cell) = outputs
            .iter()
            .find(|cell| !row.iter().any(|(given, _)| given == *cell))
        {
            return Err(EvaluationError::MissingOutput {
                formula: formula.into(),
                cell: (*cell).into(),
            });
        }
    }
    Ok(rows)
}

/// A premise asking `notation/read`: the terms of its cells.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Parameters", into = "Parameters")]
pub struct NotationReadQuery {
    terms: Parameters,
}

impl NotationReadQuery {
    /// `notation/read` applied to `terms`, held to its cells.
    pub fn new(terms: Parameters) -> Result<Self, TypeError> {
        Ok(Self {
            terms: READ_CELLS.conform(terms)?,
        })
    }

    /// The terms of its cells.
    pub fn terms(&self) -> &Parameters {
        &self.terms
    }

    /// What one row in costs, for planning.
    pub(crate) fn cost() -> usize {
        PARAM_COST * READ_CELLS.count()
    }

    /// The matches the reader's rows make of `matched`: each row held to
    /// the cells declared for `notation/read`, then written to its own
    /// copy of the row in, a row whose value disagrees with what it
    /// binds already filtered, as every formula's is. A text the row in
    /// does not bind as text is a row that does not match.
    pub(crate) fn expand(
        &self,
        matched: Match,
        reader: &dyn NotationRead,
    ) -> Result<Vec<Match>, EvaluationError> {
        let mut bindings = Bindings::new(
            Arc::new(FormulaQuery::NotationRead(self.clone())),
            matched,
            self.terms.clone(),
        );
        let text: String = match bindings.read("text") {
            Ok(text) => text,
            Err(
                EvaluationError::Conflict { .. }
                | EvaluationError::Absent { .. }
                | EvaluationError::TypeMismatch { .. },
            ) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let rows = checked(READ, &READ_OUTPUTS, &READ_CELLS, reader.read(&text)?)?;
        let mut matches = Vec::new();
        for output in rows {
            let mut row = bindings.clone();
            // An output the premise gives no term is one its asker did not
            // ask for.
            let written = output
                .iter()
                .filter(|(cell, _)| bindings.terms().get(cell).is_some())
                .try_for_each(|(cell, value)| row.write(cell, value));
            match written {
                Ok(()) => matches.push(row.source),
                // The value disagrees with what the row binds already: the
                // row does not match.
                Err(EvaluationError::Conflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(matches)
    }
}

impl TryFrom<Parameters> for NotationReadQuery {
    type Error = TypeError;

    fn try_from(terms: Parameters) -> Result<Self, TypeError> {
        Self::new(terms)
    }
}

impl From<NotationReadQuery> for Parameters {
    fn from(query: NotationReadQuery) -> Self {
        query.terms
    }
}

static BLANK: LazyLock<Term<Any>> = LazyLock::new(Term::blank);

/// A premise asking `notation/analyze`: the terms of its cells.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Parameters", into = "Parameters")]
pub struct NotationAnalyzeQuery {
    terms: Parameters,
}

impl NotationAnalyzeQuery {
    /// `notation/analyze` applied to `terms`, held to its cells.
    pub fn new(terms: Parameters) -> Result<Self, TypeError> {
        Ok(Self {
            terms: ANALYZE_CELLS.conform(terms)?,
        })
    }

    /// The terms of its cells.
    pub fn terms(&self) -> &Parameters {
        &self.terms
    }

    /// The term of the form it analyzes: what a join binds before it can
    /// be asked.
    pub(crate) fn form(&self) -> &Term<Any> {
        self.terms.get("form").unwrap_or(&BLANK)
    }

    /// Replace each term `terms` names; the others keep theirs.
    pub(crate) fn replace(&mut self, terms: &Parameters) {
        for (cell, term) in terms.iter() {
            self.terms.insert(cell.clone(), term.clone());
        }
    }

    /// The rows `analyzer` gives for each row of `selection`, read
    /// through `env`, held to the cells declared for `notation/analyze`.
    /// A row in that binds no form gives none.
    pub(crate) fn evaluate<'a, Env, M: Selection + 'a>(
        self,
        env: &'a Env,
        selection: M,
    ) -> impl Selection + 'a
    where
        Env: Scope<'a>,
    {
        let held = analyzer(env);
        try_stream! {
            let analyzer = held.ok_or_else(|| EvaluationError::Unsupplied {
                formula: ANALYZE.into(),
            })?;
            let facts = Through(env);
            for await candidate in selection {
                let base = candidate?;
                let form = match self.form() {
                    Term::Constant(value) => value.clone(),
                    term => match base.lookup(term) {
                        Ok(Binding::Present(value)) => value,
                        _ => continue,
                    },
                };
                let produced = checked(
                    ANALYZE,
                    &ANALYZE_OUTPUTS,
                    &ANALYZE_CELLS,
                    analyzer.analyze(&form, &facts).await?,
                )?;
                let projected = produced
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .filter_map(|(cell, value)| Some((self.terms.get(cell)?, Some(value))))
                            .collect()
                    })
                    .collect();
                for row in rows(&base, projected)? {
                    yield row;
                }
            }
        }
    }
}

impl TryFrom<Parameters> for NotationAnalyzeQuery {
    type Error = TypeError;

    fn try_from(terms: Parameters) -> Result<Self, TypeError> {
        Self::new(terms)
    }
}

impl From<NotationAnalyzeQuery> for Parameters {
    fn from(query: NotationAnalyzeQuery) -> Self {
        query.terms
    }
}

/// The query's environment, as [`Facts`].
struct Through<'a, Env>(&'a Env);

impl<'a, Env: Scope<'a>> Facts for Through<'a, Env> {
    fn select(&self, selector: Selector<Constrained>) -> Matched<'_> {
        let env = self.0;
        Box::pin(async move {
            let stream = Provider::<Select<'a>>::execute(env, selector).await?;
            Ok(stream.owned().try_collect::<Vec<Fact>>().await?)
        })
    }
}
