//! Formulas an embedder supplies.
//!
//! The formulas the engine knows are a closed set (`FormulaQuery`). An
//! embedder whose own code is a pure function of a premise's inputs, a
//! notation's parser say, gives it to the engine as an [`Extension`] and
//! builds premises of it through the [`Extensions`] it holds. Nothing is
//! registered anywhere else: the premise carries the extension it was
//! built of, so two embedders, or two tests, each with their own
//! `Extensions`, never meet.
//!
//! An extension's premise is evaluated only by a
//! [`PremiseQuery`](crate::query::PremiseQuery). A rule holding one is
//! refused when it is compiled ([`TypeError::EmbedderFormula`]): a rule
//! is stored, laid over a query as facts and read back from its body,
//! and a body cannot say an embedder's code, which a replica without
//! that embedder could not run. A premise naming no extension the
//! embedder holds is refused when it is built
//! ([`TypeError::UnknownFormula`]), and a query written as data naming
//! one does not deserialize, so neither reaches evaluation.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::error::{EvaluationError, TypeError};
use crate::formula::bindings::Bindings;
use crate::formula::cell::Cells;
use crate::formula::query::{FormulaQuery, PARAM_COST};
use crate::selection::Match;
use crate::{Parameters, Value};

/// One row an extension gives for a row in: the value of each output
/// cell it binds, by the cell's name.
pub type Output = Vec<(&'static str, Value)>;

/// A formula an embedder supplies: a pure function of its input cells,
/// giving any number of rows.
///
/// `Send` and `Sync` on every target, the browser's too, where
/// `ConditionalSend` asks for neither: a premise is held in `static`s
/// (the rules a `#[derive(Concept)]` writes), and a static is `Sync`
/// wherever it is compiled, so every premise is, and the extension a
/// premise carries with it.
pub trait Extension: fmt::Debug + Send + Sync {
    /// Its name, as a premise names it: `notation/parse`.
    fn name(&self) -> &'static str;

    /// Its cells, inputs and outputs, as a derived formula's.
    fn cells(&self) -> &'static Cells;

    /// The cells it gives a value for in every row it computes. Every
    /// other cell is an input.
    fn outputs(&self) -> &'static [&'static str];

    /// What one row in costs, for planning.
    fn cost(&self) -> usize {
        PARAM_COST * self.cells().count()
    }

    /// The rows for the input `bindings` holds: read each input cell
    /// with [`Bindings::read`], and give each output row as the values
    /// of its output cells, each once, each of the type its cell
    /// declares. The same inputs give the same rows. A row that names a
    /// cell outside [`outputs`](Self::outputs), leaves one out, or gives
    /// one a value its type does not admit is the extension's fault, and
    /// fails the query naming the extension and the cell.
    fn compute(&self, bindings: &mut Bindings) -> Result<Vec<Output>, EvaluationError>;
}

/// The extensions an embedder holds, by name.
#[derive(Clone, Debug, Default)]
pub struct Extensions {
    by_name: BTreeMap<&'static str, Arc<dyn Extension>>,
}

impl Extensions {
    /// No extensions.
    pub fn new() -> Self {
        Self::default()
    }

    /// These extensions and `extension`, which replaces one of its name.
    pub fn with(mut self, extension: impl Extension + 'static) -> Self {
        self.by_name.insert(extension.name(), Arc::new(extension));
        self
    }

    /// Whether an extension of `name` is held.
    pub fn holds(&self, name: &str) -> bool {
        self.by_name.contains_key(name)
    }

    /// The premise applying the extension `name` to `terms`.
    pub fn apply(&self, name: &str, terms: Parameters) -> Result<FormulaQuery, TypeError> {
        let extension = self
            .by_name
            .get(name)
            .ok_or_else(|| TypeError::UnknownFormula { name: name.into() })?;
        Ok(FormulaQuery::Extension(ExtensionQuery {
            terms: extension.cells().conform(terms)?,
            extension: extension.clone(),
        }))
    }
}

/// A premise of an extension: the extension and the terms of its cells.
#[derive(Clone, Debug)]
pub struct ExtensionQuery {
    pub(crate) extension: Arc<dyn Extension>,
    pub(crate) terms: Parameters,
}

impl ExtensionQuery {
    /// The extension's name.
    pub fn name(&self) -> &'static str {
        self.extension.name()
    }

    /// The terms of its cells.
    pub fn terms(&self) -> &Parameters {
        &self.terms
    }

    /// Its cells.
    pub(crate) fn cells(&self) -> &'static Cells {
        self.extension.cells()
    }

    /// Its cost.
    pub(crate) fn cost(&self) -> usize {
        self.extension.cost()
    }

    /// This premise applied to `terms` instead.
    pub(crate) fn with_terms(&self, terms: Parameters) -> Result<Self, TypeError> {
        Ok(Self {
            extension: self.extension.clone(),
            terms: self.extension.cells().conform(terms)?,
        })
    }

    /// The matches the extension's rows make of `bindings`.
    ///
    /// Each row is first held to the extension's own declaration: a
    /// value for a cell it does not declare as an output, an output left
    /// out, or a value its cell's type does not admit is the extension's
    /// fault and fails the query, naming the extension and the cell.
    /// Only then is the row written to its own copy of the row in, and a
    /// write that disagrees with what that row already binds filters the
    /// row, as every formula's does ([`emit`](crate::formula::emit)).
    pub(crate) fn resolve(&self, bindings: &mut Bindings) -> Result<Vec<Match>, EvaluationError> {
        let formula = self.extension.name();
        let outputs = self.extension.outputs();
        let cells = self.extension.cells();
        let mut matches = Vec::new();
        for output in self.extension.compute(bindings)? {
            for (cell, value) in &output {
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
                .find(|cell| !output.iter().any(|(given, _)| given == *cell))
            {
                return Err(EvaluationError::MissingOutput {
                    formula: formula.into(),
                    cell: (*cell).into(),
                });
            }
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

impl PartialEq for ExtensionQuery {
    /// The same extension, the one value the embedder gave, over the
    /// same terms.
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.extension, &other.extension) && self.terms == other.terms
    }
}
