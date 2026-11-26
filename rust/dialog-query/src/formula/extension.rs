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

use dialog_common::{ConditionalSend, ConditionalSync};

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
pub trait Extension: fmt::Debug + ConditionalSend + ConditionalSync {
    /// Its name, as a premise names it: `notation/parse`.
    fn name(&self) -> &'static str;

    /// Its cells, inputs and outputs, as a derived formula's.
    fn cells(&self) -> &'static Cells;

    /// What one row in costs, for planning.
    fn cost(&self) -> usize {
        PARAM_COST * self.cells().count()
    }

    /// The rows for the input `bindings` holds: read each input cell
    /// with [`Bindings::read`], and give each output row as the values
    /// of its output cells. The same inputs give the same rows.
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
        let _ = (name, terms);
        todo!("Extensions::apply")
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
    pub(crate) fn resolve(&self, bindings: &mut Bindings) -> Result<Vec<Match>, EvaluationError> {
        let _ = bindings;
        todo!("ExtensionQuery::resolve")
    }
}

impl PartialEq for ExtensionQuery {
    /// The same extension, the one value the embedder gave, over the
    /// same terms.
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.extension, &other.extension) && self.terms == other.terms
    }
}
