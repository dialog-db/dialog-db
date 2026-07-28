//! The group-by fold behind the `reduce` clause on deductive rules
//! (`notes/aggregation.md`).
//!
//! This module is the pure runtime core: given a [`Selection`] of rows,
//! the names of the grouping fields, and a list of [`ReduceEntry`]
//! folds, it groups the rows and emits one output [`Match`] per group.
//! It knows nothing about rules, descriptors, or the analyzer — those
//! wire in at milestone A3.
//!
//! Static typing (milestone A2) lives beside the fold:
//! [`Aggregator::input_requirement`] states what a fold can consume as
//! a [`Type`], [`Aggregator::output_type`] computes what it produces —
//! including the optionality algebra below — and the checked
//! constructor [`ReduceEntry::try_new`] rejects an entry whose input
//! type cannot feed its aggregator at construction, as a
//! [`TypeError`]. The dynamic path ([`ReduceEntry::new`]) stays open
//! for untyped callers; for it, every semantic violation an entry can
//! express remains a loud runtime [`EvaluationError`] backstop.
//!
//! # Semantics
//!
//! - **Group key**: the tuple of the grouping fields' bindings, one
//!   `Option<Value>` per field, compared by its canonical dag-cbor
//!   bytes (the fixpoint `AnswerTable` precedent; [`Value`] has no
//!   `Ord`). An [`Binding::Absent`] grouping binding is a legitimate
//!   key component: it encodes as dag-cbor `null`, which no present
//!   [`Value`] produces, so all-absent rows group together, distinct
//!   from every present value (as SQL groups NULLs). A grouping field
//!   that is *unbound* — not in the [`Match`] at all — is an
//!   [`EvaluationError::UnboundVariable`], never a group.
//! - **Absent inputs are skipped**: folds consume only `Present`
//!   bindings. `count` counts present bindings; `count-distinct`
//!   counts distinct present values by their dag-cbor bytes.
//! - **`sum`** accumulates integers (`UnsignedInt` *and* `SignedInt`
//!   — one integer band, because the accumulator is `i128`) with
//!   checked arithmetic and errors loudly on overflow, including an
//!   unsigned input beyond `i128::MAX`. The result is the narrowest
//!   fitting variant: non-negative sums come back `UnsignedInt`,
//!   negative sums `SignedInt`. Floats accumulate in `f64`. Mixing
//!   the integer band with floats in one group is an error: the
//!   numeric machinery (formula arithmetic, the range predicates in
//!   `constraint/compare.rs`) is strictly no-promotion between
//!   integers and floats for data, and a fold input is always data.
//!   The empty (all-absent) group returns the identity
//!   `UnsignedInt(0)`.
//! - **`min`/`max`** use the range-predicate ordering: same-variant
//!   comparison via [`Numeric::compare`] for numeric values —
//!   without literal adaptation, because fold inputs are data —
//!   extended across the COMPARABLE primitive set (strings, symbols,
//!   bytes, entities order by their natural `Ord`). Any incomparable
//!   pair inside a group (mixed variants, NaN) is an error; A2 makes
//!   that unconstructable statically, this is the runtime backstop.
//!   Compare-equal but distinct representations (`-0.0` vs `0.0`)
//!   resolve to the representation with the smaller dag-cbor bytes,
//!   keeping the output independent of row order.
//! - **`avg`** is the `f64` mean of the present numeric values and
//!   always returns [`Value::Float`]; because every input converts
//!   to `f64` anyway, mixed numeric variants are permitted here.
//! - **Empty groups do not exist**: groups arise from rows, so an
//!   empty input stream yields zero output rows even with no
//!   grouping fields. A group whose fold inputs are all Absent
//!   yields the identity for `count`/`sum` and binds the output
//!   field Absent for `min`/`max`/`avg`.
//! - **Determinism**: groups are held in a `BTreeMap` over key bytes
//!   and every fold buffers its column and folds it in a canonical
//!   order (integers sort numerically, floats by `total_cmp`), so
//!   the output — including float rounding and overflow behavior —
//!   is identical for every permutation of the input rows.
//!
//! # Example
//!
//! ```
//! use dialog_query::reduce::{Aggregator, Reduce, ReduceEntry};
//! use dialog_query::term::Term;
//!
//! // Per department: total and average salary.
//! let reduce = Reduce::new(
//!     vec!["dept".to_string()],
//!     vec![
//!         ReduceEntry::new("total", Aggregator::Sum, Term::var("salary")),
//!         ReduceEntry::new("mean", Aggregator::Avg, Term::var("salary")),
//!     ],
//! );
//! // `reduce.evaluate(selection)` folds a stream of rows into one
//! // output row per department.
//! ```

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::Display;
use std::pin::pin;

use futures_util::TryStreamExt;

use crate::artifact::Type as ValueType;
use crate::artifact::Value;
use crate::error::{EvaluationError, TypeError};
use crate::formula::number::Numeric;
use crate::selection::{Binding, Match, Selection};
use crate::term::Term;
use crate::try_stream;
use crate::type_system::{Primitive, Type};
use crate::types::Any;

/// The fold applied to a reduced field's inputs, one group at a time.
///
/// The phase-1 set from `notes/aggregation.md`. `median`, `variance`,
/// `stddev` are addable behind this same enum; `rand`/`sample` are
/// permanently excluded (nondeterministic in a convergent system).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Aggregator {
    /// Number of present bindings in the group.
    Count,
    /// Number of distinct present values in the group, compared by
    /// dag-cbor bytes.
    CountDistinct,
    /// Checked `i128` sum over the integer band, `f64` sum over
    /// floats; identity `0` for the all-absent group.
    Sum,
    /// Least present value under the range-predicate ordering.
    Min,
    /// Greatest present value under the range-predicate ordering.
    Max,
    /// `f64` mean of the present numeric values.
    Avg,
}

impl Display for Aggregator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Aggregator::Count => "count",
            Aggregator::CountDistinct => "count-distinct",
            Aggregator::Sum => "sum",
            Aggregator::Min => "min",
            Aggregator::Max => "max",
            Aggregator::Avg => "avg",
        };
        write!(f, "{name}")
    }
}

impl Aggregator {
    /// The type this fold's inputs must inhabit when present.
    ///
    /// `sum`/`avg` consume the numeric band; `min`/`max` the
    /// COMPARABLE set — exactly the range-predicate ordering they
    /// fold with; `count`/`count-distinct` accept anything present.
    /// Expressed as a [`Type`] so compatibility is a meet: an input
    /// type can feed this fold iff the meet of its present part with
    /// this requirement is non-empty.
    pub fn input_requirement(self) -> Type {
        let primitive = match self {
            Aggregator::Count | Aggregator::CountDistinct => Primitive::ALL,
            Aggregator::Sum | Aggregator::Avg => Primitive::NUMERIC,
            Aggregator::Min | Aggregator::Max => Primitive::COMPARABLE,
        };
        Type::from(primitive)
    }

    /// The output type of this fold over an input of the given type
    /// — the algebra from `notes/aggregation.md` — or `None` when
    /// the input cannot feed this fold at all: no present shape of
    /// the input meets [`Aggregator::input_requirement`] (a
    /// `Nothing`-only input, being never present, feeds nothing).
    ///
    /// - `count`/`count-distinct` produce `UnsignedInt`, never
    ///   optional: the identity 0 exists, so even an optional input
    ///   yields a present output.
    /// - `sum` produces the input's numeric band, never optional
    ///   (identity 0). The integer band stays integral — the `i128`
    ///   accumulator narrows per group to `UnsignedInt` or
    ///   `SignedInt`, so an input touching either integer type
    ///   admits both — and floats stay `Float`.
    /// - `min`/`max` produce the input type itself (the result is
    ///   one of the inputs, so a refinement rides along); `avg`
    ///   produces `Float`. These three have no identity: an
    ///   optional input propagates `Nothing` into the output type.
    ///   That propagation is what lets the existing
    ///   `RequiredHeadFromOptional` check enforce — with no new
    ///   analyzer rule — that a head field fed by `min`/`max`/`avg`
    ///   over an optional input must itself be declared optional.
    pub fn output_type(self, input: &Type) -> Option<Type> {
        // What the fold consumes: the present shapes of the input
        // that meet the requirement.
        let consumed = input
            .clone()
            .required()
            .intersect(&self.input_requirement())?;
        let optional = input.is_optional();
        Some(match self {
            Aggregator::Count | Aggregator::CountDistinct => Type::from(ValueType::UnsignedInt),
            Aggregator::Sum => {
                let integers = Primitive::singleton(ValueType::UnsignedInt)
                    .union(Primitive::singleton(ValueType::SignedInt));
                let consumed = consumed.primitive_part();
                let mut band = Primitive::EMPTY;
                if consumed.intersect(integers).is_some() {
                    band = band.union(integers);
                }
                if consumed.contains(ValueType::Float) {
                    band = band.union(Primitive::singleton(ValueType::Float));
                }
                Type::from(band)
            }
            Aggregator::Min | Aggregator::Max => {
                if optional {
                    consumed.optional()
                } else {
                    consumed
                }
            }
            Aggregator::Avg => {
                let mean = Type::from(ValueType::Float);
                if optional { mean.optional() } else { mean }
            }
        })
    }
}

/// One `reduce` block entry in the formal notation: the fold to
/// apply and the term supplying its input. The output field name is
/// the entry's *key* in the descriptor's name-keyed `reduce` map, so
/// a field being both grouped and reduced is unrepresentable — a
/// `BTreeMap` key is either present (reduced) or absent (grouping).
///
/// ```json
/// "reduce": { "total": { "apply": "sum", "of": { "?": { "name": "salary" } } } }
/// ```
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ReduceSpec {
    /// The fold applied to the group's inputs.
    pub apply: Aggregator,
    /// The term supplying the fold's input, one lookup per body row.
    pub of: Term<Any>,
}

impl From<&ReduceEntry> for ReduceSpec {
    fn from(entry: &ReduceEntry) -> Self {
        ReduceSpec {
            apply: entry.aggregator,
            of: entry.input.clone(),
        }
    }
}

/// One reduced output field: the field name, the fold to apply, and
/// the term supplying the fold's input from each row.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ReduceEntry {
    /// Name of the output field this fold binds on every emitted row.
    pub field: String,
    /// The fold applied to the group's inputs.
    pub aggregator: Aggregator,
    /// The term supplying the fold's input, looked up per row. An
    /// `Absent` binding is skipped; an unbound variable is an
    /// evaluation error.
    pub input: Term<Any>,
}

impl ReduceEntry {
    /// Create a reduce entry binding `field` to `aggregator` folded
    /// over `input`.
    ///
    /// This is the *dynamic* path: no input type is known, so
    /// nothing is checked and every violation surfaces as a runtime
    /// [`EvaluationError`]. Wherever the input's type is declared or
    /// inferred, construct through [`ReduceEntry::try_new`] instead.
    pub fn new(field: impl Into<String>, aggregator: Aggregator, input: Term<Any>) -> Self {
        Self {
            field: field.into(),
            aggregator,
            input,
        }
    }

    /// Checked construction against the declared or inferred type of
    /// the input — the valid-by-construction path (the
    /// `NamedAttributes::try_new` precedent).
    ///
    /// Fails with [`TypeError::ReduceInput`] when no present shape
    /// of `input_type` meets the aggregator's requirement: `sum`
    /// over a `String` input, `min` over a non-comparable input, or
    /// any fold over a `Nothing`-only input is unwriteable here, at
    /// construction, rather than a fold-time failure.
    pub fn try_new(
        field: impl Into<String>,
        aggregator: Aggregator,
        input: Term<Any>,
        input_type: &Type,
    ) -> Result<Self, TypeError> {
        let field = field.into();
        if aggregator.output_type(input_type).is_none() {
            return Err(TypeError::ReduceInput {
                field,
                aggregator,
                required: Box::new(aggregator.input_requirement()),
                actual: Box::new(input_type.clone()),
            });
        }
        Ok(Self {
            field,
            aggregator,
            input,
        })
    }

    /// The type this entry binds its output field to, given the
    /// input's type: [`Aggregator::output_type`] with the entry's
    /// field attached to the failure.
    pub fn output_type(&self, input_type: &Type) -> Result<Type, TypeError> {
        self.aggregator
            .output_type(input_type)
            .ok_or_else(|| TypeError::ReduceInput {
                field: self.field.clone(),
                aggregator: self.aggregator,
                required: Box::new(self.aggregator.input_requirement()),
                actual: Box::new(input_type.clone()),
            })
    }
}

/// The group-by fold: grouping field names plus reduce entries.
///
/// Grouping fields are the non-reduced head fields of the eventual
/// `reduce` clause; an empty list means one global group (per input
/// row — an empty input still yields zero rows).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Reduce {
    /// Names of the grouping fields. Every input row must have each
    /// of these bound (Present or Absent).
    pub groups: Vec<String>,
    /// The folds computed per group.
    pub entries: Vec<ReduceEntry>,
}

/// Buffered per-group state: the decoded key tuple and one column of
/// present input values per reduce entry.
struct Group {
    key: Vec<Option<Value>>,
    columns: Vec<Vec<Value>>,
}

impl Reduce {
    /// Create a reduce over the given grouping field names and
    /// entries.
    pub fn new(groups: Vec<String>, entries: Vec<ReduceEntry>) -> Self {
        Self { groups, entries }
    }

    /// Consume the selection, group its rows, and fold each group
    /// into one output row.
    ///
    /// Output rows bind the grouping fields to the group key values
    /// (Present or Absent) and each reduce output field to its fold
    /// result — Absent for the identity-less folds over an all-absent
    /// column. Rows come back ordered by group key bytes, so the
    /// result is independent of input row order.
    pub async fn fold(self, selection: impl Selection) -> Result<Vec<Match>, EvaluationError> {
        let Self { groups, entries } = self;
        let group_terms: Vec<Term<Any>> =
            groups.iter().map(|name| Term::var(name.as_str())).collect();

        let mut table: BTreeMap<Vec<u8>, Group> = BTreeMap::new();
        let mut selection = pin!(selection);
        while let Some(row) = selection.try_next().await? {
            let mut key = Vec::with_capacity(group_terms.len());
            for term in &group_terms {
                // Unbound propagates as UnboundVariable; Absent is a
                // legitimate key component.
                match row.lookup(term)? {
                    Binding::Present(value) => key.push(Some(value)),
                    Binding::Absent => key.push(None),
                }
            }
            let key_bytes = encode(&key)?;
            let group = table.entry(key_bytes).or_insert_with(|| Group {
                key,
                columns: vec![Vec::new(); entries.len()],
            });
            for (column, entry) in group.columns.iter_mut().zip(&entries) {
                match row.lookup(&entry.input)? {
                    Binding::Present(value) => column.push(value),
                    Binding::Absent => {}
                }
            }
        }

        let mut output = Vec::with_capacity(table.len());
        for group in table.into_values() {
            let mut row = Match::new();
            for (term, value) in group_terms.iter().zip(group.key) {
                match value {
                    Some(value) => row.bind(term, value)?,
                    None => row.bind_absent(term)?,
                }
            }
            for (entry, column) in entries.iter().zip(group.columns) {
                let out: Term<Any> = Term::var(entry.field.as_str());
                match fold_column(entry, column)? {
                    Some(value) => row.bind(&out, value)?,
                    None => row.bind_absent(&out)?,
                }
            }
            output.push(row);
        }
        Ok(output)
    }

    /// Evaluate as a premise-shaped stream transformer: consume the
    /// whole input selection, emit one row per group.
    pub fn evaluate<M: Selection>(self, selection: M) -> impl Selection {
        try_stream! {
            let rows = self.fold(selection).await?;
            for row in rows {
                yield row;
            }
        }
    }
}

/// Canonical dag-cbor bytes of a value or key tuple: the identity
/// used for group keys, count-distinct, and equal-tie resolution.
fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, EvaluationError> {
    serde_ipld_dagcbor::to_vec(value).map_err(|error| EvaluationError::Serialization {
        message: error.to_string(),
    })
}

/// A fold failure scoped to its entry.
fn fault(entry: &ReduceEntry, reason: impl Into<String>) -> EvaluationError {
    EvaluationError::Reduce {
        field: entry.field.clone(),
        aggregator: entry.aggregator.to_string(),
        reason: reason.into(),
    }
}

/// Fold one group's column of present values. `None` means the
/// output field binds Absent (identity-less fold, no present input).
fn fold_column(entry: &ReduceEntry, values: Vec<Value>) -> Result<Option<Value>, EvaluationError> {
    match entry.aggregator {
        Aggregator::Count => Ok(Some(Value::UnsignedInt(values.len() as u128))),
        Aggregator::CountDistinct => {
            let mut distinct = BTreeSet::new();
            for value in &values {
                distinct.insert(encode(value)?);
            }
            Ok(Some(Value::UnsignedInt(distinct.len() as u128)))
        }
        Aggregator::Sum => fold_sum(entry, values).map(Some),
        Aggregator::Min => fold_extremum(entry, values, Ordering::Less),
        Aggregator::Max => fold_extremum(entry, values, Ordering::Greater),
        Aggregator::Avg => fold_avg(entry, values),
    }
}

/// Checked sum: integers in `i128`, floats in `f64`, never mixed.
/// Folds in sorted order so overflow behavior and float rounding are
/// independent of row order.
fn fold_sum(entry: &ReduceEntry, values: Vec<Value>) -> Result<Value, EvaluationError> {
    let mut integers: Vec<i128> = Vec::new();
    let mut floats: Vec<f64> = Vec::new();
    for value in values {
        match value {
            Value::UnsignedInt(value) => integers
                .push(i128::try_from(value).map_err(|_| {
                    fault(entry, format!("{value} overflows the i128 accumulator"))
                })?),
            Value::SignedInt(value) => integers.push(value),
            Value::Float(value) => floats.push(value),
            other => {
                return Err(fault(entry, format!("non-numeric input {other:?}")));
            }
        }
    }
    if !integers.is_empty() && !floats.is_empty() {
        return Err(fault(
            entry,
            "mixed integer and float inputs in one group; \
             the numeric machinery does not promote between them",
        ));
    }
    if floats.is_empty() {
        integers.sort_unstable();
        let mut total: i128 = 0;
        for value in integers {
            total = total
                .checked_add(value)
                .ok_or_else(|| fault(entry, "sum overflows the i128 accumulator"))?;
        }
        if total >= 0 {
            Ok(Value::UnsignedInt(total as u128))
        } else {
            Ok(Value::SignedInt(total))
        }
    } else {
        floats.sort_unstable_by(|a, b| a.total_cmp(b));
        Ok(Value::Float(floats.into_iter().sum()))
    }
}

/// `f64` mean of the present numeric values; `None` (Absent) over an
/// empty column. Folds in `total_cmp` order for row-order-independent
/// rounding.
fn fold_avg(entry: &ReduceEntry, values: Vec<Value>) -> Result<Option<Value>, EvaluationError> {
    if values.is_empty() {
        return Ok(None);
    }
    let mut samples = Vec::with_capacity(values.len());
    for value in values {
        match value {
            Value::UnsignedInt(value) => samples.push(value as f64),
            Value::SignedInt(value) => samples.push(value as f64),
            Value::Float(value) => samples.push(value),
            other => {
                return Err(fault(entry, format!("non-numeric input {other:?}")));
            }
        }
    }
    samples.sort_unstable_by(|a, b| a.total_cmp(b));
    let count = samples.len() as f64;
    let total: f64 = samples.into_iter().sum();
    Ok(Some(Value::Float(total / count)))
}

/// The range-predicate ordering over data: same-variant numeric
/// comparison via [`Numeric::compare`] (no literal adaptation — fold
/// inputs are data), extended to the rest of the COMPARABLE set by
/// each variant's natural order. `None` is an incomparable pair.
fn order(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        (Value::Symbol(a), Value::Symbol(b)) => Some(a.cmp(b)),
        (Value::Bytes(a), Value::Bytes(b)) => Some(a.cmp(b)),
        (Value::Entity(a), Value::Entity(b)) => Some(a.cmp(b)),
        _ => {
            let a = Numeric::try_from(a.clone()).ok()?;
            let b = Numeric::try_from(b.clone()).ok()?;
            a.compare(b)
        }
    }
}

/// `min` (`target = Less`) / `max` (`target = Greater`): keep the
/// value that stands in `target` relation to the current best. An
/// incomparable pair errors. Compare-equal but byte-distinct values
/// (`-0.0` vs `0.0`) resolve to the smaller dag-cbor encoding so the
/// pick is independent of row order.
fn fold_extremum(
    entry: &ReduceEntry,
    values: Vec<Value>,
    target: Ordering,
) -> Result<Option<Value>, EvaluationError> {
    let mut values = values.into_iter();
    let Some(mut best) = values.next() else {
        return Ok(None);
    };
    for candidate in values {
        match order(&candidate, &best) {
            Some(Ordering::Equal) => {
                if encode(&candidate)? < encode(&best)? {
                    best = candidate;
                }
            }
            Some(ordering) if ordering == target => best = candidate,
            Some(_) => {}
            None => {
                return Err(fault(
                    entry,
                    format!("cannot order {candidate:?} against {best:?}"),
                ));
            }
        }
    }
    Ok(Some(best))
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use futures_util::stream::iter;

    /// A selection over owned rows, in the given order.
    fn selection(rows: Vec<Match>) -> impl Selection {
        iter(rows.into_iter().map(Ok))
    }

    /// A row binding each `(name, value)` pair Present and each
    /// `(name, None)` Absent.
    fn row(bindings: &[(&str, Option<Value>)]) -> Match {
        let mut row = Match::new();
        for (name, value) in bindings {
            let term: Term<Any> = Term::var(*name);
            match value {
                Some(value) => row.bind(&term, value.clone()).unwrap(),
                None => row.bind_absent(&term).unwrap(),
            }
        }
        row
    }

    /// Find the output row whose `field` is bound to `value`.
    fn find<'a>(rows: &'a [Match], field: &str, value: &Value) -> &'a Match {
        rows.iter()
            .find(|row| {
                row.lookup(&Term::var(field))
                    .ok()
                    .and_then(|binding| binding.as_value().cloned())
                    .as_ref()
                    == Some(value)
            })
            .unwrap_or_else(|| panic!("no row with {field} = {value:?}"))
    }

    fn present(row: &Match, field: &str) -> Value {
        row.lookup(&Term::var(field)).unwrap().content().unwrap()
    }

    fn dept(name: &str) -> Value {
        Value::String(name.to_string())
    }

    #[dialog_common::test]
    async fn it_groups_and_folds_multiple_entries() {
        let reduce = Reduce::new(
            vec!["dept".to_string()],
            vec![
                ReduceEntry::new("total", Aggregator::Sum, Term::var("salary")),
                ReduceEntry::new("n", Aggregator::Count, Term::var("salary")),
            ],
        );
        let rows = reduce
            .fold(selection(vec![
                row(&[
                    ("dept", Some(dept("eng"))),
                    ("salary", Some(Value::UnsignedInt(10))),
                ]),
                row(&[
                    ("dept", Some(dept("eng"))),
                    ("salary", Some(Value::UnsignedInt(20))),
                ]),
                row(&[
                    ("dept", Some(dept("ops"))),
                    ("salary", Some(Value::UnsignedInt(5))),
                ]),
            ]))
            .await
            .unwrap();

        assert_eq!(rows.len(), 2);
        let eng = find(&rows, "dept", &dept("eng"));
        assert_eq!(present(eng, "total"), Value::UnsignedInt(30));
        assert_eq!(present(eng, "n"), Value::UnsignedInt(2));
        let ops = find(&rows, "dept", &dept("ops"));
        assert_eq!(present(ops, "total"), Value::UnsignedInt(5));
        assert_eq!(present(ops, "n"), Value::UnsignedInt(1));
    }

    #[dialog_common::test]
    async fn it_skips_absent_inputs() {
        let reduce = Reduce::new(
            vec!["dept".to_string()],
            vec![
                ReduceEntry::new("total", Aggregator::Sum, Term::var("salary")),
                ReduceEntry::new("n", Aggregator::Count, Term::var("salary")),
            ],
        );
        let rows = reduce
            .fold(selection(vec![
                row(&[
                    ("dept", Some(dept("eng"))),
                    ("salary", Some(Value::UnsignedInt(10))),
                ]),
                row(&[("dept", Some(dept("eng"))), ("salary", None)]),
            ]))
            .await
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(present(&rows[0], "total"), Value::UnsignedInt(10));
        assert_eq!(present(&rows[0], "n"), Value::UnsignedInt(1));
    }

    /// An Absent grouping binding is a legitimate group-key value,
    /// distinct from every present value.
    #[dialog_common::test]
    async fn it_groups_absent_keys_together_and_apart_from_present() {
        let reduce = Reduce::new(
            vec!["dept".to_string()],
            vec![ReduceEntry::new("n", Aggregator::Count, Term::var("x"))],
        );
        let rows = reduce
            .fold(selection(vec![
                row(&[
                    ("dept", Some(dept("eng"))),
                    ("x", Some(Value::UnsignedInt(1))),
                ]),
                row(&[("dept", None), ("x", Some(Value::UnsignedInt(2)))]),
                row(&[("dept", None), ("x", Some(Value::UnsignedInt(3)))]),
            ]))
            .await
            .unwrap();

        assert_eq!(rows.len(), 2);
        let absent_group = rows
            .iter()
            .find(|row| row.lookup(&Term::var("dept")).unwrap() == Binding::Absent)
            .expect("the Absent-keyed group exists");
        assert_eq!(present(absent_group, "n"), Value::UnsignedInt(2));
        let eng = find(&rows, "dept", &dept("eng"));
        assert_eq!(present(eng, "n"), Value::UnsignedInt(1));
    }

    /// All-absent group: `count`/`sum` produce their identities,
    /// `min`/`avg` bind the output field Absent.
    #[dialog_common::test]
    async fn it_folds_the_all_absent_group() {
        let reduce = Reduce::new(
            vec!["dept".to_string()],
            vec![
                ReduceEntry::new("n", Aggregator::Count, Term::var("salary")),
                ReduceEntry::new("total", Aggregator::Sum, Term::var("salary")),
                ReduceEntry::new("least", Aggregator::Min, Term::var("salary")),
                ReduceEntry::new("mean", Aggregator::Avg, Term::var("salary")),
            ],
        );
        let rows = reduce
            .fold(selection(vec![
                row(&[("dept", Some(dept("eng"))), ("salary", None)]),
                row(&[("dept", Some(dept("eng"))), ("salary", None)]),
            ]))
            .await
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(present(&rows[0], "n"), Value::UnsignedInt(0));
        assert_eq!(present(&rows[0], "total"), Value::UnsignedInt(0));
        assert_eq!(
            rows[0].lookup(&Term::var("least")).unwrap(),
            Binding::Absent
        );
        assert_eq!(rows[0].lookup(&Term::var("mean")).unwrap(), Binding::Absent);
    }

    /// A grouping field no premise touched is an evaluation error,
    /// distinct from Absent.
    #[dialog_common::test]
    async fn it_errors_on_unbound_grouping_field() {
        let reduce = Reduce::new(
            vec!["dept".to_string()],
            vec![ReduceEntry::new("n", Aggregator::Count, Term::var("x"))],
        );
        let result = reduce
            .fold(selection(vec![row(&[("x", Some(Value::UnsignedInt(1)))])]))
            .await;
        match result {
            Err(EvaluationError::UnboundVariable { variable_name }) => {
                assert_eq!(variable_name, "dept");
            }
            other => panic!("expected UnboundVariable, got {other:?}"),
        }
    }

    #[dialog_common::test]
    async fn it_errors_loudly_on_sum_overflow() {
        // An unsigned input beyond i128::MAX cannot enter the
        // accumulator.
        let reduce = Reduce::new(
            vec![],
            vec![ReduceEntry::new("total", Aggregator::Sum, Term::var("x"))],
        );
        let result = reduce
            .fold(selection(vec![row(&[(
                "x",
                Some(Value::UnsignedInt(u128::MAX)),
            )])]))
            .await;
        assert!(
            matches!(result, Err(EvaluationError::Reduce { .. })),
            "u128::MAX must overflow the accumulator, got {result:?}"
        );

        // A running total past i128::MAX overflows.
        let reduce = Reduce::new(
            vec![],
            vec![ReduceEntry::new("total", Aggregator::Sum, Term::var("x"))],
        );
        let result = reduce
            .fold(selection(vec![
                row(&[("x", Some(Value::SignedInt(i128::MAX)))]),
                row(&[("x", Some(Value::SignedInt(1)))]),
            ]))
            .await;
        assert!(
            matches!(result, Err(EvaluationError::Reduce { .. })),
            "i128::MAX + 1 must overflow, got {result:?}"
        );
    }

    /// Both integer variants share the i128 accumulator; the result
    /// takes the narrowest fitting variant.
    #[dialog_common::test]
    async fn it_sums_signed_and_unsigned_in_one_band() {
        let reduce = Reduce::new(
            vec![],
            vec![ReduceEntry::new("total", Aggregator::Sum, Term::var("x"))],
        );
        let rows = reduce
            .fold(selection(vec![
                row(&[("x", Some(Value::UnsignedInt(1)))]),
                row(&[("x", Some(Value::SignedInt(-3)))]),
            ]))
            .await
            .unwrap();
        assert_eq!(present(&rows[0], "total"), Value::SignedInt(-2));

        let reduce = Reduce::new(
            vec![],
            vec![ReduceEntry::new("total", Aggregator::Sum, Term::var("x"))],
        );
        let rows = reduce
            .fold(selection(vec![
                row(&[("x", Some(Value::SignedInt(2)))]),
                row(&[("x", Some(Value::SignedInt(3)))]),
            ]))
            .await
            .unwrap();
        assert_eq!(
            present(&rows[0], "total"),
            Value::UnsignedInt(5),
            "a non-negative sum narrows to UnsignedInt"
        );
    }

    /// Integer and float inputs in one group do not promote — same
    /// strictness as formula arithmetic and the range predicates.
    #[dialog_common::test]
    async fn it_errors_on_mixed_integer_and_float_sum() {
        let reduce = Reduce::new(
            vec![],
            vec![ReduceEntry::new("total", Aggregator::Sum, Term::var("x"))],
        );
        let result = reduce
            .fold(selection(vec![
                row(&[("x", Some(Value::UnsignedInt(1)))]),
                row(&[("x", Some(Value::Float(1.5)))]),
            ]))
            .await;
        assert!(
            matches!(result, Err(EvaluationError::Reduce { .. })),
            "mixed int/float must error, got {result:?}"
        );
    }

    /// count-distinct dedups by dag-cbor bytes: equal values collapse,
    /// same-lexeme values of different types stay distinct.
    #[dialog_common::test]
    async fn it_counts_distinct_values_by_bytes() {
        let reduce = Reduce::new(
            vec![],
            vec![ReduceEntry::new(
                "kinds",
                Aggregator::CountDistinct,
                Term::var("x"),
            )],
        );
        let rows = reduce
            .fold(selection(vec![
                row(&[("x", Some(Value::UnsignedInt(1)))]),
                row(&[("x", Some(Value::UnsignedInt(1)))]),
                row(&[("x", Some(Value::String("1".to_string())))]),
                row(&[("x", None)]),
            ]))
            .await
            .unwrap();
        assert_eq!(present(&rows[0], "kinds"), Value::UnsignedInt(2));
    }

    /// Groups arise from rows: no rows, no groups — even with no
    /// grouping fields.
    #[dialog_common::test]
    async fn it_yields_no_rows_for_an_empty_stream() {
        let reduce = Reduce::new(
            vec![],
            vec![ReduceEntry::new("n", Aggregator::Count, Term::var("x"))],
        );
        let rows = reduce.fold(selection(vec![])).await.unwrap();
        assert!(rows.is_empty());
    }

    /// No grouping fields = one global group over all rows.
    #[dialog_common::test]
    async fn it_folds_one_global_group_without_grouping_fields() {
        let reduce = Reduce::new(
            vec![],
            vec![ReduceEntry::new("total", Aggregator::Sum, Term::var("x"))],
        );
        let rows = reduce
            .fold(selection(vec![
                row(&[("x", Some(Value::UnsignedInt(1)))]),
                row(&[("x", Some(Value::UnsignedInt(2)))]),
                row(&[("x", Some(Value::UnsignedInt(3)))]),
            ]))
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(present(&rows[0], "total"), Value::UnsignedInt(6));
    }

    /// The output — including float accumulation — is identical for
    /// every permutation of the input rows.
    #[dialog_common::test]
    async fn it_is_deterministic_across_permutations() {
        let base = [
            row(&[("dept", Some(dept("eng"))), ("x", Some(Value::Float(1e16)))]),
            row(&[("dept", Some(dept("eng"))), ("x", Some(Value::Float(1.0)))]),
            row(&[
                ("dept", Some(dept("eng"))),
                ("x", Some(Value::Float(-1e16))),
            ]),
            row(&[("dept", Some(dept("ops"))), ("x", Some(Value::Float(0.1)))]),
            row(&[("dept", Some(dept("ops"))), ("x", Some(Value::Float(0.2)))]),
        ];
        let entries = || {
            vec![
                ReduceEntry::new("total", Aggregator::Sum, Term::var("x")),
                ReduceEntry::new("mean", Aggregator::Avg, Term::var("x")),
                ReduceEntry::new("least", Aggregator::Min, Term::var("x")),
                ReduceEntry::new("greatest", Aggregator::Max, Term::var("x")),
                ReduceEntry::new("n", Aggregator::Count, Term::var("x")),
                ReduceEntry::new("kinds", Aggregator::CountDistinct, Term::var("x")),
            ]
        };

        let mut outputs = Vec::new();
        for permutation in [[0, 1, 2, 3, 4], [4, 3, 2, 1, 0], [2, 0, 4, 1, 3]] {
            let rows = permutation
                .into_iter()
                .map(|index: usize| base[index].clone())
                .collect();
            let reduce = Reduce::new(vec!["dept".to_string()], entries());
            outputs.push(reduce.fold(selection(rows)).await.unwrap());
        }
        assert_eq!(outputs[0], outputs[1]);
        assert_eq!(outputs[0], outputs[2]);
    }

    /// min/max order through the range-predicate machinery for
    /// numerics and through natural order for strings.
    #[dialog_common::test]
    async fn it_orders_min_max_via_the_compare_machinery() {
        let reduce = Reduce::new(
            vec![],
            vec![
                ReduceEntry::new("least", Aggregator::Min, Term::var("x")),
                ReduceEntry::new("greatest", Aggregator::Max, Term::var("x")),
            ],
        );
        let rows = reduce
            .fold(selection(vec![
                row(&[("x", Some(Value::UnsignedInt(3)))]),
                row(&[("x", Some(Value::UnsignedInt(1)))]),
                row(&[("x", Some(Value::UnsignedInt(2)))]),
            ]))
            .await
            .unwrap();
        assert_eq!(present(&rows[0], "least"), Value::UnsignedInt(1));
        assert_eq!(present(&rows[0], "greatest"), Value::UnsignedInt(3));

        let reduce = Reduce::new(
            vec![],
            vec![
                ReduceEntry::new("least", Aggregator::Min, Term::var("x")),
                ReduceEntry::new("greatest", Aggregator::Max, Term::var("x")),
            ],
        );
        let rows = reduce
            .fold(selection(vec![
                row(&[("x", Some(Value::String("pear".to_string())))]),
                row(&[("x", Some(Value::String("apple".to_string())))]),
                row(&[("x", Some(Value::String("quince".to_string())))]),
            ]))
            .await
            .unwrap();
        assert_eq!(
            present(&rows[0], "least"),
            Value::String("apple".to_string())
        );
        assert_eq!(
            present(&rows[0], "greatest"),
            Value::String("quince".to_string())
        );
    }

    /// An incomparable pair inside a group is a loud error — the A1
    /// runtime backstop for what A2 makes unconstructable.
    #[dialog_common::test]
    async fn it_errors_on_incomparable_min_inputs() {
        let reduce = Reduce::new(
            vec![],
            vec![ReduceEntry::new("least", Aggregator::Min, Term::var("x"))],
        );
        let result = reduce
            .fold(selection(vec![
                row(&[("x", Some(Value::UnsignedInt(1)))]),
                row(&[("x", Some(Value::String("a".to_string())))]),
            ]))
            .await;
        assert!(
            matches!(result, Err(EvaluationError::Reduce { .. })),
            "string vs integer must error, got {result:?}"
        );

        // Mixed integer variants are incomparable data, exactly like
        // the range predicates.
        let reduce = Reduce::new(
            vec![],
            vec![ReduceEntry::new("least", Aggregator::Min, Term::var("x"))],
        );
        let result = reduce
            .fold(selection(vec![
                row(&[("x", Some(Value::UnsignedInt(1)))]),
                row(&[("x", Some(Value::SignedInt(-1)))]),
            ]))
            .await;
        assert!(
            matches!(result, Err(EvaluationError::Reduce { .. })),
            "unsigned vs signed data must error, got {result:?}"
        );
    }

    #[dialog_common::test]
    async fn it_averages_to_float() {
        let reduce = Reduce::new(
            vec![],
            vec![ReduceEntry::new("mean", Aggregator::Avg, Term::var("x"))],
        );
        let rows = reduce
            .fold(selection(vec![
                row(&[("x", Some(Value::UnsignedInt(1)))]),
                row(&[("x", Some(Value::UnsignedInt(2)))]),
                row(&[("x", Some(Value::UnsignedInt(6)))]),
            ]))
            .await
            .unwrap();
        assert_eq!(present(&rows[0], "mean"), Value::Float(3.0));
    }

    /// `evaluate` composes like any premise: selection in, selection
    /// out.
    #[dialog_common::test]
    async fn it_evaluates_as_a_selection() {
        let reduce = Reduce::new(
            vec!["dept".to_string()],
            vec![ReduceEntry::new("n", Aggregator::Count, Term::var("x"))],
        );
        let rows: Vec<Match> = reduce
            .evaluate(selection(vec![
                row(&[
                    ("dept", Some(dept("eng"))),
                    ("x", Some(Value::UnsignedInt(1))),
                ]),
                row(&[
                    ("dept", Some(dept("eng"))),
                    ("x", Some(Value::UnsignedInt(2))),
                ]),
            ]))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(present(&rows[0], "n"), Value::UnsignedInt(2));
    }

    fn typed(vt: ValueType) -> Type {
        Type::from(vt)
    }

    /// The integer band `sum` produces: `UnsignedInt|SignedInt`.
    fn int_band() -> Type {
        Type::from(
            Primitive::singleton(ValueType::UnsignedInt)
                .union(Primitive::singleton(ValueType::SignedInt)),
        )
    }

    /// Constructing an entry whose input type cannot feed its
    /// aggregator fails at construction with the typed error, per
    /// aggregator: `sum`/`avg` reject non-numeric input, `min`/`max`
    /// reject non-comparable input, and even `count` rejects a
    /// `Nothing`-only input (never present, so nothing to count).
    #[dialog_common::test]
    fn it_rejects_incompatible_inputs_at_construction() {
        let rejected = [
            (Aggregator::Sum, typed(ValueType::String)),
            (Aggregator::Avg, typed(ValueType::String)),
            (Aggregator::Sum, typed(ValueType::Boolean)),
            (Aggregator::Min, typed(ValueType::Boolean)),
            (Aggregator::Max, typed(ValueType::Record)),
            (Aggregator::Count, Type::nothing()),
            (Aggregator::CountDistinct, Type::nothing()),
        ];
        for (aggregator, input_type) in rejected {
            let result = ReduceEntry::try_new("out", aggregator, Term::var("x"), &input_type);
            match result {
                Err(TypeError::ReduceInput {
                    field,
                    aggregator: reported,
                    required,
                    actual,
                }) => {
                    assert_eq!(field, "out");
                    assert_eq!(reported, aggregator);
                    assert_eq!(*required, aggregator.input_requirement());
                    assert_eq!(*actual, input_type);
                }
                other => panic!("{aggregator} over {input_type} must fail, got {other:?}"),
            }
        }
    }

    /// `count`/`count-distinct` accept anything present, including
    /// non-comparable, non-numeric shapes.
    #[dialog_common::test]
    fn it_accepts_any_present_input_for_count() {
        for aggregator in [Aggregator::Count, Aggregator::CountDistinct] {
            for input_type in [
                typed(ValueType::String),
                typed(ValueType::Boolean),
                typed(ValueType::Record),
                typed(ValueType::Record).optional(),
            ] {
                assert!(
                    ReduceEntry::try_new("n", aggregator, Term::var("x"), &input_type).is_ok(),
                    "{aggregator} over {input_type} must construct"
                );
            }
        }
    }

    /// `count`/`count-distinct` produce `UnsignedInt` and — because
    /// the identity 0 exists — a *required* output even over an
    /// optional input.
    #[dialog_common::test]
    fn it_types_count_output_as_required_unsigned() {
        for aggregator in [Aggregator::Count, Aggregator::CountDistinct] {
            for input_type in [
                typed(ValueType::String),
                typed(ValueType::String).optional(),
            ] {
                let output = aggregator.output_type(&input_type).unwrap();
                assert_eq!(output, typed(ValueType::UnsignedInt));
                assert!(!output.is_optional(), "count has an identity");
            }
        }
    }

    /// `sum` keeps the input's numeric band: either integer variant
    /// admits the whole integer band (the accumulator narrows per
    /// group), floats stay `Float`, and — identity 0 — the output is
    /// required even over an optional input.
    #[dialog_common::test]
    fn it_types_sum_output_by_numeric_band() {
        for input_type in [
            typed(ValueType::UnsignedInt),
            typed(ValueType::SignedInt),
            int_band(),
            typed(ValueType::UnsignedInt).optional(),
        ] {
            let output = Aggregator::Sum.output_type(&input_type).unwrap();
            assert_eq!(output, int_band(), "integer input sums to the integer band");
            assert!(!output.is_optional(), "sum has an identity");
        }

        let output = Aggregator::Sum
            .output_type(&typed(ValueType::Float).optional())
            .unwrap();
        assert_eq!(output, typed(ValueType::Float), "float input sums to Float");

        let numeric = Type::from(Primitive::NUMERIC);
        let output = Aggregator::Sum.output_type(&numeric).unwrap();
        assert_eq!(
            output, numeric,
            "the full numeric band sums to the full numeric band"
        );
    }

    /// `min`/`max` produce the input type itself and `avg` produces
    /// `Float`; none has an identity, so optionality propagates:
    /// optional input, optional output — required input, required
    /// output. This propagation is what routes the A3 head check
    /// through the existing `RequiredHeadFromOptional` rule.
    #[dialog_common::test]
    fn it_propagates_optionality_for_identityless_folds() {
        for aggregator in [Aggregator::Min, Aggregator::Max] {
            let output = aggregator.output_type(&typed(ValueType::String)).unwrap();
            assert_eq!(output, typed(ValueType::String));
            assert!(!output.is_optional(), "required input, required output");

            let output = aggregator
                .output_type(&typed(ValueType::Float).optional())
                .unwrap();
            assert_eq!(output, typed(ValueType::Float).optional());
            assert!(output.is_optional(), "optional input, optional output");
        }

        let output = Aggregator::Avg
            .output_type(&typed(ValueType::UnsignedInt))
            .unwrap();
        assert_eq!(output, typed(ValueType::Float));
        let output = Aggregator::Avg
            .output_type(&typed(ValueType::UnsignedInt).optional())
            .unwrap();
        assert_eq!(output, typed(ValueType::Float).optional());
    }

    /// `min`/`max` consume only the comparable part of a mixed
    /// input: the output narrows to the meet with COMPARABLE, and
    /// the entry's own `output_type` reports the same result with
    /// field context on failure.
    #[dialog_common::test]
    fn it_narrows_min_max_output_to_the_comparable_meet() {
        let mixed = Type::from(
            Primitive::singleton(ValueType::Float).union(Primitive::singleton(ValueType::Boolean)),
        );
        let output = Aggregator::Min.output_type(&mixed).unwrap();
        assert_eq!(output, typed(ValueType::Float));

        let entry = ReduceEntry::try_new("least", Aggregator::Min, Term::var("x"), &mixed).unwrap();
        assert_eq!(
            entry.output_type(&mixed).unwrap(),
            typed(ValueType::Float),
            "the entry reports the aggregator's output type"
        );
        match entry.output_type(&typed(ValueType::Boolean)) {
            Err(TypeError::ReduceInput { field, .. }) => assert_eq!(field, "least"),
            other => panic!("expected ReduceInput with field context, got {other:?}"),
        }
    }

    /// A well-typed entry constructs through the checked path and
    /// still folds correctly through the A1 engine; the folded
    /// values inhabit the statically computed output types.
    #[dialog_common::test]
    async fn it_folds_checked_entries_and_matches_output_types() {
        let salary = typed(ValueType::UnsignedInt);
        let name = typed(ValueType::String).optional();
        let total = ReduceEntry::try_new("total", Aggregator::Sum, Term::var("salary"), &salary)
            .expect("sum over UnsignedInt is well-typed");
        let first = ReduceEntry::try_new("first", Aggregator::Min, Term::var("name"), &name)
            .expect("min over optional String is well-typed");
        let total_type = total.output_type(&salary).unwrap();
        let first_type = first.output_type(&name).unwrap();
        assert_eq!(total_type, int_band());
        assert_eq!(first_type, typed(ValueType::String).optional());

        let reduce = Reduce::new(vec!["dept".to_string()], vec![total, first]);
        let rows = reduce
            .fold(selection(vec![
                row(&[
                    ("dept", Some(dept("eng"))),
                    ("salary", Some(Value::UnsignedInt(10))),
                    ("name", Some(Value::String("ada".to_string()))),
                ]),
                row(&[
                    ("dept", Some(dept("eng"))),
                    ("salary", Some(Value::UnsignedInt(20))),
                    ("name", None),
                ]),
            ]))
            .await
            .unwrap();

        assert_eq!(rows.len(), 1);
        let folded_total = present(&rows[0], "total");
        assert_eq!(folded_total, Value::UnsignedInt(30));
        assert!(total_type.admits(&folded_total));
        let folded_first = present(&rows[0], "first");
        assert_eq!(folded_first, Value::String("ada".to_string()));
        assert!(first_type.admits(&folded_first));
    }
}
