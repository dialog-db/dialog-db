use futures_util::stream::once;
use std::sync::Arc;

use crate::Claim;
use crate::artifact::Value;
use crate::error::EvaluationError;
use crate::term::Term;
use crate::type_system::Type as Kind;
use crate::types::Any;
use crate::types::Record;

use super::Selection;

/// A row-level binding for a variable.
///
/// Distinguishes [`Binding::Present`] (the variable resolved to a
/// concrete [`Value`]) from [`Binding::Absent`] (an optional
/// lookup examined the entity's attribute and found no fact).
/// `Absent` is structurally distinct from "no binding at all":
/// variables that no premise has touched aren't in the bindings map
/// and produce [`EvaluationError::UnboundVariable`] from
/// [`Match::lookup`]; variables that have been touched by an
/// optional lookup are *always* in the map, with either a `Present`
/// or an `Absent` entry.
///
/// This three-state distinction (unbound / Present / Absent) is
/// what makes set-widening optionality work without persisting any
/// `None` value at the storage layer.
///
/// # Why a dedicated type rather than `Option<Value>`
///
/// `Option` already has a job in this API: `Option<T>` on a concept
/// field declares *type-level* optionality ("this field may be
/// absent", the `Nothing` atom in the field's kind). `Binding::Absent`
/// is the *value-level* outcome ("this field is absent, for this
/// entity"), the value inhabiting that `Nothing`. If bindings were
/// `Option<Value>` too, every use of `Option` would need contextual
/// qualification to say which of the two it means; the separate type
/// keeps the declaration and the outcome from blurring.
///
/// `Binding` is also a propagator cell, not a plain container:
/// [`Match::bind`] merges (equal `Present` values are idempotent,
/// `Present` vs `Absent` is a conflict, and
/// [`Match::bind_absent`] over `Present` errors). Absence is a
/// *claim* about the store, and the merge rules are what hold that
/// claim consistent across premises; a contract `Option` does not
/// carry.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Binding {
    /// The variable resolved to a concrete value.
    Present(Value),
    /// An optional lookup examined the variable's attribute for
    /// this entity and found no fact. Distinct from "not yet
    /// bound."
    Absent,
}

impl Binding {
    /// Extract the contained [`Value`], returning
    /// [`EvaluationError::Absent`] if this binding is `Absent`.
    /// Use this when the caller cannot tolerate absence, e.g.
    /// realization paths for required fields, formula inputs.
    /// Callers that handle optionality (Coalesce, optional
    /// realize) should pattern-match on `Binding` directly.
    pub fn content(self) -> Result<Value, EvaluationError> {
        self.content_for(None)
    }

    /// Like [`Self::content`] but attaches a variable name to the
    /// resulting [`EvaluationError::Absent`] so callers can report
    /// which slot was Absent.
    pub fn content_for(self, variable_name: Option<&str>) -> Result<Value, EvaluationError> {
        match self {
            Binding::Present(value) => Ok(value),
            Binding::Absent => Err(EvaluationError::Absent {
                variable_name: variable_name.unwrap_or("_").into(),
            }),
        }
    }

    /// Returns the contained [`Value`] reference if `Present`,
    /// `None` otherwise.
    pub fn as_value(&self) -> Option<&Value> {
        match self {
            Binding::Present(value) => Some(value),
            Binding::Absent => None,
        }
    }

    /// Returns `true` iff this binding is [`Binding::Absent`].
    pub fn is_absent(&self) -> bool {
        matches!(self, Binding::Absent)
    }

    /// Returns `true` iff this binding is [`Binding::Present`].
    pub fn is_present(&self) -> bool {
        matches!(self, Binding::Present(_))
    }
}

/// A single result row produced during query evaluation.
///
/// A `Match` accumulates variable bindings as premises are
/// evaluated in sequence. Each binding maps a variable name to a
/// [`Binding`], which is either `Present(value)` (the variable
/// resolved to a concrete value) or `Absent` (an optional lookup
/// found no fact for the entity).
///
/// Matches flow through the evaluation pipeline as a stream
/// ([`Selection`](super::Selection)): each premise receives the
/// stream, potentially expands each match into zero or more new
/// matches, and passes them to the next premise.
///
/// A row is its own bindings plus a shared, immutable [`Frame`] of the
/// bindings of the row it extends. A premise that extends one row into
/// many ([`Self::share`] it first) gives every extension the same frame
/// rather than a copy of every binding and claim: a row's ancestry is
/// shared, not copied, however many rows descend from it.
#[derive(Clone, Debug, Default)]
pub struct Match {
    /// Named variable bindings made on this row since its frame was
    /// taken: maps variable names to their row-level binding (Present
    /// or Absent). A name absent from these and from the frame means
    /// "no premise has touched this variable": distinct from
    /// [`Binding::Absent`].
    ///
    /// Held as a small vec probed linearly, not a hash map: a query
    /// binds a handful of variables, and every probe of a map paid a
    /// SipHash of the name before comparing anything.
    bindings: Vec<(Arc<str>, Binding)>,
    // TODO: Once Value::Record supports the RecordFormat trait proposed in
    // https://github.com/dialog-db/dialog-db/pull/221 claims can be stored
    // directly as Value::Record in bindings, eliminating this separate list.
    claims: Vec<(Arc<str>, Arc<Claim>)>,
    /// The bindings and claims this row extends, shared with every other
    /// row extending the same ones.
    frame: Option<Arc<Frame>>,
    /// The row this one is evaluated on behalf of, when it belongs to a
    /// nested scope (a concept's rule body). The scope's own names do not
    /// see the caller's: this is never consulted by lookups. It is how a
    /// concept evaluates every incoming row through one pipeline and still
    /// merges each result back into the row it came from. The caller is
    /// shared, not copied, by every row derived in the scope.
    caller: Option<Arc<Match>>,
}

/// The bindings and claims a row extends: frozen by [`Match::share`],
/// and shared by every row derived from it after.
#[derive(Debug, Default)]
struct Frame {
    bindings: Vec<(Arc<str>, Binding)>,
    claims: Vec<(Arc<str>, Arc<Claim>)>,
    parent: Option<Arc<Frame>>,
}

/// Binding order is premise-evaluation order, an artifact of the plan;
/// two rows are the same result when they bind the same names to the
/// same values, in any order, however their frames are split.
impl PartialEq for Match {
    fn eq(&self, other: &Self) -> bool {
        fn same<T: PartialEq>(left: Vec<&(Arc<str>, T)>, right: Vec<&(Arc<str>, T)>) -> bool {
            left.len() == right.len()
                && left.iter().all(|(name, value)| {
                    right
                        .iter()
                        .any(|(other_name, other_value)| name == other_name && value == other_value)
                })
        }
        same(
            self.all_bindings().collect(),
            other.all_bindings().collect(),
        ) && same(self.all_claims(), other.all_claims())
    }
}

impl Eq for Match {}

/// Probes a name list, the small-vec analogue of `HashMap::get`.
fn probe<'a, T>(entries: &'a [(Arc<str>, T)], key: &str) -> Option<&'a T> {
    entries
        .iter()
        .find(|(name, _)| name.as_ref() == key)
        .map(|(_, value)| value)
}

impl Match {
    /// Create new empty match.
    pub fn new() -> Self {
        Self::default()
    }

    /// Make room for `additional` more bindings on this row, so a premise
    /// that binds several slots at once grows the row's own bindings once.
    pub(crate) fn reserve(&mut self, additional: usize) {
        self.bindings.reserve(additional);
    }

    /// Freeze this row's own bindings and claims into its shared frame,
    /// so clones of it (every row a premise extends it into) share them
    /// instead of each copying them.
    pub fn share(&mut self) {
        if self.bindings.is_empty() && self.claims.is_empty() {
            return;
        }
        let frame = Frame {
            bindings: std::mem::take(&mut self.bindings),
            claims: std::mem::take(&mut self.claims),
            parent: self.frame.take(),
        };
        self.frame = Some(Arc::new(frame));
    }

    /// The frames this row extends, innermost first.
    fn frames(&self) -> impl Iterator<Item = &Frame> {
        std::iter::successors(self.frame.as_deref(), |frame| frame.parent.as_deref())
    }

    /// Every binding of this row, its own and its frames'. A name is
    /// bound at most once along a row's ancestry, so nothing repeats.
    fn all_bindings(&self) -> impl Iterator<Item = &(Arc<str>, Binding)> {
        self.bindings
            .iter()
            .chain(self.frames().flat_map(|frame| frame.bindings.iter()))
    }

    /// Every claim this row cites, innermost first. A name cited again
    /// replaces the earlier citation, so only a name's first occurrence
    /// counts.
    fn all_claims(&self) -> Vec<&(Arc<str>, Arc<Claim>)> {
        let mut claims: Vec<&(Arc<str>, Arc<Claim>)> = Vec::new();
        for entry in self
            .claims
            .iter()
            .chain(self.frames().flat_map(|frame| frame.claims.iter()))
        {
            if !claims.iter().any(|(name, _)| *name == entry.0) {
                claims.push(entry);
            }
        }
        claims
    }

    /// The binding for `name` along this row's ancestry.
    fn find(&self, name: &str) -> Option<&Binding> {
        probe(&self.bindings, name)
            .or_else(|| self.frames().find_map(|frame| probe(&frame.bindings, name)))
    }

    /// The claim cited for `name` along this row's ancestry.
    fn find_claim(&self, name: &str) -> Option<&Arc<Claim>> {
        probe(&self.claims, name)
            .or_else(|| self.frames().find_map(|frame| probe(&frame.claims, name)))
    }

    /// Place this row in a scope nested in `caller`'s: the row is
    /// evaluated on the caller's behalf, and [`Self::take_caller`] hands
    /// the caller back when a result comes out of the scope.
    pub(crate) fn within(mut self, caller: Arc<Match>) -> Self {
        self.caller = Some(caller);
        self
    }

    /// Take the row this one was evaluated on behalf of (see
    /// [`Self::within`]).
    pub(crate) fn take_caller(&mut self) -> Option<Arc<Match>> {
        self.caller.take()
    }

    /// Wrap this match into a single-element `Selection` stream.
    pub fn seed(self) -> impl Selection {
        once(async { Ok(self) })
    }

    /// Provide evidence for the given term: look up the claim it cites.
    pub fn prove(&self, term: &Term<Record>) -> Result<Claim, EvaluationError> {
        let key = match term {
            Term::Variable {
                name: Some(name), ..
            } => name,
            _ => {
                return Err(EvaluationError::Store(
                    "Cannot look up claim with a non-variable term".to_string(),
                ));
            }
        };

        if let Some(claim) = self.find_claim(key) {
            Ok(claim.as_ref().clone())
        } else {
            Err(EvaluationError::Store(format!(
                "No claim found for term {:?}",
                key
            )))
        }
    }

    /// Cite a claim as evidence for the given term.
    pub fn cite(&mut self, term: &Term<Record>, claim: &Claim) -> Result<(), EvaluationError> {
        self.cite_owned(term, claim.to_owned());
        Ok(())
    }

    /// Cite a claim the caller owns, moving it in rather than copying
    /// it: a scan builds the claim for the row it yields and has no
    /// further use for it.
    pub(crate) fn cite_owned(&mut self, term: &Term<Record>, claim: Claim) {
        if let Term::Variable {
            name: Some(name), ..
        } = term
        {
            let claim = Arc::new(claim);
            match self.claims.iter_mut().find(|(held, _)| **held == **name) {
                Some((_, slot)) => *slot = claim,
                None => self.claims.push((name.clone(), claim)),
            }
        }
    }

    /// Merge every binding and claim from `other` into this match,
    /// returning `None` if the two disagree on any shared variable.
    ///
    /// This is the row-combining step of a set-at-a-time join: two rows
    /// produced independently (rather than one fed into the other) are
    /// joined by unifying their bindings. A shared variable bound to the
    /// same `Present` value in both, or `Absent` in both, unifies; a
    /// `Present`/`Absent` clash or two different `Present` values is a
    /// non-match, which yields `None` rather than an error, since a
    /// failed unification is ordinary data-dependent filtering, not a
    /// contract violation.
    ///
    /// Bindings only in `other` are added; bindings only in `self` are
    /// kept. Claims from `other` fill in only where `self` has none, so a
    /// row's own provenance is never overwritten by the row it joins with.
    pub fn combine(mut self, other: &Match) -> Option<Match> {
        for (name, binding) in other.all_bindings() {
            match self.find(name) {
                None => self.bindings.push((name.clone(), binding.clone())),
                Some(existing) if existing == binding => {}
                Some(_) => return None,
            }
        }
        for (name, claim) in other.all_claims() {
            if self.find_claim(name).is_none() {
                self.claims.push((name.clone(), claim.clone()));
            }
        }
        if self.caller.is_none() {
            self.caller = other.caller.clone();
        }
        Some(self)
    }

    /// The binding for the variable `name`, if a premise has touched
    /// it. The borrowing counterpart of [`Self::lookup`], for callers
    /// that hold a variable's name rather than a [`Term`] and need not
    /// copy the value out.
    pub fn get(&self, name: &str) -> Option<&Binding> {
        self.find(name)
    }

    /// The `Present` value bound to `name`, if any. Used by the merge
    /// join to read the join key out of a row without going through a
    /// [`Term`].
    pub fn value_of(&self, name: &str) -> Option<&Value> {
        match self.find(name) {
            Some(Binding::Present(value)) => Some(value),
            _ => None,
        }
    }

    /// The environment of variables this match binds to a `Present`
    /// value.
    ///
    /// Used to estimate a scan's cost against the bindings a row
    /// actually carries: a scan whose value variable is present here
    /// scans a narrow band rather than a full range, which is what the
    /// merge-versus-nested-loop choice turns on.
    pub fn environment(&self) -> crate::Environment {
        let mut env = crate::Environment::new();
        for (name, binding) in self.all_bindings() {
            if matches!(binding, Binding::Present(_)) {
                env.add(name.as_ref());
            }
        }
        env
    }

    /// Bind a term to a [`Binding::Present`] value. For named
    /// variables, stores the value in the bindings map; checks
    /// consistency if already bound:
    ///
    /// - existing `Present` with the same value is OK (idempotent).
    /// - existing `Present` with a different value conflicts.
    /// - existing `Absent` conflicts with an incoming `Present`.
    ///
    /// Constants and blanks are no-ops.
    pub fn bind(&mut self, term: &Term<Any>, value: Value) -> Result<(), EvaluationError> {
        match term {
            Term::Variable {
                name: Some(name), ..
            } => self.bind_variable(name, term.kind(), value),
            Term::Variable { name: None, .. } | Term::Constant(_) => Ok(()),
        }
    }

    /// [`Self::bind`] for a variable given by name and kind, so a
    /// caller holding a typed term need not widen it to a
    /// `Term<Any>` (a copy of its name) for every value it binds.
    pub(crate) fn bind_variable(
        &mut self,
        name: &str,
        kind: Option<Kind>,
        value: Value,
    ) -> Result<(), EvaluationError> {
        // Contract check: a typed variable only accepts values
        // inhabiting its kind. Scans filter mismatched facts
        // before reaching here, so a failure at this point is
        // a contract violation (e.g. an untyped construction
        // path feeding a value the rule's types exclude), not
        // a data-dependent non-match.
        if let Some(kind) = kind
            && !kind.admits(&value)
        {
            return Err(EvaluationError::KindMismatch {
                variable: name.to_string(),
                kind: kind.to_string(),
                value: format!("{value:?}"),
                value_type: format!("{:?}", value.data_type()),
            });
        }
        if let Some(existing) = self.find(name) {
            match existing {
                Binding::Present(existing_value) => {
                    if *existing_value != value {
                        Err(EvaluationError::Assignment {
                            reason: format!(
                                "Can not set {:?} to {:?} because it is already set to {:?}.",
                                name, value, existing_value
                            ),
                        })
                    } else {
                        Ok(())
                    }
                }
                Binding::Absent => Err(EvaluationError::Assignment {
                    reason: format!(
                        "Can not set {:?} to {:?} because it is already bound to Absent.",
                        name, value
                    ),
                }),
            }
        } else {
            self.bindings.push((name.into(), Binding::Present(value)));
            Ok(())
        }
    }

    /// Bind a term to [`Binding::Absent`]. Used by optional
    /// resolution premises that looked up an attribute and found
    /// no fact. Errors if the variable is already bound to a
    /// `Present` value. Constants and blanks are no-ops.
    pub fn bind_absent(&mut self, term: &Term<Any>) -> Result<(), EvaluationError> {
        match term {
            Term::Variable {
                name: Some(name), ..
            } => self.bind_absent_variable(name),
            Term::Variable { name: None, .. } | Term::Constant(_) => Ok(()),
        }
    }

    /// [`Self::bind_absent`] for a variable given by name.
    pub(crate) fn bind_absent_variable(&mut self, name: &str) -> Result<(), EvaluationError> {
        if let Some(existing) = self.find(name) {
            match existing {
                Binding::Absent => Ok(()),
                Binding::Present(value) => Err(EvaluationError::Assignment {
                    reason: format!(
                        "Can not set {:?} to Absent because it is already set to {:?}.",
                        name, value
                    ),
                }),
            }
        } else {
            self.bindings.push((name.into(), Binding::Absent));
            Ok(())
        }
    }

    /// Returns `true` iff the term is bound (Present *or* Absent)
    /// in this match. Use [`Self::is_present`] to check for
    /// `Present`-only.
    pub fn contains(&self, term: &Term<Any>) -> bool {
        match term {
            Term::Variable {
                name: Some(key), ..
            } => self.find(key).is_some(),
            Term::Variable { name: None, .. } => false,
            Term::Constant(_) => true,
        }
    }

    /// Returns `true` iff the term is bound to a `Present` value
    /// (excluding `Absent`). Constants always count as Present.
    pub fn is_present(&self, term: &Term<Any>) -> bool {
        match term {
            Term::Variable {
                name: Some(key), ..
            } => self.find(key).map(|b| b.is_present()).unwrap_or(false),
            Term::Variable { name: None, .. } => false,
            Term::Constant(_) => true,
        }
    }

    /// Look up the binding for a term.
    ///
    /// For named variables, returns the binding (Present or
    /// Absent). For constants, returns `Present(value)`. Returns
    /// [`EvaluationError::UnboundVariable`] for blank variables
    /// or for named variables that no premise has touched.
    ///
    /// Callers that want a `Value` should chain
    /// `.lookup(&term)?.content()` to convert `Absent` into an
    /// error. Callers that handle optionality (Coalesce, optional
    /// realize) pattern-match on `Binding` directly.
    pub fn lookup(&self, term: &Term<Any>) -> Result<Binding, EvaluationError> {
        match term {
            Term::Variable {
                name: Some(key), ..
            } => {
                if let Some(binding) = self.find(key) {
                    Ok(binding.clone())
                } else {
                    Err(EvaluationError::UnboundVariable {
                        variable_name: key.to_string(),
                    })
                }
            }
            Term::Variable { name: None, .. } => Err(EvaluationError::UnboundVariable {
                variable_name: "_".into(),
            }),
            Term::Constant(value) => Ok(Binding::Present(value.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::Value;

    /// A typed variable only accepts values inhabiting its kind:
    /// the contract check behind every merge and propagation.
    #[dialog_common::test]
    fn bind_rejects_value_outside_the_terms_kind() {
        let mut row = Match::new();
        let typed: Term<Any> = Term::<String>::var("name").into();

        let err = row.bind(&typed, Value::UnsignedInt(7));
        assert!(
            matches!(err, Err(EvaluationError::KindMismatch { .. })),
            "a u32 value cannot inhabit a String-typed variable, got {err:?}"
        );

        row.bind(&typed, Value::String("Alice".into()))
            .expect("a String value inhabits the kind");
    }

    #[dialog_common::test]
    fn binding_content_returns_value_for_present() {
        let b = Binding::Present(Value::String("hello".into()));
        assert_eq!(b.content(), Ok(Value::String("hello".into())));
    }

    #[dialog_common::test]
    fn binding_content_errors_on_absent() {
        let b = Binding::Absent;
        match b.content() {
            Err(EvaluationError::Absent { variable_name }) => {
                assert_eq!(variable_name, "_");
            }
            other => panic!("expected Absent error, got {:?}", other),
        }
    }

    #[dialog_common::test]
    fn binding_content_for_attaches_variable_name() {
        let b = Binding::Absent;
        match b.content_for(Some("nickname")) {
            Err(EvaluationError::Absent { variable_name }) => {
                assert_eq!(variable_name, "nickname");
            }
            other => panic!("expected Absent error, got {:?}", other),
        }
    }

    #[dialog_common::test]
    fn binding_predicates() {
        assert!(Binding::Present(Value::UnsignedInt(0)).is_present());
        assert!(!Binding::Present(Value::UnsignedInt(0)).is_absent());
        assert!(Binding::Absent.is_absent());
        assert!(!Binding::Absent.is_present());
    }

    #[dialog_common::test]
    fn match_bind_absent_creates_absent_binding() {
        let mut m = Match::new();
        let term = Term::var("nickname");
        m.bind_absent(&term).unwrap();
        assert_eq!(m.lookup(&term).unwrap(), Binding::Absent);
    }

    #[dialog_common::test]
    fn match_bind_then_bind_absent_conflicts() {
        let mut m = Match::new();
        let term = Term::var("name");
        m.bind(&term, Value::String("Alice".into())).unwrap();
        let result = m.bind_absent(&term);
        assert!(matches!(result, Err(EvaluationError::Assignment { .. })));
    }

    #[dialog_common::test]
    fn match_bind_absent_then_bind_conflicts() {
        let mut m = Match::new();
        let term = Term::var("name");
        m.bind_absent(&term).unwrap();
        let result = m.bind(&term, Value::String("Alice".into()));
        assert!(matches!(result, Err(EvaluationError::Assignment { .. })));
    }

    #[dialog_common::test]
    fn match_bind_absent_is_idempotent() {
        let mut m = Match::new();
        let term = Term::var("nickname");
        m.bind_absent(&term).unwrap();
        m.bind_absent(&term).unwrap();
        assert_eq!(m.lookup(&term).unwrap(), Binding::Absent);
    }

    #[dialog_common::test]
    fn match_lookup_unbound_returns_unbound_error() {
        let m = Match::new();
        match m.lookup(&Term::var("nope")) {
            Err(EvaluationError::UnboundVariable { variable_name }) => {
                assert_eq!(variable_name, "nope");
            }
            other => panic!("expected UnboundVariable, got {:?}", other),
        }
    }

    #[dialog_common::test]
    fn match_is_present_distinguishes_present_from_absent() {
        let mut m = Match::new();
        let pname = Term::var("name");
        let nname = Term::var("nickname");
        m.bind(&pname, Value::String("Alice".into())).unwrap();
        m.bind_absent(&nname).unwrap();
        assert!(m.is_present(&pname));
        assert!(!m.is_present(&nname));
        assert!(m.contains(&pname));
        assert!(m.contains(&nname));
    }
}

#[cfg(test)]
mod model_tests {
    #![allow(unexpected_cfgs)]

    use std::collections::HashMap;

    use super::*;
    use crate::artifact::Value;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    fn xorshift(state: &mut u64) -> u32 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        (*state >> 32) as u32
    }

    /// The small-vec representation against a `HashMap` reference model:
    /// random bind / bind_absent sequences over a small variable pool must
    /// agree on every outcome — success vs conflict, and every lookup —
    /// with what the map-backed semantics dictate.
    #[dialog_common::test]
    async fn it_matches_the_hash_map_model() {
        for seed in 0..16u64 {
            let mut rng = 0x2545F4914F6CDD1Du64 ^ seed;
            let mut row = Match::new();
            let mut model: HashMap<String, Binding> = HashMap::new();

            for _ in 0..200 {
                let name = format!("v{}", xorshift(&mut rng) % 12);
                let term: Term<Any> = Term::var(&name);
                match xorshift(&mut rng) % 3 {
                    0 => {
                        let value = Value::UnsignedInt(u128::from(xorshift(&mut rng) % 4));
                        let expect = match model.get(&name) {
                            None => {
                                model.insert(name.clone(), Binding::Present(value.clone()));
                                true
                            }
                            Some(Binding::Present(held)) => *held == value,
                            Some(Binding::Absent) => false,
                        };
                        assert_eq!(
                            row.bind(&term, value).is_ok(),
                            expect,
                            "seed {seed}: bind({name}) verdict diverged from the model"
                        );
                    }
                    1 => {
                        let expect = match model.get(&name) {
                            None => {
                                model.insert(name.clone(), Binding::Absent);
                                true
                            }
                            Some(Binding::Absent) => true,
                            Some(Binding::Present(_)) => false,
                        };
                        assert_eq!(
                            row.bind_absent(&term).is_ok(),
                            expect,
                            "seed {seed}: bind_absent({name}) verdict diverged"
                        );
                    }
                    _ => {
                        let looked = row.lookup(&term).ok();
                        assert_eq!(
                            looked.as_ref(),
                            model.get(&name),
                            "seed {seed}: lookup({name}) diverged"
                        );
                        assert_eq!(row.contains(&term), model.contains_key(&name));
                        assert_eq!(
                            row.is_present(&term),
                            model.get(&name).map(Binding::is_present).unwrap_or(false)
                        );
                    }
                }
            }
        }
    }

    /// Binding order is plan order, not identity: two rows binding the same
    /// names to the same values in different orders are equal, and any
    /// differing binding breaks equality both ways.
    #[dialog_common::test]
    async fn it_compares_rows_order_insensitively() {
        let a_term: Term<Any> = Term::var("a");
        let b_term: Term<Any> = Term::var("b");

        let mut forward = Match::new();
        forward.bind(&a_term, Value::UnsignedInt(1)).unwrap();
        forward.bind(&b_term, Value::UnsignedInt(2)).unwrap();

        let mut backward = Match::new();
        backward.bind(&b_term, Value::UnsignedInt(2)).unwrap();
        backward.bind(&a_term, Value::UnsignedInt(1)).unwrap();

        assert_eq!(forward, backward, "binding order must not affect identity");
        assert_eq!(backward, forward, "equality must be symmetric");

        let mut different = Match::new();
        different.bind(&a_term, Value::UnsignedInt(1)).unwrap();
        different.bind(&b_term, Value::UnsignedInt(3)).unwrap();
        assert_ne!(forward, different);
        assert_ne!(different, forward);

        let mut subset = Match::new();
        subset.bind(&a_term, Value::UnsignedInt(1)).unwrap();
        assert_ne!(forward, subset, "missing bindings must break equality");
        assert_ne!(subset, forward);
    }
}
