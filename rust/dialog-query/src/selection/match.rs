use futures_util::stream::once;
use std::collections::HashMap;
use std::sync::Arc;

use crate::Claim;
use crate::artifact::Value;
use crate::error::EvaluationError;
use crate::term::Term;
use crate::types::Any;
use crate::types::Record;

use super::Selection;

/// A row-level binding for a variable.
///
/// Distinguishes [`Binding::Present`] (the variable resolved to a
/// concrete [`Value`]) from [`Binding::Absent`] (an optional
/// resolution premise looked up the entity's attribute and found
/// no fact). `Absent` is structurally distinct from "no binding at
/// all" — variables that no premise has touched aren't in the
/// bindings map and produce
/// [`EvaluationError::UnboundVariable`] from
/// [`Match::lookup`]; variables that have been touched by an
/// optional resolver are *always* in the map, with either a
/// `Present` or an `Absent` entry.
///
/// This three-state distinction (unbound / Present / Absent) is
/// what makes set-widening optionality work without persisting any
/// `None` value at the storage layer. See `notes/optional-fields.md`
/// for the design rationale.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Binding {
    /// The variable resolved to a concrete value.
    Present(Value),
    /// An optional resolver looked up the variable's attribute for
    /// this entity and found no fact. Distinct from "not yet
    /// bound."
    Absent,
}

impl Binding {
    /// Extract the contained [`Value`], returning
    /// [`EvaluationError::Absent`] if this binding is `Absent`.
    /// Use this when the caller cannot tolerate absence — e.g.
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
/// resolved to a concrete value) or `Absent` (an optional resolver
/// found no fact for the entity).
///
/// Matches flow through the evaluation pipeline as a stream
/// ([`Selection`](super::Selection)): each premise receives the
/// stream, potentially expands each match into zero or more new
/// matches, and passes them to the next premise.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Match {
    /// Named variable bindings: maps variable names to their
    /// row-level binding (Present or Absent). A name absent from
    /// this map means "no premise has touched this variable" —
    /// distinct from [`Binding::Absent`].
    bindings: HashMap<String, Binding>,
    // TODO: Once Value::Record supports the RecordFormat trait proposed in
    // https://github.com/dialog-db/dialog-db/pull/221 claims can be stored
    // directly as Value::Record in bindings, eliminating this separate map.
    claims: HashMap<String, Arc<Claim>>,
}

impl Match {
    /// Create new empty match.
    pub fn new() -> Self {
        Self::default()
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

        if let Some(claim) = self.claims.get(key) {
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
        if let Term::Variable {
            name: Some(name), ..
        } = term
        {
            self.claims.insert(name.clone(), Arc::new(claim.to_owned()));
        }

        Ok(())
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
            } => {
                if let Some(existing) = self.bindings.get(name) {
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
                    self.bindings.insert(name.into(), Binding::Present(value));
                    Ok(())
                }
            }
            Term::Variable { name: None, .. } | Term::Constant(_) => Ok(()),
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
            } => {
                if let Some(existing) = self.bindings.get(name) {
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
                    self.bindings.insert(name.into(), Binding::Absent);
                    Ok(())
                }
            }
            Term::Variable { name: None, .. } | Term::Constant(_) => Ok(()),
        }
    }

    /// Returns `true` iff the term is bound (Present *or* Absent)
    /// in this match. Use [`Self::is_present`] to check for
    /// `Present`-only.
    pub fn contains(&self, term: &Term<Any>) -> bool {
        match term {
            Term::Variable {
                name: Some(key), ..
            } => self.bindings.contains_key(key),
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
            } => self
                .bindings
                .get(key)
                .map(|b| b.is_present())
                .unwrap_or(false),
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
                if let Some(binding) = self.bindings.get(key) {
                    Ok(binding.clone())
                } else {
                    Err(EvaluationError::UnboundVariable {
                        variable_name: key.clone(),
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

    #[test]
    fn binding_content_returns_value_for_present() {
        let b = Binding::Present(Value::String("hello".into()));
        assert_eq!(b.content(), Ok(Value::String("hello".into())));
    }

    #[test]
    fn binding_content_errors_on_absent() {
        let b = Binding::Absent;
        match b.content() {
            Err(EvaluationError::Absent { variable_name }) => {
                assert_eq!(variable_name, "_");
            }
            other => panic!("expected Absent error, got {:?}", other),
        }
    }

    #[test]
    fn binding_content_for_attaches_variable_name() {
        let b = Binding::Absent;
        match b.content_for(Some("nickname")) {
            Err(EvaluationError::Absent { variable_name }) => {
                assert_eq!(variable_name, "nickname");
            }
            other => panic!("expected Absent error, got {:?}", other),
        }
    }

    #[test]
    fn binding_predicates() {
        assert!(Binding::Present(Value::UnsignedInt(0)).is_present());
        assert!(!Binding::Present(Value::UnsignedInt(0)).is_absent());
        assert!(Binding::Absent.is_absent());
        assert!(!Binding::Absent.is_present());
    }

    #[test]
    fn match_bind_absent_creates_absent_binding() {
        let mut m = Match::new();
        let term = Term::var("nickname");
        m.bind_absent(&term).unwrap();
        assert_eq!(m.lookup(&term).unwrap(), Binding::Absent);
    }

    #[test]
    fn match_bind_then_bind_absent_conflicts() {
        let mut m = Match::new();
        let term = Term::var("name");
        m.bind(&term, Value::String("Alice".into())).unwrap();
        let result = m.bind_absent(&term);
        assert!(matches!(result, Err(EvaluationError::Assignment { .. })));
    }

    #[test]
    fn match_bind_absent_then_bind_conflicts() {
        let mut m = Match::new();
        let term = Term::var("name");
        m.bind_absent(&term).unwrap();
        let result = m.bind(&term, Value::String("Alice".into()));
        assert!(matches!(result, Err(EvaluationError::Assignment { .. })));
    }

    #[test]
    fn match_bind_absent_is_idempotent() {
        let mut m = Match::new();
        let term = Term::var("nickname");
        m.bind_absent(&term).unwrap();
        m.bind_absent(&term).unwrap();
        assert_eq!(m.lookup(&term).unwrap(), Binding::Absent);
    }

    #[test]
    fn match_lookup_unbound_returns_unbound_error() {
        let m = Match::new();
        match m.lookup(&Term::var("nope")) {
            Err(EvaluationError::UnboundVariable { variable_name }) => {
                assert_eq!(variable_name, "nope");
            }
            other => panic!("expected UnboundVariable, got {:?}", other),
        }
    }

    #[test]
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
