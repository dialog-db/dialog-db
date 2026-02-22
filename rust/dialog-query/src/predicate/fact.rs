pub use crate::application::fact::FactApplication;
pub use crate::artifact::{Attribute, Cause};
pub use crate::error::SchemaError;
use crate::{Cardinality, Entity, Type, Value};
pub use crate::{Parameters, Term};

/// Describes the parameter signature of a relation predicate.
///
/// A relation maps `(Attribute, Entity) -> Value` where the value type may be
/// constrained. This descriptor captures the type-level information about a
/// relation — what kind of value it produces and its cardinality — without
/// binding any specific attribute, entity, or value.
///
/// This is the relation equivalent of [`super::concept::Concept`] (the concept
/// descriptor) which describes a concept's fields and their types.
#[derive(Debug, Clone, PartialEq)]
pub struct RelationDescriptor {
    /// The expected value type, or `None` if any type is accepted.
    pub content_type: Option<Type>,
    /// Whether this relation allows one or many values per entity.
    pub cardinality: Cardinality,
}

impl RelationDescriptor {
    /// A descriptor accepting any value type with cardinality one.
    pub const ANY: Self = Self {
        content_type: None,
        cardinality: Cardinality::One,
    };

    /// Creates a descriptor with the given value type and cardinality one.
    pub fn typed(content_type: Type) -> Self {
        Self {
            content_type: Some(content_type),
            cardinality: Cardinality::One,
        }
    }

    /// Creates a descriptor with the given value type and cardinality.
    pub fn new(content_type: Option<Type>, cardinality: Cardinality) -> Self {
        Self {
            content_type,
            cardinality,
        }
    }
}

/// Validated fact selector with typed terms for each component
struct Selector {
    /// Attribute term
    pub the: Term<Attribute>,
    /// Entity term
    pub of: Term<Entity>,
    /// Value term
    pub is: Term<Value>,
    /// Cause term
    pub cause: Term<Cause>,
}

/// Builder for constructing fact queries with fluent API
pub struct Fact {
    /// Attribute constraint
    pub the: Term<Attribute>,
    /// Entity constraint
    pub of: Term<Entity>,
    /// Value constraint
    pub is: Term<Value>,
    /// Cause constraint
    pub cause: Term<Cause>,
}

impl Default for Fact {
    fn default() -> Self {
        Self::new()
    }
}

impl Fact {
    /// Create a new empty Fact selector with all blank terms
    pub fn new() -> Self {
        Fact {
            the: Term::blank(),
            of: Term::blank(),
            is: Term::blank(),
            cause: Term::blank(),
        }
    }

    /// Set the attribute (the) constraint
    pub fn the<T: crate::term::IntoAttributeTerm>(mut self, the: T) -> Self {
        self.the = the.into_attribute_term();
        self
    }

    /// Set the entity (of) constraint
    pub fn of<Of: Into<Term<Entity>>>(mut self, entity: Of) -> Self {
        self.of = entity.into();
        self
    }

    /// Set the value (is) constraint
    pub fn is<V: Into<Term<Value>>>(mut self, value: V) -> Self {
        self.is = value.into();
        self
    }

    /// Convert the builder into a FactApplication
    pub fn compile(self) -> Result<FactApplication, SchemaError> {
        let mut params = Parameters::new();
        // Convert typed terms to Term<Value> for Parameters
        let the_value = match self.the {
            Term::Variable { name, .. } => Term::Variable {
                name,
                content_type: Default::default(),
            },
            Term::Constant(attr) => Term::Constant(Value::Symbol(attr)),
        };
        let of_value = match self.of {
            Term::Variable { name, .. } => Term::Variable {
                name,
                content_type: Default::default(),
            },
            Term::Constant(entity) => Term::Constant(Value::Entity(entity)),
        };

        let cause_value = match self.cause {
            Term::Variable { name, .. } => Term::Variable {
                name,
                content_type: Default::default(),
            },
            Term::Constant(cause) => Term::Constant(Value::Bytes(cause.0.to_vec())),
        };

        params.insert("the".to_string(), the_value);
        params.insert("of".to_string(), of_value);
        params.insert("is".to_string(), self.is);
        params.insert("cause".to_string(), cause_value);

        Self::apply(params)
    }

    /// Create a new fact selector (alias for `new`)
    pub fn select() -> Self {
        Self::new()
    }

    /// Validate and convert raw parameters into a typed Selector
    fn conform(terms: Parameters) -> Result<Selector, SchemaError> {
        let the = match terms.get("the") {
            None => Err(SchemaError::OmittedRequirement {
                binding: "the".into(),
            }),
            Some(Term::Variable { name: None, .. }) => Err(SchemaError::BlankRequirement {
                binding: "the".into(),
            }),
            Some(term) => Ok(term),
        };

        let of = match terms.get("of") {
            None => Err(SchemaError::OmittedRequirement {
                binding: "of".into(),
            }),
            Some(Term::Variable { name: None, .. }) => Err(SchemaError::BlankRequirement {
                binding: "of".into(),
            }),
            Some(term) => Ok(term),
        };

        let is = match terms.get("is") {
            None => Err(SchemaError::OmittedRequirement {
                binding: "is".into(),
            }),
            Some(Term::Variable { name: None, .. }) => Err(SchemaError::BlankRequirement {
                binding: "is".into(),
            }),
            Some(term) => Ok(term),
        };

        let cause = match terms.get("cause") {
            None => Err(SchemaError::OmittedRequirement {
                binding: "cause".into(),
            }),
            Some(Term::Variable { name: None, .. }) => Err(SchemaError::BlankRequirement {
                binding: "cause".into(),
            }),
            Some(term) => Ok(term),
        };

        if matches!((&the, &of, &is), (Err(_), Err(_), Err(_))) {
            Err(SchemaError::UnconstrainedSelector)
        } else {
            // Convert Term<Value> to typed terms
            let the_term = the
                .as_ref()
                .map(|t| match t {
                    Term::Variable { name, .. } => Term::<Attribute>::Variable {
                        name: name.clone(),
                        content_type: Default::default(),
                    },
                    Term::Constant(v) => match v {
                        Value::Symbol(s) => Term::Constant(s.clone()),
                        _ => Term::blank(),
                    },
                })
                .unwrap_or_else(|_| Term::blank());

            let of_term = of
                .as_ref()
                .map(|t| match t {
                    Term::Variable { name, .. } => Term::<Entity>::Variable {
                        name: name.clone(),
                        content_type: Default::default(),
                    },
                    Term::Constant(v) => match v {
                        Value::Entity(e) => Term::Constant(e.clone()),
                        _ => Term::blank(),
                    },
                })
                .unwrap_or_else(|_| Term::blank());

            let is_term = is
                .as_ref()
                .map(|t| (*t).clone())
                .unwrap_or_else(|_| Term::blank());

            let cause_term = cause
                .as_ref()
                .map(|t| match t {
                    Term::Variable { name, .. } => Term::<Cause>::Variable {
                        name: name.clone(),
                        content_type: Default::default(),
                    },
                    Term::Constant(v) => match v {
                        Value::Bytes(b) => {
                            // Convert Vec<u8> to [u8; 32] for Blake3Hash
                            let mut hash_bytes = [0u8; 32];
                            let len = b.len().min(32);
                            hash_bytes[..len].copy_from_slice(&b[..len]);
                            Term::Constant(Cause(hash_bytes))
                        }
                        _ => Term::blank(),
                    },
                })
                .unwrap_or_else(|_| Term::blank());

            Ok(Selector {
                the: the_term,
                of: of_term,
                is: is_term,
                cause: cause_term,
            })
        }
    }
    /// Validate parameters and create a FactApplication
    pub fn apply(terms: Parameters) -> Result<FactApplication, SchemaError> {
        let Selector { the, of, is, cause } = Self::conform(terms)?;

        Ok(FactApplication::new(the, of, is, cause, Cardinality::One))
    }
}

impl From<Fact> for FactApplication {
    fn from(fact: Fact) -> Self {
        FactApplication::new(fact.the, fact.of, fact.is, fact.cause, Cardinality::One)
    }
}

impl From<Fact> for crate::Premise {
    fn from(fact: Fact) -> Self {
        crate::Premise::Apply(crate::Application::Fact(fact.into()))
    }
}
