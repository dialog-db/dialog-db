pub use crate::application::fact::FactApplication;
pub use crate::artifact::{Attribute, Cause};
pub use crate::error::SchemaError;
use crate::{Cardinality, Entity, Value};
pub use crate::{Parameters, Term};

pub struct Selector {
    pub the: Term<Attribute>,
    pub of: Term<Entity>,
    pub is: Term<Value>,
    pub cause: Term<Cause>,
}

pub struct Fact {
    pub the: Term<Attribute>,
    pub of: Term<Entity>,
    pub is: Term<Value>,
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

    /// Set the cause constraint
    pub fn cause<C: Into<Term<Cause>>>(mut self, cause: C) -> Self {
        self.cause = cause.into();
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

    pub fn select() -> Self {
        Self::new()
    }

    pub fn conform(terms: Parameters) -> Result<Selector, SchemaError> {
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
