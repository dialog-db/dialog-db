pub use crate::application::fact::FactApplication;
pub use crate::artifact::Attribute;
pub use crate::error::SchemaError;
use crate::{Cardinality, Entity, Value};
pub use crate::{Parameters, Term};

pub struct Selector {
    pub the: Term<Attribute>,
    pub of: Term<Entity>,
    pub is: Term<Value>,
}

pub struct Fact;
impl Fact {
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

            Ok(Selector {
                the: the_term,
                of: of_term,
                is: is_term,
            })
        }
    }
    pub fn apply(terms: Parameters) -> Result<FactApplication, SchemaError> {
        let Selector { the, of, is } = Self::conform(terms)?;

        Ok(FactApplication::new(the, of, is, Cardinality::One))
    }
}
