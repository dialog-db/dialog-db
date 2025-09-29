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
            None => Err(SchemaError::OmittedRequirement("the".into())),
            Some(Term::Variable { name: None, .. }) => Err(SchemaError::BlankRequirement {
                binding: "the".into(),
            }),
            Some(term) => Ok(term),
        };

        let of = match terms.get("of") {
            None => Err(SchemaError::OmittedRequirement("of".into())),
            Some(Term::Variable { name: None, .. }) => Err(SchemaError::BlankRequirement {
                binding: "of".into(),
            }),
            Some(term) => Ok(term),
        };

        let is = match terms.get("is") {
            None => Err(SchemaError::OmittedRequirement("is".into())),
            Some(Term::Variable { name: None, .. }) => Err(SchemaError::BlankRequirement {
                binding: "is".into(),
            }),
            Some(term) => Ok(term),
        };

        if matches!((the, of, is), (Err(_), Err(_), Err(_))) {
            Err(SchemaError::UnconstrainedSelector)
        } else {
            Ok(Selector {
                the: the.unwrap().into(),
                of: of.unwrap().into(),
                is: is.unwrap().into(),
            })
        }
    }
    pub fn apply(terms: Parameters) -> Result<FactApplication, SchemaError> {
        let Selector { the, of, is } = Self::conform(terms)?;

        Ok(FactApplication::new(the, of, is, Cardinality::One))
    }
}
