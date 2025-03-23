use crate::{Attribute, KeyPart, XQueryError};

use super::{Query, Scope, Value, Variable};

#[derive(Clone)]
pub enum Term {
    Constant(Value),
    Variable(Variable),
}

impl Term {
    pub fn scope(&self, scope: &Scope) -> Self {
        match self {
            any @ Term::Constant(_) => any.clone(),
            Term::Variable(variable) => Term::Variable(variable.scope(scope)),
        }
    }
}

impl From<Variable> for Term {
    fn from(value: Variable) -> Self {
        Term::Variable(value)
    }
}

impl From<&Variable> for Term {
    fn from(value: &Variable) -> Self {
        Term::Variable(value.clone())
    }
}

impl From<Value> for Term {
    fn from(value: Value) -> Self {
        Term::Constant(value)
    }
}

impl From<&Value> for Term {
    fn from(value: &Value) -> Self {
        Term::Constant(value.clone())
    }
}

#[derive(Debug, Clone)]
pub enum MatchableTerm {
    Constant {
        value: Value,
        key_part: KeyPart,
        attribute_key_part: Option<KeyPart>,
    },
    Variable(Variable),
}

impl TryFrom<Term> for MatchableTerm {
    type Error = XQueryError;

    fn try_from(value: Term) -> Result<Self, XQueryError> {
        Ok(match value {
            Term::Constant(value @ Value::Symbol(_)) => MatchableTerm::Constant {
                key_part: KeyPart::from(&value),
                attribute_key_part: Some(KeyPart::from(Attribute::try_from(&value)?)),
                value,
            },
            Term::Constant(value) => MatchableTerm::Constant {
                key_part: KeyPart::from(&value),
                attribute_key_part: None,
                value,
            },
            Term::Variable(variable) => MatchableTerm::Variable(variable),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Pattern {
    pub entity: MatchableTerm,
    pub attribute: MatchableTerm,
    pub value: MatchableTerm,
}

impl Query for Pattern {
    fn scope(&self, scope: &Scope) -> Self {
        Pattern::scope(self, scope)
    }

    fn substitute(&self, variable: &Variable, alternate: &Term) -> Result<Self, XQueryError> {
        match &self.entity {
            MatchableTerm::Variable(entity_variable) if entity_variable == variable => {
                return self.replace_entity(alternate.clone());
            }
            _ => (),
        };

        match &self.attribute {
            MatchableTerm::Variable(attribute_variable) if attribute_variable == variable => {
                return self.replace_attribute(alternate.clone());
            }
            _ => (),
        };

        match &self.value {
            MatchableTerm::Variable(value_variable) if value_variable == variable => {
                return self.replace_value(alternate.clone());
            }
            _ => (),
        };

        Ok(self.clone())
    }
}

impl<E, A, V> TryFrom<(E, A, V)> for Pattern
where
    Term: From<E>,
    Term: From<A>,
    Term: From<V>,
{
    type Error = XQueryError;

    fn try_from((entity, attribute, value): (E, A, V)) -> Result<Self, Self::Error> {
        let entity = Term::from(entity);

        match entity {
            Term::Constant(Value::Entity(_)) => (),
            Term::Constant(value) => {
                return Err(XQueryError::InvalidPattern(format!(
                    "The first term of a pattern must be an entity or a variable (got {:?})",
                    value
                )));
            }
            _ => (),
        };

        let attribute = Term::from(attribute);

        match attribute {
            Term::Constant(Value::Symbol(_)) => (),
            Term::Constant(value) => {
                return Err(XQueryError::InvalidPattern(format!(
                    "The second term of a pattern must be a symbol or a variable (got {:?})",
                    value
                )));
            }
            _ => (),
        };

        let value = Term::from(value);

        Ok(Self {
            entity: MatchableTerm::try_from(entity)?,
            attribute: MatchableTerm::try_from(attribute)?,
            value: MatchableTerm::try_from(value)?,
        })
    }
}

impl Pattern {
    pub fn replace_entity<E>(&self, entity: E) -> Result<Self, XQueryError>
    where
        Term: From<E>,
    {
        Ok(Pattern {
            entity: MatchableTerm::try_from(Term::from(entity))?,
            attribute: self.attribute.clone(),
            value: self.value.clone(),
        })
    }

    pub fn replace_attribute<A>(&self, attribute: A) -> Result<Self, XQueryError>
    where
        Term: From<A>,
    {
        Ok(Pattern {
            entity: self.entity.clone(),
            attribute: MatchableTerm::try_from(Term::from(attribute))?,
            value: self.value.clone(),
        })
    }

    pub fn replace_value<V>(&self, value: V) -> Result<Self, XQueryError>
    where
        Term: From<V>,
    {
        Ok(Pattern {
            entity: self.entity.clone(),
            attribute: self.attribute.clone(),
            value: MatchableTerm::try_from(Term::from(value))?,
        })
    }

    pub fn scope(&self, scope: &Scope) -> Self {
        Pattern {
            entity: match &self.entity {
                any @ MatchableTerm::Constant { .. } => any.clone(),
                MatchableTerm::Variable(variable) => MatchableTerm::Variable(variable.scope(scope)),
            },
            attribute: match &self.attribute {
                any @ MatchableTerm::Constant { .. } => any.clone(),
                MatchableTerm::Variable(variable) => MatchableTerm::Variable(variable.scope(scope)),
            },
            value: match &self.value {
                any @ MatchableTerm::Constant { .. } => any.clone(),
                MatchableTerm::Variable(variable) => MatchableTerm::Variable(variable.scope(scope)),
            },
        }
    }

    pub fn entity(&self) -> &MatchableTerm {
        &self.entity
    }

    pub fn attribute(&self) -> &MatchableTerm {
        &self.attribute
    }

    pub fn value(&self) -> &MatchableTerm {
        &self.value
    }

    pub fn parts(&self) -> Result<[&MatchableTerm; 3], XQueryError> {
        let entity = self.entity();
        let attribute = self.attribute();
        let value = self.value();

        Ok([entity, attribute, value])
    }
}
