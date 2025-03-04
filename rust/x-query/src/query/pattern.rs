use std::cell::OnceCell;

use crate::{Fragment, XQueryError};

use super::{Literal, Variable};

pub enum PatternPart<'a> {
    Literal(&'a Fragment),
    Variable(&'a Variable),
}

pub enum Part {
    Literal(Literal),
    Variable(Variable),
}

pub enum PatternSlot {
    Literal(Literal, OnceCell<Fragment>),
    Variable(Variable),
}

impl From<Part> for PatternSlot {
    fn from(value: Part) -> Self {
        match value {
            Part::Literal(literal) => PatternSlot::Literal(literal, OnceCell::new()),
            Part::Variable(variable) => PatternSlot::Variable(variable),
        }
    }
}

pub struct Pattern {
    entity: PatternSlot,
    attribute: PatternSlot,
    value: PatternSlot,
}

impl From<(Part, Part, Part)> for Pattern {
    fn from((entity, attribute, value): (Part, Part, Part)) -> Self {
        Pattern {
            entity: entity.into(),
            attribute: attribute.into(),
            value: value.into(),
        }
    }
}

impl Pattern {
    pub fn entity(&self) -> Result<PatternPart, XQueryError> {
        match &self.entity {
            PatternSlot::Literal(literal @ Literal::Entity(_), fragment_cache) => Ok(
                PatternPart::Literal(fragment_cache.get_or_init(|| Fragment::from(literal))),
            ),
            PatternSlot::Literal(literal, _) => Err(XQueryError::InvalidPattern(format!(
                "Expected entity, got {:?}",
                literal
            ))),
            PatternSlot::Variable(variable) => Ok(PatternPart::Variable(variable)),
        }
    }

    pub fn attribute(&self) -> Result<PatternPart, XQueryError> {
        match &self.attribute {
            PatternSlot::Literal(literal @ Literal::Attribute(_), fragment_cache) => Ok(
                PatternPart::Literal(fragment_cache.get_or_init(|| Fragment::from(literal))),
            ),
            PatternSlot::Literal(literal, _) => Err(XQueryError::InvalidPattern(format!(
                "Expected attribute, got {:?}",
                literal
            ))),
            PatternSlot::Variable(variable) => Ok(PatternPart::Variable(variable)),
        }
    }

    pub fn value(&self) -> Result<PatternPart, XQueryError> {
        match &self.value {
            PatternSlot::Literal(literal @ Literal::Value(_), fragment_cache) => Ok(
                PatternPart::Literal(fragment_cache.get_or_init(|| Fragment::from(literal))),
            ),
            PatternSlot::Literal(literal, _) => Err(XQueryError::InvalidPattern(format!(
                "Expected value, got {:?}",
                literal
            ))),
            PatternSlot::Variable(variable) => Ok(PatternPart::Variable(variable)),
        }
    }

    pub fn parts(&self) -> Result<[PatternPart; 3], XQueryError> {
        let entity = self.entity()?;
        let attribute = self.attribute()?;
        let value = self.value()?;

        Ok([entity, attribute, value])
    }
}
