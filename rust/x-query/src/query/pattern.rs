use async_stream::try_stream;

use crate::{Fragment, FrameStream, TripleStore, XQueryError, match_single, query::key_stream};

use super::{Literal, Query, Variable};

pub enum PatternPart<'a> {
    Literal(&'a Fragment),
    Variable(&'a Variable),
}

pub enum Part {
    Literal(Literal),
    Variable(Variable),
}

#[derive(Debug, Clone)]
pub enum PatternSlot {
    Literal(Literal, Fragment),
    Variable(Variable),
}

impl From<Part> for PatternSlot {
    fn from(value: Part) -> Self {
        match value {
            Part::Literal(literal) => {
                PatternSlot::Literal(literal.clone(), Fragment::from(literal))
            }
            Part::Variable(variable) => PatternSlot::Variable(variable),
        }
    }
}

#[derive(Debug, Clone)]
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
            PatternSlot::Literal(Literal::Entity(_), fragment) => {
                Ok(PatternPart::Literal(fragment))
            }
            PatternSlot::Literal(literal, _) => Err(XQueryError::InvalidPattern(format!(
                "Expected entity, got {:?}",
                literal
            ))),
            PatternSlot::Variable(variable) => Ok(PatternPart::Variable(variable)),
        }
    }

    pub fn attribute(&self) -> Result<PatternPart, XQueryError> {
        match &self.attribute {
            PatternSlot::Literal(Literal::Attribute(_), fragment) => {
                Ok(PatternPart::Literal(fragment))
            }
            PatternSlot::Literal(literal, _) => Err(XQueryError::InvalidPattern(format!(
                "Expected attribute, got {:?}",
                literal
            ))),
            PatternSlot::Variable(variable) => Ok(PatternPart::Variable(variable)),
        }
    }

    pub fn value(&self) -> Result<PatternPart, XQueryError> {
        match &self.value {
            PatternSlot::Literal(Literal::Value(_), fragment) => Ok(PatternPart::Literal(fragment)),
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

impl Query for Pattern {
    fn stream<S, F>(self, store: S, frames: F) -> impl FrameStream
    where
        S: TripleStore + 'static,
        F: FrameStream + 'static,
    {
        try_stream! {
            for await frame in frames {
                let frame = frame?;
                let stream = key_stream(store.clone(), &self);

                for await item in stream {
                    let item = item?;
                    if let Some(frame) = match_single(&item, &self, frame.clone())? {
                        yield frame;
                    }
                }
            }
        }
    }
}
