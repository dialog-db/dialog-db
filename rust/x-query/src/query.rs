mod literal;
pub use literal::*;

mod variable;
pub use variable::*;

mod pattern;
pub use pattern::*;

mod frame;
pub use frame::*;

mod r#match;
pub use r#match::*;

use crate::{Fragment, IndexKey, PrimaryKey, XQueryError};

pub fn match_single(
    key: &PrimaryKey,
    pattern: &Pattern,
    mut frame: Frame,
) -> Result<Option<Frame>, XQueryError> {
    let key_fragments = key.fragments();
    let pattern_parts = pattern.parts()?;

    for i in 0..3usize {
        let Some(next_frame) = match_part(key, &key_fragments[i], &pattern_parts[i], frame)? else {
            return Ok(None);
        };
        frame = next_frame;
    }

    Ok(Some(frame))
}

pub fn match_part(
    key: &PrimaryKey,
    fragment: &Fragment,
    part: &PatternPart,
    frame: Frame,
) -> Result<Option<Frame>, XQueryError> {
    Ok(match part {
        PatternPart::Literal(pattern_fragment) => {
            if fragment == *pattern_fragment {
                Some(frame)
            } else {
                None
            }
        }
        PatternPart::Variable(variable) => {
            if let Some(assignment) = frame.read(variable) {
                match (fragment, assignment) {
                    // Entity == Entity
                    (Fragment::Entity(left), VariableAssignment::Entity(right))
                        if left == &right.entity =>
                    {
                        Some(frame)
                    }
                    // Entity == Value
                    (Fragment::Entity(left), VariableAssignment::Value(right))
                        if left == &right.value =>
                    {
                        Some(frame)
                    }
                    // Attribute == Attribute
                    (Fragment::Attribute(left), VariableAssignment::Attribute(right))
                        if left == &right.attribute =>
                    {
                        Some(frame)
                    }
                    // Value == Value
                    (Fragment::Value(left), VariableAssignment::Value(right))
                        if left == &right.value =>
                    {
                        Some(frame)
                    }
                    // Value == Entity
                    (Fragment::Value(left), VariableAssignment::Entity(right))
                        if left == &right.entity =>
                    {
                        Some(frame)
                    }
                    _ => None,
                }
            } else {
                let key = key.clone();
                Some(frame.assign(
                    (*variable).clone(),
                    match fragment {
                        Fragment::Entity(_) => VariableAssignment::Entity(key),
                        Fragment::Attribute(_) => VariableAssignment::Attribute(key),
                        Fragment::Value(_) => VariableAssignment::Value(key),
                    },
                )?)
            }
        }
    })
}
