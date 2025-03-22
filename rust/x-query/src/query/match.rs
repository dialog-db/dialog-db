use crate::{DataType, IndexKey, KeyPart, PrimaryKey, Value, XQueryError};

use super::{Frame, MatchableTerm, Pattern, VariableAssignment};

pub fn match_single(
    key: &PrimaryKey,
    pattern: &Pattern,
    mut frame: Frame,
) -> Result<Option<Frame>, XQueryError> {
    let key_fragments = key.parts();
    let pattern_parts = pattern.parts()?;

    for i in 0..3usize {
        let Some(next_frame) = match_term(key, &key_fragments[i], &pattern_parts[i], frame)? else {
            return Ok(None);
        };
        frame = next_frame;
    }

    Ok(Some(frame))
}

pub fn match_term(
    key: &PrimaryKey,
    key_part: &KeyPart,
    pattern_term: &MatchableTerm,
    frame: Frame,
) -> Result<Option<Frame>, XQueryError> {
    Ok(match pattern_term {
        MatchableTerm::Constant {
            value,
            key_part: pattern_key_part,
            attribute_key_part,
        } => match (key_part, value, attribute_key_part) {
            (KeyPart::Attribute(_), _, Some(pattern_key_part)) => {
                if key_part == pattern_key_part {
                    Some(frame)
                } else {
                    None
                }
            }
            (KeyPart::Entity(entity), Value::Entity(pattern_entity), _) => {
                if entity == &**pattern_entity {
                    Some(frame)
                } else {
                    None
                }
            }
            _ => {
                if key_part == pattern_key_part {
                    Some(frame)
                } else {
                    None
                }
            }
        },
        MatchableTerm::Variable(variable) => {
            if let Some(assignment) = frame.read(variable) {
                match (key_part, assignment) {
                    // Entity == Entity
                    (KeyPart::Entity(left), VariableAssignment::Entity(right))
                        if left == &right.entity =>
                    {
                        Some(frame)
                    }
                    // Entity == Value
                    (KeyPart::Entity(left), VariableAssignment::Value(DataType::Entity, right))
                        if left == &right.value.1 =>
                    {
                        Some(frame)
                    }
                    // Attribute == Attribute
                    (KeyPart::Attribute(left), VariableAssignment::Attribute(right))
                        if left == &right.attribute =>
                    {
                        Some(frame)
                    }
                    // Value == Value
                    (KeyPart::Value(left), VariableAssignment::Value(_, key))
                        if left == &key.value =>
                    {
                        Some(frame)
                    }
                    // Value == Entity
                    (KeyPart::Value(left), VariableAssignment::Entity(right))
                        if left == &(DataType::Entity.into(), right.entity) =>
                    {
                        Some(frame)
                    }
                    _ => None,
                }
            } else {
                let key = key.clone();
                Some(frame.assign(
                    (*variable).clone(),
                    match key_part {
                        KeyPart::Entity(_) => VariableAssignment::Entity(key),
                        KeyPart::Attribute(_) => VariableAssignment::Attribute(key),
                        KeyPart::Value((data_type, _)) => {
                            VariableAssignment::Value(DataType::from(data_type), key)
                        }
                    },
                )?)
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        Frame, Pattern, Term, TripleStore, Value, Variable, VariableAssignment, make_store,
    };
    use anyhow::Result;

    use super::match_single;

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_makes_a_literal_match() -> Result<()> {
        let (_, data) = make_store().await?;
        let (key, entity, attribute, value) = data.get(0).unwrap();

        let pattern = Pattern::try_from((
            Value::Entity(entity.clone()),
            Value::Symbol(attribute.to_string()),
            value.clone(),
        ))?;

        let frame = Frame::default();
        let next_frame = match_single(&key, &pattern, frame)?;

        assert!(next_frame.is_some());

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_makes_a_partially_variable_match() -> Result<()> {
        let (store, data) = make_store().await?;
        let (key, entity, attribute, value) = data.get(0).unwrap();

        let pattern = Pattern::try_from((
            Value::Entity(entity.clone()),
            Value::Symbol(attribute.to_string()),
            Variable::from("foo"),
        ))?;

        let frame: Frame = Frame::default();
        let next_frame = match_single(&key, &pattern, frame)?;

        assert!(next_frame.is_some());

        let next_frame = next_frame.unwrap();
        let foo = next_frame.read(&Variable::from("foo"));

        assert!(foo.is_some());

        let foo = foo.unwrap();

        match foo {
            crate::VariableAssignment::Value(_, key) => {
                let datum = store.read(key).await?;

                assert!(datum.is_some());

                let (_, _, matched_value) = datum.unwrap();
                assert_eq!(value, &matched_value);
            }
            _ => assert!(false),
        }

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_makes_a_fully_variable_match() -> Result<()> {
        let (store, data) = make_store().await?;
        let (key, entity, attribute, value) = data.get(0).unwrap();

        let pattern = Pattern::try_from((
            Variable::from("foo"),
            Variable::from("bar"),
            Variable::from("baz"),
        ))?;

        let frame = Frame::default();
        let next_frame = match_single(&key, &pattern, frame)?;

        assert!(next_frame.is_some());

        let next_frame = next_frame.unwrap();

        let foo = next_frame.read(&Variable::from("foo"));
        let bar = next_frame.read(&Variable::from("bar"));
        let baz = next_frame.read(&Variable::from("baz"));

        assert!(foo.is_some() && bar.is_some() && baz.is_some());

        let foo = foo.unwrap();
        let bar = bar.unwrap();
        let baz = baz.unwrap();

        match (foo, bar, baz) {
            (
                VariableAssignment::Entity(entity_key),
                VariableAssignment::Attribute(attribute_key),
                VariableAssignment::Value(_, value_key),
            ) => {
                assert_eq!(entity_key, attribute_key);
                assert_eq!(entity_key, value_key);

                let datum = store.read(entity_key).await?;

                assert!(datum.is_some());

                let (matched_entity, matched_attribute, matched_value) = datum.unwrap();

                assert_eq!(entity, &matched_entity);
                assert_eq!(attribute, &matched_attribute);
                assert_eq!(value, &matched_value);
            }
            _ => assert!(false),
        }

        Ok(())
    }
}
