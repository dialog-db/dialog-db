use crate::{Fragment, IndexKey, PrimaryKey, XQueryError};

use super::{Frame, Pattern, PatternPart, VariableAssignment};

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

#[cfg(test)]
mod tests {
    use crate::{
        Frame, Literal, Part, Pattern, TripleStore, Variable, VariableAssignment, make_store,
    };
    use anyhow::Result;

    use super::match_single;

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_makes_a_literal_match() -> Result<()> {
        let (_, data) = make_store().await?;
        let (key, entity, attribute, value) = data.get(0).unwrap();

        let pattern = Pattern::from((
            Part::Literal(Literal::Entity(entity.clone())),
            Part::Literal(Literal::Attribute(attribute.clone())),
            Part::Literal(Literal::Value(value.clone())),
        ));

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

        let pattern = Pattern::from((
            Part::Literal(Literal::Entity(entity.clone())),
            Part::Literal(Literal::Attribute(attribute.clone())),
            Part::Variable(Variable::from("foo")),
        ));

        let frame: Frame = Frame::default();
        let next_frame = match_single(&key, &pattern, frame)?;

        assert!(next_frame.is_some());

        let next_frame = next_frame.unwrap();
        let foo = next_frame.read(&Variable::from("foo"));

        assert!(foo.is_some());

        let foo = foo.unwrap();

        match foo {
            crate::VariableAssignment::Value(key) => {
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

        let pattern = Pattern::from((
            Part::Variable(Variable::from("foo")),
            Part::Variable(Variable::from("bar")),
            Part::Variable(Variable::from("baz")),
        ));

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
                VariableAssignment::Value(value_key),
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
