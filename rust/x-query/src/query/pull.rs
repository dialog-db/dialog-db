use async_stream::try_stream;

use crate::{FrameStream, TripleStorePull, Value, XQueryError, match_single};

use super::{MatchableTerm, Pattern, Query, Scope, Term, Variable};

mod compound;
pub use compound::*;

mod key;
pub use key::*;

mod rule;
pub use rule::*;

pub trait PullQuery: Query {
    fn stream<S, F>(self, store: S, frames: F) -> impl FrameStream
    where
        S: TripleStorePull + 'static,
        F: FrameStream + 'static;
}

impl Query for Pattern {
    fn scope(&self, scope: &Scope) -> Self {
        Pattern::scope(self, scope)
    }

    fn substitute(&self, variable: &Variable, constant: &Value) -> Result<Self, XQueryError> {
        match &self.entity {
            MatchableTerm::Variable(entity_variable) if entity_variable == variable => {
                return self.replace_entity(Term::Constant(constant.clone()));
            }
            _ => (),
        };

        match &self.attribute {
            MatchableTerm::Variable(attribute_variable) if attribute_variable == variable => {
                return self.replace_entity(Term::Constant(constant.clone()));
            }
            _ => (),
        };

        match &self.value {
            MatchableTerm::Variable(value_variable) if value_variable == variable => {
                return self.replace_entity(Term::Constant(constant.clone()));
            }
            _ => (),
        };

        Ok(self.clone())
    }
}

impl PullQuery for Pattern {
    fn stream<S, F>(self, store: S, frames: F) -> impl FrameStream
    where
        S: TripleStorePull + 'static,
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::{
        Attribute, Frame, Pattern, PrimaryKey, Term, TripleStore, Value, Variable, make_store,
        pull::{And, Not, Or, PullQuery},
    };
    use anyhow::Result;
    use futures_util::{TryStreamExt, stream};

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_produce_a_stream_from_compound_and_query() -> Result<()> {
        let (store, _) = make_store().await?;

        let stream = And(
            Pattern::try_from((
                Variable::from("entity"),
                Value::Symbol("item/name".into()),
                Variable::from("name"),
            ))?,
            Pattern::try_from((
                Variable::from("entity"),
                Value::Symbol("item/id".into()),
                Value::from(5u32),
            ))?,
        )
        .stream(store.clone(), stream::once(async { Ok(Frame::default()) }));

        tokio::pin!(stream);

        let mut count = 0;
        let mut final_frame = Frame::default();

        while let Some(frame) = stream.try_next().await? {
            final_frame = frame;
            count += 1;
        }
        assert_eq!(count, 1);

        let name_key = PrimaryKey::from(
            final_frame
                .read(&Variable::from("name"))
                .expect("A value is assigned to 'name'"),
        );
        let (_, _, value) = store
            .read(&name_key)
            .await?
            .expect("A datum exists for the queried key");

        let name = match value {
            Value::String(value) => value,
            _ => panic!("Value should be a string!"),
        };

        assert_eq!(name, "name5");

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_produce_a_stream_from_compound_or_query() -> Result<()> {
        let (store, data) = make_store().await?;

        let stream = Or(
            Pattern::try_from((
                Variable::from("entity"),
                Value::Symbol("item/name".into()),
                Value::String("Aragorn".into()),
            ))?,
            Pattern::try_from((
                Variable::from("entity"),
                Value::Symbol("item/id".into()),
                Value::UnsignedInt(5u128),
            ))?,
        )
        .stream(store.clone(), stream::once(async { Ok(Frame::default()) }));

        tokio::pin!(stream);

        let mut count = 0;
        let mut final_frame = Frame::default();

        while let Some(frame) = stream.try_next().await? {
            final_frame = frame;
            count += 1;
        }
        assert_eq!(count, 1);

        let entity_key = PrimaryKey::from(
            final_frame
                .read(&Variable::from("entity"))
                .expect("A value is assigned to 'entity'"),
        );
        let (entity, _, _) = store
            .read(&entity_key)
            .await?
            .expect("A datum exists for the queried key");

        let expected_entity = data.get(15).unwrap().0.entity.into();

        assert_eq!(entity, expected_entity);

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_produce_a_stream_from_not_query() -> Result<()> {
        let (store, _) = make_store().await?;

        let stream = And(
            And(
                Pattern::try_from((
                    Variable::from("entity"),
                    Value::Symbol("item/name".into()),
                    Variable::from("name"),
                ))?,
                Pattern::try_from((
                    Variable::from("entity"),
                    Value::Symbol("item/id".into()),
                    Variable::from("id"),
                ))?,
            ),
            Not(Pattern::try_from((
                Variable::from("entity"),
                Value::Symbol("item/id".into()),
                Value::UnsignedInt(5u128),
            ))?),
        )
        .stream(store.clone(), stream::once(async { Ok(Frame::default()) }));

        tokio::pin!(stream);

        let mut frames = vec![];

        while let Some(frame) = stream.try_next().await? {
            let id_key = PrimaryKey::from(
                frame
                    .read(&Variable::from("id"))
                    .expect("A value is assigned to 'name'"),
            );
            let (_, _, id_value) = store
                .read(&id_key)
                .await?
                .expect("A datum exists for the queried key");

            let id = match id_value {
                Value::UnsignedInt(value) => value,
                _ => panic!("Expected an unsigned int"),
            };

            assert_ne!(id, 5);

            frames.push(frame);
        }

        assert_eq!(frames.len(), 7);

        Ok(())
    }
}
