use async_stream::try_stream;

mod literal;
pub use literal::*;

mod variable;
pub use variable::*;

mod pattern;
pub use pattern::*;

mod frame;
pub use frame::*;

mod compound;
pub use compound::*;

mod r#match;
pub use r#match::*;

use x_common::ConditionalSend;

use crate::{Fragment, FrameStream, KeyStream, TripleStore};

fn key_stream<T>(store: T, pattern: &Pattern) -> impl KeyStream
where
    T: TripleStore + 'static,
{
    let pattern = pattern.clone();

    try_stream! {
        if let PatternPart::Literal(entity @ Fragment::Entity(_)) = pattern.entity()? {
            for await item in store.attributes_of_entity(entity.clone()) {
                yield item?;
            }
        } else if let PatternPart::Literal(attribute @ Fragment::Attribute(_)) = pattern.attribute()? {
            for await item in store.entities_with_attribute(attribute.clone()) {
                yield item?;
            }
        } else if let PatternPart::Literal(value @ Fragment::Value(_)) = pattern.value()? {
            for await item in store.entities_with_value(value.clone()) {
                yield item?;
            }
        } else {
            for await item in store.keys() {
                yield item?;
            }
        }
    }
}

pub trait Query: Clone + ConditionalSend {
    fn stream<S, F>(self, store: S, frames: F) -> impl FrameStream
    where
        S: TripleStore + 'static,
        F: FrameStream + 'static;
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::{
        And, Attribute, Frame, Literal, Not, Or, Part, Pattern, PrimaryKey, Query, TripleStore,
        Variable, make_store,
    };
    use anyhow::Result;
    use futures_util::{TryStreamExt, stream};

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_produce_a_stream_from_compound_and_query() -> Result<()> {
        let (store, _) = make_store().await?;

        let stream = And(
            Pattern::from((
                Part::Variable(Variable::from("entity")),
                Part::Literal(Literal::Attribute(Attribute::from_str("item:name")?)),
                Part::Variable(Variable::from("name")),
            )),
            Pattern::from((
                Part::Variable(Variable::from("entity")),
                Part::Literal(Literal::Attribute(Attribute::from_str("item:id")?)),
                Part::Literal(Literal::Value(5usize.to_le_bytes().to_vec())),
            )),
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

        let name = String::from_utf8(value).expect("Value is a valid UTF-8 string");

        assert_eq!(name, "name5");

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_produce_a_stream_from_compound_or_query() -> Result<()> {
        let (store, data) = make_store().await?;

        let stream = Or(
            Pattern::from((
                Part::Variable(Variable::from("entity")),
                Part::Literal(Literal::Attribute(Attribute::from_str("item:name")?)),
                Part::Literal(Literal::Value(b"Aragorn".to_vec())),
            )),
            Pattern::from((
                Part::Variable(Variable::from("entity")),
                Part::Literal(Literal::Attribute(Attribute::from_str("item:id")?)),
                Part::Literal(Literal::Value(5usize.to_le_bytes().to_vec())),
            )),
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
                Pattern::from((
                    Part::Variable(Variable::from("entity")),
                    Part::Literal(Literal::Attribute(Attribute::from_str("item:name")?)),
                    Part::Variable(Variable::from("name")),
                )),
                Pattern::from((
                    Part::Variable(Variable::from("entity")),
                    Part::Literal(Literal::Attribute(Attribute::from_str("item:id")?)),
                    Part::Variable(Variable::from("id")),
                )),
            ),
            Not(Pattern::from((
                Part::Variable(Variable::from("entity")),
                Part::Literal(Literal::Attribute(Attribute::from_str("item:id")?)),
                Part::Literal(Literal::Value(5usize.to_le_bytes().to_vec())),
            ))),
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

            let id = usize::from_le_bytes(id_value.try_into().unwrap());

            assert_ne!(id, 5);

            frames.push(frame);
        }

        assert_eq!(frames.len(), 7);

        Ok(())
    }
}
