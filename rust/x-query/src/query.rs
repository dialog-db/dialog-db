mod literal;

use std::pin::Pin;

use async_stream::try_stream;
use futures_core::TryStream;
use futures_util::stream;
use futures_util::{TryStreamExt, stream_select};

pub use literal::*;
use tokio::sync::mpsc::unbounded_channel;

mod variable;
pub use variable::*;

mod pattern;
pub use pattern::*;

mod frame;
pub use frame::*;

mod r#match;
pub use r#match::*;
use x_common::ConditionalSend;

use crate::{Fragment, PrimaryKey, TripleStore, XQueryError};

pub trait QueryStream<T>:
    TryStream<Ok = T, Error = XQueryError, Item = Result<T, XQueryError>> + ConditionalSend + 'static
{
}

impl<S, T> QueryStream<T> for S where
    S: TryStream<Ok = T, Error = XQueryError, Item = Result<T, XQueryError>>
        + 'static
        + ConditionalSend
{
}

#[derive(Clone)]
pub enum Query {
    Simple(Pattern),
    And(Box<Query>, Box<Query>),
    Or(Box<Query>, Box<Query>),
    Not(Box<Query>),
}

impl Query {
    fn forked_frame_stream<S>(
        input: S,
    ) -> (
        Pin<Box<dyn QueryStream<Frame>>>,
        Pin<Box<dyn QueryStream<Frame>>>,
    )
    where
        S: TryStream<Item = Result<Frame, XQueryError>> + 'static + ConditionalSend,
    {
        let (left_tx, mut left_rx) = unbounded_channel();
        let (right_tx, mut right_rx) = unbounded_channel();

        tokio::spawn(async move {
            tokio::pin!(input);

            while let Ok(item) = input.try_next().await {
                if let Err(_error) = left_tx.send(item.clone()) {
                    break;
                };
                if let Err(_error) = right_tx.send(item) {
                    break;
                }
            }
        });

        let left = Box::pin(try_stream! {
            while let Some(Some(item)) = left_rx.recv().await {
                    yield item;
            }
        });

        let right = Box::pin(try_stream! {
            while let Some(Some(item)) = right_rx.recv().await {
                    yield item;
            }
        });

        (left, right)
    }

    fn store_stream<T>(store: T, pattern: &Pattern) -> Pin<Box<dyn QueryStream<PrimaryKey>>>
    where
        T: TripleStore + 'static,
    {
        let pattern = pattern.clone();

        Box::pin(try_stream! {
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
        })
    }

    pub fn frame_stream<T>(
        self: Query,
        store: T,
        frames: Pin<Box<dyn QueryStream<Frame>>>,
    ) -> Pin<Box<dyn QueryStream<Frame>>>
    where
        T: TripleStore + 'static,
    {
        Box::pin(try_stream! {
            match self {
                Query::Simple(pattern) => {
                    for await frame in frames {
                        let frame = frame?;
                        let stream = Self::store_stream(store.clone(), &pattern);

                        for await item in stream {
                            let item = item?;
                            if let Some(frame) = match_single(&item, &pattern, frame.clone())? {
                                yield frame;
                            }
                        }
                    }
                }
                Query::And(left, right) => {
                    let left_frames = left.frame_stream(store.clone(), frames);
                    let right_frames = right.frame_stream(store, left_frames);

                    for await frame in right_frames {
                        yield frame?;
                    }
                }
                Query::Or(left, right) => {
                    let (left_frames, right_frames) = Self::forked_frame_stream(frames);

                    let left_frames = left.frame_stream(store.clone(), left_frames);
                    let right_frames = right.frame_stream(store.clone(), right_frames);

                    for await frame in stream_select!(left_frames, right_frames) {
                        yield frame?;
                    }
                }
                Query::Not(not) => {
                    for await frame in frames {
                        let frame = frame?;
                        let not_frame = frame.clone();
                        let not_stream = not.clone().frame_stream(store.clone(), Box::pin(stream::once(async move { Ok(not_frame) })));

                        tokio::pin!(not_stream);

                        if let Ok(Some(_)) = not_stream.try_next().await {
                            continue;
                        }

                        yield frame;
                    }
                }
            }
        })
    }

    pub fn stream<T>(
        self,
        store: T,
    ) -> impl TryStream<Item = Result<Frame, XQueryError>> + 'static + ConditionalSend
    where
        T: TripleStore + 'static,
    {
        self.frame_stream(
            store,
            Box::pin(stream::once(async { Ok(Frame::default()) })),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::{
        Attribute, Frame, Literal, Part, Pattern, PrimaryKey, TripleStore, Variable, make_store,
    };

    use super::Query;
    use anyhow::Result;
    use futures_util::TryStreamExt;

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_produce_a_stream_from_compound_and() -> Result<()> {
        let (store, _) = make_store().await?;

        let stream = Query::And(
            Query::Simple(Pattern::from((
                Part::Variable(Variable::from("entity")),
                Part::Literal(Literal::Attribute(Attribute::from_str("item:name")?)),
                Part::Variable(Variable::from("name")),
            )))
            .into(),
            Query::Simple(Pattern::from((
                Part::Variable(Variable::from("entity")),
                Part::Literal(Literal::Attribute(Attribute::from_str("item:id")?)),
                Part::Literal(Literal::Value(5usize.to_le_bytes().to_vec())),
            )))
            .into(),
        )
        .stream(store.clone());

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
}
