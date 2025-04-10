use async_stream::try_stream;
use futures_util::{TryStreamExt, stream, stream_select};

use crate::{FrameStream, Scope, Term, TripleStorePull, Variable, XQueryError, fork_stream};

use super::{PullQuery, Query};

#[derive(Clone, Debug)]
pub struct And<L, R>(pub L, pub R)
where
    L: PullQuery + 'static,
    R: PullQuery + 'static;

impl<L, R> Query for And<L, R>
where
    L: PullQuery + 'static,
    R: PullQuery + 'static,
{
    fn scope(&self, scope: &Scope) -> Self {
        Self(self.0.scope(scope), self.1.scope(scope))
    }

    fn substitute(&self, variable: &Variable, alternate: &Term) -> Result<Self, XQueryError> {
        Ok(Self(
            self.0.substitute(variable, alternate)?,
            self.1.substitute(variable, alternate)?,
        ))
    }
}

impl<L, R> PullQuery for And<L, R>
where
    L: PullQuery + Send + 'static,
    R: PullQuery + Send + 'static,
{
    fn stream<S, F>(self, store: S, frames: F) -> impl FrameStream
    where
        S: TripleStorePull + Send + 'static,
        F: FrameStream + 'static,
    {
        try_stream! {
            let Self(left, right) = self;

            let left_frames = left.stream(store.clone(), frames);
            let right_frames = right.stream(store, left_frames);

            for await frame in right_frames {
                yield frame?;
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct Or<L, R>(pub L, pub R)
where
    L: PullQuery + 'static,
    R: PullQuery + 'static;

impl<L, R> Query for Or<L, R>
where
    L: PullQuery + 'static,
    R: PullQuery + 'static,
{
    fn scope(&self, scope: &Scope) -> Self {
        Self(self.0.scope(scope), self.1.scope(scope))
    }

    fn substitute(&self, variable: &Variable, alternate: &Term) -> Result<Self, XQueryError> {
        Ok(Self(
            self.0.substitute(variable, alternate)?,
            self.1.substitute(variable, alternate)?,
        ))
    }
}

impl<L, R> PullQuery for Or<L, R>
where
    L: PullQuery + Send + 'static,
    R: PullQuery + Send + 'static,
{
    fn stream<S, F>(self, store: S, frames: F) -> impl FrameStream
    where
        S: TripleStorePull + Send + 'static,
        F: FrameStream + 'static,
    {
        try_stream! {
            let (left_frames, right_frames) = fork_stream(frames);

            let Self(left, right) = self;

            let left_frames = left.stream(store.clone(), left_frames);
            let right_frames = right.stream(store.clone(), right_frames);

            tokio::pin!(left_frames);
            tokio::pin!(right_frames);

            for await frame in stream_select!(left_frames, right_frames) {
                yield frame?;
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct Not<Q>(pub Q)
where
    Q: PullQuery + 'static;

impl<Q> Query for Not<Q>
where
    Q: PullQuery + 'static,
{
    fn scope(&self, scope: &Scope) -> Self {
        Self(self.0.scope(scope))
    }

    fn substitute(&self, variable: &Variable, alternate: &Term) -> Result<Self, XQueryError> {
        Ok(Self(self.0.substitute(variable, alternate)?))
    }
}

impl<Q> PullQuery for Not<Q>
where
    Q: PullQuery + Send + 'static,
{
    fn stream<S, F>(self, store: S, frames: F) -> impl FrameStream
    where
        S: TripleStorePull + Send + 'static,
        F: FrameStream + 'static,
    {
        try_stream! {
            let Self(inner_query) = self;

            for await frame in frames {
                let frame = frame?;
                let not_frame = frame.clone();
                let not_stream = inner_query.clone().stream(store.clone(), stream::once(async move { Ok(not_frame) }));

                tokio::pin!(not_stream);

                if let Ok(Some(_)) = not_stream.try_next().await {
                    continue;
                }

                yield frame;
            }
        }
    }
}
