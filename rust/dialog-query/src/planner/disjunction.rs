use crate::selection::Selection;
use crate::stream::{fork_stream, stream_select};
use crate::{Source, try_stream};
use core::pin::Pin;

use super::Conjunction;

/// A union of alternative [`Conjunction`] plans whose result streams are merged.
///
/// When a concept has multiple deductive rules, each rule body produces a
/// separate `Conjunction`. A `Disjunction` combines them so that evaluation runs all
/// alternatives concurrently and yields the union of their matches. This
/// is the disjunction counterpart to `Conjunction`'s conjunction.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum Disjunction {
    /// No alternatives - produces no results
    #[default]
    Empty,
    /// Single alternative join
    Solo(Conjunction),
    /// Two alternative joins
    Duet(Conjunction, Conjunction),
    /// Three or more alternative joins (recursive)
    Or(Box<Disjunction>, Conjunction),
}

impl Disjunction {
    /// Creates a new empty join (identity).
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new join of two plans.
    pub fn or(self, right: Conjunction) -> Self {
        match self {
            Self::Empty => Self::Solo(right),
            Self::Solo(left) => Self::Duet(left, right),
            _ => Self::Or(Box::new(self), right),
        }
    }

    /// Evaluate all alternatives, merging their result streams.
    ///
    /// Returns `Pin<Box<...>>` because Disjunction is recursive — Or holds a
    /// `Box<Disjunction>` whose evaluate calls back into this method. Boxing
    /// keeps each alternative at pointer size on the stack.
    pub fn evaluate<S: Source, M: Selection>(
        self,
        selection: M,
        source: &S,
    ) -> Pin<Box<dyn Selection>> {
        match self {
            Self::Empty => Box::pin(futures_util::stream::empty()),
            Self::Solo(join) => Box::pin(join.evaluate(selection, source)),
            Self::Duet(left, right) => Self::merge(Self::Solo(left), right, selection, source),
            Self::Or(left, right) => Self::merge(*left, right, selection, source),
        }
    }
}

impl FromIterator<Conjunction> for Disjunction {
    fn from_iter<I: IntoIterator<Item = Conjunction>>(iter: I) -> Self {
        iter.into_iter()
            .fold(Self::new(), |fork, join| fork.or(join))
    }
}

impl Disjunction {
    /// Disjunction the input stream and merge two alternative evaluations.
    fn merge<S: Source, M: Selection>(
        left: Disjunction,
        right: Conjunction,
        selection: M,
        source: &S,
    ) -> Pin<Box<dyn Selection>> {
        let source = source.clone();
        Box::pin(try_stream! {
            let (left_input, right_input) = fork_stream(selection);

            let left_output = left.evaluate(left_input, &source);
            let right_output = right.evaluate(right_input, &source);

            tokio::pin!(left_output);
            tokio::pin!(right_output);

            for await each in stream_select!(left_output, right_output) {
                yield each?;
            }
        })
    }
}
