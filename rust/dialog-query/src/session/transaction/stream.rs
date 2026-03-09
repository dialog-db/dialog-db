use crate::artifact::Instruction;
use futures_util::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::vec::IntoIter;

use super::Transaction;

/// A [`Stream`] adapter that drains a consumed [`Transaction`] into
/// individual [`Instruction`]s for writing to the artifact store.
pub struct TransactionStream {
    /// Iterator over the transaction's instructions
    iter: IntoIter<Instruction>,
}

impl From<Transaction> for TransactionStream {
    fn from(transaction: Transaction) -> Self {
        Self {
            iter: transaction.into_iter(),
        }
    }
}

impl Stream for TransactionStream {
    type Item = Instruction;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.iter.next())
    }
}
