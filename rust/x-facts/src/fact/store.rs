use async_trait::async_trait;
use futures_util::Stream;
use x_common::ConditionalSend;

use crate::{Instruction, XFactsError};

use super::{Fact, FactSelector};

/// A trait that may be implemented by anything that is capable of
/// querying [`Fact`]s.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait FactStore
where
    Self: Sized,
{
    /// Query for [`Fact`]s that match the given [`FactSelector`]. Results are
    /// provided as a [`Stream`], implying that they are produced from the
    /// implementation lazily.
    ///
    /// For additional details, see the documentation for [`FactSelector`].
    fn select(
        &self,
        selector: FactSelector,
    ) -> impl Stream<Item = Result<Fact, XFactsError>> + '_ + ConditionalSend;
}

/// A trait that may be implemented by anything that is capable of
/// of storing [`Fact`]s.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait FactStoreMut: FactStore {
    /// Commit one or more [`Fact`]s to storage. Implementors should take care
    /// to ensure that commits are transactional and resilient to unexpected
    /// halts and other such failure modes.
    async fn commit<I>(&mut self, instructions: I) -> Result<(), XFactsError>
    where
        I: IntoIterator<Item = Instruction> + ConditionalSend,
        I::IntoIter: ConditionalSend;
}
