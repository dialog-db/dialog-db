use async_trait::async_trait;

use crate::{ZSet, ZSetElement};

#[async_trait]
pub trait Operator<I, O>
where
    I: ZSetElement,
    O: ZSetElement,
{
    async fn process(&mut self, input: ZSet<I>) -> ZSet<O>;
}
