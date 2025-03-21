use crate::{ZSet, ZSetElement};

pub struct Diff<T>
where
    T: ZSetElement,
{
    elements: ZSet<T>,
    clock: usize,
}

impl<T> Diff<T>
where
    T: ZSetElement,
{
    pub fn new(elements: ZSet<T>, clock: usize) -> Self {
        Self { elements, clock }
    }
}
