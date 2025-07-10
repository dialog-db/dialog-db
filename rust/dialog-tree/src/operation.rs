use std::marker::PhantomData;

use nonempty::NonEmpty;

use crate::{Branch, KeyBuffer, Node, TreeStorage, ValueBuffer};

// type BranchStack<const HASH_SIZE: usize, Key, Hash> = Vec<(
//     Option<NonEmpty<Reference<HASH_SIZE, Key, Hash>>>,
//     Option<NonEmpty<Reference<HASH_SIZE, Key, Hash>>>,
// )>;
type BranchStack<'a, Key> = Vec<(Option<NonEmpty<&'a Branch<'a, Key>>>, Option<NonEmpty<&'a Branch<'a, Key>>>>)>;

pub struct TreeOperation<'a, const BRANCH_FACTOR: u32, Key, Value, Distribution, Storage>
where
    Key: KeyBuffer<'a>,
    Value: ValueBuffer<'a>,
{
    key: PhantomData<Key>,
    value: PhantomData<Value>,
    distribution: PhantomData<Distribution>,
    lifetime: PhantomData<&'a ()>,

    root: &'a Node<'a, Key, Value>,
    storage: &'a Storage,
}

impl<'a, const BRANCH_FACTOR: u32, Key, Value, Distribution, Storage>
    TreeOperation<'a, BRANCH_FACTOR, Key, Value, Distribution, Storage>
where
    Key: KeyBuffer<'a>,
    Value: ValueBuffer<'a>,
    Distribution: crate::Distribution<'a>,
    Storage: TreeStorage,
{
    pub fn new(root: &'a Node<'a, Key, Value>, storage: &'a Storage) -> Self {
        Self {
            key: PhantomData,
            value: PhantomData,
            distribution: PhantomData,
            lifetime: PhantomData,

            root,
            storage,
        }
    }

    pub fn insert(&self, key: Key, value: Value) -> Node<'a, Key, Value> {
        todo!()
    }

    pub fn bisect(&self, key: &Key) -> 
}
