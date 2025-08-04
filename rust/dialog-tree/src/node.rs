use std::marker::PhantomData;

use bumpalo::Bump;
use bytes::Bytes;
use dialog_common::{Blake3Hash, Blake3Hashed};
use dialog_encoding::{BufOrRef, decode, encode};
use nonempty::NonEmpty;
use once_cell::sync::OnceCell;
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes};

use crate::{
    AppendCache, DialogTreeError, Entry, Index, Key, KeyRef, Link, NodeBody, Rank, Segment, Value,
    ValueRef, distribution,
};

#[derive(IntoBytes, Immutable, TryFromBytes, KnownLayout, Debug)]
#[repr(u8)]
pub enum NodeType {
    Branch = 0,
    Segment = 1,
}

impl NodeType {
    pub fn is_branch(&self) -> bool {
        match self {
            NodeType::Branch => true,
            _ => false,
        }
    }

    pub fn is_segment(&self) -> bool {
        match self {
            NodeType::Segment => true,
            _ => false,
        }
    }
}

impl From<NodeType> for u8 {
    fn from(value: NodeType) -> Self {
        match value {
            NodeType::Branch => 0,
            NodeType::Segment => 1,
        }
    }
}

// pub trait Rankable {
//     fn rank(&self) -> Rank;
// }

// pub trait Distributables
// where
//     Node: From<Self::Body>,
// {
//     type Element: Rankable;
//     type Body;
// }

// pub struct Distributables<D, B> {
//     elements: NonEmpty<D>,
//     body_marker: PhantomData<B>,
// }

// impl<D, B> Distributables<D, B>
// where
//     B: From<NonEmpty<D>>,
// {
//     fn into_body(self) -> B {
//         B::from(self.elements)
//     }
// }

pub enum NodeMutation<T> {
    Upsert(T),
    Remove(T),
}

#[derive(Clone, Debug)]
pub struct Node<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: self::ValueRef<'a, Value>,
{
    buffer: Bytes,
    body: OnceCell<NodeBody<'a, Key, Value>>,
    hash: OnceCell<Blake3Hash>,
    dirty: bool,
}

impl<'a, Key, Value> Node<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: self::ValueRef<'a, Value>,
{
    pub fn buffer(&self) -> &Bytes {
        &self.buffer
    }

    pub fn segment(segment: Segment<'a, Key, Value>) -> Result<Self, DialogTreeError> {
        let body = NodeBody::Segment(segment);
        let buffer = Self::encode_body(&body)?;

        Ok(Self::with_buffer(buffer, true))
    }

    pub fn segment_with_entry(entry: Entry<'a, Key, Value>) -> Result<Self, DialogTreeError> {
        let body = NodeBody::Segment(Segment::new(entry));
        let buffer = Self::encode_body(&body)?;

        Ok(Self {
            buffer,
            body: OnceCell::with_value(body),
            hash: OnceCell::new(),
            dirty: true,
        })
    }

    pub fn index(index: Index<'a, Key>) -> Result<Self, DialogTreeError> {
        let body = NodeBody::Index {
            index,
            child_cache: AppendCache::new(),
        };
        let buffer = Self::encode_body(&body)?;

        Ok(Self::with_buffer(buffer, true))
    }

    pub fn index_with_link(link: Link<'a, Key>) -> Result<Self, DialogTreeError> {
        let body = NodeBody::Index {
            index: Index::new(link),
            child_cache: AppendCache::new(),
        };
        let buffer = Self::encode_body(&body)?;

        Ok(Self {
            buffer,
            body: OnceCell::with_value(body),
            hash: OnceCell::new(),
            dirty: true,
        })
    }

    pub fn with_buffer(buffer: Bytes, dirty: bool) -> Self {
        Self {
            buffer,
            body: OnceCell::new(),
            hash: OnceCell::new(),
            dirty,
        }
    }

    pub fn is_index(&'a self) -> Result<bool, DialogTreeError> {
        Ok(self.body()?.is_index())
    }

    pub fn is_segment(&'a self) -> Result<bool, DialogTreeError> {
        Ok(self.body()?.is_segment())
    }

    pub fn is_dirty(&'a self) -> bool {
        self.dirty
    }

    pub fn body(&'a self) -> Result<&'a NodeBody<'a, Key, Value>, DialogTreeError> {
        self.body
            .get_or_try_init(|| Self::decode_body(&self.buffer))
    }

    pub fn mutate_index(
        &'a self,
        mutation: NodeMutation<Link<'a, Key>>,
    ) -> Result<Option<Self>, DialogTreeError> {
        let body = self.body()?;

        let next_node = match (body, mutation) {
            (NodeBody::Index { index, .. }, NodeMutation::Upsert(link)) => Some(Node {
                buffer: Self::encode_body(&NodeBody::Index {
                    index: index.upsert(link)?,
                    child_cache: AppendCache::new(),
                })?,
                dirty: true,
                body: OnceCell::new(),
                hash: OnceCell::new(),
            }),
            (NodeBody::Index { index, .. }, NodeMutation::Remove(link)) => {
                if let Some(result) = index.remove(link.upper_bound()).map(|index| {
                    Self::encode_body(&NodeBody::Index {
                        index,
                        child_cache: AppendCache::new(),
                    })
                    .map(|buffer| Node {
                        buffer,
                        dirty: true,
                        body: OnceCell::new(),
                        hash: OnceCell::new(),
                    })
                }) {
                    Some(result?)
                } else {
                    None
                }
            }
            _ => {
                return Err(DialogTreeError::Node(format!(
                    "Attempt to mutate segment as index"
                )));
            }
        };

        Ok(next_node)
    }

    pub fn mutate_segment<'b, KeyB, ValueB>(
        &'a self,
        mutation: NodeMutation<Entry<'a, Key, Value>>,
    ) -> Result<Option<Node<'b, KeyB, ValueB>>, DialogTreeError>
    where
        KeyB: self::Key<'b>,
        KeyB::Ref: self::KeyRef<'b, KeyB>,
        ValueB: self::Value<'b>,
        ValueB::Ref: self::ValueRef<'b, ValueB>,
    {
        let body = self.body()?;

        let next_node = match (body, mutation) {
            (NodeBody::Segment(segment), NodeMutation::Upsert(entry)) => Some(Node {
                buffer: Self::encode_body(&NodeBody::Segment(segment.upsert(entry)?))?,
                dirty: true,
                body: OnceCell::new(),
                hash: OnceCell::new(),
            }),

            (NodeBody::Segment(segment), NodeMutation::Remove(entry)) => {
                if let Some(result) = segment.remove(entry.key()).map(|segment| {
                    Self::encode_body(&NodeBody::Segment(segment)).map(|buffer| Node {
                        buffer,
                        dirty: true,
                        body: OnceCell::new(),
                        hash: OnceCell::new(),
                    })
                }) {
                    Some(result?)
                } else {
                    None
                }
            }

            _ => {
                return Err(DialogTreeError::Node(format!(
                    "Attempt to mutate segment as index"
                )));
            }
        };

        Ok(next_node)
    }

    pub fn redistribute_entries(
        entries: NonEmpty<&'a BufOrRef<'a, Entry<'a, Key, Value>>>,
        minimum_rank: Rank,
    ) -> Result<NonEmpty<(Node<'a, Key, Value>, Rank)>, DialogTreeError> {
        let mut output: Vec<(Self, Rank)> = Vec::new();
        let mut pending = Vec::new();

        for entry in entries {
            let entry = entry.to_ref();
            let rank = distribution::geometric::rank(entry.key().hash());
            pending.push(BufOrRef::Ref(entry));

            if rank > minimum_rank {
                let segment =
                    Segment::from(NonEmpty::from_vec(std::mem::take(&mut pending)).ok_or(
                        DialogTreeError::Node("Cannot adopt an empty child list".into()),
                    )?);
                let node = Self::segment(segment)?;
                output.push((node, rank));
            }
        }

        if let Some(pending) = NonEmpty::from_vec(pending) {
            let final_node = Self::segment(Segment::from(pending))?;
            output.push((final_node, minimum_rank));
        }

        NonEmpty::from_vec(output).ok_or_else(|| {
            DialogTreeError::Node(format!("Result of redistribution was an empty node list"))
        })
    }

    pub fn redistribute_links(
        links: NonEmpty<&'a BufOrRef<'a, Link<'a, Key>>>,
        minimum_rank: Rank,
    ) -> Result<NonEmpty<(Node<'a, Key, Value>, Rank)>, DialogTreeError> {
        let mut output: Vec<(Self, Rank)> = Vec::new();
        let mut pending = Vec::new();

        for link in links {
            let link = link.to_ref();
            let rank = distribution::geometric::rank(link.upper_bound().hash());
            pending.push(BufOrRef::Ref(link));

            if rank > minimum_rank {
                let index = Index::from(NonEmpty::from_vec(std::mem::take(&mut pending)).ok_or(
                    DialogTreeError::Node("Cannot adopt an empty child list".into()),
                )?);
                let node = Self::index(index)?;
                output.push((node, rank));
            }
        }

        if let Some(pending) = NonEmpty::from_vec(pending) {
            let final_node = Self::index(Index::from(pending))?;
            output.push((final_node, minimum_rank));
        }

        NonEmpty::from_vec(output).ok_or_else(|| {
            DialogTreeError::Node(format!("Result of redistribution was an empty node list"))
        })
    }

    // pub fn redistribute(&'a self) -> Result<(Self,NonEmpty<Self>), DialogTreeError> {
    // }

    fn encode_body(body: &NodeBody<'a, Key, Value>) -> Result<Bytes, DialogTreeError> {
        let mut next_buffer = vec![];
        match &body {
            NodeBody::Index { index: branch, .. } => {
                next_buffer.push(u8::from(NodeType::Branch));
                encode(branch, &mut next_buffer)?;
            }
            NodeBody::Segment(segment) => {
                next_buffer.push(u8::from(NodeType::Segment));
                encode(segment, &mut next_buffer)?;
            }
        }
        Ok(Bytes::from(next_buffer))
    }

    fn decode_body(buffer: &'a Bytes) -> Result<NodeBody<'a, Key, Value>, DialogTreeError> {
        match NodeType::try_ref_from_bytes(&buffer[0..1]) {
            Ok(NodeType::Branch) => {
                let branch = decode::<'a, _, _>(&buffer[1..])?;
                Ok(NodeBody::Index {
                    index: branch,
                    child_cache: AppendCache::new(),
                })
            }
            Ok(NodeType::Segment) => {
                let segment = decode::<'a, _, _>(&buffer[1..])?;
                Ok(NodeBody::Segment(segment))
            }
            Err(error) => {
                return Err(DialogTreeError::Node(format!(
                    "Could not determine node type: {}",
                    error
                )));
            }
        }
    }
}

// fn decode_body<'a, Key, Value>(buffer: &'a Bytes) -> Result<NodeBody<'a, Key, Value>, DialogTreeError> where Key:{
//     match NodeType::try_ref_from_bytes(&buffer[0..1]) {
//         Ok(NodeType::Branch) => {
//             let branch = decode::<'a, _, _>(&buffer[1..])?;
//             Ok(NodeBody::Index {
//                 index: branch,
//                 child_cache: AppendCache::new(),
//             })
//         }
//         Ok(NodeType::Segment) => {
//             let segment = decode::<'a, _, _>(&buffer[1..])?;
//             Ok(NodeBody::Segment(segment))
//         }
//         Err(error) => {
//             return Err(DialogTreeError::Node(format!(
//                 "Could not determine node type: {}",
//                 error
//             )));
//         }
//     }
// }

impl<'a, Key, Value> Blake3Hashed for Node<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: self::ValueRef<'a, Value>,
{
    fn hash(&self) -> &Blake3Hash {
        self.hash.get_or_init(|| Blake3Hash::hash(&self.buffer))
    }
}

impl<'a, Key, Value> Eq for Node<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: self::ValueRef<'a, Value>,
{
}

impl<'a, Key, Value> PartialEq for Node<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: self::ValueRef<'a, Value>,
{
    fn eq(&self, other: &Self) -> bool {
        self.buffer == other.buffer
    }
}

impl<'a, Key, Value> PartialOrd for Node<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: self::ValueRef<'a, Value>,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.buffer.partial_cmp(&other.buffer)
    }
}
impl<'a, Key, Value> Ord for Node<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: self::ValueRef<'a, Value>,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.buffer.cmp(&other.buffer)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use dialog_common::{Blake3Hash, Blake3Hashed};
    use dialog_encoding::{Buf, BufOrRef, Cellular, DialogEncodingError, Ref, Width};
    use futures_util::stream::Once;
    use once_cell::sync::OnceCell;

    use crate::{Entry, Key, KeyRef, NodeBody, NodeMutation, Value, ValueRef};

    use super::Node;

    #[derive(Clone, Debug)]
    pub struct TestKey([u8; 32], OnceCell<Blake3Hash>);

    impl TestKey {
        pub fn new(value: [u8; 32]) -> Self {
            Self(value, OnceCell::new())
        }
    }

    impl PartialEq for TestKey {
        fn eq(&self, other: &Self) -> bool {
            self.0 == other.0
        }
    }

    impl PartialOrd for TestKey {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            self.0.partial_cmp(&other.0)
        }
    }

    impl Eq for TestKey {}

    impl Ord for TestKey {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.0.cmp(&other.0)
        }
    }

    impl<'a> Key<'a> for TestKey {}

    impl Blake3Hashed for TestKey {
        fn hash(&self) -> &Blake3Hash {
            self.1.get_or_init(|| Blake3Hash::hash(&self.0))
        }
    }

    impl<'a> Buf<'a> for TestKey {
        type Ref = TestKeyRef<'a>;

        fn to_ref(&'a self) -> Self::Ref {
            TestKeyRef(self.0.as_ref(), OnceCell::new())
        }
    }

    impl<'a> Cellular<'a> for TestKey {
        fn cell_width() -> Width {
            Width::Bounded(1)
        }

        fn cells(&self) -> impl Iterator<Item = &[u8]> {
            std::iter::once(self.0.as_ref())
        }

        fn try_from_cells<I>(cells: &mut I) -> std::result::Result<Self, DialogEncodingError>
        where
            I: Iterator<Item = &'a [u8]>,
        {
            let Some(cell) = cells.next() else {
                panic!("Missing cell!");
            };

            Ok(Self(cell.try_into().unwrap(), OnceCell::new()))
        }
    }

    #[derive(Clone, Debug)]
    pub struct TestKeyRef<'a>(&'a [u8], OnceCell<Blake3Hash>);

    impl<'a> KeyRef<'a, TestKey> for TestKeyRef<'a> {}

    impl<'a> PartialEq for TestKeyRef<'a> {
        fn eq(&self, other: &Self) -> bool {
            self.0 == other.0
        }
    }

    impl<'a> PartialOrd for TestKeyRef<'a> {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            self.0.partial_cmp(other.0)
        }
    }

    impl<'a> Eq for TestKeyRef<'a> {}

    impl<'a> Ord for TestKeyRef<'a> {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.0.cmp(other.0)
        }
    }

    impl<'a> Blake3Hashed for TestKeyRef<'a> {
        fn hash(&self) -> &Blake3Hash {
            self.1.get_or_init(|| Blake3Hash::hash(&self.0))
        }
    }

    impl<'a> PartialEq<TestKey> for TestKeyRef<'a> {
        fn eq(&self, other: &TestKey) -> bool {
            self.0.eq(other.0.as_ref())
        }
    }

    impl<'a> PartialOrd<TestKey> for TestKeyRef<'a> {
        fn partial_cmp(&self, other: &TestKey) -> Option<std::cmp::Ordering> {
            self.0.partial_cmp(other.0.as_ref())
        }
    }

    impl<'a> Ref<'a, TestKey> for TestKeyRef<'a> {
        fn to_buf(&self) -> TestKey {
            let mut value = [0u8; 32];
            if value.len() == 32 {
                value.copy_from_slice(self.0);
            }
            TestKey(value, self.1.to_owned())
        }
    }

    impl<'a> Cellular<'a> for TestKeyRef<'a> {
        fn cell_width() -> Width {
            Width::Bounded(1)
        }

        fn cells(&self) -> impl Iterator<Item = &[u8]> {
            std::iter::once(self.0)
        }

        fn try_from_cells<I>(cells: &mut I) -> std::result::Result<Self, DialogEncodingError>
        where
            I: Iterator<Item = &'a [u8]>,
        {
            let Some(cell) = cells.next() else {
                panic!("Missing cell!");
            };

            Ok(Self(cell, OnceCell::new()))
        }
    }

    #[derive(Clone, Debug)]
    pub struct TestValue(Vec<u8>, Blake3Hash);

    impl TestValue {
        pub fn new(value: Vec<u8>) -> Self {
            let hash = Blake3Hash::hash(&value);
            Self(value, hash)
        }
    }

    impl Blake3Hashed for TestValue {
        fn hash(&self) -> &dialog_common::Blake3Hash {
            &self.1
        }
    }

    impl<'a> Value<'a> for TestValue {}

    impl<'a> Buf<'a> for TestValue {
        type Ref = TestValueRef<'a>;

        fn to_ref(&'a self) -> Self::Ref {
            TestValueRef::new(self.0.as_ref())
        }
    }

    impl<'a> Cellular<'a> for TestValue {
        fn cell_width() -> Width {
            Width::Bounded(1)
        }

        fn cells(&self) -> impl Iterator<Item = &[u8]> {
            std::iter::once(self.0.as_ref())
        }

        fn try_from_cells<I>(cells: &mut I) -> std::result::Result<Self, DialogEncodingError>
        where
            I: Iterator<Item = &'a [u8]>,
        {
            let Some(cell) = cells.next() else {
                panic!("Missing cell!");
            };

            Ok(Self::new(cell.to_vec()))
        }
    }

    #[derive(Clone, Debug)]
    pub struct TestValueRef<'a>(&'a [u8], Blake3Hash);

    impl<'a> TestValueRef<'a> {
        pub fn new(value: &'a [u8]) -> Self {
            let hash = Blake3Hash::hash(value);
            Self(value, hash)
        }
    }

    impl<'a> PartialEq<TestValue> for TestValueRef<'a> {
        fn eq(&self, other: &TestValue) -> bool {
            self.0.eq(other.0.as_slice())
        }
    }

    impl<'a> Blake3Hashed for TestValueRef<'a> {
        fn hash(&self) -> &dialog_common::Blake3Hash {
            &self.1
        }
    }

    impl<'a> ValueRef<'a, TestValue> for TestValueRef<'a> {}

    impl<'a> Ref<'a, TestValue> for TestValueRef<'a> {
        fn to_buf(&self) -> TestValue {
            TestValue::new(self.0.to_vec())
        }
    }

    impl<'a> Cellular<'a> for TestValueRef<'a> {
        fn cell_width() -> Width {
            Width::Bounded(1)
        }

        fn cells(&self) -> impl Iterator<Item = &[u8]> {
            std::iter::once(self.0)
        }

        fn try_from_cells<I>(cells: &mut I) -> std::result::Result<Self, DialogEncodingError>
        where
            I: Iterator<Item = &'a [u8]>,
        {
            let Some(cell) = cells.next() else {
                panic!("Missing cell!");
            };

            Ok(Self::new(cell))
        }
    }

    #[test]
    fn it_doesnt_take_forever_to_update_large_nodes() -> Result<()> {
        let entry = Entry::new(TestKey::new([0u8; 32]), TestValue::new(vec![1, 2, 3]));
        let mut node = Node::segment_with_entry(entry)?;
        // let mut aggregate = vec![Node::segment_with_entry(entry)?];
        // let mut node = &aggregate[0];
        // let mut mutated_node;

        // let node_list = (0..100000usize)
        //     .map(|i| {
        //         let next_value = i.to_le_bytes().to_vec();
        //         (
        //             node.mutate_segment(NodeMutation::Upsert(Entry::new(
        //                 TestKey::new([0u8; 32]),
        //                 TestValue::new([vec![1, 2, 3], next_value.clone()].concat()),
        //             )))
        //             .unwrap()
        //             .unwrap(),
        //             next_value,
        //         )
        //     })
        //     .map(|(node, next_value)| {
        //         node.mutate_segment::<TestKey, TestValue>(NodeMutation::Upsert(Entry::new(
        //             TestKey::new(rand::random()),
        //             TestValue::new(next_value.clone()),
        //         )))
        //         .unwrap()
        //         .unwrap()
        //     })
        //     .collect::<Vec<_>>();

        for i in 0..1000usize {
            let next_value = i.to_le_bytes().to_vec();

            let mutated_node = node
                .mutate_segment(NodeMutation::Upsert(Entry::new(
                    TestKey::new([0u8; 32]),
                    TestValue::new([vec![1, 2, 3], next_value.clone()].concat()),
                )))?
                .unwrap();

            for _ in 0..100usize {
                node.mutate_segment::<'_, TestKey, TestValue>(NodeMutation::Upsert(Entry::new(
                    TestKey::new([0u8; 32]),
                    TestValue::new([vec![1, 2, 3], next_value.clone()].concat()),
                )))?
                .unwrap();
            }

            let extended_node = mutated_node
                .mutate_segment::<'_, TestKey, TestValue>(NodeMutation::Upsert(Entry::new(
                    TestKey::new(rand::random()),
                    TestValue::new(next_value.clone()),
                )))?
                .unwrap();

            node = extended_node;

            match node.body()? {
                NodeBody::Segment(segment) => {
                    println!("Entries: {}", segment.entries().len());
                    assert_eq!(segment.entries().len(), i + 2);
                    let entry = segment.entries().get(0).unwrap();

                    match entry {
                        BufOrRef::Buf(_) => panic!("Entry should be a reference!"),
                        BufOrRef::Ref(entry) => {
                            assert_eq!(
                                *entry.value(),
                                TestValue::new([vec![1, 2, 3], next_value.clone()].concat())
                            )
                        }
                    }
                }
                _ => panic!("Wrong node body type!"),
            };

            // aggregate.push(mutated_node);
            // aggregate.push(extended_node);
        }

        Ok(())
    }

    #[test]
    fn it_mutates_a_node() -> Result<()> {
        let entry = Entry::new(TestKey::new([0u8; 32]), TestValue::new(vec![0, 1, 2]));
        let node = Node::segment_with_entry(entry)?;

        let mutated_node = node
            .mutate_segment::<'_, TestKey, TestValue>(NodeMutation::Upsert(Entry::new(
                TestKey::new([0u8; 32]),
                TestValue::new(vec![1, 2, 3]),
            )))?
            .unwrap();

        match mutated_node.body()? {
            NodeBody::Segment(segment) => {
                assert_eq!(segment.entries().len(), 1);
                let entry = segment.entries().get(0).unwrap();

                match entry {
                    BufOrRef::Buf(_) => panic!("Entry should be a reference!"),
                    BufOrRef::Ref(entry) => {
                        assert_eq!(*entry.value(), TestValue::new(vec![1, 2, 3]))
                    }
                }
            }
            _ => panic!("Wrong node body type!"),
        };

        Ok(())
    }
}
