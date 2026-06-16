//! A novelty buffer over a [`PersistentTree`].
//!
//! A [`Buffered`] pairs a canonical base tree with a small, sorted buffer of
//! pending operations held at the root. A write appends to the buffer rather
//! than rebuilding the tree, so writes are cheap and recent novelty is
//! concentrated in one place: the buffer.
//!
//! This is a hitchhiker tree with the buffer only at the root. There are no
//! per-node logs and no level-by-level cascade. When the buffer fills, the whole
//! batch is flushed into the base in one canonical rebuild and the buffer drains
//! to empty.
//!
//! The base nodes are never touched by buffering, so their hashes are stable and
//! structural sharing is intact. Only the [`tree_hash`](Buffered::tree_hash)
//! moves as writes arrive, computed by streaming the sorted buffer onto the base
//! root hash.
//!
//! The buffer is sorted by key (a [`BTreeMap`]) and holds at most one op per key
//! (last writer wins, matching tree semantics). Because it is canonical in its
//! key set rather than its write order, the same set of buffered ops over the
//! same base always yields the same `tree_hash`. That is what lets two replicas
//! converge by exchanging and unioning their buffers.
//!
//! This type is generic and storage-agnostic about what the ops mean. It owns
//! the buffer's storage, hashing, and union; it does not know how to flush a
//! buffered fact into a base that materializes several index views of it, nor
//! how to merge the buffer into a read. Those are the consumer's concern: the
//! consumer iterates [`novelty`](Buffered::novelty) in sorted order and drives
//! the base [`edit`](PersistentTree::edit) / scan itself.

use std::collections::BTreeMap;
use std::marker::PhantomData;

use dialog_common::{Blake3Hash, ConditionalSync};
use rkyv::{
    Deserialize, Serialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{
    DialogSearchTreeError, Distribution, Geometric, Key, PersistentTree, SymmetryWith, Value,
};

/// A buffered operation on a key: assert a value, or retract whatever is there.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op<Value> {
    /// Assert (insert or update) this value at the key.
    Assert(Value),
    /// Retract the key.
    Retract,
}

impl<Value> Op<Value> {
    /// The one-byte tag distinguishing the variants in the hash fold.
    fn tag(&self) -> u8 {
        match self {
            Op::Assert(_) => 0,
            Op::Retract => 1,
        }
    }
}

/// A [`PersistentTree`] with a sorted novelty buffer of pending ops at its root.
pub struct Buffered<Key, Value, D = Geometric>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Value: self::Value,
    D: Distribution,
{
    base: PersistentTree<Key, Value, D>,
    novelty: BTreeMap<Key, Op<Value>>,
    distribution: PhantomData<D>,
}

impl<Key, Value, D> Buffered<Key, Value, D>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Value: self::Value,
    D: Distribution,
{
    /// Creates a buffered tree over `base` with an empty novelty buffer.
    pub fn new(base: PersistentTree<Key, Value, D>) -> Self {
        Self {
            base,
            novelty: BTreeMap::new(),
            distribution: PhantomData,
        }
    }

    /// Creates a buffered tree over `base` carrying an existing novelty buffer.
    ///
    /// Used to reconstruct a buffered tree from its durable parts (a base root
    /// and a buffer received over sync or loaded from storage).
    pub fn with_novelty(
        base: PersistentTree<Key, Value, D>,
        novelty: BTreeMap<Key, Op<Value>>,
    ) -> Self {
        Self {
            base,
            novelty,
            distribution: PhantomData,
        }
    }

    /// The canonical base tree (unaffected by buffering).
    pub fn base(&self) -> &PersistentTree<Key, Value, D> {
        &self.base
    }

    /// The sorted novelty buffer.
    ///
    /// Iterated in ascending key order, which is the order a read merge and the
    /// hash fold both consume it in.
    pub fn novelty(&self) -> &BTreeMap<Key, Op<Value>> {
        &self.novelty
    }

    /// Returns the number of buffered ops.
    pub fn len(&self) -> usize {
        self.novelty.len()
    }

    /// Returns `true` when the buffer holds no ops.
    pub fn is_empty(&self) -> bool {
        self.novelty.is_empty()
    }

    /// Buffers an op at `key`, replacing any prior op at the same key.
    ///
    /// Last writer wins: a later write to the same key supersedes the earlier
    /// one, matching the tree's one-value-per-key semantics. The buffer stays
    /// sorted by key by construction.
    pub fn write(&mut self, key: Key, op: Op<Value>) {
        self.novelty.insert(key, op);
    }

    /// Unions `other`'s ops into this buffer (sync reconciliation over a shared
    /// base), last writer wins per key with `other` taking precedence.
    ///
    /// The caller is responsible for ensuring both buffers are over the same
    /// base; reconciling buffers over diverging bases is a higher-level concern.
    pub fn union(&mut self, other: BTreeMap<Key, Op<Value>>) {
        self.novelty.extend(other);
    }

    /// Replaces the base and drains the buffer, returning the drained ops.
    ///
    /// The consumer flushes by applying these ops to the base (deriving whatever
    /// index views the base materializes), persisting it, and calling this with
    /// the resulting base so the buffered tree ends as `(new_base, empty)`.
    pub fn reset(&mut self, base: PersistentTree<Key, Value, D>) -> BTreeMap<Key, Op<Value>> {
        self.base = base;
        std::mem::take(&mut self.novelty)
    }
}

impl<Key, Value, D> Buffered<Key, Value, D>
where
    Key: self::Key
        + ConditionalSync
        + 'static
        + PartialOrd<Key::Archived>
        + PartialEq<Key::Archived>
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + ConditionalSync
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value
        + ConditionalSync
        + 'static
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
    D: Distribution,
{
    /// The tree hash: the base root hash with the sorted novelty folded in.
    ///
    /// Computed by streaming the base root bytes, then each buffered op in
    /// ascending key order, through one Blake3 hasher. Because the buffer is
    /// canonical in its key set (sorted, one op per key) the hash is a pure
    /// function of `{base, op-set}`: two buffered trees with the same base and
    /// the same buffered ops have the same `tree_hash`, regardless of the order
    /// the ops were written. An empty buffer hashes to a fold of just the base
    /// root, which is still distinct from the bare base root (the buffer's
    /// presence is part of the identity).
    pub fn tree_hash(&self) -> Result<Blake3Hash, DialogSearchTreeError> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.base.root().as_bytes());
        for (key, op) in &self.novelty {
            hasher.update(key.as_ref());
            hasher.update(&[op.tag()]);
            if let Op::Assert(value) = op {
                let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(value)
                    .map_err(|error| DialogSearchTreeError::Encoding(format!("{error}")))?;
                hasher.update(bytes.as_slice());
            }
        }
        Ok(Blake3Hash::from(*hasher.finalize().as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;

    use super::{Buffered, Op};
    use crate::PersistentTree;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    type TestTree = PersistentTree<[u8; 4], Vec<u8>>;

    fn key(n: u32) -> [u8; 4] {
        n.to_le_bytes()
    }

    #[dialog_common::test]
    async fn it_writes_and_reads_back_the_buffer() -> Result<()> {
        let mut buffered = Buffered::new(TestTree::empty());
        assert!(buffered.is_empty());

        buffered.write(key(1), Op::Assert(vec![1]));
        buffered.write(key(2), Op::Assert(vec![2]));

        assert_eq!(buffered.len(), 2);
        assert_eq!(buffered.novelty().get(&key(1)), Some(&Op::Assert(vec![1])));
        assert_eq!(buffered.novelty().get(&key(2)), Some(&Op::Assert(vec![2])));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_collapses_repeated_writes_to_the_same_key() -> Result<()> {
        let mut buffered = Buffered::new(TestTree::empty());

        buffered.write(key(1), Op::Assert(vec![1]));
        buffered.write(key(1), Op::Assert(vec![99]));
        buffered.write(key(1), Op::Retract);

        // Last writer wins: one op per key.
        assert_eq!(buffered.len(), 1);
        assert_eq!(buffered.novelty().get(&key(1)), Some(&Op::Retract));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_hashes_independent_of_write_order() -> Result<()> {
        // The tree hash must be a pure function of {base, op-set}, so two
        // buffers with the same ops written in different orders agree.
        let mut a = Buffered::new(TestTree::empty());
        a.write(key(1), Op::Assert(vec![1]));
        a.write(key(2), Op::Assert(vec![2]));
        a.write(key(3), Op::Retract);

        let mut b = Buffered::new(TestTree::empty());
        b.write(key(3), Op::Retract);
        b.write(key(2), Op::Assert(vec![2]));
        b.write(key(1), Op::Assert(vec![1]));

        assert_eq!(a.tree_hash()?, b.tree_hash()?);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_hashes_differently_for_different_op_sets() -> Result<()> {
        let empty = Buffered::new(TestTree::empty());

        let mut one = Buffered::new(TestTree::empty());
        one.write(key(1), Op::Assert(vec![1]));

        let mut other_value = Buffered::new(TestTree::empty());
        other_value.write(key(1), Op::Assert(vec![2]));

        let mut retract = Buffered::new(TestTree::empty());
        retract.write(key(1), Op::Retract);

        // Empty buffer, an assert, a different-valued assert, and a retract
        // at the same key are all distinct identities.
        let hashes = [
            empty.tree_hash()?,
            one.tree_hash()?,
            other_value.tree_hash()?,
            retract.tree_hash()?,
        ];
        for i in 0..hashes.len() {
            for j in (i + 1)..hashes.len() {
                assert_ne!(hashes[i], hashes[j], "hashes {i} and {j} must differ");
            }
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn it_unions_buffers_with_other_winning() -> Result<()> {
        let mut a = Buffered::new(TestTree::empty());
        a.write(key(1), Op::Assert(vec![1]));
        a.write(key(2), Op::Assert(vec![2]));

        let mut b = Buffered::new(TestTree::empty());
        b.write(key(2), Op::Retract); // collides with a's key 2
        b.write(key(3), Op::Assert(vec![3]));

        a.union(b.novelty().clone());

        assert_eq!(a.len(), 3);
        assert_eq!(a.novelty().get(&key(1)), Some(&Op::Assert(vec![1])));
        assert_eq!(a.novelty().get(&key(2)), Some(&Op::Retract)); // other won
        assert_eq!(a.novelty().get(&key(3)), Some(&Op::Assert(vec![3])));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_drains_the_buffer_on_reset() -> Result<()> {
        let mut buffered = Buffered::new(TestTree::empty());
        buffered.write(key(1), Op::Assert(vec![1]));
        buffered.write(key(2), Op::Assert(vec![2]));

        let drained = buffered.reset(TestTree::empty());

        assert_eq!(drained.len(), 2);
        assert!(buffered.is_empty());
        // After reset the tree hash is the fold of just the (new) base.
        assert_eq!(
            buffered.tree_hash()?,
            Buffered::new(TestTree::empty()).tree_hash()?
        );

        Ok(())
    }
}
