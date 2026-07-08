//! Record values: opaque, self-describing atomic values.
//!
//! A [`Record`] is a [`Value`] that is not a scalar but still represents a
//! single atomic unit in the `{the, of, is}` model. It is opaque to the query
//! layer: the engine carries it, stores it, and compares it by bytes, but never
//! looks inside. Only a type that implements [`RecordFormat`] knows how to
//! interpret the bytes.
//!
//! See [`notes/record-value.md`](https://github.com/dialog-db/dialog-db/blob/main/notes/record-value.md)
//! for the design rationale.
//!
//! [`Value`]: crate::Value

use std::{
    any::{Any, TypeId},
    cmp::Ordering,
    collections::HashMap,
    fmt::{Debug, Formatter, Result as FmtResult},
    hash::{Hash, Hasher},
    sync::{Arc, RwLock},
};

use dialog_common::{ConditionalSend, ConditionalSync};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// Errors that may occur while encoding, decoding, or realizing a [`Record`].
#[derive(Debug, Error, PartialEq)]
pub enum RecordError {
    /// The record bytes could not be decoded into the requested format.
    #[error("Failed to decode record value: {0}")]
    Decode(String),

    /// The format value could not be encoded into record bytes.
    #[error("Failed to encode record value: {0}")]
    Encode(String),

    /// A cached form was realized as a different format than was requested.
    #[error("Record realized as the wrong format type")]
    TypeMismatch,
}

/// A self-describing encoding for a [`Record`] value.
///
/// The type itself knows how to turn its bytes into a value and back. No
/// separate format object is needed. The query layer treats every
/// `RecordFormat` identically — it only ever handles the encoded bytes — and
/// the consumer that knows the concrete type calls [`Record::realize`] to
/// recover the typed value.
pub trait RecordFormat: ConditionalSend + ConditionalSync + Sized + Clone + 'static {
    /// Decode a value of this format from its byte representation.
    fn decode(bytes: &[u8]) -> Result<Self, RecordError>;

    /// Encode this value into its byte representation.
    fn encode(&self) -> Result<Vec<u8>, RecordError>;

    /// Merge two concurrent values of this format into one.
    ///
    /// The default is last-write-wins (`b` wins). Formats with richer
    /// conflict-resolution semantics — an automerge CRDT document, for
    /// example — override this to merge both sides.
    fn merge(a: &Self, b: &Self) -> Self {
        let _ = a;
        b.clone()
    }
}

/// A decoded form cached inside a [`Record`], erased to [`Any`] so that a
/// single record can memoize forms of several concrete types.
///
/// The boxed value is always an `Arc<F>`, so retrieval is a
/// [`Any::downcast_ref`] to `&Arc<F>` followed by a cheap `Arc` clone — which
/// preserves the memoized allocation across calls. We box `Arc<F>` (rather than
/// erase the `Arc` itself) because `Arc<dyn Any>::downcast` is not available on
/// all targets, whereas `<dyn Any>::downcast_ref` is.
///
/// On native targets the cache is shared across threads, so the erased form
/// must be `Send + Sync`; on `wasm32` neither bound applies. This mirrors the
/// `ConditionalSend`/`ConditionalSync` bounds of [`RecordFormat`] using the
/// actual auto traits.
#[cfg(not(target_arch = "wasm32"))]
type ErasedForm = Box<dyn Any + Send + Sync>;
#[cfg(target_arch = "wasm32")]
type ErasedForm = Box<dyn Any>;

/// The shared interior of a [`Record`].
///
/// `source` is the canonical byte representation and always exists. `forms`
/// memoizes decoded values keyed by their [`TypeId`], populated lazily on the
/// first [`Record::realize`] for a given type and reused thereafter. The bytes
/// live outside the lock so they remain accessible even while the cache is
/// being written; if the lock cannot be acquired, `realize` simply decodes
/// directly from the bytes.
struct RecordState {
    source: Vec<u8>,
    forms: RwLock<HashMap<TypeId, ErasedForm>>,
}

/// An opaque, atomic value carried by the query layer as bytes.
///
/// A `Record` always holds its `source` bytes and lazily memoizes decoded
/// [`RecordFormat`] forms. Cloning is cheap: it bumps a reference count.
/// Equality, ordering, hashing, and serialization all operate on the `source`
/// bytes, so two records with identical bytes are identical in every respect.
#[derive(Clone)]
pub struct Record(Arc<RecordState>);

impl Record {
    /// Eagerly encode a [`RecordFormat`] value into a record, memoizing the
    /// value so a subsequent [`realize`](Record::realize) of the same type is
    /// free.
    ///
    /// This is the write-side constructor. It cannot be a blanket
    /// `TryFrom<F>` impl: that would overlap the standard-library
    /// `TryFrom<U> for T where U: Into<T>` blanket (which already covers
    /// `From<Vec<u8>>`), so it is an inherent function instead.
    pub fn from_format<F: RecordFormat>(form: F) -> Result<Record, RecordError> {
        let source = form.encode()?;
        let mut forms = HashMap::new();
        let erased: ErasedForm = Box::new(Arc::new(form));
        forms.insert(TypeId::of::<F>(), erased);
        Ok(Record(Arc::new(RecordState {
            source,
            forms: RwLock::new(forms),
        })))
    }

    /// The canonical byte representation of this record.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0.source
    }

    /// Decode (or recover from cache) this record as a concrete
    /// [`RecordFormat`].
    ///
    /// The first call for a given format decodes from bytes and memoizes the
    /// result; subsequent calls return the cached form. If the cache lock
    /// cannot be acquired, the record is decoded directly from bytes without
    /// caching.
    pub fn realize<F: RecordFormat>(&self) -> Result<Arc<F>, RecordError> {
        let key = TypeId::of::<F>();

        // Try reading a memoized form. The value stored under this key is
        // always an `Arc<F>`, so the downcast succeeds; `TypeMismatch` guards
        // the impossible case rather than papering over a real one.
        if let Ok(forms) = self.0.forms.try_read()
            && let Some(form) = forms.get(&key)
        {
            return form
                .downcast_ref::<Arc<F>>()
                .cloned()
                .ok_or(RecordError::TypeMismatch);
        }

        // Decode from bytes.
        let form = Arc::new(F::decode(&self.0.source)?);

        // Memoize the decoded form. If the lock is held, skip caching.
        if let Ok(mut forms) = self.0.forms.try_write() {
            let erased: ErasedForm = Box::new(form.clone());
            forms.insert(key, erased);
        }

        Ok(form)
    }
}

impl From<Vec<u8>> for Record {
    fn from(source: Vec<u8>) -> Record {
        Record(Arc::new(RecordState {
            source,
            forms: RwLock::new(HashMap::new()),
        }))
    }
}

impl Debug for Record {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("Record")
            .field(&format_args!("{} bytes", self.0.source.len()))
            .finish()
    }
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.0.source == other.0.source
    }
}

impl Eq for Record {}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Record {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.source.cmp(&other.0.source)
    }
}

impl Hash for Record {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.source.hash(state);
    }
}

impl Serialize for Record {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.source.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Record {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Record::from(Vec::<u8>::deserialize(deserializer)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A toy format that stores a sorted, deduplicated set of bytes and merges
    /// by union — enough to exercise the trait, the cache, and `merge` without
    /// pulling in a CRDT dependency.
    #[derive(Debug, Clone, PartialEq)]
    struct ByteSet(Vec<u8>);

    impl RecordFormat for ByteSet {
        fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
            Ok(ByteSet(bytes.to_vec()))
        }

        fn encode(&self) -> Result<Vec<u8>, RecordError> {
            Ok(self.0.clone())
        }

        fn merge(a: &Self, b: &Self) -> Self {
            let mut merged = a.0.clone();
            merged.extend_from_slice(&b.0);
            merged.sort_unstable();
            merged.dedup();
            ByteSet(merged)
        }
    }

    #[test]
    fn it_round_trips_through_a_format() {
        let form = ByteSet(vec![3, 1, 2]);
        let record = Record::from_format(form.clone()).unwrap();
        assert_eq!(record.as_bytes(), &[3, 1, 2]);

        let realized = record.realize::<ByteSet>().unwrap();
        assert_eq!(*realized, form);
    }

    #[test]
    fn it_hydrates_opaque_bytes_lazily() {
        let record = Record::from(vec![10, 20, 30]);
        assert_eq!(record.as_bytes(), &[10, 20, 30]);

        let realized = record.realize::<ByteSet>().unwrap();
        assert_eq!(realized.0, vec![10, 20, 30]);
    }

    #[test]
    fn it_memoizes_the_decoded_form() {
        let record = Record::from(vec![1, 2, 3]);
        let first = record.realize::<ByteSet>().unwrap();
        let second = record.realize::<ByteSet>().unwrap();
        // The memoized form is returned as the very same allocation.
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn it_compares_and_hashes_by_bytes() {
        use std::collections::hash_map::DefaultHasher;

        let a = Record::from(vec![1, 2, 3]);
        let b = Record::from_format(ByteSet(vec![1, 2, 3])).unwrap();
        let c = Record::from(vec![9, 9]);

        assert_eq!(a, b);
        assert_ne!(a, c);

        fn digest(record: &Record) -> u64 {
            let mut hasher = DefaultHasher::new();
            record.hash(&mut hasher);
            hasher.finish()
        }
        assert_eq!(digest(&a), digest(&b));
    }

    #[test]
    fn it_merges_via_the_format() {
        let a = ByteSet(vec![1, 2]);
        let b = ByteSet(vec![2, 3]);
        assert_eq!(ByteSet::merge(&a, &b), ByteSet(vec![1, 2, 3]));
    }

    #[test]
    fn default_merge_is_last_write_wins() {
        #[derive(Debug, Clone, PartialEq)]
        struct Lww(Vec<u8>);
        impl RecordFormat for Lww {
            fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
                Ok(Lww(bytes.to_vec()))
            }
            fn encode(&self) -> Result<Vec<u8>, RecordError> {
                Ok(self.0.clone())
            }
        }
        let a = Lww(vec![1]);
        let b = Lww(vec![2]);
        assert_eq!(Lww::merge(&a, &b), b);
    }
}
