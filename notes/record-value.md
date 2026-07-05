# Record Value

## The Problem

Dialog models data as `{the, of, is}` triples where `is` holds an atomic value. Today `Value` covers scalars: strings, integers, floats, booleans, bytes, entities, symbols. This works when each attribute maps to a single scalar, but not all values are scalars.

Consider geolocation. A location is a latitude/longitude pair. These two numbers form a single atomic unit. Storing them as separate claims (`location/latitude`, `location/longitude`) is wrong because you can end up in a state where one is updated without the other, or where the two are inconsistent with each other. The pair needs to be written and read as one value.

The same applies to any compound value that should be treated atomically: a color (r, g, b), a bounding box (x, y, width, height), an automerge text document, a signed credential. These are not collections of independent facts. They are single values that happen to have internal structure.

`Value` already has a `Record` variant, but it is a placeholder. This note describes what it should actually be.

## Context: What Record Means

A `Record` is a value that is not a scalar but still represents a single atomic unit in the `{the, of, is}` model. It is opaque to the query layer. The query engine carries it, stores it, compares it (by bytes), but never looks inside. Only the type that implements `RecordFormat` knows how to interpret the bytes.

This is different from modeling structured data as multiple claims. Multiple claims give you independent facts with independent provenance and independent conflict resolution. A Record gives you one fact whose internal structure is the concern of its type, not of Dialog.

## Concrete Cases

**Compound atomic values.** Geolocation, color, bounding box. These are small structured values where splitting into separate claims introduces consistency problems. `GeoLocation` implements `RecordFormat` to encode `{lat, lon}` as a single `Record`.

**Rich text and collaborative types.** An automerge document is a CRDT with its own binary format and merge semantics. A text attribute backed by automerge should be a single value in one claim, not spread across multiple claims. A newtype wrapper like `TextDocument(automerge::Automerge)` implements `RecordFormat` using `Automerge::load`/`save`.

**Capturing whole claim (a.k.a artifact) in a binding.** When claim selection evaluates, it produces a `Claim` (the resolved artifact). Downstream consumers need the full `Claim`, not just its individual fields. Today we work around this with a side `claims` map, separate from the general bindings. With `Value::Record`, we could treat claims as regular bindings produced by query evaluation.

## Design: RecordFormat Trait

The type itself knows how to encode and decode. No separate format object needed.

```rust
type ErasedForm = Arc<dyn Any + ConditionalSend + ConditionalSync>;

trait RecordFormat: ConditionalSend + ConditionalSync + Sized + 'static {
    fn decode(bytes: &[u8]) -> Result<Self, RecordError>;
    fn encode(&self) -> Result<Vec<u8>, RecordError>;
}
```

### Record

`Record` is a type-erased container that holds serialized bytes and decoded forms.

```rust
#[derive(Debug, Clone)]
struct Record(Arc<RecordState>);

struct RecordState {
    source: Vec<u8>,
    forms: RwLock<HashMap<TypeId, ErasedForm>>,
}
```

`Record` always has bytes. The `forms` map is populated lazily on first `realize` call for a given type and reused on subsequent calls. The bytes live outside the lock so they are always accessible. If the lock cannot be acquired, `realize` decodes directly from bytes.

```rust
impl<F: RecordFormat> TryFrom<F> for Record {
    type Error = RecordError;
    fn try_from(form: F) -> Result<Record, RecordError> {
        let source = form.encode()?;
        let mut forms = HashMap::new();
        forms.insert(TypeId::of::<F>(), Arc::new(form) as ErasedForm);
        Ok(Record(Arc::new(RecordState {
            source,
            forms: RwLock::new(forms),
        })))
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

impl Record {
    pub fn realize<F: RecordFormat>(&self) -> Result<Arc<F>, RecordError> {
        let key = TypeId::of::<F>();

        // Try reading from forms.
        if let Ok(forms) = self.0.forms.try_read() {
            if let Some(form) = forms.get(&key) {
                return form
                    .clone()
                    .downcast::<F>()
                    .map_err(|_| RecordError::TypeMismatch);
            }
        }

        // Decode from bytes.
        let form = Arc::new(F::decode(&self.0.source)?);

        // Store the decoded form. If the lock is held, skip it.
        if let Ok(mut forms) = self.0.forms.try_write() {
            forms.insert(key, form.clone());
        }

        Ok(form)
    }
}
```

### Usage

```rust
// Write a geolocation as a record value.
let geo = GeoLocation { lat: 37.7749, lon: -122.4194 };
let record: Record = geo.try_into()?;
let value = Value::Record(record);

// Read it back.
let Value::Record(record) = claim.is() else { ... };
let geo: Arc<GeoLocation> = record.realize::<GeoLocation>()?;
```

### Trait Bounds

`Record` lives inside `Value` which requires `Clone`, `Eq`, `Hash`, `Debug`, `Serialize`, `Deserialize`.

- **Clone**: Derived. Clones the inner `Arc` (cheap reference count bump).
- **Eq/Hash**: Compare/hash the `source` bytes. Same bytes = same record.
- **Debug**: Print byte length or hex prefix.
- **Serialize/Deserialize**: Operate on `source` bytes. `Deserialize` produces `Record::from(bytes)`.

### Example Implementations

```rust
impl RecordFormat for GeoLocation {
    fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        Ok(serde_cbor::from_slice(bytes)?)
    }
    fn encode(&self) -> Result<Vec<u8>, RecordError> {
        Ok(serde_cbor::to_vec(self)?)
    }
}


struct TextDocument(automerge::Automerge);

impl RecordFormat for TextDocument {
    fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        Ok(TextDocument(automerge::Automerge::load(bytes)?))
    }
    fn encode(&self) -> Result<Vec<u8>, RecordError> {
        Ok(self.0.save())
    }
}
```

The query layer treats both identically. It carries the bytes, and the consumer who knows the type calls `realize` to get the typed object out.

## Future: Records From Storage

Today the storage layer fully deserializes every datum into an `Artifact` before the query layer sees it. Ideally, record values would arrive as raw bytes wrapped in `Record::from(bytes)`, with deserialization deferred to `realize` on demand. The prolly tree keys already encode enough information for filtering and grouping without deserializing the datum. Getting there requires changes to `ArtifactStore` and its consumers, but the `Record` type is designed with this path in mind.

## Decision

**Add `Record` as a `Value` variant with `RecordFormat` trait for self-describing encode/decode.**

### Incremental Path

1. **Now**: Create `Record` type and `RecordFormat` trait. Use `TryFrom` to create records with eager serialization. Eliminate the claims side-channel by storing claims as `Value::Record`. Self-contained, does not touch the storage layer.

2. **Later**: Have the storage layer produce `Record::from(bytes)` directly, deferring deserialization to the consumer.

### Rationale

1. **Some values are compound but still atomic.** Geolocation, color, rich text. Splitting them into separate claims creates consistency problems. Storing them as a single opaque value in one claim preserves atomicity.

2. **The query layer should not need to understand record contents.** It carries bytes. The type knows how to encode and decode itself. No separate format object to pass around.

3. **Eager serialization is acceptable as a starting point.** `TryFrom` pays one encode at construction. The path to zero-copy from storage is clear and incremental.

4. **The same trait extends to conflict resolution.** `RecordFormat` can grow a `merge` method. An automerge implementation overrides it with CRDT merge. A simple struct uses last-write-wins. The storage layer calls merge when it encounters conflicting values for the same `(entity, attribute)` pair, without knowing what is inside the record.

```rust
trait RecordFormat: ConditionalSend + ConditionalSync + Sized + 'static {
    fn decode(bytes: &[u8]) -> Result<Self, RecordError>;
    fn encode(&self) -> Result<Vec<u8>, RecordError>;

    fn merge(a: &Self, b: &Self) -> Self {
        b.clone() // default: last-write-wins
    }
}
```
