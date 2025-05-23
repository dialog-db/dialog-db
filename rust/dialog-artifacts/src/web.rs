//! Web bindings for the `dialog-artifacts` crate.
//!
//! Example usage in JavaScript:
//!
//! ```ignore
//! // This is JavaScript code that uses the WASM bindings, not a Rust doctest
//! import { Artifacts, generateEntity, InstructionType, ValueDataType } from "dialog-artifacts";
//!
//! let artifacts = await Artifacts.open("test");
//!
//! await artifacts.commit([{
//!     type: InstructionType.Assert,
//!     artifact: {
//!         the: "profile/name",
//!         of: generateEntity(),
//!         is: {
//!             type: ValueDataType.String,
//!             value: "Foo Bar"
//!         }
//!     }
//! }]);
//! let query = artifacts.select({
//!     the: "profile/name"
//! });
//!
//! for await (const element of query) {
//!    console.log('The', element.the, 'of', element.of, 'is', element.is);
//! }
//! ```

use std::{pin::Pin, sync::Arc};

use base58::{FromBase58, ToBase58};
use dialog_storage::{
    Blake3Hash, IndexedDbStorageBackend, StorageCache, web::ObjectSafeStorageBackend,
};
use futures_util::{Stream, StreamExt};
use rand::{Rng, distr::Alphanumeric};
use tokio::sync::{Mutex, RwLock};
use wasm_bindgen::{convert::TryFromJsValue, prelude::*};
use wasm_bindgen_futures::js_sys::{self, Object, Reflect, Symbol, Uint8Array};

use crate::{
    Artifact, ArtifactSelector, ArtifactStore, ArtifactStoreMutExt, Artifacts, Attribute, Cause,
    DialogArtifactsError, Entity, HASH_SIZE, Instruction, RawEntity, Value, ValueDataType,
    artifacts::selector::Constrained,
};

#[wasm_bindgen(typescript_custom_section)]
const ARTIFACT_INTERFACE: &'static str = r#"
/**
 * The predicate of a semantic triple
 */
type Attribute = string;

/**
 * The subject of a semantic triple; it must be exactly 32-bytes long.
 * A valid, unique `Entity` can be created using `generateEntity`.
 */
type Entity = Uint8Array;

/**
 * The object of a semantic triple. It's internal representation will
 * vary based on the value of the `type` property. For more details,
 * see the documentation on `ValueDataType`.
 */
interface Value {
  type: ValueDataType,
  value: null|Uint8Array|string|boolean|number
}

/**
 * A causal reference to an earlier version of an `Artifact`
 */
type Cause = Uint8Array;

/**
 * An `Artifact` embodies a datum - a semantic triple - that may be stored in or
 * retrieved from `Artifacts`.
 */
interface Artifact {
  the: Attribute,
  of: Entity,
  is: Value,
  cause?: Cause
}

interface ArtifactApi {
  update(value: Value): (Artifact & ArtifactApi)|void;
}

/**
 * The instruction variants that are accepted by `Artifacts.commit`.
 */
interface Instruction {
  type: InstructionType,
  artifact: Artifact
}

/**
 * The shape of the "iterable" that is expected by `Artifacts.commit`
 */
type InstructionIterable = Iterable<Instruction>;

/**
 * A basic filter that can be used to query `Artifacts`
 */
interface ArtifactSelector {
  the?: Attribute,
  of?: Entity,
  is?: Value
}

/**
 * The shape of the "async iterable" that is returned by `Artifacts.select`
 */
type ArtifactIterable = AsyncIterable<Artifact & ArtifactApi>;
"#;

#[wasm_bindgen]
extern "C" {
    #[allow(missing_docs)]
    #[wasm_bindgen(typescript_type = "InstructionIterable")]
    pub type InstructionIterableDuckType;

    #[allow(missing_docs)]
    #[wasm_bindgen(typescript_type = "ArtifactSelector")]
    pub type ArtifactSelectorDuckType;

    #[allow(missing_docs)]
    #[wasm_bindgen(typescript_type = "Artifact")]
    pub type ArtifactDuckType;

    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

impl From<DialogArtifactsError> for JsValue {
    fn from(value: DialogArtifactsError) -> Self {
        format!("{value}").into()
    }
}

/// Generate the BLAKE3 hash of some input bytes
#[wasm_bindgen(js_name = "makeReference")]
pub fn make_reference(bytes: Vec<u8>) -> Vec<u8> {
    crate::make_reference(bytes).to_vec()
}

/// Convert the input bytes to a string using base58 encoding
#[wasm_bindgen]
pub fn encode(bytes: Vec<u8>) -> String {
    bytes.to_base58()
}

/// Decode base58-encoded bytes from a string
#[wasm_bindgen]
pub fn decode(encoded: String) -> Result<Vec<u8>, JsValue> {
    Ok(encoded
        .from_base58()
        .map_err(|error| format!("Decoding failed: {:?}", error))?)
}

/// Generate a unique, valid `Entity`
#[wasm_bindgen(js_name = "generateEntity")]
pub fn generate_entity() -> Vec<u8> {
    RawEntity::from(Entity::new()).to_vec()
}

/// Used to specify if an `Instruction` is an assertion or a retraction
#[repr(u8)]
#[wasm_bindgen(js_name = "InstructionType")]
pub enum InstructionTypeBinding {
    /// The `Instruction` is an assertion
    Assert = 0,
    /// The `Instruction` is a retraction
    Retract = 1,
}

type WebStorageBackend = Arc<Mutex<dyn ObjectSafeStorageBackend>>;

const STORAGE_CACHE_CAPACITY: usize = 2usize.pow(16);

/// A triple store that can be used to store and retrieve semantic triples
/// in the form of `Artifact`s.
#[wasm_bindgen(js_name = "Artifacts")]
pub struct ArtifactsBinding {
    artifacts: Arc<RwLock<Artifacts<WebStorageBackend>>>,
}

#[wasm_bindgen(js_class = "Artifacts")]
impl ArtifactsBinding {
    /// Initialize a new, empty [`Artifacts`] with a randomly generated
    /// identifier
    #[wasm_bindgen]
    pub async fn anonymous() -> Result<Self, JsError> {
        let identifier = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(32)
            .map(char::from)
            .collect();

        Self::open(identifier).await
    }

    /// The name used to uniquely identify the data of this [`Artifacts`]
    /// instance
    #[wasm_bindgen]
    pub async fn identifier(&self) -> String {
        self.artifacts.read().await.identifier().to_owned()
    }

    /// Construct a new `Artifacts`, backed by a database. If the same name is
    /// used for multiple instances (or across sessions), the same database will
    /// be used.
    #[wasm_bindgen]
    pub async fn open(identifier: String) -> Result<Self, JsError> {
        let storage_backend = StorageCache::new(
            IndexedDbStorageBackend::new(&identifier, "dialog-artifact-blocks")
                .await
                .map_err(|error| DialogArtifactsError::from(error))?,
            STORAGE_CACHE_CAPACITY,
        )
        .map_err(|error| DialogArtifactsError::from(error))?;

        // Erase the type:
        let storage_backend: WebStorageBackend = Arc::new(Mutex::new(storage_backend));
        let artifacts = Artifacts::open(identifier.to_owned(), storage_backend).await?;

        Ok(Self {
            artifacts: Arc::new(RwLock::new(artifacts)),
        })
    }

    /// Get the current revision of the triple store. This value will change on
    /// every successful call to `Artifacts.commit`. The returned value is
    /// suitable for use with `Artifacts.restore`, for example when re-opening
    /// the triple store on future sessions.
    #[wasm_bindgen]
    pub async fn revision(&self) -> Result<Vec<u8>, JsError> {
        Ok(self.artifacts.read().await.revision().await?.to_vec())
    }

    /// Persist a set of data in the triple store. The returned prommise
    /// resolves when all data has been persisted and the revision has been
    /// updated. Any data that does not match the expected shape of an
    /// `Artifact` is quietly ignored (this is probably bad, but it is
    /// expedient). If there is an error during the commit, the change is
    /// abandoned and the revision remains the same as it was at the start of
    /// the transaction.
    #[wasm_bindgen]
    pub async fn commit(&self, iterable: &InstructionIterableDuckType) -> Result<Vec<u8>, JsError> {
        let Some(iterator) = js_sys::try_iter(iterable).map_err(js_value_to_error)? else {
            return Err(JsError::new("Only iterables are allowed"));
        };

        let iterator = iterator.filter_map(|element| {
            if let Ok(element) = element {
                // NOTE: We are silently dropping unconvertable instructions here; probably bad
                Instruction::try_from(element).ok()
            } else {
                None
            }
        });

        Ok(self
            .artifacts
            .write()
            .await
            .commit(iterator)
            .await?
            .to_vec())
    }

    /// Reset the root of the database to `revision` if provided, or else reset
    /// to the stored root if available, or else to an empty database.
    #[wasm_bindgen]
    pub async fn reset(&self, revision: Option<Vec<u8>>) -> Result<(), JsError> {
        let revision = if let Some(revision) = revision {
            Some(Blake3Hash::try_from(revision).map_err(|bytes: Vec<u8>| {
                DialogArtifactsError::InvalidRevision(format!(
                    "Incorrect byte length (expected {HASH_SIZE}, received {})",
                    bytes.len()
                ))
            })?)
        } else {
            None
        };

        self.artifacts.write().await.reset(revision).await?;

        Ok(())
    }

    /// Query for `Artifact`s that match the given selector. Matching results
    /// are provided via an async iterator.
    #[wasm_bindgen(unchecked_return_type = "ArtifactIterable")]
    pub fn select(&self, selector: ArtifactSelectorDuckType) -> Result<JsValue, JsError> {
        let selector = ArtifactSelector::try_from(JsValue::from(selector))?;
        let artifacts = self.artifacts.clone();

        let iterable = JsValue::from(Object::new());
        let async_iterator: Closure<dyn FnMut() -> ArtifactIteratorBinding> =
            Closure::new(move || ArtifactIteratorBinding::new(selector.clone(), artifacts.clone()));
        let async_iterator = async_iterator.into_js_value();

        Reflect::set(&iterable, &Symbol::async_iterator(), &async_iterator)
            .map_err(js_value_to_error)?;

        Ok(iterable)
    }
}

/// An async iterator that lazily yields `Artifact`s
#[wasm_bindgen(js_name = "ArtifactIterator")]
pub struct ArtifactIteratorBinding {
    selector: ArtifactSelector<Constrained>,
    artifacts: Arc<RwLock<Artifacts<WebStorageBackend>>>,
    stream: Option<Pin<Box<dyn Stream<Item = Result<Artifact, DialogArtifactsError>>>>>,
}

#[wasm_bindgen(js_class = "ArtifactIterator")]
impl ArtifactIteratorBinding {
    fn new(
        selector: ArtifactSelector<Constrained>,
        artifacts: Arc<RwLock<Artifacts<WebStorageBackend>>>,
    ) -> Self {
        Self {
            selector,
            artifacts,
            stream: None,
        }
    }

    /// Get the next `Artifact` yielded by this iterator
    #[wasm_bindgen(unchecked_return_type = "IteratorResult<Artifact>")]
    pub async fn next(&mut self) -> Result<JsValue, JsError> {
        if self.stream.is_none() {
            self.stream = Some(Box::pin(
                self.artifacts.read().await.select(self.selector.clone()),
            ));
        }

        let Some(stream) = &mut self.stream else {
            return iterable_result(None);
        };

        let Some(next_element) = stream.next().await else {
            return iterable_result(None);
        };

        let next_element = JsValue::try_from(next_element?)?;

        iterable_result(Some(next_element))
    }
}

// NOTE: Everything below this line is a conversion to support duck typing on the
// JavaScript side of the API boundary

fn js_value_to_error(value: JsValue) -> JsError {
    JsError::new(&format!("{:?}", value))
}

fn iterable_result(value: Option<JsValue>) -> Result<JsValue, JsError> {
    let result = JsValue::from(Object::new());

    Reflect::set(
        &result,
        &"done".into(),
        &JsValue::from_bool(value.is_none()),
    )
    .map_err(js_value_to_error)?;

    if let Some(value) = value {
        Reflect::set(&result, &"value".into(), &value).map_err(js_value_to_error)?;
    };

    Ok(result)
}

impl From<(InstructionTypeBinding, Artifact)> for Instruction {
    fn from((instruction, artifact): (InstructionTypeBinding, Artifact)) -> Self {
        match instruction {
            InstructionTypeBinding::Assert => Instruction::Assert(artifact),
            InstructionTypeBinding::Retract => Instruction::Retract(artifact),
        }
    }
}

impl TryFrom<Value> for JsValue {
    type Error = JsError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let data_type = JsValue::from(value.data_type());
        let value = match value {
            Value::Null => JsValue::null(),
            Value::Bytes(bytes) => {
                let result = Uint8Array::new_with_length(bytes.len() as u32);
                result.copy_from(bytes.as_ref());
                JsValue::from(result)
            }
            Value::Entity(raw_entity) => Entity::from(raw_entity).into(),
            Value::Boolean(boolean) => JsValue::from_bool(boolean),
            Value::String(string) => string.into(),
            // TODO: BigNum support
            Value::UnsignedInt(uint) => JsValue::from_f64(uint as f64),
            Value::SignedInt(int) => JsValue::from_f64(int as f64),
            Value::Float(float) => JsValue::from_f64(float),
            Value::Record(bytes) => {
                let result = Uint8Array::new_with_length(bytes.len() as u32);
                result.copy_from(bytes.as_ref());
                JsValue::from(result)
            }
            Value::Symbol(attribute) => JsValue::from(attribute),
        };

        let object = JsValue::from(Object::new());

        Reflect::set(&object, &"type".into(), &data_type).map_err(js_value_to_error)?;
        Reflect::set(&object, &"value".into(), &value).map_err(js_value_to_error)?;

        Ok(object)
    }
}

impl From<Attribute> for JsValue {
    fn from(attribute: Attribute) -> Self {
        JsValue::from(String::from(attribute))
    }
}

impl From<Entity> for JsValue {
    fn from(value: Entity) -> Self {
        let value = RawEntity::from(value);
        let result = Uint8Array::new_with_length(HASH_SIZE as u32);
        result.copy_from(&value);
        JsValue::from(result)
    }
}

impl From<Cause> for JsValue {
    fn from(value: Cause) -> Self {
        let result = Uint8Array::new_with_length(HASH_SIZE as u32);
        result.copy_from(value.as_ref());
        JsValue::from(result)
    }
}

impl TryFrom<Artifact> for JsValue {
    type Error = JsError;
    fn try_from(artifact: Artifact) -> Result<Self, Self::Error> {
        let current_version = artifact.clone();

        let update = Closure::<dyn Fn(JsValue) -> JsValue>::new(move |value: JsValue| {
            if let Ok(value) = Value::try_from(value) {
                JsValue::try_from(current_version.clone().update(value))
                    .unwrap_or(JsValue::undefined())
            } else {
                JsValue::undefined()
            }
        })
        .into_js_value();

        let object = JsValue::from(Object::new());
        let attribute = JsValue::from(artifact.the);
        let entity = JsValue::from(artifact.of);
        let value = JsValue::try_from(artifact.is)?;

        Reflect::set(&object, &"the".into(), &attribute).map_err(js_value_to_error)?;
        Reflect::set(&object, &"of".into(), &entity).map_err(js_value_to_error)?;
        Reflect::set(&object, &"is".into(), &value).map_err(js_value_to_error)?;
        Reflect::set(&object, &"update".into(), &update).map_err(js_value_to_error)?;

        if let Some(cause) = artifact.cause {
            Reflect::set(&object, &"cause".into(), &JsValue::from(cause))
                .map_err(js_value_to_error)?;
        }

        Ok(object)
    }
}

impl TryFrom<JsValue> for Attribute {
    type Error = JsError;

    fn try_from(attribute: JsValue) -> Result<Self, Self::Error> {
        let Some(string) = attribute.as_string() else {
            return Err(DialogArtifactsError::InvalidAttribute("Not a string".into()).into());
        };

        Ok(Attribute::try_from(string)?)
    }
}

impl TryFrom<JsValue> for Entity {
    type Error = JsError;

    fn try_from(entity: JsValue) -> Result<Self, Self::Error> {
        let bytes = entity.dyn_into::<Uint8Array>().map_err(js_value_to_error)?;
        let raw_entity: RawEntity = bytes.to_vec().try_into().map_err(|value: Vec<u8>| {
            DialogArtifactsError::InvalidEntity(format!(
                "Wrong length; expected {}, got {}",
                HASH_SIZE,
                value.len()
            ))
        })?;
        Ok(Entity::from(raw_entity))
    }
}

impl TryFrom<JsValue> for Value {
    type Error = JsError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        let data_type = Reflect::get(&value, &"type".into())
            .and_then(|value| {
                value.as_f64().ok_or_else(|| {
                    DialogArtifactsError::InvalidValue("Could not parse value data type".into())
                        .into()
                })
            })
            .map(|value| ValueDataType::from(value as u8))
            .map_err(js_value_to_error)?;

        let value = Reflect::get(&value, &"value".into()).map_err(js_value_to_error)?;

        Value::try_from((data_type, value))
    }
}

impl TryFrom<JsValue> for Cause {
    type Error = JsError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        let bytes = value
            .dyn_into::<Uint8Array>()
            .map_err(js_value_to_error)?
            .to_vec();
        Ok(Cause::try_from(bytes)?)
    }
}

impl TryFrom<(ValueDataType, JsValue)> for Value {
    type Error = JsError;

    fn try_from((data_type, value): (ValueDataType, JsValue)) -> Result<Self, Self::Error> {
        if value.is_undefined() {
            return if matches!(data_type, ValueDataType::Null) {
                Ok(Value::Null)
            } else {
                Err(DialogArtifactsError::InvalidValue(
                    "Non-null data type must be initialized with a value".into(),
                )
                .into())
            };
        };

        let value = match data_type {
            ValueDataType::Null => Value::Null,
            ValueDataType::Bytes => {
                let byte_array: Uint8Array = value.dyn_into().map_err(js_value_to_error)?;
                Value::Bytes(byte_array.to_vec())
            }
            ValueDataType::Entity => {
                let byte_array: Uint8Array = value.dyn_into().map_err(js_value_to_error)?;
                let raw_entity: RawEntity =
                    byte_array.to_vec().try_into().map_err(|value: Vec<u8>| {
                        DialogArtifactsError::InvalidEntity(format!(
                            "Wrong length; expected {}, got {}",
                            HASH_SIZE,
                            value.len()
                        ))
                    })?;
                Value::Entity(raw_entity)
            }
            ValueDataType::Boolean => Value::Boolean(value.is_truthy()),
            ValueDataType::String => Value::String(value.as_string().ok_or_else(|| {
                DialogArtifactsError::InvalidValue("Could not interpret value as a string".into())
            })?),
            ValueDataType::UnsignedInt => {
                // TODO: BigNum support
                let Some(value) = value.as_f64() else {
                    return Err(DialogArtifactsError::InvalidValue(
                        "Value is not a numeric".into(),
                    )
                    .into());
                };

                Value::UnsignedInt(value as u128)
            }
            ValueDataType::SignedInt => {
                let Some(value) = value.as_f64() else {
                    return Err(DialogArtifactsError::InvalidValue(
                        "Value is not a numeric".into(),
                    )
                    .into());
                };

                Value::SignedInt(value as i128)
            }
            ValueDataType::Float => {
                let Some(value) = value.as_f64() else {
                    return Err(DialogArtifactsError::InvalidValue(
                        "Value is not a numeric".into(),
                    )
                    .into());
                };

                Value::Float(value)
            }
            ValueDataType::Record => {
                let byte_array: Uint8Array = value.dyn_into().map_err(js_value_to_error)?;
                Value::Record(byte_array.to_vec())
            }
            ValueDataType::Symbol => Value::Symbol(Attribute::try_from(
                value.as_string().ok_or_else(|| {
                    DialogArtifactsError::InvalidValue(
                        "Could not interpret value as a string".into(),
                    )
                })?,
            )?),
        };

        Ok(value)
    }
}

impl TryFrom<JsValue> for Artifact {
    type Error = JsError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        let the = Reflect::get(&value, &"the".into())
            .map_err(js_value_to_error)
            .and_then(Attribute::try_from)?;
        let of = Reflect::get(&value, &"of".into())
            .map_err(js_value_to_error)
            .and_then(Entity::try_from)?;
        let is = Reflect::get(&value, &"is".into())
            .map_err(js_value_to_error)
            .and_then(Value::try_from)?;
        let cause = Reflect::get(&value, &"cause".into())
            .and_then(|value| {
                if value.is_undefined() {
                    Ok(None)
                } else {
                    Ok(Some(Cause::try_from(value)?))
                }
            })
            .map_err(js_value_to_error)?;

        Ok(Artifact { the, of, is, cause })
    }
}

impl TryFrom<JsValue> for Instruction {
    type Error = JsError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        let instruction_type = Reflect::get(&value, &"type".into())
            .and_then(InstructionTypeBinding::try_from_js_value)
            .map_err(js_value_to_error)?;

        let artifact = Reflect::get(&value, &"artifact".into())
            .map_err(js_value_to_error)
            .and_then(Artifact::try_from)?;

        Ok(Instruction::from((instruction_type, artifact)))
    }
}

impl TryFrom<JsValue> for ArtifactSelector<Constrained> {
    type Error = JsError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        let selector = if let Some(the) = Reflect::get(&value, &"the".into())
            .ok()
            .and_then(|value| if value.is_truthy() { Some(value) } else { None })
        {
            Some(ArtifactSelector::new().the(Attribute::try_from(the)?))
        } else {
            None
        };

        let selector = if let Some(of) = Reflect::get(&value, &"of".into())
            .ok()
            .and_then(|value| if value.is_truthy() { Some(value) } else { None })
        {
            let entity = Entity::try_from(of)?;
            if let Some(selector) = selector {
                Some(selector.of(entity))
            } else {
                Some(ArtifactSelector::new().of(entity))
            }
        } else {
            selector
        };

        let selector = if let Some(is) = Reflect::get(&value, &"is".into())
            .ok()
            .and_then(|value| if value.is_truthy() { Some(value) } else { None })
        {
            let value = Value::try_from(is)?;
            if let Some(selector) = selector {
                Some(selector.is(value))
            } else {
                Some(ArtifactSelector::new().is(value))
            }
        } else {
            selector
        };

        selector.ok_or_else(|| DialogArtifactsError::EmptySelector.into())
    }
}
