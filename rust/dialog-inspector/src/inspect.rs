//! Inspection of dialog-db instances via IndexedDB.
//!
//! This module opens a dialog-db database by name and provides read-only
//! access to its revision, facts, and (in the future) tree structure.
//! It reuses the same [`IndexedDbStorageBackend`] and [`Artifacts`] types
//! that the database itself uses, ensuring byte-level compatibility.

use std::sync::Arc;

use base58::ToBase58;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStore, Artifacts, Attribute, Entity, Value,
};
use dialog_storage::{
    IndexedDbStorageBackend, StorageCache,
    web::ObjectSafeStorageBackend,
};
use futures_util::StreamExt;
use tokio::sync::{Mutex, RwLock};
use wasm_bindgen::prelude::*;

use crate::discovery::DatabaseInfo;

/// Type alias matching the web bindings in dialog-artifacts.
type WebBackend = Arc<Mutex<dyn ObjectSafeStorageBackend>>;

const STORAGE_CACHE_CAPACITY: usize = 2usize.pow(14); // 16k entries

/// A handle to an opened dialog-db instance for read-only inspection.
///
/// This wraps [`Artifacts`] and provides a simplified API surface focused
/// on introspection rather than mutation.
pub struct InspectedDatabase {
    artifacts: Arc<RwLock<Artifacts<WebBackend>>>,
    identifier: String,
}

/// Summary information about an inspected database.
#[derive(Debug, Clone)]
pub struct DatabaseSummary {
    /// The database identifier / name
    pub identifier: String,
    /// The current revision hash (base58-encoded)
    pub revision: String,
    /// Whether the database is empty (null revision)
    pub is_empty: bool,
}

/// A single fact from the database, formatted for display.
#[derive(Debug, Clone)]
pub struct DisplayFact {
    /// The attribute (predicate) as a string
    pub the: String,
    /// The entity (subject) as a string
    pub of: String,
    /// The value (object) as a display string
    pub is: String,
    /// The value type tag
    pub value_type: String,
    /// The causal reference (base58), if any
    pub cause: Option<String>,
}

impl InspectedDatabase {
    /// Open a dialog-db instance by name for read-only inspection.
    ///
    /// This opens the underlying IndexedDB database using the same storage
    /// backend that dialog-db uses, ensuring full compatibility.
    pub async fn open(info: &DatabaseInfo) -> Result<Self, JsValue> {
        let storage_backend = StorageCache::new(
            IndexedDbStorageBackend::new(&info.name)
                .await
                .map_err(|e| JsValue::from_str(&format!("{e}")))?,
            STORAGE_CACHE_CAPACITY,
        )
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;

        let backend: WebBackend = Arc::new(Mutex::new(storage_backend));
        let artifacts = Artifacts::open(info.name.clone(), backend)
            .await
            .map_err(|e| JsValue::from_str(&format!("{e}")))?;

        Ok(Self {
            identifier: info.name.clone(),
            artifacts: Arc::new(RwLock::new(artifacts)),
        })
    }

    /// Get a summary of this database (identifier + current revision).
    pub async fn summary(&self) -> Result<DatabaseSummary, JsValue> {
        let artifacts = self.artifacts.read().await;
        let revision_hash = artifacts
            .revision()
            .await
            .map_err(|e| JsValue::from_str(&format!("{e}")))?;

        let is_empty = revision_hash == dialog_artifacts::NULL_REVISION_HASH;
        let revision = revision_hash.to_base58();

        Ok(DatabaseSummary {
            identifier: self.identifier.clone(),
            revision,
            is_empty,
        })
    }

    /// Query facts from the database matching the given attribute filter.
    ///
    /// If `attribute` is `None`, this returns an error since the selector
    /// requires at least one constraint. Use a known attribute namespace
    /// to browse facts.
    pub async fn query_facts(
        &self,
        attribute: Option<&str>,
        entity: Option<&str>,
        limit: usize,
    ) -> Result<Vec<DisplayFact>, JsValue> {
        let artifacts = self.artifacts.read().await;

        let selector = if let Some(attr) = attribute {
            let attr = Attribute::try_from(attr.to_string())
                .map_err(|e| JsValue::from_str(&format!("{e}")))?;
            ArtifactSelector::new().the(attr)
        } else if let Some(ent) = entity {
            let ent = Entity::try_from(ent.to_string())
                .map_err(|e| JsValue::from_str(&format!("{e}")))?;
            ArtifactSelector::new().of(ent)
        } else {
            return Err(JsValue::from_str(
                "At least one selector constraint (attribute or entity) is required",
            ));
        };

        let stream = artifacts.select(selector);
        tokio::pin!(stream);

        let mut facts = Vec::new();
        while let Some(result) = stream.next().await {
            match result {
                Ok(artifact) => {
                    facts.push(format_artifact(&artifact));
                    if facts.len() >= limit {
                        break;
                    }
                }
                Err(_) => continue,
            }
        }

        Ok(facts)
    }
}

fn format_artifact(artifact: &Artifact) -> DisplayFact {
    let value_type = match &artifact.is {
        Value::String(_) => "String",
        Value::Bytes(_) => "Bytes",
        Value::Boolean(_) => "Boolean",
        Value::Entity(_) => "Entity",
        Value::UnsignedInt(_) => "UnsignedInt",
        Value::SignedInt(_) => "SignedInt",
        Value::Float(_) => "Float",
        Value::Record(_) => "Record",
        Value::Symbol(_) => "Symbol",
    };

    let is = match &artifact.is {
        Value::String(s) => s.clone(),
        Value::Boolean(b) => b.to_string(),
        Value::UnsignedInt(n) => n.to_string(),
        Value::SignedInt(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Entity(e) => e.to_string(),
        Value::Symbol(a) => String::from(a.clone()),
        Value::Bytes(b) => format!("<{} bytes>", b.len()),
        Value::Record(b) => format!("<record {} bytes>", b.len()),
    };

    DisplayFact {
        the: String::from(artifact.the.clone()),
        of: artifact.of.to_string(),
        is,
        value_type: value_type.to_string(),
        cause: artifact.cause.as_ref().map(|c| {
            let bytes: &[u8] = c.as_ref();
            bytes.to_base58()
        }),
    }
}
