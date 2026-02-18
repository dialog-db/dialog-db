use dialog_capability::{Provider, Subject};
use dialog_effects::memory::{self, Memory, Space};
use dialog_storage::{CborEncoder, DialogStorageError, Encoder};
use serde::{Serialize, de::DeserializeOwned};
use std::marker::PhantomData;

use super::RepositoryError;

/// A descriptor for a memory cell that stores typed values.
///
/// `Cell` is pure data — it describes *where* a value lives (subject, space,
/// cell name) and *how* to encode it (codec). Methods return command structs
/// whose `.perform(env)` executes the actual I/O.
#[derive(Debug, Clone)]
pub struct Cell<Codec = CborEncoder> {
    subject: Subject,
    space: String,
    cell: String,
    codec: Codec,
}

impl Cell {
    /// Create a new Cell with the default CBOR codec.
    pub fn new(
        subject: impl Into<Subject>,
        space: impl Into<String>,
        cell: impl Into<String>,
    ) -> Self {
        Self {
            subject: subject.into(),
            space: space.into(),
            cell: cell.into(),
            codec: CborEncoder,
        }
    }
}

impl<Codec: Encoder> Cell<Codec> {
    /// Create a new Cell with a custom codec.
    pub fn with_codec(
        subject: impl Into<Subject>,
        space: impl Into<String>,
        cell: impl Into<String>,
        codec: Codec,
    ) -> Self {
        Self {
            subject: subject.into(),
            space: space.into(),
            cell: cell.into(),
            codec,
        }
    }

    /// Create a command to resolve (read) the current value from this cell.
    pub fn resolve<T>(&self) -> Resolve<'_, T, Codec> {
        Resolve {
            cell: self,
            _phantom: PhantomData,
        }
    }

    /// Create a command to publish (write) a value to this cell.
    ///
    /// `edition` is the CAS edition from the last resolve. Pass `None` when
    /// creating a new cell for the first time.
    pub fn publish<T: Serialize>(
        &self,
        value: T,
        edition: Option<Vec<u8>>,
    ) -> Publish<'_, T, Codec> {
        Publish {
            cell: self,
            value,
            edition,
        }
    }
}

/// Command struct for resolving (reading) a cell's value.
///
/// Created by [`Cell::resolve`]. Call `.perform(env)` to execute.
pub struct Resolve<'a, T, Codec = CborEncoder> {
    cell: &'a Cell<Codec>,
    _phantom: PhantomData<T>,
}

impl<T, Codec> Resolve<'_, T, Codec>
where
    T: DeserializeOwned + dialog_common::ConditionalSync,
    Codec: Encoder,
{
    /// Execute the resolve operation, returning the current value and its
    /// edition (for CAS), or `None` if the cell is empty.
    pub async fn perform<Env>(self, env: &mut Env) -> Result<Option<(T, Vec<u8>)>, RepositoryError>
    where
        Env: Provider<memory::Resolve>,
    {
        let publication = self
            .cell
            .subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new(&self.cell.space))
            .attenuate(memory::Cell::new(&self.cell.cell))
            .invoke(memory::Resolve)
            .perform(env)
            .await
            .map_err(|e| RepositoryError::StorageError(format!("Memory resolve failed: {}", e)))?;

        match publication {
            None => Ok(None),
            Some(pub_data) => {
                let value: T = self
                    .cell
                    .codec
                    .decode(&pub_data.content)
                    .await
                    .map_err(|e| {
                        RepositoryError::StorageError(format!(
                            "Failed to decode cell value: {}",
                            Into::<DialogStorageError>::into(e)
                        ))
                    })?;

                Ok(Some((value, pub_data.edition)))
            }
        }
    }
}

/// Command struct for publishing (writing) a value to a cell.
///
/// Created by [`Cell::publish`]. Call `.perform(env)` to execute.
pub struct Publish<'a, T, Codec = CborEncoder> {
    cell: &'a Cell<Codec>,
    value: T,
    edition: Option<Vec<u8>>,
}

impl<T: Serialize, Codec> Publish<'_, T, Codec> {
    /// Execute the publish operation, returning the new edition.
    pub async fn perform<Env>(self, env: &mut Env) -> Result<Vec<u8>, RepositoryError>
    where
        Env: Provider<memory::Publish>,
    {
        let content = serde_ipld_dagcbor::to_vec(&self.value)
            .map_err(|e| RepositoryError::StorageError(format!("Failed to encode value: {}", e)))?;

        let new_edition = self
            .cell
            .subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new(&self.cell.space))
            .attenuate(memory::Cell::new(&self.cell.cell))
            .invoke(memory::Publish::new(content, self.edition))
            .perform(env)
            .await
            .map_err(|e| RepositoryError::StorageError(format!("Memory publish failed: {}", e)))?;

        Ok(new_edition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::Did;
    use dialog_storage::provider::Volatile;

    fn test_subject() -> Subject {
        let did: Did = "did:test:cell-tests".parse().unwrap();
        Subject::from(did)
    }

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestValue {
        count: u32,
        name: String,
    }

    #[dialog_common::test]
    async fn it_resolves_empty_cell() -> anyhow::Result<()> {
        let mut provider = Volatile::new();
        let cell = Cell::new(test_subject(), "local", "missing");

        let result: Option<(TestValue, Vec<u8>)> = cell.resolve().perform(&mut provider).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_publishes_then_resolves() -> anyhow::Result<()> {
        let mut provider = Volatile::new();
        let cell = Cell::new(test_subject(), "local", "test");

        let value = TestValue {
            count: 42,
            name: "hello".into(),
        };

        // Publish with no prior edition (new cell)
        let edition = cell.publish(&value, None).perform(&mut provider).await?;
        assert!(!edition.is_empty());

        // Resolve and check
        let result: Option<(TestValue, Vec<u8>)> = cell.resolve().perform(&mut provider).await?;
        let (resolved_value, resolved_edition) = result.expect("cell should have content");

        assert_eq!(resolved_value, value);
        assert_eq!(resolved_edition, edition);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_updates_with_correct_edition() -> anyhow::Result<()> {
        let mut provider = Volatile::new();
        let cell = Cell::new(test_subject(), "local", "update");

        let v1 = TestValue {
            count: 1,
            name: "first".into(),
        };
        let edition1 = cell.publish(&v1, None).perform(&mut provider).await?;

        let v2 = TestValue {
            count: 2,
            name: "second".into(),
        };
        let edition2 = cell
            .publish(&v2, Some(edition1.clone()))
            .perform(&mut provider)
            .await?;

        assert_ne!(edition1, edition2);

        let result: Option<(TestValue, Vec<u8>)> = cell.resolve().perform(&mut provider).await?;
        let (resolved, _) = result.expect("cell should have content");
        assert_eq!(resolved, v2);

        Ok(())
    }
}
