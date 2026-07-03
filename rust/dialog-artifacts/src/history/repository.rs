use ed25519_dalek::SigningKey;
use futures_util::stream;
use serde::{Deserialize, Serialize};

use dialog_common::ConditionalSync;
use dialog_storage::{
    Blake3Hash, CborEncoder, ContentAddressedStorage, DialogStorageError, Storage, StorageBackend,
};

use crate::{
    Artifact, Artifacts, DialogArtifactsError, Entity, HASH_SIZE, Instruction, make_reference,
};

use super::{
    Authority, Causality, Cause, Claim, HistoryStore, Issuer, Origin, Record, Revision, Version,
    causality, revision_record,
};

/// The durable head record of a [`Repository`]: the latest signed revision
/// and the root of the history index at that revision
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Head {
    revision: Option<Revision>,
    history: Option<Blake3Hash>,
}

/// A version-controlled view over [`Artifacts`].
///
/// A [`Repository`] pairs the EAV indexes with the history index described in
/// `notes/version-control.md`. Every commit:
///
/// 1. derives the next [`Version`] from the revision DAG (`cause` is the
///    current head, plus any merged remote heads),
/// 2. applies the instructions to the EAV indexes, tagging every asserted
///    datum with that version,
/// 3. records a [`Record`] per instruction in the history index, whose cause
///    lists the versions of the claims it supersedes,
/// 4. issues a signed [`Revision`] over the new tree root and records its
///    lineage claim, and
/// 5. atomically advances the durable head to the new revision and history
///    root (rolling the EAV indexes back if any step fails).
pub struct Repository<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync
        + 'static,
{
    subject: Entity,
    signing_key: SigningKey,
    authority: Authority,
    artifacts: Artifacts<Backend>,
    storage: Storage<CborEncoder, Backend>,
    history: HistoryStore<Backend>,
    head: Option<Revision>,
}

impl<Backend> Repository<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync
        + 'static,
{
    /// Open the repository identified by `subject` (its DID), acting as the
    /// issuer identified by `signing_key` on behalf of `authority`.
    ///
    /// Note that per the origin invariant, each replica must act under its
    /// own signing key: the same key used concurrently from two replicas can
    /// mint colliding versions.
    pub async fn open(
        subject: Entity,
        signing_key: SigningKey,
        authority: Authority,
        backend: Backend,
    ) -> Result<Self, DialogArtifactsError> {
        let artifacts = Artifacts::open(subject.to_string(), backend.clone()).await?;
        let storage = Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        };

        let head = match storage.get(&head_pointer(&subject)).await? {
            Some(bytes) => {
                let hash = Blake3Hash::try_from(bytes).map_err(|bytes| {
                    DialogArtifactsError::InvalidRevision(format!(
                        "Incorrect head hash length (expected {HASH_SIZE}, received {})",
                        bytes.len()
                    ))
                })?;
                storage.read::<Head>(&hash).await?.ok_or_else(|| {
                    DialogArtifactsError::InvalidRevision("Head record not found in storage".into())
                })?
            }
            None => Head {
                revision: None,
                history: None,
            },
        };

        if let Some(revision) = &head.revision {
            revision.verify()?;
        }

        let history = match &head.history {
            Some(hash) => HistoryStore::from_hash(hash, backend),
            None => HistoryStore::new(backend),
        };

        Ok(Self {
            subject,
            signing_key,
            authority,
            artifacts,
            storage,
            history,
            head: head.revision,
        })
    }

    /// The DID identifying this repository
    pub fn subject(&self) -> &Entity {
        &self.subject
    }

    /// The issuer this replica acts as
    pub fn issuer(&self) -> Issuer {
        Issuer::from(self.signing_key.verifying_key())
    }

    /// The [`Origin`] of this replica within this repository
    pub fn origin(&self) -> Origin {
        Origin::derive(&self.issuer(), &self.subject)
    }

    /// The latest signed revision, or `None` when nothing has been committed
    pub fn head(&self) -> Option<&Revision> {
        self.head.as_ref()
    }

    /// The underlying [`Artifacts`] (EAV indexes)
    pub fn artifacts(&self) -> &Artifacts<Backend> {
        &self.artifacts
    }

    /// The history index
    pub fn history(&self) -> &HistoryStore<Backend> {
        &self.history
    }

    /// Commit the given instructions as a new signed revision on top of the
    /// current head
    pub async fn commit<Instructions>(
        &mut self,
        instructions: Instructions,
    ) -> Result<Revision, DialogArtifactsError>
    where
        Instructions: IntoIterator<Item = Instruction>,
    {
        self.transact(instructions.into_iter().collect(), Vec::new())
            .await
    }

    /// Commit the given instructions as a new signed revision that merges the
    /// given remote head into the current head: the revision's cause lists
    /// both parents, so its edition advances past everything either lineage
    /// has seen. The remote revision is verified before it is adopted; its
    /// claims are expected to have been [`integrate`](Repository::integrate)d
    /// beforehand.
    pub async fn merge<Instructions>(
        &mut self,
        remote: &Revision,
        instructions: Instructions,
    ) -> Result<Revision, DialogArtifactsError>
    where
        Instructions: IntoIterator<Item = Instruction>,
    {
        remote.verify()?;
        self.transact(instructions.into_iter().collect(), vec![remote.version()])
            .await
    }

    /// Integrate a replicated record from another repository (or another
    /// replica of this one) into the local indexes.
    ///
    /// The record is added to the history index, and the EAV indexes are
    /// updated to reflect it: an assertion replaces any currently asserted
    /// data whose versions appear in the record's cause (and coexists with
    /// data it does not supersede — genuinely concurrent values remain until
    /// deliberately resolved); a retraction withdraws the data whose versions
    /// appear in its cause.
    pub async fn integrate(
        &mut self,
        version: &Version,
        record: Record,
    ) -> Result<(), DialogArtifactsError> {
        let claim = record.claim().clone();

        // Integration is idempotent: a record that is already part of the
        // local history has already been applied
        if self.history.contains(version, &claim).await? {
            return Ok(());
        }

        let mut instructions = Vec::new();

        for datum in self.artifacts.select_data(&claim.of, &claim.the).await? {
            if let Some(datum_version) = datum.version
                && claim.cause.contains(&datum_version)
            {
                instructions.push(Instruction::Retract(Artifact::try_from(datum)?));
            }
        }

        if record.is_assertion() {
            instructions.push(Instruction::Assert(Artifact {
                the: claim.the.clone(),
                of: claim.of.clone(),
                is: claim.is.clone(),
                cause: None,
            }));
        }

        if !instructions.is_empty() {
            self.artifacts
                .commit_with_version(Some(*version), stream::iter(instructions))
                .await?;
        }

        self.history.record(version, record).await?;
        self.persist_head().await?;

        Ok(())
    }

    /// Determine the causal relationship between two claims on the same
    /// `(entity, attribute)` using the local history index
    pub async fn causality(
        &self,
        a: (&Claim, &Version),
        b: (&Claim, &Version),
    ) -> Result<Causality, DialogArtifactsError> {
        causality(a, b, &self.history).await
    }

    async fn transact(
        &mut self,
        instructions: Vec<Instruction>,
        remote_parents: Vec<Version>,
    ) -> Result<Revision, DialogArtifactsError> {
        let mut parents = remote_parents;
        if let Some(head) = &self.head {
            parents.push(head.version());
        }
        let cause = Cause::new(parents);
        let version = Version::new(self.origin(), cause.edition());

        // Derive the history records against the pre-commit EAV state, so
        // that each record's cause lists the versions of the claims the
        // instruction supersedes
        let mut records = Vec::with_capacity(instructions.len());
        for instruction in &instructions {
            records.push(self.derive_record(instruction).await?);
        }

        let base_revision = self.artifacts.revision().await?;
        let previous_history = self.history.hash();

        // Rolls itself back on failure
        let tree = self
            .artifacts
            .commit_with_version(Some(version), stream::iter(instructions))
            .await?;

        let outcome = async {
            let revision = Revision::issue(
                tree,
                self.subject.clone(),
                self.authority,
                cause,
                &self.signing_key,
            );
            debug_assert_eq!(revision.version(), version);

            // Record every claim plus the revision's lineage claim as a
            // single history index edit
            let mut batch: Vec<(Version, Record)> = records
                .into_iter()
                .map(|record| (version, record))
                .collect();
            batch.push(revision_record(&revision)?);
            self.history.record_all(batch).await?;

            self.head = Some(revision.clone());
            self.persist_head().await?;

            Ok(revision) as Result<Revision, DialogArtifactsError>
        }
        .await;

        match outcome {
            Ok(revision) => Ok(revision),
            Err(error) => {
                // Roll the EAV indexes, the history index and the head back
                // to their pre-transaction state
                self.artifacts.reset(Some(base_revision)).await?;
                self.history.reset(previous_history.as_ref());
                self.head = match self
                    .storage
                    .get(&head_pointer(&self.subject))
                    .await?
                    .map(Blake3Hash::try_from)
                {
                    Some(Ok(hash)) => self
                        .storage
                        .read::<Head>(&hash)
                        .await?
                        .and_then(|head| head.revision),
                    _ => None,
                };
                Err(error)
            }
        }
    }

    /// Derive the history [`Record`] for an instruction against the current
    /// EAV state — see [`Record::derive`].
    async fn derive_record(
        &self,
        instruction: &Instruction,
    ) -> Result<Record, DialogArtifactsError> {
        let artifact = match instruction {
            Instruction::Assert(artifact)
            | Instruction::Replace(artifact)
            | Instruction::Retract(artifact) => artifact,
        };
        let current = self
            .artifacts
            .select_data(&artifact.of, &artifact.the)
            .await?;
        Ok(Record::derive(instruction, &current))
    }

    async fn persist_head(&mut self) -> Result<(), DialogArtifactsError> {
        let head = Head {
            revision: self.head.clone(),
            history: self.history.hash(),
        };
        let hash = self.storage.write(&head).await?;
        self.storage
            .set(head_pointer(&self.subject), hash.to_vec())
            .await?;
        Ok(())
    }
}

/// The storage key of the mutable pointer to a repository's [`Head`] record
fn head_pointer(subject: &Entity) -> Blake3Hash {
    make_reference(format!("{subject}#dialog.db/head").as_bytes())
}
