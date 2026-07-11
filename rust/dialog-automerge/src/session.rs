//! An open editing session over a record-typed attribute — the doc-handle.
//!
//! Reading a record attribute yields a snapshot; an editor holds a document
//! *open* while concurrent writers keep committing. The doc-handle carries a
//! live in-memory document through that window and gives merge work its three
//! app-controlled moments (`notes/automerge-integration-spec.md` §4.3) — none
//! on the sync path, none per-keystroke:
//!
//! 1. **The fold at open** — [`DocumentSession::open`] reads every stored
//!    sibling of the attribute and merges them into one document.
//! 2. **Incremental absorption during the session** — when sync lands a
//!    concurrent sibling mid-session, [`absorb`](DocumentSession::absorb) (or
//!    a [`refresh`](DocumentSession::refresh) re-scan) merges it into the
//!    live document, so pending local edits and the arrival both survive.
//! 3. **Physical collapse on the next ordinary edit** —
//!    [`commit`](DocumentSession::commit) is a typed `Cardinality::One`
//!    assert, which the statement layer turns into an `Instruction::Replace`:
//!    the session's merged document supersedes every stored sibling. No
//!    scheduled write-back exists; storage converges exactly when the
//!    session commits.
//!
//! Skipping the discipline is what loses data: an app that reads one sibling
//! and blind-writes performs a `Replace` that discards the other fork's
//! edits — the precise failure CRDTs exist to prevent (spec §6.13/§6.15).

use std::cmp::Ordering;
use std::collections::HashSet;
use std::marker::PhantomData;

use dialog_artifacts::{
    Artifact, ArtifactSelector, Attribute as ArtifactsAttribute, Cause, DialogArtifactsError,
    Entity, Record, RecordError, RecordFormat, Recorded, Value,
};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::Identify;
use dialog_effects::memory::{Publish, Resolve};
use dialog_query::{
    Attribute, AttributeDescriptor, Cardinality, Descriptor, StaticAttributeExpressionBuilder,
};
use dialog_repository::{Branch, CommitError, RemoteSite, Revision};
use dialog_search_tree::DialogSearchTreeError;
use futures_util::TryStreamExt as _;
use thiserror::Error;

/// Errors that may occur while opening, refreshing, or committing a
/// [`DocumentSession`].
#[derive(Debug, Error)]
pub enum SessionError {
    /// The attribute's descriptor declares a cardinality other than `one`.
    /// A sibling set is `Cardinality::Many`'s intended semantics — there is
    /// no single document to hold open, and a commit would not supersede.
    #[error(
        "attribute {the} has cardinality {cardinality:?}; \
         a document session requires cardinality one"
    )]
    NotSingular {
        /// The attribute name from the descriptor.
        the: String,
        /// The cardinality the descriptor declares.
        cardinality: Cardinality,
    },
    /// The attribute holds stored siblings, but none of them decodes as the
    /// session's format — foreign or corrupt bytes written by a
    /// schema-ignoring tool (spec §6.12). There is nothing to edit.
    #[error("none of the {siblings} stored sibling(s) decodes as the session's record format")]
    Undecodable {
        /// How many siblings the attribute holds.
        siblings: usize,
    },
    /// Encoding the live document or decoding a sibling failed.
    #[error(transparent)]
    Record(#[from] RecordError),
    /// A sibling failed to stream out of the branch index.
    #[error(transparent)]
    Artifacts(#[from] DialogArtifactsError),
    /// The branch's search tree could not be read.
    #[error(transparent)]
    Tree(#[from] DialogSearchTreeError),
    /// The commit could not be applied or published.
    #[error(transparent)]
    Commit(#[from] CommitError),
}

/// The doc-handle: a live, in-memory document `F` held open for editing
/// against the record-typed attribute `A` of one entity.
///
/// The session tracks which stored siblings its document already contains
/// (by record identity — canonical bytes), so absorption is idempotent and a
/// [`refresh`](DocumentSession::refresh) can tell an unseen arrival from a
/// sibling it has already merged.
///
/// # Threading and latency (spec §4.3, normative)
///
/// Decode and merge are CPU-bound and scale with document *history*, not
/// visible size. Run the session off the UI thread: the async methods await
/// repository I/O, but the fold work inside them executes on the calling
/// task. Hand the UI a rendered document, never unfolded siblings.
///
/// # Formats
///
/// Absorption relies on the format's [`RecordFormat::merge`] being
/// information-preserving (a CRDT merge), which is what makes pending local
/// edits and concurrent arrivals both survive. For a format that keeps the
/// default replace-merge, the session orients every merge so the **live
/// document wins**: an arrival that cannot be unified is subsumed rather
/// than silently discarding the user's pending edits, and the next commit
/// supersedes it visibly (traceable supersession, not silent loss).
///
/// # Example
///
/// ```ignore
/// let mut session = DocumentSession::<Body, _>::open(&branch, &env, note)
///     .await?
///     .expect("note body exists");
/// session.document_mut().splice(0, 0, "hello")?; // pending local edit
/// session.refresh(&branch, &env).await?;         // absorb concurrent arrivals
/// session.commit(&branch, &env).await?;          // Replace supersedes all siblings
/// ```
pub struct DocumentSession<A, F> {
    entity: Entity,
    /// The live document. Pending (uncommitted) edits live here.
    document: F,
    /// Every stored sibling already folded into `document`, by record
    /// identity (canonical bytes) — including the record this session last
    /// committed.
    absorbed: HashSet<Record>,
    attribute: PhantomData<fn() -> A>,
}

/// Winner order between sibling artifacts of one `(the, of)` group,
/// mirroring the read path's pick-one rule (`choose` in
/// `dialog-query/src/attribute/query/resolution.rs`): higher cause wins;
/// when causes are equal (including both `None`), the fact hash breaks the
/// tie. Returns [`Ordering::Greater`] when `a` is the winner.
fn winner_order(a: &Artifact, b: &Artifact) -> Ordering {
    match (&a.cause, &b.cause) {
        (Some(x), Some(y)) if x != y => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        _ => Cause::from(a)
            .partial_cmp(&Cause::from(b))
            .unwrap_or(Ordering::Equal),
    }
}

impl<A, F> DocumentSession<A, F>
where
    A: Attribute<Type = Recorded<F>>
        + Descriptor<AttributeDescriptor>
        + StaticAttributeExpressionBuilder
        + From<Recorded<F>>
        + Clone,
    F: RecordFormat,
{
    /// Open a session on `entity`'s attribute, folding every stored sibling
    /// into one live document before returning.
    ///
    /// Returns `Ok(None)` when the attribute holds no value for the entity —
    /// start one with [`create`](DocumentSession::create) instead. Siblings
    /// that fail to decode as `F` are dropped from the fold
    /// deterministically, the same posture as the read-side fold (spec
    /// §6.9); if siblings exist but none decodes, this is
    /// [`SessionError::Undecodable`].
    pub async fn open<Env>(
        branch: &Branch,
        env: &Env,
        entity: Entity,
    ) -> Result<Option<Self>, SessionError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        match Self::open_progressive(branch, env, entity).await? {
            None => Ok(None),
            Some((mut session, deferred)) => {
                for sibling in &deferred {
                    // Undecodable siblings drop out of the fold
                    // deterministically; every other error is unreachable
                    // from `absorb`.
                    let _ = session.absorb(sibling);
                }
                Ok(Some(session))
            }
        }
    }

    /// Open a session seeded from the deterministic winner sibling only —
    /// the same document a pick-one reader projects, at the same cost as the
    /// no-divergence case — returning the remaining siblings for the caller
    /// to [`absorb`](DocumentSession::absorb) on its own schedule.
    ///
    /// This is the progressive open of spec §4.3: render the winner
    /// immediately, then deliver the other fork's edits to the live document
    /// exactly as if they had arrived by sync moments after opening. The
    /// deferred records come back in winner order; absorbing them (in any
    /// order) converges to the same document [`open`](DocumentSession::open)
    /// yields, since divergence is local bytes — never a remote fetch on the
    /// open path.
    pub async fn open_progressive<Env>(
        branch: &Branch,
        env: &Env,
        entity: Entity,
    ) -> Result<Option<(Self, Vec<Record>)>, SessionError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        Self::guard_singular()?;
        let siblings = Self::stored_siblings(branch, env, &entity).await?;
        if siblings.is_empty() {
            return Ok(None);
        }

        // Winner order, then record values only: a non-record sibling (a
        // schema-ignoring tool can write any value under this attribute) is
        // undecodable by definition and drops out here.
        let total = siblings.len();
        let mut records: Vec<Record> = siblings
            .into_iter()
            .filter_map(|artifact| match artifact.is {
                Value::Record(record) => Some(record),
                _ => None,
            })
            .collect();

        // Seed from the first sibling (in winner order) that decodes; the
        // ones ranked above it are undecodable and dropped, exactly as the
        // read fold drops them.
        let seed = records.iter().enumerate().find_map(|(index, record)| {
            record
                .realize::<F>()
                .ok()
                .map(|form| (index, (*form).clone()))
        });
        let Some((index, document)) = seed else {
            return Err(SessionError::Undecodable { siblings: total });
        };
        let seed_record = records.remove(index);
        // Undecodable siblings ranked above the seed have been consumed by
        // the seed search; hand back only what is still absorbable.
        records.drain(..index);

        // `Record`'s interior mutability is only its decode memo cache;
        // `Eq`/`Hash` cover the immutable canonical bytes, so membership in
        // the absorbed set is stable.
        #[allow(clippy::mutable_key_type)]
        let mut absorbed = HashSet::new();
        absorbed.insert(seed_record);
        let session = Self {
            entity,
            document,
            absorbed,
            attribute: PhantomData,
        };
        Ok(Some((session, records)))
    }

    /// Start a session for an entity whose attribute holds no document yet.
    ///
    /// Nothing is written until [`commit`](DocumentSession::commit). The
    /// document's identity is minted here: replicas that should converge
    /// must descend from this one stored document, not from independent
    /// creations (see the crate docs on shared ancestry).
    pub fn create(entity: Entity, document: F) -> Result<Self, SessionError> {
        Self::guard_singular()?;
        Ok(Self {
            entity,
            document,
            absorbed: HashSet::new(),
            attribute: PhantomData,
        })
    }

    /// The entity whose attribute this session edits.
    pub fn entity(&self) -> &Entity {
        &self.entity
    }

    /// The live document.
    pub fn document(&self) -> &F {
        &self.document
    }

    /// Mutable access to the live document, for edits. Edits are pending —
    /// local to this session — until [`commit`](DocumentSession::commit).
    pub fn document_mut(&mut self) -> &mut F {
        &mut self.document
    }

    /// Merge one stored sibling into the live document — the delivery point
    /// for a concurrent write that sync landed mid-session.
    ///
    /// Pending local edits survive: the sibling is merged *into* the live
    /// document, not swapped for it, so the next commit's `Replace` is
    /// inclusive rather than data-losing (spec §6.15). Absorption is
    /// idempotent — a sibling this session has already folded in (or itself
    /// committed) returns `Ok(false)` without decoding.
    ///
    /// Errors with [`SessionError::Record`] when the sibling's bytes do not
    /// decode as `F`; the live document is untouched.
    pub fn absorb(&mut self, sibling: &Record) -> Result<bool, SessionError> {
        if self.absorbed.contains(sibling) {
            return Ok(false);
        }
        let form = sibling.realize::<F>()?;
        // The live document sits in `merge`'s second slot: an
        // order-insensitive CRDT merge is unaffected, while the default
        // replace-merge resolves toward the live document — never silently
        // toward the arrival over pending local edits.
        self.document = F::merge(&form, &self.document);
        self.absorbed.insert(sibling.clone());
        Ok(true)
    }

    /// Re-scan the attribute's stored siblings and absorb every one the live
    /// document has not seen, returning how many arrived.
    ///
    /// This is the poll-driven absorption moment: call it when a pull
    /// completes (or on the app's own cadence) so the session picks up
    /// concurrent writes. Undecodable siblings are dropped deterministically,
    /// as at [`open`](DocumentSession::open). When standing subscriptions
    /// land in the repository layer, their deltas feed
    /// [`absorb`](DocumentSession::absorb) directly and this re-scan becomes
    /// unnecessary.
    pub async fn refresh<Env>(&mut self, branch: &Branch, env: &Env) -> Result<usize, SessionError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let siblings = Self::stored_siblings(branch, env, &self.entity).await?;
        let mut arrived = 0;
        for artifact in siblings {
            let Value::Record(record) = artifact.is else {
                continue;
            };
            match self.absorb(&record) {
                Ok(true) => arrived += 1,
                Ok(false) => {}
                // Foreign bytes under this attribute: dropped from the fold
                // deterministically (spec §6.9), same as at open.
                Err(SessionError::Record(_)) => {}
                Err(other) => return Err(other),
            }
        }
        Ok(arrived)
    }

    /// Commit the live document: a typed `Cardinality::One` assert, which
    /// supersedes **every** stored sibling with the session's document (an
    /// `Instruction::Replace` — spec §4.4's physical convergence).
    ///
    /// The document is encoded to its canonical bytes eagerly; committing
    /// the same canonical bytes that already stand alone is a no-op at the
    /// tree, so concurrent identical write-backs collide onto the same key.
    /// On a [`CommitError`] (e.g. a concurrent head advance losing the
    /// publish CAS) the session is unchanged — absorb what arrived via
    /// [`refresh`](DocumentSession::refresh) and commit again.
    pub async fn commit<Env>(
        &mut self,
        branch: &Branch,
        env: &Env,
    ) -> Result<Revision, SessionError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let recorded = Recorded::new(self.document.clone())?;
        let committed = recorded.record().clone();
        let revision = branch
            .transaction()
            .assert(A::of(self.entity.clone()).is(recorded))
            .commit()
            .perform(env)
            .await?;
        // The Replace left exactly one stored sibling: the committed record.
        self.absorbed.clear();
        self.absorbed.insert(committed);
        Ok(revision)
    }

    /// Every stored sibling of the attribute for `entity`, winner first.
    ///
    /// This is a raw index scan on purpose: the typed query layer projects
    /// `Cardinality::One` groups down to a single row, while the session
    /// reasons at the sibling level — which forks exist, which are already
    /// folded in.
    async fn stored_siblings<Env>(
        branch: &Branch,
        env: &Env,
        entity: &Entity,
    ) -> Result<Vec<Artifact>, SessionError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let the: ArtifactsAttribute = A::descriptor().the().clone().into();
        let stream = branch
            .claims()
            .select(ArtifactSelector::new().the(the).of(entity.clone()))
            .perform(env)
            .await?;
        let mut siblings: Vec<Artifact> = stream.try_collect().await?;
        siblings.sort_by(|a, b| winner_order(b, a));
        Ok(siblings)
    }

    /// A session only makes sense over a `Cardinality::One` attribute: a
    /// sibling set on `Cardinality::Many` is intended data, and a commit
    /// there would not supersede.
    fn guard_singular() -> Result<(), SessionError> {
        let descriptor = A::descriptor();
        if descriptor.cardinality() != Cardinality::One {
            return Err(SessionError::NotSingular {
                the: descriptor.the().to_string(),
                cardinality: descriptor.cardinality(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use super::*;

    /// `winner_order` mirrors `resolution::choose`: with causes absent the
    /// fact hash decides, `Some` beats `None`, and higher causes win.
    #[test]
    fn winner_order_mirrors_choose() {
        let entity = Entity::new().unwrap();
        let artifact = |bytes: Vec<u8>, cause: Option<Cause>| Artifact {
            the: ArtifactsAttribute::from_str("note/body").unwrap(),
            of: entity.clone(),
            is: Value::Record(Record::from(bytes)),
            cause,
        };

        let a = artifact(vec![1], None);
        let b = artifact(vec![2], None);
        let by_hash = Cause::from(&a).partial_cmp(&Cause::from(&b)).unwrap();
        assert_eq!(winner_order(&a, &b), by_hash);
        assert_eq!(winner_order(&b, &a), by_hash.reverse());

        let caused = artifact(vec![3], Some(Cause::from(&a)));
        assert_eq!(winner_order(&caused, &a), Ordering::Greater);
        assert_eq!(winner_order(&a, &caused), Ordering::Less);
    }
}
