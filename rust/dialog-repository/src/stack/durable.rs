//! Durable layers: trees the primary points at.
//!
//! A durable layer, `memory:<name>`, has no head of its own. The primary
//! holds two facts of the layer's entity, written together under `last`
//! (see [`Pointer`]): `dialog.branch/revision`, the entity of the
//! revision the layer stands at, and `dialog.branch/tree`, that
//! revision's tree. The layer is at that tree, read through a line that
//! names no head ([`Snapshot::detached`]), and a dispatch that writes
//! into it mints the next tree on that line and names it in the same
//! commit of the primary.
//!
//! A pointer names a revision by its entity, which no revision can be
//! read back from, so the tree fact is what finds the layer's tree. The
//! revision itself, which a commit on the layer builds on, is read out
//! of that tree: every revision a commit mints records itself in its own
//! tree, at its entity ([`RevisionRecord`]), and the record holds all a
//! head states but the tree root and the head's signature. The tree
//! root is the pointer's, and a layer's head is never published, so it
//! needs no signature.
//!
//! When two replicas each moved a layer, a pull leaves the primary's
//! pointer cells holding both claims. [`merge`] then replays this
//! replica's changes onto the other's tree, as a branch pull rebases
//! local commits over pulled ones: our writes win on the cells they
//! touch, theirs stand elsewhere.

use std::collections::HashMap;
use std::sync::Arc;

use base58::FromBase58 as _;
use dialog_capability::{Did, Provider, Subject};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::ConditionalSync;
use dialog_corpus::history::{
    CausalityCache, Claim, ContextCache, Edition, History, REVISION_ATTRIBUTE, Record,
    RevisionRecord, TreeHistory, Version, common_ancestor, novelty,
};
use dialog_corpus::tree::{CorpusNode, WriteScope, fetch_spilled};
use dialog_corpus::{
    AttributeKey, AttributeKeyPart, Changes, DialogCorpusError, Entity, EntityKeyPart, Fact, Index,
    Key, KeyViewMut as _, Pick, Relation, State, Statement as _, Update as _, Value,
    make_reference,
};
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::archive::prelude::ArchiveScope;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::{Import as BlobImport, Load as BlobLoad, Read as BlobRead};
use dialog_effects::memory::Resolve;
use dialog_search_tree::nodes;
use futures_util::{StreamExt as _, TryStreamExt as _};
use parking_lot::RwLock;

use dialog_capability::Fork;

use crate::placement::{Pointer, branch_revision_attr, branch_tree_attr, tree_value};
use crate::repository::branch::transaction::induction_ignores;
use crate::repository::source::{Caches, Live, Source, View};
use crate::{Branch, CommitError, RemoteSite, Revision, Snapshot, TreeReference, origin_of};
use crate::{Mint, Outcome};
use crate::{TreeEnv, erased};

/// A durable layer of a stack as [`Stack::layer`](crate::Stack::layer)
/// hands it out: its name and the revision it stands at, as the stack
/// last read it. The primary, [`MEMORY`](crate::MEMORY), is one too, at
/// its head; a declared `memory:<name>` stands where the primary points
/// it, and at no revision while the primary points it nowhere.
#[derive(Debug, Clone, PartialEq)]
pub struct Durable {
    name: Entity,
    revision: Option<Revision>,
}

impl Durable {
    /// A durable layer named `name`, at `revision`.
    pub(crate) fn new(name: Entity, revision: Option<Revision>) -> Self {
        Self { name, revision }
    }

    /// The layer's name: `memory:` or `memory:<name>`.
    pub fn name(&self) -> &Entity {
        &self.name
    }

    /// The revision the layer stands at, if any.
    pub fn revision(&self) -> Option<&Revision> {
        self.revision.as_ref()
    }
}

/// One claim of a durable layer's pointer, as a primary's tree holds it:
/// the revision entity its `dialog.branch/revision` names and the tree
/// its `dialog.branch/tree` names beside it, written by the same commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Pointed {
    /// The entity of the revision the layer stands at.
    pub(crate) revision: Entity,
    /// That revision's tree.
    pub(crate) tree: TreeReference,
    /// The standing of the claim in the cardinality-one election: what
    /// `last` elects the greatest of.
    standing: Option<(Edition, [u8; 32])>,
}

/// The claims of `the` on `of` that `root` holds, each with the versions
/// of the commits that wrote it.
async fn cell(
    archive: &ArchiveScope,
    root: &TreeReference,
    the: &Relation,
    of: &Entity,
    store: &dyn TreeEnv,
) -> Result<Vec<(Value, Vec<Version>)>, DialogCorpusError> {
    let tree = Index::at(archive.index(), NodeHash::from(*root.hash()));
    let start = <AttributeKey<Key> as dialog_corpus::KeyViewConstruct>::min()
        .set_attribute(AttributeKeyPart::from(the))
        .set_entity(EntityKeyPart::from(of))
        .into_key();
    let end = <AttributeKey<Key> as dialog_corpus::KeyViewConstruct>::max()
        .set_attribute(AttributeKeyPart::from(the))
        .set_entity(EntityKeyPart::from(of))
        .into_key();
    let storage = store;
    let entries = tree.stream_range(start..=end, &storage);
    futures_util::pin_mut!(entries);
    let mut claims = Vec::new();
    while let Some(entry) = entries.next().await {
        let entry = entry?;
        if let State::Added(datum) = &entry.value {
            let spilled = fetch_spilled(&storage, archive, &entry.key).await?;
            let fact = Fact::from_key_datum_with_value(&entry.key, datum, spilled)?;
            claims.push((fact.is, datum.versions().copied().collect()));
        }
    }
    Ok(claims)
}

/// The standing `versions` bring to the cardinality-one election, as
/// [`FactView::standing`](dialog_corpus::FactView::standing) computes it.
fn standing(versions: &[Version]) -> Option<(Edition, [u8; 32])> {
    versions
        .iter()
        .map(|version| (version.edition, make_reference(version.key_bytes())))
        .max()
}

/// Every pointer claim `root`, a primary's tree, holds of `layer`: each
/// `dialog.branch/revision` claim paired with the `dialog.branch/tree`
/// claim the same commit wrote. A claim of one cell with no partner in
/// the other is not a pointer this crate wrote, and is left out.
pub(crate) async fn pointed(
    archive: &ArchiveScope,
    root: &TreeReference,
    layer: &Entity,
    store: &dyn TreeEnv,
) -> Result<Vec<Pointed>, DialogCorpusError> {
    let revisions = cell(archive, root, &branch_revision_attr(), layer, store).await?;
    let trees = cell(archive, root, &branch_tree_attr(), layer, store).await?;
    let mut pointed = Vec::new();
    for (value, versions) in revisions {
        let Value::Entity(revision) = value else {
            continue;
        };
        let partner = trees.iter().find_map(|(tree, written)| {
            let shared = written.iter().any(|version| versions.contains(version));
            match tree {
                Value::String(text) if shared => tree_reference(text),
                _ => None,
            }
        });
        if let Some(tree) = partner {
            pointed.push(Pointed {
                revision,
                tree,
                standing: standing(&versions),
            });
        }
    }
    Ok(pointed)
}

/// A `dialog.branch/tree` value read back as a tree.
fn tree_reference(text: &str) -> Option<TreeReference> {
    let bytes: [u8; 32] = text.from_base58().ok()?.try_into().ok()?;
    Some(TreeReference::from(bytes))
}

/// The claim among `claims` that `last` elects: the one a read of the
/// layer's pointer returns.
pub(crate) fn elected(claims: &[Pointed]) -> Option<&Pointed> {
    claims.iter().max_by_key(|claim| claim.standing)
}

/// The revision `pointed` names, read out of its tree: the record the
/// revision wrote there, at its entity, gives all the head states but the
/// tree, which the pointer gives, and the signature, which a head never
/// published does without. The head's own count is left to the tree's
/// context region, as for a head minted before heads carried it.
pub(crate) async fn revision_of(
    archive: &ArchiveScope,
    pointed: &Pointed,
    store: &dyn TreeEnv,
) -> Result<Revision, CommitError> {
    let the: Relation = REVISION_ATTRIBUTE
        .parse()
        .map_err(|error| CommitError::Induction(format!("revision attribute: {error}")))?;
    let records = cell(archive, &pointed.tree, &the, &pointed.revision, store).await?;
    for (value, _) in records {
        let Value::Record(bytes) = value else {
            continue;
        };
        let Ok(record) = RevisionRecord::try_from_bytes(&bytes) else {
            continue;
        };
        let version = record.version();
        if version.entity() != pointed.revision || record.verify(&version).is_err() {
            continue;
        }
        let issuer: Did = record.issuer.parse().map_err(|_| {
            CommitError::Induction(format!("revision {version} names an issuer that is no DID"))
        })?;
        return Ok(Revision {
            branch: record.branch.clone(),
            issuer,
            tree: pointed.tree.clone(),
            edition: record.edition(),
            context: None,
            region: true,
            count: None,
            signature: Vec::new(),
        });
    }
    Err(CommitError::Induction(format!(
        "the tree {:?} holds no record of the revision {} the primary points at",
        pointed.tree, pointed.revision
    )))
}

/// Where a stack's durable layers stand, as last read from the primary:
/// the tree of the primary they were read at, and each layer's revision,
/// in the order of the stack's layers. A layer the primary points
/// nowhere is `None`, and reads empty.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Pinned {
    /// The tree of the primary the pointers were read at.
    pub(crate) at: Option<TreeReference>,
    /// Each layer's revision, by the layer's index.
    pub(crate) revisions: Vec<Option<Revision>>,
}

/// The durable layers of a stack: their names, the line each mints on,
/// and where the primary was last read to point them.
pub(crate) struct Durables {
    /// The layers, in the schema's order.
    layers: Vec<Entity>,
    /// The line each layer's commits mint on, by index: derived from the
    /// primary's branch and the layer, so two writers of one layer never
    /// mint the same version, and one writer's commits follow one
    /// another.
    lineages: Vec<Entity>,
    /// The repository the layers' trees live in.
    subject: Subject,
    /// The primary's caches, whose content-addressed parts the layers'
    /// lines share.
    caches: Caches,
    pinned: RwLock<Pinned>,
}

impl Durables {
    /// The durable `layers` of a stack on `primary`, by `profile`.
    pub(crate) fn new(primary: &Branch, layers: Vec<Entity>, profile: &Did) -> Self {
        let lineages = layers
            .iter()
            .map(|layer| {
                crate::branch_of(
                    primary.of(),
                    profile,
                    &format!("{}\u{1f}{layer}", primary.name()),
                )
            })
            .collect();
        Self {
            layers,
            lineages,
            subject: primary.subject(),
            caches: primary.caches(),
            pinned: RwLock::new(Pinned::default()),
        }
    }

    /// The layers, in the schema's order.
    pub(crate) fn layers(&self) -> &[Entity] {
        &self.layers
    }

    /// Where the layers stand, as last read.
    pub(crate) fn pinned(&self) -> Pinned {
        self.pinned.read().clone()
    }

    /// Hold `pinned` as where the layers stand.
    pub(crate) fn set(&self, pinned: Pinned) {
        *self.pinned.write() = pinned;
    }

    /// Whether the layers were read at a primary other than `primary`.
    pub(crate) fn stale(&self, primary: Option<&Revision>) -> bool {
        !self.layers.is_empty() && self.pinned.read().at != primary.map(|head| head.tree.clone())
    }

    /// The archive the layers' trees live in.
    pub(crate) fn archive(&self) -> ArchiveScope {
        ArchiveScope::new(self.subject.clone())
    }

    /// A line of the layer at `index`, with caches of its own for what
    /// is keyed by version: a commit on the layer that loses the
    /// primary's compare-and-swap is minted again under the same version,
    /// so nothing keyed by it may reach the primary's memos.
    fn line(&self, index: usize) -> Snapshot {
        let entries = self.caches.limits.entry_capacity();
        let caches = Caches {
            causality: CausalityCache::with_capacity(entries),
            contexts: ContextCache::with_capacity(entries),
            records: dialog_search_tree::Cache::with_capacity(entries),
            ..self.caches.clone()
        };
        Snapshot::detached(self.subject.clone(), caches, self.lineages[index].clone())
    }

    /// Each layer as a dispatch reads and writes it, at `pinned`.
    pub(crate) fn lines(&self, pinned: &Pinned) -> Vec<DurableLayer> {
        self.layers
            .iter()
            .enumerate()
            .map(|(index, layer)| DurableLayer {
                layer: layer.clone(),
                lineage: self.lineages[index].clone(),
                revision: pinned.revisions.get(index).cloned().flatten(),
                line: self.line(index),
            })
            .collect()
    }

    /// The layer named `layer` as a dispatch reads and writes it, at
    /// `revision`.
    pub(crate) fn line_of(
        &self,
        layer: &Entity,
        revision: Option<Revision>,
    ) -> Option<DurableLayer> {
        let index = self.layers.iter().position(|held| held == layer)?;
        Some(DurableLayer {
            layer: layer.clone(),
            lineage: self.lineages[index].clone(),
            revision,
            line: self.line(index),
        })
    }

    /// Each layer as a read sees it now, with its name.
    pub(crate) fn sources(&self) -> Vec<(Entity, Source)> {
        self.lines(&self.pinned())
            .into_iter()
            .map(|line| (line.layer.clone(), line.source()))
            .collect()
    }

    /// Read where `primary` points the layers.
    pub(crate) async fn resolve<Env: TreeEnv>(
        &self,
        primary: Option<&Revision>,
        env: &Env,
    ) -> Result<Pinned, CommitError> {
        let store = erased(env);
        let archive = self.archive();
        let mut revisions = Vec::with_capacity(self.layers.len());
        for layer in &self.layers {
            let revision = match primary {
                Some(primary) => {
                    let claims = pointed(&archive, &primary.tree, layer, store).await?;
                    match elected(&claims) {
                        Some(claim) => Some(revision_of(&archive, claim, store).await?),
                        None => None,
                    }
                }
                None => None,
            };
            revisions.push(revision);
        }
        Ok(Pinned {
            at: primary.map(|head| head.tree.clone()),
            revisions,
        })
    }

    /// Read where `primary` points the layers, and hold it.
    pub(crate) async fn refresh<Env: TreeEnv>(
        &self,
        primary: Option<&Revision>,
        env: &Env,
    ) -> Result<(), CommitError> {
        let pinned = self.resolve(primary, env).await?;
        self.set(pinned);
        Ok(())
    }
}

/// A durable layer of a stack as one dispatch reads and writes it: the
/// layer's name, the line its commits mint on, and the revision the
/// primary pointed it at when the dispatch began, which its reads see
/// and its commit builds on.
#[derive(Clone)]
pub(crate) struct DurableLayer {
    pub(crate) layer: Entity,
    lineage: Entity,
    pub(crate) revision: Option<Revision>,
    line: Snapshot,
}

/// What [`DurableLayer::mint`] needs of its environment: what a commit's
/// mint needs.
pub(crate) trait MintEnv:
    Provider<BlobImport>
    + Provider<BlobRead>
    + Provider<Get>
    + Provider<Put>
    + Provider<Import>
    + Provider<nodes::Write<CorpusNode>>
    + Provider<Resolve>
    + Provider<Identify>
    + Provider<Attest>
    + Provider<ArchiveLoad>
    + Provider<nodes::Read<CorpusNode>>
    + Provider<nodes::Preload<CorpusNode>>
    + Provider<BlobLoad>
    + Provider<Fork<RemoteSite, Resolve>>
    + dialog_common::Holds
    + ConditionalSync
    + 'static
{
}

impl<T> MintEnv for T where
    T: Provider<BlobImport>
        + Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Import>
        + Provider<nodes::Write<CorpusNode>>
        + Provider<Resolve>
        + Provider<Identify>
        + Provider<Attest>
        + Provider<ArchiveLoad>
        + Provider<nodes::Read<CorpusNode>>
        + Provider<nodes::Preload<CorpusNode>>
        + Provider<BlobLoad>
        + Provider<Fork<RemoteSite, Resolve>>
        + dialog_common::Holds
        + ConditionalSync
        + 'static
{
}

impl DurableLayer {
    /// The layer at the pinned revision, as a line a read sees.
    pub(crate) fn source(&self) -> Source {
        Source::detached(self.line.clone(), self.revision.clone())
    }

    /// The layer at `base`, as a commit on it builds on.
    pub(crate) fn view_at<'a>(&'a self, base: Option<&'a Revision>) -> View<'a> {
        View::at(Live::Snapshot(&self.line), base)
    }

    /// Mint `changes`, settled, on the layer at `base`: the layer's next
    /// tree, persisted, with the revision that records it in it. Nothing
    /// is published: the layer has no head, and the revision is named by
    /// the primary's commit that carries its pointer. `None` when the
    /// changes leave the tree as it was.
    pub(crate) async fn mint<Env: MintEnv>(
        &self,
        base: Option<&Revision>,
        changes: Changes,
        env: &Env,
    ) -> Result<Option<Revision>, CommitError> {
        let lineage = self.lineage.clone();
        let outcome = Mint {
            view: self.view_at(base),
            changes: changes.into_stream(),
            entries: Vec::new(),
            machinery: Vec::new(),
            scope: WriteScope::Application,
            allow_empty: false,
            canonicalize: false,
            amend: None,
            rebuild: None,
        }
        .perform(env, |_, issuer| {
            let origin = origin_of(&lineage, issuer);
            (lineage.clone(), origin)
        })
        .await?;
        Ok(match outcome {
            Outcome::Unchanged(_) => None,
            Outcome::Minted(minted) => Some(minted.revision),
        })
    }
}

/// Two histories read as one: a version's record is looked for in the
/// first, then the second. Two trees of one durable layer each hold the
/// records of their own ancestry, and a walk over both finds where they
/// meet.
struct Both<'a, A, B>(&'a A, &'a B);

impl<A: History, B: History> History for Both<'_, A, B> {
    async fn claims_at(
        &self,
        version: &Version,
        of: &Entity,
        the: &Relation,
    ) -> Result<Vec<Claim>, DialogCorpusError> {
        let claims = self.0.claims_at(version, of, the).await?;
        if !claims.is_empty() {
            return Ok(claims);
        }
        self.1.claims_at(version, of, the).await
    }

    async fn revision_record(
        &self,
        version: &Version,
    ) -> Result<Option<RevisionRecord>, DialogCorpusError> {
        match self.0.revision_record(version).await? {
            Some(record) => Ok(Some(record)),
            None => self.1.revision_record(version).await,
        }
    }
}

/// Whether `ancestor` is `of` or a revision `of` descends from.
async fn descends<Env: TreeEnv>(
    archive: &ArchiveScope,
    ancestor: &Revision,
    of: &Revision,
    env: &Env,
) -> Result<bool, CommitError> {
    let store = erased(env);
    let catalog = archive.index();
    let left = TreeHistory::from_root(catalog.clone(), ancestor.tree.hash(), store);
    let right = TreeHistory::from_root(catalog, of.tree.hash(), store);
    let met = common_ancestor(&ancestor.version(), &of.version(), &Both(&right, &left)).await?;
    Ok(met == Some(ancestor.version()))
}

/// What the revisions a replay carries did to one cell: the values
/// they asserted and retracted, and whether a write of theirs succeeded
/// the cell's claims.
#[derive(Default)]
struct Touched {
    asserted: Vec<Value>,
    retracted: Vec<Value>,
    succeeds: bool,
}

/// The changes that, applied to `onto`'s tree, carry into it what
/// `ours` changed since the two met: every revision in `ours`'s history
/// that `onto`'s does not hold is replayed by its effect on the cells
/// it wrote, read from where `ours` left each cell. A value we asserted
/// that `ours` still holds is asserted; one we retracted that `ours`
/// no longer holds is retracted; and a cell one of our writes succeeded
/// (a write under a choosing pick) is ours alone, so every claim `onto`
/// holds in it that `ours` does not is retracted. The other cells keep
/// what `onto` holds. The machinery under `dialog.` is never replayed.
async fn replay<Env: TreeEnv>(
    archive: &ArchiveScope,
    ours: &Revision,
    onto: &Revision,
    env: &Env,
) -> Result<Changes, CommitError> {
    let store = erased(env);
    let catalog = archive.index();
    let mine = TreeHistory::from_root(catalog.clone(), ours.tree.hash(), store);
    let theirs = TreeHistory::from_root(catalog, onto.tree.hash(), store);
    let novel = novelty(&ours.version(), &mine, &theirs).await?;
    // What our revisions did to each cell: the values asserted and
    // retracted, and whether a write succeeded the cell's claims.
    let mut cells: HashMap<(Entity, Relation), Touched> = HashMap::new();
    for (version, _) in &novel {
        let records: Vec<(Version, Record)> = mine.select(*version).try_collect().await?;
        for (_, record) in records {
            let succeeds = record.is_assertion() && !record.claim().cause.versions().is_empty();
            let claim = record.claim();
            if induction_ignores(claim.the.as_str()) {
                continue;
            }
            let entry = cells
                .entry((claim.of.clone(), claim.the.clone()))
                .or_default();
            match &record {
                Record::Assert(_) => entry.asserted.push(claim.is.clone()),
                Record::Retract(_) => entry.retracted.push(claim.is.clone()),
            }
            entry.succeeds |= succeeds;
        }
    }
    let mut changes = Changes::new();
    for (
        (of, the),
        Touched {
            asserted,
            retracted,
            succeeds,
        },
    ) in cells
    {
        let held: Vec<Value> = cell(archive, &ours.tree, &the, &of, store)
            .await?
            .into_iter()
            .map(|(value, _)| value)
            .collect();
        for value in asserted {
            if held.contains(&value) {
                changes.associate(the.clone(), of.clone(), value, Pick::All);
            }
        }
        for value in retracted {
            if !held.contains(&value) {
                changes.dissociate(the.clone(), of.clone(), value);
            }
        }
        if succeeds {
            for (value, _) in cell(archive, &onto.tree, &the, &of, store).await? {
                if !held.contains(&value) {
                    changes.dissociate(the.clone(), of.clone(), value);
                }
            }
        }
    }
    Ok(changes)
}

/// Settle where `layer` stands after a pull: `claims` are the pointer
/// claims the pulled primary holds of it, and `ours` the revision the
/// primary pointed it at before the pull. When the claims name more than
/// one revision, the pull merged two primaries that each moved the
/// layer, and `last` would pick one of them; instead, every revision a
/// claim names that another descends from is dropped, the ones that
/// remain are folded into one, and ours, if it remains, is replayed onto
/// that (see [`replay`]), minted on this replica's line of the layer.
/// Answers the pointer facts that leave the primary naming the one
/// revision: the retraction of every claim and the pointer to it. Empty
/// when the primary already names one revision alone.
pub(crate) async fn merge<Env: MintEnv + TreeEnv>(
    durables: &Arc<Durables>,
    layer: &Entity,
    claims: &[Pointed],
    ours: Option<&Revision>,
    env: &Env,
) -> Result<Changes, CommitError> {
    let archive = durables.archive();
    let store = erased(env);
    let mut named: Vec<Revision> = Vec::new();
    for claim in claims {
        if named.iter().all(|held| held.entity() != claim.revision) {
            named.push(revision_of(&archive, claim, store).await?);
        }
    }
    if named.len() <= 1 && claims.len() <= 1 {
        return Ok(Changes::new());
    }
    // Ours is replayed only if no named revision holds it already.
    let mut ours = ours.cloned();
    let mut theirs: Vec<Revision> = Vec::new();
    for revision in named {
        if ours.as_ref().map(Revision::entity) == Some(revision.entity()) {
            continue;
        }
        if let Some(held) = &ours {
            if descends(&archive, &revision, held, env).await? {
                continue;
            }
            if descends(&archive, held, &revision, env).await? {
                ours = None;
            }
        }
        theirs.push(revision);
    }
    let mut kept: Vec<Revision> = Vec::new();
    for (index, revision) in theirs.iter().enumerate() {
        let mut subsumed = false;
        for (other, candidate) in theirs.iter().enumerate() {
            if other != index && descends(&archive, revision, candidate, env).await? {
                subsumed = true;
                break;
            }
        }
        if !subsumed {
            kept.push(revision.clone());
        }
    }
    kept.sort_by_key(|revision| revision.entity().to_string());
    let line = durables
        .line_of(layer, None)
        .ok_or_else(|| CommitError::UnboundLayer {
            attribute: "dialog.branch/revision".into(),
            layer: layer.to_string(),
        })?;
    let mut rest = kept.into_iter();
    let mut merged = match rest.next() {
        Some(first) => first,
        None => match ours.take() {
            Some(ours) => ours,
            None => return Ok(Changes::new()),
        },
    };
    // Theirs fold first, ours last: our writes win where they touch.
    let mut replayed = rest.collect::<Vec<_>>();
    replayed.extend(ours);
    for next in replayed {
        let changes = replay(&archive, &next, &merged, env).await?;
        if let Some(minted) = line.mint(Some(&merged), changes, env).await? {
            merged = minted;
        }
    }
    let mut changes = Changes::new();
    let mut retracted: Vec<(&Entity, &TreeReference)> = Vec::new();
    for claim in claims {
        if retracted.contains(&(&claim.revision, &claim.tree)) {
            continue;
        }
        retracted.push((&claim.revision, &claim.tree));
        changes.dissociate(
            branch_revision_attr(),
            layer.clone(),
            Value::Entity(claim.revision.clone()),
        );
        changes.dissociate(branch_tree_attr(), layer.clone(), tree_value(&claim.tree));
    }
    Pointer {
        layer,
        revision: &merged,
    }
    .assert(&mut changes);
    Ok(changes)
}
