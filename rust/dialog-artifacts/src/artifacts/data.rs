//! Internal data representation for storing artifacts in indexes.
//!
//! This module defines the [`Datum`] type: the per-entry payload stored
//! alongside a key in the prolly tree indexes.
//!
//! The key already carries the entity, attribute, value type, and (for a value
//! within the inline threshold) the value itself in order-preserving form, so
//! the payload no longer duplicates them. For a *spilled* value (whose key
//! carries just a 32-byte reference), the raw value bytes are not in the payload
//! either: they live as a content-addressed block in the archive block store,
//! keyed by that same 32-byte reference (written on commit by
//! [`ArtifactTreeExt::apply`](crate::tree::ArtifactTreeExt::apply) and fetched
//! on read). The payload holds only the [`Cause`], which the key cannot
//! reconstruct. A [`Artifact`] is reconstructed from the key plus this payload
//! (plus the fetched spilled block, if any) by
//! [`Artifact::from_key_datum`]/[`Artifact::from_key_datum_with_value`].

use rkyv::Archive;
use serde::{Deserialize, Serialize};

use crate::ValueType;

use crate::{Artifact, Cause, history::Version};

#[cfg(doc)]
use crate::{Artifacts, Attribute, Entity, Value};

/// A [`Datum`] is the per-entry payload stored against a key in the
/// [`Artifacts`] indexes: the parts of a fact the key does not already carry.
#[derive(
    Clone, Debug, PartialEq, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct Datum {
    /// The [`Cause`] of this fact, if any: a reference to an ancestor version
    /// with a different [`Value`].
    pub cause: Option<Cause>,
    /// An opaque record carried ONLY by the blob index
    /// ([`BlobRecord`](crate::BlobRecord)), never by an EAV/AEV/VAE fact. Blob
    /// keys occupy a tag range disjoint from the fact indexes, so a blob
    /// entry's `Datum` is never seen by the fact scan; this field is its
    /// storage. It is NOT the raw bytes of a spilled value — those live as a
    /// content-addressed block in the archive, keyed by the key's 32-byte
    /// reference.
    pub blob: Option<Vec<u8>>,
    /// The [`Version`] of the revision that produced this [`Datum`], when it
    /// was committed through a version-tagged write (see
    /// [`ArtifactTreeExt::apply_versioned`](crate::tree::ArtifactTreeExt::apply_versioned)).
    /// Data committed directly through [`Artifacts`] carries no version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<Version>,
    /// Additional claim versions collapsed into this entry. The fact
    /// orderings address a claim by `(entity, attribute, value)`, so two
    /// writers' claims of the *identical* value stand at ONE key:
    /// [`Datum::version`] carries one of them and this field the rest,
    /// sorted and deduplicated. Everything that reasons about the entry's
    /// claims consults the whole set ([`Datum::versions`]): a retraction
    /// covers all of them, a covering record retires only the versions it
    /// names (the fact stays live while any collapsed claim remains
    /// uncovered), and merge contests union the two sides' sets. Always
    /// empty on history records and unversioned data.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub collapsed: Vec<Version>,
    /// For history records (entries under
    /// [`HISTORY_KEY_TAG`](crate::HISTORY_KEY_TAG)): the versions of the
    /// prior claims on the same `(entity, attribute)` that this record
    /// supersedes. Always empty on index data.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supersedes: Vec<Version>,
    /// For history records: whether this record withdraws (retracts) its
    /// value rather than asserting it. Always `false` on index data.
    #[serde(default)]
    pub retraction: bool,
}

impl Datum {
    /// The payload for `artifact`: only the cause. A spilled value's bytes are
    /// written separately as a content-addressed block in the archive (keyed by
    /// the key's 32-byte reference), so the payload never carries value bytes.
    pub fn for_artifact(artifact: &Artifact) -> Self {
        Self {
            cause: artifact.cause.clone(),
            blob: None,
            version: None,
            collapsed: Vec::new(),
            supersedes: Vec::new(),
            retraction: false,
        }
    }

    /// Every claim version this entry stands for: the primary
    /// [`version`](Datum::version) plus the [`collapsed`](Datum::collapsed)
    /// set.
    pub fn versions(&self) -> impl Iterator<Item = &Version> {
        self.version.iter().chain(self.collapsed.iter())
    }

    /// Fold another claim's versions into this entry (the two claims stand
    /// at the same key, i.e. assert the same fact), CANONICALIZING the
    /// result: the union is sorted and deduplicated, the smallest version
    /// becomes the primary and the rest the collapsed set. Canonical form
    /// is load-bearing for convergence: two replicas can reach the same
    /// claim-version set through different intermediate entries (one saw
    /// the claims arrive one by one, the other received an already-fused
    /// copy with observed versions stripped by the R1 screen), and their
    /// entries must be byte-identical whenever their sets agree — an
    /// input-dependent primary would leave same-set entries with
    /// different bytes that no further exchange can reconcile (every
    /// version being mutually observed, the screens strip everything and
    /// the trees never converge). [`retire_covered`](Datum::retire_covered)
    /// re-canonicalizes the same way.
    pub fn absorb_versions<'a>(&mut self, versions: impl IntoIterator<Item = &'a Version>) {
        let mut all: Vec<Version> = self.versions().copied().collect();
        all.extend(versions.into_iter().copied());
        all.sort();
        all.dedup();
        let mut all = all.into_iter();
        self.version = all.next().or(self.version);
        self.collapsed = all.collect();
    }

    /// Retire the claims `covered` names from this entry: `None` when every
    /// claim is covered (the fact dies), or the entry standing on its
    /// surviving claims in the same canonical form
    /// [`absorb_versions`](Datum::absorb_versions) maintains (smallest
    /// surviving version primary, rest collapsed, sorted) so both replicas
    /// of a partial retirement produce identical bytes.
    pub fn retire_covered(&self, covered: &[Version]) -> Option<Datum> {
        let mut survivors: Vec<Version> = self
            .versions()
            .filter(|version| !covered.contains(version))
            .copied()
            .collect();
        if self.version.is_none() {
            // An unversioned entry cannot be covered by version.
            return Some(self.clone());
        }
        survivors.sort();
        survivors.dedup();
        let mut survivors = survivors.into_iter();
        let primary = survivors.next()?;
        Some(Datum {
            version: Some(primary),
            collapsed: survivors.collect(),
            ..self.clone()
        })
    }
}

impl ValueType for Datum {}
