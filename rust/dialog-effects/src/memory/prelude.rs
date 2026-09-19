//! Extension traits for fluent memory capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::memory::prelude::*;
//! ```
//!
//! # Why the builder defers
//!
//! A verb is a level of the capability hierarchy, above the namespace it
//! applies to: `/use/get/memory/cell`. A caller, though, names the
//! resource before the operation — `.memory().space(s).cell(c)` and only
//! then `.publish(..)`. The chain therefore cannot be built as it is
//! written: the verb belongs at the top but is not known until the end.
//!
//! So the navigation methods accumulate names without committing to a
//! chain, and the effect method builds the whole thing at once with its
//! own verb on top. The caller writes resource-first while the
//! capability comes out verb-first, and neither has to know about the
//! other.

use dialog_capability::{Capability, Did, Policy, Subject};

use super::{Cell, Memory, Publish, Resolve, Retract, Space, Version};
use crate::{AttenuateVerb, Delete, Get, Put};

/// Extension trait to start a memory capability chain.
pub trait MemorySubjectExt {
    /// The resulting memory chain type.
    type Memory;
    /// Begin a memory capability chain.
    fn memory(self) -> Self::Memory;
}

impl MemorySubjectExt for Subject {
    type Memory = MemoryScope;
    fn memory(self) -> MemoryScope {
        MemoryScope { subject: self }
    }
}

impl MemorySubjectExt for Did {
    type Memory = MemoryScope;
    fn memory(self) -> MemoryScope {
        MemoryScope {
            subject: Subject::from(self),
        }
    }
}

/// A memory chain that has not chosen its verb yet.
///
/// Holds the subject until an effect is named; see the module note.
#[derive(Debug, Clone)]
pub struct MemoryScope {
    subject: Subject,
}

/// Extension methods for scoping memory to a named space.
pub trait MemoryExt {
    /// The resulting space chain type.
    type Space;
    /// Scope to a named space.
    fn space(self, name: impl Into<String>) -> Self::Space;
}

impl MemoryExt for MemoryScope {
    type Space = SpaceScope;
    fn space(self, name: impl Into<String>) -> SpaceScope {
        SpaceScope {
            subject: self.subject,
            space: name.into(),
        }
    }
}

/// A space chain that has not chosen its verb yet.
#[derive(Debug, Clone)]
pub struct SpaceScope {
    subject: Subject,
    space: String,
}

impl SpaceScope {
    /// The subject this chain is rooted at.
    pub fn subject(&self) -> &dialog_capability::Did {
        self.subject.did()
    }

    /// The space name.
    pub fn space_name(&self) -> &str {
        &self.space
    }
}

/// Extension methods for scoping a space to a named cell.
pub trait SpaceExt {
    /// The resulting cell chain type.
    type Cell;
    /// Scope to a named cell within the space.
    fn cell(self, name: impl Into<String>) -> Self::Cell;
}

impl SpaceExt for SpaceScope {
    type Cell = CellScope;
    fn cell(self, name: impl Into<String>) -> CellScope {
        CellScope {
            subject: self.subject,
            space: self.space,
            cell: name.into(),
        }
    }
}

/// A cell chain that has not chosen its verb yet.
///
/// The names are held until an effect is named, at which point the whole
/// capability is built with that effect's verb at the top.
#[derive(Debug, Clone)]
pub struct CellScope {
    subject: Subject,
    space: String,
    cell: String,
}

impl CellScope {
    /// The subject this chain is rooted at.
    pub fn subject(&self) -> &dialog_capability::Did {
        self.subject.did()
    }

    /// The space name this cell lives in.
    pub fn space_name(&self) -> &str {
        &self.space
    }

    /// The cell name.
    pub fn cell_name(&self) -> &str {
        &self.cell
    }

    /// Build the chain under `V`, the verb of the effect about to be
    /// invoked.
    fn under<V>(self) -> Capability<Cell<V>>
    where
        V: crate::Verb,
        V::Of: dialog_capability::Constraint,
        Subject: AttenuateVerb<V>,
    {
        AttenuateVerb::verb(self.subject)
            .attenuate(Memory::<V>::new())
            .attenuate(Space::<V>::new(self.space))
            .attenuate(Cell::<V>::new(self.cell))
    }
}

/// Extension methods for invoking effects on a cell.
pub trait CellExt {
    /// The resulting resolve chain type.
    type Resolve;
    /// The resulting publish chain type.
    type Publish;
    /// The resulting retract chain type.
    type Retract;
    /// Resolve the current cell content and version.
    fn resolve(self) -> Self::Resolve;
    /// Publish content to the cell. Pass `Some(version)` as `when` to
    /// require the current version to match (CAS), `None` to publish
    /// unconditionally.
    fn publish(self, content: impl Into<Vec<u8>>, when: Option<Version>) -> Self::Publish;
    /// Retract (delete) cell content with CAS semantics.
    fn retract(self, when: impl Into<Version>) -> Self::Retract;
}

impl CellExt for CellScope {
    type Resolve = Capability<Resolve>;
    type Publish = Capability<Publish>;
    type Retract = Capability<Retract>;

    fn resolve(self) -> Capability<Resolve> {
        self.under::<Get>().invoke(Resolve)
    }

    fn publish(self, content: impl Into<Vec<u8>>, when: Option<Version>) -> Capability<Publish> {
        self.under::<Put>().invoke(Publish::new(content, when))
    }

    fn retract(self, when: impl Into<Version>) -> Capability<Retract> {
        self.under::<Delete>().invoke(Retract::new(when))
    }
}

/// Field accessors on `Capability<Resolve>`.
pub trait ResolveExt {
    /// Get the space name from the capability chain.
    fn space(&self) -> &str;
    /// Get the cell name from the capability chain.
    fn cell(&self) -> &str;
}

impl ResolveExt for Capability<Resolve> {
    fn space(&self) -> &str {
        &Space::<Get>::of(self).space
    }

    fn cell(&self) -> &str {
        &Cell::<Get>::of(self).cell
    }
}

/// Field accessors on `Capability<Publish>`.
pub trait PublishExt {
    /// Get the space name from the capability chain.
    fn space(&self) -> &str;
    /// Get the cell name from the capability chain.
    fn cell(&self) -> &str;
    /// Get the content to publish.
    fn content(&self) -> &[u8];
    /// Get the expected version (when condition).
    fn when(&self) -> Option<&Version>;
}

impl PublishExt for Capability<Publish> {
    fn space(&self) -> &str {
        &Space::<Put>::of(self).space
    }

    fn cell(&self) -> &str {
        &Cell::<Put>::of(self).cell
    }

    fn content(&self) -> &[u8] {
        &Publish::of(self).content
    }

    fn when(&self) -> Option<&Version> {
        Publish::of(self).when.as_ref()
    }
}

/// Field accessors on `Capability<Retract>`.
pub trait RetractExt {
    /// Get the space name from the capability chain.
    fn space(&self) -> &str;
    /// Get the cell name from the capability chain.
    fn cell(&self) -> &str;
    /// Get the expected version (when condition).
    fn when(&self) -> &Version;
}

impl RetractExt for Capability<Retract> {
    fn space(&self) -> &str {
        &Space::<Delete>::of(self).space
    }

    fn cell(&self) -> &str {
        &Cell::<Delete>::of(self).cell
    }

    fn when(&self) -> &Version {
        &Retract::of(self).when
    }
}
