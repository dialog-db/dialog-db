//! Extension traits for fluent memory capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::memory::prelude::*;
//! ```
//!
//! A chain is written in the order the path reads: the method, then the
//! namespace, then the resource, then the effect.
//!
//! ```text
//! subject.get().memory().space(s).cell(c).resolve()
//!         │     │                  │      └ the effect
//!         │     │                  └ the resource: `cell`
//!         │     └ the namespace: `memory`
//!         └ the method: `/use` and `get`
//!
//!                                       = /use/get/memory/cell
//! ```
//!
//! The method comes first because that is where it sits in the path.
//! Naming it up front is also what lets the chain be built as it is
//! written, rather than accumulated and assembled at the end.

use dialog_capability::{Capability, Constraint, Policy};

use super::{Cell, Memory, Publish, Resolve, Retract, Space, Version};
use crate::{Method, MethodExt as _, method};

/// Scope a method to the memory namespace.
pub trait MemoryExt {
    /// The resulting memory chain type.
    type Memory;
    /// Scope to memory.
    fn memory(self) -> Self::Memory;
}

impl<M: Method> MemoryExt for Capability<M>
where
    M::Of: Constraint,
{
    type Memory = Capability<Memory<M>>;
    fn memory(self) -> Self::Memory {
        self.attenuate(Memory::new())
    }
}

/// A subject reaches its memory without naming a method, because a
/// handle held across reads and writes has no one method to name. The
/// operation performed on it picks the method.
impl MemoryExt for dialog_capability::Subject {
    type Memory = MemoryScope;
    fn memory(self) -> Self::Memory {
        MemoryScope { subject: self }
    }
}

impl MemoryExt for dialog_capability::Did {
    type Memory = MemoryScope;
    fn memory(self) -> Self::Memory {
        MemoryScope {
            subject: dialog_capability::Subject::from(self),
        }
    }
}

/// A subject's memory, named but not yet reached by any method.
#[derive(Debug, Clone, PartialEq)]
pub struct MemoryScope {
    subject: dialog_capability::Subject,
}

impl MemoryScope {
    /// Name a space within it.
    pub fn space(&self, name: impl Into<String>) -> SpaceScope {
        SpaceScope::new(self.subject.clone(), name)
    }
}

/// Scope memory to a named space.
pub trait SpaceExt {
    /// The resulting space chain type.
    type Space;
    /// Scope to a named space.
    fn space(self, name: impl Into<String>) -> Self::Space;
}

impl<M: Method> SpaceExt for Capability<Memory<M>>
where
    M::Of: Constraint,
{
    type Space = Capability<Space<M>>;
    fn space(self, name: impl Into<String>) -> Self::Space {
        self.attenuate(Space::new(name))
    }
}

/// Scope a space to a named cell.
pub trait CellExt {
    /// The resulting cell chain type.
    type Cell;
    /// Scope to a named cell within the space.
    fn cell(self, name: impl Into<String>) -> Self::Cell;
}

impl<M: Method> CellExt for Capability<Space<M>>
where
    M::Of: Constraint,
{
    type Cell = Capability<Cell<M>>;
    fn cell(self, name: impl Into<String>) -> Self::Cell {
        self.attenuate(Cell::new(name))
    }
}

/// Read a cell.
pub trait ResolveCellExt {
    /// Resolve the current cell content and version.
    fn resolve(self) -> Capability<Resolve>;
}

impl ResolveCellExt for Capability<Cell<method::Get>> {
    fn resolve(self) -> Capability<Resolve> {
        self.invoke(Resolve)
    }
}

/// Write a cell.
pub trait PublishCellExt {
    /// Publish content to the cell. Pass `Some(version)` as `when` to
    /// require the current version to match (CAS), `None` to publish
    /// unconditionally.
    fn publish(self, content: impl Into<Vec<u8>>, when: Option<Version>) -> Capability<Publish>;
}

impl PublishCellExt for Capability<Cell<method::Put>> {
    fn publish(self, content: impl Into<Vec<u8>>, when: Option<Version>) -> Capability<Publish> {
        self.invoke(Publish::new(content, when))
    }
}

/// Empty a cell.
pub trait RetractCellExt {
    /// Retract (delete) cell content with CAS semantics.
    fn retract(self, when: impl Into<Version>) -> Capability<Retract>;
}

impl RetractCellExt for Capability<Cell<method::Delete>> {
    fn retract(self, when: impl Into<Version>) -> Capability<Retract> {
        self.invoke(Retract::new(when))
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
        &Space::of(self).space
    }

    fn cell(&self) -> &str {
        &Cell::of(self).cell
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
        &Space::of(self).space
    }

    fn cell(&self) -> &str {
        &Cell::of(self).cell
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
        &Space::of(self).space
    }

    fn cell(&self) -> &str {
        &Cell::of(self).cell
    }

    fn when(&self) -> &Version {
        &Retract::of(self).when
    }
}

/// A cell named but not yet reached by any method.
///
/// A handle that reads, writes and deletes the same cell needs all
/// three chains, and a chain commits to one method by construction. So
/// the handle keeps the names and builds whichever chain the operation
/// asks for.
///
/// This is the shape a caller wants when the method is decided later
/// than the resource -- a stored handle, a reference passed around. A
/// caller writing a chain inline names the method first instead.
#[derive(Debug, Clone, PartialEq)]
pub struct CellScope {
    subject: dialog_capability::Subject,
    space: String,
    cell: String,
}

impl CellScope {
    /// Name a cell without choosing a method.
    pub fn new(
        subject: dialog_capability::Subject,
        space: impl Into<String>,
        cell: impl Into<String>,
    ) -> Self {
        Self {
            subject,
            space: space.into(),
            cell: cell.into(),
        }
    }

    /// The subject this cell belongs to.
    pub fn subject(&self) -> &dialog_capability::Did {
        self.subject.did()
    }

    /// The space this cell lives in.
    pub fn space_name(&self) -> &str {
        &self.space
    }

    /// The cell's name.
    pub fn cell_name(&self) -> &str {
        &self.cell
    }

    /// The chain for reading it.
    pub fn read(&self) -> Capability<Cell<method::Get>> {
        self.subject
            .clone()
            .get()
            .memory()
            .space(self.space.clone())
            .cell(self.cell.clone())
    }

    /// The chain for writing it.
    pub fn write(&self) -> Capability<Cell<method::Put>> {
        self.subject
            .clone()
            .put()
            .memory()
            .space(self.space.clone())
            .cell(self.cell.clone())
    }

    /// The chain for emptying it.
    pub fn empty(&self) -> Capability<Cell<method::Delete>> {
        self.subject
            .clone()
            .delete()
            .memory()
            .space(self.space.clone())
            .cell(self.cell.clone())
    }

    /// Read the cell's current content and version.
    pub fn resolve(&self) -> Capability<Resolve> {
        self.read().resolve()
    }

    /// Publish content to the cell. `when` is the version the write
    /// expects to replace, or `None` to write unconditionally.
    pub fn publish(
        &self,
        content: impl Into<Vec<u8>>,
        when: Option<Version>,
    ) -> Capability<Publish> {
        self.write().publish(content, when)
    }

    /// Empty the cell, expecting it to hold `when`.
    pub fn retract(&self, when: impl Into<Version>) -> Capability<Retract> {
        self.empty().retract(when)
    }
}

/// A space named but not yet reached by any method.
#[derive(Debug, Clone, PartialEq)]
pub struct SpaceScope {
    subject: dialog_capability::Subject,
    space: String,
}

impl SpaceScope {
    /// Name a space without choosing a method.
    pub fn new(subject: dialog_capability::Subject, space: impl Into<String>) -> Self {
        Self {
            subject,
            space: space.into(),
        }
    }

    /// The subject this space belongs to.
    pub fn subject(&self) -> &dialog_capability::Did {
        self.subject.did()
    }

    /// The space's name.
    pub fn space_name(&self) -> &str {
        &self.space
    }

    /// Name a cell within it.
    pub fn cell(&self, name: impl Into<String>) -> CellScope {
        CellScope::new(self.subject.clone(), self.space.clone(), name)
    }
}
