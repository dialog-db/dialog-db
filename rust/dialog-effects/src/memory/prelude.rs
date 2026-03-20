//! Extension traits for fluent memory capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::memory::prelude::*;
//! ```

use dialog_capability::{Capability, Did, Subject, site::Site};

use super::{Cell, Memory, Publish, Resolve, Retract, Space};

/// Extension trait to start a memory capability chain.
pub trait SubjectExt {
    /// The resulting memory chain type.
    type Memory;
    /// Begin a memory capability chain.
    fn memory(self) -> Self::Memory;
}

impl SubjectExt for Subject {
    type Memory = Capability<Memory>;
    fn memory(self) -> Capability<Memory> {
        self.attenuate(Memory)
    }
}

impl SubjectExt for Did {
    type Memory = Capability<Memory>;
    fn memory(self) -> Capability<Memory> {
        Subject::from(self).attenuate(Memory)
    }
}

impl<S: Site> SubjectExt for Capability<Subject, S> {
    type Memory = Capability<Memory, S>;
    fn memory(self) -> Capability<Memory, S> {
        self.attenuate(Memory)
    }
}

/// Extension methods for scoping memory to a named space.
pub trait MemoryExt {
    /// The resulting space chain type.
    type Space;
    /// Scope to a named space.
    fn space(self, name: impl Into<String>) -> Self::Space;
}

impl<S: Site> MemoryExt for Capability<Memory, S> {
    type Space = Capability<Space, S>;
    fn space(self, name: impl Into<String>) -> Capability<Space, S> {
        self.attenuate(Space::new(name))
    }
}

/// Extension methods for scoping a space to a named cell.
pub trait SpaceExt {
    /// The resulting cell chain type.
    type Cell;
    /// Scope to a named cell within the space.
    fn cell(self, name: impl Into<String>) -> Self::Cell;
}

impl<S: Site> SpaceExt for Capability<Space, S> {
    type Cell = Capability<Cell, S>;
    fn cell(self, name: impl Into<String>) -> Capability<Cell, S> {
        self.attenuate(Cell::new(name))
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
    /// Resolve the current cell content and edition.
    fn resolve(self) -> Self::Resolve;
    /// Publish content to the cell with CAS semantics.
    fn publish(self, content: impl Into<Vec<u8>>, when: Option<Vec<u8>>) -> Self::Publish;
    /// Retract (delete) cell content with CAS semantics.
    fn retract(self, when: impl Into<Vec<u8>>) -> Self::Retract;
}

impl<S: Site> CellExt for Capability<Cell, S> {
    type Resolve = Capability<Resolve, S>;
    type Publish = Capability<Publish, S>;
    type Retract = Capability<Retract, S>;

    fn resolve(self) -> Capability<Resolve, S> {
        self.invoke(Resolve)
    }

    fn publish(self, content: impl Into<Vec<u8>>, when: Option<Vec<u8>>) -> Capability<Publish, S> {
        self.invoke(Publish::new(content, when))
    }

    fn retract(self, when: impl Into<Vec<u8>>) -> Capability<Retract, S> {
        self.invoke(Retract::new(when))
    }
}
