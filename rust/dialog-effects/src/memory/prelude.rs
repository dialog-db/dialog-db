//! Extension traits for fluent memory capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::memory::prelude::*;
//! ```

use dialog_capability::{Capability, Did, Subject};

use super::{Cell, Memory, Publish, Resolve, Retract, Space, Version};

/// Extension trait to start a memory capability chain.
pub trait MemorySubjectExt {
    /// The resulting memory chain type.
    type Memory;
    /// Begin a memory capability chain.
    fn memory(self) -> Self::Memory;
}

impl MemorySubjectExt for Subject {
    type Memory = Capability<Memory>;
    fn memory(self) -> Capability<Memory> {
        self.attenuate(Memory)
    }
}

impl MemorySubjectExt for Did {
    type Memory = Capability<Memory>;
    fn memory(self) -> Capability<Memory> {
        Subject::from(self).attenuate(Memory)
    }
}

/// Extension methods for scoping memory to a named space.
pub trait MemoryExt {
    /// The resulting space chain type.
    type Space;
    /// Scope to a named space.
    fn space(self, name: impl Into<String>) -> Self::Space;
}

impl MemoryExt for Capability<Memory> {
    type Space = Capability<Space>;
    fn space(self, name: impl Into<String>) -> Capability<Space> {
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

impl SpaceExt for Capability<Space> {
    type Cell = Capability<Cell>;
    fn cell(self, name: impl Into<String>) -> Capability<Cell> {
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
    /// Resolve the current cell content and version.
    fn resolve(self) -> Self::Resolve;
    /// Publish content to the cell. Pass `Some(version)` as `when` to
    /// require the current version to match (CAS), `None` to publish
    /// unconditionally.
    fn publish(self, content: impl Into<Vec<u8>>, when: Option<Version>) -> Self::Publish;
    /// Retract (delete) cell content with CAS semantics.
    fn retract(self, when: impl Into<Version>) -> Self::Retract;
}

impl CellExt for Capability<Cell> {
    type Resolve = Capability<Resolve>;
    type Publish = Capability<Publish>;
    type Retract = Capability<Retract>;

    fn resolve(self) -> Capability<Resolve> {
        self.invoke(Resolve)
    }

    fn publish(self, content: impl Into<Vec<u8>>, when: Option<Version>) -> Capability<Publish> {
        self.invoke(Publish::new(content, when))
    }

    fn retract(self, when: impl Into<Version>) -> Capability<Retract> {
        self.invoke(Retract::new(when))
    }
}
