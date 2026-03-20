//! Extension traits for fluent memory capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::memory::prelude::*;
//! ```

use dialog_capability::{AuthorizationRequest, Capability, Claim, Did, Subject};

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

impl<'a, A: ?Sized> SubjectExt for Claim<'a, A, Subject> {
    type Memory = Claim<'a, A, Memory>;
    fn memory(self) -> Claim<'a, A, Memory> {
        self.attenuate(Memory)
    }
}

impl<'a, S: ?Sized> SubjectExt for AuthorizationRequest<'a, S, Subject> {
    type Memory = AuthorizationRequest<'a, S, Memory>;
    fn memory(self) -> AuthorizationRequest<'a, S, Memory> {
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

impl MemoryExt for Capability<Memory> {
    type Space = Capability<Space>;
    fn space(self, name: impl Into<String>) -> Capability<Space> {
        self.attenuate(Space::new(name))
    }
}

impl<'a, A: ?Sized> MemoryExt for Claim<'a, A, Memory> {
    type Space = Claim<'a, A, Space>;
    fn space(self, name: impl Into<String>) -> Claim<'a, A, Space> {
        self.attenuate(Space::new(name))
    }
}

impl<'a, S: ?Sized> MemoryExt for AuthorizationRequest<'a, S, Memory> {
    type Space = AuthorizationRequest<'a, S, Space>;
    fn space(self, name: impl Into<String>) -> AuthorizationRequest<'a, S, Space> {
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

impl<'a, A: ?Sized> SpaceExt for Claim<'a, A, Space> {
    type Cell = Claim<'a, A, Cell>;
    fn cell(self, name: impl Into<String>) -> Claim<'a, A, Cell> {
        self.attenuate(Cell::new(name))
    }
}

impl<'a, S: ?Sized> SpaceExt for AuthorizationRequest<'a, S, Space> {
    type Cell = AuthorizationRequest<'a, S, Cell>;
    fn cell(self, name: impl Into<String>) -> AuthorizationRequest<'a, S, Cell> {
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

impl CellExt for Capability<Cell> {
    type Resolve = Capability<Resolve>;
    type Publish = Capability<Publish>;
    type Retract = Capability<Retract>;

    fn resolve(self) -> Capability<Resolve> {
        self.invoke(Resolve)
    }

    fn publish(self, content: impl Into<Vec<u8>>, when: Option<Vec<u8>>) -> Capability<Publish> {
        self.invoke(Publish::new(content, when))
    }

    fn retract(self, when: impl Into<Vec<u8>>) -> Capability<Retract> {
        self.invoke(Retract::new(when))
    }
}

impl<'a, A: ?Sized> CellExt for Claim<'a, A, Cell> {
    type Resolve = Claim<'a, A, Resolve>;
    type Publish = Claim<'a, A, Publish>;
    type Retract = Claim<'a, A, Retract>;

    fn resolve(self) -> Claim<'a, A, Resolve> {
        self.invoke(Resolve)
    }

    fn publish(self, content: impl Into<Vec<u8>>, when: Option<Vec<u8>>) -> Claim<'a, A, Publish> {
        self.invoke(Publish::new(content, when))
    }

    fn retract(self, when: impl Into<Vec<u8>>) -> Claim<'a, A, Retract> {
        self.invoke(Retract::new(when))
    }
}

impl<'a, S: ?Sized> CellExt for AuthorizationRequest<'a, S, Cell> {
    type Resolve = AuthorizationRequest<'a, S, Resolve>;
    type Publish = AuthorizationRequest<'a, S, Publish>;
    type Retract = AuthorizationRequest<'a, S, Retract>;

    fn resolve(self) -> AuthorizationRequest<'a, S, Resolve> {
        self.invoke(Resolve)
    }

    fn publish(
        self,
        content: impl Into<Vec<u8>>,
        when: Option<Vec<u8>>,
    ) -> AuthorizationRequest<'a, S, Publish> {
        self.invoke(Publish::new(content, when))
    }

    fn retract(self, when: impl Into<Vec<u8>>) -> AuthorizationRequest<'a, S, Retract> {
        self.invoke(Retract::new(when))
    }
}
