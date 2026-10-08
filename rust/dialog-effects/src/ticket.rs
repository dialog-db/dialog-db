//! Tickets: the delegations a subject holds for the principals it grants
//! access to.
//!
//! A ticket is a UCAN container carrying a delegation chain rooted at the
//! subject, kept in the subject's own memory at `ticket/{holder}`: the
//! space [`SPACE`], in the cell named by the holder's DID. Whoever can
//! write the subject's memory can leave a ticket there or take it back;
//! whoever can sign as the holder fetches it with the `/ucan/claim`
//! command ([`CLAIM`]), which names the holder as its `sub` and the
//! subject the ticket is held in as its [`SUBJECT`] argument.
//!
//! Taking a ticket back hides it from the next claim. It does not revoke
//! the delegation: a chain already fetched keeps proving until it is
//! revoked.
//!
//! ```
//! use dialog_capability::did;
//! use dialog_effects::ticket;
//!
//! let space = did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK");
//! let holder = did!("key:z6MkiTBz1ymuepAQ4HEHYSF1H8quG5GLVVQR3djdX3mDooWp");
//!
//! let cell = ticket::reader(&space, &holder);
//! assert_eq!(cell.ability(), "/use/get/memory/cell");
//! ```

use crate::MethodExt as _;
use crate::memory::prelude::{CellExt as _, MemoryExt as _, SpaceExt as _};
use crate::memory::{Cell, Resolve};
use crate::method;
use dialog_capability::{Capability, Did, Subject};

/// The memory space tickets are kept in.
pub const SPACE: &str = "ticket";

/// The command a holder fetches its ticket with: `/ucan/claim`.
pub const CLAIM: [&str; 2] = ["ucan", "claim"];

/// The `/ucan/claim` argument naming the subject the ticket is held in.
pub const SUBJECT: &str = "sub";

/// The cell `subject` keeps `holder`'s ticket in, to read.
pub fn reader(subject: &Did, holder: &Did) -> Capability<Cell<method::Get>> {
    Subject::from(subject.clone())
        .reader()
        .memory()
        .space(SPACE)
        .cell(holder.as_str())
}

/// The cell `subject` keeps `holder`'s ticket in, to write.
pub fn writer(subject: &Did, holder: &Did) -> Capability<Cell<method::Put>> {
    Subject::from(subject.clone())
        .writer()
        .memory()
        .space(SPACE)
        .cell(holder.as_str())
}

/// Read `holder`'s ticket out of `subject`'s memory: what a `/ucan/claim`
/// by `holder` naming `subject` performs.
pub fn resolve(subject: &Did, holder: &Did) -> Capability<Resolve> {
    reader(subject, holder).invoke(Resolve)
}
