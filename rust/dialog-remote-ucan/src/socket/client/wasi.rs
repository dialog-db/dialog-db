//! The connection on WASI: none. A WASI build has no socket to open, so
//! every attempt is refused as unavailable, and what would have gone over
//! the socket goes as requests, as it does while a socket cannot be opened.

use dialog_effects::memory::MemoryError;

use super::{Routes, unavailable};

/// A connection that a WASI build never holds.
#[derive(Debug)]
pub(crate) enum Link {}

impl Link {
    pub(crate) async fn open(_url: &str, _routes: Routes) -> Result<Self, MemoryError> {
        Err(unavailable("a WASI build has no socket"))
    }

    pub(crate) fn send(&self, _frame: Vec<u8>) -> Result<(), MemoryError> {
        match *self {}
    }

    pub(crate) fn ping(&self) -> Result<(), MemoryError> {
        match *self {}
    }

    pub(crate) fn close(&self, _reason: &str) {
        match *self {}
    }
}
