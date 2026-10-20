//! The connection on wasm off the browser, such as a component on
//! `wasm32-wasip3`: there is none. WASI gives a component no WebSocket, so
//! no link opens. A watch is refused as unsupported, and the cell is read
//! by resolving it again; an invocation goes as a request.

use dialog_effects::Rejection;
use dialog_effects::memory::MemoryError;

use super::Routes;

/// A link that cannot exist: [`open`](Self::open) always refuses.
#[derive(Debug)]
pub(crate) enum Link {}

impl Link {
    pub(crate) async fn open(_url: &str, _routes: Routes) -> Result<Self, MemoryError> {
        Err(Rejection::Unsupported {
            reason: "a socket cannot be opened off the browser".into(),
        }
        .into())
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
