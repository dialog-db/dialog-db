//! The connection in a browser or worker: the platform's WebSocket, whose
//! events route each reply as it arrives.

use std::sync::{Arc, Mutex, PoisonError};

use dialog_effects::memory::MemoryError;
use futures::channel::oneshot;
use js_sys::{ArrayBuffer, Uint8Array};
use wasm_bindgen::JsCast as _;
use wasm_bindgen::closure::Closure;
use web_sys::{BinaryType, CloseEvent, Event, MessageEvent, WebSocket};

use super::{Routes, unavailable};
use crate::socket::{Reply, SUBPROTOCOL};

type Opening = Arc<Mutex<Option<oneshot::Sender<Result<(), String>>>>>;

pub(crate) struct Link {
    socket: WebSocket,
    _message: Closure<dyn FnMut(MessageEvent)>,
    _open: Closure<dyn FnMut(Event)>,
    _error: Closure<dyn FnMut(Event)>,
    _close: Closure<dyn FnMut(CloseEvent)>,
}

impl std::fmt::Debug for Link {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Link")
            .field("url", &self.socket.url())
            .finish_non_exhaustive()
    }
}

impl Link {
    pub(crate) async fn open(url: &str, routes: Routes) -> Result<Self, MemoryError> {
        let socket = WebSocket::new_with_str(url, SUBPROTOCOL)
            .map_err(|error| unavailable(format!("{error:?}")))?;
        socket.set_binary_type(BinaryType::Arraybuffer);

        let (opened, opening) = oneshot::channel();
        let opened: Opening = Arc::new(Mutex::new(Some(opened)));
        let settle = |opened: &Opening, outcome: Result<(), String>| {
            if let Some(sender) = opened.lock().unwrap_or_else(PoisonError::into_inner).take() {
                let _ = sender.send(outcome);
            }
        };

        let on_open = {
            let opened = opened.clone();
            Closure::<dyn FnMut(Event)>::new(move |_| settle(&opened, Ok(())))
        };
        let on_error = {
            let opened = opened.clone();
            Closure::<dyn FnMut(Event)>::new(move |_| {
                settle(&opened, Err("the socket could not be reached".into()))
            })
        };
        let on_close = {
            let opened = opened.clone();
            let routes = routes.clone();
            Closure::<dyn FnMut(CloseEvent)>::new(move |_| {
                settle(&opened, Err("the socket closed".into()));
                routes.close();
            })
        };
        let on_message = {
            let routes = routes.clone();
            Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
                if let Ok(buffer) = event.data().dyn_into::<ArrayBuffer>() {
                    let bytes = Uint8Array::new(&buffer).to_vec();
                    if let Ok(reply) = Reply::decode(&bytes) {
                        routes.route(reply);
                    }
                }
            })
        };
        socket.set_onopen(Some(on_open.as_ref().unchecked_ref()));
        socket.set_onerror(Some(on_error.as_ref().unchecked_ref()));
        socket.set_onclose(Some(on_close.as_ref().unchecked_ref()));
        socket.set_onmessage(Some(on_message.as_ref().unchecked_ref()));

        let link = Self {
            socket,
            _message: on_message,
            _open: on_open,
            _error: on_error,
            _close: on_close,
        };
        match opening.await {
            Ok(Ok(())) => {}
            Ok(Err(reason)) => return Err(unavailable(reason)),
            Err(_) => return Err(unavailable("the socket was dropped while opening")),
        }
        if link.socket.protocol() != SUBPROTOCOL {
            return Err(unavailable(format!(
                "the service does not speak {SUBPROTOCOL}"
            )));
        }
        Ok(link)
    }

    pub(crate) fn send(&self, frame: Vec<u8>) -> Result<(), MemoryError> {
        self.socket
            .send_with_u8_array(&frame)
            .map_err(|error| unavailable(format!("{error:?}")))
    }
}

impl Drop for Link {
    /// Detach the handlers before closing: the socket outlives this link
    /// in the browser, and its events (the close this causes among them)
    /// must not reach closures that are gone.
    fn drop(&mut self) {
        self.socket.set_onopen(None);
        self.socket.set_onerror(None);
        self.socket.set_onclose(None);
        self.socket.set_onmessage(None);
        let _ = self.socket.close();
    }
}
