//! Transport bridge between the UI panel and the inspection backend.
//!
//! The UI always calls [`send`] with a [`Request`]. Depending on the
//! runtime context, `send` either:
//!
//! - **Direct dispatch** (standalone / same-origin): calls
//!   [`dispatch::dispatch`] in-process.  No serialization overhead.
//! - **Message dispatch** (extension devtools panel): serializes the
//!   request as JSON and sends it to the content script via
//!   `chrome.runtime.sendMessage` / `chrome.tabs.sendMessage`.
//!   The content script deserializes, calls `dispatch`, and replies.
//!
//! The mode is detected at runtime by checking for the presence of
//! `chrome.devtools` — if it exists we're in a devtools panel running
//! in the extension's origin and must use message passing.

use crate::handler::{Request, Response};
use wasm_bindgen::prelude::*;

/// Send a [`Request`] and await the [`Response`].
///
/// Automatically picks direct dispatch or message-based dispatch depending
/// on the runtime context.
pub async fn send(request: Request) -> Response {
    if is_extension_panel() {
        send_via_message(request).await
    } else {
        crate::dispatch::dispatch(request).await
    }
}

/// Detect whether we're running inside a devtools panel (extension origin).
///
/// If `chrome.devtools` exists, we're in the extension context and cannot
/// access the host page's IndexedDB directly.
fn is_extension_panel() -> bool {
    let Ok(chrome) = js_sys::Reflect::get(&js_sys::global(), &"chrome".into()) else {
        return false;
    };
    if chrome.is_undefined() || chrome.is_null() {
        return false;
    }
    let Ok(devtools) = js_sys::Reflect::get(&chrome, &"devtools".into()) else {
        return false;
    };
    !devtools.is_undefined() && !devtools.is_null()
}

/// Send a request to the content script via `chrome.tabs.sendMessage`.
///
/// The devtools panel gets the inspected tab ID from
/// `chrome.devtools.inspectedWindow.tabId`, then sends the request to that
/// tab.  The content script listening in that tab runs `dispatch` and
/// responds.
async fn send_via_message(request: Request) -> Response {
    let result = send_to_tab(request).await;
    match result {
        Ok(response) => response,
        Err(e) => Response::Error {
            message: format!("Message bridge error: {e:?}"),
        },
    }
}

async fn send_to_tab(request: Request) -> Result<Response, JsValue> {
    let request_json =
        serde_json::to_string(&request).map_err(|e| JsValue::from_str(&e.to_string()))?;

    // Build the message object: { __dialogInspector: true, payload: <json string> }
    let msg = js_sys::Object::new();
    js_sys::Reflect::set(&msg, &"__dialogInspector".into(), &JsValue::TRUE)?;
    js_sys::Reflect::set(&msg, &"payload".into(), &JsValue::from_str(&request_json))?;

    // Get chrome.devtools.inspectedWindow.tabId
    let chrome = js_sys::Reflect::get(&js_sys::global(), &"chrome".into())?;
    let devtools = js_sys::Reflect::get(&chrome, &"devtools".into())?;
    let inspected_window = js_sys::Reflect::get(&devtools, &"inspectedWindow".into())?;
    let tab_id = js_sys::Reflect::get(&inspected_window, &"tabId".into())?;

    // chrome.tabs.sendMessage(tabId, msg) -> Promise
    let tabs = js_sys::Reflect::get(&chrome, &"tabs".into())?;
    let send_message_fn: js_sys::Function =
        js_sys::Reflect::get(&tabs, &"sendMessage".into())?.unchecked_into();
    let promise: js_sys::Promise = send_message_fn
        .call2(&tabs, &tab_id, &msg)?
        .unchecked_into();

    let result = wasm_bindgen_futures::JsFuture::from(promise).await?;

    // The content script responds with { payload: <json string> }
    let payload_str = js_sys::Reflect::get(&result, &"payload".into())?
        .as_string()
        .ok_or_else(|| JsValue::from_str("missing payload in response"))?;

    serde_json::from_str::<Response>(&payload_str)
        .map_err(|e| JsValue::from_str(&format!("failed to parse response: {e}")))
}
