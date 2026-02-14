//! Transport bridge between the UI panel and the inspection backend.
//!
//! The panel runs in the extension's origin (devtools panel) and cannot
//! access the host page's IndexedDB directly.  [`send`] serializes
//! requests as JSON and sends them to the content script via
//! `chrome.tabs.sendMessage`.  The content script deserializes, calls
//! [`dispatch`](crate::dispatch::dispatch), and replies.

use crate::handler::{Request, Response};
use wasm_bindgen::prelude::*;

/// Send a [`Request`] to the content script and await the [`Response`].
pub async fn send(request: Request) -> Response {
    match send_to_tab(request).await {
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
