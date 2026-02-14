//! Transport bridge between the UI panel and the inspection backend.
//!
//! The UI always calls [`send`] with a [`Request`]. Depending on the
//! runtime context, `send` picks one of three transport modes:
//!
//! 1. **Direct dispatch** (standalone / same-origin): calls
//!    [`dispatch::dispatch`] in-process.  No serialization overhead.
//! 2. **Fetch dispatch** (service-worker backed): the host app has
//!    registered a dialog-inspector service worker that serves an API
//!    at `/dialog-inspector/api/*`.  The bridge sends a `fetch()` to
//!    that endpoint.
//! 3. **Message dispatch** (extension devtools panel): serializes the
//!    request as JSON and sends it to the content script via
//!    `chrome.tabs.sendMessage`.  The content script deserializes,
//!    calls `dispatch`, and replies.
//!
//! Mode detection order: extension panel → service worker → direct.

use crate::handler::{Request, Response};
use wasm_bindgen::prelude::*;

/// Send a [`Request`] and await the [`Response`].
///
/// Automatically picks the appropriate transport based on the runtime context.
pub async fn send(request: Request) -> Response {
    if is_extension_panel() {
        send_via_message(request).await
    } else if has_inspector_sw().await {
        send_via_fetch(request).await
    } else {
        crate::dispatch::dispatch(request).await
    }
}

/// Detect whether we're running inside a devtools panel (extension origin).
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

/// Check whether a dialog-inspector service worker plugin is available.
///
/// Probes the `/dialog-inspector/api/ping` endpoint which the sw-plugin
/// always handles (even before WASM loads).  Returns `true` if the SW
/// responds with 200.
async fn has_inspector_sw() -> bool {
    probe_sw().await.unwrap_or(false)
}

async fn probe_sw() -> Result<bool, JsValue> {
    let window: web_sys::Window = js_sys::global().unchecked_into();
    let promise = window.fetch_with_str("/dialog-inspector/api/ping");
    let resp: web_sys::Response = wasm_bindgen_futures::JsFuture::from(promise)
        .await?
        .unchecked_into();
    // The sw-plugin always responds 200 to /ping (with { ready: bool }).
    // A 404 means no SW plugin is installed.
    Ok(resp.ok())
}

// ── Fetch-based transport (service worker) ──────────────────────────

/// Send a request to the dialog-inspector service worker via `fetch()`.
async fn send_via_fetch(request: Request) -> Response {
    match fetch_from_sw(request).await {
        Ok(response) => response,
        Err(e) => Response::Error {
            message: format!("SW fetch error: {e:?}"),
        },
    }
}

/// Build a URL from the request, `fetch()` it, and parse the JSON response.
fn request_to_url(request: &Request) -> String {
    match request {
        Request::ListDatabases => "/dialog-inspector/api/list_databases".to_string(),
        Request::DatabaseSummary { name } => {
            format!(
                "/dialog-inspector/api/database_summary?name={}",
                js_sys::encode_uri_component(name)
            )
        }
        Request::QueryFacts {
            name,
            attribute,
            entity,
            limit,
        } => {
            let mut url = format!(
                "/dialog-inspector/api/query_facts?name={}&limit={limit}",
                js_sys::encode_uri_component(name),
            );
            if let Some(attr) = attribute {
                url.push_str(&format!(
                    "&attribute={}",
                    js_sys::encode_uri_component(attr)
                ));
            }
            if let Some(ent) = entity {
                url.push_str(&format!(
                    "&entity={}",
                    js_sys::encode_uri_component(ent)
                ));
            }
            url
        }
    }
}

async fn fetch_from_sw(request: Request) -> Result<Response, JsValue> {
    let url = request_to_url(&request);
    let window: web_sys::Window = js_sys::global().unchecked_into();
    let promise = window.fetch_with_str(&url);
    let resp: web_sys::Response =
        wasm_bindgen_futures::JsFuture::from(promise).await?.unchecked_into();

    let json_promise = resp.text()?;
    let body_val = wasm_bindgen_futures::JsFuture::from(json_promise).await?;
    let body_str = body_val
        .as_string()
        .ok_or_else(|| JsValue::from_str("response body is not a string"))?;

    serde_json::from_str::<Response>(&body_str)
        .map_err(|e| JsValue::from_str(&format!("failed to parse SW response: {e}")))
}

// ── Message-based transport (extension) ─────────────────────────────

/// Send a request to the content script via `chrome.tabs.sendMessage`.
async fn send_via_message(request: Request) -> Response {
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
