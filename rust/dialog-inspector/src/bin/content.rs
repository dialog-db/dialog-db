//! Content script entry point for the dialog-inspector extension.
//!
//! This binary is compiled to WASM and injected into the inspected page
//! by the extension. It runs in the host page's origin, giving it access
//! to the page's IndexedDB databases.
//!
//! It listens for messages from the devtools panel (sent via
//! `chrome.tabs.sendMessage`), dispatches them through
//! [`dialog_inspector::dispatch::dispatch`], and sends the response back.
//!
//! # Message format
//!
//! Incoming: `{ __dialogInspector: true, payload: "<JSON Request>" }`
//! Response: `{ payload: "<JSON Response>" }`

fn main() {
    // Content scripts don't mount DOM — they only listen for messages.
    // On wasm32 we set up the message listener; on other targets this is a no-op.
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        console_error_panic_hook::set_once();
        setup_message_listener();
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn setup_message_listener() {
    use dialog_inspector::dispatch;
    use dialog_inspector::handler::{Request, Response};
    use js_sys::Reflect;
    use wasm_bindgen::prelude::*;
    use wasm_bindgen_futures::spawn_local;

    // chrome.runtime.onMessage.addListener(callback)
    //
    // The callback receives (message, sender, sendResponse).
    // Returning `true` from the callback tells Chrome to keep the
    // sendResponse channel open for async use.
    let callback = Closure::wrap(Box::new(
        move |message: JsValue, _sender: JsValue, send_response: js_sys::Function| -> JsValue {
            // Only handle our messages
            let is_ours = Reflect::get(&message, &"__dialogInspector".into())
                .ok()
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            if !is_ours {
                return JsValue::FALSE;
            }

            let payload_str = match Reflect::get(&message, &"payload".into())
                .ok()
                .and_then(|v| v.as_string())
            {
                Some(s) => s,
                None => return JsValue::FALSE,
            };

            let request: Request = match serde_json::from_str(&payload_str) {
                Ok(r) => r,
                Err(_) => return JsValue::FALSE,
            };

            // Dispatch asynchronously, then call sendResponse with the result
            spawn_local(async move {
                let response: Response = dispatch::dispatch(request).await;
                let response_json = serde_json::to_string(&response).unwrap_or_default();

                let reply = js_sys::Object::new();
                let _ =
                    Reflect::set(&reply, &"payload".into(), &JsValue::from_str(&response_json));
                let _ = send_response.call1(&JsValue::UNDEFINED, &reply);
            });

            // Return true to indicate we will respond asynchronously
            JsValue::TRUE
        },
    )
        as Box<dyn FnMut(JsValue, JsValue, js_sys::Function) -> JsValue>);

    // Register the listener
    let chrome = Reflect::get(&js_sys::global(), &"chrome".into()).unwrap_or(JsValue::UNDEFINED);
    if !chrome.is_undefined() {
        if let Ok(runtime) = Reflect::get(&chrome, &"runtime".into()) {
            if let Ok(on_message) = Reflect::get(&runtime, &"onMessage".into()) {
                if let Ok(add_listener) = Reflect::get(&on_message, &"addListener".into()) {
                    let add_listener: js_sys::Function = add_listener.unchecked_into();
                    let _ = add_listener.call1(&on_message, callback.as_ref());
                }
            }
        }
    }

    // Leak the closure so it lives for the page lifetime
    callback.forget();
}
