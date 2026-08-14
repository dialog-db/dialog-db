//! Exercises the dedicated-worker harness path (the page spawns a module
//! worker, tests run inside it, output is relayed back to the page).

#![cfg(target_arch = "wasm32")]

use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[wasm_bindgen_test]
fn runs_inside_a_worker() {
    let global = js_sys::global();
    assert!(
        js_sys::Reflect::has(&global, &"WorkerGlobalScope".into()).unwrap_or(false),
        "expected to be running inside a worker"
    );
}

#[wasm_bindgen_test]
async fn async_worker_tests_work() {
    let value = wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&wasm_bindgen::JsValue::from(7)))
        .await
        .unwrap();
    assert_eq!(value.as_f64(), Some(7.0));
}
