//! Fixture tests for wbg-pool. The two storage tests write the same
//! localStorage key and assert it was absent beforehand: they can only both
//! pass when each test runs on its own fresh origin, which is exactly the
//! isolation wbg-pool provides. Under a shared origin (for example a whole
//! binary executed in one page) the second test fails.

#![cfg(target_arch = "wasm32")]

use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

fn storage() -> web_sys::Storage {
    web_sys::window()
        .expect("no window")
        .local_storage()
        .expect("localStorage errored")
        .expect("no localStorage")
}

const KEY: &str = "wbg-pool-isolation-canary";

#[wasm_bindgen_test]
fn storage_isolation_first() {
    let storage = storage();
    assert!(
        storage.get_item(KEY).unwrap().is_none(),
        "state leaked in from another test: this test did not get a fresh origin"
    );
    storage.set_item(KEY, "first").unwrap();
}

#[wasm_bindgen_test]
fn storage_isolation_second() {
    let storage = storage();
    assert!(
        storage.get_item(KEY).unwrap().is_none(),
        "state leaked in from another test: this test did not get a fresh origin"
    );
    storage.set_item(KEY, "second").unwrap();
}

#[wasm_bindgen_test]
async fn async_tests_work() {
    let value = wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&wasm_bindgen::JsValue::from(42)))
        .await
        .unwrap();
    assert_eq!(value.as_f64(), Some(42.0));
}

#[wasm_bindgen_test]
fn console_output_is_captured() {
    web_sys::console::log_1(&"console output from a passing test".into());
}

#[wasm_bindgen_test]
#[ignore]
fn deliberately_fails() {
    web_sys::console::log_1(&"console context before the failure".into());
    panic!("this test exists to exercise the failure path");
}
