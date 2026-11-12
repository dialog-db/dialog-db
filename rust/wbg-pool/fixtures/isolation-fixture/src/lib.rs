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

/// Resolves after `millis`.
async fn sleep(millis: i32) {
    let promise = js_sys::Promise::new(&mut |resolve, _| {
        web_sys::window()
            .expect("no window")
            .set_timeout_with_callback_and_timeout_and_arguments_0(&resolve, millis)
            .expect("setTimeout errored");
    });
    wasm_bindgen_futures::JsFuture::from(promise).await.unwrap();
}

/// Whether `promise` settles within `millis`.
async fn settles_within(promise: js_sys::Promise, millis: i32) -> bool {
    let late = wasm_bindgen::JsValue::from_str("late");
    let timeout = js_sys::Promise::new(&mut |resolve, _| {
        web_sys::window()
            .expect("no window")
            .set_timeout_with_callback_and_timeout_and_arguments_1(&resolve, millis, &late)
            .expect("setTimeout errored");
    });
    let first = js_sys::Promise::race(&js_sys::Array::of2(&promise, &timeout));
    wasm_bindgen_futures::JsFuture::from(first).await.unwrap() != late
}

/// Every test has a tab of its own, and Chrome shows only the latest one.
/// The others it hides: `visibilityState` is `hidden`, `hasFocus()` is
/// false, and animation frames and `ResizeObserver` deliveries are held
/// back. Run this test several times at once, one runner invocation each
/// as nextest does, and all of them pass only when the runner makes every
/// tab act as the focused one; without that most of them fail.
#[wasm_bindgen_test]
async fn a_tab_among_many_renders() {
    use wasm_bindgen::prelude::*;

    // Long enough for the sibling invocations to open their tabs.
    sleep(1500).await;

    let window = web_sys::window().expect("no window");
    let document = window.document().expect("no document");

    let frame = js_sys::Promise::new(&mut |resolve, _| {
        window
            .request_animation_frame(&resolve)
            .expect("requestAnimationFrame errored");
    });

    let target: web_sys::HtmlElement = document.create_element("div").unwrap().dyn_into().unwrap();
    target.style().set_property("width", "10px").unwrap();
    target.style().set_property("height", "10px").unwrap();
    document
        .body()
        .expect("no body")
        .append_child(&target)
        .unwrap();

    // The first delivery reports the size the element starts with; the
    // second can only follow the resize below.
    let resized = js_sys::Promise::new(&mut |resolve, _| {
        let mut deliveries = 0;
        let delivered = Closure::<dyn FnMut()>::new(move || {
            deliveries += 1;
            if deliveries == 2 {
                resolve.call0(&JsValue::NULL).unwrap();
            }
        });
        web_sys::ResizeObserver::new(delivered.as_ref().unchecked_ref())
            .expect("ResizeObserver errored")
            .observe(&target);
        delivered.forget();
    });
    sleep(200).await;
    target.style().set_property("width", "50px").unwrap();

    let frame = settles_within(frame, 8000).await;
    let resized = settles_within(resized, 8000).await;

    assert!(
        document.visibility_state() == web_sys::VisibilityState::Visible
            && document.has_focus().unwrap()
            && frame
            && resized,
        "this tab was put behind a sibling test's: visibility {:?}, focus {}, \
         animation frame {frame}, resize delivery {resized}",
        document.visibility_state(),
        document.has_focus().unwrap(),
    );
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
