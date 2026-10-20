//! Cross-platform time utilities.
//!
//! This module provides time utilities that work on both native and WASM targets.

pub use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Returns the current system time.
///
/// Uses `std::time::SystemTime::now()` on native and on WASI, and
/// `web_time::SystemTime::now().to_std()` in the browser.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub fn now() -> SystemTime {
    SystemTime::now()
}

/// Returns the current system time.
///
/// Uses `std::time::SystemTime::now()` on native and on WASI, and
/// `web_time::SystemTime::now().to_std()` in the browser.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub fn now() -> SystemTime {
    use web_time::web::SystemTimeExt;
    web_time::SystemTime::now().to_std()
}

/// A clock that only moves forward, for measuring how long something took
/// or how long ago it happened. Unlike [`now`], it is not thrown off by the
/// system clock being set.
pub use web_time::Instant;

/// Wait for `duration`.
///
/// On native this is a tokio timer, so it needs a tokio runtime with time
/// enabled. In a browser or worker it is the platform's `setTimeout`, so
/// it runs on the event loop of whatever global it is called in.
#[cfg(not(target_arch = "wasm32"))]
pub async fn sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

/// Wait for `duration`.
///
/// On native this is a tokio timer, so it needs a tokio runtime with time
/// enabled. In a browser or worker it is the platform's `setTimeout`, so
/// it runs on the event loop of whatever global it is called in.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub async fn sleep(duration: Duration) {
    use wasm_bindgen::prelude::wasm_bindgen;

    #[wasm_bindgen]
    extern "C" {
        // The global `setTimeout`, which a window and a worker both have.
        #[wasm_bindgen(js_name = setTimeout)]
        fn set_timeout(handler: &js_sys::Function, timeout: i32) -> wasm_bindgen::JsValue;
    }

    let milliseconds = i32::try_from(duration.as_millis()).unwrap_or(i32::MAX);
    let elapsed = js_sys::Promise::new(&mut |resolve, _| {
        set_timeout(&resolve, milliseconds);
    });
    // A timer cannot fail; a promise that is never awaited again (its
    // sleep dropped) resolves to nobody.
    let _ = wasm_bindgen_futures::JsFuture::from(elapsed).await;
}

/// Wait for `duration`.
///
/// On WASI 0.3 this is the host's monotonic clock, through
/// `wasi:clocks/monotonic-clock.wait-for`, so the component's other tasks
/// run while it waits.
#[cfg(all(target_arch = "wasm32", target_os = "wasi", target_env = "p3"))]
pub async fn sleep(duration: Duration) {
    let nanoseconds = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
    wasip3::clocks::monotonic_clock::wait_for(nanoseconds).await;
}

/// Wait for `duration`.
///
/// A WASI before 0.3 has no wait a task can yield in, so this blocks the
/// thread for the whole duration.
#[cfg(all(
    target_arch = "wasm32",
    not(target_os = "unknown"),
    not(all(target_os = "wasi", target_env = "p3"))
))]
pub async fn sleep(duration: Duration) {
    std::thread::sleep(duration);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// A sleep waits at least as long as it was asked to.
    #[dialog_common::test]
    async fn it_sleeps_for_as_long_as_asked() {
        let started = Instant::now();
        sleep(Duration::from_millis(50)).await;
        assert!(started.elapsed() >= Duration::from_millis(50));
    }

    #[test]
    fn it_returns_reasonable_timestamp() {
        let nanos = now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        // Should be after year 2020 (in nanoseconds)
        let year_2020_nanos: u128 = 1577836800 * 1_000_000_000;
        assert!(nanos > year_2020_nanos);
    }

    #[test]
    fn it_returns_increasing_values() {
        let t1 = now();
        let t2 = now();
        assert!(t2 >= t1);
    }
}
