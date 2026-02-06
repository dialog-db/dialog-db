//! Cross-platform time utilities.
//!
//! This module provides time utilities that work on both native and WASM targets.

pub use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Returns the current system time.
///
/// Uses `std::time::SystemTime::now()` on native and `web_time::SystemTime::now().to_std()`
/// on WASM.
#[cfg(not(target_arch = "wasm32"))]
pub fn now() -> SystemTime {
    SystemTime::now()
}

/// Returns the current system time.
///
/// Uses `std::time::SystemTime::now()` on native and `web_time::SystemTime::now().to_std()`
/// on WASM.
#[cfg(target_arch = "wasm32")]
pub fn now() -> SystemTime {
    use web_time::web::SystemTimeExt;
    web_time::SystemTime::now().to_std()
}

#[cfg(test)]
mod tests {
    use super::*;

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
