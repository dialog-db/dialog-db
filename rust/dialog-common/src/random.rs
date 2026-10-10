//! Random bytes from the system, on every target.
//!
//! [`fill`] is `getrandom` 0.2 everywhere but on WASI. There, 0.2 reaches the
//! random source through WASI preview 1 alone, so a component would import
//! the preview 1 adapter, and with it WASI 0.2 interfaces that a host of
//! WASI 0.3 need not give. On WASI, [`fill`] uses `getrandom` 0.4, which
//! imports `wasi:random` of the target's own WASI version.

/// Fills `bytes` with random bytes from the system.
///
/// # Errors
///
/// If the random source of the system fails.
pub fn fill(bytes: &mut [u8]) -> Result<(), getrandom::Error> {
    #[cfg(not(all(target_arch = "wasm32", not(target_os = "unknown"))))]
    {
        getrandom::getrandom(bytes)
    }

    #[cfg(all(target_arch = "wasm32", not(target_os = "unknown")))]
    {
        // The source of WASI 0.2 and 0.3 cannot fail, so this error does
        // not happen in practice.
        getrandom_04::fill(bytes).map_err(|_| getrandom::Error::UNSUPPORTED)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_macros::test]
    async fn it_fills_each_byte_with_random_bytes() {
        let (mut first, mut second) = ([0u8; 32], [0u8; 32]);

        fill(&mut first).unwrap();
        fill(&mut second).unwrap();

        assert_ne!(first, [0u8; 32]);
        assert_ne!(first, second);
    }
}
