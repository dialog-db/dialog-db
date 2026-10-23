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

/// Fills `bytes` with random bytes for a fresh identifier: an entity a
/// replica mints, such as the line a branch's first commit allocates.
/// Never a key: keys draw through [`fill`].
///
/// The source is [`fill`]'s. With the `helpers` feature, a harness that
/// seeded the current thread ([`helpers::entropy`](crate::helpers::entropy))
/// gets its seed's stream instead, so a run that mints identifiers replays
/// them. Nothing else switches that on, a build without the feature has no
/// such stream, and [`fill`] never reads it.
///
/// # Errors
///
/// If the random source of the system fails.
pub fn identifier(bytes: &mut [u8]) -> Result<(), getrandom::Error> {
    #[cfg(feature = "helpers")]
    if crate::helpers::entropy::draw(bytes) {
        return Ok(());
    }
    fill(bytes)
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

    /// Unseeded, an identifier is fresh from the system on every draw: in
    /// a build without `helpers`, and in one with it (as every workspace
    /// test build is, the feature unifying across it) until a caller seeds
    /// the thread.
    #[dialog_macros::test]
    async fn it_draws_fresh_identifiers_unless_a_caller_seeded_the_thread() {
        let (mut first, mut second) = ([0u8; 32], [0u8; 32]);
        identifier(&mut first).unwrap();
        identifier(&mut second).unwrap();
        assert_ne!(first, second);
    }

    /// A seeded thread replays its identifiers, and [`fill`], the source
    /// keys draw from, stays the system's all the while.
    #[cfg(feature = "helpers")]
    #[dialog_macros::test]
    async fn it_seeds_identifiers_and_never_fill() {
        let draw = || {
            let _seeded = crate::helpers::entropy::seed(7);
            let (mut identified, mut first, mut second) = ([0u8; 32], [0u8; 32], [0u8; 32]);
            identifier(&mut identified).unwrap();
            fill(&mut first).unwrap();
            fill(&mut second).unwrap();
            assert_ne!(first, second, "fill is fresh under a seed");
            (identified, first)
        };
        let (one, two) = (draw(), draw());
        assert_eq!(one.0, two.0, "identifiers replay");
        assert_ne!(one.1, two.1, "fill does not");
    }
}
