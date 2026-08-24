//! When to rotate.
//!
//! Deciding *when* is separate from deciding *how*, and only the trigger may
//! be deterministic. A key derived deterministically from public state is
//! known to whoever knew the previous key; a rotation *scheduled* by public
//! state is fine, because the key it mints still comes from fresh entropy.
//!
//! The production answer is [`RotationPolicy::OnDemand`]. The others exist so
//! the test suite can produce multi-epoch trees everywhere without anyone
//! remembering to rotate — which matters, because a suite that only ever sees
//! one epoch will not notice the day something starts assuming there is only
//! one.

use dialog_common::Blake3Hash;

/// What state a policy gets to look at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RotationContext {
    /// How many blobs have been sealed under the current epoch.
    pub seals: u32,
    /// The address of the blob just sealed, if there was one.
    pub last: Option<Blake3Hash>,
}

/// When a writer should mint a new epoch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RotationPolicy {
    /// Only when asked. The production default, because rotation is not free:
    /// content sealed under different epochs no longer shares an address even
    /// when it is byte-identical, so every rotation partitions deduplication
    /// from that point forward.
    #[default]
    OnDemand,

    /// Rotate once `seals` blobs have been written under the current epoch.
    EverySeals(u32),

    /// Rotate whenever a sealed blob's address begins with this many zero
    /// bits.
    ///
    /// Deterministic and reproducible — the same writes rotate at the same
    /// points on every run — which makes it a good test policy and a bad
    /// production one. Two replicas that happen to seal the same content will
    /// both rotate here, and independently, minting two epochs rather than
    /// one. That is not a failure (concurrent epochs are supported by
    /// construction) but it is not coordination either, and no deterministic
    /// rule can make it so.
    WhenAddressLeadingZeros(u32),
}

impl RotationPolicy {
    /// Whether the writer should rotate now.
    #[must_use]
    pub fn wants_rotation(&self, context: &RotationContext) -> bool {
        match *self {
            Self::OnDemand => false,
            Self::EverySeals(n) => n > 0 && context.seals >= n,
            Self::WhenAddressLeadingZeros(bits) => context
                .last
                .as_ref()
                .is_some_and(|address| leading_zeros(address) >= bits),
        }
    }
}

/// Count the leading zero bits of an address.
fn leading_zeros(address: &Blake3Hash) -> u32 {
    let mut zeros = 0;
    for byte in address.as_bytes() {
        zeros += byte.leading_zeros();
        if *byte != 0 {
            break;
        }
    }
    zeros
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use super::*;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    fn context(seals: u32, address: Option<[u8; 32]>) -> RotationContext {
        RotationContext {
            seals,
            last: address.map(Blake3Hash::from),
        }
    }

    #[dialog_common::test]
    async fn on_demand_never_rotates_on_its_own() {
        assert!(!RotationPolicy::OnDemand.wants_rotation(&context(1_000_000, Some([0u8; 32]))));
    }

    #[dialog_common::test]
    async fn every_seals_counts() {
        let policy = RotationPolicy::EverySeals(4);

        assert!(!policy.wants_rotation(&context(3, None)));
        assert!(policy.wants_rotation(&context(4, None)));
    }

    #[dialog_common::test]
    async fn a_zero_interval_does_not_rotate_on_every_write() {
        // Guard against the reading where `EverySeals(0)` means "always".
        assert!(!RotationPolicy::EverySeals(0).wants_rotation(&context(0, None)));
    }

    #[dialog_common::test]
    async fn leading_zeros_reads_across_the_byte_boundary() {
        let policy = RotationPolicy::WhenAddressLeadingZeros(9);

        let mut address = [0u8; 32];
        address[1] = 0b0100_0000; // nine leading zeros exactly
        assert!(policy.wants_rotation(&context(0, Some(address))));

        address[1] = 0b1000_0000; // eight
        assert!(!policy.wants_rotation(&context(0, Some(address))));
    }

    #[dialog_common::test]
    async fn nothing_sealed_yet_is_not_a_trigger() {
        assert!(!RotationPolicy::WhenAddressLeadingZeros(0).wants_rotation(&context(0, None)));
    }
}
