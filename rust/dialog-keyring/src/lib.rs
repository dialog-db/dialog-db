#![warn(missing_docs)]

//! Epoch-keyed sealing for content-addressed data.
//!
//! This is the encryption half of the design in `notes/beekem.md`, built
//! ahead of the key agreement half and deliberately independent of it. It
//! answers "what key seals this node, and how does a reader find it again"
//! without answering "how does a group agree on that key" — which is what a
//! CGKA is for, and what [`LocalKeyring`] stands in for until one lands.
//!
//! ```no_run
//! # use dialog_keyring::{Keyring, KeyringExt, LocalKeyring, KeyringError};
//! # async fn example() -> Result<(), KeyringError> {
//! let mut keyring = LocalKeyring::create([7u8; 32])?;
//!
//! let sealed = keyring.seal(b"a node buffer").await?;
//! let address = sealed.address();
//!
//! // Rotating does not disturb what is already written.
//! keyring.rotate().await?;
//! assert_eq!(keyring.open(&sealed).await?, b"a node buffer");
//!
//! // New content seals under the new epoch, at a different address.
//! let resealed = keyring.seal(b"a node buffer").await?;
//! assert_ne!(resealed.address(), address);
//! # Ok(())
//! # }
//! ```
//!
//! # The three properties this exists to pin down
//!
//! **Sealing preserves convergence.** Identical plaintext under one epoch
//! seals to identical bytes and therefore an identical address, so a prolly
//! tree diff can still prune matching subtrees without reading them. This is
//! why the nonce is derived rather than random — see [`Sealed`].
//!
//! **Rotation is uncoordinated and that is fine.** An epoch is named by the
//! hash of its record, not by a position in a sequence, so two replicas that
//! rotate during a partition mint two epochs and both survive the merge. See
//! [`epoch`].
//!
//! **Rotation costs deduplication.** The same content sealed under two epochs
//! has two addresses. Nothing already written is invalidated and correctness
//! is untouched, but a diff across an epoch boundary transfers content it
//! would otherwise have pruned. Hence [`RotationPolicy::OnDemand`] in
//! production and the aggressive policies only in tests.

pub mod epoch;
pub use epoch::{Epoch, EpochId, EpochLog};

mod error;
pub use error::KeyringError;

mod keyring;
pub use keyring::{Keyring, KeyringExt, LocalKeyring};

mod policy;
pub use policy::{RotationContext, RotationPolicy};

mod sealed;
pub use sealed::Sealed;

mod sealer;
pub use sealer::NodeSealer;
