//! Ability trait for capability chains.
//!
//! The `Ability` trait is the abstract interface for all capability chain types,
//! providing access to the subject DID and command path.

use super::policy::Policy;
use super::constrained::Constrained;
use super::subject::{Did, Subject};

#[cfg(feature = "ucan")]
use super::settings::Parameters;

/// Trait for representing an abstract capability (subject + command path).
///
/// Implemented by `Subject` and all the `Constrained<P, Of>` chains
/// that stem from it.
pub trait Ability: Sized {
    /// Subject of this capability in `did:key` format which is both a resource
    /// and a root issuer of this capability.
    fn subject(&self) -> &Did;

    /// Command path representing a level of access this capability
    /// has over the `subject` resource.
    fn command(&self) -> String;

    /// Collects parameters into given settings
    #[cfg(feature = "ucan")]
    fn parametrize(&self, parameters: &mut Parameters);
}

/// Subject represents unconstrained capability, hence
/// the `/` for command.
impl Ability for Subject {
    fn subject(&self) -> &Did {
        &self.0
    }

    fn command(&self) -> String {
        "/".into()
    }

    #[cfg(feature = "ucan")]
    fn parametrize(&self, _: &mut Parameters) {}
}

/// Constrained capabilities are also capabilities
/// that share subject with the root.
impl<P, Of> Ability for Constrained<P, Of>
where
    P: Policy,
    Of: Ability,
{
    fn subject(&self) -> &Did {
        self.capability.subject()
    }

    fn command(&self) -> String {
        let command = self.capability.command();
        // policy may restrinct capability space or just
        if let Some(segment) = P::attenuation() {
            if command == "/" {
                format!("/{}", segment.to_lowercase())
            } else {
                format!("{}/{}", command, segment.to_lowercase())
            }
        } else {
            command
        }
    }

    #[cfg(feature = "ucan")]
    fn parametrize(&self, parameters: &mut Parameters) {
        self.capability.parametrize(parameters);
        self.constraint.parametrize(parameters);
    }
}
