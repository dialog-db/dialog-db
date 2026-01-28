use super::{Constrained, Did, Policy, Subject};

#[cfg(feature = "ucan")]
use super::Parameters;

/// Trait for representing an abstract capability (subject + ability path).
///
/// Implemented by `Subject` and all the `Constrained<P, Of>` chains
/// that stem from it.
pub trait Ability: Sized {
    /// Subject of this capability in `did:key` format which is both a resource
    /// and a root issuer of this capability.
    fn subject(&self) -> &Did;

    /// Ability path representing a level of access this capability
    /// has over the `subject` resource (e.g., `/storage/get`, `/memory/publish`).
    fn ability(&self) -> String;

    /// Collects parameters into given settings
    #[cfg(feature = "ucan")]
    fn parametrize(&self, parameters: &mut Parameters);
}

/// Subject represents unconstrained capability, hence
/// the `/` for ability.
impl Ability for Subject {
    fn subject(&self) -> &Did {
        &self.0
    }

    fn ability(&self) -> String {
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

    fn ability(&self) -> String {
        let ability = self.capability.ability();
        // policy may restrict capability space or just
        if let Some(segment) = P::attenuation() {
            if ability == "/" {
                format!("/{}", segment.to_lowercase())
            } else {
                format!("{}/{}", ability, segment.to_lowercase())
            }
        } else {
            ability
        }
    }

    #[cfg(feature = "ucan")]
    fn parametrize(&self, parameters: &mut Parameters) {
        self.capability.parametrize(parameters);
        self.constraint.parametrize(parameters);
    }
}
