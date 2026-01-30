use crate::{Constrained, Did, Parameters, Policy, Subject};
use crate::settings::Settings;

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

    /// Collects parameters into the given collector.
    ///
    /// This method walks the capability chain and calls `params.set()` for each
    /// constraint's fields. The `Parameters` trait allows consumers to decide
    /// the output format (e.g., IPLD for UCAN invocations).
    fn parametrize<P: Parameters>(&self, params: &mut P);
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

    fn parametrize<P: Parameters>(&self, _params: &mut P) {}
}

/// Constrained capabilities are also capabilities
/// that share subject with the root.
impl<C, Of> Ability for Constrained<C, Of>
where
    C: Policy,
    Of: Ability,
{
    fn subject(&self) -> &Did {
        self.capability.subject()
    }

    fn ability(&self) -> String {
        let ability = self.capability.ability();
        // policy may restrict capability space or just
        if let Some(segment) = C::attenuation() {
            if ability == "/" {
                format!("/{}", segment.to_lowercase())
            } else {
                format!("{}/{}", ability, segment.to_lowercase())
            }
        } else {
            ability
        }
    }

    fn parametrize<P: Parameters>(&self, params: &mut P) {
        self.capability.parametrize(params);
        Settings::parametrize(&self.constraint, params);
    }
}
