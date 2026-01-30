use crate::{Constrained, Did, Policy, PolicyBuilder, Subject};

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

    /// Collects all constrains from this capability chain into the policy builder.
    ///
    /// Each member of the chain calls `builder.push(self)` to contribute
    /// its serializable data. Consumers (like UCAN) implement `PolicyBuilder`
    /// to collect caveats in their preferred format.
    fn constrain(&self, builder: &mut impl PolicyBuilder);
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

    fn constrain(&self, _builder: &mut impl PolicyBuilder) {
        // Subject has no caveats
    }
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

    fn constrain(&self, builder: &mut impl PolicyBuilder) {
        self.capability.constrain(builder);
        self.constraint.constrain(builder);
    }
}
