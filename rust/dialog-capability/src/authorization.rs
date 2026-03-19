use crate::{Capability, Constraint, Effect, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use std::fmt::{Debug, Formatter};

/// A capability paired with a site-specific permit (proof, access token, etc.).
///
/// Used at every authorization stage — the site type changes as
/// authorization progresses through the `Authorize` → `Redeem` pipeline.
///
/// - `C` is the constraint type (e.g., `storage::Get`)
/// - `Site` is the stage-specific proof (e.g., `S3Permit`, `AuthorizedRequest`)
pub struct Authorization<C: Constraint, Site> {
    capability: Capability<C>,
    site: Site,
}

impl<C: Constraint, Site: Clone> Clone for Authorization<C, Site>
where
    C::Capability: Clone,
{
    fn clone(&self) -> Self {
        Self {
            capability: Capability(self.capability.0.clone()),
            site: self.site.clone(),
        }
    }
}

impl<C: Constraint + Debug, Site: Debug> Debug for Authorization<C, Site>
where
    C::Capability: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authorization")
            .field("capability", &self.capability)
            .field("site", &self.site)
            .finish()
    }
}

impl<C: Constraint, Site> Authorization<C, Site> {
    /// Create a new authorization from a capability and site.
    pub fn new(capability: Capability<C>, site: Site) -> Self {
        Self { capability, site }
    }

    /// Get the capability.
    pub fn capability(&self) -> &Capability<C> {
        &self.capability
    }

    /// Get the site.
    pub fn site(&self) -> &Site {
        &self.site
    }

    /// Consume and return the inner capability.
    pub fn into_capability(self) -> Capability<C> {
        self.capability
    }

    /// Consume and return the inner site.
    pub fn into_site(self) -> Site {
        self.site
    }

    /// Consume and return both parts.
    pub fn into_parts(self) -> (Capability<C>, Site) {
        (self.capability, self.site)
    }
}

impl<Fx: Effect + Constraint, Site> Authorization<Fx, Site> {
    /// Perform the authorized capability.
    ///
    /// The authorization proof was already fully formed during `acquire`,
    /// so this just delegates to the env's `Provider<Authorization<Fx, Site>>`.
    pub async fn perform<Env>(self, env: &Env) -> Fx::Output
    where
        Env: Provider<Self> + ConditionalSend + ConditionalSync,
    {
        env.execute(self).await
    }
}
