//! The site: the same address and authorization as the permit-based UCAN
//! remote, with an exchange that performs the operation in the request
//! that proves it.

use std::sync::Arc;

use dialog_capability::access::{
    Access, Authorization as _, Authorize as AuthorizeEffect, AuthorizeError, FromCapability,
    Protocol, TimeRange,
};
use dialog_capability::{
    Ability, Capability, Constraint, Did, Effect, Fork, ForkInvocation, Provider, Site, SiteFork,
    Subject,
};
use dialog_common::time::{self, UNIX_EPOCH};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_did_web::WebResolver;
use dialog_effects::authority::{self, OperatorExt};
use dialog_effects::memory::{CellState, Edition, MemoryError};
use dialog_remote_ucan_s3::{DefaultResolver, Ucan, UcanAuthorization, UcanSite as PermitSite};

use crate::address::UcanAddress;
use crate::record::{Record, Unproven};
use crate::socket::Sockets;

/// A UCAN site that proves and performs each operation in one request.
///
/// Holds the permit-based site beside it for the addresses that ask
/// for the permit exchange.
#[derive(Debug, Clone, Default)]
pub struct UcanSite {
    permits: PermitSite,
    sockets: Sockets,
    verifier: Verifier,
    unrecorded: Unrecorded,
}

/// What a site does with a cell that holds content and no record of who
/// wrote it, as every cell did before cells held records.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Unrecorded {
    /// The content is read, unproven, as it was before cells held
    /// records. While this is so, a service that serves a cell's content
    /// without its record is believed, so a record proves a write only to
    /// a site that refuses what has none.
    #[default]
    Accept,
    /// The cell is refused as [`MemoryError::Unproven`].
    Refuse,
}

/// The resolver a site verifies a cell's record with, shared by its
/// clones.
#[derive(Clone)]
struct Verifier(Arc<DefaultResolver>);

impl Default for Verifier {
    fn default() -> Self {
        Self(Arc::new(DefaultResolver::new(WebResolver::new())))
    }
}

impl std::fmt::Debug for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Verifier")
    }
}

impl UcanSite {
    /// The permit-based site this one completes permits through.
    pub(crate) fn permits(&self) -> &PermitSite {
        &self.permits
    }

    /// The socket connections the site keeps, shared by its clones.
    pub(crate) fn sockets(&self) -> &Sockets {
        &self.sockets
    }

    /// A site whose socket connections are kept by `liveness`: how often
    /// they are pinged, and when one is given up on (see
    /// [`Liveness`](crate::socket::Liveness)).
    pub fn with_liveness(liveness: crate::socket::Liveness) -> Self {
        Self {
            sockets: Sockets::with_liveness(liveness),
            ..Self::default()
        }
    }

    /// The site, doing `unrecorded` with a cell that holds no record.
    pub fn with_unrecorded(mut self, unrecorded: Unrecorded) -> Self {
        self.unrecorded = unrecorded;
        self
    }

    /// What `state`, read from the cell `cell` of the space `space` of
    /// `subject`, holds once its record is verified: the content the
    /// record proves, at the version the store gave it.
    pub(crate) async fn open(
        &self,
        subject: &Did,
        space: &str,
        cell: &str,
        state: CellState,
    ) -> Result<CellState, MemoryError> {
        let Some(edition) = state else {
            return Ok(None);
        };
        if !Record::is_record(&edition.content) {
            return match self.unrecorded {
                Unrecorded::Accept => Ok(Some(edition)),
                Unrecorded::Refuse => Err(Unproven::Unrecorded.into()),
            };
        }
        let record = Record::decode(&edition.content).map_err(Unproven::Record)?;
        record
            .verify(subject, space, cell, self.verifier.0.as_ref())
            .await?;
        Ok(Some(Edition {
            content: record.into_content().unwrap_or_default(),
            version: edition.version,
        }))
    }
}

impl Site for UcanSite {
    type Authorization = UcanAuthorization;
    type Address = UcanAddress;
    type Fork<Fx: Effect> = UcanFork<Fx>;
}

/// Site-owned fork wrapper for [`UcanSite`].
pub struct UcanFork<Fx: Effect>(Fork<UcanSite, Fx>);

impl<Fx: Effect> From<Fork<UcanSite, Fx>> for UcanFork<Fx> {
    fn from(fork: Fork<UcanSite, Fx>) -> Self {
        Self(fork)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Fx, Env> SiteFork<Env> for UcanFork<Fx>
where
    Fx: Effect + Clone + ConditionalSend + ConditionalSync + 'static,
    Fx::Of: Constraint<Capability: ConditionalSend + ConditionalSync>,
    Capability<Fx>: Ability + ConditionalSend + ConditionalSync,
    Env: Provider<AuthorizeEffect<Ucan>> + Provider<authority::Identify> + ConditionalSync,
{
    type Site = UcanSite;
    type Effect = Fx;

    /// Mint the signed invocation for this fork's capability, the same
    /// way the permit-based site does: authority good at the moment it
    /// is presented, scoped to exactly this capability.
    async fn authorize(self, env: &Env) -> Result<ForkInvocation<UcanSite, Fx>, AuthorizeError> {
        let identity =
            authority::Identify
                .perform(env)
                .await
                .map_err(|e| AuthorizeError::Malformed {
                    detail: e.to_string(),
                })?;
        let profile = identity.profile().clone();
        let operator = identity.did();

        let scope = <Ucan as Protocol>::Access::from_capability(self.0.capability());

        let at = now_s();
        let authorization = Subject::from(profile)
            .attenuate(Access)
            .invoke(
                AuthorizeEffect::<Ucan>::new(operator, scope).during(TimeRange {
                    not_before: Some(at),
                    expiration: Some(at),
                }),
            )
            .perform(env)
            .await?;

        let invocation = authorization.invoke().await?;
        Ok(self.0.attest(UcanAuthorization::from(invocation)))
    }
}

fn now_s() -> u64 {
    time::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or_default()
}
