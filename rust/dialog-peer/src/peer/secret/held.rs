//! Principals whose key the peer holds, such as a space, shared with more
//! holders, revoked from one, handed from one holder to another, or
//! carried from another branch of the peer's space.
//!
//! Every key is opened, used and dropped inside `perform`: the peer's own
//! copy opens it, or the account's, through a custodian of the account the
//! principal is held for. Nothing here hands a key out.

use super::{
    ACCOUNT, SpaceVaultExt as _, Vault, VaultKey, copy, held_context, opened, reissue, retain,
    seal_key, sealable, session_writes_nothing, unavailable, unopened,
};
use crate::peer::{Mode, Peer, PeerSpace};
use dialog_credentials::secret::SealedSecret;
use dialog_credentials::{Ed25519Signer, SignerCredential};
use dialog_effects::credential::CredentialError;
use dialog_repository::secrets::{self, HeldPrincipal};
use dialog_repository::{Branch, BranchReference, RepositoryMemoryExt as _};
use dialog_ucan::UcanDelegation;
use dialog_varsig::{Did, Principal as _};

impl<S: PeerSpace, M: Mode> Peer<S, M> {
    /// The principal `principal`, whose key this peer holds: a space it
    /// created or adopted, or another principal held for its account.
    pub fn held_principal(&self, principal: &Did) -> HeldReference {
        HeldReference {
            principal: principal.clone(),
        }
    }
}

/// A principal whose key the peer holds, named by its DID. Created by
/// [`Peer::held_principal`].
pub struct HeldReference {
    principal: Did,
}

impl HeldReference {
    /// Share the principal with `holder`: seal a copy of its key to
    /// `holder`, and have it delegate to `holder`. Those it is held for
    /// already keep their copies and delegations.
    pub fn share(self, holder: Did) -> HeldShare {
        HeldShare {
            principal: self.principal,
            holder,
            via: None,
        }
    }

    /// Stop sharing the principal with `holder`: forget the copies of its
    /// key sealed to `holder`, and retract its delegations to `holder`.
    pub fn revoke(self, holder: Did) -> HeldRevoke {
        HeldRevoke {
            principal: self.principal,
            holder,
            via: None,
        }
    }

    /// Hand the principal over to `holder`: share it with `holder`, then
    /// revoke it from the holder it is held for now.
    pub fn hand_over(self, holder: Did) -> HeldHandOver {
        HeldHandOver {
            principal: self.principal,
            holder,
            via: None,
        }
    }

    /// Carry the principal from the branch `branch` of the peer's space,
    /// where another peer over the same space keeps its records, into the
    /// branch this peer keeps its own in.
    pub fn carry_from(self, branch: impl Into<String>) -> HeldCarry {
        HeldCarry {
            principal: self.principal,
            from: branch.into(),
        }
    }
}

/// Carry a held principal from another branch of the peer's space.
/// Created by [`HeldReference::carry_from`].
///
/// A space's branches each keep the records of the peer opened on them,
/// so a principal held through one is unknown to the peer on another.
/// Carrying records here what the other branch records of the principal:
/// who its key is held for, every copy of its key sealed to a holder,
/// and the delegations it issued. No key is opened: the records are
/// sealed to their holders and are carried as they are, so the peer holds
/// the principal for whoever it was held for there.
///
/// The other branch is left as it was. Forgetting it is a separate act,
/// for whoever owns it to make once nothing on it is wanted.
pub struct HeldCarry {
    principal: Did,
    from: String,
}

impl HeldCarry {
    /// Carry it. What this peer's branch records already is not written
    /// again, so carrying twice writes nothing the second time. A
    /// principal the other branch does not hold is `NotFound`. The peer's
    /// own act: a session is refused.
    pub async fn perform<S: PeerSpace, M: Mode>(
        self,
        peer: &Peer<S, M>,
    ) -> Result<(), CredentialError> {
        session_writes_nothing::<M>(&peer.did())?;
        let state = BranchReference::from(peer.state());
        if state.name() == self.from {
            return Err(CredentialError::Storage(format!(
                "{} is the branch {} keeps its records in",
                self.from,
                peer.did()
            )));
        }
        let to = opened(&state, peer).await?;
        let from = opened(&state.subject().branch(self.from.as_str()), peer).await?;
        if !secrets::carry(&from, &to, &self.principal, peer)
            .await
            .map_err(unavailable)?
        {
            return Err(CredentialError::NotFound(format!(
                "no key held for {} on {}",
                self.principal, self.from
            )));
        }
        // Retaining is by content: a delegation this peer retains already
        // is not written again.
        for delegation in peer
            .issued_by_on(&from, &self.principal)
            .await
            .map_err(unavailable)?
        {
            retain(peer, delegation.chain().clone()).await?;
        }
        Ok(())
    }
}

/// Share a held principal with another holder. Created by
/// [`HeldReference::share`].
pub struct HeldShare {
    principal: Did,
    holder: Did,
    via: Option<SignerCredential>,
}

impl HeldShare {
    /// Open the principal's key through the account `custodian` guards,
    /// when the peer keeps no copy of its own.
    pub fn via(mut self, custodian: &SignerCredential) -> Self {
        self.via = Some(custodian.clone());
        self
    }

    /// Share it, yielding the principal's delegation to the holder: the
    /// direct consent a service asks of a space. A holder that has a copy
    /// and a delegation already gets that delegation back, and nothing is
    /// written. The peer's own act: a session is refused.
    pub async fn perform<S: PeerSpace, M: Mode>(
        self,
        peer: &Peer<S, M>,
    ) -> Result<UcanDelegation, CredentialError> {
        session_writes_nothing::<M>(&peer.did())?;
        let branch = opened(&BranchReference::from(peer.state()), peer).await?;
        let held = open(&branch, &self.principal, self.via.as_ref(), peer).await?;
        share(&branch, &held, &self.holder, Record::Copy, peer).await
    }
}

/// Stop sharing a held principal with a holder. Created by
/// [`HeldReference::revoke`].
pub struct HeldRevoke {
    principal: Did,
    holder: Did,
    via: Option<SignerCredential>,
}

impl HeldRevoke {
    /// Open the principal's key through the account `custodian` guards,
    /// when the peer keeps no copy of its own.
    pub fn via(mut self, custodian: &SignerCredential) -> Self {
        self.via = Some(custodian.clone());
        self
    }

    /// Revoke it. A copy the holder already opened elsewhere stands, as a
    /// delegation it copied does. The last holder is refused: a principal
    /// is never left with no one who can open its key. The peer's own act,
    /// by a peer that can open the key: a session is refused.
    pub async fn perform<S: PeerSpace, M: Mode>(
        self,
        peer: &Peer<S, M>,
    ) -> Result<(), CredentialError> {
        session_writes_nothing::<M>(&peer.did())?;
        let branch = opened(&BranchReference::from(peer.state()), peer).await?;
        let held = open(&branch, &self.principal, self.via.as_ref(), peer).await?;
        revoke(&branch, &held, &self.holder, peer).await
    }
}

/// Hand a held principal over to another holder. Created by
/// [`HeldReference::hand_over`].
///
/// The principal is shared with the new holder before it is revoked from
/// the old, so a hand-over that stops part-way leaves it shared, never
/// held by no one. A copy of a space's key a peer keeps for itself is not
/// handed over: it stays with the peer, as it does through an account's
/// hand-over, until the account is rotated without the peer.
pub struct HeldHandOver {
    principal: Did,
    holder: Did,
    via: Option<SignerCredential>,
}

impl HeldHandOver {
    /// Open the principal's key through the account `custodian` guards,
    /// when the peer keeps no copy of its own.
    pub fn via(mut self, custodian: &SignerCredential) -> Self {
        self.via = Some(custodian.clone());
        self
    }

    /// Hand it over, yielding the principal's delegation to the new
    /// holder. The peer's own act: a session is refused.
    pub async fn perform<S: PeerSpace, M: Mode>(
        self,
        peer: &Peer<S, M>,
    ) -> Result<UcanDelegation, CredentialError> {
        session_writes_nothing::<M>(&peer.did())?;
        let branch = opened(&BranchReference::from(peer.state()), peer).await?;
        let held = open(&branch, &self.principal, self.via.as_ref(), peer).await?;
        hand_over(&branch, &held, &self.holder, peer).await
    }
}

/// A held principal with its key open.
pub(super) struct Held {
    principal: Did,
    kind: String,
    seed: [u8; 32],
}

impl Held {
    /// `principal`, whose key `vault` holds in `held`: the account, opened,
    /// revealing the key of a principal held for it.
    pub(super) async fn revealed(
        vault: &Vault,
        principal: &Did,
        held: &HeldPrincipal,
    ) -> Result<Self, CredentialError> {
        let seed = vault
            .key()
            .await?
            .secret(held_context(&held.kind))
            .reveal(&SealedSecret::from_bytes(&held.sealed).map_err(unopened)?)
            .await
            .map_err(unopened)?;
        Self::checked(principal, &held.kind, seed).await
    }

    /// The key `seed`, checked to be `principal`'s.
    async fn checked(principal: &Did, kind: &str, seed: Vec<u8>) -> Result<Self, CredentialError> {
        let seed: [u8; 32] = seed
            .try_into()
            .map_err(|_| unopened(format!("the key held for {principal} is not a key")))?;
        let signer = Ed25519Signer::import(&seed).await.map_err(unavailable)?;
        if signer.did() != *principal {
            return Err(unopened(format!(
                "the key held for {principal} is not its key"
            )));
        }
        Ok(Held {
            principal: principal.clone(),
            kind: kind.to_string(),
            seed,
        })
    }
}

/// The key of `principal`, open: through the peer's own copy, or through
/// the copy held for the account `via` guards.
async fn open<S: PeerSpace, M: Mode>(
    branch: &Branch,
    principal: &Did,
    via: Option<&SignerCredential>,
    peer: &Peer<S, M>,
) -> Result<Held, CredentialError> {
    let held = secrets::held_principal(branch, principal, peer)
        .await
        .map_err(unavailable)?
        .ok_or_else(|| CredentialError::NotFound(format!("no key held for {principal}")))?;
    match copy(branch, principal, peer.credential(), peer).await? {
        Some(VaultKey::Seed(seed)) => {
            return Held::checked(principal, &held.kind, seed.to_vec()).await;
        }
        Some(VaultKey::Held(_)) => {
            return Err(CredentialError::Withheld(format!(
                "{principal} is the peer's own key and is shared with no one"
            )));
        }
        None => {}
    }
    let Some(custodian) = via else {
        return Err(CredentialError::Withheld(format!(
            "{} holds no key of {principal}",
            peer.did()
        )));
    };
    let account = branch
        .vault(ACCOUNT)
        .load()
        .via(custodian)
        .perform(peer)
        .await?;
    if held.to != *account.did() {
        return Err(CredentialError::Withheld(format!(
            "the key of {principal} is held for {}, not the account",
            held.to
        )));
    }
    Held::revealed(&account, principal, &held).await
}

/// How a holder's copy of a principal's key is recorded.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Record {
    /// A copy beside those it has, as a vault member's.
    Copy,
    /// The holder the account-level operations act on, in place of the
    /// one before.
    Row,
}

/// Share `held` with `holder`, recording its copy as `record` says, and
/// yield the principal's delegation to `holder`. What `holder` has already
/// is not written again.
async fn share<S: PeerSpace, M: Mode>(
    branch: &Branch,
    held: &Held,
    holder: &Did,
    record: Record,
    peer: &Peer<S, M>,
) -> Result<UcanDelegation, CredentialError> {
    let row = held_for(branch, &held.principal, peer).await?;
    match record {
        Record::Row if row.to != *holder => {
            let sealed = sealable(holder)?
                .secret(held_context(&held.kind))
                .conceal(&held.seed)
                .await
                .map_err(unavailable)?;
            secrets::hold_principal(
                branch,
                &held.principal,
                &held.kind,
                secrets::sealed_message(holder, sealed.to_bytes()),
                peer,
            )
            .await
            .map_err(unavailable)?;
        }
        Record::Copy
            if row.to != *holder
                && secrets::keys_of(branch, &held.principal, holder, peer)
                    .await
                    .map_err(unavailable)?
                    .is_empty() =>
        {
            let sealed = seal_key(&held.seed, holder).await?;
            secrets::grant(branch, &held.principal, holder, sealed, peer)
                .await
                .map_err(unavailable)?;
        }
        _ => {}
    }
    if let Some(delegation) = delegated(&held.principal, holder, peer).await? {
        return Ok(delegation);
    }
    let signer = Ed25519Signer::import(&held.seed)
        .await
        .map_err(unavailable)?;
    reissue(&signer, holder, peer)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| unavailable(format!("{} delegated nothing", held.principal)))
}

/// Revoke `held` from `holder`, refusing the last holder. The holder the
/// account-level operations act on, when it is `holder`, becomes another.
pub(super) async fn revoke<S: PeerSpace, M: Mode>(
    branch: &Branch,
    held: &Held,
    holder: &Did,
    peer: &Peer<S, M>,
) -> Result<(), CredentialError> {
    let row = held_for(branch, &held.principal, peer).await?;
    let own = peer.holder();
    let mut remaining: Vec<Did> = secrets::holders_of(branch, &held.principal, peer)
        .await
        .map_err(unavailable)?
        .into_iter()
        .filter(|other| *other != own && other != holder)
        .collect();
    if row.to != *holder && !remaining.contains(&row.to) {
        remaining.push(row.to.clone());
    }
    remaining.sort_by_key(|other| other.to_string());
    let Some(next) = remaining.first() else {
        return Err(CredentialError::Withheld(format!(
            "{holder} is the last holder of {}: its key would open for no one",
            held.principal
        )));
    };
    if row.to == *holder {
        share(branch, held, next, Record::Row, peer).await?;
    }
    secrets::revoke(branch, &held.principal, holder, peer)
        .await
        .map_err(unavailable)?;
    for delegation in peer.issued_by(&held.principal).await.map_err(unavailable)? {
        if delegation.chain().audience() == holder {
            peer.retract(delegation).await.map_err(unavailable)?;
        }
    }
    Ok(())
}

/// Hand `held` over to `holder`: share it as the holder the account-level
/// operations act on, then revoke it from the one before.
pub(super) async fn hand_over<S: PeerSpace, M: Mode>(
    branch: &Branch,
    held: &Held,
    holder: &Did,
    peer: &Peer<S, M>,
) -> Result<UcanDelegation, CredentialError> {
    let previous = held_for(branch, &held.principal, peer).await?.to;
    let delegation = share(branch, held, holder, Record::Row, peer).await?;
    if previous != *holder {
        revoke(branch, held, &previous, peer).await?;
    }
    Ok(delegation)
}

/// Who `principal`'s key is held for, as `branch` records it now.
async fn held_for<S: PeerSpace, M: Mode>(
    branch: &Branch,
    principal: &Did,
    peer: &Peer<S, M>,
) -> Result<HeldPrincipal, CredentialError> {
    secrets::held_principal(branch, principal, peer)
        .await
        .map_err(unavailable)?
        .ok_or_else(|| CredentialError::NotFound(format!("no key held for {principal}")))
}

/// The delegation `principal` issued to `holder`, if the peer retains one.
async fn delegated<S: PeerSpace, M: Mode>(
    principal: &Did,
    holder: &Did,
    peer: &Peer<S, M>,
) -> Result<Option<UcanDelegation>, CredentialError> {
    Ok(peer
        .issued_by(principal)
        .await
        .map_err(unavailable)?
        .into_iter()
        .find(|delegation| delegation.chain().audience() == holder))
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{
        onboard, test_credential_store, test_grant, test_owned, test_session_with_peer,
        test_storage, unique_name,
    };
    use crate::{Mode, Peer, PeerSpace, SpaceVaultExt as _};
    use dialog_capability::Subject;
    use dialog_capability::access::{Access, AuthorizeError, Prove};
    use dialog_credentials::{Ed25519Signer, SignerCredential};
    use dialog_effects::credential::CredentialError;
    use dialog_effects::storage::Location;
    use dialog_identity::OpenCredential;
    use dialog_repository::{BranchReference, Repository, RepositoryExt as _, secrets, spaces};
    use dialog_storage::Flaky;
    use dialog_storage::provider::storage::{Storage, VolatileSpace};
    use dialog_storage::provider::{Space, Volatile};
    use dialog_ucan::{Parameters, Scope, Ucan};
    use dialog_ucan_core::command::Command as UcanCommand;
    use dialog_ucan_core::subject::Subject as UcanSubject;
    use dialog_varsig::{Did, Principal as _};

    /// A volatile space whose memory loses the publishes it is told to.
    type FlakySpace = Space<Volatile, Flaky, Volatile, Volatile, Volatile>;

    /// An onboarded peer on `storage`, with the custodian of its account.
    async fn onboarded<S: PeerSpace>(
        storage: Storage<S>,
    ) -> anyhow::Result<(Peer<S>, SignerCredential)> {
        let credential = OpenCredential::open(unique_name("alice"))
            .perform(&test_credential_store())
            .await?;
        let peer = Peer::new(credential.clone())
            .at(Location::profile(unique_name("held")))
            .space(Repository::from(credential.did()).branch("main"))
            .with(storage)
            .grant(test_grant().await)
            .await?;
        let custodian = onboard(&peer).await?;
        Ok((peer, custodian))
    }

    /// A space `peer` creates.
    async fn created<S: PeerSpace>(peer: &Peer<S>) -> anyhow::Result<Did> {
        Ok(peer
            .space(unique_name("notes"))
            .create()
            .perform(peer)
            .await?
            .did())
    }

    /// A holder with nothing yet.
    async fn holder() -> anyhow::Result<Did> {
        Ok(Ed25519Signer::generate().await?.did())
    }

    /// Whether `peer` proves `principal` may archive `space`.
    async fn proves<S: PeerSpace, M: Mode>(
        peer: &Peer<S, M>,
        principal: &Did,
        space: &Did,
    ) -> Result<(), AuthorizeError>
    where
        Peer<S, M>: dialog_capability::Provider<Prove<Ucan>>,
    {
        let scope = Scope {
            subject: UcanSubject::Specific(space.clone()),
            command: UcanCommand(vec!["archive".to_string()]),
            parameters: Parameters::default(),
        };
        Subject::from(peer.did())
            .attenuate(Access)
            .invoke(Prove::<Ucan>::new(principal.clone(), scope))
            .perform(peer)
            .await
            .map(|_| ())
    }

    /// The head of the branch `peer` keeps its records and delegations on.
    async fn head<S: PeerSpace, M: Mode>(
        peer: &Peer<S, M>,
    ) -> anyhow::Result<Option<dialog_repository::Revision>> {
        peer.state().refresh(peer).await?;
        Ok(peer.state().revision())
    }

    /// A session shares, revokes and hands over nothing, and writes
    /// nothing asking.
    #[dialog_common::test]
    async fn it_refuses_a_session() -> anyhow::Result<()> {
        let (session, peer) = test_session_with_peer().await;
        let space = created(&peer).await?;
        let other = holder().await?;
        let before = head(&peer).await?;

        let shared = session
            .held_principal(&space)
            .share(other.clone())
            .perform(&session)
            .await
            .map(|_| ());
        assert!(
            matches!(shared, Err(CredentialError::Withheld(_))),
            "{shared:?}"
        );
        let revoked = session
            .held_principal(&space)
            .revoke(peer.authority().await?)
            .perform(&session)
            .await;
        assert!(
            matches!(revoked, Err(CredentialError::Withheld(_))),
            "{revoked:?}"
        );
        let handed = session
            .held_principal(&space)
            .hand_over(other)
            .perform(&session)
            .await
            .map(|_| ());
        assert!(
            matches!(handed, Err(CredentialError::Withheld(_))),
            "{handed:?}"
        );
        assert_eq!(head(&peer).await?, before, "nothing was written");
        Ok(())
    }

    /// Two peers over one space and one key, each keeping its records in
    /// a branch of its own: the one a device works in signed out, and the
    /// one it returns to.
    async fn on_two_branches()
    -> anyhow::Result<(Peer<VolatileSpace>, SignerCredential, Peer<VolatileSpace>)> {
        let storage = test_storage().await;
        let credential = OpenCredential::open(unique_name("alice"))
            .perform(&test_credential_store())
            .await?;
        let location = Location::profile(unique_name("held"));
        let open = |branch: &'static str| {
            let credential = credential.clone();
            let location = location.clone();
            let storage = storage.clone();
            async move {
                anyhow::Ok(
                    Peer::new(credential.clone())
                        .at(location)
                        .space(Repository::from(credential.did()).branch(branch))
                        .with(storage)
                        .grant(test_grant().await)
                        .await?,
                )
            }
        };
        let workspace = open("workspace").await?;
        let custodian = onboard(&workspace).await?;
        let account = open("main").await?;
        onboard(&account).await?;
        Ok((workspace, custodian, account))
    }

    /// A space held through one branch is unknown to the peer on another
    /// until it is carried there. Carrying records who it is held for,
    /// every copy of its key and the delegations it issued, as they are:
    /// the peer then proves the holder's authority over the space and
    /// opens its key through the copy it carried. The branch it came from
    /// is left as it was, and carrying again writes nothing.
    #[dialog_common::test]
    async fn it_carries_a_space_from_another_branch() -> anyhow::Result<()> {
        let (workspace, custodian, account) = on_two_branches().await?;
        let space = created(&workspace).await?;
        let root = holder().await?;
        workspace
            .held_principal(&space)
            .hand_over(root.clone())
            .via(&custodian)
            .perform(&workspace)
            .await?;
        let source = secrets::held_principal(workspace.state(), &space, &workspace)
            .await?
            .expect("the workspace holds the space");
        assert_eq!(source.to, root);
        assert!(
            secrets::held_principal(account.state(), &space, &account)
                .await?
                .is_none(),
            "the other branch knows nothing of the space"
        );
        assert!(proves(&account, &root, &space).await.is_err());

        let left = head(&workspace).await?;
        account
            .held_principal(&space)
            .carry_from("workspace")
            .perform(&account)
            .await?;

        let carried = secrets::held_principal(account.state(), &space, &account)
            .await?
            .expect("the space is held here now");
        assert_eq!(carried, source, "held for whom it was, sealed as it was");
        proves(&account, &root, &space).await?;
        // The copy of the key the peer keeps for itself came too, so the
        // peer on this branch opens the key with no custodian.
        let other = holder().await?;
        account
            .held_principal(&space)
            .share(other.clone())
            .perform(&account)
            .await?;
        proves(&account, &other, &space).await?;
        assert_eq!(head(&workspace).await?, left, "the source is untouched");

        let before = head(&account).await?;
        account
            .held_principal(&space)
            .carry_from("workspace")
            .perform(&account)
            .await?;
        assert_eq!(head(&account).await?, before, "nothing was written");
        Ok(())
    }

    /// A principal the other branch does not hold is not found, and a
    /// peer's own branch is not somewhere to carry from. Neither writes.
    #[dialog_common::test]
    async fn it_carries_nothing_that_is_not_held_there() -> anyhow::Result<()> {
        let (workspace, _, account) = on_two_branches().await?;
        let space = created(&account).await?;
        let stranger = holder().await?;
        let before = head(&account).await?;
        let left = head(&workspace).await?;

        let missing = account
            .held_principal(&stranger)
            .carry_from("workspace")
            .perform(&account)
            .await;
        assert!(
            matches!(missing, Err(CredentialError::NotFound(_))),
            "{missing:?}"
        );
        // Held here, not there.
        let elsewhere = account
            .held_principal(&space)
            .carry_from("workspace")
            .perform(&account)
            .await;
        assert!(
            matches!(elsewhere, Err(CredentialError::NotFound(_))),
            "{elsewhere:?}"
        );
        let own = account
            .held_principal(&space)
            .carry_from("main")
            .perform(&account)
            .await;
        assert!(matches!(own, Err(CredentialError::Storage(_))), "{own:?}");
        assert_eq!(head(&account).await?, before, "nothing was written");
        assert_eq!(head(&workspace).await?, left, "nothing was written");
        Ok(())
    }

    /// A session carries nothing, and writes nothing asking.
    #[dialog_common::test]
    async fn it_refuses_a_session_carrying() -> anyhow::Result<()> {
        let (session, peer) = test_session_with_peer().await;
        let space = created(&peer).await?;
        let before = head(&peer).await?;

        let carried = session
            .held_principal(&space)
            .carry_from("workspace")
            .perform(&session)
            .await;
        assert!(
            matches!(carried, Err(CredentialError::Withheld(_))),
            "{carried:?}"
        );
        assert_eq!(head(&peer).await?, before, "nothing was written");
        Ok(())
    }

    /// A peer that keeps no copy of a space's key, asking without a
    /// custodian, is refused and writes nothing.
    #[dialog_common::test]
    async fn it_refuses_a_peer_without_the_key() -> anyhow::Result<()> {
        let storage = test_storage().await;
        let (peer, _) = onboarded(storage.clone()).await?;
        let space = created(&peer).await?;
        let other = OpenCredential::open(unique_name("bob"))
            .perform(&test_credential_store())
            .await?;
        let other = Peer::new(other)
            .space(BranchReference::from(peer.state()))
            .with(storage)
            .grant(test_grant().await)
            .await?;
        let recipient = holder().await?;
        let before = head(&peer).await?;

        let shared = other
            .held_principal(&space)
            .share(recipient.clone())
            .perform(&other)
            .await
            .map(|_| ());
        assert!(
            matches!(shared, Err(CredentialError::Withheld(_))),
            "{shared:?}"
        );
        let revoked = other
            .held_principal(&space)
            .revoke(peer.authority().await?)
            .perform(&other)
            .await;
        assert!(
            matches!(revoked, Err(CredentialError::Withheld(_))),
            "{revoked:?}"
        );
        let handed = other
            .held_principal(&space)
            .hand_over(recipient)
            .perform(&other)
            .await
            .map(|_| ());
        assert!(
            matches!(handed, Err(CredentialError::Withheld(_))),
            "{handed:?}"
        );
        assert_eq!(head(&peer).await?, before, "nothing was written");
        Ok(())
    }

    /// Sharing a space keeps it held for its account and adds a holder:
    /// both prove authority over it, the new holder with a copy of its key
    /// and the space's delegation, which is what sharing yields. Sharing
    /// again yields the same delegation and writes nothing.
    #[dialog_common::test]
    async fn it_shares_a_space_with_another_holder() -> anyhow::Result<()> {
        let (peer, _) = onboarded(test_storage().await).await?;
        let space = created(&peer).await?;
        let account = peer.authority().await?;
        let other = holder().await?;

        let unshared = head(&peer).await?;
        let delegation = peer
            .held_principal(&space)
            .share(other.clone())
            .perform(&peer)
            .await?;
        assert_ne!(head(&peer).await?, unshared, "sharing writes");
        assert_eq!(delegation.chain().audience(), &other);
        assert_eq!(delegation.chain().issuer(), &space);
        proves(&peer, &account, &space).await?;
        proves(&peer, &other, &space).await?;
        assert_eq!(
            secrets::keys_of(peer.state(), &space, &other, &peer)
                .await?
                .len(),
            1
        );
        let held = secrets::held_principal(peer.state(), &space, &peer)
            .await?
            .expect("the space's key is held");
        assert_eq!(held.to, account, "still held for the account");

        let before = head(&peer).await?;
        let again = peer
            .held_principal(&space)
            .share(other)
            .perform(&peer)
            .await?;
        assert_eq!(again.chain().to_bytes()?, delegation.chain().to_bytes()?);
        assert_eq!(head(&peer).await?, before, "nothing was written");
        Ok(())
    }

    /// Revoking a holder forgets its copy and its delegation, so it proves
    /// nothing over the space. The last holder is refused, and nothing is
    /// written.
    #[dialog_common::test]
    async fn it_revokes_a_holder_but_not_the_last() -> anyhow::Result<()> {
        let (peer, _) = onboarded(test_storage().await).await?;
        let space = created(&peer).await?;
        let account = peer.authority().await?;
        let other = holder().await?;
        peer.held_principal(&space)
            .share(other.clone())
            .perform(&peer)
            .await?;

        peer.held_principal(&space)
            .revoke(other.clone())
            .perform(&peer)
            .await?;
        assert!(
            secrets::keys_of(peer.state(), &space, &other, &peer)
                .await?
                .is_empty()
        );
        let unproven = proves(&peer, &other, &space).await;
        assert!(
            matches!(unproven, Err(AuthorizeError::UnprovenSubject { .. })),
            "{unproven:?}"
        );
        proves(&peer, &account, &space).await?;

        let before = head(&peer).await?;
        let last = peer
            .held_principal(&space)
            .revoke(account.clone())
            .perform(&peer)
            .await;
        assert!(
            matches!(last, Err(CredentialError::Withheld(_))),
            "{last:?}"
        );
        assert_eq!(head(&peer).await?, before, "nothing was written");
        proves(&peer, &account, &space).await?;
        Ok(())
    }

    /// Handing a space over leaves it held for the new holder alone: the
    /// new holder proves authority over it and the account it was held for
    /// does not. The peer's own copy of the key stays.
    #[dialog_common::test]
    async fn it_hands_a_space_over_to_another_holder() -> anyhow::Result<()> {
        let (peer, _) = onboarded(test_storage().await).await?;
        let space = created(&peer).await?;
        let account = peer.authority().await?;
        let other = holder().await?;

        let delegation = peer
            .held_principal(&space)
            .hand_over(other.clone())
            .perform(&peer)
            .await?;
        assert_eq!(delegation.chain().audience(), &other);
        proves(&peer, &other, &space).await?;
        let unproven = proves(&peer, &account, &space).await;
        assert!(
            matches!(unproven, Err(AuthorizeError::UnprovenSubject { .. })),
            "{unproven:?}"
        );
        let held = secrets::held_principal(peer.state(), &space, &peer)
            .await?
            .expect("the space's key is held");
        assert_eq!(held.kind, spaces::SPACE);
        assert_eq!(held.to, other);
        assert!(peer.holds_key(&space).await?, "the peer keeps its copy");
        Ok(())
    }

    /// A hand-over that stops once the space is shared with the new
    /// holder leaves it shared: both holders prove authority over it.
    #[dialog_common::test]
    async fn it_leaves_a_space_shared_when_a_hand_over_stops_part_way() -> anyhow::Result<()> {
        let storage = test_owned(Storage::<FlakySpace>::new()).await;
        let (peer, _) = onboarded(storage.clone()).await?;
        let space = created(&peer).await?;
        let account = peer.authority().await?;
        let other = holder().await?;

        // Sharing is two commits: the key held for the new holder, and
        // the space's delegation to it. Every commit after those is lost,
        // so the old holder is never revoked.
        let memory = storage.space(peer.home()).expect("mounted").memory;
        memory.lose_publishes("branch/main", "revision", 2..);
        let stopped = peer
            .held_principal(&space)
            .hand_over(other.clone())
            .perform(&peer)
            .await;
        assert!(stopped.is_err(), "the hand-over went through");
        memory.lose_publishes("branch/main", "revision", 0..0);

        proves(&peer, &other, &space).await?;
        proves(&peer, &account, &space).await?;
        Ok(())
    }

    /// Handing the account over moves the key held for it to the owner,
    /// and leaves a holder the space was shared with as it was.
    #[dialog_common::test]
    async fn it_keeps_a_shared_holder_through_an_account_hand_over() -> anyhow::Result<()> {
        let (peer, custodian) = onboarded(test_storage().await).await?;
        let space = created(&peer).await?;
        let other = holder().await?;
        let shared = peer
            .held_principal(&space)
            .share(other.clone())
            .perform(&peer)
            .await?;
        let owner = holder().await?;

        let account = peer
            .state()
            .vault("account")
            .load()
            .via(&custodian)
            .perform(&peer)
            .await?;
        account.hand_over(owner.clone()).perform(&peer).await?;

        let held = secrets::held_principal(peer.state(), &space, &peer)
            .await?
            .expect("the space's key is held");
        assert_eq!(held.to, owner, "the account's key moved to the owner");
        assert_eq!(
            secrets::keys_of(peer.state(), &space, &other, &peer)
                .await?
                .len(),
            1,
            "the shared holder keeps its copy"
        );
        let issued = peer
            .issued_by(&space)
            .await?
            .into_iter()
            .filter(|delegation| delegation.chain().audience() == &other)
            .map(|delegation| delegation.chain().to_bytes())
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            issued,
            vec![shared.chain().to_bytes()?],
            "and its delegation"
        );
        proves(&peer, &other, &space).await?;
        Ok(())
    }
}
