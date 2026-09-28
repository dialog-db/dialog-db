//! Vaults: secrets sealed to named principals.
//!
//! A vault is a principal secrets are sealed to. A top-level vault's key
//! is generated when it is [created](VaultReference::create) and sealed to
//! its members; a vault below one is derived from its parent's key, so
//! whoever holds the parent derives the same child. `account` is the
//! account a peer acts for. Its members are custodians: the key that
//! guards the account, never the peer, which acts for the account through
//! what the account delegates to it.
//!
//! A handle either holds a vault's key or it does not. A
//! [`VaultReference`] names a vault and holds nothing; opening it yields a
//! [`Vault`], which holds the key. A vault opens through a copy of its key
//! sealed to the opener: the peer's own key, or a custodian's given with
//! [`via`](OpenVault::via). No key is ever passed as bytes: a custodian is
//! a signer handle, and the vault's key stays inside.
//!
//! ```no_run
//! # use dialog_peer::{Peer, SpaceVaultExt as _};
//! # async fn example(
//! #     peer: &Peer<dialog_storage::provider::storage::VolatileSpace>,
//! #     custodian: &dialog_credentials::SignerCredential,
//! # ) -> anyhow::Result<()> {
//! // Onboarding: the account is created, guarded by a custodian, and
//! // delegates to the peer. The peer holds no copy of its key.
//! let account = peer.state().vault("account").create().perform(peer).await?;
//! account.add(custodian.did()).perform(peer).await?;
//! account.delegate(peer.did()).perform(peer).await?;
//!
//! // Later: the account opens only through its custodian.
//! let account = peer.state().vault("account").open().via(custodian).perform(peer).await?;
//! let rotated = account.rotate().perform(peer).await?;
//! # let _ = rotated;
//! # Ok(())
//! # }
//! ```
//!
//! Opening a reference reached from an open vault derives the child from
//! the key in hand, recording it with the parent's signature; opening one
//! reached from the space opens the opener's own copy, or fails. A child
//! record its parent did not sign is ignored. A session of a peer holds no
//! copy of any key and creates no vault: it opens what was sealed to its
//! own key, and nothing else.

use core::fmt::{self, Debug, Display};

use super::space::SPACE_KEY;
use super::{Local, Mode, Peer, PeerSpace};
use dialog_capability::access::{Access, Retain};
use dialog_capability::{Capability, Policy, Provider, Subject};
use dialog_credentials::key::{ExtractableKey, KeyExport};
use dialog_credentials::secret::{Context, SealedSecret, SecretExtractableDerive};
use dialog_credentials::{Ed25519Signer, Ed25519Verifier, Extractable, Signer, SignerCredential};
use dialog_effects::credential::{self, CredentialError, Secret as SiteSecret};
use dialog_repository::registry::RegistryEnv;
use dialog_repository::schema::SealedMessage;
use dialog_repository::{Branch, BranchReference};
use dialog_repository::{secrets, spaces};
use dialog_ucan::{Ucan, UcanDelegation};
use dialog_ucan_core::subject::Subject as UcanSubject;
use dialog_ucan_core::{DelegationBuilder, DelegationChain};
use dialog_varsig::eddsa::Ed25519Signature;
use dialog_varsig::{Did, Principal as _, Signer as _, Verifier as _};

/// A write a session asked for: sessions read the peer's space and write
/// nothing to it.
fn session_writes_nothing<M: Mode>(peer_did: &Did) -> Result<(), CredentialError> {
    if M::HOLDS_KEYS {
        Ok(())
    } else {
        Err(CredentialError::Withheld(format!(
            "{peer_did} is a session: it writes nothing to its peer's space"
        )))
    }
}

/// The context a secret is sealed in, so a sealed secret opens only as
/// one.
const SECRET: Context = Context::new("dialog.secret/message");

/// The context a vault's key is sealed in to each of its members.
const KEY: Context = Context::new("dialog.secret/key");

/// The context a vault's key is derived from its parent's in.
const DERIVE: Context = Context::new("dialog.vault/derive");

/// The top-level vault a peer acts for.
const ACCOUNT: &str = "account";

/// A failure reading or writing the peer's records.
fn unavailable(error: impl Display) -> CredentialError {
    CredentialError::Storage(error.to_string())
}

/// A sealed value that could not be opened.
fn unopened(error: impl Display) -> CredentialError {
    CredentialError::Corrupted(error.to_string())
}

/// The key `did` names, to seal to or verify with.
fn sealable(did: &Did) -> Result<Ed25519Verifier, CredentialError> {
    did.to_string()
        .parse()
        .map_err(|_| CredentialError::Storage(format!("{did} has no key to seal to")))
}

/// What a parent signs to vouch for a child it derived.
fn vouched(parent: &Did, name: &str, vault: &Did) -> Vec<u8> {
    format!("dialog.vault:{parent}:{name}:{vault}").into_bytes()
}

/// A vault's key seed, sealed to `member`.
async fn seal_key(seed: &[u8], member: &Did) -> Result<Vec<u8>, CredentialError> {
    Ok(sealable(member)?
        .secret(KEY)
        .conceal(seed)
        .await
        .map_err(unavailable)?
        .to_bytes())
}

/// A vault's key seed, from a copy sealed to `credential`.
async fn open_key(
    credential: &SignerCredential,
    sealed: &[u8],
) -> Result<Vec<u8>, CredentialError> {
    let opener = credential.signer().as_ed25519().ok_or_else(|| {
        CredentialError::Withheld(format!("{} opens nothing sealed", credential.did()))
    })?;
    opener
        .secret(KEY)
        .reveal(&SealedSecret::from_bytes(sealed).map_err(unopened)?)
        .await
        .map_err(unopened)
}

/// The seed of an extractable key.
async fn seed_of(key: &Ed25519Signer<Extractable>) -> Result<[u8; 32], CredentialError> {
    // Every export of an extractable key is its seed.
    #[allow(irrefutable_let_patterns)]
    let KeyExport::Extractable(seed) = key.export().await.map_err(unavailable)? else {
        return Err(unavailable("a vault's key is not extractable"));
    };
    seed.as_slice()
        .try_into()
        .map_err(|_| unopened("a vault's key is not a key"))
}

/// The branch `space` names, at its current head: the peer's own handle
/// when it is the peer's space, so what is written through a vault moves
/// the head the peer reads.
async fn opened<S: PeerSpace, M: Mode>(
    space: &BranchReference,
    peer: &Peer<S, M>,
) -> Result<Branch, CredentialError> {
    let state = peer.state();
    if space.of() == state.of() && space.name() == state.name() {
        state.refresh(peer).await.map_err(unavailable)?;
        return Ok(state.clone());
    }
    space
        .clone()
        .open()
        .perform(peer)
        .await
        .map_err(unavailable)
}

/// The vaults of a space.
pub trait SpaceVaultExt {
    /// The top-level vault `name` of this space.
    fn vault(&self, name: impl Into<String>) -> VaultReference;
}

impl SpaceVaultExt for BranchReference {
    fn vault(&self, name: impl Into<String>) -> VaultReference {
        VaultReference {
            space: self.clone(),
            names: vec![name.into()],
            parent: None,
        }
    }
}

impl SpaceVaultExt for Branch {
    fn vault(&self, name: impl Into<String>) -> VaultReference {
        BranchReference::from(self).vault(name)
    }
}

/// A vault named in a space, holding no key: open it to use it.
#[derive(Clone)]
pub struct VaultReference {
    space: BranchReference,
    names: Vec<String>,
    /// The open vault this reference was reached from, whose key derives
    /// it.
    parent: Option<Vault>,
}

impl VaultReference {
    /// The vault named `name` below this one.
    pub fn vault(mut self, name: impl Into<String>) -> Self {
        self.names.push(name.into());
        self
    }

    /// Open this vault if it exists and the peer can have its key, or
    /// create it if it does not: a top-level vault with a generated key, a
    /// vault below another derived from the key above it.
    pub fn open(self) -> OpenVault {
        OpenVault {
            vault: self,
            mode: Opening::Open,
            via: None,
        }
    }

    /// Create this vault, refusing one that exists: a top-level vault's
    /// key is generated, and a vault below another is derived from the key
    /// above it, which the peer must be able to have.
    pub fn create(self) -> OpenVault {
        OpenVault {
            vault: self,
            mode: Opening::Create,
            via: None,
        }
    }

    /// Load this vault if it exists, failing otherwise.
    pub fn load(self) -> OpenVault {
        OpenVault {
            vault: self,
            mode: Opening::Load,
            via: None,
        }
    }
}

/// A vault opened: its key in hand.
#[derive(Clone)]
pub struct Vault {
    space: BranchReference,
    /// The name of a top-level vault; a vault below another has none of
    /// its own to rotate by.
    name: Option<String>,
    did: Did,
    seed: [u8; 32],
}

impl Debug for Vault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Vault")
            .field("did", &self.did)
            .finish_non_exhaustive()
    }
}

impl Vault {
    /// The vault's DID: what secrets for it are sealed to.
    pub fn did(&self) -> &Did {
        &self.did
    }

    /// The vault named `name` below this one, derived from this one's key
    /// when opened.
    pub fn vault(&self, name: impl Into<String>) -> VaultReference {
        VaultReference {
            space: self.space.clone(),
            names: vec![name.into()],
            parent: Some(self.clone()),
        }
    }

    /// Seal this vault's key to `member`, so it opens what is sealed to the
    /// vault: one record, and nothing sealed to the vault is sealed again.
    pub fn add(&self, member: Did) -> Add {
        Add {
            vault: self.clone(),
            member,
        }
    }

    /// Have this vault delegate its whole authority to `audience`: the
    /// account to a peer, so the spaces delegated to the account are the
    /// peer's to use.
    pub fn delegate(&self, audience: Did) -> Delegate {
        Delegate {
            vault: self.clone(),
            audience,
        }
    }

    /// The secret this vault keeps as `name`.
    pub fn secret(&self, name: impl Into<String>) -> SecretReference {
        SecretReference {
            vault: self.clone(),
            name: name.into(),
        }
    }

    /// Seal `secret` to this vault, yielding the sealed message.
    pub fn conceal(&self, secret: impl Into<Vec<u8>>) -> Conceal {
        Conceal {
            vault: self.clone(),
            secret: secret.into(),
        }
    }

    /// Open a message sealed to this vault.
    pub fn reveal(&self, message: SealedMessage) -> Reveal {
        Reveal {
            vault: self.clone(),
            message,
        }
    }

    /// Replace this top-level vault's key with one generated inside,
    /// leaving out the members [removed](Rotate::without): what the old
    /// key reached is moved to the new one.
    pub fn rotate(&self) -> Rotate {
        Rotate {
            vault: self.clone(),
            without: Vec::new(),
        }
    }

    /// The vault's key.
    pub(crate) async fn key(&self) -> Result<Ed25519Signer, CredentialError> {
        Ed25519Signer::import(&self.seed).await.map_err(unavailable)
    }

    /// The child `name` derived from this vault's key, recorded with this
    /// vault's signature when `record` is set and it is not recorded yet.
    async fn derive<S: PeerSpace, M: Mode>(
        &self,
        branch: &Branch,
        name: &str,
        record: bool,
        peer: &Peer<S, M>,
    ) -> Result<Vault, CredentialError> {
        let key = self.key().await?;
        let child = SecretExtractableDerive::derive(&key.secret(DERIVE), name.as_bytes())
            .await
            .map_err(unavailable)?;
        let did = child.did();
        let recorded = secrets::children(branch, &self.did, name, peer)
            .await
            .map_err(unavailable)?
            .into_iter()
            .any(|(vault, _)| vault == did);
        if record && !recorded {
            let signature = key
                .sign(&vouched(&self.did, name, &did))
                .await
                .map_err(unavailable)?;
            secrets::record_child(branch, &did, &self.did, name, signature.0.to_vec(), peer)
                .await
                .map_err(unavailable)?;
        }
        Ok(Vault {
            space: self.space.clone(),
            name: None,
            did,
            seed: seed_of(&child).await?,
        })
    }
}

/// How a vault is opened.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Opening {
    /// Load it if it exists, create it otherwise.
    Open,
    /// Create it, refusing one that exists.
    Create,
    /// Load it, failing when it does not exist.
    Load,
}

/// Open, create, or load a vault.
pub struct OpenVault {
    vault: VaultReference,
    mode: Opening,
    /// The custodian whose copy of the key opens the vault, when it is
    /// not the peer's own.
    via: Option<SignerCredential>,
}

impl OpenVault {
    /// Open the vault through `custodian`'s copy of its key rather than
    /// the peer's: the way an account, whose members are its custodians,
    /// is opened. A signer handle, not a key: nothing is passed as bytes.
    pub fn via(mut self, custodian: &SignerCredential) -> Self {
        self.via = Some(custodian.clone());
        self
    }

    /// Open the vault as the mode says. Its key is had through the
    /// opener's own copy of it, or derived from its parent's key, had the
    /// same way or already in hand when the reference was reached from an
    /// open vault. A missing top-level vault is created with a generated
    /// key. A session loads; it creates nothing.
    pub async fn perform<S: PeerSpace, M: Mode>(
        self,
        peer: &Peer<S, M>,
    ) -> Result<Vault, CredentialError> {
        let OpenVault {
            vault: reference,
            mode,
            via,
        } = self;
        if mode != Opening::Load {
            session_writes_nothing::<M>(&peer.did())?;
        }
        let opener = via.unwrap_or_else(|| peer.credential().clone());
        let branch = opened(&reference.space, peer).await?;
        let path = reference.names.join(" → ");
        let start = reference.parent.as_ref().map(|vault| vault.did.clone());
        let exists = resolve(&branch, start.as_ref(), &reference.names, peer)
            .await?
            .is_some();
        match (mode, exists) {
            (Opening::Create, true) => {
                return Err(unavailable(format!("the vault {path} exists")));
            }
            (Opening::Load, false) => {
                return Err(CredentialError::NotFound(format!("no vault {path}")));
            }
            _ => {}
        }
        let obtained = obtain(
            &branch,
            &reference.space,
            reference.parent,
            &reference.names,
            mode != Opening::Load,
            &opener,
            peer,
        )
        .await?;
        obtained.ok_or_else(|| {
            if exists {
                CredentialError::Withheld(format!("{} holds no key of {path}", opener.did()))
            } else {
                CredentialError::NotFound(format!(
                    "no vault {path}, and {} holds no key of the vault above it",
                    peer.did()
                ))
            }
        })
    }
}

/// The vault `names` names below `base` (or at the top level without one),
/// open: through the peer's own copy of its key, or derived from its
/// parent's, had the same way. With `create`, what is missing on the way is
/// created: a top-level vault generated, a child derived and recorded.
async fn obtain<S: PeerSpace, M: Mode>(
    branch: &Branch,
    space: &BranchReference,
    base: Option<Vault>,
    names: &[String],
    create: bool,
    opener: &SignerCredential,
    peer: &Peer<S, M>,
) -> Result<Option<Vault>, CredentialError> {
    let Some((last, above)) = names.split_last() else {
        return Ok(base);
    };
    let start = base.as_ref().map(|vault| vault.did.clone());
    let recorded = resolve(branch, start.as_ref(), names, peer).await?;
    if let Some(did) = &recorded
        && let Some(seed) = copy(branch, did, opener, peer).await?
    {
        return Ok(Some(Vault {
            space: space.clone(),
            name: (above.is_empty() && base.is_none()).then(|| last.clone()),
            did: did.clone(),
            seed,
        }));
    }
    if above.is_empty() && base.is_none() {
        if recorded.is_some() || !create {
            return Ok(None);
        }
        let key = <Ed25519Signer<Extractable> as ExtractableKey>::generate()
            .await
            .map_err(unavailable)?;
        let did = key.did();
        secrets::record_root(branch, &did, last, peer)
            .await
            .map_err(unavailable)?;
        return Ok(Some(Vault {
            space: space.clone(),
            name: Some(last.clone()),
            did,
            seed: seed_of(&key).await?,
        }));
    }
    let Some(parent) = Box::pin(obtain(branch, space, base, above, create, opener, peer)).await?
    else {
        return Ok(None);
    };
    if recorded.is_none() && !create {
        return Ok(None);
    }
    Ok(Some(parent.derive(branch, last, create, peer).await?))
}

/// The DID of the vault `names` names below `start` (or at the top level
/// without one) in `branch`: each child as its parent vouches for it.
async fn resolve<S: PeerSpace, M: Mode>(
    branch: &Branch,
    start: Option<&Did>,
    names: &[String],
    peer: &Peer<S, M>,
) -> Result<Option<Did>, CredentialError> {
    let (mut did, rest) = match start {
        Some(start) => (start.clone(), names),
        None => {
            let Some((first, rest)) = names.split_first() else {
                return Ok(None);
            };
            let Some(did) = secrets::root(branch, first, peer)
                .await
                .map_err(unavailable)?
            else {
                return Ok(None);
            };
            (did, rest)
        }
    };
    for name in rest {
        let parent = sealable(&did)?;
        let mut found = None;
        for (child, signature) in secrets::children(branch, &did, name, peer)
            .await
            .map_err(unavailable)?
        {
            let Ok(bytes) = <[u8; 64]>::try_from(signature.as_slice()) else {
                continue;
            };
            if parent
                .verify(&vouched(&did, name, &child), &Ed25519Signature(bytes))
                .await
                .is_ok()
            {
                found = Some(child);
                break;
            }
        }
        let Some(found) = found else {
            return Ok(None);
        };
        did = found;
    }
    Ok(Some(did))
}

/// The seed of `vault`'s key from a copy `branch` records sealed to
/// `opener`.
async fn copy<S: PeerSpace, M: Mode>(
    branch: &Branch,
    vault: &Did,
    opener: &SignerCredential,
    peer: &Peer<S, M>,
) -> Result<Option<[u8; 32]>, CredentialError> {
    let copies = secrets::keys_of(branch, vault, &opener.did(), peer)
        .await
        .map_err(unavailable)?;
    let Some(copy) = copies.into_iter().next() else {
        return Ok(None);
    };
    let seed = open_key(opener, &copy).await?;
    Ok(Some(seed.try_into().map_err(|_| {
        unopened(format!("the key of {vault} is not a key"))
    })?))
}

/// Make a principal a member of a vault.
pub struct Add {
    vault: Vault,
    member: Did,
}

impl Add {
    /// Seal the vault's key to the member. The peer's own act: a session
    /// makes no one a member.
    pub async fn perform<S: PeerSpace>(self, peer: &Peer<S, Local>) -> Result<(), CredentialError> {
        let branch = opened(&self.vault.space, peer).await?;
        let sealed = seal_key(&self.vault.seed, &self.member).await?;
        secrets::grant(&branch, &self.vault.did, &self.member, sealed, peer)
            .await
            .map_err(unavailable)
    }
}

/// Replace a top-level vault's key with one generated inside.
///
/// The new key is sealed to every member of the old vault but those
/// removed. Each vault below it is derived again from the new key, sealed
/// to its members but those removed, and the secrets it keeps are sealed
/// again to it. A space whose key is held sealed to the old vault is held
/// sealed to the new one, and delegates to it; what the old vault
/// delegated is delegated by the new one, to all but those removed, and
/// every delegation from the old vault is retracted where the peer proves
/// from, as is a space's to it. Last of all the vault is recorded under
/// its name with the new key: until then the old key is the vault, so a
/// rotation that stops part-way leaves the account as it was.
///
/// The old key is opened through a custodian's copy, and the new one is
/// never handed out: handing an account over means adding the new
/// custodian as a member and rotating without the old one.
///
/// A member removed keeps what it already had: the old key opens what was
/// sealed to it before, and a delegation it copied elsewhere stands until
/// it is revoked there. What is sealed to the vault from now on is not
/// its to open.
pub struct Rotate {
    vault: Vault,
    without: Vec<Did>,
}

impl Rotate {
    /// Leave `member` out of the rotated vault and everything below it.
    pub fn without(mut self, member: Did) -> Self {
        self.without.push(member);
        self
    }

    /// Rotate the vault, yielding it with its new key. The peer's own
    /// act, with the old vault opened through its custodian.
    pub async fn perform<S: PeerSpace>(
        self,
        peer: &Peer<S, Local>,
    ) -> Result<Vault, CredentialError> {
        let Rotate {
            vault: old,
            without,
        } = self;
        let Some(name) = old.name.clone() else {
            return Err(unavailable(format!(
                "{} is below another vault and is rotated with it",
                old.did
            )));
        };
        let key = <Ed25519Signer<Extractable> as ExtractableKey>::generate()
            .await
            .map_err(unavailable)?;
        let new = Vault {
            space: old.space.clone(),
            name: Some(name.clone()),
            did: key.did(),
            seed: seed_of(&key).await?,
        };
        let branch = opened(&old.space, peer).await?;
        let members = secrets::members(&branch, &old.did, peer)
            .await
            .map_err(unavailable)?;
        for member in members.iter().filter(|member| !without.contains(member)) {
            new.add(member.clone()).perform(peer).await?;
        }
        moved(&branch, &old, &new, &without, peer).await?;
        rehold(&branch, &old, &new, peer).await?;
        redelegate(&old, &new, &without, peer).await?;
        secrets::replace_root(&branch, &new.did, &name, peer)
            .await
            .map_err(unavailable)?;
        Ok(new)
    }
}

/// Move what `old` reaches to `new`: the secrets it keeps, and each vault
/// below it, derived again from `new` and sealed to its members but those
/// `without`.
async fn moved<S: PeerSpace>(
    branch: &Branch,
    old: &Vault,
    new: &Vault,
    without: &[Did],
    peer: &Peer<S, Local>,
) -> Result<(), CredentialError> {
    for (name, sealed) in secrets::secrets_of(branch, &old.did, peer)
        .await
        .map_err(unavailable)?
    {
        let secret = old.reveal(sealed).perform(peer).await?;
        new.secret(name).conceal(secret).perform(peer).await?;
    }
    let mut names: Vec<String> = Vec::new();
    for (name, did) in secrets::children_of(branch, &old.did, peer)
        .await
        .map_err(unavailable)?
    {
        if names.contains(&name) {
            continue;
        }
        // Only the child the old key derives is moved: a record naming
        // another is one the old vault never vouched for.
        let from = old.derive(branch, &name, false, peer).await?;
        if from.did != did {
            continue;
        }
        names.push(name.clone());
        let to = new.derive(branch, &name, true, peer).await?;
        for member in secrets::members(branch, &from.did, peer)
            .await
            .map_err(unavailable)?
            .into_iter()
            .filter(|member| !without.contains(member))
        {
            to.add(member).perform(peer).await?;
        }
        Box::pin(moved(branch, &from, &to, without, peer)).await?;
    }
    Ok(())
}

/// Hold every space key held sealed to `old` sealed to `new` instead, and
/// have each space delegate to `new`.
async fn rehold<S: PeerSpace>(
    branch: &Branch,
    old: &Vault,
    new: &Vault,
    peer: &Peer<S, Local>,
) -> Result<(), CredentialError> {
    let opener = old.key().await?;
    for (space, held) in secrets::held_by(branch, &old.did, peer)
        .await
        .map_err(unavailable)?
    {
        if held.kind != spaces::SPACE {
            continue;
        }
        let seed = opener
            .secret(SPACE_KEY)
            .reveal(&SealedSecret::from_bytes(&held.sealed).map_err(unopened)?)
            .await
            .map_err(unopened)?;
        let seed: [u8; 32] = seed
            .try_into()
            .map_err(|_| unopened(format!("the key held for {space} is not a key")))?;
        let signer = Ed25519Signer::import(&seed).await.map_err(unavailable)?;
        if signer.did() != space {
            return Err(unopened(format!("the key held for {space} is not its key")));
        }
        let sealed = sealable(&new.did)?
            .secret(SPACE_KEY)
            .conceal(&seed)
            .await
            .map_err(unavailable)?;
        secrets::hold_principal(
            branch,
            &space,
            spaces::SPACE,
            secrets::sealed_message(&new.did, sealed.to_bytes()),
            peer,
        )
        .await
        .map_err(unavailable)?;
        let delegation = DelegationBuilder::new()
            .issuer(Signer::from(signer))
            .audience(&new.did)
            .subject(UcanSubject::Specific(space.clone()))
            .command(Vec::new())
            .try_build()
            .await
            .map_err(|error| unavailable(format!("{error:?}")))?;
        retain(peer, DelegationChain::new(delegation)).await?;
        for delegation in peer.issued_by(&space).await.map_err(unavailable)? {
            if delegation.chain().audience() == &old.did {
                peer.retract(delegation).await.map_err(unavailable)?;
            }
        }
    }
    Ok(())
}

/// Have `new` delegate what `old` did, to all but those `without`, and
/// retract every delegation `old` issued.
async fn redelegate<S: PeerSpace>(
    old: &Vault,
    new: &Vault,
    without: &[Did],
    peer: &Peer<S, Local>,
) -> Result<(), CredentialError> {
    let issuer = Signer::from(new.key().await?);
    for delegation in peer.issued_by(&old.did).await.map_err(unavailable)? {
        let issued = delegation
            .chain()
            .export()
            .map(|(_, issued)| issued)
            .next()
            .ok_or_else(|| unopened("a delegation with no certificate"))?;
        if !without.contains(issued.audience()) {
            let builder = DelegationBuilder::new()
                .issuer(issuer.clone())
                .audience(issued.audience())
                .subject(issued.subject().clone())
                .command(issued.command().0.clone())
                .policy(issued.policy().clone());
            let builder = match issued.expiration() {
                Some(expiration) => builder.expiration(expiration),
                None => builder,
            };
            let reissued = builder
                .try_build()
                .await
                .map_err(|error| unavailable(format!("{error:?}")))?;
            retain(peer, DelegationChain::new(reissued)).await?;
        }
        peer.retract(delegation).await.map_err(unavailable)?;
    }
    Ok(())
}

/// Retain `chain` where the peer proves from.
async fn retain<S: PeerSpace>(
    peer: &Peer<S, Local>,
    chain: DelegationChain,
) -> Result<(), CredentialError> {
    Subject::from(peer.home().clone())
        .attenuate(Access)
        .invoke(Retain::<Ucan>::new(UcanDelegation::new(chain)))
        .perform(peer)
        .await
        .map_err(unavailable)
}

/// Have a vault delegate to a principal.
pub struct Delegate {
    vault: Vault,
    audience: Did,
}

impl Delegate {
    /// Issue the delegation with the vault's key and retain it where the
    /// peer proves from. The peer's own act.
    pub async fn perform<S: PeerSpace>(self, peer: &Peer<S, Local>) -> Result<(), CredentialError> {
        let delegation = DelegationBuilder::new()
            .issuer(Signer::from(self.vault.key().await?))
            .audience(&self.audience)
            .subject(UcanSubject::Any)
            .command(Vec::new())
            .try_build()
            .await
            .map_err(|error| unavailable(format!("{error:?}")))?;
        retain(peer, DelegationChain::new(delegation)).await
    }
}

/// Seal a secret to a vault.
pub struct Conceal {
    vault: Vault,
    secret: Vec<u8>,
}

impl Conceal {
    /// Seal the secret, yielding the sealed message: a fact to assert.
    pub async fn perform<S: PeerSpace, M: Mode>(
        self,
        _peer: &Peer<S, M>,
    ) -> Result<SealedMessage, CredentialError> {
        let sealed = sealable(&self.vault.did)?
            .secret(SECRET)
            .conceal(&self.secret)
            .await
            .map_err(unavailable)?;
        Ok(secrets::sealed_message(&self.vault.did, sealed.to_bytes()))
    }
}

/// Open a message sealed to a vault.
pub struct Reveal {
    vault: Vault,
    message: SealedMessage,
}

impl Reveal {
    /// Open the message with the vault's key.
    pub async fn perform<S: PeerSpace, M: Mode>(
        self,
        _peer: &Peer<S, M>,
    ) -> Result<Vec<u8>, CredentialError> {
        if self.message.to.0.to_string() != self.vault.did.to_string() {
            return Err(CredentialError::Withheld(format!(
                "the message is not sealed to {}",
                self.vault.did
            )));
        }
        self.vault
            .key()
            .await?
            .secret(SECRET)
            .reveal(&SealedSecret::from_bytes(&self.message.message.0).map_err(unopened)?)
            .await
            .map_err(unopened)
    }
}

/// A secret a vault keeps by name.
pub struct SecretReference {
    vault: Vault,
    name: String,
}

impl SecretReference {
    /// Seal `secret` to the vault and keep it under the name, in place of
    /// what was kept under it before.
    pub fn conceal(self, secret: impl Into<Vec<u8>>) -> KeepSecret {
        KeepSecret {
            secret: self,
            value: secret.into(),
        }
    }

    /// Open the secret kept under the name.
    pub fn reveal(self) -> RevealSecret {
        RevealSecret { secret: self }
    }

    /// Forget the secret kept under the name.
    pub fn forget(self) -> ForgetSecret {
        ForgetSecret { secret: self }
    }
}

/// Keep a secret in a vault.
pub struct KeepSecret {
    secret: SecretReference,
    value: Vec<u8>,
}

impl KeepSecret {
    /// Seal the secret and record it under its name. The peer's own act.
    pub async fn perform<S: PeerSpace>(self, peer: &Peer<S, Local>) -> Result<(), CredentialError> {
        let vault = self.secret.vault;
        let branch = opened(&vault.space, peer).await?;
        let sealed = vault.conceal(self.value).perform(peer).await?;
        secrets::keep_secret(&branch, &vault.did, &self.secret.name, sealed, peer)
            .await
            .map_err(unavailable)
    }
}

/// Open a secret a vault keeps.
pub struct RevealSecret {
    secret: SecretReference,
}

impl RevealSecret {
    /// Find the secret kept under the name and open it.
    pub async fn perform<S: PeerSpace, M: Mode>(
        self,
        peer: &Peer<S, M>,
    ) -> Result<Vec<u8>, CredentialError> {
        let vault = self.secret.vault;
        let branch = opened(&vault.space, peer).await?;
        let sealed = secrets::secret(&branch, &vault.did, &self.secret.name, peer)
            .await
            .map_err(unavailable)?
            .ok_or_else(|| CredentialError::NotFound(format!("no secret {}", self.secret.name)))?;
        vault.reveal(sealed).perform(peer).await
    }
}

/// Forget a secret a vault keeps.
pub struct ForgetSecret {
    secret: SecretReference,
}

impl ForgetSecret {
    /// Retract the secret kept under the name. The peer's own act.
    pub async fn perform<S: PeerSpace>(self, peer: &Peer<S, Local>) -> Result<(), CredentialError> {
        let vault = self.secret.vault;
        let branch = opened(&vault.space, peer).await?;
        secrets::forget_secret(&branch, &vault.did, &self.secret.name, peer)
            .await
            .map_err(unavailable)
    }
}

impl<S: Clone, M: Mode> Peer<S, M> {
    /// The account this peer acts on behalf of: the `account` vault its
    /// space records. Read from the space each time, so it follows a
    /// sign-in without the peer being built again.
    pub async fn authority(&self) -> Result<Did, CredentialError>
    where
        Self: RegistryEnv,
    {
        self.state().refresh(self).await.map_err(unavailable)?;
        secrets::root(self.state(), ACCOUNT, self)
            .await
            .map_err(unavailable)?
            .ok_or_else(|| CredentialError::NotFound("no account vault".to_string()))
    }
}

impl<S: PeerSpace, M: Mode> Peer<S, M> {
    /// Refuse a site credential kept under a subject other than this
    /// peer's home: the peer keeps its own, not anyone else's.
    fn keeps(&self, subject: &Did) -> Result<(), CredentialError> {
        if subject == self.home() {
            Ok(())
        } else {
            Err(CredentialError::Withheld(format!(
                "{subject} keeps its own site credentials, not {}",
                self.home()
            )))
        }
    }
}

/// A site's credential is sealed to the peer itself and kept in its
/// space under the site's name: the peer, which does the syncing, opens
/// it, and nothing else does. A session is refused: it writes nothing to
/// its peer's space and is handed no secret of its peer's.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<credential::Save<SiteSecret>> for Peer<S, M>
where
    S: PeerSpace,
{
    async fn execute(
        &self,
        input: Capability<credential::Save<SiteSecret>>,
    ) -> Result<(), CredentialError> {
        self.keeps(input.subject())?;
        session_writes_nothing::<M>(&self.did())?;
        let site = credential::Site::of(&input).address.to_string();
        let secret = credential::Save::<SiteSecret>::of(&input)
            .credential
            .as_bytes()
            .to_vec();
        let sealed = sealable(&self.did())?
            .secret(SECRET)
            .conceal(&secret)
            .await
            .map_err(unavailable)?;
        let message = secrets::sealed_message(&self.did(), sealed.to_bytes());
        let state = self.state();
        state.refresh(self).await.map_err(unavailable)?;
        secrets::keep_secret(state, &self.did(), &site, message, self)
            .await
            .map_err(unavailable)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<credential::Load<SiteSecret>> for Peer<S, M>
where
    S: PeerSpace,
{
    async fn execute(
        &self,
        input: Capability<credential::Load<SiteSecret>>,
    ) -> Result<SiteSecret, CredentialError> {
        self.keeps(input.subject())?;
        if !M::HOLDS_KEYS {
            return Err(CredentialError::Withheld(format!(
                "{} is a session: its peer's site secrets are not handed to it",
                self.did()
            )));
        }
        let site = credential::Site::of(&input).address.to_string();
        let state = self.state();
        state.refresh(self).await.map_err(unavailable)?;
        let sealed = secrets::secret(state, &self.did(), &site, self)
            .await
            .map_err(unavailable)?
            .ok_or_else(|| CredentialError::NotFound(format!("no secret for {site}")))?;
        let opener = self.credential().signer().as_ed25519().ok_or_else(|| {
            CredentialError::Withheld(format!("{} opens nothing sealed", self.did()))
        })?;
        let revealed = opener
            .secret(SECRET)
            .reveal(&SealedSecret::from_bytes(&sealed.message.0).map_err(unopened)?)
            .await
            .map_err(unopened)?;
        Ok(SiteSecret::from(revealed))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<credential::Retract<SiteSecret>> for Peer<S, M>
where
    S: PeerSpace,
{
    async fn execute(
        &self,
        input: Capability<credential::Retract<SiteSecret>>,
    ) -> Result<(), CredentialError> {
        self.keeps(input.subject())?;
        session_writes_nothing::<M>(&self.did())?;
        let site = credential::Site::of(&input).address.to_string();
        let state = self.state();
        state.refresh(self).await.map_err(unavailable)?;
        secrets::forget_secret(state, &self.did(), &site, self)
            .await
            .map_err(unavailable)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::SpaceVaultExt as _;
    use crate::Peer;
    use crate::helpers::{
        test_custodian, test_grant, test_peer, test_session_with_peer, test_storage, unique_name,
    };
    use dialog_credentials::{Ed25519Signer, SignerCredential};
    use dialog_effects::credential::{CredentialError, Secret};
    use dialog_effects::storage::Location;
    use dialog_repository::{BranchReference, Repository, secrets};
    use dialog_storage::provider::storage::{Storage, VolatileSpace};
    use dialog_varsig::Principal as _;

    /// A peer keeping its records in `space`, holding no vault key.
    async fn peer_on(
        space: BranchReference,
        storage: Storage<VolatileSpace>,
    ) -> anyhow::Result<Peer<VolatileSpace>> {
        let key = SignerCredential::from(Ed25519Signer::generate().await?);
        Ok(Peer::new(key)
            .space(space)
            .with(storage)
            .grant(test_grant().await)
            .await?)
    }

    /// A peer with a space of its own that records no vault.
    async fn bare_peer() -> anyhow::Result<Peer<VolatileSpace>> {
        let key = SignerCredential::from(Ed25519Signer::generate().await?);
        let space = Repository::from(key.did()).branch("main");
        Ok(Peer::new(key)
            .at(Location::profile(unique_name("bare")))
            .space(space)
            .with(test_storage().await)
            .grant(test_grant().await)
            .await?)
    }

    /// `load` fails for a missing vault, `create` refuses an existing one,
    /// and `open` does whichever applies; each opens through the
    /// custodian that guards the account.
    #[dialog_common::test]
    async fn it_opens_creates_and_loads_as_asked() -> anyhow::Result<()> {
        let peer = bare_peer().await?;
        let custodian = test_custodian(&peer).await?;
        let account = || peer.state().vault("account");

        let missing = account().load().via(&custodian).perform(&peer).await;
        assert!(
            matches!(missing, Err(CredentialError::NotFound(_))),
            "{missing:?}"
        );

        let created = account().create().perform(&peer).await?;
        created.add(custodian.did()).perform(&peer).await?;
        assert!(
            account().create().perform(&peer).await.is_err(),
            "created twice"
        );

        assert_eq!(
            account().load().via(&custodian).perform(&peer).await?.did(),
            created.did()
        );
        assert_eq!(
            account().open().via(&custodian).perform(&peer).await?.did(),
            created.did()
        );
        Ok(())
    }

    /// The peer is not a member of its account: it acts for the account
    /// through what the account delegates to it, and opens no copy of
    /// the account's key.
    #[dialog_common::test]
    async fn it_holds_no_copy_of_its_accounts_key() -> anyhow::Result<()> {
        let peer = test_peer().await;
        let refused = peer.state().vault("account").load().perform(&peer).await;
        assert!(
            matches!(refused, Err(CredentialError::Withheld(_))),
            "{refused:?}"
        );
        let custodian = test_custodian(&peer).await?;
        assert!(
            peer.state()
                .vault("account")
                .load()
                .via(&custodian)
                .perform(&peer)
                .await
                .is_ok(),
            "the custodian opens the account"
        );
        Ok(())
    }

    /// A child is derived from the key above it, whether that key is in
    /// hand or had through a copy of it: the same child either way.
    #[dialog_common::test]
    async fn it_derives_a_child_from_the_key_above_it() -> anyhow::Result<()> {
        let peer = bare_peer().await?;
        let custodian = test_custodian(&peer).await?;
        let account = peer
            .state()
            .vault("account")
            .create()
            .perform(&peer)
            .await?;
        account.add(custodian.did()).perform(&peer).await?;

        let in_hand = account.vault("peer").open().perform(&peer).await?;
        let through_copy = peer
            .state()
            .vault("account")
            .vault("peer")
            .open()
            .via(&custodian)
            .perform(&peer)
            .await?;
        assert_eq!(in_hand.did(), through_copy.did());
        Ok(())
    }

    /// A member of a child opens it through its own copy without holding
    /// anything above it, and opens nothing above it.
    #[dialog_common::test]
    async fn it_opens_a_child_its_member_holds_without_the_parent() -> anyhow::Result<()> {
        let peer = bare_peer().await?;
        let account = peer
            .state()
            .vault("account")
            .create()
            .perform(&peer)
            .await?;
        let peers = account.vault("peer").open().perform(&peer).await?;
        let other = peer_on(peer.state().into(), peer.storage().clone()).await?;
        peers.add(other.did()).perform(&peer).await?;

        let opened = other
            .state()
            .vault("account")
            .vault("peer")
            .load()
            .perform(&other)
            .await?;
        assert_eq!(opened.did(), peers.did());
        let above = other.state().vault("account").load().perform(&other).await;
        assert!(
            matches!(above, Err(CredentialError::Withheld(_))),
            "{above:?}"
        );
        Ok(())
    }

    /// A peer holding no key on the way to a vault neither opens nor
    /// creates it.
    #[dialog_common::test]
    async fn it_refuses_a_vault_its_peer_has_no_key_toward() -> anyhow::Result<()> {
        let peer = bare_peer().await?;
        peer.state()
            .vault("account")
            .create()
            .perform(&peer)
            .await?;
        let other = peer_on(peer.state().into(), peer.storage().clone()).await?;

        let opened = other
            .state()
            .vault("account")
            .vault("peer")
            .open()
            .perform(&other)
            .await;
        assert!(opened.is_err(), "a peer with no key opened a vault");
        Ok(())
    }

    /// A session opens what is sealed to it and creates nothing: not a
    /// vault, not a member, not a secret.
    #[dialog_common::test]
    async fn it_lets_a_session_create_no_vault() -> anyhow::Result<()> {
        let (session, peer) = test_session_with_peer().await;
        let refused = session
            .state()
            .vault("scratch")
            .create()
            .perform(&session)
            .await;
        assert!(
            matches!(refused, Err(CredentialError::Withheld(_))),
            "{refused:?}"
        );
        let refused = session
            .state()
            .vault("scratch")
            .open()
            .perform(&session)
            .await;
        assert!(
            matches!(refused, Err(CredentialError::Withheld(_))),
            "{refused:?}"
        );
        assert!(
            secrets::root(peer.state(), "scratch", &peer)
                .await?
                .is_none(),
            "the session recorded a vault"
        );
        Ok(())
    }

    /// A child record its parent did not sign is ignored: a peer cannot
    /// point a vault at a key of its own.
    #[dialog_common::test]
    async fn it_ignores_a_child_record_its_parent_did_not_sign() -> anyhow::Result<()> {
        let peer = bare_peer().await?;
        let custodian = test_custodian(&peer).await?;
        let account = peer
            .state()
            .vault("account")
            .create()
            .perform(&peer)
            .await?;
        account.add(custodian.did()).perform(&peer).await?;
        let forger = Ed25519Signer::generate().await?;
        secrets::record_child(
            peer.state(),
            &forger.did(),
            account.did(),
            "peer",
            vec![0; 64],
            &peer,
        )
        .await?;

        let peers = peer
            .state()
            .vault("account")
            .vault("peer")
            .open()
            .via(&custodian)
            .perform(&peer)
            .await?;
        assert_ne!(*peers.did(), forger.did(), "the forged record was used");
        Ok(())
    }

    /// A peer acts for the account vault its space records, and for none
    /// before one is created.
    #[dialog_common::test]
    async fn it_acts_for_the_account_its_space_records() -> anyhow::Result<()> {
        let peer = bare_peer().await?;
        assert!(peer.authority().await.is_err());
        let account = peer
            .state()
            .vault("account")
            .create()
            .perform(&peer)
            .await?;
        assert_eq!(peer.authority().await?, *account.did());
        assert_ne!(*account.did(), peer.did(), "the account's key is its own");
        Ok(())
    }

    /// A vault keeps secrets by name: concealed, revealed, replaced under
    /// the same name, and forgotten. A peer made a member of a vault below
    /// the account opens it through its own copy.
    #[dialog_common::test]
    async fn it_keeps_secrets_by_name() -> anyhow::Result<()> {
        let peer = test_peer().await;
        let custodian = test_custodian(&peer).await?;
        let account = peer
            .state()
            .vault("account")
            .open()
            .via(&custodian)
            .perform(&peer)
            .await?;
        account
            .vault("peer")
            .open()
            .perform(&peer)
            .await?
            .add(peer.did())
            .perform(&peer)
            .await?;
        let peers = peer
            .state()
            .vault("account")
            .vault("peer")
            .load()
            .perform(&peer)
            .await?;

        peers
            .secret("token")
            .conceal(b"first".to_vec())
            .perform(&peer)
            .await?;
        peers
            .secret("token")
            .conceal(b"second".to_vec())
            .perform(&peer)
            .await?;
        assert_eq!(
            peers.secret("token").reveal().perform(&peer).await?,
            b"second"
        );

        peers.secret("token").forget().perform(&peer).await?;
        let gone = peers.secret("token").reveal().perform(&peer).await;
        assert!(
            matches!(gone, Err(CredentialError::NotFound(_))),
            "{gone:?}"
        );
        Ok(())
    }

    /// Rotating the account without a member: the member opens neither the
    /// account nor anything below it afterwards, and is delegated nothing
    /// by it; the members left open both, and keep what was kept before.
    /// The account's new key is generated inside and never handed out.
    #[dialog_common::test]
    async fn it_rotates_a_member_out_of_the_account() -> anyhow::Result<()> {
        let peer = test_peer().await;
        let custodian = test_custodian(&peer).await?;
        let old = peer
            .state()
            .vault("account")
            .load()
            .via(&custodian)
            .perform(&peer)
            .await?;
        let peers = old.vault("peer").open().perform(&peer).await?;
        peers
            .secret("token")
            .conceal(b"kept".to_vec())
            .perform(&peer)
            .await?;
        let removed = peer_on(peer.state().into(), peer.storage().clone()).await?;
        old.add(removed.did()).perform(&peer).await?;
        peers.add(removed.did()).perform(&peer).await?;
        old.delegate(removed.did()).perform(&peer).await?;

        let account = old.rotate().without(removed.did()).perform(&peer).await?;
        assert_ne!(account.did(), old.did());
        assert_eq!(peer.authority().await?, *account.did());

        let reopened = peer
            .state()
            .vault("account")
            .load()
            .via(&custodian)
            .perform(&peer)
            .await?;
        assert_eq!(
            reopened.did(),
            account.did(),
            "the custodian opens the new key"
        );
        let opened = reopened.vault("peer").load().perform(&peer).await?;
        assert_ne!(opened.did(), peers.did(), "the child is derived again");
        assert_eq!(
            opened.secret("token").reveal().perform(&peer).await?,
            b"kept"
        );

        for names in [vec!["account"], vec!["account", "peer"]] {
            let mut reference = removed.state().vault(names[0]);
            for name in &names[1..] {
                reference = reference.vault(*name);
            }
            let refused = reference.load().perform(&removed).await;
            assert!(
                matches!(refused, Err(CredentialError::Withheld(_))),
                "{names:?}: {refused:?}"
            );
        }

        assert!(peer.issued_by(old.did()).await?.is_empty());
        let audiences: Vec<_> = peer
            .issued_by(account.did())
            .await?
            .into_iter()
            .map(|delegation| delegation.chain().audience().clone())
            .collect();
        assert!(audiences.contains(&peer.did()), "{audiences:?}");
        assert!(!audiences.contains(&removed.did()), "{audiences:?}");
        Ok(())
    }

    /// A vault below another is rotated with the one above it, not alone.
    #[dialog_common::test]
    async fn it_refuses_to_rotate_a_vault_below_another() -> anyhow::Result<()> {
        let peer = test_peer().await;
        let custodian = test_custodian(&peer).await?;
        let peers = peer
            .state()
            .vault("account")
            .vault("peer")
            .open()
            .via(&custodian)
            .perform(&peer)
            .await?;
        assert!(peers.rotate().perform(&peer).await.is_err());
        Ok(())
    }

    /// A site secret the peer kept and then forgot is not found.
    #[dialog_common::test]
    async fn it_saves_retracts_and_reloads_a_site_secret() -> anyhow::Result<()> {
        let peer = test_peer().await;
        let secrets = || peer.secrets().site("example.com");

        secrets()
            .save(Secret::from(vec![1u8, 2, 3]))
            .perform(&peer)
            .await?;
        assert_eq!(
            secrets().load::<Secret>().perform(&peer).await?.as_bytes(),
            &[1u8, 2, 3]
        );
        secrets().retract().perform(&peer).await?;

        let result = secrets().load::<Secret>().perform(&peer).await;
        assert!(
            matches!(result, Err(CredentialError::NotFound(_))),
            "{result:?}"
        );
        Ok(())
    }

    /// A session neither keeps nor reads its peer's site secrets.
    #[dialog_common::test]
    async fn it_withholds_site_secrets_from_a_session() -> anyhow::Result<()> {
        let (session, peer) = test_session_with_peer().await;
        peer.secrets()
            .site("example.com")
            .save(Secret::from(vec![4u8, 5, 6]))
            .perform(&peer)
            .await?;

        let saved = peer
            .secrets()
            .site("other.example")
            .save(Secret::from(vec![7u8]))
            .perform(&session)
            .await;
        assert!(
            matches!(saved, Err(CredentialError::Withheld(_))),
            "{saved:?}"
        );
        let loaded = peer
            .secrets()
            .site("example.com")
            .load::<Secret>()
            .perform(&session)
            .await;
        assert!(
            matches!(loaded, Err(CredentialError::Withheld(_))),
            "{loaded:?}"
        );
        Ok(())
    }
}
