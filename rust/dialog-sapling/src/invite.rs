//! The invite that a share of Sapling prints, and that a join takes. See
//! Sapling's PDD006, "Share" and "Suggested Names".
//!
//! An invite is one line of text: the prefix [`PREFIX`], then the base64url
//! form, with no padding, of a dag-cbor map with these fields:
//!
//! | Field    | Value                                                          |
//! | -------- | -------------------------------------------------------------- |
//! | `space`  | The DID of the space                                           |
//! | `chain`  | The delegations space → sharer and sharer → joiner, as a UCAN  |
//! |          | container                                                      |
//! | `peer`   | The DID of the device peer of the sharer                       |
//! | `routes` | The routes of the endpoint of the sharer, when the share ran   |
//! | `name`   | The suggested name of the sharer: its shown name in the space. |
//! |          | Absent when it has none.                                       |
//!
//! The invite holds no name of the space, and no secret: it works only for the
//! persona that its chain names. The decode reads the fields, and checks no
//! signature.
//!
//! The last link of the chain, from the sharer to the joiner, carries `peer`
//! and `name` again in its `meta`, under [`META_PEER`] and [`META_NAME`], so
//! the signature of the sharer covers them. It also carries the kernel record
//! of the `main` of the sharer, under [`META_KERNEL`].
//! [`Invite::check_signed_fields`] refuses an invite whose `peer` or `name` is
//! not the one that the sharer signed. The routes are hints, and no signature
//! covers them.
//!
//! An invite of an earlier version, with the prefix [`EARLIER_PREFIX`], signs
//! neither its peer nor its name. The decode refuses it, and the sharer
//! shares again.

use std::collections::BTreeMap;
use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use dialog_ucan_core::DelegationChain;
use dialog_varsig::Did;
use ipld_core::ipld::Ipld;

use crate::did::parse_key;

/// The start of each invite, which names its version.
pub const PREFIX: &str = "sapling-invite-2.";

/// The start of an invite of the version before, whose chain signs neither
/// its peer nor its name.
pub const EARLIER_PREFIX: &str = "sapling-invite-1.";

/// The key of the `meta` of the last link of the chain that holds the DID of
/// the device peer of the sharer, as text.
pub const META_PEER: &str = "sapling.invite/peer";

/// The key of the `meta` of the last link of the chain that holds the
/// suggested name of the sharer, as text. Absent when the invite has no name.
pub const META_NAME: &str = "sapling.invite/name";

/// The key of the `meta` of the last link of the chain that holds the bytes
/// of the kernel record of the `main` of the sharer. Absent when the sharer
/// held none.
pub const META_KERNEL: &str = "sapling.invite/kernel";

/// The `meta` of the link from the sharer to the joiner of an invite whose
/// peer is `peer`, whose name is `name`, and whose kernel record is
/// `kernel`.
pub fn meta(peer: &Did, name: Option<&str>, kernel: Option<&[u8]>) -> BTreeMap<String, Ipld> {
    let mut meta = BTreeMap::from([(META_PEER.to_owned(), Ipld::String(peer.to_string()))]);
    if let Some(name) = name {
        meta.insert(META_NAME.to_owned(), Ipld::String(name.to_owned()));
    }
    if let Some(kernel) = kernel {
        meta.insert(META_KERNEL.to_owned(), Ipld::Bytes(kernel.to_vec()));
    }
    meta
}

/// The `meta` of the last link of `chain`, which its signature covers: empty
/// when it has none.
pub fn last_meta(chain: &DelegationChain) -> BTreeMap<String, Ipld> {
    chain
        .proofs()
        .last()
        .map(|delegation| delegation.meta().clone())
        .unwrap_or_default()
}

/// The fields of an invite.
#[derive(Clone, Debug, PartialEq)]
pub struct Invite {
    pub space: Did,
    pub chain: DelegationChain,
    pub peer: Did,
    pub routes: Vec<String>,
    pub name: Option<String>,
}

impl Invite {
    /// The sharer: the issuer of the last link of the chain.
    pub fn sharer(&self) -> Option<&Did> {
        self.chain.proofs().last().map(|link| link.issuer())
    }

    /// The invite as one line of text.
    pub fn encode(&self) -> Result<String, InviteError> {
        let chain = self
            .chain
            .to_bytes()
            .map_err(|error| InviteError(format!("cannot encode the chain: {error}")))?;
        let mut map = BTreeMap::from([
            ("space".to_owned(), Ipld::String(self.space.to_string())),
            ("chain".to_owned(), Ipld::Bytes(chain)),
            ("peer".to_owned(), Ipld::String(self.peer.to_string())),
            (
                "routes".to_owned(),
                Ipld::List(self.routes.iter().cloned().map(Ipld::String).collect()),
            ),
        ]);
        if let Some(name) = &self.name {
            map.insert("name".to_owned(), Ipld::String(name.clone()));
        }
        let bytes = serde_ipld_dagcbor::to_vec(&Ipld::Map(map))
            .map_err(|error| InviteError(format!("cannot encode the invite: {error}")))?;
        Ok(format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes)))
    }

    /// The invite of `text`, as [`Invite::encode`] makes it. Surrounding
    /// white space is ignored. The decode checks the form, and no signature.
    pub fn decode(text: &str) -> Result<Self, InviteError> {
        let text = text.trim();
        if text.starts_with(EARLIER_PREFIX) {
            return Err(InviteError(
                "the invite is of an earlier version of Sapling, which signs neither its peer \
                 nor its name: ask the sharer to share the space again"
                    .to_owned(),
            ));
        }
        let encoded = text.strip_prefix(PREFIX).ok_or_else(|| {
            InviteError(format!(
                "an invite starts with {PREFIX:?}, and this text does not"
            ))
        })?;
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|error| InviteError(format!("the invite is not base64url: {error}")))?;
        let Ipld::Map(map) = serde_ipld_dagcbor::from_slice::<Ipld>(&bytes)
            .map_err(|error| InviteError(format!("the invite is not dag-cbor: {error}")))?
        else {
            return Err(InviteError("the invite is not a map".to_owned()));
        };
        let text = |field: &str| match map.get(field) {
            Some(Ipld::String(text)) => Ok(text.clone()),
            _ => Err(InviteError(format!("the invite has no text `{field}`"))),
        };
        let did = |field: &str| {
            text(field).and_then(|text| {
                parse_key(&text).ok_or_else(|| {
                    InviteError(format!("the `{field}` of the invite is not a did:key"))
                })
            })
        };
        let chain = match map.get("chain") {
            Some(Ipld::Bytes(bytes)) => DelegationChain::try_from(bytes.as_slice())
                .map_err(|error| InviteError(format!("the chain of the invite: {error}")))?,
            _ => return Err(InviteError("the invite has no bytes `chain`".to_owned())),
        };
        let routes = match map.get("routes") {
            Some(Ipld::List(routes)) => routes
                .iter()
                .map(|route| match route {
                    Ipld::String(route) => Ok(route.clone()),
                    _ => Err(InviteError("a route of the invite is not text".to_owned())),
                })
                .collect::<Result<Vec<_>, _>>()?,
            _ => return Err(InviteError("the invite has no list `routes`".to_owned())),
        };
        let name = match map.get("name") {
            None => None,
            Some(Ipld::String(name)) => Some(name.clone()),
            Some(_) => {
                return Err(InviteError(
                    "the `name` of the invite is not text".to_owned(),
                ));
            }
        };
        Ok(Self {
            space: did("space")?,
            chain,
            peer: did("peer")?,
            routes,
            name,
        })
    }

    /// Checks that `peer` and `name` are those of the `meta` of the last link
    /// of the chain, which the sharer signed. It checks no signature: the
    /// join verifies the chain.
    pub fn check_signed_fields(&self) -> Result<(), InviteError> {
        let meta = last_meta(&self.chain);
        let text = |key: &str| match meta.get(key) {
            None => Ok(None),
            Some(Ipld::String(text)) => Ok(Some(text.as_str())),
            Some(_) => Err(InviteError(format!(
                "the `{key}` that the sharer signed is not text"
            ))),
        };
        if text(META_PEER)? != Some(self.peer.as_str()) {
            return Err(InviteError(format!(
                "its peer, {}, is not the peer that the sharer signed",
                self.peer
            )));
        }
        if text(META_NAME)? != self.name.as_deref() {
            return Err(InviteError(
                "its name is not the name that the sharer signed".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Text that is not an invite.
#[derive(Debug, PartialEq, Eq)]
pub struct InviteError(String);

impl fmt::Display for InviteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for InviteError {}

/// Invites for tests: a new space, shared by a new sharer to a new joiner.
#[cfg(any(test, feature = "helpers"))]
pub mod helpers {
    use std::collections::BTreeMap;

    use dialog_credentials::{Ed25519Signer, Signer};
    use dialog_ucan_core::subject::Subject;
    use dialog_ucan_core::{DelegationBuilder, DelegationChain};
    use dialog_varsig::{Did, Principal as _};
    use ipld_core::ipld::Ipld;

    use super::{Invite, meta};

    /// A delegation from `issuer` to `audience` over `subject`, with `meta`.
    async fn delegate(
        issuer: &Ed25519Signer,
        audience: &Did,
        subject: &Did,
        meta: BTreeMap<String, Ipld>,
    ) -> DelegationChain {
        let delegation = DelegationBuilder::new()
            .issuer(Signer::from(issuer.clone()))
            .audience(audience)
            .subject(Subject::Specific(subject.clone()))
            .command(vec!["use".to_owned()])
            .meta(meta)
            .try_build()
            .await
            .expect("a delegation signs");
        DelegationChain::new(delegation)
    }

    /// An invite of a new space, from a new sharer to a new joiner, with the
    /// suggested name `name`, and the sharer.
    pub async fn invite(name: Option<&str>) -> (Invite, Did) {
        let generate = || async { Ed25519Signer::generate().await.expect("a key") };
        let (space, ana, ben, peer) = (
            generate().await,
            generate().await,
            generate().await,
            generate().await,
        );
        let first = delegate(&space, &ana.did(), &space.did(), BTreeMap::new()).await;
        let last = delegate(
            &ana,
            &ben.did(),
            &space.did(),
            meta(&peer.did(), name, None),
        )
        .await;
        let mut chain = first;
        for link in last.proofs() {
            chain = chain.push(link.clone()).expect("the chain links up");
        }
        let invite = Invite {
            space: space.did(),
            chain,
            peer: peer.did(),
            routes: vec![
                "ip:192.0.2.7:4433".to_owned(),
                "relay:https://relay.example/".to_owned(),
            ],
            name: name.map(str::to_owned),
        };
        (invite, ana.did())
    }
}

#[cfg(test)]
mod tests {
    use dialog_credentials::Ed25519Signer;
    use dialog_varsig::Principal as _;

    use super::helpers::invite;
    use super::*;

    #[tokio::test]
    async fn it_decodes_the_five_fields_of_an_invite_that_it_encoded() {
        let (invite, sharer) = invite(Some("Ana")).await;

        let text = invite.encode().unwrap();

        assert!(text.starts_with(PREFIX), "{text}");
        assert!(!text.contains(['=', '+', '/', '\n']), "{text}");
        let decoded = Invite::decode(&format!("  {text}\n")).unwrap();
        assert_eq!(decoded, invite);
        assert_eq!(decoded.sharer(), Some(&sharer));
    }

    #[tokio::test]
    async fn it_holds_no_name_when_the_sharer_has_no_shown_name() {
        let (invite, _) = invite(None).await;

        let text = invite.encode().unwrap();
        let encoded = text.strip_prefix(PREFIX).unwrap();
        let bytes = URL_SAFE_NO_PAD.decode(encoded).unwrap();
        let Ipld::Map(map) = serde_ipld_dagcbor::from_slice::<Ipld>(&bytes).unwrap() else {
            panic!("an invite is a map");
        };

        let fields: Vec<&str> = map.keys().map(String::as_str).collect();
        assert_eq!(fields, ["chain", "peer", "routes", "space"]);
        assert_eq!(Invite::decode(&text).unwrap().name, None);
    }

    #[test]
    fn it_refuses_text_that_is_not_an_invite() {
        for text in [
            "",
            "sapling-invite-3.AAAA",
            "sapling-invite-2.%%%",
            "sapling-invite-2.oA",
        ] {
            assert!(Invite::decode(text).is_err(), "{text:?}");
        }
    }

    /// The `peer` and the `name` of an invite are in the `meta` of the link
    /// that the sharer signed: another party that changes either one makes
    /// an invite that fails the check.
    #[tokio::test]
    async fn it_refuses_an_invite_whose_peer_or_name_changed() {
        let (invite, _) = invite(Some("Ana")).await;
        assert_eq!(invite.check_signed_fields(), Ok(()));
        let other = Ed25519Signer::generate().await.unwrap().did();

        let changed = [
            Invite {
                peer: other,
                ..invite.clone()
            },
            Invite {
                name: Some("Bank".to_owned()),
                ..invite.clone()
            },
            Invite {
                name: None,
                ..invite.clone()
            },
        ];

        for changed in changed {
            let decoded = Invite::decode(&changed.encode().unwrap()).unwrap();
            assert!(decoded.check_signed_fields().is_err(), "{changed:?}");
        }
        let unnamed = Invite {
            name: Some("Ana".to_owned()),
            ..self::invite(None).await.0
        };
        assert!(unnamed.check_signed_fields().is_err());
    }

    /// An invite of the version before signs neither its peer nor its name,
    /// so the decode refuses it, and asks for a new share.
    #[tokio::test]
    async fn it_refuses_an_invite_of_the_earlier_version() {
        let text = invite(Some("Ana")).await.0.encode().unwrap();
        let earlier = text.replacen(PREFIX, EARLIER_PREFIX, 1);

        let refused = Invite::decode(&earlier).unwrap_err().to_string();

        assert!(refused.contains("share the space again"), "{refused}");
    }
}
