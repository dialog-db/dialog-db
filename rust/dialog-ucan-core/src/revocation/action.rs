//! The `/ucan/revoke` invocation.
//!
//! A revocation is an ordinary [`Invocation`] whose command is
//! `/ucan/revoke`. [`Revocation`] is a newtype that has checked its *shape*:
//! the command, the empty nonce, and that `args.rev` and `args.pth` are the
//! link types the schema names.
//!
//! Shape is all this checks. Whether the revoker was *authorized* over the
//! delegation it names is a question about a whole container, and lives on
//! [`RevocationChain`](crate::container::RevocationChain).

use crate::{Invocation, command::Command, crypto::nonce::Nonce, promise::Promised};
use dialog_varsig::{Did, Signature};
use ipld_core::cid::Cid;
use std::sync::LazyLock;
use thiserror::Error;

/// The command every revocation carries.
pub static REVOKE: LazyLock<Command> = LazyLock::new(|| {
    #[allow(clippy::expect_used)]
    Command::parse("/ucan/revoke").expect("a compile-time constant command must parse")
});

/// The argument naming the delegation being revoked.
pub const REVOKED: &str = "rev";

/// The argument carrying the witness path.
pub const PATH: &str = "pth";

/// An [`Invocation`] whose shape conforms to the revocation schema.
///
/// ```text
/// type RevocationAction <: Action {
///   cmd "/ucan/revoke"
///   nnc ""
///   arg RevocationArguments
/// }
///
/// type RevocationArguments struct {
///   rev &Delegation
///   pth [&Delegation]
/// }
/// ```
///
/// Constructing one proves the shape holds; it proves nothing about
/// authority.
#[derive(Debug, Clone)]
pub struct Revocation<S: Signature> {
    invocation: Invocation<S>,
    revoked: Cid,
    path: Vec<Cid>,
}

impl<S: Signature> Revocation<S> {
    /// The delegation this revokes.
    #[must_use]
    pub const fn revoked(&self) -> &Cid {
        &self.revoked
    }

    /// The witness path: the delegations offered as proof that the revoker
    /// held authority over [`revoked`](Self::revoked).
    #[must_use]
    pub fn path(&self) -> &[Cid] {
        &self.path
    }

    /// The principal issuing this revocation.
    #[must_use]
    pub const fn revoker(&self) -> &Did {
        self.invocation.issuer()
    }

    /// The underlying invocation.
    #[must_use]
    pub const fn invocation(&self) -> &Invocation<S> {
        &self.invocation
    }

    /// Consume this, returning the underlying invocation.
    #[must_use]
    pub fn into_invocation(self) -> Invocation<S> {
        self.invocation
    }
}

/// Why an invocation is not a well-formed revocation.
///
/// Every variant says the artifact is malformed, never that its issuer was
/// unauthorized: authority is judged separately, and only once the shape is
/// known to hold.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MalformedRevocation {
    /// The command is not `/ucan/revoke`.
    #[error("expected command '{expected}', found '{found}'")]
    NotARevocation {
        /// The command a revocation must carry.
        expected: Command,
        /// The command this invocation carries.
        found: Command,
    },

    /// The nonce is not empty. Revocation is idempotent, so the schema
    /// pins `nnc` to `""`; a nonce would make two revocations of the same
    /// delegation distinct artifacts.
    #[error("a revocation's nonce must be empty")]
    NonEmptyNonce,

    /// A required argument is absent.
    #[error("missing required argument '{0}'")]
    MissingArgument(&'static str),

    /// An argument is present but not the type the schema names.
    #[error("argument '{argument}' must be {expected}")]
    WrongArgumentType {
        /// The offending argument.
        argument: &'static str,
        /// What the schema requires.
        expected: &'static str,
    },
}

impl<S: Signature> TryFrom<Invocation<S>> for Revocation<S> {
    type Error = MalformedRevocation;

    fn try_from(invocation: Invocation<S>) -> Result<Self, Self::Error> {
        if invocation.command() != &*REVOKE {
            return Err(MalformedRevocation::NotARevocation {
                expected: REVOKE.clone(),
                found: invocation.command().clone(),
            });
        }

        // `nnc ""` in the schema. An empty custom nonce and a zero-length
        // byte string are the same thing on the wire.
        match invocation.nonce() {
            Nonce::Custom(bytes) if bytes.is_empty() => {}
            _ => return Err(MalformedRevocation::NonEmptyNonce),
        }

        let arguments = invocation.arguments();

        let revoked = match arguments.get(REVOKED) {
            Some(Promised::Link(cid)) => *cid,
            Some(_) => {
                return Err(MalformedRevocation::WrongArgumentType {
                    argument: REVOKED,
                    expected: "a delegation link",
                });
            }
            None => return Err(MalformedRevocation::MissingArgument(REVOKED)),
        };

        // `pth` is a list of links. A single link is not a list: the schema
        // names `[&Delegation]`, and accepting a bare link would make the
        // one-hop case shaped differently from every other.
        let path = match arguments.get(PATH) {
            Some(Promised::List(items)) => items
                .iter()
                .map(|item| match item {
                    Promised::Link(cid) => Ok(*cid),
                    _ => Err(MalformedRevocation::WrongArgumentType {
                        argument: PATH,
                        expected: "a list of delegation links",
                    }),
                })
                .collect::<Result<Vec<Cid>, _>>()?,
            Some(_) => {
                return Err(MalformedRevocation::WrongArgumentType {
                    argument: PATH,
                    expected: "a list of delegation links",
                });
            }
            None => return Err(MalformedRevocation::MissingArgument(PATH)),
        };

        Ok(Self {
            invocation,
            revoked,
            path,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::generate_signer;
    use crate::{InvocationBuilder, cid::to_dagcbor_cid, revocation::builder::RevocationBuilder};
    use dialog_varsig::{AnySignature, Principal};
    use std::collections::BTreeMap;
    use testresult::TestResult;

    fn link() -> Cid {
        to_dagcbor_cid(&"a delegation")
    }

    /// Build an invocation with the given command and arguments, so shape
    /// checks can be exercised one deviation at a time.
    async fn invocation(
        command: Vec<String>,
        arguments: BTreeMap<String, Promised>,
        nonce: Nonce,
    ) -> TestResult<Invocation<AnySignature>> {
        let signer = generate_signer().await;
        Ok(InvocationBuilder::new()
            .issuer(signer.clone())
            .audience(&signer.did())
            .subject(&signer.did())
            .command(command)
            .arguments(arguments)
            .proofs(vec![])
            .nonce(nonce)
            .try_build()
            .await?)
    }

    fn well_formed_args() -> BTreeMap<String, Promised> {
        [
            (REVOKED.to_string(), Promised::Link(link())),
            (
                PATH.to_string(),
                Promised::List(vec![Promised::Link(link())]),
            ),
        ]
        .into_iter()
        .collect()
    }

    fn revoke_command() -> Vec<String> {
        REVOKE.segments().clone()
    }

    #[dialog_common::test]
    async fn the_builder_produces_a_well_formed_revocation() -> TestResult {
        let alice = generate_signer().await;
        let target = link();

        let revocation: Revocation<AnySignature> = RevocationBuilder::new(alice.clone(), target)
            .witness(target)
            .try_build()
            .await?;

        assert_eq!(revocation.revoked(), &target);
        assert_eq!(revocation.path(), &[target]);
        // The revoker is the subject: this artifact is about their
        // withdrawal, not about the capability being withdrawn.
        assert_eq!(revocation.revoker(), &alice.did());
        assert_eq!(revocation.invocation().subject(), &alice.did());
        Ok(())
    }

    #[dialog_common::test]
    async fn another_command_is_not_a_revocation() -> TestResult {
        let other = invocation(
            vec!["storage".to_string(), "get".to_string()],
            well_formed_args(),
            Nonce::Custom(Vec::new()),
        )
        .await?;

        assert!(matches!(
            Revocation::try_from(other),
            Err(MalformedRevocation::NotARevocation { .. })
        ));
        Ok(())
    }

    #[dialog_common::test]
    async fn a_nonce_makes_it_malformed() -> TestResult {
        // Revocation is idempotent, so the schema pins `nnc` to "". A nonce
        // would make two revocations of the same delegation distinct.
        let nonced =
            invocation(revoke_command(), well_formed_args(), Nonce::generate_16()?).await?;

        assert!(matches!(
            Revocation::try_from(nonced),
            Err(MalformedRevocation::NonEmptyNonce)
        ));
        Ok(())
    }

    #[dialog_common::test]
    async fn a_missing_argument_is_malformed() -> TestResult {
        for absent in [REVOKED, PATH] {
            let mut args = well_formed_args();
            args.remove(absent);
            let missing = invocation(revoke_command(), args, Nonce::Custom(Vec::new())).await?;

            assert!(
                matches!(
                    Revocation::try_from(missing),
                    Err(MalformedRevocation::MissingArgument(name)) if name == absent
                ),
                "'{absent}' is required"
            );
        }
        Ok(())
    }

    #[dialog_common::test]
    async fn a_bare_link_is_not_a_path() -> TestResult {
        // `pth` is `[&Delegation]`. Accepting a bare link would make the
        // one-hop case shaped differently from every other.
        let mut args = well_formed_args();
        args.insert(PATH.to_string(), Promised::Link(link()));
        let wrong = invocation(revoke_command(), args, Nonce::Custom(Vec::new())).await?;

        assert!(matches!(
            Revocation::try_from(wrong),
            Err(MalformedRevocation::WrongArgumentType { argument: PATH, .. })
        ));
        Ok(())
    }

    #[dialog_common::test]
    async fn a_non_link_argument_is_malformed() -> TestResult {
        let mut args = well_formed_args();
        args.insert(REVOKED.to_string(), Promised::String("not a link".into()));
        let wrong = invocation(revoke_command(), args, Nonce::Custom(Vec::new())).await?;

        assert!(matches!(
            Revocation::try_from(wrong),
            Err(MalformedRevocation::WrongArgumentType {
                argument: REVOKED,
                ..
            })
        ));
        Ok(())
    }

    #[dialog_common::test]
    async fn an_empty_path_is_shape_valid() -> TestResult {
        // The schema does not mark `pth` optional, but an empty list is a
        // list. Whether it *justifies* anything is `validate`'s question,
        // and it cannot: `rev` is not in an empty path.
        let mut args = well_formed_args();
        args.insert(PATH.to_string(), Promised::List(vec![]));
        let empty = invocation(revoke_command(), args, Nonce::Custom(Vec::new())).await?;

        let revocation = Revocation::try_from(empty)?;
        assert!(revocation.path().is_empty());
        Ok(())
    }
}
