//! Binding a payload to the invocation that authorizes it.
//!
//! A signed invocation does **not** carry the bytes it authorizes. Its
//! arguments come from the capability's attenuation, and for a write
//! that attenuation is a projection: `archive::Put` declares
//!
//! ```text
//! #[attenuate(into = Blake3Hash, with = digest_of,   rename = digest)]
//! #[attenuate(into = Checksum,   with = checksum_of, rename = checksum)]
//! pub block: Buffer,
//! ```
//!
//! so the signature covers a `digest` and a `checksum` of the block,
//! never the block. That is deliberate — it is what lets an access
//! service presign a URL for `{subject}/{catalog}/{digest}` without ever
//! holding the content — and it is the reason this crate's [`Request`]
//! carries two payloads rather than one.
//!
//! [`Request`]: crate::wire::Request
//!
//! # The check this module is
//!
//! Two payloads means two sources of truth, and a signature that covers
//! only one of them authorizes nothing unless they are made to agree. So
//! before a peer performs anything it re-derives the arguments from the
//! payload it was handed and checks them against the arguments that were
//! actually signed. A block whose digest is not the signed digest is not
//! a corrupted upload to be repaired; it is an unauthorized write
//! wearing an authorized invocation, and it is refused.
//!
//! The check is a pure function of decoded values, with no crypto and no
//! I/O, which is why it lives apart from chain verification rather than
//! inside it: chain verification asks whether the invocation is real,
//! this asks whether the payload is the one it spoke about. Both have to
//! hold, and neither implies the other.

use dialog_capability::{Ability, Capability, Constraint, Effect};
use dialog_ucan::{Args, parameters, parameters_to_args};
use dialog_varsig::did::Did;

/// Why a payload does not belong to its invocation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BindError {
    /// The invocation authorizes a different command than the one the
    /// payload was decoded as.
    #[error("invocation is for '{signed}', payload is for '{claimed}'")]
    Command {
        /// The command path the invocation carries.
        signed: String,
        /// The command path the payload was read as.
        claimed: String,
    },
    /// The invocation authorizes a different subject than the payload
    /// names.
    #[error("invocation is for subject '{signed}', payload names '{claimed}'")]
    Subject {
        /// The subject the invocation carries.
        signed: Box<Did>,
        /// The subject the payload names.
        claimed: Box<Did>,
    },
    /// An argument the invocation signed is absent from the payload's
    /// own derived arguments.
    #[error("invocation signed '{parameter}', which the payload does not carry")]
    Missing {
        /// The parameter that went missing.
        parameter: String,
    },
    /// An argument the payload derives differs from the signed one. For
    /// a write this is the integrity failure: the bytes are not the
    /// bytes that were authorized.
    #[error("payload's '{parameter}' is not the one the invocation signed")]
    Mismatch {
        /// The parameter that disagreed.
        parameter: String,
    },
}

/// Check that `capability` is the payload `signed_args` spoke about.
///
/// `signed_command` and `signed_subject` come from the *verified*
/// invocation — calling this with an unverified chain checks that a
/// payload matches an assertion nobody vouched for, which is not a
/// security property. Verify first.
///
/// Extra arguments in the invocation beyond what the payload derives are
/// a mismatch, not a courtesy: they were signed, so something authorized
/// them, and performing an effect that ignores them would be performing
/// something other than what was authorized. Extra arguments the
/// *payload* derives are likewise refused by the count check.
pub fn bind<Fx>(
    signed_command: &[String],
    signed_subject: &Did,
    signed_args: &Args,
    capability: &Capability<Fx>,
) -> Result<(), BindError>
where
    Fx: Effect,
    Fx::Of: Constraint,
    Capability<Fx>: Ability,
{
    let signed = format!("/{}", signed_command.join("/"));
    let claimed = capability.ability().to_string();
    if signed != claimed {
        return Err(BindError::Command { signed, claimed });
    }

    let subject = capability.subject();
    if subject != signed_subject {
        return Err(BindError::Subject {
            signed: Box::new(signed_subject.clone()),
            claimed: Box::new(subject.clone()),
        });
    }

    // Re-derive what this payload *would* have been signed as, then
    // require the two maps to be equal. Equality both ways: a missing
    // parameter means the payload is narrower than what was authorized,
    // and an extra one means it is wider.
    let derived = parameters_to_args(parameters(capability));
    for (parameter, signed_value) in signed_args {
        match derived.get(parameter) {
            None => {
                return Err(BindError::Missing {
                    parameter: parameter.clone(),
                });
            }
            Some(value) if value != signed_value => {
                return Err(BindError::Mismatch {
                    parameter: parameter.clone(),
                });
            }
            Some(_) => {}
        }
    }
    if let Some(parameter) = derived.keys().find(|key| !signed_args.contains_key(*key)) {
        return Err(BindError::Mismatch {
            parameter: parameter.clone(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::{Subject, did};
    use dialog_effects::Use;
    use dialog_effects::archive::{Archive, Buffer, Catalog, Get, Put};

    fn put(bytes: &[u8]) -> Capability<Put> {
        Subject::from(did!("key:zSpace"))
            .attenuate(Use)
            .attenuate(Archive)
            .attenuate(Catalog::new("blocks"))
            .invoke(Put::new(Buffer::from(bytes.to_vec())))
    }

    fn args_of<Fx>(capability: &Capability<Fx>) -> Args
    where
        Fx: Effect,
        Fx::Of: Constraint,
        Capability<Fx>: Ability,
    {
        parameters_to_args(parameters(capability))
    }

    #[dialog_common::test]
    fn the_payload_that_was_signed_binds() {
        let capability = put(b"the authorized block");
        let args = args_of(&capability);
        let command: Vec<String> = capability
            .ability()
            .trim_start_matches('/')
            .split('/')
            .map(str::to_string)
            .collect();

        bind(&command, &did!("key:zSpace"), &args, &capability).unwrap();
    }

    /// The reason this module exists: a valid invocation plus different
    /// bytes must not perform.
    #[dialog_common::test]
    fn a_substituted_block_does_not_bind() {
        let authorized = put(b"the authorized block");
        let args = args_of(&authorized);
        let command: Vec<String> = authorized
            .ability()
            .trim_start_matches('/')
            .split('/')
            .map(str::to_string)
            .collect();

        // Same invocation, same command, same subject — different bytes.
        let substituted = put(b"something else entirely");

        let refusal = bind(&command, &did!("key:zSpace"), &args, &substituted)
            .expect_err("a block the invocation did not sign must not bind");
        assert!(
            matches!(refusal, BindError::Mismatch { .. }),
            "expected a mismatch, got {refusal:?}"
        );
    }

    #[dialog_common::test]
    fn another_subjects_invocation_does_not_bind() {
        let capability = put(b"block");
        let args = args_of(&capability);
        let command: Vec<String> = capability
            .ability()
            .trim_start_matches('/')
            .split('/')
            .map(str::to_string)
            .collect();

        let refusal = bind(&command, &did!("key:zOther"), &args, &capability)
            .expect_err("an invocation for another subject must not bind");
        assert!(matches!(refusal, BindError::Subject { .. }));
    }

    /// A read invocation must not authorize a write, even though both
    /// live under the same catalog.
    #[dialog_common::test]
    fn a_read_invocation_does_not_bind_a_write() {
        let read = Subject::from(did!("key:zSpace"))
            .attenuate(Use)
            .attenuate(Archive)
            .attenuate(Catalog::new("blocks"))
            .invoke(Get::new(dialog_effects::archive::Blake3Hash::from(
                [3u8; 32],
            )));
        let read_command: Vec<String> = read
            .ability()
            .trim_start_matches('/')
            .split('/')
            .map(str::to_string)
            .collect();

        let write = put(b"block");
        let refusal = bind(&read_command, &did!("key:zSpace"), &args_of(&read), &write)
            .expect_err("a get invocation must not bind a put payload");
        assert!(matches!(refusal, BindError::Command { .. }));
    }
}
