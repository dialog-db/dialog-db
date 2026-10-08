//! The client side of `/ucan/claim`: fetch the ticket a subject holds for
//! a holder (see [`dialog_effects::ticket`]).
//!
//! The invocation is the holder acting on itself, so it needs no proof:
//! whoever can sign as the holder may ask, and what they get back is the
//! holder's ticket alone. The ticket is a container of the delegation
//! chain the subject issued; it carries its own signatures, so nothing
//! the service answers has to be taken on trust.

use std::collections::HashMap;

use dialog_capability::Did;
use dialog_effects::ticket;
use dialog_remote_s3::S3Error;
use dialog_remote_ucan_s3::{UcanAuthorization, UcanInvocation};
use dialog_ucan_core::issuer::Issuer;
use dialog_ucan_core::promise::Promised;
use dialog_ucan_core::{InvocationBuilder, InvocationChain};
use dialog_varsig::AnySignature;

use crate::address::{Exchange, UcanAddress};
use crate::direct;

/// Fetch the ticket `subject` holds for `holder` from the access service
/// at `address`.
///
/// Answers the ticket's bytes (a UCAN container), or `None` when
/// `subject` holds no ticket for `holder`.
///
/// # Errors
///
/// An [`S3Error`] when the invocation cannot be signed, the service
/// cannot be reached, or it refuses the claim.
pub async fn claim<I>(
    address: &UcanAddress,
    holder: I,
    subject: &Did,
) -> Result<Option<Vec<u8>>, S3Error>
where
    I: Issuer<AnySignature> + 'static,
{
    let holder_did = holder.did();
    let arguments = [(
        ticket::SUBJECT.to_string(),
        Promised::String(subject.to_string()),
    )]
    .into_iter()
    .collect();
    let invocation = InvocationBuilder::new()
        .issuer(holder)
        .audience(&holder_did)
        .subject(&holder_did)
        .command(
            ticket::CLAIM
                .iter()
                .map(|segment| segment.to_string())
                .collect(),
        )
        .arguments(arguments)
        .proofs(vec![])
        .try_build()
        .await
        .map_err(|error| S3Error::Serialization(format!("{error:?}")))?;
    let authorization = UcanAuthorization::from(UcanInvocation {
        chain: Box::new(InvocationChain::new(invocation, HashMap::new())),
        subject: holder_did,
        ability: format!("/{}", ticket::CLAIM.join("/")),
    });

    match address.exchange() {
        Exchange::Direct => {
            let answer = direct::invoke(address, &authorization, None).await?;
            match answer {
                answer if answer.is_success() => Ok(Some(answer.bytes().await?)),
                answer if answer.status == 404 => Ok(None),
                answer if answer.is_refusal() => Err(answer.refusal().await),
                answer => Err(S3Error::Transport(format!(
                    "Failed to claim a ticket: {}",
                    answer.status
                ))),
            }
        }
        Exchange::Permit => {
            let permit = authorization.redeem(&address.permits()).await?;
            let response = permit.send().await?;
            match response.status().as_u16() {
                200..300 => Ok(Some(response.bytes().await?.to_vec())),
                404 => Ok(None),
                status => Err(S3Error::Transport(format!(
                    "Failed to claim a ticket: {status}"
                ))),
            }
        }
    }
}
