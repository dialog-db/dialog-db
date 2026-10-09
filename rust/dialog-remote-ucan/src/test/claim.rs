//! `/ucan/claim` against the service: a holder fetches the ticket a
//! subject keeps for it, and nothing else.

use super::*;
use dialog_effects::memory::prelude::PublishCellExt as _;
use dialog_effects::ticket;
use dialog_ucan_core::promise::Promised;
use dialog_ucan_core::subject::Subject as DelegatedSubject;
use dialog_ucan_core::{DelegationBuilder, DelegationChain, InvocationBuilder, InvocationChain};
use std::collections::{BTreeMap, HashMap};

/// The delegation `owner` issues `holder`, as the ticket container a
/// subject keeps for it.
async fn ticket_for(owner: &Ed25519Signer, holder: &Ed25519Signer) -> anyhow::Result<Vec<u8>> {
    let delegation = DelegationBuilder::new()
        .issuer(Signer::from(owner.clone()))
        .audience(holder)
        .subject(DelegatedSubject::Specific(owner.did()))
        .command(vec!["use".to_string(), "get".to_string()])
        .try_build()
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    Ok(DelegationChain::new(delegation).to_bytes()?)
}

/// Leave `holder`'s ticket in `owner`'s memory, as the owner.
async fn issue(
    service: &UcanServiceAddress,
    owner: &Ed25519Signer,
    holder: &Ed25519Signer,
) -> anyhow::Result<Vec<u8>> {
    let content = ticket_for(owner, holder).await?;
    perform(
        service,
        owner,
        ticket::writer(&owner.did(), &holder.did()).publish(content.clone(), None),
    )
    .await?;
    Ok(content)
}

#[dialog_common::test]
async fn it_answers_the_holder_the_ticket_the_subject_keeps_for_it(
    service: UcanServiceAddress,
) -> anyhow::Result<()> {
    let (owner, subject) = owner().await;
    let holder = Ed25519Signer::generate().await?;
    let issued = issue(&service, &owner, &holder).await?;

    let claimed = crate::claim(
        &UcanAddress::new(&service.endpoint),
        Signer::from(holder.clone()),
        subject.did(),
    )
    .await?
    .expect("the subject keeps a ticket for the holder");

    assert_eq!(claimed, issued);
    let chain = DelegationChain::try_from(claimed.as_slice())?;
    assert_eq!(chain.audience(), &holder.did());
    assert_eq!(chain.subject(), Some(subject.did()));
    Ok(())
}

#[dialog_common::test]
async fn it_answers_another_principal_only_its_own_ticket(
    service: UcanServiceAddress,
) -> anyhow::Result<()> {
    let (owner, subject) = owner().await;
    let holder = Ed25519Signer::generate().await?;
    issue(&service, &owner, &holder).await?;

    // A stranger claiming against the same subject reaches the cell
    // named by its own DID, which holds nothing: the holder's ticket is
    // not in reach of a claim the holder did not sign.
    let stranger = Ed25519Signer::generate().await?;
    let claimed = crate::claim(
        &UcanAddress::new(&service.endpoint),
        Signer::from(stranger),
        subject.did(),
    )
    .await?;
    assert_eq!(claimed, None);
    Ok(())
}

#[dialog_common::test]
async fn it_answers_none_where_the_subject_keeps_no_ticket(
    service: UcanServiceAddress,
) -> anyhow::Result<()> {
    let (_, subject) = owner().await;
    let holder = Ed25519Signer::generate().await?;
    let claimed = crate::claim(
        &UcanAddress::new(&service.endpoint),
        Signer::from(holder),
        subject.did(),
    )
    .await?;
    assert_eq!(claimed, None);
    Ok(())
}

/// A `/ucan/claim` signed by `issuer` acting as `holder`, with no proof,
/// naming `subject` (or nothing, when `subject` is `None`).
async fn claim_credential(
    issuer: &Ed25519Signer,
    holder: &dialog_varsig::Did,
    subject: Option<&dialog_varsig::Did>,
) -> anyhow::Result<String> {
    let arguments: BTreeMap<String, Promised> = subject
        .map(|subject| {
            (
                ticket::SUBJECT.to_string(),
                Promised::String(subject.to_string()),
            )
        })
        .into_iter()
        .collect();
    let invocation = InvocationBuilder::new()
        .issuer(Signer::from(issuer.clone()))
        .audience(holder)
        .subject(holder)
        .command(ticket::CLAIM.iter().map(|s| s.to_string()).collect())
        .arguments(arguments)
        .proofs(vec![])
        .try_build()
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    let chain = InvocationChain::new(invocation, HashMap::new());
    Ok(credential(Container::from(&chain))?)
}

/// The layer, driven directly: a claim for a holder the issuer is not
/// is refused before any ticket is read, and a claim that names no
/// subject is malformed.
mod layer {
    use super::*;
    use crate::helpers::MemoryStore;

    #[dialog_common::test]
    async fn it_refuses_a_claim_on_behalf_of_a_holder_it_cannot_sign_as() -> anyhow::Result<()> {
        let (owner, subject) = owner().await;
        let holder = Ed25519Signer::generate().await?;
        let access = Access::new(MemoryStore::default());
        let ticket = ticket_for(&owner, &holder).await?;
        Provider::<dialog_effects::memory::Publish>::execute(
            access.provider(),
            ticket::writer(subject.did(), &holder.did()).publish(ticket, None),
        )
        .await?;

        let stranger = Ed25519Signer::generate().await?;
        let value = claim_credential(&stranger, &holder.did(), Some(subject.did())).await?;
        match access.handle(Request::new(Some(&value))).await {
            Answer::Refused(refusal) => assert!(
                matches!(
                    refusal.reason(),
                    AuthorizeError::UnprovenSubject { .. } | AuthorizeError::InvalidAudience { .. }
                ),
                "expected the subject to be unproven, got {:?}",
                refusal.reason()
            ),
            other => anyhow::bail!("expected a refusal, got {other:?}"),
        }
        Ok(())
    }

    #[dialog_common::test]
    async fn it_refuses_a_claim_that_names_no_subject() -> anyhow::Result<()> {
        let holder = Ed25519Signer::generate().await?;
        let access = Access::new(MemoryStore::default());
        let value = claim_credential(&holder, &holder.did(), None).await?;
        match access.handle(Request::new(Some(&value))).await {
            Answer::Refused(refusal) => assert!(
                matches!(refusal.reason(), AuthorizeError::Malformed { .. }),
                "expected a malformed claim, got {:?}",
                refusal.reason()
            ),
            other => anyhow::bail!("expected a refusal, got {other:?}"),
        }
        Ok(())
    }
}
