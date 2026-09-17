//! The site against the service, on native and on wasm: every effect
//! the service performs, the refusals it answers with, and the layer's
//! own checks on what a container carries.

#![cfg(feature = "helpers")]

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use dialog_capability::access::{Authorization as _, AuthorizeError, TimeRange};
use dialog_capability::{Ability, Capability, Effect, ForkInvocation, Provider, Subject};
use dialog_common::{Blake3Hash, Buffer};
use dialog_credentials::{Ed25519Signer, Signer};
use dialog_effects::archive::ArchiveError;
use dialog_effects::archive::prelude::*;
use dialog_effects::blob::prelude::*;
use dialog_effects::memory::prelude::*;
use dialog_effects::memory::{MemoryError, Version};
use dialog_remote_ucan::helpers::{MemoryStore, UcanServiceAddress};
use dialog_remote_ucan::{Access, Answer, Request, UcanAddress, UcanAuthorization, UcanSite};
use dialog_ucan::Scope;
use dialog_ucan_core::Container;
use dialog_varsig::Principal as _;

fn now_s() -> u64 {
    dialog_common::time::now()
        .duration_since(dialog_common::time::UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or_default()
}

/// The invocation `signer` mints for `capability` on its own authority:
/// no delegation, so it holds only when the signer is the subject.
async fn issued<Fx>(signer: &Ed25519Signer, capability: &Capability<Fx>) -> UcanAuthorization
where
    Fx: Effect + Clone,
    Capability<Fx>: Ability,
{
    let at = now_s();
    let minting = dialog_ucan::UcanAuthorization {
        chain: None,
        signer: Signer::from(signer.clone()),
        scope: Scope::invoke(capability),
        duration: TimeRange {
            not_before: Some(at),
            expiration: Some(at + 60),
        },
        meta: None,
    };
    UcanAuthorization::from(minting.invoke().await.expect("the invocation mints"))
}

/// Perform `capability` at `service`, signed by `signer`.
async fn perform<Fx>(
    service: &UcanServiceAddress,
    signer: &Ed25519Signer,
    capability: Capability<Fx>,
) -> Fx::Output
where
    Fx: Effect + Clone,
    Capability<Fx>: Ability,
    UcanSite: Provider<ForkInvocation<UcanSite, Fx>>,
{
    let authorization = issued(signer, &capability).await;
    UcanSite::default()
        .execute(ForkInvocation::new(
            capability,
            UcanAddress::new(&service.endpoint),
            authorization,
        ))
        .await
}

async fn owner() -> (Ed25519Signer, Subject) {
    let signer = Ed25519Signer::generate().await.expect("a signer");
    let subject = Subject::from(signer.did());
    (signer, subject)
}

#[dialog_common::test]
async fn it_stores_a_block_and_serves_it_back(service: UcanServiceAddress) -> anyhow::Result<()> {
    let (signer, subject) = owner().await;
    let content = b"a block, proved and stored in one request".to_vec();
    let digest = Blake3Hash::hash(&content);

    perform(
        &service,
        &signer,
        subject
            .clone()
            .archive()
            .catalog("index")
            .put(Buffer::from(content.clone())),
    )
    .await?;
    let served = perform(
        &service,
        &signer,
        subject.archive().catalog("index").get(digest),
    )
    .await?;

    assert_eq!(served, Some(content));
    Ok(())
}

#[dialog_common::test]
async fn it_answers_none_for_a_block_it_does_not_hold(
    service: UcanServiceAddress,
) -> anyhow::Result<()> {
    let (signer, subject) = owner().await;
    let served = perform(
        &service,
        &signer,
        subject
            .archive()
            .catalog("index")
            .get(Blake3Hash::hash(b"never stored")),
    )
    .await?;
    assert_eq!(served, None);
    Ok(())
}

#[dialog_common::test]
async fn it_keeps_catalogs_apart(service: UcanServiceAddress) -> anyhow::Result<()> {
    let (signer, subject) = owner().await;
    let content = b"filed under one catalog".to_vec();
    let digest = Blake3Hash::hash(&content);
    perform(
        &service,
        &signer,
        subject
            .clone()
            .archive()
            .catalog("index")
            .put(Buffer::from(content)),
    )
    .await?;
    let elsewhere = perform(
        &service,
        &signer,
        subject.archive().catalog("blob").get(digest),
    )
    .await?;
    assert_eq!(elsewhere, None, "a block is served only from its catalog");
    Ok(())
}

#[dialog_common::test]
async fn it_publishes_resolves_and_updates_a_cell(
    service: UcanServiceAddress,
) -> anyhow::Result<()> {
    let (signer, subject) = owner().await;
    let cell = || subject.clone().memory().space("sync").cell("head");

    let first = perform(&service, &signer, cell().publish(b"one".to_vec(), None)).await?;
    let resolved = perform(&service, &signer, cell().resolve())
        .await?
        .expect("the cell was published");
    assert_eq!(resolved.content, b"one");
    assert_eq!(resolved.version, first);

    let second = perform(
        &service,
        &signer,
        cell().publish(b"two".to_vec(), Some(first.clone())),
    )
    .await?;
    assert_ne!(second, first, "an update mints a new version");
    let resolved = perform(&service, &signer, cell().resolve())
        .await?
        .expect("the cell still stands");
    assert_eq!(resolved.content, b"two");
    assert_eq!(resolved.version, second);
    Ok(())
}

#[dialog_common::test]
async fn it_refuses_a_publish_against_a_stale_version(
    service: UcanServiceAddress,
) -> anyhow::Result<()> {
    let (signer, subject) = owner().await;
    let cell = || subject.clone().memory().space("sync").cell("head");

    let first = perform(&service, &signer, cell().publish(b"one".to_vec(), None)).await?;
    perform(
        &service,
        &signer,
        cell().publish(b"two".to_vec(), Some(first.clone())),
    )
    .await?;

    let stale = perform(
        &service,
        &signer,
        cell().publish(b"three".to_vec(), Some(first)),
    )
    .await;
    assert!(
        matches!(stale, Err(MemoryError::VersionMismatch { .. })),
        "a stale precondition is a version mismatch, got {stale:?}"
    );
    let resolved = perform(&service, &signer, cell().resolve())
        .await?
        .expect("the cell stands");
    assert_eq!(resolved.content, b"two", "the stale write changed nothing");
    Ok(())
}

#[dialog_common::test]
async fn it_refuses_to_create_over_a_cell_that_exists(
    service: UcanServiceAddress,
) -> anyhow::Result<()> {
    let (signer, subject) = owner().await;
    let cell = || subject.clone().memory().space("sync").cell("head");
    perform(&service, &signer, cell().publish(b"one".to_vec(), None)).await?;
    let again = perform(&service, &signer, cell().publish(b"other".to_vec(), None)).await;
    assert!(matches!(again, Err(MemoryError::VersionMismatch { .. })));
    Ok(())
}

#[dialog_common::test]
async fn it_retracts_a_cell_at_its_version(service: UcanServiceAddress) -> anyhow::Result<()> {
    let (signer, subject) = owner().await;
    let cell = || subject.clone().memory().space("sync").cell("head");
    let version = perform(&service, &signer, cell().publish(b"one".to_vec(), None)).await?;

    let wrong = perform(&service, &signer, cell().retract(Version::from("v0"))).await;
    assert!(matches!(wrong, Err(MemoryError::VersionMismatch { .. })));

    perform(&service, &signer, cell().retract(version)).await?;
    let resolved = perform(&service, &signer, cell().resolve()).await?;
    assert_eq!(resolved, None, "a retracted cell resolves to nothing");
    Ok(())
}

#[dialog_common::test]
async fn it_refuses_an_invocation_its_subject_did_not_issue(
    service: UcanServiceAddress,
) -> anyhow::Result<()> {
    let (_, subject) = owner().await;
    let stranger = Ed25519Signer::generate().await?;
    let refused = perform(
        &service,
        &stranger,
        subject
            .clone()
            .archive()
            .catalog("index")
            .get(Blake3Hash::hash(b"anything")),
    )
    .await;
    // The refusal travels as itself and names the two parties: the
    // subject whose authority was claimed and the stranger who claimed
    // it.
    match refused {
        Err(ArchiveError::Authorization(
            AuthorizeError::InvalidAudience {
                claimed,
                authorized,
            }
            | AuthorizeError::UnprovenSubject {
                claimed,
                authorized,
            },
        )) => {
            let named = [claimed, authorized];
            assert!(named.contains(&stranger.did()), "the stranger is named");
            assert!(named.contains(subject.did()), "the subject is named");
            Ok(())
        }
        other => anyhow::bail!("expected a refusal naming both parties, got {other:?}"),
    }
}

/// The layer itself, driven directly: what it does with a request that
/// asks for no outcome, an operation it does not perform, and a
/// container whose payload does not match what the invocation bound.
mod layer {
    use super::*;

    async fn container_for<Fx>(
        signer: &Ed25519Signer,
        capability: &Capability<Fx>,
        payload: Option<&[u8]>,
    ) -> Vec<u8>
    where
        Fx: Effect + Clone,
        Capability<Fx>: Ability,
    {
        let authorization = issued(signer, capability).await;
        let mut container = Container::from(authorization.invocation().chain());
        if let Some(payload) = payload {
            container = container.with_payload(payload);
        }
        container.into_bytes().expect("the container encodes")
    }

    #[dialog_common::test]
    async fn it_leaves_a_request_for_a_permit_to_the_embedder() {
        let (signer, subject) = owner().await;
        let capability = subject
            .archive()
            .catalog("index")
            .get(Blake3Hash::hash(b"x"));
        let body = container_for(&signer, &capability, None).await;
        let access = Access::new(MemoryStore::default());
        assert!(matches!(
            access.handle(Request::new(&body)).await,
            Answer::Unsupported
        ));
        assert!(matches!(
            access
                .handle(Request::new(&body).accept("application/cbor"))
                .await,
            Answer::Unsupported
        ));
    }

    #[dialog_common::test]
    async fn it_leaves_a_blob_stream_to_the_embedder() {
        let (signer, subject) = owner().await;
        let capability = subject.archive().blob().read(Blake3Hash::hash(b"x"));
        let body = container_for(&signer, &capability, None).await;
        let access = Access::new(MemoryStore::default());
        assert!(matches!(
            access
                .handle(Request::new(&body).accept("application/octet-stream"))
                .await,
            Answer::Unsupported
        ));
    }

    #[dialog_common::test]
    async fn it_requires_the_payload_a_write_stores() {
        let (signer, subject) = owner().await;
        let capability = subject
            .archive()
            .catalog("index")
            .put(Buffer::from(b"content".to_vec()));
        let body = container_for(&signer, &capability, None).await;
        let access = Access::new(MemoryStore::default());
        match access
            .handle(Request::new(&body).accept("application/octet-stream"))
            .await
        {
            Answer::Performed(response) => assert_eq!(response.status, 411),
            other => panic!("expected a 411, got {other:?}"),
        }
        assert_eq!(access.provider().blocks(), 0);
    }

    #[dialog_common::test]
    async fn it_refuses_a_payload_that_is_not_what_the_invocation_bound() {
        let (signer, subject) = owner().await;
        let capability = subject
            .archive()
            .catalog("index")
            .put(Buffer::from(b"content".to_vec()));
        let body = container_for(&signer, &capability, Some(b"something else")).await;
        let access = Access::new(MemoryStore::default());
        match access
            .handle(Request::new(&body).accept("application/octet-stream"))
            .await
        {
            Answer::Performed(response) => assert_eq!(response.status, 400),
            other => panic!("expected a 400, got {other:?}"),
        }
        assert_eq!(access.provider().blocks(), 0, "nothing was stored");
    }

    #[dialog_common::test]
    async fn it_refuses_a_container_it_cannot_read() {
        let access = Access::new(MemoryStore::default());
        match access
            .handle(Request::new(b"not a container").accept("application/octet-stream"))
            .await
        {
            Answer::Refused(refusal) => {
                assert_eq!(refusal.status(), 400);
                assert!(matches!(refusal.reason(), AuthorizeError::Malformed { .. }));
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[dialog_common::test]
    async fn it_stores_a_verified_write_and_serves_it() {
        let (signer, subject) = owner().await;
        let content = b"content".to_vec();
        let put = subject
            .clone()
            .archive()
            .catalog("index")
            .put(Buffer::from(content.clone()));
        let access = Access::new(MemoryStore::default());
        let body = container_for(&signer, &put, Some(&content)).await;
        match access
            .handle(Request::new(&body).accept("application/octet-stream"))
            .await
        {
            Answer::Performed(response) => assert!(response.is_success(), "{response:?}"),
            other => panic!("expected success, got {other:?}"),
        }
        let get = subject
            .archive()
            .catalog("index")
            .get(Blake3Hash::hash(&content));
        let body = container_for(&signer, &get, None).await;
        match access
            .handle(Request::new(&body).accept("application/octet-stream"))
            .await
        {
            Answer::Performed(response) => {
                assert_eq!(response.status, 200);
                assert_eq!(response.body, content);
            }
            other => panic!("expected the block, got {other:?}"),
        }
    }
}
