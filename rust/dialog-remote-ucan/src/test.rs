//! The site against the service, on native and on wasm: every effect
//! the service performs, the refusals it answers with, and the layer's
//! own checks on what a request carries. The service is provisioned
//! natively; on wasm the client half runs in a browser against it.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::helpers::{MemoryStore, UcanServiceAddress};
use crate::{
    Access, Answer, Content, Payload, Request, UcanAddress, UcanAuthorization, UcanSite, credential,
};
use dialog_capability::access::{Authorization as _, AuthorizeError, TimeRange};
use dialog_capability::{Ability, Capability, Effect, ForkInvocation, Provider, Subject};
use dialog_common::{Blake3Hash, Buffer};
use dialog_credentials::{Ed25519Signer, Signer};
use dialog_effects::archive::ArchiveError;
use dialog_effects::archive::prelude::*;
use dialog_effects::blob::BlobError;
use dialog_effects::memory::prelude::*;
use dialog_effects::memory::{MemoryError, Version};
use dialog_ucan::Scope;
use dialog_ucan_core::{Container, Tag};
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

/// Read a blob to its end.
async fn drain(mut reader: dialog_effects::blob::BlobReader) -> anyhow::Result<(Vec<u8>, usize)> {
    let mut bytes = Vec::new();
    let mut chunks = 0;
    while let Some(chunk) = reader.next().await? {
        bytes.extend_from_slice(&chunk);
        chunks += 1;
    }
    Ok((bytes, chunks))
}

/// A blob big enough to travel as several chunks.
fn blob() -> Vec<u8> {
    (0..20_000u32).flat_map(|i| i.to_le_bytes()).collect()
}

#[dialog_common::test]
async fn it_imports_a_blob_and_streams_it_back(service: UcanServiceAddress) -> anyhow::Result<()> {
    let (signer, subject) = owner().await;
    let content = blob();
    let digest = Blake3Hash::hash(&content);

    let mut sink = perform(
        &service,
        &signer,
        subject
            .clone()
            .archive()
            .blob()
            .import(digest.clone(), content.len() as u64),
    )
    .await?;
    for part in content.chunks(3_000) {
        sink.write_all(part).await?;
    }
    assert_eq!(sink.finish().await?, digest);

    let reader = perform(&service, &signer, subject.archive().blob().read(digest)).await?;
    let (served, chunks) = drain(reader).await?;
    assert_eq!(served, content);
    assert!(chunks >= 1, "the blob came back as {chunks} chunks");
    Ok(())
}

#[dialog_common::test]
async fn it_reads_a_range_of_a_blob(service: UcanServiceAddress) -> anyhow::Result<()> {
    let (signer, subject) = owner().await;
    let content = blob();
    let digest = Blake3Hash::hash(&content);
    let mut sink = perform(
        &service,
        &signer,
        subject
            .clone()
            .archive()
            .blob()
            .import(digest.clone(), content.len() as u64),
    )
    .await?;
    sink.write_all(&content).await?;
    sink.finish().await?;

    let ranged = subject
        .clone()
        .archive()
        .blob()
        .invoke(dialog_effects::blob::Read::range(
            digest.clone(),
            10_000,
            Some(5_000),
        ));
    let reader = perform(&service, &signer, ranged).await?;
    let (served, _) = drain(reader).await?;
    assert_eq!(served, &content[10_000..15_000]);

    let tail = subject
        .archive()
        .blob()
        .invoke(dialog_effects::blob::Read::range(digest, 70_000, None));
    let reader = perform(&service, &signer, tail).await?;
    let (served, _) = drain(reader).await?;
    assert_eq!(served, &content[70_000..]);
    Ok(())
}

#[dialog_common::test]
async fn it_answers_not_found_for_a_blob_it_does_not_hold(
    service: UcanServiceAddress,
) -> anyhow::Result<()> {
    let (signer, subject) = owner().await;
    let missing = perform(
        &service,
        &signer,
        subject
            .archive()
            .blob()
            .read(Blake3Hash::hash(b"never imported")),
    )
    .await;
    let missing = missing.err();
    assert!(
        matches!(missing, Some(BlobError::NotFound(_))),
        "a blob the service does not hold is not found, got {missing:?}"
    );
    Ok(())
}

#[dialog_common::test]
async fn it_refuses_an_import_whose_bytes_do_not_hash_to_the_digest(
    service: UcanServiceAddress,
) -> anyhow::Result<()> {
    let (signer, subject) = owner().await;
    let content = b"what the invocation declares".to_vec();
    let mut sink = perform(
        &service,
        &signer,
        subject
            .archive()
            .blob()
            .import(Blake3Hash::hash(&content), content.len() as u64),
    )
    .await?;
    sink.write_all(b"what is actually written.....").await?;
    let finished = sink.finish().await;
    assert!(
        matches!(finished, Err(BlobError::DigestMismatch { .. })),
        "got {finished:?}"
    );
    Ok(())
}

/// The request a fork sends names its command and subject in the URL,
/// for whoever reads a network log, and nothing else: the arguments
/// would make every URL distinct and cost a preflight each.
#[dialog_common::test]
async fn it_labels_the_request_with_the_command_and_the_subject() {
    let (signer, subject) = owner().await;
    let capability = subject
        .clone()
        .archive()
        .catalog("index")
        .put(Buffer::from(b"content".to_vec()));
    let authorization = issued(&signer, &capability).await;
    let chain = authorization.invocation().chain();
    let url = crate::direct::labeled("https://access.example/ucan/", chain);
    assert_eq!(
        url,
        format!(
            "https://access.example/ucan/?cmd=/use/put/archive/block&sub={}",
            subject.did()
        )
    );
    let url = crate::direct::labeled("https://access.example/ucan/?cache=bypass", chain);
    assert!(
        url.starts_with("https://access.example/ucan/?cache=bypass&cmd="),
        "{url}"
    );
}

/// The layer itself, driven directly: what it does with a request that
/// carries no invocation, a credential it cannot read, an operation it
/// does not perform, and a body that is not what the invocation bound.
mod layer {
    use super::*;

    async fn credential_for<Fx>(signer: &Ed25519Signer, capability: &Capability<Fx>) -> String
    where
        Fx: Effect + Clone,
        Capability<Fx>: Ability,
    {
        let authorization = issued(signer, capability).await;
        credential(Container::from(authorization.invocation().chain())).expect("encodes")
    }

    async fn performed(answer: Answer) -> (u16, Vec<u8>) {
        match answer {
            Answer::Performed(response) => {
                let status = response.status;
                (status, response.body.collect().await.expect("readable"))
            }
            other => panic!("expected the operation's outcome, got {other:?}"),
        }
    }

    #[dialog_common::test]
    async fn it_leaves_a_request_without_an_invocation_to_the_embedder() {
        let access = Access::new(MemoryStore::default());
        assert!(matches!(
            access.handle(Request::new(None)).await,
            Answer::Unsupported
        ));
        assert!(matches!(
            access.handle(Request::new(Some("Bearer abc"))).await,
            Answer::Unsupported
        ));
    }

    #[dialog_common::test]
    async fn it_refuses_a_credential_it_cannot_read() {
        let access = Access::new(MemoryStore::default());
        for value in ["UCAN ", "UCAN Cnot-a-container", "UCAN Zabc"] {
            match access.handle(Request::new(Some(value))).await {
                Answer::Refused(refusal) => {
                    assert_eq!(refusal.status(), 400, "{value}");
                    assert!(
                        matches!(refusal.reason(), AuthorizeError::Malformed { .. }),
                        "{value}"
                    );
                }
                other => panic!("expected a refusal to {value:?}, got {other:?}"),
            }
        }
    }

    #[dialog_common::test]
    async fn it_reads_the_invocation_in_either_text_form() {
        let (signer, subject) = owner().await;
        let content = b"content".to_vec();
        let put = subject
            .archive()
            .catalog("index")
            .put(Buffer::from(content.clone()));
        let authorization = issued(&signer, &put).await;
        let container = Container::from(authorization.invocation().chain());
        let access = Access::new(MemoryStore::default());
        for tag in [Tag::Base64Url, Tag::Base64UrlGzip] {
            let text = String::from_utf8(container.clone().encode(tag).unwrap()).unwrap();
            let value = format!("UCAN {text}");
            let request = Request::new(Some(&value)).payload(content.clone());
            let (status, _) = performed(access.handle(request).await).await;
            assert_eq!(status, 200, "{tag:?}");
        }
        assert_eq!(access.provider().blocks(), 1);
    }

    #[dialog_common::test]
    async fn it_requires_the_body_a_write_stores() {
        let (signer, subject) = owner().await;
        let capability = subject
            .archive()
            .catalog("index")
            .put(Buffer::from(b"content".to_vec()));
        let value = credential_for(&signer, &capability).await;
        let access = Access::new(MemoryStore::default());
        let (status, _) = performed(access.handle(Request::new(Some(&value))).await).await;
        assert_eq!(status, 411);
        assert_eq!(access.provider().blocks(), 0);
    }

    #[dialog_common::test]
    async fn it_refuses_a_body_that_is_not_what_the_invocation_bound() {
        let (signer, subject) = owner().await;
        let capability = subject
            .archive()
            .catalog("index")
            .put(Buffer::from(b"content".to_vec()));
        let value = credential_for(&signer, &capability).await;
        let access = Access::new(MemoryStore::default());
        let request = Request::new(Some(&value)).payload(b"something else".to_vec());
        let (status, body) = performed(access.handle(request).await).await;
        assert_eq!(status, 400);
        assert!(
            String::from_utf8_lossy(&body).contains("ChecksumMismatch"),
            "{body:?}"
        );
        assert_eq!(access.provider().blocks(), 0, "nothing was stored");
    }

    #[dialog_common::test]
    async fn it_refuses_an_import_shorter_than_declared() {
        let (signer, subject) = owner().await;
        let content = b"a blob of some length".to_vec();
        let capability = subject
            .archive()
            .blob()
            .import(Blake3Hash::hash(&content), content.len() as u64);
        let value = credential_for(&signer, &capability).await;
        let access = Access::new(MemoryStore::default());
        let request = Request::new(Some(&value)).payload(content[..5].to_vec());
        let (status, body) = performed(access.handle(request).await).await;
        assert_eq!(status, 400);
        assert!(
            String::from_utf8_lossy(&body).contains("SizeMismatch"),
            "{body:?}"
        );
        assert_eq!(access.provider().blobs(), 0, "nothing was stored");
    }

    #[dialog_common::test]
    async fn it_refuses_an_import_that_does_not_hash_to_its_digest() {
        let (signer, subject) = owner().await;
        let content = b"a blob of some length".to_vec();
        let capability = subject
            .archive()
            .blob()
            .import(Blake3Hash::hash(&content), content.len() as u64);
        let value = credential_for(&signer, &capability).await;
        let access = Access::new(MemoryStore::default());
        let other = b"a blob of same length".to_vec();
        let request = Request::new(Some(&value)).payload(other);
        let (status, body) = performed(access.handle(request).await).await;
        assert_eq!(status, 400);
        assert!(
            String::from_utf8_lossy(&body).contains("DigestMismatch"),
            "{body:?}"
        );
        assert_eq!(access.provider().blobs(), 0, "nothing was stored");
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
        let value = credential_for(&signer, &put).await;
        let request = Request::new(Some(&value)).payload(content.clone());
        let (status, _) = performed(access.handle(request).await).await;
        assert_eq!(status, 200);

        let get = subject
            .archive()
            .catalog("index")
            .get(Blake3Hash::hash(&content));
        let value = credential_for(&signer, &get).await;
        let (status, body) = performed(access.handle(Request::new(Some(&value))).await).await;
        assert_eq!(status, 200);
        assert_eq!(body, content);
    }

    /// A blob's bytes reach the provider as they arrive: the layer
    /// feeds a streamed body into the sink chunk by chunk, and answers
    /// a read with a stream.
    #[dialog_common::test]
    async fn it_streams_a_blob_in_and_out() {
        let (signer, subject) = owner().await;
        let content = blob();
        let digest = Blake3Hash::hash(&content);
        let import = subject
            .clone()
            .archive()
            .blob()
            .import(digest.clone(), content.len() as u64);
        let access = Access::new(MemoryStore::default());
        let value = credential_for(&signer, &import).await;
        let source: dialog_effects::blob::BlobReader = Box::new(Pieces {
            pieces: content.chunks(7_000).map(<[u8]>::to_vec).collect(),
        });
        let request = Request::new(Some(&value)).payload(Payload::Stream(source));
        let (status, _) = performed(access.handle(request).await).await;
        assert_eq!(status, 200);
        assert_eq!(access.provider().blobs(), 1);

        let read = subject.archive().blob().read(digest);
        let value = credential_for(&signer, &read).await;
        match access.handle(Request::new(Some(&value))).await {
            Answer::Performed(response) => {
                assert_eq!(response.status, 200);
                assert!(matches!(response.body, Content::Stream(_)), "{response:?}");
                assert_eq!(response.body.collect().await.unwrap(), content);
            }
            other => panic!("expected the blob, got {other:?}"),
        }
    }

    /// A body that yields the pieces it was given, in order.
    struct Pieces {
        pieces: std::collections::VecDeque<Vec<u8>>,
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl dialog_effects::blob::BlobSource for Pieces {
        async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
            Ok(self.pieces.pop_front())
        }
    }
}
