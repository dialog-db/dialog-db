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
use dialog_effects::MethodExt as _;
use dialog_effects::archive::ArchiveError;
use dialog_effects::archive::prelude::*;
use dialog_effects::blob::BlobError;
use dialog_effects::blob::prelude::*;
use dialog_effects::memory::prelude::CellScope;
use dialog_effects::memory::{Edition, MemoryError, Publish, Resolve, Version, Watch};
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
            .writer()
            .archive()
            .catalog("index")
            .put(Buffer::from(content.clone())),
    )
    .await?;
    let served = perform(
        &service,
        &signer,
        subject.reader().archive().catalog("index").get(digest),
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
            .reader()
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
            .writer()
            .archive()
            .catalog("index")
            .put(Buffer::from(content)),
    )
    .await?;
    let elsewhere = perform(
        &service,
        &signer,
        subject.reader().archive().catalog("blob").get(digest),
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
    let cell = || CellScope::new(subject.clone(), "sync", "head");

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
    let cell = || CellScope::new(subject.clone(), "sync", "head");

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
    let cell = || CellScope::new(subject.clone(), "sync", "head");
    perform(&service, &signer, cell().publish(b"one".to_vec(), None)).await?;
    let again = perform(&service, &signer, cell().publish(b"other".to_vec(), None)).await;
    assert!(matches!(again, Err(MemoryError::VersionMismatch { .. })));
    Ok(())
}

#[dialog_common::test]
async fn it_retracts_a_cell_at_its_version(service: UcanServiceAddress) -> anyhow::Result<()> {
    let (signer, subject) = owner().await;
    let cell = || CellScope::new(subject.clone(), "sync", "head");
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
            .reader()
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
            .writer()
            .archive()
            .blob()
            .import(digest.clone(), content.len() as u64),
    )
    .await?;
    for part in content.chunks(3_000) {
        sink.write_all(part).await?;
    }
    assert_eq!(sink.finish().await?, digest);

    let reader = perform(
        &service,
        &signer,
        subject.reader().archive().blob().read(digest),
    )
    .await?;
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
            .writer()
            .archive()
            .blob()
            .import(digest.clone(), content.len() as u64),
    )
    .await?;
    sink.write_all(&content).await?;
    sink.finish().await?;

    let ranged =
        subject
            .clone()
            .reader()
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
        .reader()
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
            .reader()
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
            .writer()
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
        .writer()
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

/// A watch over the service's socket answers what the cell holds when it
/// begins, then each change: here, a publish made over HTTP.
#[dialog_common::test]
async fn it_watches_a_cell_over_the_socket(service: UcanServiceAddress) -> anyhow::Result<()> {
    let (signer, subject) = owner().await;
    let cell = CellScope::new(subject, "local", "head");
    let watch = cell.watch();
    let authorization = issued(&signer, &watch).await;
    let address = UcanAddress::new(&service.endpoint).with_socket(&service.socket);
    let mut editions = Provider::<ForkInvocation<UcanSite, Watch>>::execute(
        &UcanSite::default(),
        ForkInvocation::new(watch, address, authorization),
    )
    .await?;
    assert_eq!(editions.next().await?, Some(None), "the cell begins empty");

    let version = perform(&service, &signer, cell.publish(b"first".to_vec(), None)).await?;
    assert_eq!(
        editions.next().await?,
        Some(Some(Edition {
            content: b"first".to_vec(),
            version: version.clone(),
        })),
        "the publish reaches the watch"
    );

    perform(
        &service,
        &signer,
        cell.publish(b"second".to_vec(), Some(version)),
    )
    .await?;
    let next = editions.next().await?;
    assert_eq!(
        next.and_then(|state| state).map(|edition| edition.content),
        Some(b"second".to_vec()),
        "and so does the next"
    );
    Ok(())
}

/// Where the address names a socket, a cell's invocations go over it: the
/// endpoint here answers nothing, so a publish and a resolve that succeed
/// went over the socket.
#[dialog_common::test]
async fn it_reads_and_writes_a_cell_over_the_socket(
    service: UcanServiceAddress,
) -> anyhow::Result<()> {
    let (signer, subject) = owner().await;
    let cell = CellScope::new(subject, "local", "head");
    let address = UcanAddress::new("http://127.0.0.1:1/").with_socket(&service.socket);

    let publish = cell.publish(b"framed".to_vec(), None);
    let authorization = issued(&signer, &publish).await;
    let version = Provider::<ForkInvocation<UcanSite, Publish>>::execute(
        &UcanSite::default(),
        ForkInvocation::new(publish, address.clone(), authorization),
    )
    .await?;

    let resolve = cell.resolve();
    let authorization = issued(&signer, &resolve).await;
    let resolved = Provider::<ForkInvocation<UcanSite, Resolve>>::execute(
        &UcanSite::default(),
        ForkInvocation::new(resolve, address, authorization),
    )
    .await?;
    assert_eq!(
        resolved,
        Some(Edition {
            content: b"framed".to_vec(),
            version,
        })
    );
    Ok(())
}

/// A socket that cannot be reached is passed over: the cell's invocation
/// goes to the endpoint as a request.
#[dialog_common::test]
async fn it_passes_over_a_socket_it_cannot_reach(
    service: UcanServiceAddress,
) -> anyhow::Result<()> {
    let (signer, subject) = owner().await;
    let cell = CellScope::new(subject, "local", "head");
    let address = UcanAddress::new(&service.endpoint).with_socket("ws://127.0.0.1:1/");
    let publish = cell.publish(b"requested".to_vec(), None);
    let authorization = issued(&signer, &publish).await;
    Provider::<ForkInvocation<UcanSite, Publish>>::execute(
        &UcanSite::default(),
        ForkInvocation::new(publish, address, authorization),
    )
    .await?;
    Ok(())
}

/// A service that names no socket cannot follow a cell: the watch is
/// refused as unsupported, so the cell is resolved again instead.
#[dialog_common::test]
async fn it_refuses_a_watch_where_the_service_names_no_socket(
    service: UcanServiceAddress,
) -> anyhow::Result<()> {
    let (signer, subject) = owner().await;
    let watch = CellScope::new(subject, "local", "head").watch();
    let authorization = issued(&signer, &watch).await;
    let watched = Provider::<ForkInvocation<UcanSite, Watch>>::execute(
        &UcanSite::default(),
        ForkInvocation::new(watch, UcanAddress::new(&service.endpoint), authorization),
    )
    .await;
    assert!(
        matches!(
            watched,
            Err(MemoryError::Rejected(
                dialog_effects::Rejection::Unsupported { .. }
            ))
        ),
        "{:?}",
        watched.err()
    );
    Ok(())
}

/// The layer itself, driven directly: what it does with a request that
/// carries no invocation, a credential it cannot read, an operation it
/// does not perform, and a body that is not what the invocation bound.
mod layer {
    use super::*;
    use crate::socket::{Change, Reply, Request as Frame, Session};
    use crate::{FRESHNESS, Issuance};
    use dialog_effects::memory::Edition;
    use dialog_ucan_core::promise::Promised;
    use dialog_ucan_core::subject::Subject as DelegatedSubject;
    use dialog_ucan_core::time::timestamp::{Duration, Timestamp, UNIX_EPOCH};
    use dialog_ucan_core::{
        DelegationBuilder, InvocationBuilder, InvocationChain, RevocationChecker, RevocationMatch,
        RevocationSelector,
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

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
            .writer()
            .archive()
            .catalog("index")
            .put(Buffer::from(content.clone()));
        let authorization = issued(&signer, &put).await;
        let container = Container::from(authorization.invocation().chain());
        // One invocation, read in each form by a service of its own: a
        // service that saw it once would refuse it as presented again.
        let store = MemoryStore::default();
        for tag in [Tag::Base64Url, Tag::Base64UrlGzip] {
            let access = Access::new(store.clone());
            let text = String::from_utf8(container.clone().encode(tag).unwrap()).unwrap();
            let value = format!("UCAN {text}");
            let request = Request::new(Some(&value)).payload(content.clone());
            let (status, _) = performed(access.handle(request).await).await;
            assert_eq!(status, 200, "{tag:?}");
        }
        assert_eq!(store.blocks(), 1);
    }

    #[dialog_common::test]
    async fn it_requires_the_body_a_write_stores() {
        let (signer, subject) = owner().await;
        let capability = subject
            .writer()
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
            .writer()
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
            .writer()
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
            .writer()
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
            .writer()
            .archive()
            .catalog("index")
            .put(Buffer::from(content.clone()));
        let access = Access::new(MemoryStore::default());
        let value = credential_for(&signer, &put).await;
        let request = Request::new(Some(&value)).payload(content.clone());
        let (status, _) = performed(access.handle(request).await).await;
        assert_eq!(status, 200);

        let get = subject
            .reader()
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
            .writer()
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

        let read = subject.reader().archive().blob().read(digest);
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

    /// The credential for `capability`, minted by `signer` on its own
    /// authority, saying it was issued at `issued_at` (Unix seconds), or not
    /// saying when.
    async fn issued_at<Fx>(
        signer: &Ed25519Signer,
        capability: &Capability<Fx>,
        issued_at: Option<u64>,
    ) -> String
    where
        Fx: Effect + Clone,
        Capability<Fx>: Ability,
    {
        let minted = issued(signer, capability).await;
        let arguments = minted.invocation().chain().arguments().clone();
        let command = capability
            .ability()
            .split('/')
            .filter(|segment| !segment.is_empty())
            .map(String::from)
            .collect();
        let builder = InvocationBuilder::new()
            .issuer(Signer::from(signer.clone()))
            .audience(signer)
            .subject(signer)
            .command(command)
            .arguments(arguments)
            .proofs(Vec::new());
        let builder = match issued_at {
            Some(at) => builder
                .issued_at(Timestamp::new(UNIX_EPOCH + Duration::from_secs(at)).expect("in range")),
            None => builder,
        };
        let invocation = builder.try_build().await.expect("the invocation mints");
        let chain = InvocationChain::new(invocation, Default::default());
        credential(Container::from(&chain)).expect("encodes")
    }

    fn refused(answer: Answer) -> AuthorizeError {
        match answer {
            Answer::Refused(refusal) => {
                assert_eq!(refusal.status(), 401, "a fresh invocation would do");
                refusal.reason().clone()
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    /// Every invocation the client mints says when it was issued.
    #[dialog_common::test]
    async fn it_mints_invocations_that_say_when_they_were_issued() {
        let (signer, subject) = owner().await;
        let resolve = CellScope::new(subject, "local", "head").resolve();
        let minted = issued(&signer, &resolve).await;
        let issued = minted
            .invocation()
            .chain()
            .invocation
            .issued_at()
            .map(|at| at.to_unix());
        let at = now_s();
        assert!(
            issued.is_some_and(|issued| issued + 5 >= at && issued <= at),
            "issued at {issued:?}, checked at {at}"
        );
    }

    /// An invocation presented a second time is refused, whatever it
    /// would do, and does not do it again.
    #[dialog_common::test]
    async fn it_refuses_an_invocation_presented_twice() {
        let (signer, subject) = owner().await;
        let content = b"first".to_vec();
        let publish =
            CellScope::new(subject.clone(), "local", "head").publish(content.clone(), None);
        let access = Access::new(MemoryStore::default());
        let value = credential_for(&signer, &publish).await;

        let request = Request::new(Some(&value)).payload(content.clone());
        let (status, _) = performed(access.handle(request).await).await;
        assert_eq!(status, 200);
        let version = access
            .provider()
            .cell(&subject, "local", "head")
            .map(|edition| edition.version);

        let request = Request::new(Some(&value)).payload(content);
        let reason = refused(access.handle(request).await);
        assert!(
            matches!(reason, AuthorizeError::Replayed { .. }),
            "{reason:?}"
        );
        assert_eq!(
            access
                .provider()
                .cell(&subject, "local", "head")
                .map(|edition| edition.version),
            version,
            "the replay wrote nothing"
        );
    }

    /// An invocation issued longer ago than the freshness window is
    /// refused as stale, and what it would write is not written.
    #[dialog_common::test]
    async fn it_refuses_an_invocation_issued_too_long_ago() {
        let (signer, subject) = owner().await;
        let content = b"late".to_vec();
        let publish =
            CellScope::new(subject.clone(), "local", "head").publish(content.clone(), None);
        let access = Access::new(MemoryStore::default());
        let long_ago = now_s() - FRESHNESS.as_secs() - 60;
        let value = issued_at(&signer, &publish, Some(long_ago)).await;

        let request = Request::new(Some(&value)).payload(content);
        let reason = refused(access.handle(request).await);
        assert!(
            matches!(
                reason,
                AuthorizeError::Stale {
                    issued_at: Some(_),
                    ..
                }
            ),
            "{reason:?}"
        );
        assert!(
            access.provider().cell(&subject, "local", "head").is_none(),
            "the stale invocation wrote nothing"
        );
    }

    /// An invocation that does not say when it was issued is served where
    /// saying is optional, as over HTTP, and refused where it is required.
    #[dialog_common::test]
    async fn it_requires_an_issue_time_only_where_asked() {
        let (signer, subject) = owner().await;
        let resolve = CellScope::new(subject, "local", "head").resolve();
        let access = Access::new(MemoryStore::default());

        let value = issued_at(&signer, &resolve, None).await;
        let (status, _) = performed(access.handle(Request::new(Some(&value))).await).await;
        assert_eq!(status, 404, "served: the cell is empty");

        let value = issued_at(&signer, &resolve, None).await;
        let container = crate::direct::credential_container(&value).expect("reads");
        let refused = access.admit(container, Issuance::Required).await;
        assert!(
            matches!(
                refused.as_ref().map_err(|refusal| refusal.reason()),
                Err(AuthorizeError::Stale {
                    issued_at: None,
                    ..
                })
            ),
            "{:?}",
            refused.err()
        );
    }

    /// Answers a revocation of `delegation` (its content identifier), by
    /// `principal`, once `revoked` is set.
    #[derive(Clone)]
    struct Revocable {
        delegation: String,
        principal: dialog_capability::Did,
        revoked: Arc<AtomicBool>,
    }

    impl RevocationChecker for Revocable {
        type Error = std::convert::Infallible;

        async fn query(
            &self,
            selector: RevocationSelector<'_>,
        ) -> Result<Option<RevocationMatch>, Self::Error> {
            let revoked = self.revoked.load(Ordering::SeqCst)
                && selector.delegation.to_string() == self.delegation
                && selector.by.contains(&self.principal);
            Ok(revoked.then(|| RevocationMatch {
                revocation: selector.delegation,
                principal: self.principal.clone(),
            }))
        }
    }

    /// A watch of the cell `head` in `local`, made by an operator the
    /// subject delegated reading its cells to: the delegation's content
    /// identifier, the subject, and the watch.
    async fn delegated_watch() -> (String, Ed25519Signer, Container) {
        let subject = Ed25519Signer::generate().await.expect("a subject");
        let operator = Ed25519Signer::generate().await.expect("an operator");
        let delegation = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&operator.did())
            .subject(DelegatedSubject::Specific(subject.did()))
            .command(["use", "get", "memory", "cell"].map(String::from).to_vec())
            .try_build()
            .await
            .expect("the delegation mints");
        let cid = delegation.to_cid();
        let mut arguments = std::collections::BTreeMap::new();
        arguments.insert("space".to_string(), Promised::String("local".into()));
        arguments.insert("cell".to_string(), Promised::String("head".into()));
        let invocation = InvocationBuilder::new()
            .issuer(operator)
            .audience(&subject.did())
            .subject(&subject.did())
            .command(
                ["use", "get", "memory", "cell", "watch"]
                    .map(String::from)
                    .to_vec(),
            )
            .arguments(arguments)
            .proofs(vec![cid])
            .issued_at(Timestamp::now())
            .try_build()
            .await
            .expect("the watch mints");
        let mut delegations = std::collections::HashMap::new();
        delegations.insert(cid, Arc::new(delegation));
        let chain = InvocationChain::new(invocation, delegations);
        (cid.to_string(), subject, Container::from(&chain))
    }

    /// A watch is accepted with what the cell holds when it begins, and is
    /// named by its invocation.
    #[dialog_common::test]
    async fn it_subscribes_a_watch_with_what_the_cell_holds() {
        let (signer, subject) = owner().await;
        let content = b"held".to_vec();
        let publish =
            CellScope::new(subject.clone(), "local", "head").publish(content.clone(), None);
        let access = Access::new(MemoryStore::default());
        let value = credential_for(&signer, &publish).await;
        let (status, _) = performed(
            access
                .handle(Request::new(Some(&value)).payload(content.clone()))
                .await,
        )
        .await;
        assert_eq!(status, 200);

        let watch = CellScope::new(subject.clone(), "local", "head").watch();
        let minted = issued(&signer, &watch).await;
        let chain = minted.invocation().chain();
        let (subscription, state) = access
            .subscribe(Container::from(chain))
            .await
            .expect("the watch is accepted");

        assert_eq!(state.map(|edition| edition.content), Some(content));
        assert_eq!(
            subscription.invocation(),
            chain.invocation.to_cid().to_string()
        );
        assert!(subscription.follows(subject.did(), "local", "head"));
        assert!(!subscription.follows(subject.did(), "local", "tail"));
    }

    /// A watch is authority that lasts, so one that does not say when it
    /// was issued is refused as stale, where a request would be served.
    #[dialog_common::test]
    async fn it_refuses_a_watch_that_does_not_say_when_it_was_issued() {
        let (signer, subject) = owner().await;
        let watch = CellScope::new(subject, "local", "head").watch();
        let access = Access::new(MemoryStore::default());
        let value = issued_at(&signer, &watch, None).await;
        let container = crate::direct::credential_container(&value).expect("reads");

        match access.subscribe(container).await {
            Err(Answer::Refused(refusal)) => assert!(
                matches!(
                    refusal.reason(),
                    AuthorizeError::Stale {
                        issued_at: None,
                        ..
                    }
                ),
                "{:?}",
                refusal.reason()
            ),
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    /// An invocation that is not a watch is not taken for one.
    #[dialog_common::test]
    async fn it_subscribes_nothing_but_a_watch() {
        let (signer, subject) = owner().await;
        let resolve = CellScope::new(subject, "local", "head").resolve();
        let access = Access::new(MemoryStore::default());
        let minted = issued(&signer, &resolve).await;
        let answer = access
            .subscribe(Container::from(minted.invocation().chain()))
            .await;
        assert!(matches!(answer, Err(Answer::Unsupported)), "{answer:?}");
    }

    /// A watch whose delegation is revoked after it began keeps being
    /// delivered to until its authority is checked again, which happens
    /// once the recheck interval has passed since it last held, and is
    /// then refused as revoked.
    #[dialog_common::test]
    async fn it_refuses_a_running_watch_once_its_delegation_is_revoked() {
        let (delegation, subject, watch) = delegated_watch().await;
        let revoked = Arc::new(AtomicBool::new(false));
        let access = Access::new(MemoryStore::default()).with_revocations(Revocable {
            delegation,
            principal: subject.did(),
            revoked: revoked.clone(),
        });
        let (mut subscription, state) = access.subscribe(watch).await.expect("accepted");
        assert_eq!(state, None, "nothing is published yet");
        let began = subscription.checked();
        let interval = std::time::Duration::from_secs(60);

        revoked.store(true, Ordering::SeqCst);
        access
            .recheck(&mut subscription, interval, began + 30)
            .await
            .expect("within the interval the authority is not checked again");

        let refused = access
            .recheck(&mut subscription, interval, began + 60)
            .await
            .expect_err("past the interval the revocation is found");
        assert!(
            matches!(refused.reason(), AuthorizeError::Revoked { .. }),
            "{:?}",
            refused.reason()
        );
        assert_eq!(
            subscription.checked(),
            began,
            "a refused check holds nothing"
        );
    }

    /// A watch whose authority still holds is found to, and when.
    #[dialog_common::test]
    async fn it_keeps_a_running_watch_whose_authority_holds() {
        let (_, _, watch) = delegated_watch().await;
        let access = Access::new(MemoryStore::default());
        let (mut subscription, _) = access.subscribe(watch).await.expect("accepted");
        let at = subscription.checked() + 120;
        access
            .recheck(&mut subscription, std::time::Duration::from_secs(60), at)
            .await
            .expect("the authority holds");
        assert_eq!(subscription.checked(), at);
    }

    /// A subscription is data a service can store between messages and
    /// read back whole.
    #[dialog_common::test]
    async fn it_stores_a_subscription_and_reads_it_back() {
        let (_, _, watch) = delegated_watch().await;
        let access = Access::new(MemoryStore::default());
        let (subscription, _) = access.subscribe(watch).await.expect("accepted");
        let bytes = serde_ipld_dagcbor::to_vec(&subscription).expect("encodes");
        let read: crate::Subscription = serde_ipld_dagcbor::from_slice(&bytes).expect("decodes");
        assert_eq!(read, subscription);
    }

    fn invoke(container: &Container, payload: Option<Vec<u8>>) -> Vec<u8> {
        Frame::Invoke {
            container: container.to_bytes().expect("encodes"),
            payload,
        }
        .encode()
    }

    /// Over a socket a watch is answered with what its cell holds, each
    /// change to the cell is delivered to it, and once it is cancelled
    /// nothing more is.
    #[dialog_common::test]
    async fn it_answers_a_watch_frame_and_delivers_changes_until_cancelled() {
        let (signer, subject) = owner().await;
        let access = Access::new(MemoryStore::default());
        let mut session = Session::new();

        let watch = CellScope::new(subject.clone(), "local", "head").watch();
        let minted = issued(&signer, &watch).await;
        let container = Container::from(minted.invocation().chain());
        let invocation = minted.invocation().chain().invocation.to_cid().to_string();
        let reply = session.receive(&access, &invoke(&container, None)).await;
        assert_eq!(
            reply,
            Some(Reply::State {
                invocation: invocation.clone(),
                state: None
            })
        );

        let state = Some(Edition {
            content: b"next".to_vec(),
            version: Version::from(b"v1".as_slice()),
        });
        let change = Change {
            subject: subject.did(),
            space: "local",
            cell: "head",
            state: &state,
        };
        let delivered = session
            .deliver(&access, change, Duration::from_secs(60), now_s())
            .await;
        assert_eq!(
            delivered,
            vec![Reply::State {
                invocation: invocation.clone(),
                state: state.clone()
            }]
        );
        let elsewhere = Change {
            cell: "tail",
            ..change
        };
        assert!(
            session
                .deliver(&access, elsewhere, Duration::from_secs(60), now_s())
                .await
                .is_empty(),
            "a change to another cell reaches no watch"
        );

        let cancel = Frame::Cancel { invocation }.encode();
        assert_eq!(session.receive(&access, &cancel).await, None);
        assert!(
            session
                .deliver(&access, change, Duration::from_secs(60), now_s())
                .await
                .is_empty(),
            "a cancelled watch is delivered nothing"
        );
    }

    /// A frame's invocation must say when it was issued, where a request's
    /// need not, and one that does not is refused and does nothing.
    #[dialog_common::test]
    async fn it_refuses_a_frame_that_does_not_say_when_it_was_issued() {
        let (signer, subject) = owner().await;
        let content = b"undated".to_vec();
        let publish =
            CellScope::new(subject.clone(), "local", "head").publish(content.clone(), None);
        let access = Access::new(MemoryStore::default());
        let mut session = Session::new();
        let value = issued_at(&signer, &publish, None).await;
        let container = crate::direct::credential_container(&value).expect("reads");

        let reply = session
            .receive(&access, &invoke(&container, Some(content)))
            .await;
        let Some(Reply::Answer { status, body, .. }) = reply else {
            panic!("expected an answer, got {reply:?}");
        };
        assert_eq!(status, 401);
        let reason: AuthorizeError = serde_json::from_slice(&body).expect("a reason");
        assert!(
            matches!(
                reason,
                AuthorizeError::Stale {
                    issued_at: None,
                    ..
                }
            ),
            "{reason:?}"
        );
        assert!(
            access.provider().cell(&subject, "local", "head").is_none(),
            "the refused frame wrote nothing"
        );
    }

    /// A watch whose delegation was revoked is ended, with the reason,
    /// once its authority is checked again, and is then dropped.
    #[dialog_common::test]
    async fn it_ends_a_watch_whose_authority_no_longer_holds() {
        let (delegation, subject, watch) = delegated_watch().await;
        let revoked = Arc::new(AtomicBool::new(false));
        let access = Access::new(MemoryStore::default()).with_revocations(Revocable {
            delegation,
            principal: subject.did(),
            revoked: revoked.clone(),
        });
        let mut session = Session::new();
        let reply = session.receive(&access, &invoke(&watch, None)).await;
        let Some(Reply::State { invocation, .. }) = reply else {
            panic!("expected the watch accepted, got {reply:?}");
        };

        revoked.store(true, Ordering::SeqCst);
        let state = None;
        let did = subject.did();
        let change = Change {
            subject: &did,
            space: "local",
            cell: "head",
            state: &state,
        };
        let delivered = session
            .deliver(&access, change, Duration::ZERO, now_s())
            .await;
        let [
            Reply::Ended {
                invocation: ended,
                status,
                body,
            },
        ] = delivered.as_slice()
        else {
            panic!("expected the watch ended, got {delivered:?}");
        };
        assert_eq!(ended, &invocation);
        assert_eq!(*status, 403);
        let reason: AuthorizeError = serde_json::from_slice(body).expect("a reason");
        assert!(
            matches!(reason, AuthorizeError::Revoked { .. }),
            "{reason:?}"
        );
        assert_eq!(session.watches().count(), 0, "the ended watch is dropped");
    }
}
