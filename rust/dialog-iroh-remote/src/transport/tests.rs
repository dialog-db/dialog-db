//! Two endpoints, one loopback, and a block that goes the whole way.
//!
//! Everything else in this crate tests the protocol over a function
//! call. This tests what the function call stands in for: that a signed
//! container survives a real QUIC stream, that the answer survives the
//! way back, and that the framing assumption — one exchange is one
//! stream, ended by finishing it — holds when both sides are reading a
//! socket rather than passing a `Vec`.

use std::sync::Arc;

use dialog_capability::{Fork, ForkInvocation, Provider, SiteFork, Subject};
use dialog_common::Blake3Hash;
use dialog_common::Buffer;
use dialog_did_web::{CachingResolver, WebResolver};
use dialog_effects::Use;
use dialog_effects::archive::{self, ArchiveError};
use dialog_effects::blob::{self, BlobError};
use dialog_operator::helpers::test_operator_with_profile;
use iroh::Endpoint;
use iroh::endpoint::presets;
use iroh_base::{EndpointAddr, SecretKey};

use super::{ALPN, IrohChannel, accept};
use crate::channel::{Channel, ChannelError};
use crate::helpers::Volatile;
use crate::serve::Responder;
use crate::site::{Iroh, IrohAddress, IrohFork};

type Peer = Arc<Responder<Volatile, CachingResolver<WebResolver>>>;

/// A bound endpoint answering on [`ALPN`], and the store it answers
/// from.
///
/// Relays are off on both sides: the endpoints are in one process, so a
/// direct route always exists, and reaching for n0's relays would make
/// this test fail whenever their network — rather than this code — is
/// having a bad day.
async fn peer() -> (Endpoint, IrohAddress, Peer) {
    let endpoint = Endpoint::builder(presets::N0DisableRelay)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await
        .expect("an endpoint binds");
    let responder = Arc::new(Responder::new(
        Volatile::default(),
        CachingResolver::new(WebResolver::new()),
    ));
    let address = IrohAddress::from(endpoint.addr());

    tokio::spawn(accept(endpoint.clone(), responder.clone()));

    (endpoint, address, responder)
}

async fn dialer() -> (Endpoint, IrohChannel) {
    let endpoint = Endpoint::builder(presets::N0DisableRelay)
        .bind()
        .await
        .expect("an endpoint binds");
    (endpoint.clone(), IrohChannel::new(endpoint))
}

#[tokio::test]
async fn a_block_crosses_a_real_stream_and_reads_back() {
    let (serving, address, responder) = peer().await;
    let (dialing, channel) = dialer().await;
    let site = Iroh::new(channel);

    let (operator, profile) = test_operator_with_profile().await;
    let subject = profile.did();
    let bytes = b"over QUIC, signed and verified".to_vec();
    let digest = Buffer::from(bytes.clone()).blake3_hash().clone();

    let put = Subject::from(subject.clone())
        .attenuate(Use)
        .attenuate(archive::Archive)
        .attenuate(archive::Catalog::new("blocks"))
        .invoke(archive::Put::new(Buffer::from(bytes.clone())));
    let fork: IrohFork<archive::Put> = Fork::<Iroh, _>::new(put, address.clone()).into();
    let invocation = fork.authorize(&operator).await.expect("authorized");
    let stored: Result<(), ArchiveError> =
        Provider::<ForkInvocation<Iroh, archive::Put>>::execute(&site, invocation).await;
    stored.expect("the peer performs the put");

    assert_eq!(
        responder.store().get(&digest),
        Some(bytes.clone()),
        "the block reached the peer's store"
    );

    // A second exchange over the same channel, which is what the
    // connection cache exists for: it must reuse the connection and
    // still answer correctly, rather than reading a stale or truncated
    // stream.
    let read = Subject::from(subject)
        .attenuate(Use)
        .attenuate(archive::Archive)
        .attenuate(archive::Catalog::new("blocks"))
        .invoke(archive::Get::new(digest));
    let fork: IrohFork<archive::Get> = Fork::<Iroh, _>::new(read, address).into();
    let invocation = fork.authorize(&operator).await.expect("authorized");
    let found: Result<Option<Vec<u8>>, ArchiveError> =
        Provider::<ForkInvocation<Iroh, archive::Get>>::execute(&site, invocation).await;

    assert_eq!(
        found.expect("the get succeeds"),
        Some(bytes),
        "the answer came back over the reused connection intact"
    );

    dialing.close().await;
    serving.close().await;
}

/// A peer that was never reached is a transport failure, and has to be
/// reported as one: a caller retries an unreachable peer and does not
/// retry a peer that refused it.
#[tokio::test]
async fn a_peer_that_is_not_there_is_unreachable() {
    let (dialing, channel) = dialer().await;

    // A well-formed endpoint id nothing is listening on. Relays and
    // discovery are off, so there is nowhere to look it up and the dial
    // fails rather than hanging on a lookup.
    let nobody = IrohAddress::from(EndpointAddr::from(SecretKey::generate().public()));

    let error = channel
        .exchange(&nobody, vec![0u8; 4])
        .await
        .expect_err("nothing is listening");
    assert!(
        matches!(error, ChannelError::Unreachable { .. }),
        "a peer that was never reached is unreachable, not interrupted: {error}"
    );

    dialing.close().await;
}

/// A blob written to a peer and read back, over one connection.
///
/// This is the case the whole streaming layer exists for, and the one a
/// value-answered protocol cannot express: the bytes never appear in an
/// invocation's arguments, and the peer's answer is a transfer rather
/// than a value.
#[tokio::test]
async fn a_blob_streams_both_ways() {
    let (serving, address, responder) = peer().await;
    let (dialing, channel) = dialer().await;
    let site = Iroh::new(channel);

    let (operator, profile) = test_operator_with_profile().await;
    let subject = profile.did();

    // Bigger than one QUIC datagram, so the transfer is genuinely
    // chunked and a reader that assumed one chunk would fail here.
    let bytes: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();

    let write = Subject::from(subject.clone())
        .attenuate(Use)
        .attenuate(archive::Archive)
        .attenuate(blob::Blob)
        .attenuate(blob::Write);
    let fork: IrohFork<blob::Write> = Fork::<Iroh, _>::new(write, address.clone()).into();
    let invocation = fork.authorize(&operator).await.expect("authorized");
    let mut writer = Provider::<ForkInvocation<Iroh, blob::Write>>::execute(&site, invocation)
        .await
        .expect("the peer accepts a blob");

    for chunk in bytes.chunks(64 * 1024) {
        writer.write_all(chunk).await.expect("the chunk goes out");
    }
    let digest = writer.finish().await.expect("the peer commits the blob");

    assert_eq!(
        responder.store().blob(&digest),
        Some(bytes.clone()),
        "every byte arrived, in order, and hashed to what the peer reported"
    );

    let read = Subject::from(subject)
        .attenuate(Use)
        .attenuate(archive::Archive)
        .attenuate(blob::Blob)
        .attenuate(blob::Read::new(digest));
    let fork: IrohFork<blob::Read> = Fork::<Iroh, _>::new(read, address).into();
    let invocation = fork.authorize(&operator).await.expect("authorized");
    let mut reader = Provider::<ForkInvocation<Iroh, blob::Read>>::execute(&site, invocation)
        .await
        .expect("the peer has the blob");

    let mut read_back = Vec::new();
    while let Some(chunk) = reader.next().await.expect("the transfer holds") {
        read_back.extend_from_slice(&chunk);
    }
    assert_eq!(read_back, bytes, "the blob came back byte for byte");

    dialing.close().await;
    serving.close().await;
}

/// An import declares its digest, and a peer that receives different
/// bytes must refuse rather than commit them under the declared name.
#[tokio::test]
async fn an_import_that_lies_about_its_digest_is_refused() {
    let (serving, address, responder) = peer().await;
    let (dialing, channel) = dialer().await;
    let site = Iroh::new(channel);

    let (operator, profile) = test_operator_with_profile().await;
    let honest = b"the bytes that were promised".to_vec();
    let declared = Blake3Hash::from(*blake3::hash(&honest).as_bytes());

    let import = Subject::from(profile.did())
        .attenuate(Use)
        .attenuate(archive::Archive)
        .attenuate(blob::Blob)
        .attenuate(blob::Import::new(declared.clone(), honest.len() as u64));
    let fork: IrohFork<blob::Import> = Fork::<Iroh, _>::new(import, address).into();
    let invocation = fork.authorize(&operator).await.expect("authorized");
    let mut writer = Provider::<ForkInvocation<Iroh, blob::Import>>::execute(&site, invocation)
        .await
        .expect("the peer accepts the import");

    writer
        .write_all(b"entirely different bytes")
        .await
        .expect("the bytes go out");

    match writer.finish().await {
        Err(BlobError::DigestMismatch { .. }) => {}
        other => panic!("expected the peer to catch the mismatch, got {other:?}"),
    }

    assert_eq!(
        responder.store().blob(&declared),
        None,
        "nothing was committed under the digest that was not delivered"
    );

    dialing.close().await;
    serving.close().await;
}
