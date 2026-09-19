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
use dialog_common::Buffer;
use dialog_did_web::{CachingResolver, WebResolver};
use dialog_effects::Use;
use dialog_effects::archive::{self, ArchiveError};
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
