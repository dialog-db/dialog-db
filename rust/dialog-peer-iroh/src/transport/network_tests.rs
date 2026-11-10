//! Two endpoints that find each other the way peers on different
//! networks do: through n0's public relays and its address lookup.
//!
//! The tests beside this module turn relays and lookup off, so a direct
//! route on loopback is all they ever exercise. A peer behind a NAT is
//! dialed by its relay or by its key alone, and nothing else checks that
//! a signed container survives either path. These depend on n0's
//! services being up, which is why they sit behind
//! `iroh-network-tests` and run in their own workflow, as the S3 and R2
//! tests do.

use std::sync::Arc;
use std::time::Duration;

use dialog_capability::{Fork, ForkInvocation, Provider, SiteFork, Subject};
use dialog_common::Buffer;
use dialog_did_web::{CachingResolver, WebResolver};
use dialog_effects::archive::{self, ArchiveError};
use dialog_effects::prelude::*;
use dialog_peer::helpers::test_session_with_peer;
use iroh::Endpoint;
use iroh::endpoint::presets;
use iroh_base::EndpointAddr;

use super::{ALPN, IrohChannel, accept};
use crate::helpers::Volatile;
use crate::serve::Responder;
use crate::site::{Iroh, IrohAddress, IrohFork};

type Peer = Arc<Responder<Volatile, CachingResolver<WebResolver>>>;

/// How long a peer gets to reach a home relay, and a dial to find a
/// peer by its key. Generous, because both wait on services across the
/// internet, and a slow run should fail by timing out rather than hang.
const PATIENCE: Duration = Duration::from_secs(60);

/// A peer on n0's defaults, answering on [`ALPN`], once it holds a
/// connection to its home relay.
async fn peer() -> (Endpoint, Peer) {
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await
        .expect("an endpoint binds");
    tokio::time::timeout(PATIENCE, endpoint.online())
        .await
        .expect("the peer reaches a home relay");

    let responder = Arc::new(Responder::new(
        Volatile::default(),
        CachingResolver::new(WebResolver::new()),
    ));
    tokio::spawn(accept(endpoint.clone(), responder.clone()));

    (endpoint, responder)
}

async fn dialer() -> (Endpoint, IrohChannel) {
    let endpoint = Endpoint::builder(presets::N0)
        .bind()
        .await
        .expect("an endpoint binds");
    (endpoint.clone(), IrohChannel::new(endpoint))
}

/// Puts a block on the peer at `address` and reads it back, and answers
/// whether both exchanges went through.
async fn round_trip(site: &Iroh, address: &IrohAddress, responder: &Peer) -> Result<(), String> {
    let (session, host) = test_session_with_peer().await;
    let subject = host.did();
    let bytes = b"across the internet, signed and verified".to_vec();
    let digest = Buffer::from(bytes.clone()).blake3_hash().clone();

    let put = Subject::from(subject.clone())
        .writer()
        .archive()
        .catalog("blocks")
        .put(Buffer::from(bytes.clone()));
    let fork: IrohFork<archive::Put> = Fork::<Iroh, _>::new(put, address.clone()).into();
    let invocation = fork.authorize(&session).await.expect("authorized");
    let stored: Result<(), ArchiveError> =
        Provider::<ForkInvocation<Iroh, archive::Put>>::execute(site, invocation).await;
    stored.map_err(|error| format!("the put failed: {error}"))?;

    assert_eq!(
        responder.store().get(&digest),
        Some(bytes.clone()),
        "the block reached the peer's store"
    );

    let read = Subject::from(subject)
        .reader()
        .archive()
        .catalog("blocks")
        .get(digest);
    let fork: IrohFork<archive::Get> = Fork::<Iroh, _>::new(read, address.clone()).into();
    let invocation = fork.authorize(&session).await.expect("authorized");
    let found: Result<Option<Vec<u8>>, ArchiveError> =
        Provider::<ForkInvocation<Iroh, archive::Get>>::execute(site, invocation).await;

    assert_eq!(
        found.map_err(|error| format!("the get failed: {error}"))?,
        Some(bytes),
        "the block came back intact"
    );
    Ok(())
}

/// A peer named by its key and its home relay, with no IP address to
/// dial, is reached through that relay.
///
/// This is how a peer behind a NAT is written down: its routes are the
/// relay it can be found at. The connection may move to a direct path
/// once both sides are talking, but the first dial has nothing else to
/// go on, so the exchange only succeeds if the relay carried it.
#[tokio::test]
async fn a_peer_is_reached_through_its_relay() {
    let (serving, responder) = peer().await;
    let (dialing, channel) = dialer().await;
    let site = Iroh::new(channel);

    let relay = serving
        .addr()
        .relay_urls()
        .next()
        .cloned()
        .expect("an online peer has a home relay");
    let address = IrohAddress::from(EndpointAddr::new(serving.id()).with_relay_url(relay));
    assert_eq!(
        address.addr().ip_addrs().count(),
        0,
        "the address carries no route but the relay"
    );

    tokio::time::timeout(PATIENCE, round_trip(&site, &address, &responder))
        .await
        .expect("the exchange finishes")
        .expect("the relay carries the exchange");

    dialing.close().await;
    serving.close().await;
}

/// A peer named by its key alone is found through n0's address lookup.
///
/// This is the claim behind writing a peer as a bare `did:key`: wherever
/// something can resolve it, the key is the whole address. The peer
/// publishes its relay when it comes online, and that takes a moment to
/// become visible, so the first dials may find nothing; the test retries
/// until the lookup answers or [`PATIENCE`] runs out.
#[tokio::test]
async fn a_peer_named_by_its_key_alone_is_found() {
    let (serving, responder) = peer().await;
    let (dialing, channel) = dialer().await;
    let site = Iroh::new(channel);

    let address = IrohAddress::from(EndpointAddr::new(serving.id()));

    let found = tokio::time::timeout(PATIENCE, async {
        loop {
            match round_trip(&site, &address, &responder).await {
                Ok(()) => return,
                Err(_) => tokio::time::sleep(Duration::from_secs(2)).await,
            }
        }
    })
    .await;
    assert!(
        found.is_ok(),
        "the peer was found by its key within {PATIENCE:?}"
    );

    dialing.close().await;
    serving.close().await;
}
