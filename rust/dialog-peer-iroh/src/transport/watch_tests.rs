//! A watch over a real QUIC stream: a write on one host reaching a
//! watcher on another, and a revoked grant ending the watch.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use dialog_capability::{Capability, Fork, ForkInvocation, Provider, SiteFork, Subject};
use dialog_did_web::{CachingResolver, WebResolver};
use dialog_effects::archive::{self, ArchiveError};
use dialog_effects::blob::{self, BlobError, BlobReader, BlobWriter};
use dialog_effects::memory::{self, CellState, MemoryError};
use dialog_effects::{peer, prelude::*};
use dialog_peer::helpers::test_session_with_peer;
use dialog_ucan_core::revocation::{RevocationChecker, RevocationMatch, RevocationSelector};
use iroh::endpoint::presets;
use iroh::tls::CaTlsConfig;
use iroh::{Endpoint, RelayMap, RelayMode, RelayUrl};
use iroh_base::EndpointAddr;

use super::{ALPN, IrohChannel, accept};
use crate::changes::Changes;
use crate::helpers::Volatile;
use crate::serve::Responder;
use crate::site::{Iroh, IrohAddress, IrohFork};

/// Blocks from the crate's helper store, cells from dialog-storage's,
/// which keeps them with compare-and-swap as a real store does.
#[derive(Default)]
struct Store {
    blocks: Volatile,
    cells: dialog_storage::provider::Volatile,
    /// Resolves fail while this is set.
    failing: Arc<AtomicBool>,
}

#[dialog_capability::provider]
impl Provider<memory::Resolve> for Store {
    async fn execute(&self, input: Capability<memory::Resolve>) -> Result<CellState, MemoryError> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(MemoryError::Storage("the store is down".into()));
        }
        Provider::<memory::Resolve>::execute(&self.cells, input).await
    }
}

macro_rules! forwards {
    ($field:ident: $($effect:ty => $output:ty),+ $(,)?) => {
        $(
            #[dialog_capability::provider]
            impl Provider<$effect> for Store {
                async fn execute(&self, input: Capability<$effect>) -> $output {
                    Provider::<$effect>::execute(&self.$field, input).await
                }
            }
        )+
    };
}

forwards!(blocks:
    archive::Get => Result<Option<Vec<u8>>, ArchiveError>,
    archive::Put => Result<(), ArchiveError>,
    archive::Import => Result<(), ArchiveError>,
    blob::Read => Result<BlobReader, BlobError>,
    blob::Write => Result<BlobWriter, BlobError>,
    blob::Import => Result<BlobWriter, BlobError>,
    peer::Hello => Result<peer::Greeting, peer::PeerError>,
    peer::Spaces => Result<Vec<peer::Offer>, peer::PeerError>,
);
forwards!(cells:
    memory::Publish => Result<memory::Version, MemoryError>,
    memory::Retract => Result<(), MemoryError>,
);

/// Every delegation is revoked once the flag is set.
#[derive(Clone, Default)]
struct Revocable(Arc<AtomicBool>);

impl RevocationChecker for Revocable {
    type Error = std::convert::Infallible;

    async fn query(
        &self,
        selector: RevocationSelector<'_>,
    ) -> Result<Option<RevocationMatch>, Self::Error> {
        Ok(self.0.load(Ordering::SeqCst).then(|| RevocationMatch {
            revocation: selector.delegation,
            principal: selector.by[0].clone(),
            proof: None,
        }))
    }
}

/// A relay in this process, which every endpoint here reaches the others
/// through. Each endpoint is then a host with no route to the others but
/// the relay, which is the case a watch across machines is, and needs no
/// network but loopback.
struct Relay {
    map: RelayMap,
    url: RelayUrl,
    _server: Box<dyn std::any::Any>,
}

async fn relay() -> Relay {
    let (map, url, server) = iroh::test_utils::run_relay_server()
        .await
        .expect("a relay runs on loopback");
    Relay {
        map,
        url,
        _server: Box::new(server),
    }
}

async fn endpoint(relay: &Relay, alpns: Vec<Vec<u8>>) -> Endpoint {
    let endpoint = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Custom(relay.map.clone()))
        .ca_tls_config(CaTlsConfig::insecure_skip_verify())
        .alpns(alpns)
        .bind()
        .await
        .expect("an endpoint binds");
    endpoint.online().await;
    endpoint
}

async fn serving(
    relay: &Relay,
    revocations: Revocable,
    recheck: Duration,
) -> (Endpoint, IrohAddress) {
    serving_store(
        relay,
        Store::default(),
        revocations,
        recheck,
        Arc::default(),
    )
    .await
}

async fn serving_store(
    relay: &Relay,
    store: Store,
    revocations: Revocable,
    recheck: Duration,
    changes: Arc<Changes>,
) -> (Endpoint, IrohAddress) {
    let endpoint = endpoint(relay, vec![ALPN.to_vec()]).await;
    let responder = Arc::new(
        Responder::new(store, CachingResolver::new(WebResolver::new()))
            .with_changes(changes)
            .with_revocations(revocations)
            .with_recheck(recheck),
    );
    tokio::spawn(accept(endpoint.clone(), responder));
    let address =
        IrohAddress::from(EndpointAddr::new(endpoint.id()).with_relay_url(relay.url.clone()));
    (endpoint, address)
}

async fn dialing(relay: &Relay) -> (Endpoint, Iroh) {
    let endpoint = endpoint(relay, Vec::new()).await;
    (endpoint.clone(), Iroh::new(IrohChannel::new(endpoint)))
}

/// A publish from host A reaches a watcher on host B, both through the
/// serving peer, in well under a second.
#[tokio::test]
async fn a_publish_reaches_a_watcher_on_another_host() {
    let relay = relay().await;
    let (server, address) = serving(&relay, Revocable::default(), Duration::from_secs(30)).await;
    let (a, writer) = dialing(&relay).await;
    let (b, watcher) = dialing(&relay).await;
    let (session, host) = test_session_with_peer().await;
    let subject = Subject::from(host.did());

    let watch = subject
        .clone()
        .reader()
        .memory()
        .space("local")
        .cell("head")
        .watch();
    let fork: IrohFork<memory::Watch> = Fork::<Iroh, _>::new(watch, address.clone()).into();
    let invocation = fork.authorize(&session).await.expect("authorized");
    let mut editions =
        Provider::<ForkInvocation<Iroh, memory::Watch>>::execute(&watcher, invocation)
            .await
            .expect("the peer accepts the watch");
    assert_eq!(
        editions.next().await.expect("a state"),
        Some(None),
        "the watch opens with the cell as it stands: empty"
    );

    let mut when = None;
    for round in 0..3u8 {
        let content = vec![round; 8];
        let publish = subject
            .clone()
            .writer()
            .memory()
            .space("local")
            .cell("head")
            .publish(content.clone(), when.clone());
        let fork: IrohFork<memory::Publish> = Fork::<Iroh, _>::new(publish, address.clone()).into();
        let invocation = fork.authorize(&session).await.expect("authorized");
        let started = Instant::now();
        let version =
            Provider::<ForkInvocation<Iroh, memory::Publish>>::execute(&writer, invocation)
                .await
                .expect("the publish lands");

        let state = tokio::time::timeout(Duration::from_secs(1), editions.next())
            .await
            .expect("the change arrives within a second")
            .expect("a state")
            .expect("the watch is still open")
            .expect("the cell holds something");
        assert_eq!(state.content, content);
        assert_eq!(state.version, version);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
        when = Some(version);
    }

    drop(editions);
    a.close().await;
    b.close().await;
    server.close().await;
}

/// Revoking the grant behind a watch ends it, even when its cell never
/// changes again: the idle recheck finds it.
#[tokio::test]
async fn revoking_the_grant_ends_the_watch() {
    let revocations = Revocable::default();
    let relay = relay().await;
    let (server, address) = serving(&relay, revocations.clone(), Duration::from_millis(200)).await;
    let (b, watcher) = dialing(&relay).await;
    let (session, host) = test_session_with_peer().await;
    let subject = Subject::from(host.did());

    let watch = subject
        .reader()
        .memory()
        .space("local")
        .cell("head")
        .watch();
    let fork: IrohFork<memory::Watch> = Fork::<Iroh, _>::new(watch, address).into();
    let invocation = fork.authorize(&session).await.expect("authorized");
    let mut editions =
        Provider::<ForkInvocation<Iroh, memory::Watch>>::execute(&watcher, invocation)
            .await
            .expect("the peer accepts the watch");
    editions.next().await.expect("the opening state");

    revocations.0.store(true, Ordering::SeqCst);

    let ended = tokio::time::timeout(Duration::from_secs(2), editions.next())
        .await
        .expect("the watch ends promptly");
    assert!(
        matches!(ended, Err(MemoryError::Authorization(_))),
        "the watch ends as unauthorized: {ended:?}"
    );
    assert!(
        matches!(editions.next().await, Ok(None)),
        "and nothing follows"
    );

    b.close().await;
    server.close().await;
}

/// A store failure during a watch is the watch's last state: the watcher
/// hears it once, the watch ends, and a new watch opens once the store
/// is back.
#[tokio::test]
async fn a_store_failure_ends_the_watch() {
    let relay = relay().await;
    let store = Store::default();
    let failing = store.failing.clone();
    let changes = Arc::new(Changes::default());
    let (server, address) = serving_store(
        &relay,
        store,
        Revocable::default(),
        Duration::from_secs(30),
        changes.clone(),
    )
    .await;
    let (a, writer) = dialing(&relay).await;
    let (b, watcher) = dialing(&relay).await;
    let (session, host) = test_session_with_peer().await;
    let subject = Subject::from(host.did());

    let open = || async {
        let watch = subject
            .clone()
            .reader()
            .memory()
            .space("local")
            .cell("head")
            .watch();
        let fork: IrohFork<memory::Watch> = Fork::<Iroh, _>::new(watch, address.clone()).into();
        let invocation = fork.authorize(&session).await.expect("authorized");
        Provider::<ForkInvocation<Iroh, memory::Watch>>::execute(&watcher, invocation).await
    };

    let mut editions = open().await.expect("the peer accepts the watch");
    editions.next().await.expect("the opening state");

    // The store goes down, then a write announces a change.
    failing.store(true, Ordering::SeqCst);
    let publish = subject
        .clone()
        .writer()
        .memory()
        .space("local")
        .cell("head")
        .publish(vec![1; 8], None);
    let fork: IrohFork<memory::Publish> = Fork::<Iroh, _>::new(publish, address.clone()).into();
    let invocation = fork.authorize(&session).await.expect("authorized");
    Provider::<ForkInvocation<Iroh, memory::Publish>>::execute(&writer, invocation)
        .await
        .expect("the publish lands");

    let failed = tokio::time::timeout(Duration::from_secs(2), editions.next())
        .await
        .expect("the failure arrives promptly");
    assert!(
        matches!(failed, Err(MemoryError::Storage(_))),
        "the failure is the watch's answer: {failed:?}"
    );
    let after = tokio::time::timeout(Duration::from_secs(2), editions.next())
        .await
        .expect("the watch ends promptly");
    assert!(matches!(after, Ok(None)), "and nothing follows: {after:?}");
    // The client still holds its end; only the server can have let go.
    let deadline = Instant::now() + Duration::from_secs(2);
    while changes.following() > 0 {
        assert!(Instant::now() < deadline, "the server stops following");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    failing.store(false, Ordering::SeqCst);
    let mut editions = open().await.expect("a new watch opens");
    let state = editions
        .next()
        .await
        .expect("a state")
        .expect("the watch is open")
        .expect("the cell holds the write");
    assert_eq!(state.content, vec![1; 8], "with the cell as it stands");

    a.close().await;
    b.close().await;
    server.close().await;
}

/// A write the serving host makes itself, through the responder's store,
/// reaches a watcher on another host as a write over iroh does.
#[tokio::test]
async fn a_local_write_reaches_a_watcher_on_another_host() {
    let relay = relay().await;
    let endpoint = endpoint(&relay, vec![ALPN.to_vec()]).await;
    let responder = Arc::new(Responder::new(
        Store::default(),
        CachingResolver::new(WebResolver::new()),
    ));
    tokio::spawn(accept(endpoint.clone(), responder.clone()));
    let address =
        IrohAddress::from(EndpointAddr::new(endpoint.id()).with_relay_url(relay.url.clone()));
    let (b, watcher) = dialing(&relay).await;
    let (session, host) = test_session_with_peer().await;
    let subject = Subject::from(host.did());

    let watch = subject
        .clone()
        .reader()
        .memory()
        .space("local")
        .cell("head")
        .watch();
    let fork: IrohFork<memory::Watch> = Fork::<Iroh, _>::new(watch, address).into();
    let invocation = fork.authorize(&session).await.expect("authorized");
    let mut editions =
        Provider::<ForkInvocation<Iroh, memory::Watch>>::execute(&watcher, invocation)
            .await
            .expect("the peer accepts the watch");
    editions.next().await.expect("the opening state");

    let publish = subject
        .writer()
        .memory()
        .space("local")
        .cell("head")
        .publish(vec![7; 8], None);
    Provider::<memory::Publish>::execute(responder.store(), publish)
        .await
        .expect("the local publish lands");

    let state = tokio::time::timeout(Duration::from_secs(1), editions.next())
        .await
        .expect("the change arrives within a second")
        .expect("a state")
        .expect("the watch is still open")
        .expect("the cell holds something");
    assert_eq!(state.content, vec![7; 8]);

    b.close().await;
    endpoint.close().await;
}
