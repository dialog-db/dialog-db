//! The two halves meeting: a client signs an invocation, a peer performs
//! it, and the answer comes back typed.
//!
//! The channel here is a function call rather than a network, on
//! purpose. What is under test is that the client's container is
//! exactly what the responder expects and that the responder's answer is
//! exactly what the client can read — the protocol, not the transport,
//! which iroh proves separately and which nothing in this crate can
//! influence.

use super::*;
use crate::channel::{Channel, ChannelError};
use crate::helpers::Volatile;
use crate::serve::Responder;
use dialog_capability::{
    Ability, Capability, Effect, Fork, ForkInvocation, Provider, SiteFork, Subject,
};
use dialog_common::Buffer;
use dialog_did_web::{CachingResolver, WebResolver};
use dialog_effects::Use;
use dialog_effects::archive::{self, ArchiveError};
use dialog_operator::helpers::test_operator_with_profile;
use iroh_base::{EndpointAddr, SecretKey};
use std::sync::Arc;

/// Hands the container straight to a responder, which is what a stream
/// would do with one more hop.
struct Loopback(Arc<Responder<Volatile, CachingResolver<WebResolver>>>);

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Channel for Loopback {
    async fn exchange(
        &self,
        _peer: &IrohAddress,
        request: Vec<u8>,
    ) -> Result<Vec<u8>, ChannelError> {
        let response = self.0.answer(&request).await.without_stream();
        Ok(crate::wire::encode("response", &response).expect("a response encodes"))
    }
}

fn peer() -> IrohAddress {
    // Routes are empty: what a peer *is* is its endpoint id, and this
    // exchange never dials.
    IrohAddress::from(EndpointAddr::from(SecretKey::generate().public()))
}

/// Drive one effect the whole way and hand back both the outcome and the
/// store, so a test can assert on what actually happened rather than
/// only on what was returned.
async fn exchange<Fx>(
    build: impl FnOnce(&dialog_capability::Did) -> Capability<Fx>,
) -> (
    Fx::Output,
    Arc<Responder<Volatile, CachingResolver<WebResolver>>>,
)
where
    Fx: Effect + crate::carries::Carries + Clone + 'static,
    Fx::Of: dialog_capability::Constraint,
    Capability<Fx>: Ability,
    Iroh: Provider<dialog_capability::ForkInvocation<Iroh, Fx>>,
    IrohFork<Fx>: SiteFork<
            dialog_operator::Operator<dialog_storage::provider::storage::VolatileSpace>,
            Site = Iroh,
            Effect = Fx,
        >,
{
    let (operator, profile) = test_operator_with_profile().await;
    let responder = Arc::new(Responder::new(
        Volatile::default(),
        CachingResolver::new(WebResolver::new()),
    ));
    let site = Iroh::new(Loopback(responder.clone()));

    // The subject is the profile the operator actually holds authority
    // for. Any other DID is a subject it cannot prove, which is the
    // point of the check and not something a test should route around.
    let fork: IrohFork<Fx> = Fork::<Iroh, Fx>::new(build(&profile.did()), peer()).into();
    let invocation = fork
        .authorize(&operator)
        .await
        .expect("the operator holds a powerline delegation");
    (
        Provider::<dialog_capability::ForkInvocation<Iroh, Fx>>::execute(&site, invocation).await,
        responder,
    )
}

fn put_of(subject: &dialog_capability::Did, bytes: &[u8]) -> Capability<archive::Put> {
    Subject::from(subject.clone())
        .attenuate(Use)
        .attenuate(archive::Archive)
        .attenuate(archive::Catalog::new("blocks"))
        .invoke(archive::Put::new(Buffer::from(bytes.to_vec())))
}

#[dialog_common::test]
async fn a_put_signed_here_is_performed_there() {
    let bytes = b"a block that went the whole way".to_vec();
    let carried = bytes.clone();

    let (outcome, responder) = exchange(move |subject| put_of(subject, &carried)).await;
    outcome.expect("the peer performs the put");

    assert_eq!(
        responder
            .store()
            .get(Buffer::from(bytes.clone()).blake3_hash()),
        Some(bytes),
        "the peer stored the block the client signed for"
    );
}

/// A get comes back typed, which is the half a refusal-only test would
/// miss: the output crossed the wire as bytes and decoded into the
/// effect's own `Result`.
#[dialog_common::test]
async fn a_get_that_finds_nothing_is_an_answer() {
    let (outcome, _) = exchange(|subject| {
        Subject::from(subject.clone())
            .attenuate(Use)
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("blocks"))
            .invoke(archive::Get::new(
                dialog_effects::archive::Blake3Hash::from([4u8; 32]),
            ))
    })
    .await;
    assert_eq!(
        outcome.expect("a miss is a successful get"),
        None,
        "a block that is not there is None, not an error"
    );
}

/// An effect failure is the peer's answer, not a transport problem, and
/// must arrive as the effect's own error rather than as a refusal.
#[dialog_common::test]
async fn an_effect_failure_arrives_as_that_effect_failing() {
    let (outcome, _) = exchange(|subject| {
        Subject::from(subject.clone())
            .attenuate(Use)
            .attenuate(dialog_effects::memory::Memory)
            .attenuate(dialog_effects::memory::Space::new("space"))
            .attenuate(dialog_effects::memory::Cell::new("cell"))
            .invoke(dialog_effects::memory::Resolve)
    })
    .await;
    match outcome {
        Err(dialog_effects::memory::MemoryError::Storage(detail)) => {
            assert!(
                detail.contains("blocks only"),
                "the peer's own words: {detail}"
            );
        }
        other => panic!("expected the store's failure to survive the trip, got {other:?}"),
    }
}

/// The type the archive answers with survives the round trip unchanged.
#[dialog_common::test]
async fn a_stored_block_reads_back() {
    let bytes = b"written, then read".to_vec();
    let digest = Buffer::from(bytes.clone()).blake3_hash().clone();

    let (operator, profile) = test_operator_with_profile().await;
    let subject = profile.did();
    let responder = Arc::new(Responder::new(
        Volatile::default(),
        CachingResolver::new(WebResolver::new()),
    ));
    let site = Iroh::new(Loopback(responder.clone()));

    for capability in [put_of(&subject, &bytes)] {
        let fork: IrohFork<archive::Put> = Fork::<Iroh, _>::new(capability, peer()).into();
        let invocation = fork.authorize(&operator).await.expect("authorized");
        let outcome: Result<(), ArchiveError> =
            Provider::<ForkInvocation<Iroh, archive::Put>>::execute(&site, invocation).await;
        outcome.expect("the put succeeds");
    }

    let read = Subject::from(subject)
        .attenuate(Use)
        .attenuate(archive::Archive)
        .attenuate(archive::Catalog::new("blocks"))
        .invoke(archive::Get::new(digest));
    let fork: IrohFork<archive::Get> = Fork::<Iroh, _>::new(read, peer()).into();
    let invocation = fork.authorize(&operator).await.expect("authorized");
    let found: Result<Option<Vec<u8>>, ArchiveError> =
        Provider::<ForkInvocation<Iroh, archive::Get>>::execute(&site, invocation).await;

    assert_eq!(found.expect("the get succeeds"), Some(bytes));
}

/// A peer describing itself, which is the first thing a client needs
/// and the smallest whole exchange this protocol supports: no payload
/// out, a typed answer back, and the invocation signed and verified
/// like any other.
#[dialog_common::test]
async fn a_peer_says_who_it_is() {
    let (operator, profile) = test_operator_with_profile().await;
    let subject = profile.did();
    let responder = Arc::new(Responder::new(
        Volatile::default(),
        CachingResolver::new(WebResolver::new()),
    ));
    let site = Iroh::new(Loopback(responder.clone()));

    let hello = Subject::from(subject.clone())
        .attenuate(Use)
        .attenuate(dialog_effects::peer::Peer)
        .attenuate(dialog_effects::peer::Hello);
    let fork: IrohFork<dialog_effects::peer::Hello> = Fork::<Iroh, _>::new(hello, peer()).into();
    let invocation = fork.authorize(&operator).await.expect("authorized");

    let greeting =
        Provider::<ForkInvocation<Iroh, dialog_effects::peer::Hello>>::execute(&site, invocation)
            .await
            .expect("the peer describes itself");

    assert_eq!(
        greeting.subject, subject,
        "the answer names the subject that was asked about, not one the peer chose"
    );
    assert!(greeting.profile.to_string().starts_with("did:"));
    assert!(greeting.operator.to_string().starts_with("did:"));
}

/// The other half of self-description: what a peer holds, asked for by
/// a caller that holds no authority over any of it.
///
/// The offers are seeded rather than derived so the assertion can be
/// about the values themselves. A store answering with an empty list
/// would satisfy a test that only checked the call succeeded, and that
/// is exactly the failure — the effect reaching a provider that has
/// nothing to say — this has to be able to catch.
#[dialog_common::test]
async fn a_peer_says_which_spaces_it_holds() {
    use dialog_effects::peer::Offer;

    let (operator, profile) = test_operator_with_profile().await;
    let subject = profile.did();

    let offered = vec![
        Offer {
            subject: dialog_capability::did!("key:zSpaceOne"),
            name: Some("notes".into()),
        },
        Offer {
            subject: dialog_capability::did!("key:zSpaceTwo"),
            name: None,
        },
    ];

    let responder = Arc::new(Responder::new(
        Volatile::default().offering(offered.clone()),
        CachingResolver::new(WebResolver::new()),
    ));
    let site = Iroh::new(Loopback(responder.clone()));

    let ask = Subject::from(subject)
        .attenuate(Use)
        .attenuate(dialog_effects::peer::Peer)
        .attenuate(dialog_effects::peer::Spaces);
    let fork: IrohFork<dialog_effects::peer::Spaces> = Fork::<Iroh, _>::new(ask, peer()).into();
    let invocation = fork.authorize(&operator).await.expect("authorized");

    let held =
        Provider::<ForkInvocation<Iroh, dialog_effects::peer::Spaces>>::execute(&site, invocation)
            .await
            .expect("the peer lists what it holds");

    assert_eq!(
        held, offered,
        "every space came back as it was offered, name and all"
    );
}

/// Connecting is lazy and happens once.
///
/// The property the browser depends on. Its worker builds the site at
/// startup, when no page has opened a carrier and there is no channel to
/// hand over; if the site connected eagerly it would have nothing to
/// connect to, and if it reconnected per exchange each one would bind a
/// fresh endpoint — a different peer every time, and a stranger to
/// anything the last one spoke to.
#[dialog_common::test]
async fn a_site_connects_once_and_not_before_it_must() {
    use crate::channel::{ChannelError, Connect};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingConnect {
        responder: Arc<Responder<Volatile, CachingResolver<WebResolver>>>,
        connects: Arc<AtomicUsize>,
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Connect for CountingConnect {
        async fn connect(&self) -> Result<Arc<dyn crate::channel::Channel>, ChannelError> {
            self.connects.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(Loopback(self.responder.clone())))
        }
    }

    let (operator, profile) = test_operator_with_profile().await;
    let subject = profile.did();
    let responder = Arc::new(Responder::new(
        Volatile::default(),
        CachingResolver::new(WebResolver::new()),
    ));
    let connects = Arc::new(AtomicUsize::new(0));
    let site = Iroh::connecting(CountingConnect {
        responder,
        connects: connects.clone(),
    });

    assert_eq!(
        connects.load(Ordering::SeqCst),
        0,
        "building a site must not connect: in a browser there is nothing to connect to yet"
    );

    for _ in 0..3 {
        let hello = Subject::from(subject.clone())
            .attenuate(Use)
            .attenuate(dialog_effects::peer::Peer)
            .attenuate(dialog_effects::peer::Hello);
        let fork: IrohFork<dialog_effects::peer::Hello> =
            Fork::<Iroh, _>::new(hello, peer()).into();
        let invocation = fork.authorize(&operator).await.expect("authorized");
        Provider::<ForkInvocation<Iroh, dialog_effects::peer::Hello>>::execute(&site, invocation)
            .await
            .expect("the peer answers");
    }

    assert_eq!(
        connects.load(Ordering::SeqCst),
        1,
        "three exchanges, one endpoint"
    );
}

/// A connect that failed is not remembered.
///
/// "No page has dialed yet" is the ordinary state before anyone tries,
/// and the one condition guaranteed to stop being true without anything
/// being rebuilt. A site that cached the failure would refuse for the
/// life of the process precisely when the carrier had just arrived.
#[dialog_common::test]
async fn a_site_that_could_not_connect_tries_again() {
    use crate::channel::{ChannelError, Connect};
    use std::sync::Mutex as StdMutex;

    struct EventuallyReady {
        responder: Arc<Responder<Volatile, CachingResolver<WebResolver>>>,
        refusals_left: StdMutex<usize>,
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Connect for EventuallyReady {
        async fn connect(&self) -> Result<Arc<dyn crate::channel::Channel>, ChannelError> {
            let mut left = self.refusals_left.lock().expect("not poisoned");
            if *left > 0 {
                *left -= 1;
                return Err(ChannelError::Unreachable {
                    peer: "nobody".into(),
                    detail: "no carrier yet".into(),
                });
            }
            Ok(Arc::new(Loopback(self.responder.clone())))
        }
    }

    let (operator, profile) = test_operator_with_profile().await;
    let subject = profile.did();
    let responder = Arc::new(Responder::new(
        Volatile::default(),
        CachingResolver::new(WebResolver::new()),
    ));
    let site = Iroh::connecting(EventuallyReady {
        responder,
        refusals_left: StdMutex::new(1),
    });

    let ask = || {
        let hello = Subject::from(subject.clone())
            .attenuate(Use)
            .attenuate(dialog_effects::peer::Peer)
            .attenuate(dialog_effects::peer::Hello);
        Fork::<Iroh, _>::new(hello, peer())
    };

    let first: IrohFork<dialog_effects::peer::Hello> = ask().into();
    let invocation = first.authorize(&operator).await.expect("authorized");
    Provider::<ForkInvocation<Iroh, dialog_effects::peer::Hello>>::execute(&site, invocation)
        .await
        .expect_err("the first exchange has no carrier to ride");

    let second: IrohFork<dialog_effects::peer::Hello> = ask().into();
    let invocation = second.authorize(&operator).await.expect("authorized");
    Provider::<ForkInvocation<Iroh, dialog_effects::peer::Hello>>::execute(&site, invocation)
        .await
        .expect("the carrier arrived, so the second connects");
}
