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
        let response = self.0.answer(&request).await;
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
