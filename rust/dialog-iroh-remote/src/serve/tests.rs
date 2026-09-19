//! End to end: a signed invocation goes in as container bytes, an
//! effect runs against a store, and its output comes back encoded.
//!
//! Nothing is stubbed on the authorization path — real delegations,
//! real signatures, real chain verification — because the thing worth
//! proving is that a peer performs exactly what was authorized and
//! nothing else.

use super::*;
use dialog_capability::{Ability, Principal};
use dialog_common::{Buffer, Checksum};
use dialog_credentials::Ed25519Signer;
use dialog_did_web::{CachingResolver, WebResolver};
use dialog_effects::archive::{ArchiveError, Blake3Hash};
use dialog_ucan::Scope;
use dialog_ucan_core::subject::Subject as DelegatedSubject;
use dialog_ucan_core::{DelegationBuilder, InvocationBuilder};
use std::collections::HashMap;
use std::sync::Mutex;

/// A store that records what it was asked to do.
#[derive(Default)]
struct Recording {
    blocks: Mutex<HashMap<Blake3Hash, Vec<u8>>>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<archive::Put> for Recording {
    async fn execute(&self, input: Capability<archive::Put>) -> Result<(), ArchiveError> {
        let block = &input.constraint.block;
        self.blocks
            .lock()
            .expect("not poisoned")
            .insert(block.blake3_hash().clone(), block.as_ref().to_vec());
        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<archive::Get> for Recording {
    async fn execute(
        &self,
        input: Capability<archive::Get>,
    ) -> Result<Option<Vec<u8>>, ArchiveError> {
        Ok(self
            .blocks
            .lock()
            .expect("not poisoned")
            .get(&input.constraint.digest)
            .cloned())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<archive::Import> for Recording {
    async fn execute(&self, input: Capability<archive::Import>) -> Result<(), ArchiveError> {
        let mut blocks = self.blocks.lock().expect("not poisoned");
        for block in &input.constraint.blocks {
            blocks.insert(block.blake3_hash().clone(), block.as_ref().to_vec());
        }
        Ok(())
    }
}

/// The memory effects are not exercised here; they exist so the store
/// satisfies [`Store`], which is what makes an unservable store a
/// compile error rather than a refusal.
macro_rules! unserved {
    ($effect:ty, $output:ty) => {
        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl Provider<$effect> for Recording {
            async fn execute(&self, _: Capability<$effect>) -> $output {
                Err(memory::MemoryError::Storage("not under test".into()))
            }
        }
    };
}
unserved!(
    memory::Resolve,
    Result<Option<memory::Edition<Vec<u8>>>, memory::MemoryError>
);
unserved!(memory::Publish, Result<memory::Version, memory::MemoryError>);
unserved!(memory::Retract, Result<(), memory::MemoryError>);

async fn signers() -> (Ed25519Signer, Ed25519Signer) {
    (
        Ed25519Signer::import(&[1u8; 32])
            .await
            .expect("subject key"),
        Ed25519Signer::import(&[2u8; 32])
            .await
            .expect("operator key"),
    )
}

/// Mint what a client would send: a delegation from the subject to the
/// operator, an invocation of `capability`, and the blocks its arguments
/// name, all in one `ctn-v1` container.
async fn request<Fx>(
    subject: &Ed25519Signer,
    operator: &Ed25519Signer,
    capability: &Capability<Fx>,
    blocks: Vec<Vec<u8>>,
) -> Vec<u8>
where
    Fx: dialog_capability::Effect + Clone,
    Fx::Of: dialog_capability::Constraint,
    Capability<Fx>: Ability,
{
    let subject_did = subject.did();
    // `Scope::invoke`, never `Scope::from`: the former projects payload
    // fields through `Attenuate`, so a block becomes a digest and a
    // checksum. `Scope::from` would inline the block into the signed
    // arguments, which is a different protocol.
    let scope = Scope::invoke(capability);
    let command = scope.command.segments().clone();

    let delegation = DelegationBuilder::new()
        .issuer(subject.clone())
        .audience(operator)
        .subject(DelegatedSubject::Specific(subject_did.clone()))
        .command(command.clone())
        .try_build()
        .await
        .expect("the delegation builds");
    let proof = delegation.to_cid();

    let invocation = InvocationBuilder::new()
        .issuer(operator.clone())
        .audience(&subject_did)
        .subject(&subject_did)
        .command(command)
        .arguments(scope.parameters.args())
        .proofs(vec![proof])
        .try_build()
        .await
        .expect("the invocation builds");

    let chain = InvocationChain::new(
        invocation,
        HashMap::from([(proof, std::sync::Arc::new(delegation))]),
    );
    let bundle = InvocationBundle::from_chain(&chain, blocks).expect("the bundle assembles");
    dialog_ucan_core::container::Container::from(&bundle)
        .into_bytes()
        .expect("the container encodes")
}

fn responder(store: Recording) -> Responder<Recording, CachingResolver<WebResolver>> {
    Responder::new(store, CachingResolver::new(WebResolver::new()))
}

fn put_of(subject: &Ed25519Signer, bytes: &[u8]) -> Capability<archive::Put> {
    Subject::from(subject.did())
        .attenuate(Use)
        .attenuate(archive::Archive)
        .attenuate(archive::Catalog::new("blocks"))
        .invoke(archive::Put::new(Buffer::from(bytes.to_vec())))
}

#[dialog_common::test]
async fn a_put_travels_and_is_stored() {
    let (subject, operator) = signers().await;
    let bytes = b"a block that crossed a wire".to_vec();
    let capability = put_of(&subject, &bytes);
    let container = request(&subject, &operator, &capability, vec![bytes.clone()]).await;

    let responder = responder(Recording::default());
    let response = responder.answer(&container).await;

    let Response::Performed(encoded) = response else {
        panic!("expected the effect to run, got {response:?}");
    };
    let outcome: Result<(), ArchiveError> =
        crate::wire::decode("output", &encoded).expect("the output decodes");
    outcome.expect("the put succeeds");

    let stored = responder.store.blocks.lock().expect("not poisoned");
    assert_eq!(
        stored.get(Buffer::from(bytes.clone()).blake3_hash()),
        Some(&bytes),
        "the block must be stored under its own hash"
    );
}

/// The attack `resolve::block` exists for, end to end: a well-formed,
/// correctly signed invocation whose carried bytes are not the block it
/// committed to.
#[dialog_common::test]
async fn a_put_carrying_the_wrong_block_is_refused() {
    let (subject, operator) = signers().await;
    let signed_for = b"the block the invocation names".to_vec();
    let capability = put_of(&subject, &signed_for);

    // Everything is honest except the freight.
    let container = request(
        &subject,
        &operator,
        &capability,
        vec![b"a different block entirely".to_vec()],
    )
    .await;

    let response = responder(Recording::default()).answer(&container).await;
    match response {
        // The substituted block is not at the address the signed
        // checksum names, so it is missing rather than wrong — which is
        // the same answer for the same reason.
        Response::Refused(Refusal::Malformed(_) | Refusal::Unauthorized(_)) => {}
        other => panic!("a block that was not signed for must not be stored, got {other:?}"),
    }
}

/// An invocation the subject never delegated must not perform, however
/// well formed it is.
#[dialog_common::test]
async fn an_unproven_invocation_is_refused() {
    let (subject, operator) = signers().await;
    let bytes = b"a block".to_vec();
    let capability = put_of(&subject, &bytes);
    let subject_did = subject.did();
    let scope = Scope::invoke(&capability);

    // No delegation at all: the operator simply asserts authority.
    let invocation = InvocationBuilder::new()
        .issuer(operator.clone())
        .audience(&subject_did)
        .subject(&subject_did)
        .command(scope.command.segments().clone())
        .arguments(scope.parameters.args())
        .proofs(vec![])
        .try_build()
        .await
        .expect("the invocation builds");
    let chain = InvocationChain::new(invocation, HashMap::new());
    let bundle = InvocationBundle::from_chain(&chain, vec![bytes]).expect("bundle assembles");
    let container = dialog_ucan_core::container::Container::from(&bundle)
        .into_bytes()
        .expect("container encodes");

    let store = Recording::default();
    let responder = responder(store);
    let response = responder.answer(&container).await;
    assert!(
        matches!(response, Response::Refused(Refusal::Unauthorized(_))),
        "an undelegated invocation must be refused, got {response:?}"
    );
    assert!(
        responder
            .store
            .blocks
            .lock()
            .expect("not poisoned")
            .is_empty(),
        "a refused invocation must not have stored anything"
    );
}

/// Blob effects answer with streams, so they are refused by name rather
/// than being quietly absent or faked into a buffered response.
#[dialog_common::test]
async fn a_blob_command_says_why_it_is_not_served() {
    let (subject, operator) = signers().await;
    let capability = Subject::from(subject.did())
        .attenuate(Use)
        .attenuate(archive::Archive)
        .attenuate(dialog_effects::blob::Blob)
        .invoke(dialog_effects::blob::Read::new(Blake3Hash::from([9u8; 32])));
    let container = request(&subject, &operator, &capability, vec![]).await;

    let response = responder(Recording::default()).answer(&container).await;
    match response {
        Response::Refused(Refusal::UnknownCommand(reason)) => {
            assert!(
                reason.contains("stream"),
                "the refusal should say why: {reason}"
            );
        }
        other => panic!("expected a named refusal, got {other:?}"),
    }
}

/// Checksums are what address a block, so this is the one place the two
/// hashes must be kept straight.
#[dialog_common::test]
fn a_blocks_address_comes_from_its_sha256() {
    let bytes = b"block".as_slice();
    let capability = Subject::from(dialog_capability::did!("key:zSpace"))
        .attenuate(Use)
        .attenuate(archive::Archive)
        .attenuate(archive::Catalog::new("blocks"))
        .invoke(archive::Put::new(Buffer::from(bytes.to_vec())));
    let params = Scope::invoke(&capability).parameters;
    let params = params.as_map();
    assert!(
        params.contains_key("checksum") && params.contains_key("digest"),
        "an invocation commits to both hashes: {params:?}"
    );
    assert!(
        !params.contains_key("content"),
        "an invocation must not inline the block it commits to: {params:?}"
    );
    assert_eq!(
        params.get("checksum"),
        Some(&ipld_core::serde::to_ipld(Checksum::sha256(bytes)).unwrap()),
        "the signed checksum must be the sha256 that addresses the block"
    );
}
