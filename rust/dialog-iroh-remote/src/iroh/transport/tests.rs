//! End-to-end tests over real in-process iroh endpoints.
//!
//! Nodes bind with the `Minimal` preset (no relays, no external address
//! lookup) and connect via direct localhost addresses, so the tests are
//! fully hermetic.

use std::collections::HashMap;
use std::sync::Arc;

use dialog_capability::Principal;
use dialog_capability::{
    Ability, Capability, Constraint, Effect, ForkInvocation, Provider, Subject,
};
use dialog_common::{Blake3Hash, Buffer};
use dialog_credentials::Ed25519Signer;
use dialog_effects::archive::prelude::*;
use dialog_effects::blob::BlobSource as _;
use dialog_effects::blob::prelude::*;
use dialog_effects::memory::prelude::*;
use dialog_effects::{archive, blob, memory};
use dialog_storage::provider::Volatile;
use dialog_ucan::{Scope, UcanInvocation};
use dialog_ucan_core::{InvocationBuilder, InvocationChain};
use testresult::TestResult;

use super::request;
use crate::protocol::WireError;
use crate::{Iroh, IrohAuthorization, IrohNode, IrohRemoteError};

/// Sign a self-issued invocation (issuer *is* the subject) for the given
/// capability, returning the CBOR UCAN container the wire carries.
async fn sign<Fx>(signer: &Ed25519Signer, capability: &Capability<Fx>) -> UcanInvocation
where
    Fx: Effect + Clone,
    Fx::Of: Constraint,
    Capability<Fx>: Ability,
{
    let scope = Scope::invoke(capability);
    let subject = capability.subject().clone();
    let invocation = InvocationBuilder::new()
        .issuer(signer.clone())
        .audience(&subject)
        .subject(&subject)
        .command(scope.command.0.clone())
        .arguments(scope.args())
        .proofs(Vec::new())
        .try_build()
        .await
        .expect("invocation builds");
    let chain = InvocationChain::new(invocation, HashMap::new());
    UcanInvocation {
        chain: Box::new(chain),
        subject,
        ability: capability.ability().to_string(),
    }
}

async fn container<Fx>(signer: &Ed25519Signer, capability: &Capability<Fx>) -> Vec<u8>
where
    Fx: Effect + Clone,
    Fx::Of: Constraint,
    Capability<Fx>: Ability,
{
    sign(signer, capability)
        .await
        .to_bytes()
        .expect("container serializes")
}

/// A hermetic (relay-less) node hosting `subject` from `env`.
async fn host_node(
    subject: &dialog_capability::Did,
    env: Volatile,
) -> Result<Arc<IrohNode>, IrohRemoteError> {
    IrohNode::builder()
        .direct_only()
        .host(subject.clone(), env)
        .spawn()
        .await
}

/// A hermetic client-only node.
async fn client_node() -> Result<Arc<IrohNode>, IrohRemoteError> {
    IrohNode::builder().direct_only().spawn().await
}

#[tokio::test(flavor = "multi_thread")]
async fn it_roundtrips_archive_blocks_over_the_wire() -> TestResult {
    let signer = Ed25519Signer::import(&[7; 32]).await?;
    let subject = signer.did();

    let server = host_node(&subject, Volatile::new()).await?;
    let client = client_node().await?;
    let connection = client.connect(&server.address()).await?;

    let block = Buffer::from(b"hello dialog over iroh".as_slice());
    let digest = block.blake3_hash().clone();
    let index = subject.clone().archive().catalog("index");

    // Put a block.
    let put = index.clone().put(block.clone());
    let invocation = container(&signer, &put).await;
    let response = request::archive_put(&connection, invocation, block.as_ref().to_vec()).await?;
    assert!(response.is_ok(), "put failed: {response:?}");

    // Get it back.
    let get = index.clone().get(digest.clone());
    let invocation = container(&signer, &get).await;
    let fetched = request::archive_get(&connection, invocation).await??;
    assert_eq!(fetched.as_deref(), Some(block.as_ref()));

    // A missing block resolves to None.
    let get = index.clone().get(Blake3Hash::hash(b"absent"));
    let invocation = container(&signer, &get).await;
    let fetched = request::archive_get(&connection, invocation).await??;
    assert_eq!(fetched, None);

    // Batch import.
    let blocks = vec![
        Buffer::from(b"one".as_slice()),
        Buffer::from(b"two".as_slice()),
    ];
    let import = index.clone().import(blocks.clone());
    let invocation = container(&signer, &import).await;
    let bodies = blocks
        .iter()
        .map(|b| serde_bytes::ByteBuf::from(b.as_ref().to_vec()))
        .collect();
    let response = request::archive_import(&connection, invocation, bodies).await?;
    assert!(response.is_ok(), "import failed: {response:?}");

    let get = index.get(blocks[1].blake3_hash().clone());
    let invocation = container(&signer, &get).await;
    let fetched = request::archive_get(&connection, invocation).await??;
    assert_eq!(fetched.as_deref(), Some(b"two".as_slice()));

    server.shutdown().await;
    client.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn it_publishes_and_resolves_memory_cells_with_cas() -> TestResult {
    let signer = Ed25519Signer::import(&[8; 32]).await?;
    let subject = signer.did();

    let server = host_node(&subject, Volatile::new()).await?;
    let client = client_node().await?;
    let connection = client.connect(&server.address()).await?;

    let cell = subject.clone().memory().space("main").cell("head");

    // Empty cell resolves to None.
    let resolve = cell.clone().resolve();
    let invocation = container(&signer, &resolve).await;
    let edition = request::memory_resolve(&connection, invocation).await??;
    assert!(edition.is_none());

    // First publish (no precondition).
    let publish = cell.clone().publish(b"revision-1".to_vec(), None);
    let invocation = container(&signer, &publish).await;
    let version =
        request::memory_publish(&connection, invocation, b"revision-1".to_vec()).await??;

    // Resolve returns the published edition.
    let resolve = cell.clone().resolve();
    let invocation = container(&signer, &resolve).await;
    let edition = request::memory_resolve(&connection, invocation)
        .await??
        .expect("cell has content");
    assert_eq!(edition.content, b"revision-1".to_vec());
    assert_eq!(edition.version, version);

    // CAS publish with the right precondition succeeds.
    let publish = cell
        .clone()
        .publish(b"revision-2".to_vec(), Some(version.clone()));
    let invocation = container(&signer, &publish).await;
    let version =
        request::memory_publish(&connection, invocation, b"revision-2".to_vec()).await??;

    // CAS publish with a stale precondition surfaces VersionMismatch
    // structurally across the wire.
    let publish = cell
        .clone()
        .publish(b"revision-3".to_vec(), Some(memory::Version::from("stale")));
    let invocation = container(&signer, &publish).await;
    let result = request::memory_publish(&connection, invocation, b"revision-3".to_vec()).await?;
    match result {
        Err(WireError::VersionMismatch { actual, .. }) => {
            assert_eq!(actual, Some(version.clone()));
        }
        other => panic!("expected version mismatch, got {other:?}"),
    }

    // Retract with the current version.
    let retract = cell.clone().retract(version);
    let invocation = container(&signer, &retract).await;
    request::memory_retract(&connection, invocation).await??;

    let resolve = cell.resolve();
    let invocation = container(&signer, &resolve).await;
    let edition = request::memory_resolve(&connection, invocation).await??;
    assert!(edition.is_none());

    server.shutdown().await;
    client.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn it_streams_blobs_over_the_wire() -> TestResult {
    let signer = Ed25519Signer::import(&[9; 32]).await?;
    let subject = signer.did();

    let server = host_node(&subject, Volatile::new()).await?;
    let client = client_node().await?;
    let connection = client.connect(&server.address()).await?;

    // 3 MiB of patterned bytes: crosses several chunk frames.
    let payload: Vec<u8> = (0..3 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let digest = Blake3Hash::hash(&payload);

    // Import the blob through the streaming sink.
    let import = subject
        .clone()
        .archive()
        .blob()
        .import(digest.clone(), payload.len() as u64);
    let invocation = container(&signer, &import).await;
    let mut sink: blob::BlobWriter = Box::new(request::blob_import(&connection, invocation).await?);
    sink.write_all(&payload).await?;
    let committed = sink.finish().await?;
    assert_eq!(committed, digest);

    // Read it back through the streaming source.
    let read = subject.clone().archive().blob().read(digest.clone());
    let invocation = container(&signer, &read).await;
    let mut source = request::blob_read(&connection, invocation)
        .await?
        .expect("read starts");
    let mut fetched = Vec::new();
    while let Some(chunk) = source.next().await? {
        fetched.extend_from_slice(&chunk);
    }
    assert_eq!(fetched, payload);

    server.shutdown().await;
    client.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn it_denies_unauthorized_and_unserved_requests() -> TestResult {
    let signer = Ed25519Signer::import(&[10; 32]).await?;
    let subject = signer.did();

    let server = host_node(&subject, Volatile::new()).await?;
    let client = client_node().await?;
    let connection = client.connect(&server.address()).await?;

    let index = subject.clone().archive().catalog("index");

    // An invocation signed by a key that is not the subject (and holds no
    // delegation) is denied.
    let stranger = Ed25519Signer::import(&[11; 32]).await?;
    let get = index.clone().get(Blake3Hash::hash(b"whatever"));
    let scope = Scope::invoke(&get);
    let invocation = InvocationBuilder::new()
        .issuer(stranger.clone())
        .audience(&subject)
        .subject(&subject)
        .command(scope.command.0.clone())
        .arguments(scope.args())
        .proofs(Vec::new())
        .try_build()
        .await?;
    let bytes = InvocationChain::new(invocation, HashMap::new()).to_bytes()?;
    let response = request::archive_get(&connection, bytes).await?;
    assert!(
        matches!(response, Err(WireError::Denied(_))),
        "expected denial, got {response:?}"
    );

    // A subject this peer does not serve is rejected, even when validly
    // signed.
    let other = Ed25519Signer::import(&[12; 32]).await?;
    let foreign = other
        .did()
        .archive()
        .catalog("index")
        .get(Blake3Hash::hash(b"whatever"));
    let invocation = container(&other, &foreign).await;
    let response = request::archive_get(&connection, invocation).await?;
    assert!(
        matches!(response, Err(WireError::Rejected(_))),
        "expected rejection, got {response:?}"
    );

    // A body that does not match the signed digest is rejected: sign a put
    // for one block, send different bytes.
    let put = index.put(Buffer::from(b"signed content".as_slice()));
    let invocation = container(&signer, &put).await;
    let response =
        request::archive_put(&connection, invocation, b"tampered content".to_vec()).await?;
    assert!(
        matches!(response, Err(WireError::Rejected(_))),
        "expected tamper rejection, got {response:?}"
    );

    server.shutdown().await;
    client.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn it_fetches_blocks_from_the_swarm() -> TestResult {
    let signer = Ed25519Signer::import(&[13; 32]).await?;
    let subject = signer.did();

    // Peer A replicates the space and holds the block.
    let storage_a = Volatile::new();
    let block = Buffer::from(b"gossip block".as_slice());
    let digest = block.blake3_hash().clone();
    Provider::<archive::Put>::execute(
        &storage_a,
        Subject::from(subject.clone())
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("index"))
            .invoke(archive::Put::new(block.clone())),
    )
    .await?;
    let node_a = host_node(&subject, storage_a).await?;

    // Peer B joins the swarm via A but holds nothing.
    let node_b = client_node().await?;

    let swarm_a = node_a.join_swarm(&subject, Vec::new()).await?;
    let swarm_b = node_b.join_swarm(&subject, vec![node_a.address()]).await?;
    swarm_a.joined().await;
    swarm_b.joined().await;

    // B broadcasts Want, A answers Have, B fetches from A directly with
    // the same subject-rooted invocation.
    let get = subject
        .clone()
        .archive()
        .catalog("index")
        .get(digest.clone());
    let invocation = container(&signer, &get).await;
    let fetched = swarm_b.fetch(&node_b, "index", &digest, &invocation).await;
    assert_eq!(
        fetched.as_deref(),
        Some(block.as_ref()),
        "swarm fetch should produce the block"
    );

    node_a.shutdown().await;
    node_b.shutdown().await;
    Ok(())
}

/// Exercises the public provider path — `ForkInvocation<Iroh, Fx>` executed
/// against the [`Iroh`] site — including the gossip fallback when the
/// addressed remote misses. Uses the process-global node, so all
/// global-node scenarios live in this one test.
#[tokio::test(flavor = "multi_thread")]
async fn it_executes_fork_invocations_with_swarm_fallback() -> TestResult {
    let signer = Ed25519Signer::import(&[14; 32]).await?;
    let subject = signer.did();

    // Peer A replicates the space and holds the block.
    let storage_a = Volatile::new();
    let block = Buffer::from(b"only A has this".as_slice());
    let digest = block.blake3_hash().clone();
    Provider::<archive::Put>::execute(
        &storage_a,
        Subject::from(subject.clone())
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("index"))
            .invoke(archive::Put::new(block.clone())),
    )
    .await?;
    let node_a = host_node(&subject, storage_a).await?;

    // Peer C also serves the space but has an empty replica: the addressed
    // remote that will miss.
    let node_c = host_node(&subject, Volatile::new()).await?;

    // The process-global client node, hermetic for tests.
    let client = client_node().await?;
    crate::install(client.clone())?;

    let swarm_a = node_a.join_swarm(&subject, Vec::new()).await?;
    let swarm_client = client.join_swarm(&subject, vec![node_a.address()]).await?;
    swarm_a.joined().await;
    swarm_client.joined().await;

    let index = subject.clone().archive().catalog("index");

    // Direct hit through the provider path against A.
    let get = index.clone().get(digest.clone());
    let authorization = IrohAuthorization::new(sign(&signer, &get).await);
    let fetched = ForkInvocation::<Iroh, archive::Get>::new(get, node_a.address(), authorization)
        .perform(&Iroh)
        .await?;
    assert_eq!(fetched.as_deref(), Some(block.as_ref()));

    // Addressed at C (which misses), the provider falls back to the swarm
    // and gets the block from A.
    let get = index.clone().get(digest.clone());
    let authorization = IrohAuthorization::new(sign(&signer, &get).await);
    let fetched = ForkInvocation::<Iroh, archive::Get>::new(get, node_c.address(), authorization)
        .perform(&Iroh)
        .await?;
    assert_eq!(
        fetched.as_deref(),
        Some(block.as_ref()),
        "provider should fall back to the swarm on a miss"
    );

    // A publish through the provider path (against A) works end to end.
    let cell = subject.clone().memory().space("main").cell("head");
    let publish = cell.clone().publish(b"head-1".to_vec(), None);
    let authorization = IrohAuthorization::new(sign(&signer, &publish).await);
    let version =
        ForkInvocation::<Iroh, memory::Publish>::new(publish, node_a.address(), authorization)
            .perform(&Iroh)
            .await?;

    let resolve = cell.resolve();
    let authorization = IrohAuthorization::new(sign(&signer, &resolve).await);
    let edition =
        ForkInvocation::<Iroh, memory::Resolve>::new(resolve, node_a.address(), authorization)
            .perform(&Iroh)
            .await?
            .expect("cell has content");
    assert_eq!(edition.content, b"head-1".to_vec());
    assert_eq!(edition.version, version);

    node_a.shutdown().await;
    node_c.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn it_derives_stable_swarm_topics() {
    let a: dialog_capability::Did = "did:key:z6MkTest".parse().unwrap();
    let b: dialog_capability::Did = "did:key:z6MkOther".parse().unwrap();
    assert_eq!(super::swarm::topic_for(&a), super::swarm::topic_for(&a));
    assert_ne!(super::swarm::topic_for(&a), super::swarm::topic_for(&b));
}
