//! Sealed lines across a remote: what a push puts on an S3 remote, and
//! what a pull from it reads back.
//!
//! These need `--features integration-tests` (a local S3 server is
//! provisioned natively); under `web-integration-tests` the same bodies
//! run their client in the browser against it.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::Result;
use dialog_artifacts::{Artifact, ArtifactSelector, Asset, Instruction, Value};
use dialog_capability::{Fork, Provider, Subject};
use dialog_common::{Blake3Hash, ConditionalSync};
use dialog_effects::MethodExt as _;
use dialog_effects::archive::Get;
use dialog_effects::archive::prelude::{ArchiveExt as _, CatalogExt as _, GetBlockExt as _};
use dialog_effects::blob::prelude::{ArchiveBlobExt as _, ReadBlobExt as _};
use dialog_effects::blob::{BlobError, ByteRange, Read as BlobRead};
use dialog_keyring::EpochId;
use dialog_keyring::layered::asset::CHUNK;
use dialog_keyring::layered::{Access, Envelope, Level, LevelSecret, StructureKey, Writer};
use dialog_peer::helpers::{test_session_with_peer, unique_name};
use dialog_remote_s3::helpers::S3Address;
use dialog_remote_s3::{Address as S3SiteAddress, S3Credential};
use dialog_search_tree::Manifest;
use futures_util::{StreamExt, stream};

use super::{TreeSpace, writer_space};
use crate::helpers::connect;
use crate::repository::archive::local::read_all;
use crate::repository::source::SourceRef;
use crate::{Blob, ConnectedReplica, RemoteSite, RepositoryExt as _, Revision, recorded_sealed};

fn secret(tag: u8) -> LevelSecret {
    LevelSecret::new(EpochId::from([tag; 32]), [tag.wrapping_mul(31); 32])
}

fn member() -> Access {
    Access::content(Level::new().with(secret(1)), Level::new().with(secret(2)))
}

fn writer() -> TreeSpace {
    writer_space(Writer::new(secret(1), secret(2)), member())
}

fn site(s3: &S3Address) -> S3SiteAddress {
    S3SiteAddress::builder(&s3.endpoint)
        .region("us-east-1")
        .bucket(&s3.bucket)
        .build()
        .expect("a site address")
}

/// A value long enough to spill out of its leaf.
fn spilling(marker: &str) -> String {
    let inline = Manifest::default().inline_n as usize;
    format!("{marker}{}", "x".repeat(inline + 16))
}

fn facts(marker: &str) -> Result<Vec<Instruction>> {
    Ok(vec![
        Instruction::Assert(Artifact {
            the: "note/title".parse()?,
            of: "note:1".parse()?,
            is: Value::String(format!("{marker}-inline")),
            cause: None,
        }),
        Instruction::Assert(Artifact {
            the: "note/body".parse()?,
            of: "note:1".parse()?,
            is: Value::String(spilling(marker)),
            cause: None,
        }),
    ])
}

/// Every envelope and sealed value the remote holds for the sealed tree
/// `head` names, reached as a replicator reaches it: from the head's
/// structure key alone, checking each block against its address.
async fn remote_closure<Env>(
    remote: &ConnectedReplica,
    head: &Revision,
    env: &Env,
) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>)>
where
    Env: Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, BlobRead>>
        + ConditionalSync
        + 'static,
{
    let sealed = head.sealed.clone().expect("a sealed head");
    let connection = remote.connection(env);
    let mut envelopes = Vec::new();
    let mut values = Vec::new();
    let mut pending = vec![(
        Blake3Hash::from(sealed.address),
        StructureKey::from_bytes(sealed.structure),
    )];
    while let Some((address, key)) = pending.pop() {
        let bytes = Subject::from(remote.did())
            .reader()
            .archive()
            .catalog("index")
            .get(address.clone())
            .perform(&connection)
            .await?
            .expect("the remote holds every envelope the head reaches");
        assert_eq!(Blake3Hash::hash(&bytes), address);
        let envelope = Envelope::from_bytes(&bytes)?;
        pending.extend(envelope.children(&key)?);
        for value in envelope.attachments(&key)? {
            let reader = Subject::from(remote.did())
                .reader()
                .archive()
                .blob()
                .read(value.clone())
                .perform(&connection)
                .await?;
            let bytes = read_all(reader).await?;
            assert_eq!(Blake3Hash::hash(&bytes), value);
            values.push(bytes);
        }
        envelopes.push(bytes);
    }
    Ok((envelopes, values))
}

fn holds(bytes: &[u8], marker: &str) -> bool {
    bytes
        .windows(marker.len())
        .any(|window| window == marker.as_bytes())
}

/// A sealed branch pushed to a remote and pulled by another replica
/// holding the keys reads back whole, inline and spilled values alike,
/// while the remote holds only envelopes and sealed values: walked from
/// the head as a replicator walks it, none carries the values in the
/// clear, and nothing is stored under the root's plaintext identity.
#[dialog_common::test]
async fn it_pushes_and_pulls_a_sealed_branch_through_a_remote(s3: S3Address) -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let marker = unique_name("sealed-remote");

    let alice = profile
        .space(unique_name("alice"))
        .create()
        .perform(&operator)
        .await?;
    profile
        .secrets()
        .site(site(&s3))
        .save(S3Credential::new(&s3.access_key_id, &s3.secret_access_key))
        .perform(&profile)
        .await?;
    let origin = connect("origin", site(&s3), alice.did(), &operator).await?;
    let branch = alice
        .branch("main")
        .open()
        .sealed(writer())
        .perform(&operator)
        .await?;
    let upstream = origin.branch("main").open().perform(&operator).await?;
    branch.set_upstream(upstream).perform(&operator).await?;

    branch
        .commit(stream::iter(facts(&marker)?))
        .perform(&operator)
        .await?;
    let pushed = branch.push().perform(&operator).await?.expect("a push");
    assert!(pushed.sealed.is_some());

    let (envelopes, values) = remote_closure(&origin, &pushed, &operator).await?;
    assert!(envelopes.len() > 1, "expected more than a root");
    assert!(!values.is_empty(), "the spilled value crossed sealed");
    for bytes in envelopes.iter().chain(&values) {
        assert!(
            !holds(bytes, &marker),
            "a value reached the remote in the clear"
        );
    }
    let plaintext_root = Subject::from(origin.did())
        .reader()
        .archive()
        .catalog("index")
        .get(Blake3Hash::from(*pushed.tree.hash()))
        .perform(&origin.connection(&operator))
        .await?;
    assert!(
        plaintext_root.is_none(),
        "the remote is addressable by plaintext identity"
    );

    // Another replica of the same space, holding the keys, pulls and reads.
    let bob = profile
        .space(unique_name("bob"))
        .open()
        .perform(&operator)
        .await?;
    let origin_for_bob = connect("origin", site(&s3), alice.did(), &operator).await?;
    let bob_branch = bob
        .branch("main")
        .open()
        .sealed(writer())
        .perform(&operator)
        .await?;
    let upstream = origin_for_bob
        .branch("main")
        .open()
        .perform(&operator)
        .await?;
    bob_branch.set_upstream(upstream).perform(&operator).await?;
    let pulled = bob_branch.pull().perform(&operator).await?;
    assert!(pulled.is_some(), "Bob's pull found Alice's head");

    let read: Vec<Artifact> = bob_branch
        .claims()
        .select(ArtifactSelector::new().of("note:1".parse()?))
        .to_owned()
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;
    let mut values: Vec<_> = read
        .iter()
        .map(|fact| (fact.the.to_string(), fact.is.clone()))
        .collect();
    values.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        values,
        vec![
            ("note/body".to_string(), Value::String(spilling(&marker))),
            (
                "note/title".to_string(),
                Value::String(format!("{marker}-inline"))
            ),
        ]
    );
    Ok(())
}

/// The notes on `branch`, by entity and attribute.
macro_rules! notes {
    ($branch:expr, $operator:expr) => {{
        let read: Vec<Artifact> = $branch
            .claims()
            .select(ArtifactSelector::new().the("note/title".parse()?))
            .to_owned()
            .perform($operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        let mut notes: Vec<_> = read
            .iter()
            .map(|fact| (fact.of.to_string(), fact.is.clone()))
            .collect();
        notes.sort_by(|a, b| a.0.cmp(&b.0));
        notes
    }};
}

fn note(entity: &str, title: &str) -> Result<Instruction> {
    Ok(Instruction::Assert(Artifact {
        the: "note/title".parse()?,
        of: entity.parse()?,
        is: Value::String(title.to_string()),
        cause: None,
    }))
}

/// Two replicas of a sealed line edit concurrently and sync through a
/// remote: the second push from each side diffs against its sealed sync
/// point, the pull that meets a concurrent edit merges and seals what it
/// mints, and both sides converge on every note. What crosses stays
/// sealed throughout.
#[dialog_common::test]
async fn it_merges_concurrent_sealed_edits_through_a_remote(s3: S3Address) -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let marker = unique_name("sealed-merge");
    profile
        .secrets()
        .site(site(&s3))
        .save(S3Credential::new(&s3.access_key_id, &s3.secret_access_key))
        .perform(&profile)
        .await?;

    let alice = profile
        .space(unique_name("alice"))
        .create()
        .perform(&operator)
        .await?;
    let origin = connect("origin", site(&s3), alice.did(), &operator).await?;
    let alice_branch = alice
        .branch("main")
        .open()
        .sealed(writer())
        .perform(&operator)
        .await?;
    let upstream = origin.branch("main").open().perform(&operator).await?;
    alice_branch
        .set_upstream(upstream)
        .perform(&operator)
        .await?;
    alice_branch
        .commit(stream::iter(vec![note("note:a", &format!("{marker}-a"))?]))
        .perform(&operator)
        .await?;
    alice_branch.push().perform(&operator).await?;

    let bob = profile
        .space(unique_name("bob"))
        .open()
        .perform(&operator)
        .await?;
    let origin_for_bob = connect("origin", site(&s3), alice.did(), &operator).await?;
    let bob_branch = bob
        .branch("main")
        .open()
        .sealed(writer())
        .perform(&operator)
        .await?;
    let upstream = origin_for_bob
        .branch("main")
        .open()
        .perform(&operator)
        .await?;
    bob_branch.set_upstream(upstream).perform(&operator).await?;
    bob_branch.pull().perform(&operator).await?;

    // Concurrent edits: Alice pushes first, Bob merges hers into his.
    alice_branch
        .commit(stream::iter(vec![note("note:b", &format!("{marker}-b"))?]))
        .perform(&operator)
        .await?;
    let theirs = alice_branch
        .push()
        .perform(&operator)
        .await?
        .expect("a push");
    bob_branch
        .commit(stream::iter(vec![note("note:c", &format!("{marker}-c"))?]))
        .perform(&operator)
        .await?;
    let ours = bob_branch.revision().expect("a head");
    let merged = bob_branch
        .pull()
        .perform(&operator)
        .await?
        .expect("a merged head");
    assert!(
        merged.tree != theirs.tree && merged.tree != ours.tree,
        "the pull minted a merge rather than adopting either side"
    );
    assert!(merged.sealed.is_some(), "the merge sealed what it minted");
    let pushed = bob_branch.push().perform(&operator).await?.expect("a push");

    alice_branch.pull().perform(&operator).await?;
    let expected: Vec<_> = ["a", "b", "c"]
        .iter()
        .map(|tag| {
            (
                format!("note:{tag}"),
                Value::String(format!("{marker}-{tag}")),
            )
        })
        .collect();
    assert_eq!(notes!(alice_branch, &operator), expected);
    assert_eq!(notes!(bob_branch, &operator), expected);

    let (envelopes, _) = remote_closure(&origin, &pushed, &operator).await?;
    for bytes in &envelopes {
        assert!(
            !holds(bytes, &marker),
            "a note reached the remote in the clear"
        );
    }
    Ok(())
}

/// The bytes `remote`'s blob store holds under `digest`, or `None`.
async fn remote_blob<Env>(
    remote: &ConnectedReplica,
    digest: [u8; 32],
    env: &Env,
) -> Result<Option<Vec<u8>>>
where
    Env: Provider<Fork<RemoteSite, BlobRead>> + ConditionalSync + 'static,
{
    let read = Subject::from(remote.did())
        .reader()
        .archive()
        .blob()
        .read(Blake3Hash::from(digest))
        .perform(&remote.connection(env))
        .await;
    match read {
        Ok(reader) => Ok(Some(read_all(reader).await?)),
        Err(BlobError::NotFound(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// A sealed asset pushed to a remote crosses as its sealed copy only: the
/// remote holds nothing under the asset's plaintext hash, and the copy it
/// holds carries none of the plaintext. Another replica holding the keys
/// pulls, reads the asset (hydrating the copy and opening it), and
/// downloads the branch, after which the copy is held locally.
#[dialog_common::test]
async fn it_pushes_and_pulls_a_sealed_asset_through_a_remote(s3: S3Address) -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    profile
        .secrets()
        .site(site(&s3))
        .save(S3Credential::new(&s3.access_key_id, &s3.secret_access_key))
        .perform(&profile)
        .await?;

    let alice = profile
        .space(unique_name("alice"))
        .create()
        .perform(&operator)
        .await?;
    let origin = connect("origin", site(&s3), alice.did(), &operator).await?;
    let branch = alice
        .branch("main")
        .open()
        .sealed(writer())
        .perform(&operator)
        .await?;
    let upstream = origin.branch("main").open().perform(&operator).await?;
    branch.set_upstream(upstream).perform(&operator).await?;

    let marker = unique_name("remote-asset");
    let mut bytes = Vec::new();
    while bytes.len() < 2 * CHUNK + 1000 {
        bytes.extend_from_slice(marker.as_bytes());
    }
    let asset = Asset::new(bytes.clone());
    branch
        .transaction()
        .assert(asset.clone())
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch.push().perform(&operator).await?.expect("a push");

    let (copy, _) = recorded_sealed(SourceRef::from(&branch), asset.hash(), &operator)
        .await?
        .expect("a sealed copy");
    assert_eq!(remote_blob(&origin, *asset.hash(), &operator).await?, None);
    let sealed = remote_blob(&origin, copy.address, &operator)
        .await?
        .expect("the remote holds the sealed copy");
    assert_eq!(sealed.len() as u64, copy.length);
    assert!(
        !holds(&sealed, &marker),
        "the asset reached the remote in the clear"
    );

    let bob = profile
        .space(unique_name("bob"))
        .open()
        .perform(&operator)
        .await?;
    let origin_for_bob = connect("origin", site(&s3), alice.did(), &operator).await?;
    let bob_branch = bob
        .branch("main")
        .open()
        .sealed(writer())
        .perform(&operator)
        .await?;
    let upstream = origin_for_bob
        .branch("main")
        .open()
        .perform(&operator)
        .await?;
    bob_branch.set_upstream(upstream).perform(&operator).await?;
    bob_branch.pull().perform(&operator).await?.expect("a pull");
    let before = bob_branch
        .archive()
        .blob()
        .read(Blake3Hash::from(copy.address))
        .perform(&operator)
        .await;
    assert!(
        matches!(before, Err(BlobError::NotFound(_))),
        "Bob held the sealed copy before reading it"
    );

    let read = Blob::from(asset.entity()?)
        .slice(ByteRange {
            offset: CHUNK as u64 - 3,
            length: Some(10),
        })
        .read((&bob_branch).into())
        .perform(&operator)
        .await?;
    assert_eq!(read_all(read).await?, &bytes[CHUNK - 3..CHUNK + 7]);

    bob_branch.download().perform(&operator).await?;
    let held = bob_branch
        .archive()
        .blob()
        .read(Blake3Hash::from(copy.address))
        .perform(&operator)
        .await;
    assert!(held.is_ok(), "the download left the sealed copy local");
    let whole = Blob::from(asset.entity()?)
        .read((&bob_branch).into())
        .perform(&operator)
        .await?;
    assert_eq!(read_all(whole).await?, bytes);
    Ok(())
}
