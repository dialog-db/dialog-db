#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use super::*;
use dialog_capability::Subject;
use dialog_capability::access::{Authorization as _, TimeRange};
use dialog_credentials::{Ed25519Signer, Signer};
use dialog_effects::memory::prelude::CellScope;
use dialog_ucan::Scope;
use dialog_ucan_core::promise::Promised;
use dialog_ucan_core::subject::Subject as DelegatedSubject;
use dialog_ucan_core::time::timestamp::Timestamp;
use dialog_ucan_core::{DelegationBuilder, InvocationBuilder, InvocationChain};
use dialog_varsig::Principal as _;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

const CONTENT: &[u8] = b"the revision a cell points at";

/// A publish of `content` to `signer`'s own cell, minted as a client
/// mints one: the keys and encodings a record reads its references from.
async fn owned_publish(signer: &Ed25519Signer, content: &[u8]) -> InvocationChain<AnySignature> {
    let subject = Subject::from(signer.did());
    let capability = CellScope::new(subject, "sync", "head").publish(content.to_vec(), None);
    let at = Timestamp::now().to_unix();
    let minting = dialog_ucan::UcanAuthorization {
        chain: None,
        signer: Signer::from(signer.clone()),
        scope: Scope::invoke(&capability),
        duration: TimeRange {
            not_before: Some(at),
            expiration: Some(at + 60),
        },
        meta: None,
    };
    let invocation = minting.invoke().await.expect("the publish mints");
    invocation.chain().clone()
}

/// A publish of `content` by an operator the subject delegated publishing
/// to, through two delegations, with `prf` listing `proofs` (indices into
/// those two), and the container carrying both.
async fn delegated_publish(content: &[u8], proofs: &[usize]) -> Container {
    let subject = Ed25519Signer::generate().await.expect("a subject");
    let middle = Ed25519Signer::generate().await.expect("a middle");
    let operator = Ed25519Signer::generate().await.expect("an operator");
    let command: Vec<String> = PUBLISH.iter().map(|segment| segment.to_string()).collect();
    let root = DelegationBuilder::new()
        .issuer(subject.clone())
        .audience(&middle.did())
        .subject(DelegatedSubject::Specific(subject.did()))
        .command(command.clone())
        .try_build()
        .await
        .expect("the root mints");
    let leaf = DelegationBuilder::new()
        .issuer(middle)
        .audience(&operator.did())
        .subject(DelegatedSubject::Specific(subject.did()))
        .command(command.clone())
        .try_build()
        .await
        .expect("the leaf mints");
    let delegations = [root, leaf];
    let cids: Vec<_> = delegations.iter().map(|d| d.to_cid()).collect();
    let invocation = InvocationBuilder::new()
        .issuer(operator.clone())
        .audience(&subject.did())
        .subject(&subject.did())
        .command(command)
        .arguments(owned_publish(&subject, content).await.arguments().clone())
        .proofs(proofs.iter().map(|&at| cids[at]).collect())
        .issued_at(Timestamp::now())
        .try_build()
        .await
        .expect("the publish mints");
    let held: HashMap<_, _> = cids
        .iter()
        .copied()
        .zip(delegations.into_iter().map(Arc::new))
        .collect();
    Container::from(&InvocationChain::new(invocation, held))
}

/// A watch, which writes nothing and so names no content.
async fn watch() -> Container {
    let signer = Ed25519Signer::generate().await.expect("a signer");
    let mut arguments = BTreeMap::new();
    arguments.insert("space".to_string(), Promised::String("sync".into()));
    arguments.insert("cell".to_string(), Promised::String("head".into()));
    let invocation = InvocationBuilder::new()
        .issuer(signer.clone())
        .audience(&signer.did())
        .subject(&signer.did())
        .command(["memory", "cell", "watch"].map(String::from).to_vec())
        .arguments(arguments)
        .proofs(Vec::new())
        .issued_at(Timestamp::now())
        .try_build()
        .await
        .expect("the watch mints");
    Container::from(&InvocationChain::new(invocation, HashMap::new()))
}

/// The delegations `container` carries, each once, in its order.
fn delegations(container: &Container) -> Vec<(Reference, Vec<u8>)> {
    let mut delegations: Vec<(Reference, Vec<u8>)> = Vec::new();
    for token in &container.tokens()[1..] {
        let reference = Reference::Delegation(dagcbor_cid(token));
        if !delegations.iter().any(|(held, _)| *held == reference) {
            delegations.push((reference, token.clone()));
        }
    }
    delegations
}

/// The references of the delegations `container` carries.
fn references_of(container: &Container) -> Vec<Reference> {
    delegations(container)
        .into_iter()
        .map(|(reference, _)| reference)
        .collect()
}

/// The sections a record of `container` writing `content` reads as.
fn carried(container: &Container, content: &[u8]) -> Vec<Section> {
    let mut sections = vec![Section::Invocation(container.tokens()[0].clone())];
    for (reference, bytes) in delegations(container) {
        sections.push(Section::Block { reference, bytes });
    }
    sections.push(Section::Block {
        reference: Reference::Content(Checksum::sha256(content)),
        bytes: content.to_vec(),
    });
    sections
}

/// The bytes of a CARv1 header naming `roots`.
fn car_header(roots: Vec<Cid>) -> Vec<u8> {
    let header = serde_ipld_dagcbor::to_vec(&CarHeader { version: 1, roots }).expect("encodes");
    let mut out = Vec::new();
    put_varint(&mut out, header.len() as u64);
    out.extend(header);
    out
}

/// The bytes of a section addressing `block` by `cid`.
fn sectioned(cid: &Cid, block: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    section(&mut out, cid, block);
    out
}

/// The header and invocation section a record of `container` begins with.
fn rooted(container: &Container) -> Vec<u8> {
    let invocation = &container.tokens()[0];
    let root = dagcbor_cid(invocation);
    let mut out = car_header(vec![root]);
    out.extend(sectioned(&root, invocation));
    out
}

#[dialog_common::test]
async fn it_reads_back_the_invocation_its_delegations_and_its_content() {
    let container = delegated_publish(CONTENT, &[0, 1]).await;
    let record = encode(&container, Some(CONTENT)).expect("encodes");

    assert!(Record::is_record(&record));
    assert_eq!(decode(&record), Ok(carried(&container, CONTENT)));
}

#[dialog_common::test]
async fn it_names_a_publish_of_the_owner_by_its_content_alone() {
    let signer = Ed25519Signer::generate().await.expect("a signer");
    let container = Container::from(&owned_publish(&signer, CONTENT).await);
    let record = encode(&container, Some(CONTENT)).expect("encodes");

    assert_eq!(decode(&record), Ok(carried(&container, CONTENT)));
}

#[dialog_common::test]
async fn it_records_an_invocation_that_writes_nothing() {
    let container = watch().await;
    let record = encode(&container, None).expect("encodes");

    assert_eq!(
        decode(&record),
        Ok(vec![Section::Invocation(container.tokens()[0].clone())])
    );
}

#[dialog_common::test]
async fn it_carries_a_delegation_the_invocation_lists_twice_once() {
    let container = delegated_publish(CONTENT, &[0, 1, 0]).await;
    let record = encode(&container, Some(CONTENT)).expect("encodes");

    assert_eq!(decode(&record), Ok(carried(&container, CONTENT)));
}

#[dialog_common::test]
async fn it_is_a_car_rooted_at_the_invocation_with_each_block_by_its_cid() {
    let container = delegated_publish(CONTENT, &[0, 1]).await;
    let record = encode(&container, Some(CONTENT)).expect("encodes");
    let tokens = container.tokens();

    let mut expected = rooted(&container);
    expected.extend(sectioned(&dagcbor_cid(&tokens[1]), &tokens[1]));
    expected.extend(sectioned(&dagcbor_cid(&tokens[2]), &tokens[2]));
    let content = Reference::Content(Checksum::sha256(CONTENT)).cid();
    expected.extend(sectioned(&content, CONTENT));
    assert_eq!(record, expected);

    // The content is raw, by the very digest the invocation's checksum
    // signed.
    assert_eq!(content.codec(), RAW);
    assert_eq!(content.hash().code(), SHA2_256);
    assert_eq!(
        content.hash().digest(),
        Checksum::sha256(CONTENT).as_bytes()
    );
    // The header is exactly `{version, roots}`, as a strict CAR reader
    // requires.
    let mut at = 0;
    let length = take_varint(&record, &mut at).unwrap().unwrap() as usize;
    let header: BTreeMap<String, Ipld> =
        serde_ipld_dagcbor::from_slice(&record[at..at + length]).expect("a map");
    assert_eq!(
        header.keys().map(String::as_str).collect::<Vec<_>>(),
        ["roots", "version"]
    );
}

#[dialog_common::test]
async fn it_refuses_to_encode_without_the_content_the_invocation_signed() {
    let container = delegated_publish(CONTENT, &[0, 1]).await;
    let expected = Reference::Content(Checksum::sha256(CONTENT));

    assert_eq!(
        encode(&container, None),
        Err(RecordError::Truncated(Some(expected.clone())))
    );
    assert_eq!(
        encode(&container, Some(b"something else")),
        Err(RecordError::Truncated(Some(expected)))
    );
}

#[dialog_common::test]
async fn it_refuses_to_encode_without_a_delegation_the_invocation_lists() {
    let container = delegated_publish(CONTENT, &[0, 1]).await;
    let [_, root] = references_of(&container).try_into().expect("two");
    let short = Container::new(container.tokens()[..2].to_vec());

    assert_eq!(
        encode(&short, Some(CONTENT)),
        Err(RecordError::Truncated(Some(root)))
    );
}

#[dialog_common::test]
async fn it_refuses_bytes_that_are_not_a_record() {
    let container = delegated_publish(CONTENT, &[0, 1]).await;
    let container_bytes = container.to_bytes().expect("encodes");

    for bytes in [&container_bytes[..], b"", CONTENT] {
        assert!(!Record::is_record(bytes));
        assert!(matches!(decode(bytes), Err(RecordError::NotARecord(_))));
    }
    let header = car_header(vec![dagcbor_cid(&container.tokens()[0])]);
    assert!(Record::is_record(&header));
    assert_eq!(decode(&header), Err(RecordError::Truncated(None)));
}

#[dialog_common::test]
async fn it_refuses_a_header_with_other_than_one_root() {
    let container = delegated_publish(CONTENT, &[0, 1]).await;
    let tokens = container.tokens();
    let root = dagcbor_cid(&tokens[0]);

    for roots in [vec![], vec![root, dagcbor_cid(&tokens[1])]] {
        let count = roots.len();
        let mut record = car_header(roots);
        record.extend(sectioned(&root, &tokens[0]));
        assert_eq!(decode(&record), Err(RecordError::Roots(count)));
    }
}

#[dialog_common::test]
async fn it_refuses_a_header_with_a_field_past_version_and_roots() {
    #[derive(serde::Serialize)]
    struct Extended {
        version: u64,
        roots: Vec<Cid>,
        meta: String,
    }
    let container = delegated_publish(CONTENT, &[0, 1]).await;
    let root = dagcbor_cid(&container.tokens()[0]);
    let header = serde_ipld_dagcbor::to_vec(&Extended {
        version: 1,
        roots: vec![root],
        meta: "dialog".into(),
    })
    .expect("encodes");
    let mut record = Vec::new();
    put_varint(&mut record, header.len() as u64);
    record.extend(header);
    record.extend(sectioned(&root, &container.tokens()[0]));

    assert!(matches!(decode(&record), Err(RecordError::NotARecord(_))));
}

#[dialog_common::test]
async fn it_refuses_a_header_in_other_than_its_one_byte_form() {
    let container = delegated_publish(CONTENT, &[0, 1]).await;
    let root = dagcbor_cid(&container.tokens()[0]);
    // `{version: 1, roots: [root]}` with `version` first: the same header
    // to a lax reader, and not DAG-CBOR's key order.
    let mut header = vec![0xa2, 0x67];
    header.extend(b"version");
    header.extend([0x01, 0x65]);
    header.extend(b"roots");
    let cid = root.to_bytes();
    header.extend([0x81, 0xd8, 0x2a, 0x58, cid.len() as u8 + 1, 0x00]);
    header.extend(cid);
    let mut record = Vec::new();
    put_varint(&mut record, header.len() as u64);
    record.extend(header);
    record.extend(sectioned(&root, &container.tokens()[0]));

    assert_eq!(decode(&record), Err(RecordError::Header));
}

#[dialog_common::test]
async fn it_refuses_a_first_block_that_is_not_the_root() {
    let container = delegated_publish(CONTENT, &[0, 1]).await;
    let tokens = container.tokens();
    let mut record = car_header(vec![dagcbor_cid(&tokens[0])]);
    record.extend(sectioned(&dagcbor_cid(&tokens[1]), &tokens[1]));
    assert_eq!(decode(&record), Err(RecordError::Root));

    // The root's CID over other bytes.
    let mut record = car_header(vec![dagcbor_cid(&tokens[0])]);
    record.extend(sectioned(&dagcbor_cid(&tokens[0]), &tokens[1]));
    assert_eq!(decode(&record), Err(RecordError::Root));
}

#[dialog_common::test]
async fn it_refuses_a_root_that_is_not_an_invocation() {
    let container = delegated_publish(CONTENT, &[0, 1]).await;
    let delegation = &container.tokens()[1];
    let mut record = car_header(vec![dagcbor_cid(delegation)]);
    record.extend(sectioned(&dagcbor_cid(delegation), delegation));

    assert!(matches!(decode(&record), Err(RecordError::Invocation(_))));
}

#[dialog_common::test]
async fn it_refuses_blocks_out_of_the_order_the_invocation_names() {
    let container = delegated_publish(CONTENT, &[0, 1]).await;
    let [leaf, _] = references_of(&container).try_into().expect("two");
    let tokens = container.tokens();
    let mut record = rooted(&container);
    record.extend(sectioned(&dagcbor_cid(&tokens[2]), &tokens[2]));

    assert_eq!(
        decode(&record),
        Err(RecordError::Mismatch {
            section: 1,
            reference: leaf
        })
    );
}

#[dialog_common::test]
async fn it_refuses_content_other_than_what_the_invocation_signed() {
    let container = delegated_publish(CONTENT, &[0, 1]).await;
    let content = Reference::Content(Checksum::sha256(CONTENT));
    let whole = encode(&container, Some(CONTENT)).expect("encodes");
    let prefix = &whole[..whole.len() - sectioned(&content.cid(), CONTENT).len()];
    let other = b"the revision a cell points to";

    // Other bytes under the signed CID, and other bytes under their own.
    for cid in [
        content.cid(),
        Reference::Content(Checksum::sha256(other)).cid(),
    ] {
        let mut record = prefix.to_vec();
        record.extend(sectioned(&cid, other));
        assert_eq!(
            decode(&record),
            Err(RecordError::Mismatch {
                section: 3,
                reference: content.clone()
            })
        );
    }
}

#[dialog_common::test]
async fn it_refuses_a_block_in_a_codec_other_than_its_reference_names() {
    let container = delegated_publish(CONTENT, &[0, 1]).await;
    let content = Reference::Content(Checksum::sha256(CONTENT));
    let whole = encode(&container, Some(CONTENT)).expect("encodes");
    let prefix = &whole[..whole.len() - sectioned(&content.cid(), CONTENT).len()];
    // The content's digest, claimed as DAG-CBOR: its bytes hash right,
    // its CID does not.
    let claimed = Cid::new_v1(DAG_CBOR, *content.cid().hash());
    let mut record = prefix.to_vec();
    record.extend(sectioned(&claimed, CONTENT));

    assert_eq!(
        decode(&record),
        Err(RecordError::Mismatch {
            section: 3,
            reference: content
        })
    );
}

#[dialog_common::test]
async fn it_refuses_a_block_past_the_last_reference() {
    let container = delegated_publish(CONTENT, &[0, 1]).await;
    let mut record = encode(&container, Some(CONTENT)).expect("encodes");
    record.extend(sectioned(&dagcbor_cid(b"smuggled"), b"smuggled"));

    assert_eq!(decode(&record), Err(RecordError::Unexpected { section: 4 }));
}

#[dialog_common::test]
async fn it_refuses_a_record_that_ends_early() {
    let container = delegated_publish(CONTENT, &[0, 1]).await;
    let record = encode(&container, Some(CONTENT)).expect("encodes");
    let content = Reference::Content(Checksum::sha256(CONTENT));

    // Ending between sections, and inside one.
    let between = record.len() - sectioned(&content.cid(), CONTENT).len();
    assert_eq!(
        decode(&record[..between]),
        Err(RecordError::Truncated(Some(content.clone())))
    );
    assert_eq!(
        decode(&record[..record.len() - 1]),
        Err(RecordError::Truncated(Some(content)))
    );
}

#[dialog_common::test]
async fn it_refuses_a_varint_that_is_not_minimal() {
    let container = delegated_publish(CONTENT, &[0, 1]).await;
    let record = encode(&container, Some(CONTENT)).expect("encodes");
    let mut at = 0;
    let length = take_varint(&record, &mut at).unwrap().unwrap();
    assert_eq!(at, 1, "a header's length is one byte");

    // The same header length in two bytes: the same CAR to a lax reader.
    let mut padded = vec![(length as u8) | 0x80, 0x00];
    padded.extend(&record[1..]);
    assert_eq!(decode(&padded), Err(RecordError::Varint { at: 0 }));
    assert!(!Record::is_record(&padded));

    let mut overflowing = vec![0xff; 9];
    overflowing.push(0x02);
    assert_eq!(decode(&overflowing), Err(RecordError::Varint { at: 0 }));
}

#[dialog_common::test]
async fn it_refuses_a_length_past_any_block_before_reading_it() {
    let container = delegated_publish(CONTENT, &[0, 1]).await;
    let mut record = car_header(vec![dagcbor_cid(&container.tokens()[0])]);
    put_varint(&mut record, MAX_BLOCK + 1);

    assert_eq!(
        decode(&record),
        Err(RecordError::TooLarge {
            section: 0,
            length: MAX_BLOCK + 1
        })
    );
}

/// A publish of `arguments` to `subject`'s cell, signed by `issuer` with
/// no delegation, issued now unless `undated`.
async fn signed_by(
    issuer: &Ed25519Signer,
    subject: &Ed25519Signer,
    command: &[&str],
    arguments: BTreeMap<String, Promised>,
    undated: bool,
) -> Container {
    let builder = InvocationBuilder::new()
        .issuer(issuer.clone())
        .audience(&subject.did())
        .subject(&subject.did())
        .command(command.iter().map(|segment| segment.to_string()).collect())
        .arguments(arguments)
        .proofs(Vec::new());
    let builder = if undated {
        builder
    } else {
        builder.issued_at(Timestamp::now())
    };
    let invocation = builder.try_build().await.expect("the invocation mints");
    Container::from(&InvocationChain::new(invocation, HashMap::new()))
}

const PUBLISH: &[&str] = &["use", "put", "memory", "cell"];

fn resolver() -> dialog_remote_ucan_s3::DefaultResolver {
    dialog_remote_ucan_s3::DefaultResolver::new(dialog_did_web::WebResolver::new())
}

async fn verified(container: &Container, subject: &Ed25519Signer) -> Result<(), Unproven> {
    let record =
        Record::decode(&encode(container, Some(CONTENT)).expect("encodes")).expect("decodes");
    record
        .verify(&subject.did(), "sync", "head", &resolver())
        .await
}

#[dialog_common::test]
async fn it_proves_a_publish_the_subject_signed() {
    let owner = Ed25519Signer::generate().await.expect("an owner");
    let container = Container::from(&owned_publish(&owner, CONTENT).await);

    assert_eq!(verified(&container, &owner).await, Ok(()));
}

#[dialog_common::test]
async fn it_proves_a_publish_the_subject_delegated() {
    let container = delegated_publish(CONTENT, &[0, 1]).await;
    let record =
        Record::decode(&encode(&container, Some(CONTENT)).expect("encodes")).expect("decodes");
    let invocation: Invocation<AnySignature> =
        serde_ipld_dagcbor::from_slice(&container.tokens()[0]).expect("an invocation");

    assert_eq!(
        record
            .verify(invocation.subject(), "sync", "head", &resolver())
            .await,
        Ok(())
    );
}

/// A publish signed by someone the subject never authorized proves
/// nothing, though it names the cell and its content is intact.
#[dialog_common::test]
async fn it_refuses_a_publish_by_someone_the_subject_did_not_authorize() {
    let owner = Ed25519Signer::generate().await.expect("an owner");
    let stranger = Ed25519Signer::generate().await.expect("a stranger");
    let arguments = owned_publish(&owner, CONTENT).await.arguments().clone();
    let container = signed_by(&stranger, &owner, PUBLISH, arguments, false).await;

    assert!(matches!(
        verified(&container, &owner).await,
        Err(Unproven::Chain(_))
    ));
}

/// A delegation that leaves out a link of the chain proves nothing.
#[dialog_common::test]
async fn it_refuses_a_publish_whose_chain_skips_a_delegation() {
    let container = delegated_publish(CONTENT, &[0]).await;
    let invocation: Invocation<AnySignature> =
        serde_ipld_dagcbor::from_slice(&container.tokens()[0]).expect("an invocation");
    let record =
        Record::decode(&encode(&container, Some(CONTENT)).expect("encodes")).expect("decodes");

    assert!(matches!(
        record
            .verify(invocation.subject(), "sync", "head", &resolver())
            .await,
        Err(Unproven::Chain(_))
    ));
}

/// A genuine publish to another of the subject's cells does not prove
/// what this one holds: a service cannot answer one cell with another's.
#[dialog_common::test]
async fn it_refuses_a_publish_to_another_cell() {
    let owner = Ed25519Signer::generate().await.expect("an owner");
    let container = Container::from(&owned_publish(&owner, CONTENT).await);
    let record =
        Record::decode(&encode(&container, Some(CONTENT)).expect("encodes")).expect("decodes");

    assert!(matches!(
        record
            .verify(&owner.did(), "sync", "tail", &resolver())
            .await,
        Err(Unproven::Elsewhere { .. })
    ));
    let other = Ed25519Signer::generate().await.expect("another subject");
    assert!(matches!(
        record
            .verify(&other.did(), "sync", "head", &resolver())
            .await,
        Err(Unproven::Elsewhere { .. })
    ));
}

/// An invocation the subject signed that is not a publish (a watch of
/// the cell, carrying the same arguments) does not prove a write.
#[dialog_common::test]
async fn it_refuses_an_invocation_that_is_not_a_publish() {
    let owner = Ed25519Signer::generate().await.expect("an owner");
    let arguments = owned_publish(&owner, CONTENT).await.arguments().clone();
    let container = signed_by(
        &owner,
        &owner,
        &["use", "get", "memory", "cell", "watch"],
        arguments,
        false,
    )
    .await;

    assert!(matches!(
        verified(&container, &owner).await,
        Err(Unproven::Command(_))
    ));
}

#[dialog_common::test]
async fn it_refuses_a_publish_that_does_not_say_when_it_was_issued() {
    let owner = Ed25519Signer::generate().await.expect("an owner");
    let arguments = owned_publish(&owner, CONTENT).await.arguments().clone();
    let container = signed_by(&owner, &owner, PUBLISH, arguments, true).await;

    assert_eq!(verified(&container, &owner).await, Err(Unproven::Undated));
}

/// A site hands on the content a cell's record proves, and refuses one
/// whose content was swapped for other bytes, as unproven.
#[dialog_common::test]
async fn it_opens_a_proven_cell_and_refuses_a_tampered_one() {
    let owner = Ed25519Signer::generate().await.expect("an owner");
    let container = Container::from(&owned_publish(&owner, CONTENT).await);
    let stored = encode(&container, Some(CONTENT)).expect("encodes");
    let site = crate::UcanSite::default();
    let state = |content: Vec<u8>| {
        Some(dialog_effects::memory::Edition {
            content,
            version: "v1".into(),
        })
    };

    let opened = site
        .open(&owner.did(), "sync", "head", state(stored.clone()))
        .await
        .expect("the record proves the content");
    assert_eq!(opened, state(CONTENT.to_vec()));

    let mut tampered = stored[..stored.len() - CONTENT.len()].to_vec();
    tampered.extend_from_slice(b"the revision a cell points to");
    assert!(matches!(
        site.open(&owner.did(), "sync", "head", state(tampered))
            .await,
        Err(dialog_effects::memory::MemoryError::Unproven { .. })
    ));
}

/// A cell that holds content and no record, as cells did before they
/// held records, is read as it was unless the site refuses it.
#[dialog_common::test]
async fn it_reads_a_cell_with_no_record_only_where_that_is_accepted() {
    let owner = Ed25519Signer::generate().await.expect("an owner");
    let state = Some(dialog_effects::memory::Edition {
        content: CONTENT.to_vec(),
        version: "v1".into(),
    });

    let accepting = crate::UcanSite::default();
    assert_eq!(
        accepting
            .open(&owner.did(), "sync", "head", state.clone())
            .await
            .expect("accepted"),
        state
    );

    let refusing = crate::UcanSite::default().with_unrecorded(crate::Unrecorded::Refuse);
    assert!(matches!(
        refusing.open(&owner.did(), "sync", "head", state).await,
        Err(dialog_effects::memory::MemoryError::Unproven { .. })
    ));
    assert_eq!(
        refusing.open(&owner.did(), "sync", "head", None).await.ok(),
        Some(None),
        "an empty cell needs no record"
    );
}
