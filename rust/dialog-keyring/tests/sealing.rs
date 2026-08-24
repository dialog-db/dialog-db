//! The properties the sealing layer exists to guarantee.
//!
//! Every one of these runs with no key agreement anywhere — which is the
//! point. If they hold against `LocalKeyring`, they hold against whatever
//! implements `Keyring` later, because nothing above the trait knows the
//! difference.

use dialog_keyring::{Keyring, KeyringError, KeyringExt, LocalKeyring, Sealed};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// A keyring built from fixed material, so two "replicas" can be built
/// identically and compared byte for byte.
fn replica() -> LocalKeyring {
    LocalKeyring::genesis([7u8; 32], [1u8; 32])
}

#[dialog_common::test]
async fn seals_and_opens() {
    let keyring = replica();

    let sealed = keyring.seal(b"a node buffer").await.unwrap();

    assert_eq!(keyring.open(&sealed).await.unwrap(), b"a node buffer");
}

#[dialog_common::test]
async fn the_ciphertext_does_not_contain_the_plaintext() {
    let keyring = replica();

    let sealed = keyring.seal(b"a node buffer").await.unwrap();

    let bytes = sealed.to_bytes();
    assert!(
        !bytes.windows(13).any(|window| window == b"a node buffer"),
        "plaintext survived into the sealed blob"
    );
}

#[dialog_common::test]
async fn identical_content_seals_to_an_identical_address() {
    // Two replicas that have never spoken, holding the same space secret and
    // the same epoch. This is the property a prolly tree's convergence rests
    // on: independently computing the same node must yield the same address,
    // or a diff would see changes that are not there.
    let one = replica();
    let other = replica();

    let here = one.seal(b"a node buffer").await.unwrap();
    let there = other.seal(b"a node buffer").await.unwrap();

    assert_eq!(here.to_bytes(), there.to_bytes());
    assert_eq!(here.address(), there.address());
}

#[dialog_common::test]
async fn different_content_gets_different_addresses() {
    let keyring = replica();

    let one = keyring.seal(b"one node").await.unwrap();
    let other = keyring.seal(b"another node").await.unwrap();

    assert_ne!(one.address(), other.address());
}

#[dialog_common::test]
async fn rotation_leaves_earlier_content_readable() {
    // Nothing is re-encrypted when the key rotates. The blob names the epoch
    // it was written under, and the keyring resolves it however old it is.
    let mut keyring = replica();
    let before = keyring.seal(b"written earlier").await.unwrap();

    keyring.rotate_with([2u8; 32]);
    keyring.rotate_with([3u8; 32]);
    keyring.rotate_with([4u8; 32]);

    assert_eq!(keyring.open(&before).await.unwrap(), b"written earlier");
}

#[dialog_common::test]
async fn rotation_moves_the_address_of_identical_content() {
    // The cost of rotation, asserted rather than assumed: the same bytes
    // sealed under a new epoch land at a new address, so a diff across the
    // boundary transfers what it would otherwise have pruned.
    let mut keyring = replica();
    let before = keyring.seal(b"a node buffer").await.unwrap();

    keyring.rotate_with([2u8; 32]);
    let after = keyring.seal(b"a node buffer").await.unwrap();

    assert_ne!(before.address(), after.address());
    assert_ne!(before.epoch(), after.epoch());
    // Both remain readable — this is a new address, not a lost one.
    assert_eq!(keyring.open(&before).await.unwrap(), b"a node buffer");
    assert_eq!(keyring.open(&after).await.unwrap(), b"a node buffer");
}

#[dialog_common::test]
async fn an_unreplicated_epoch_cannot_be_resolved() {
    // A reader who has the blob but not the epoch record cannot invent the
    // key. This is what makes the keyring log load-bearing rather than
    // advisory.
    let mut writer = replica();
    writer.rotate_with([2u8; 32]);
    let sealed = writer.seal(b"written after rotating").await.unwrap();

    let behind = replica();

    assert!(matches!(
        behind.open(&sealed).await,
        Err(KeyringError::UnknownEpoch(_))
    ));
}

#[dialog_common::test]
async fn another_space_cannot_open() {
    // Same epoch record, different space secret.
    let keyring = replica();
    let stranger = LocalKeyring::genesis([9u8; 32], [1u8; 32]);

    let sealed = keyring.seal(b"a node buffer").await.unwrap();

    assert!(matches!(
        stranger.open(&sealed).await,
        Err(KeyringError::Failed)
    ));
}

#[dialog_common::test]
async fn tampering_is_detected() {
    let keyring = replica();
    let sealed = keyring.seal(b"a node buffer").await.unwrap();

    let mut bytes = sealed.to_bytes();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    let tampered = Sealed::from_bytes(&bytes).unwrap();

    assert!(matches!(
        keyring.open(&tampered).await,
        Err(KeyringError::Failed)
    ));
}

#[dialog_common::test]
async fn a_relabelled_epoch_is_detected() {
    // The header is plaintext, so it can be rewritten in transit. It is also
    // the AEAD's additional data, so rewriting it stops the blob opening
    // rather than quietly redirecting a reader to another key.
    let mut keyring = replica();
    let sealed = keyring.seal(b"a node buffer").await.unwrap();
    let other = keyring.rotate_with([2u8; 32]);

    let mut bytes = sealed.to_bytes();
    bytes[1..33].copy_from_slice(other.as_bytes());
    let relabelled = Sealed::from_bytes(&bytes).unwrap();

    assert!(matches!(
        keyring.open(&relabelled).await,
        Err(KeyringError::Failed)
    ));
}

#[dialog_common::test]
async fn concurrent_rotation_survives_the_merge() {
    // The case that has no coordinated answer: two replicas rotate during a
    // partition, with no chance to agree. Both epochs are real, both sets of
    // writes stay readable, and after the merge the two converge on one epoch
    // again — so subsequent identical writes share an address without either
    // replica rotating a third time.
    let base = replica();
    let mut here = base.clone();
    let mut there = base;

    here.rotate_with([2u8; 32]);
    there.rotate_with([3u8; 32]);

    let written_here = here.seal(b"partitioned write").await.unwrap();
    let written_there = there.seal(b"partitioned write").await.unwrap();
    assert_ne!(
        written_here.address(),
        written_there.address(),
        "different epochs must not collide"
    );

    // Neither can read the other yet.
    assert!(here.open(&written_there).await.is_err());
    assert!(there.open(&written_here).await.is_err());

    // The partition heals: each takes in the other's epoch log.
    let (mine, theirs) = (here.log().clone(), there.log().clone());
    here.merge(&theirs).unwrap();
    there.merge(&mine).unwrap();

    assert_eq!(
        here.open(&written_there).await.unwrap(),
        b"partitioned write"
    );
    assert_eq!(
        there.open(&written_here).await.unwrap(),
        b"partitioned write"
    );

    assert_eq!(here.current(), there.current(), "settled on one epoch");
    assert_eq!(
        here.seal(b"after the merge").await.unwrap().address(),
        there.seal(b"after the merge").await.unwrap().address(),
        "convergence restored without rotating again"
    );
}

#[dialog_common::test]
async fn a_rotation_after_a_merge_collapses_both_epochs() {
    let base = replica();
    let mut here = base.clone();
    let mut there = base;
    here.rotate_with([2u8; 32]);
    there.rotate_with([3u8; 32]);

    let theirs = there.log().clone();
    here.merge(&theirs).unwrap();
    assert_eq!(here.log().heads().len(), 2, "two concurrent heads");

    let collapsed = here.rotate_with([4u8; 32]);

    assert_eq!(here.log().heads().len(), 1);
    assert_eq!(
        here.log().get(&collapsed).unwrap().predecessors().len(),
        2,
        "the new epoch names both of the epochs it supersedes"
    );
}

#[dialog_common::test]
async fn the_wire_format_round_trips() {
    let keyring = replica();
    let sealed = keyring.seal(b"a node buffer").await.unwrap();

    let decoded = Sealed::from_bytes(&sealed.to_bytes()).unwrap();

    assert_eq!(decoded, sealed);
    assert_eq!(keyring.open(&decoded).await.unwrap(), b"a node buffer");
}

#[dialog_common::test]
async fn a_truncated_blob_is_malformed() {
    assert!(matches!(
        Sealed::from_bytes(&[]),
        Err(KeyringError::Malformed)
    ));
    assert!(matches!(
        Sealed::from_bytes(&[1u8; 44]),
        Err(KeyringError::Malformed)
    ));
}

#[dialog_common::test]
async fn an_unknown_version_is_refused() {
    let keyring = replica();
    let mut bytes = keyring.seal(b"a node buffer").await.unwrap().to_bytes();
    bytes[0] = 99;

    assert!(matches!(
        Sealed::from_bytes(&bytes),
        Err(KeyringError::UnsupportedVersion(99))
    ));
}

#[dialog_common::test]
async fn rotation_samples_real_entropy() {
    // `rotate_with` is the deterministic door for tests; `rotate` is the one
    // production uses, and it must actually reach the platform's CSPRNG.
    let mut keyring = replica();

    let first = keyring.rotate().await.unwrap();
    let second = keyring.rotate().await.unwrap();

    assert_ne!(first, second);
    assert_eq!(keyring.log().len(), 3);
}

#[dialog_common::test]
async fn the_address_is_the_same_on_every_platform() {
    // A golden vector. Native runs AES-GCM and HKDF through RustCrypto, the
    // browser routes both through WebCrypto; a replica on either must produce
    // byte-identical blobs or two peers on different platforms would disagree
    // on every address they compute.
    let keyring = replica();

    let sealed = keyring.seal(b"a node buffer").await.unwrap();

    assert_eq!(sealed.address().to_string(), GOLDEN_ADDRESS);
}

/// The address of `b"a node buffer"` sealed by [`replica`]'s genesis epoch.
const GOLDEN_ADDRESS: &str = "blake3#6YaQwrhb37ra75Nu5it94bQ7SLLyF1rbZ259oU22MLqm";

#[dialog_common::test]
async fn sync_and_async_sealing_agree() {
    // The write path seals synchronously, because the tree's persist does not
    // await; anything that resolves a key ahead of time can seal
    // asynchronously. Both must produce the same bytes, or a node written by
    // one path would be unreadable by a peer using the other.
    let keyring = replica();
    let epoch = keyring.current();
    let key = keyring.key(&epoch).await.unwrap();

    let asynchronous = Sealed::seal(&key, &epoch, b"a node buffer").await.unwrap();
    let synchronous = Sealed::seal_now(&key, &epoch, b"a node buffer").unwrap();

    assert_eq!(asynchronous, synchronous);
    assert_eq!(synchronous.open(&key).await.unwrap(), b"a node buffer");
    assert_eq!(asynchronous.open_now(&key).unwrap(), b"a node buffer");
}

#[dialog_common::test]
async fn the_blinding_key_does_not_rotate() {
    // A node's address has to stay put across rotations, or every link
    // written before one would dangle.
    let mut keyring = replica();
    let before = keyring.blinding_key();

    keyring.rotate_with([2u8; 32]);

    assert_eq!(keyring.blinding_key(), before);
}
