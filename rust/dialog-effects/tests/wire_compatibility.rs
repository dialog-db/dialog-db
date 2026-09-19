//! The wire contract every deployed client already speaks.
//!
//! A capability reaches the wire as two independent channels:
//!
//! - `cmd` — the ability path, built by walking the chain and asking each
//!   link for its segment.
//! - `prm` — the parameters map, built by serializing each link's fields.
//!   Produced by a blanket `Caveat` impl over `Serialize`, so it is
//!   entirely independent of whether a link contributes to `cmd`.
//!
//! Both are matched by authorizers holding delegations minted before any
//! given release, so neither may move. This file exists to be written
//! BEFORE the capability layer is refactored and to stay green
//! throughout: it is the definition of "wire compatible", not a
//! description of the current implementation. Every string below was
//! measured from the shipped code, not transcribed from a design.
//!
//! When a refactor changes how a path is assembled — who emits which
//! segment, whether effects name themselves — these assertions must not
//! need editing. If one does, the change is a wire break.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use dialog_capability::{Subject, did};
use dialog_common::Blake3Hash;
use dialog_effects::Use;
use dialog_effects::archive::{Archive, Catalog, Get, Import as ArchiveImport, Put};
use dialog_effects::blob::{Blob, Import as BlobImport, Read, Write};
use dialog_effects::memory::{Cell, Memory, Publish, Resolve, Retract, Space};

fn subject() -> Subject {
    Subject::from(did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"))
}

/// The three memory commands, as deployed clients spell them.
#[dialog_common::test]
fn it_preserves_memory_commands() {
    let cell = subject()
        .attenuate(Use)
        .attenuate(Memory)
        .attenuate(Space::new("branch/main"))
        .attenuate(Cell::new("revision"));

    assert_eq!(
        cell.clone().invoke(Resolve).ability(),
        "/use/get/memory/cell"
    );
    assert_eq!(
        cell.clone()
            .invoke(Publish::new(b"content".to_vec(), None))
            .ability(),
        "/use/put/memory/cell"
    );
    assert_eq!(
        cell.invoke(Retract::new(b"version")).ability(),
        "/use/delete/memory/cell"
    );
}

/// Archive blocks. `Import` deliberately shares `Put`'s command: it is
/// the same authority over the same resource, differing only in how the
/// payload arrives.
#[dialog_common::test]
fn it_preserves_archive_commands() {
    let digest = Blake3Hash::hash(b"block");
    let catalog = subject()
        .attenuate(Use)
        .attenuate(Archive)
        .attenuate(Catalog::new("index"));

    assert_eq!(
        catalog.clone().invoke(Get::new(digest.clone())).ability(),
        "/use/get/archive/block"
    );
    assert_eq!(
        catalog
            .clone()
            .invoke(Put::new(b"block".to_vec()))
            .ability(),
        "/use/put/archive/block"
    );
    assert_eq!(
        catalog
            .invoke(ArchiveImport::new(vec![b"block".to_vec()]))
            .ability(),
        "/use/put/archive/block"
    );
}

/// Blobs live under the `archive` namespace even though they are
/// declared in the `blob` module — a relationship currently visible only
/// in these literals, and one a chain-derived path must reproduce.
#[dialog_common::test]
fn it_preserves_blob_commands() {
    let digest = Blake3Hash::hash(b"blob");
    let blob = subject().attenuate(Use).attenuate(Archive).attenuate(Blob);

    assert_eq!(
        blob.clone().invoke(Read::new(digest.clone())).ability(),
        "/use/get/archive/blob"
    );
    assert_eq!(
        blob.clone().invoke(Write::new()).ability(),
        "/use/put/archive/blob"
    );
    assert_eq!(
        blob.invoke(BlobImport::new(digest, 7)).ability(),
        "/use/put/archive/blob"
    );
}

/// Every command a delegation may be attenuated to, in one list.
///
/// A refactor that renames or reorders a segment shows up here as a
/// diff, which is the point: this is the set an authorizer matches
/// against, so it is the set that must not change.
#[dialog_common::test]
fn it_preserves_the_whole_command_vocabulary() {
    let digest = Blake3Hash::hash(b"x");
    let cell = subject()
        .attenuate(Use)
        .attenuate(Memory)
        .attenuate(Space::new("s"))
        .attenuate(Cell::new("c"));
    let catalog = subject()
        .attenuate(Use)
        .attenuate(Archive)
        .attenuate(Catalog::new("i"));
    let blob = subject().attenuate(Use).attenuate(Archive).attenuate(Blob);

    let mut commands = vec![
        cell.clone().invoke(Resolve).ability(),
        cell.clone()
            .invoke(Publish::new(b"c".to_vec(), None))
            .ability(),
        cell.invoke(Retract::new(b"v")).ability(),
        catalog.clone().invoke(Get::new(digest.clone())).ability(),
        catalog.clone().invoke(Put::new(b"c".to_vec())).ability(),
        blob.clone().invoke(Read::new(digest.clone())).ability(),
        blob.invoke(Write::new()).ability(),
    ];
    commands.sort();
    commands.dedup();

    assert_eq!(
        commands,
        vec![
            "/use/delete/memory/cell",
            "/use/get/archive/blob",
            "/use/get/archive/block",
            "/use/get/memory/cell",
            "/use/put/archive/blob",
            "/use/put/archive/block",
            "/use/put/memory/cell",
        ]
    );
}

/// Every command sits under a root that says whether it changes data or
/// destroys it. Nothing outside `/use` and `/void` is reachable, which
/// is what keeps a data delegation clear of `/ucan`.
#[dialog_common::test]
fn it_roots_every_command_under_use_or_void() {
    let cell = subject()
        .attenuate(Use)
        .attenuate(Memory)
        .attenuate(Space::new("s"))
        .attenuate(Cell::new("c"));

    for ability in [
        cell.clone().invoke(Resolve).ability(),
        cell.invoke(Publish::new(b"c".to_vec(), None)).ability(),
    ] {
        assert!(
            ability.starts_with("/use/") || ability.starts_with("/void/"),
            "{ability} escapes the use/void roots"
        );
    }
}
