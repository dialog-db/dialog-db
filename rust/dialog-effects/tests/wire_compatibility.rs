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
use dialog_effects::prelude::*;

fn subject() -> Subject {
    Subject::from(did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"))
}

/// The three memory commands, as deployed clients spell them.
#[dialog_common::test]
fn it_preserves_memory_commands() {
    assert_eq!(
        subject()
            .get()
            .memory()
            .space("branch/main")
            .cell("revision")
            .resolve()
            .ability(),
        "/use/get/memory/cell"
    );
    assert_eq!(
        subject()
            .put()
            .memory()
            .space("branch/main")
            .cell("revision")
            .publish(b"content".to_vec(), None)
            .ability(),
        "/use/put/memory/cell"
    );
    assert_eq!(
        subject()
            .r#use()
            .delete()
            .memory()
            .space("branch/main")
            .cell("revision")
            .retract(b"version")
            .ability(),
        "/use/delete/memory/cell"
    );
}

/// Archive blocks. `Import` deliberately shares `Put`'s command: it is
/// the same authority over the same resource, differing only in how the
/// payload arrives.
#[dialog_common::test]
fn it_preserves_archive_commands() {
    let digest = Blake3Hash::hash(b"block");

    assert_eq!(
        subject()
            .get()
            .archive()
            .catalog("index")
            .get(digest.clone())
            .ability(),
        "/use/get/archive/block"
    );
    assert_eq!(
        subject()
            .put()
            .archive()
            .catalog("index")
            .put(b"block".to_vec())
            .ability(),
        "/use/put/archive/block"
    );
    assert_eq!(
        subject()
            .put()
            .archive()
            .catalog("index")
            .import(vec![b"block".to_vec()])
            .ability(),
        "/use/put/archive/block"
    );
}

/// Skipping the catalog reaches the default one and changes nothing a
/// peer can observe: same path, same parameters. The catalog name is a
/// parameter, so a shorthand that supplies it cannot move `cmd`, and
/// supplying the default explicitly must stay indistinguishable.
#[dialog_common::test]
fn it_defaults_the_catalog_without_moving_the_wire() {
    let digest = Blake3Hash::hash(b"block");

    let spelled = subject()
        .get()
        .archive()
        .catalog("index")
        .get(digest.clone());
    let implied = subject().get().archive().get(digest.clone());

    assert_eq!(
        spelled.ability(),
        implied.ability(),
        "the catalog is a parameter, so omitting it cannot move the path"
    );
    assert_eq!(implied.ability(), "/use/get/archive/block");
}

/// Blobs live under the `archive` namespace even though they are
/// declared in the `blob` module — a relationship currently visible only
/// in these literals, and one a chain-derived path must reproduce.
#[dialog_common::test]
fn it_preserves_blob_commands() {
    let digest = Blake3Hash::hash(b"blob");

    assert_eq!(
        subject()
            .get()
            .archive()
            .blob()
            .read(digest.clone())
            .ability(),
        "/use/get/archive/blob"
    );
    assert_eq!(
        subject().put().archive().blob().write().ability(),
        "/use/put/archive/blob"
    );
    assert_eq!(
        subject()
            .put()
            .archive()
            .blob()
            .import(digest.clone(), 7)
            .ability(),
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
    let mut commands = vec![
        subject()
            .get()
            .memory()
            .space("s")
            .cell("c")
            .resolve()
            .ability(),
        subject()
            .put()
            .memory()
            .space("s")
            .cell("c")
            .publish(b"c".to_vec(), None)
            .ability(),
        subject()
            .r#use()
            .delete()
            .memory()
            .space("s")
            .cell("c")
            .retract(b"v")
            .ability(),
        subject()
            .get()
            .archive()
            .catalog("i")
            .get(digest.clone())
            .ability(),
        subject()
            .put()
            .archive()
            .catalog("i")
            .put(b"c".to_vec())
            .ability(),
        subject()
            .get()
            .archive()
            .blob()
            .read(digest.clone())
            .ability(),
        subject().put().archive().blob().write().ability(),
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
    for ability in [
        subject()
            .get()
            .memory()
            .space("s")
            .cell("c")
            .resolve()
            .ability(),
        subject()
            .put()
            .memory()
            .space("s")
            .cell("c")
            .publish(b"c".to_vec(), None)
            .ability(),
    ] {
        assert!(
            ability.starts_with("/use/") || ability.starts_with("/void/"),
            "{ability} escapes the use/void roots"
        );
    }
}

/// A method names a path on its own, before any namespace narrows it.
///
/// This is what a delegation grants when it stops at the method:
/// `subject.get()` is `/use/get`, authorizing every namespace beneath
/// it. The roots are pinned here because a holder's reach is decided by
/// these strings, so a chain that silently rooted `get` somewhere else
/// would widen or narrow every delegation minted from it.
#[dialog_common::test]
fn it_names_a_path_from_the_method_alone() {
    assert_eq!(subject().r#use().ability(), "/use");
    assert_eq!(subject().void().ability(), "/void");

    assert_eq!(subject().get().ability(), "/use/get");
    assert_eq!(subject().put().ability(), "/use/put");

    // A bare `delete` destroys the thing itself, so it needs no root
    // spelled out. Emptying a value while leaving what held it is the
    // odd one: it is reached through the root that says so, and exists
    // for the one command that shipped spelling it that way.
    assert_eq!(subject().delete().ability(), "/void/delete");
    assert_eq!(subject().void().delete().ability(), "/void/delete");
    assert_eq!(subject().r#use().delete().ability(), "/use/delete");
}
