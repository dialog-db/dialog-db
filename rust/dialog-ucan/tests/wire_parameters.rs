//! The `prm` half of the wire contract: what a capability chain
//! serializes into a UCAN invocation's parameters.
//!
//! The companion to `dialog-effects/tests/wire_compatibility.rs`, which
//! pins the `cmd` half. Together they are the definition of "wire
//! compatible" for the capability layer.
//!
//! The distinction these tests exist to protect: a link's contribution
//! to `cmd` and its contribution to `prm` come from two unrelated
//! mechanisms. `cmd` comes from `Policy::attenuation()` — whether a link
//! names itself in the ability path. `prm` comes from a blanket `Caveat`
//! impl over `Serialize` — what a link's fields encode to. Changing a
//! link between `Policy` and `Attenuation` therefore moves `cmd` and
//! must leave `prm` untouched.
//!
//! That independence is what makes it safe to rework how ability paths
//! are assembled: the names a holder is actually scoped to — which
//! space, which cell — ride in `prm` and never depended on the path.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use dialog_capability::identity::{Entity, Revision, TreeReference};
use dialog_capability::{Subject, did};
use dialog_effects::MethodExt as _;
use dialog_effects::branch::prelude::*;
use dialog_effects::memory::prelude::CellScope;
use dialog_ucan::parameters;

fn subject() -> Subject {
    Subject::from(did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"))
}

/// A cell capability scopes its holder by space and cell name, and both
/// travel in `prm`. These are the values an authorizer matches a
/// delegation's caveats against, so they may not move or be renamed.
#[dialog_common::test]
fn it_preserves_memory_parameters() {
    let cell = || CellScope::new(subject(), "branch/main", "revision");

    let resolve = parameters(&cell().resolve());
    assert_eq!(resolve.get("space").unwrap(), &"branch/main".into());
    assert_eq!(resolve.get("cell").unwrap(), &"revision".into());
    assert_eq!(
        resolve.len(),
        2,
        "a resolve carries the space and cell it is scoped to, nothing more: {resolve:?}"
    );

    // An effect adds its own fields on top of the chain's, and the
    // chain's are unchanged by which effect is invoked.
    let publish = parameters(&cell().publish(b"hi".to_vec(), None));
    assert_eq!(publish.get("space").unwrap(), &"branch/main".into());
    assert_eq!(publish.get("cell").unwrap(), &"revision".into());
    assert!(
        publish.contains_key("content"),
        "a publish carries its content: {publish:?}"
    );
}

/// The scoping names come from the links the caller named, not from
/// the effect: every effect on the same cell carries the same space and
/// cell, whichever verb it is reached through.
#[dialog_common::test]
fn it_derives_parameters_from_the_chain_not_the_effect() {
    let cell = || CellScope::new(subject(), "branch/main", "revision");

    let read = parameters(&cell().resolve());
    let write = parameters(&cell().publish(b"x".to_vec(), None));
    let delete = parameters(&cell().retract(b"v1"));

    for prm in [&read, &write, &delete] {
        assert_eq!(prm.get("space").unwrap(), &"branch/main".into());
        assert_eq!(prm.get("cell").unwrap(), &"revision".into());
    }

    assert_eq!(
        read.len(),
        2,
        "a resolve carries only what it is scoped to: {read:?}"
    );
}

/// Links that name themselves in the ability path do not thereby appear
/// in `prm`. `Use` is an `Attenuation` (it contributes `use` to `cmd`)
/// and a unit struct (it contributes nothing to `prm`) — the two axes
/// are independent, which is the property a path refactor relies on.
#[dialog_common::test]
fn it_keeps_path_segments_out_of_parameters() {
    let chain = || CellScope::new(subject(), "s", "c");

    let prm = parameters(&chain().resolve());
    assert!(
        !prm.contains_key("use") && !prm.contains_key("memory"),
        "path-only links must not leak into prm: {prm:?}"
    );
    assert_eq!(chain().resolve().ability(), "/use/get/memory/cell");
}

fn revision() -> Revision {
    Revision::new(
        TreeReference::default(),
        "did:key:zMain".parse::<Entity>().expect("valid entity"),
        did!("key:zIssuer"),
    )
}

/// An empty create sends exactly the parameters it always has: the
/// optional revision is omitted, not sent as null, so a delegation
/// caveated on the branch name alone keeps matching.
#[dialog_common::test]
fn it_keeps_an_empty_create_to_its_name() {
    let prm = parameters(&subject().writer().branches().branch("feature").create());

    assert_eq!(prm.get("name").unwrap(), &"feature".into());
    assert!(!prm.contains_key("revision"), "{prm:?}");
}

/// A create at a revision and a delete both carry the revision, since
/// that is what each is about: where the branch will point, and where
/// it must still point to be removed.
#[dialog_common::test]
fn it_carries_the_revision_a_create_or_delete_names() {
    let create = parameters(
        &subject()
            .writer()
            .branches()
            .branch("feature")
            .create()
            .revision(revision()),
    );
    let delete = parameters(
        &subject()
            .voider()
            .branches()
            .branch("feature")
            .delete(revision()),
    );

    assert!(create.contains_key("revision"), "{create:?}");
    assert!(delete.contains_key("revision"), "{delete:?}");
}

/// A switch carries the branch it points the replica at, and nothing
/// that would scope it to a branch name: the branch need not be one the
/// replica holds.
#[dialog_common::test]
fn it_carries_the_branch_a_switch_names() {
    let branch: Entity = "did:key:zFeature".parse().expect("valid entity");
    let switch = parameters(&subject().writer().branches().switch(branch.clone()));

    assert_eq!(
        switch.get("branch").unwrap(),
        &branch.to_string().into(),
        "a switch carries its branch: {switch:?}"
    );
    assert_eq!(switch.len(), 1, "and nothing else: {switch:?}");
}
