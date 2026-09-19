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

use dialog_capability::{Subject, did};
use dialog_effects::prelude::*;
use dialog_ucan::parameters;

fn subject() -> Subject {
    Subject::from(did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"))
}

/// A cell capability scopes its holder by space and cell name, and both
/// travel in `prm`. These are the values an authorizer matches a
/// delegation's caveats against, so they may not move or be renamed.
#[dialog_common::test]
fn it_preserves_memory_parameters() {
    let cell = || subject().memory().space("branch/main").cell("revision");

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

/// The scoping names come from the chain alone: a chain carries them
/// before any effect is invoked on it, which is why an effect's path
/// spelling cannot affect them.
#[dialog_common::test]
fn it_derives_parameters_from_the_chain_not_the_effect() {
    let cell = || subject().memory().space("branch/main").cell("revision");

    let without_effect = parameters(&cell());
    let with_effect = parameters(&cell().resolve());

    assert_eq!(
        without_effect, with_effect,
        "Resolve carries no fields, so invoking it adds nothing to prm"
    );
    assert_eq!(without_effect.get("space").unwrap(), &"branch/main".into());
    assert_eq!(without_effect.get("cell").unwrap(), &"revision".into());
}

/// Links that name themselves in the ability path do not thereby appear
/// in `prm`. `Use` is an `Attenuation` (it contributes `use` to `cmd`)
/// and a unit struct (it contributes nothing to `prm`) — the two axes
/// are independent, which is the property a path refactor relies on.
#[dialog_common::test]
fn it_keeps_path_segments_out_of_parameters() {
    let chain = || subject().memory().space("s").cell("c");

    let prm = parameters(&chain().resolve());
    assert!(
        !prm.contains_key("use") && !prm.contains_key("memory"),
        "path-only links must not leak into prm: {prm:?}"
    );
    assert_eq!(chain().resolve().ability(), "/use/get/memory/cell");
}
