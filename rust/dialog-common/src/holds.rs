//! Handles an environment keeps on behalf of the code running in it.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A handle an environment holds, type-erased. `Send + Sync` on native,
/// where an environment is shared across threads; unconstrained on wasm,
/// where handles may hold thread-local state.
#[cfg(not(target_arch = "wasm32"))]
pub type Held = Arc<dyn Any + Send + Sync>; // bare-send-ok: type-erased value needs real auto-trait bounds
/// A handle an environment holds, type-erased.
#[cfg(target_arch = "wasm32")]
pub type Held = Arc<dyn Any>;

/// An environment that keeps long-lived handles for the code running in
/// it: something opened once and kept warm, so the next caller reuses it
/// rather than opening it cold.
///
/// The environment does not know what it holds. The code that puts a
/// handle in is the code that takes it out, under a key of its own
/// choosing, and downcasts it -- so the environment's crate needs no
/// dependency on the handle's.
pub trait Holds {
    /// The handle held under `key`, if any.
    fn held(&self, key: &str) -> Option<Held>;

    /// Hold `handle` under `key`, replacing whatever was held there.
    fn hold(&self, key: String, handle: Held);
}

/// A map of held handles, for an environment to embed and delegate
/// [`Holds`] to. Clones share the map.
#[derive(Clone, Default)]
pub struct Holdings(Arc<Mutex<HashMap<String, Held>>>);

impl std::fmt::Debug for Holdings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let held = self.0.lock().map(|map| map.len()).unwrap_or_default();
        f.debug_struct("Holdings").field("held", &held).finish()
    }
}

impl Holds for Holdings {
    fn held(&self, key: &str) -> Option<Held> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(key)
            .cloned()
    }

    fn hold(&self, key: String, handle: Held) {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(key, handle);
    }
}

#[cfg(test)]
mod tests {
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::{Holdings, Holds};
    use std::sync::Arc;

    /// What is held under a key comes back as itself, to the code that
    /// knows its type, and every clone of the holdings sees it.
    #[dialog_common::test]
    fn it_returns_what_it_holds() {
        let holdings = Holdings::default();
        let shared = holdings.clone();
        holdings.hold("answer".into(), Arc::new(42u32));

        let held = shared.held("answer").expect("held");
        assert_eq!(held.downcast_ref::<u32>(), Some(&42));
        assert!(shared.held("missing").is_none());
    }
}
