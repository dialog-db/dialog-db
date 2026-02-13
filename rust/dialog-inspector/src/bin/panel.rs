//! WASM entry point for the inspector panel UI.
//!
//! This binary is compiled to WebAssembly via Trunk and mounted to the
//! DOM body. It renders the Leptos-based inspector interface.

use dialog_inspector::components::InspectorApp;
use leptos::prelude::*;

fn main() {
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    console_error_panic_hook::set_once();

    mount_to_body(InspectorApp);
}
