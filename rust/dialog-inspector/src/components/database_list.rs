//! Database list component.
//!
//! Discovers and displays all dialog-db instances in the current origin.

use leptos::prelude::*;

/// A discovered database entry for the list.
#[derive(Debug, Clone, PartialEq)]
pub struct DbListEntry {
    /// Database name
    pub name: String,
    /// IndexedDB version
    pub version: u64,
}

/// Sidebar component that lists all discovered dialog-db instances.
///
/// On mount, this calls the discovery module to enumerate IndexedDB databases,
/// probes each one for the dialog-db schema, and displays the results.
/// Clicking an entry calls `on_select` with the database name.
#[component]
pub fn DatabaseList(
    /// Callback invoked when a database is selected.
    on_select: impl Fn(String) + Send + Sync + 'static + Clone,
) -> impl IntoView {
    let databases = RwSignal::new(Vec::<DbListEntry>::new());
    let loading = RwSignal::new(true);
    let error_msg = RwSignal::new(Option::<String>::None);

    // Discover databases on mount.
    // On non-wasm targets this is a no-op stub; the real discovery
    // only runs in the browser.
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        use wasm_bindgen_futures::spawn_local;
        spawn_local(async move {
            match crate::discovery::discover_instances().await {
                Ok(instances) => {
                    let entries: Vec<DbListEntry> = instances
                        .into_iter()
                        .map(|info| DbListEntry {
                            name: info.name,
                            version: info.version,
                        })
                        .collect();
                    databases.set(entries);
                }
                Err(e) => {
                    error_msg.set(Some(format!("Discovery failed: {:?}", e)));
                }
            }
            loading.set(false);
        });
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    {
        loading.set(false);
    }

    view! {
        <div class="database-list">
            {move || {
                if loading.get() {
                    view! { <p class="loading">"Discovering databases..."</p> }.into_any()
                } else if let Some(err) = error_msg.get() {
                    view! { <p class="error">{err}</p> }.into_any()
                } else {
                    let entries = databases.get();
                    if entries.is_empty() {
                        view! { <p class="empty">"No dialog-db instances found."</p> }.into_any()
                    } else {
                        let on_select = on_select.clone();
                        view! {
                            <ul>
                                {entries.into_iter().map(|entry| {
                                    let name = entry.name.clone();
                                    let on_select = on_select.clone();
                                    view! {
                                        <li>
                                            <button
                                                class="db-entry"
                                                on:click=move |_| on_select(name.clone())
                                            >
                                                <span class="db-name">{entry.name.clone()}</span>
                                                <span class="db-version">"v"{entry.version.to_string()}</span>
                                            </button>
                                        </li>
                                    }
                                }).collect::<Vec<_>>()}
                            </ul>
                        }.into_any()
                    }
                }
            }}
        </div>
    }
}
