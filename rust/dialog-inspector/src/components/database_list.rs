//! Database list component.
//!
//! Discovers and displays all dialog-db instances via the bridge
//! (which dispatches directly or through the content script depending
//! on whether we're in standalone or extension mode).

use leptos::prelude::*;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::handler::{Request, Response};

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
/// On mount, sends a [`Request::ListDatabases`] through the bridge.
/// Clicking an entry calls `on_select` with the database name.
#[component]
pub fn DatabaseList(
    /// Callback invoked when a database is selected.
    on_select: impl Fn(String) + Send + Sync + 'static + Clone,
) -> impl IntoView {
    let databases = RwSignal::new(Vec::<DbListEntry>::new());
    let loading = RwSignal::new(true);
    let error_msg = RwSignal::new(Option::<String>::None);

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        wasm_bindgen_futures::spawn_local(async move {
            let response = crate::bridge::send(Request::ListDatabases).await;
            match response {
                Response::Databases { entries } => {
                    let items: Vec<DbListEntry> = entries
                        .into_iter()
                        .map(|e| DbListEntry {
                            name: e.name,
                            version: e.version,
                        })
                        .collect();
                    databases.set(items);
                }
                Response::Error { message } => {
                    error_msg.set(Some(message));
                }
                _ => {
                    error_msg.set(Some("Unexpected response".into()));
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
