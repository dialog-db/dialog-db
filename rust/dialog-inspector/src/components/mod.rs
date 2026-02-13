//! Leptos UI components for the inspector panel.
//!
//! The component tree is:
//!
//! ```text
//! InspectorApp
//! ├── DatabaseList          (sidebar: discovered databases)
//! │   └── DatabaseEntry     (single database row)
//! └── DatabaseDetail        (main panel: selected database)
//!     ├── summary header    (identifier, revision)
//!     └── FactBrowser       (query + results table)
//! ```

mod database_list;
mod database_detail;

pub use database_list::*;
pub use database_detail::*;

use leptos::prelude::*;

/// Root component for the inspector panel.
///
/// Manages the selected database state and renders the two-pane layout:
/// a database list sidebar and a detail panel for the selected database.
#[component]
pub fn InspectorApp() -> impl IntoView {
    let (selected_db, set_selected_db) = signal(Option::<String>::None);

    view! {
        <div class="inspector-root">
            <div class="inspector-sidebar">
                <h2>"Dialog Databases"</h2>
                <DatabaseList
                    on_select=move |name: String| set_selected_db.set(Some(name))
                />
            </div>
            <div class="inspector-main">
                {move || {
                    match selected_db.get() {
                        Some(name) => view! {
                            <DatabaseDetail name=name.clone() />
                        }.into_any(),
                        None => view! {
                            <div class="inspector-empty">
                                <p>"Select a database from the sidebar to inspect it."</p>
                            </div>
                        }.into_any(),
                    }
                }}
            </div>
        </div>
    }
}
