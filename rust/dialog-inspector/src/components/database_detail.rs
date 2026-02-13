//! Database detail view component.
//!
//! Displays summary information about a selected database and provides
//! an interface for querying its facts.

use leptos::prelude::*;

/// Detail view for a selected database.
///
/// Shows the database identifier, current revision, and a simple
/// fact query interface. Facts can be filtered by attribute.
#[component]
pub fn DatabaseDetail(
    /// The name/identifier of the database to inspect.
    name: String,
) -> impl IntoView {
    let revision = RwSignal::new(String::from("loading..."));
    let is_empty = RwSignal::new(false);
    let facts = RwSignal::new(Vec::<FactRow>::new());
    let query_attr = RwSignal::new(String::new());
    let facts_loading = RwSignal::new(false);
    let facts_error = RwSignal::new(Option::<String>::None);

    // Load summary on mount.
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        let name = name.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let db_info = crate::discovery::DatabaseInfo {
                name: name.clone(),
                version: 0, // version doesn't matter for opening
            };
            match crate::inspect::InspectedDatabase::open(&db_info).await {
                Ok(db) => match db.summary().await {
                    Ok(summary) => {
                        revision.set(summary.revision);
                        is_empty.set(summary.is_empty);
                    }
                    Err(e) => {
                        revision.set(format!("error: {:?}", e));
                    }
                },
                Err(e) => {
                    revision.set(format!("failed to open: {:?}", e));
                }
            }
        });
    }

    let name_for_query = name.clone();

    let do_query = move |_| {
        let attr = query_attr.get();
        if attr.is_empty() {
            return;
        }
        let _name = name_for_query.clone();
        facts_loading.set(true);
        facts_error.set(None);
        facts.set(Vec::new());

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            let name = _name;
            wasm_bindgen_futures::spawn_local(async move {
                let db_info = crate::discovery::DatabaseInfo {
                    name: name.clone(),
                    version: 0,
                };
                match crate::inspect::InspectedDatabase::open(&db_info).await {
                    Ok(db) => {
                        match db.query_facts(Some(&attr), None, 100).await {
                            Ok(results) => {
                                let rows: Vec<FactRow> = results
                                    .into_iter()
                                    .map(|f| FactRow {
                                        the: f.the,
                                        of: f.of,
                                        is: f.is,
                                        value_type: f.value_type,
                                        cause: f.cause,
                                    })
                                    .collect();
                                facts.set(rows);
                            }
                            Err(e) => {
                                facts_error.set(Some(format!("{:?}", e)));
                            }
                        }
                    }
                    Err(e) => {
                        facts_error.set(Some(format!("Failed to open: {:?}", e)));
                    }
                }
                facts_loading.set(false);
            });
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            facts_loading.set(false);
        }
    };

    view! {
        <div class="database-detail">
            <div class="db-header">
                <h2>{name.clone()}</h2>
                <div class="db-meta">
                    <dl>
                        <dt>"Revision"</dt>
                        <dd class="revision">{move || revision.get()}</dd>
                    </dl>
                    <dl>
                        <dt>"Empty"</dt>
                        <dd>{move || if is_empty.get() { "yes" } else { "no" }}</dd>
                    </dl>
                </div>
            </div>

            <div class="fact-query">
                <h3>"Query Facts"</h3>
                <div class="query-controls">
                    <input
                        type="text"
                        placeholder="attribute (e.g. profile/name)"
                        prop:value=move || query_attr.get()
                        on:input=move |ev| {
                            query_attr.set(event_target_value(&ev));
                        }
                    />
                    <button on:click=do_query>"Query"</button>
                </div>

                {move || {
                    if facts_loading.get() {
                        view! { <p class="loading">"Loading facts..."</p> }.into_any()
                    } else if let Some(err) = facts_error.get() {
                        view! { <p class="error">{err}</p> }.into_any()
                    } else {
                        let rows = facts.get();
                        if rows.is_empty() {
                            view! { <p class="empty">"No facts to display."</p> }.into_any()
                        } else {
                            view! {
                                <table class="fact-table">
                                    <thead>
                                        <tr>
                                            <th>"Attribute"</th>
                                            <th>"Entity"</th>
                                            <th>"Value"</th>
                                            <th>"Type"</th>
                                            <th>"Cause"</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {rows.into_iter().map(|row| {
                                            view! {
                                                <tr>
                                                    <td class="attr">{row.the}</td>
                                                    <td class="entity">{row.of}</td>
                                                    <td class="value">{row.is}</td>
                                                    <td class="type">{row.value_type}</td>
                                                    <td class="cause">{row.cause.unwrap_or_default()}</td>
                                                </tr>
                                            }
                                        }).collect::<Vec<_>>()}
                                    </tbody>
                                </table>
                            }.into_any()
                        }
                    }
                }}
            </div>
        </div>
    }
}

/// A row in the facts display table.
#[derive(Debug, Clone, PartialEq)]
struct FactRow {
    the: String,
    of: String,
    is: String,
    value_type: String,
    cause: Option<String>,
}
