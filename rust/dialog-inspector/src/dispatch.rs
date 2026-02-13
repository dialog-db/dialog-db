//! Request dispatcher — executes [`Request`]s against IndexedDB.
//!
//! This module provides [`dispatch`], the async function that takes a
//! [`handler::Request`] and produces a [`handler::Response`] by calling
//! the [`discovery`] and [`inspect`] modules. It must run in a context
//! that has same-origin access to the host page's IndexedDB:
//!
//! - **Standalone mode**: called directly in the panel's WASM context
//!   (panel and page share the same origin).
//! - **Extension mode**: called inside a content script that was injected
//!   into the inspected page.

use crate::discovery;
use crate::handler::*;
use crate::inspect::InspectedDatabase;

/// Execute a [`Request`] against the current origin's IndexedDB and return
/// the corresponding [`Response`].
pub async fn dispatch(request: Request) -> Response {
    match request {
        Request::ListDatabases => match discovery::discover_instances().await {
            Ok(instances) => Response::Databases {
                entries: instances
                    .into_iter()
                    .map(|db| DatabaseEntry {
                        name: db.name,
                        version: db.version,
                    })
                    .collect(),
            },
            Err(e) => Response::Error {
                message: format!("{e:?}"),
            },
        },

        Request::DatabaseSummary { name } => {
            let info = discovery::DatabaseInfo { name, version: 0 };
            match InspectedDatabase::open(&info).await {
                Ok(db) => match db.summary().await {
                    Ok(s) => Response::Summary(DatabaseSummaryResponse {
                        identifier: s.identifier,
                        revision: s.revision,
                        is_empty: s.is_empty,
                    }),
                    Err(e) => Response::Error {
                        message: format!("{e:?}"),
                    },
                },
                Err(e) => Response::Error {
                    message: format!("{e:?}"),
                },
            }
        }

        Request::QueryFacts {
            name,
            attribute,
            entity,
            limit,
        } => {
            let info = discovery::DatabaseInfo { name, version: 0 };
            match InspectedDatabase::open(&info).await {
                Ok(db) => {
                    match db
                        .query_facts(attribute.as_deref(), entity.as_deref(), limit)
                        .await
                    {
                        Ok(facts) => Response::Facts {
                            rows: facts
                                .into_iter()
                                .map(|f| FactEntry {
                                    the: f.the,
                                    of: f.of,
                                    is: f.is,
                                    value_type: f.value_type,
                                    cause: f.cause,
                                })
                                .collect(),
                        },
                        Err(e) => Response::Error {
                            message: format!("{e:?}"),
                        },
                    }
                }
                Err(e) => Response::Error {
                    message: format!("{e:?}"),
                },
            }
        }
    }
}
