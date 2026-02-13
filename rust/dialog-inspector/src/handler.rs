//! Fetch-like request handler for the inspector.
//!
//! This module defines a platform-agnostic request/response abstraction
//! that can be bound to a service worker `fetch` event, a web extension
//! background script, or any other request-response context.
//!
//! The handler serves two purposes:
//!
//! 1. **API endpoints** (`/api/*`): Return JSON data about discovered
//!    databases, their revisions, and facts. The Leptos UI calls these.
//! 2. **Static assets** (`/`): In a service worker context, the handler
//!    can serve the panel UI itself at the root URL.
//!
//! # Design
//!
//! The handler is defined as a trait ([`InspectorHandler`]) with a default
//! implementation, so callers only need to implement the IO glue (reading
//! request path, writing response bytes). The trait is deliberately not
//! async-trait-based to keep it simple for initial use; callers wrap the
//! sync routing logic in their own async context.

use serde::Serialize;

/// A minimal request representation for routing.
pub struct InspectorRequest {
    /// The request path, e.g. "/api/databases"
    pub path: String,
    /// Query parameters as key-value pairs
    pub query: Vec<(String, String)>,
}

/// A response from the inspector handler.
pub struct InspectorResponse {
    /// HTTP status code
    pub status: u16,
    /// Content-Type header value
    pub content_type: String,
    /// Response body bytes
    pub body: Vec<u8>,
}

impl InspectorResponse {
    /// Create a JSON response.
    pub fn json<T: Serialize>(data: &T) -> Self {
        match serde_json::to_vec(data) {
            Ok(body) => Self {
                status: 200,
                content_type: "application/json".into(),
                body,
            },
            Err(e) => Self::error(500, &format!("JSON serialization failed: {e}")),
        }
    }

    /// Create an error response.
    pub fn error(status: u16, message: &str) -> Self {
        let body = serde_json::json!({ "error": message });
        Self {
            status,
            content_type: "application/json".into(),
            body: serde_json::to_vec(&body).unwrap_or_default(),
        }
    }

    /// Create a 404 Not Found response.
    pub fn not_found() -> Self {
        Self::error(404, "Not found")
    }
}

/// Route an incoming request to the appropriate handler.
///
/// This is a synchronous routing function that determines which API endpoint
/// to call. The actual data fetching (which is async) must be done by the
/// caller after matching. This two-phase approach keeps the router simple
/// and lets the caller decide how to run async code (e.g., `wasm_bindgen_futures::spawn_local`).
///
/// # Routes
///
/// - `GET /api/databases` — List all discovered dialog-db instances
/// - `GET /api/databases/:name` — Get summary for a specific database
/// - `GET /api/databases/:name/facts?the=...&of=...&limit=N` — Query facts
///
/// Returns the matched [`Route`] or `None` if no route matches.
pub fn route(request: &InspectorRequest) -> Option<Route> {
    let path = request.path.trim_end_matches('/');

    if path == "/api/databases" {
        return Some(Route::ListDatabases);
    }

    if let Some(rest) = path.strip_prefix("/api/databases/") {
        if let Some(name) = rest.strip_suffix("/facts") {
            let the = query_param(&request.query, "the");
            let of = query_param(&request.query, "of");
            let limit = query_param(&request.query, "limit")
                .and_then(|s| s.parse().ok())
                .unwrap_or(100);

            return Some(Route::QueryFacts {
                name: name.to_string(),
                attribute: the,
                entity: of,
                limit,
            });
        }

        if !rest.contains('/') {
            return Some(Route::DatabaseSummary {
                name: rest.to_string(),
            });
        }
    }

    None
}

/// A matched API route with extracted parameters.
#[derive(Debug, Clone)]
pub enum Route {
    /// List all discovered dialog-db instances
    ListDatabases,
    /// Get summary for a specific database
    DatabaseSummary {
        /// Database name/identifier
        name: String,
    },
    /// Query facts from a database
    QueryFacts {
        /// Database name/identifier
        name: String,
        /// Attribute filter
        attribute: Option<String>,
        /// Entity filter
        entity: Option<String>,
        /// Maximum number of results
        limit: usize,
    },
}

fn query_param(query: &[(String, String)], key: &str) -> Option<String> {
    query
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
}

/// Serializable representation of a database for API responses.
#[derive(Debug, Clone, Serialize)]
pub struct DatabaseEntry {
    /// Database name/identifier
    pub name: String,
    /// IndexedDB schema version
    pub version: u64,
}

/// Serializable summary of a database for API responses.
#[derive(Debug, Clone, Serialize)]
pub struct DatabaseSummaryResponse {
    /// Database name/identifier
    pub identifier: String,
    /// Current revision hash (base58)
    pub revision: String,
    /// Whether the database is empty
    pub is_empty: bool,
}

/// Serializable fact for API responses.
#[derive(Debug, Clone, Serialize)]
pub struct FactEntry {
    /// Attribute (predicate)
    pub the: String,
    /// Entity (subject)
    pub of: String,
    /// Value (object) as display string
    pub is: String,
    /// Value type tag
    pub value_type: String,
    /// Causal reference (base58), if any
    pub cause: Option<String>,
}
