//! Message protocol and request handling for the inspector.
//!
//! This module defines the request/response types that form the communication
//! protocol between the UI panel and the inspection backend. The types are
//! [`serde`]-serializable so they can travel over `chrome.tabs.sendMessage`
//! as JSON, from the devtools panel to the content script (which runs in
//! the host page's origin and has IndexedDB access).
//!
//! # Message flow
//!
//! ```text
//! Panel (extension origin)          Content script (host page origin)
//! ┌─────────────────────┐           ┌──────────────────────────┐
//! │  UI sends Request   │──message─▸│  receives Request        │
//! │                     │           │  calls dispatch()        │
//! │  UI receives        │◂─message──│  sends back Response     │
//! │  Response           │           │                          │
//! └─────────────────────┘           └──────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/// A request from the UI panel to the inspection backend.
///
/// Each variant maps to an operation that requires host-page IndexedDB access.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum Request {
    /// Discover all dialog-db instances in the current origin.
    #[serde(rename = "list_databases")]
    ListDatabases,

    /// Get summary (identifier, revision, empty?) for a specific database.
    #[serde(rename = "database_summary")]
    DatabaseSummary { name: String },

    /// Query facts from a database.
    #[serde(rename = "query_facts")]
    QueryFacts {
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        attribute: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        entity: Option<String>,
        #[serde(default = "default_limit")]
        limit: usize,
    },
}

fn default_limit() -> usize {
    100
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

/// A response from the inspection backend to the UI panel.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum Response {
    /// List of discovered databases.
    #[serde(rename = "databases")]
    Databases { entries: Vec<DatabaseEntry> },

    /// Summary of a single database.
    #[serde(rename = "summary")]
    Summary(DatabaseSummaryResponse),

    /// Query results (list of facts).
    #[serde(rename = "facts")]
    Facts { rows: Vec<FactEntry> },

    /// An error occurred while handling the request.
    #[serde(rename = "error")]
    Error { message: String },
}

// ---------------------------------------------------------------------------
// Payload types
// ---------------------------------------------------------------------------

/// A discovered database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseEntry {
    pub name: String,
    pub version: u64,
}

/// Summary of an inspected database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseSummaryResponse {
    pub identifier: String,
    pub revision: String,
    pub is_empty: bool,
}

/// A single fact formatted for display.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactEntry {
    pub the: String,
    pub of: String,
    pub is: String,
    pub value_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
}
