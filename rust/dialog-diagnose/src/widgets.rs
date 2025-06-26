//! UI widgets for the diagnose TUI application.
//!
//! This module contains all the ratatui widgets that make up the user interface,
//! including the main application layout, facts table, and tree explorer.

mod app;
pub use app::*;

mod facts;
pub use facts::*;

mod tree;
pub use tree::*;
