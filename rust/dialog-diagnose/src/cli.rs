//! Command-line interface definitions for the diagnose tool.

use std::path::PathBuf;

use clap::Parser;

/// Command-line arguments for the diagnose tool.
///
/// This structure defines the available command-line options for configuring
/// the diagnose TUI application.
#[derive(Debug, Parser)]
#[command(name = "diagnose")]
#[command(bin_name = "diagnose")]
#[command(about = "Dev tools for Dialog databases", long_about = None)]
pub struct DiagnoseCli {
    /// Start with the tree view tab selected instead of the facts view
    #[arg(short, long)]
    pub tree: bool,

    /// Print the causal-context profile of the branch head stored in this
    /// file (a dag-cbor `revision` cell) and exit, instead of opening the
    /// TUI
    #[arg(long, value_name = "REVISION_FILE")]
    pub head: Option<PathBuf>,

    /// Path to the CSV file to import and analyze
    #[arg(required_unless_present = "head")]
    pub csv: Option<PathBuf>,
}
