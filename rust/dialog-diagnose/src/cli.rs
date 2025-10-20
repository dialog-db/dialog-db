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

    /// Path to the CSV file to import and analyze
    pub csv: PathBuf,
}
