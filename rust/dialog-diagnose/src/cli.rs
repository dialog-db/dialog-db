use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "diagnose")]
#[command(bin_name = "diagnose")]
#[command(about = "Dev tools for Dialog databases", long_about = None)]
pub struct DiagnoseCli {
    #[arg(short, long)]
    pub tree: bool,

    pub csv: PathBuf,
}
