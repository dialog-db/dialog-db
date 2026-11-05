//! `dialog-migrate`: runs the migrations this build of `dialog-migration`
//! was built with.
//!
//! Run as `dialog-migrate <migration> <arguments>`, or with no arguments to
//! list the migrations this build has.
//!
//! - `archive-file --spaces <directory>` moves every space under a
//!   directory (a profile directory holds the peer's own space and one per
//!   repository) into the file each space reads, `archive.dialog` beside the
//!   rest of the space.
//! - `archive-file <source> <target file> [catalog]` moves one store:
//!   `source` is a space's directory (or its `archive` directory), whose
//!   blocks the `fs` provider keeps a directory per catalog; or, with
//!   `catalog` given, a flat directory of one catalog's blocks, a file per
//!   block named by its hash. The target file is created, or added to.

// The library is native-only, so on wasm32 there is nothing to run.
#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(not(target_arch = "wasm32"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let migration = args.next();
    let rest: Vec<String> = args.collect();
    match migration.as_deref() {
        #[cfg(feature = "archive-file")]
        Some("archive-file") => archive_file::run(rest),
        _ => {
            drop(rest);
            let migrations: &[&str] = &[
                #[cfg(feature = "archive-file")]
                "archive-file",
            ];
            if migrations.is_empty() {
                eprintln!(
                    "this build has no migrations: install it with the feature of the one \
                     to run, such as `--features archive-file`"
                );
            } else {
                eprintln!(
                    "usage: dialog-migrate <{}> <arguments>",
                    migrations.join("|")
                );
            }
            std::process::exit(2);
        }
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "archive-file"))]
mod archive_file {
    use std::time::Instant;

    use dialog_archive_file::FileArchive;
    use dialog_migration::archive_file::{Directory, Report, transcode, transcode_spaces};

    const USAGE: &str = "usage: dialog-migrate archive-file --spaces <directory>\n       \
                         dialog-migrate archive-file <source> <target file> [catalog]";

    fn mib(bytes: u64) -> String {
        format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
    }

    fn print(report: &Report) {
        for catalog in &report.catalogs {
            println!(
                "  {}: {} roots; {} tree nodes as frames ({} as blocks), {} kept whole ({}), \
                 {} other blocks ({}){}",
                catalog.catalog,
                catalog.roots,
                catalog.frames,
                mib(catalog.frame_source_bytes),
                catalog.whole_nodes,
                mib(catalog.whole_node_bytes),
                catalog.others,
                mib(catalog.other_bytes),
                if catalog.corrupt.is_empty() {
                    String::new()
                } else {
                    format!(
                        "; {} files did not hash to their name",
                        catalog.corrupt.len()
                    )
                },
            );
        }
    }

    fn usage() -> ! {
        eprintln!("{USAGE}");
        std::process::exit(2);
    }

    pub fn run(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
        let started = Instant::now();
        match args.as_slice() {
            [flag, root] if flag == "--spaces" => {
                for (space, report) in transcode_spaces(root)? {
                    println!("{}", space.display());
                    print(&report);
                }
            }
            [source, target, rest @ ..] if rest.len() <= 1 => {
                let source = match rest.first() {
                    Some(catalog) => Directory::catalog(source, catalog.clone()),
                    None => Directory::space(source),
                };
                let archive = FileArchive::open(target)?;
                let report = transcode(&source, &archive)?;
                archive.sync()?;
                print(&report);
                let stats = archive.stats();
                println!(
                    "{target}: {} ({} frames, {} tokens, {} whole)",
                    mib(stats.file_bytes),
                    mib(stats.frame_bytes),
                    mib(stats.token_bytes),
                    mib(stats.block_bytes),
                );
            }
            _ => usage(),
        }
        println!("in {:.1}s", started.elapsed().as_secs_f64());
        Ok(())
    }
}
