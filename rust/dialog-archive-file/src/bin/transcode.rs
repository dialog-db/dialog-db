//! `dialog-transcode`: moves a block store into one archive file.
//!
//! Run as `dialog-transcode --spaces <directory>` to move every space under
//! a directory (a profile directory holds the peer's own space and one per
//! repository) into the file each space reads, `archive.dialog` beside the
//! rest of the space. Run as `dialog-transcode <source> <target file>
//! [catalog]` to move one store. `source` is a space's directory (or its `archive` directory), whose
//! blocks the `fs` provider keeps a directory per catalog; or, with
//! `catalog` given, a flat directory of one catalog's blocks, a file per
//! block named by its hash. The target file is created, or added to.

// The library is native-only, so on wasm32 there is nothing to run.
#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(not(target_arch = "wasm32"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    native::main()
}

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::time::Instant;

    use dialog_archive_file::FileArchive;
    use dialog_archive_file::transcode::{Directory, Report, transcode, transcode_spaces};

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

    pub fn main() -> Result<(), Box<dyn std::error::Error>> {
        let mut args = std::env::args().skip(1);
        let usage =
            "usage: dialog-transcode --spaces <directory> | <source> <target file> [catalog]";
        let first = args.next().unwrap_or_else(|| {
            eprintln!("{usage}");
            std::process::exit(2);
        });
        if first == "--spaces" {
            let root = args.next().unwrap_or_else(|| {
                eprintln!("{usage}");
                std::process::exit(2);
            });
            let started = Instant::now();
            for (space, report) in transcode_spaces(&root)? {
                println!("{}", space.display());
                print(&report);
            }
            println!("in {:.1}s", started.elapsed().as_secs_f64());
            return Ok(());
        }
        let (source, Some(target)) = (first, args.next()) else {
            eprintln!("{usage}");
            std::process::exit(2);
        };
        let source = match args.next() {
            Some(catalog) => Directory::catalog(&source, catalog),
            None => Directory::space(&source),
        };
        let archive = FileArchive::open(&target)?;
        let started = Instant::now();
        let report = transcode(&source, &archive)?;
        archive.sync()?;
        print(&report);
        let stats = archive.stats();
        println!(
            "{}: {} in {:.1}s ({} frames, {} tokens, {} whole)",
            target,
            mib(stats.file_bytes),
            started.elapsed().as_secs_f64(),
            mib(stats.frame_bytes),
            mib(stats.token_bytes),
            mib(stats.block_bytes),
        );
        Ok(())
    }
}
