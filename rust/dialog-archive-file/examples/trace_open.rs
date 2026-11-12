//! Traces what opening archive files reads and writes, for the browser
//! spike's failover (`research/single-file-format/browser`, round four): a
//! replacement worker opens the file another left and replays this to
//! recover it.
//!
//! ```sh
//! cargo run -p dialog-archive-file --features trace --example trace_open -- \
//!   <out.trace> <directory> <file>...
//! ```
//!
//! Every file is opened under `directory`, in the order given (the trace
//! numbers them so), as one operation named `open`. The archives are not
//! closed: the process exits once the trace is written, as one whose tab
//! went away would, so nothing a close writes is in the trace. Give it
//! copies: opening may write (a spare extent, a cut tail).

#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use dialog_archive_file::{FileArchive, trace};
    use std::path::Path;

    let args: Vec<String> = std::env::args().skip(1).collect();
    let [out, under, files @ ..] = args.as_slice() else {
        return Err("usage: trace_open <out.trace> <directory> <file>...".into());
    };
    trace::start(Path::new(out), Path::new(under))?;
    trace::begin("open");
    let mut archives = Vec::new();
    for file in files {
        archives.push(FileArchive::open(file)?);
    }
    trace::end();
    trace::finish()?;
    std::mem::forget(archives);
    std::process::exit(0);
}

#[cfg(not(unix))]
fn main() {
    eprintln!("trace_open traces on Unix only");
}
