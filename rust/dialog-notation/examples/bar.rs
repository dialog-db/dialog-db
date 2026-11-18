//! How far a document in the notation gets through the three phases.
//!
//! ```sh
//! cargo run -p dialog-notation --example bar -- \
//!     design/macros/sketches/core.clj design/macros/sketches/notebook.clj
//! ```
//!
//! Several documents are read in order as one, as tonk reads a component
//! joined with core. For each top-level form: whether it is read,
//! expanded and compiled,
//! and for those that stop, the first error. Then the three numbers,
//! and the stopping errors grouped by what they say.

use anyhow::Result;

#[path = "notebook/bar.rs"]
mod bar;

fn main() -> Result<()> {
    let mut paths: Vec<String> = std::env::args().skip(1).collect();
    if paths.is_empty() {
        paths = vec![
            "design/macros/sketches/core.clj".into(),
            "design/macros/sketches/notebook.clj".into(),
        ];
    }
    let texts = paths
        .iter()
        .map(std::fs::read_to_string)
        .collect::<Result<Vec<_>, _>>()?;
    let bar = bar::Bar::of_all(&texts.iter().map(String::as_str).collect::<Vec<_>>())?;
    let path = paths.join(" + ");
    println!(
        "; {path}: {} forms, {} read, {} expanded, {} compiled",
        bar.forms, bar.read, bar.expanded, bar.compiled
    );
    for (said, stops) in bar.groups() {
        let forms: Vec<String> = stops
            .iter()
            .map(|stop| match stop.form {
                Some(at) => at.to_string(),
                None => "?".into(),
            })
            .collect();
        println!("{:>3}  {said}  [forms {}]", stops.len(), forms.join(" "));
    }
    Ok(())
}
