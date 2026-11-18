//! How far a document in the notation gets through the three phases.
//!
//! ```sh
//! cargo run -p dialog-notation --example bar -- design/macros/sketches/notebook.clj
//! ```
//!
//! For each top-level form: whether it is read, expanded and compiled,
//! and for those that stop, the first error. Then the three numbers,
//! and the stopping errors grouped by what they say.

use anyhow::Result;

#[path = "notebook/bar.rs"]
mod bar;

fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "design/macros/sketches/notebook.clj".into());
    let text = std::fs::read_to_string(&path)?;
    let bar = bar::Bar::of(&text)?;
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
