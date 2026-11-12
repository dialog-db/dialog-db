//! Run documents in the notation on a repository kept in memory.
//!
//! ```sh
//! cargo run -p dialog-notation --example notebook -- \
//!     rust/dialog-notation/examples/todo.clj rust/dialog-notation/examples/done.clj
//! ```
//!
//! Each file is one document, submitted in the order given: what it
//! writes, installs and issues is one transaction, and its queries run
//! against the result. With no file, the document is read from standard
//! input.

use std::io::Read as _;

use anyhow::Result;

#[path = "notebook/session.rs"]
mod session;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut documents = Vec::new();
    for path in std::env::args().skip(1) {
        documents.push((path.clone(), std::fs::read_to_string(path)?));
    }
    if documents.is_empty() {
        let mut text = String::new();
        std::io::stdin().read_to_string(&mut text)?;
        documents.push(("standard input".into(), text));
    }

    let mut notebook = session::Notebook::open().await?;
    for (name, text) in documents {
        println!("; {name}");
        let (report, diagnostics) = notebook.submit(&text).await?;
        for row in report {
            println!("{row}");
        }
        for diagnostic in diagnostics {
            println!("; warning: {}: {}", diagnostic.message, diagnostic.form);
        }
    }
    if notebook.held() > 0 {
        println!(
            "; {} rule(s) hold a struct, which Dialog-DB cannot keep yet: \
             they ran here and are not on the branch",
            notebook.held()
        );
    }
    Ok(())
}
