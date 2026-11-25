//! The names of derived attributes a process keeps, in a process of
//! their own: what is made here lets go of what other tests made.

#![cfg(not(target_arch = "wasm32"))]

use anyhow::Result;
use dialog_notation::{Analyzer, Step, compile, parse};

/// A tree the analyzer made, which derives an attribute, compiles in
/// the process that made it for as long as the process runs, however
/// many derived attributes are named after it: what may be compiled
/// does not hang on which names are still kept for showing.
#[test]
fn it_compiles_a_derived_attribute_made_long_ago() -> Result<()> {
    let list = "(concept (list ?l name: ?n)
         (xyz.list/name of: ?l is: ?n as: :text cardinality: :one))";
    let first = Analyzer::new()?.analyze(&parse(&format!(
        "{list} (rule (list/label ?l ?n) (list ?l name: ?n))"
    ))?);
    assert!(first.diagnostics.is_empty(), "{:?}", first.diagnostics);
    // More than the names a process keeps for showing, 4096.
    let many: String = (0..4200)
        .map(|at| format!("(rule (list/n{at} ?l ?n) (list ?l name: ?n))\n"))
        .collect();
    let later = Analyzer::new()?.analyze(&parse(&format!("{list}\n{many}"))?);
    assert!(later.diagnostics.is_empty(), "{:?}", later.diagnostics);
    for tree in &first.analyzed {
        let steps = compile(tree).map_err(|error| anyhow::anyhow!("{tree}: {error}"))?;
        assert!(steps.iter().all(|step| !matches!(step, Step::View(_))));
    }
    Ok(())
}
