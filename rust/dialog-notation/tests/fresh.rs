//! A tree the analyzer made in another process, compiled in this one:
//! nothing in it is a hash, so it compiles wherever it is given.

#![cfg(not(target_arch = "wasm32"))]

use anyhow::Result;
use dialog_notation::{Parameters, Step, Syntax, compile, parse};

/// A rule that derives `list/label`, as an analyzer anywhere writes it,
/// built here by hand as an embedder that stored it would rebuild it:
/// compiled in a process that analyzed nothing, it derives.
#[test]
fn it_compiles_an_analyzed_tree_it_did_not_analyze() -> Result<()> {
    let head = Syntax::Application(
        Box::new(Syntax::Reference("dialog.n.c/list/label".into())),
        Parameters {
            items: vec![Syntax::Variable("l".into())],
            terms: [("is".to_string(), Syntax::Variable("n".into()))].into(),
        },
    );
    let premise = parse("(xyz.list ?l name: (:one (:text ?n)))")?.remove(0);
    let rule = Syntax::Application(
        Box::new(Syntax::Reference("rule".into())),
        Parameters {
            items: vec![head, premise],
            terms: Default::default(),
        },
    );
    let steps = compile(&rule)?;
    assert!(matches!(steps.as_slice(), [Step::Deduce(_)]), "{steps:?}");
    Ok(())
}
