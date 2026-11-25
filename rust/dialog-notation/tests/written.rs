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

/// `tree` with the label `from:` written `to:` wherever it stands, as
/// a caller that builds trees may write it and no reader would.
fn relabelled(tree: dialog_notation::Syntax, from: &str, to: &str) -> dialog_notation::Syntax {
    use dialog_notation::{Parameters, Syntax};
    match tree {
        Syntax::Application(operator, operand) => Syntax::Application(
            Box::new(relabelled(*operator, from, to)),
            Parameters {
                items: operand
                    .items
                    .into_iter()
                    .map(|item| relabelled(item, from, to))
                    .collect(),
                terms: operand
                    .terms
                    .into_iter()
                    .map(|(label, term)| {
                        let label = if label == from { to.to_string() } else { label };
                        (label, relabelled(term, from, to))
                    })
                    .collect(),
            },
        ),
        other => other,
    }
}

/// A tree built through the public API and given to `compile` writes
/// nothing a document cannot: no field under the notation's namespace,
/// whatever its head's domain, and no field of a name that is no
/// attribute of a domain, `b/c:`. Each is refused as the reader refuses
/// it.
#[test]
fn it_compiles_no_tree_that_writes_what_a_document_cannot() -> Result<()> {
    use dialog_notation::Error;
    for (head, label) in [
        (
            "(assert (xyz.a ?l x: ?n))",
            "dialog.n.c.hudthsqxprxaqks3o67ts47m5i/is",
        ),
        ("(assert (xyz.a ?l x: ?n))", "b/c"),
        ("(xyz.a ?l x: ?n)", "dialog.n/x"),
        ("(xyz.a ?l x: ?n)", "b/c"),
    ] {
        let text = format!("(rule {head} (xyz.list ?l name: ?n))");
        let tree = relabelled(parse(&text)?.remove(0), "x", label);
        let compiled = compile(&tree);
        assert!(
            matches!(&compiled, Err(Error::Reserved(_) | Error::Refused(_))),
            "{tree}: {:?}",
            compiled.map(|steps| steps.len())
        );
    }
    Ok(())
}
