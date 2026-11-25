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

/// The rule `(rule HEAD (xyz.list ?l name: ?n))` with HEAD built by hand
/// as `(WORD (RELATION ?l LABEL: ?n))`, or bare when WORD is none.
fn ruled(word: Option<&str>, relation: &str, label: &str) -> dialog_notation::Syntax {
    use dialog_notation::{Parameters, Syntax};
    let read = Syntax::Application(
        Box::new(Syntax::Reference(relation.into())),
        Parameters {
            items: vec![Syntax::Variable("l".into())],
            terms: [(label.to_string(), Syntax::Variable("n".into()))].into(),
        },
    );
    let head = match word {
        Some(word) => Syntax::Application(
            Box::new(Syntax::Reference(word.into())),
            Parameters {
                items: vec![read],
                terms: Default::default(),
            },
        ),
        None => read,
    };
    let premise = parse("(xyz.list ?l name: ?n)").unwrap().remove(0);
    Syntax::Application(
        Box::new(Syntax::Reference("rule".into())),
        Parameters {
            items: vec![head, premise],
            terms: Default::default(),
        },
    )
}

/// A tree built by hand may name a derived attribute as the analyzer
/// does, `dialog.n.c/list/label`, as a head that derives it or as a
/// read, and nothing else: an assert, a retract, a command or a field
/// into it is refused, and so is a malformed name of one and any hash
/// of the notation's, `dialog.n.c.…`, wherever it stands.
#[test]
fn it_compiles_a_derived_attribute_built_by_hand_only_where_one_is_derived() -> Result<()> {
    use dialog_notation::Error;
    let derives = compile(&ruled(None, "dialog.n.c/list/label", "is"))?;
    assert!(
        matches!(derives.as_slice(), [Step::Deduce(_)]),
        "{derives:?}"
    );
    let reads = parse("(rule (xyz.out ?l v: ?n) (xyz.list ?l name: ?n))")?.remove(0);
    let reads = relabelled_relation(reads, "xyz.list", "dialog.n.c/list/label", "name", "is");
    assert!(compile(&reads).is_ok(), "{reads}");
    let mut refused = vec![
        ruled(Some("assert"), "dialog.n.c/list/label", "is"),
        ruled(Some("retract"), "dialog.n.c/list/label", "is"),
        ruled(Some("assert"), "xyz.a", "dialog.n.c/list/label/is"),
        ruled(None, "xyz.a", "dialog.n.c/list/label/is"),
    ];
    for malformed in [
        "dialog.n.c/x",
        "dialog.n.c//f",
        "dialog.n.c/a/",
        "dialog.n.c/a.b/f",
        "dialog.n.c/a/f.g",
        "dialog.n.c/a/f:",
        "dialog.n.c.hudthsqxprxaqks3o67ts47m5i",
        "dialog.n.c.anything",
    ] {
        refused.push(ruled(None, malformed, "is"));
        refused.push(ruled(None, "xyz.a", &format!("{malformed}/is")));
    }
    for tree in refused {
        let compiled = compile(&tree);
        assert!(
            matches!(&compiled, Err(Error::Reserved(_) | Error::Refused(_))),
            "{tree}: {:?}",
            compiled.map(|steps| steps.len())
        );
    }
    Ok(())
}

/// `tree` with the relation `from` written `to` and the label `old:`
/// written `new:` where it applies it.
fn relabelled_relation(
    tree: dialog_notation::Syntax,
    from: &str,
    to: &str,
    old: &str,
    new: &str,
) -> dialog_notation::Syntax {
    use dialog_notation::{Parameters, Syntax};
    match tree {
        Syntax::Application(operator, operand) => {
            let applies = matches!(&*operator, Syntax::Reference(name) if name == from);
            let operator = match applies {
                true => Box::new(Syntax::Reference(to.into())),
                false => Box::new(relabelled_relation(*operator, from, to, old, new)),
            };
            Syntax::Application(
                operator,
                Parameters {
                    items: operand
                        .items
                        .into_iter()
                        .map(|item| relabelled_relation(item, from, to, old, new))
                        .collect(),
                    terms: operand
                        .terms
                        .into_iter()
                        .map(|(label, term)| {
                            let label = if applies && label == old {
                                new.to_string()
                            } else {
                                label
                            };
                            (label, relabelled_relation(term, from, to, old, new))
                        })
                        .collect(),
                },
            )
        }
        other => other,
    }
}

/// What a document may not write, a hand-built tree may not either:
/// for each write the analyzer refuses, the same kernel rule built by
/// hand is refused by `compile`, as the same kind of refusal. The
/// notation's own record of descriptions, `syntax.meta`, is written by
/// nothing but the notation, as a head, an assert, a retract or a field;
/// a head's `this:` is the entity, and no field.
#[test]
fn it_compiles_no_write_a_document_is_refused() -> Result<()> {
    use dialog_notation::{Analyzer, Error, check};
    let kind = |error: &Error| std::mem::discriminant(error);
    for (word, relation, label, document) in [
        (
            None,
            "syntax.meta",
            "description",
            "(rule (syntax.meta ?l description: ?n) (xyz.list ?l name: ?n))",
        ),
        (
            Some("assert"),
            "syntax.meta",
            "description",
            "(rule (assert (syntax.meta ?l description: ?n)) (xyz.list ?l name: ?n))",
        ),
        (
            Some("retract"),
            "syntax.meta",
            "description",
            "(rule (retract (syntax.meta ?l description: ?n)) (xyz.list ?l name: ?n))",
        ),
        (
            None,
            "xyz.out",
            "this",
            "(rule (xyz.out ?l this: ?n) (xyz.list ?l name: ?n))",
        ),
        (
            Some("assert"),
            "xyz.out",
            "this",
            "(rule (assert (xyz.out ?l this: ?n)) (xyz.list ?l name: ?n))",
        ),
        (
            None,
            "dialog.n.anything",
            "v",
            "(rule (dialog.n.anything ?l v: ?n) (xyz.list ?l name: ?n))",
        ),
    ] {
        let checked = check(&mut Analyzer::new()?, document);
        let refused = checked
            .refusals()
            .find_map(|refused| refused.refusal.clone())
            .ok_or_else(|| anyhow::anyhow!("{document} was not refused"))?;
        let tree = ruled(word, relation, label);
        let compiled = compile(&tree);
        assert!(
            matches!(&compiled, Err(error) if kind(error) == kind(&refused)),
            "{tree}: {:?} where the document is refused as {refused:?}",
            compiled.map(|steps| steps.len())
        );
    }
    // A field of the record, written by its label from another head,
    // is refused too.
    for word in [None, Some("assert")] {
        let tree = ruled(word, "xyz.out", "syntax.meta/description");
        assert!(matches!(compile(&tree), Err(Error::Reserved(_))), "{tree}");
    }
    Ok(())
}

/// `check_forms` takes an embedder's forms, as text: each forgery
/// `compile` refuses, written out as a form and given to `check_forms`,
/// is refused there too, as the same kind of refusal, and nothing of it
/// compiles. The one difference is a derived attribute named as the
/// analyzer names it, `dialog.n.c/list/label`, which a tree may name and
/// is refused for writing, and which no text may name at all, so its
/// form is refused as `Reserved`. A head into the record
/// of descriptions or into the notation's namespace, `this:` in a head,
/// a hash of the notation's, a malformed derived attribute, an assert, a
/// retract and a command into a derived attribute, and a label that
/// names another domain's attribute or the notation's.
#[test]
fn it_refuses_in_check_forms_every_forgery_compile_refuses() -> Result<()> {
    use dialog_notation::{Error, Segment, check_forms};
    let kind = |error: &Error| std::mem::discriminant(error);
    let mut forged = vec![
        ruled(None, "syntax.meta", "description"),
        ruled(Some("assert"), "syntax.meta", "description"),
        ruled(Some("retract"), "syntax.meta", "description"),
        ruled(None, "xyz.out", "syntax.meta/description"),
        ruled(Some("assert"), "xyz.out", "syntax.meta/description"),
        ruled(None, "xyz.out", "this"),
        ruled(Some("assert"), "xyz.out", "this"),
        ruled(None, "dialog.n.anything", "v"),
        ruled(Some("assert"), "dialog.n.anything", "v"),
        ruled(Some("assert"), "dialog.n.c/list/label", "is"),
        ruled(Some("retract"), "dialog.n.c/list/label", "is"),
        ruled(Some("assert"), "xyz.a", "dialog.n.c/list/label/is"),
        ruled(None, "xyz.a", "dialog.n.c/list/label/is"),
        ruled(None, "xyz.a", "dialog.n/x"),
        ruled(Some("assert"), "xyz.a", "dialog.n/x"),
        ruled(None, "xyz.a", "b/c"),
        ruled(Some("assert"), "xyz.a", "b/c"),
        ruled(
            Some("assert"),
            "xyz.a",
            "dialog.n.c.hudthsqxprxaqks3o67ts47m5i/is",
        ),
    ];
    for malformed in [
        "dialog.n.c/x",
        "dialog.n.c//f",
        "dialog.n.c/a/",
        "dialog.n.c/a.b/f",
        "dialog.n.c/a/f.g",
        "dialog.n.c.hudthsqxprxaqks3o67ts47m5i",
        "dialog.n.c.anything",
    ] {
        forged.push(ruled(None, malformed, "is"));
        forged.push(ruled(None, "xyz.a", &format!("{malformed}/is")));
    }
    // A derived attribute concluded on the control layer.
    let derived =
        parse("(rule (xyz.list ?l name: (:control ?n)) (xyz.list ?l name: ?n))")?.remove(0);
    forged.push(relabelled_relation(
        derived,
        "xyz.list",
        "dialog.n.c/list/label",
        "name",
        "is",
    ));
    // A command into a derived attribute: its value on the control layer.
    let issued = parse("(rule (assert (xyz.list ?l name: (:control ?n))) (xyz.list ?l name: ?n))")?
        .remove(0);
    forged.push(relabelled_relation(
        issued,
        "xyz.list",
        "dialog.n.c/list/label",
        "name",
        "is",
    ));
    for tree in forged {
        let compiled = compile(&tree);
        let Err(refused) = &compiled else {
            panic!("{tree} compiles: {:?}", compiled.map(|steps| steps.len()));
        };
        let text = tree.printed()?;
        let checked = check_forms(&mut Analyzer::new()?, &text);
        let said = checked
            .refusals()
            .find_map(|said| said.refusal.clone())
            .ok_or_else(|| anyhow::anyhow!("{text} is not refused by check_forms"))?;
        // A document may not write a derived attribute as the analyzer
        // names it at all, so its text is refused for the name, where a
        // tree may name one and is refused for writing it.
        let expected = match text.contains("dialog.n.c/list/label") {
            true => Error::Reserved(String::new()),
            false => refused.clone(),
        };
        assert_eq!(
            kind(&said),
            kind(&expected),
            "{text}: {said:?} against {refused:?}"
        );
        assert!(checked.steps.is_empty(), "{text}: {:?}", checked.steps);
        assert!(
            checked
                .refusals()
                .all(|said| said.path.first() == Some(&Segment::Item(0))),
            "{text}"
        );
    }
    Ok(())
}
