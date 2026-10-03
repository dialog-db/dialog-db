//! The canonical spelling of a rule body.
//!
//! Two authors writing the same rule name its local variables
//! differently and list its premises in a different order. A rule's
//! identity is a hash of its body, and everything keyed by that
//! identity (the entity its facts are stored under, the plan cache,
//! the body memo) should treat those two spellings as one rule. So a
//! rule compiles to a canonical spelling: its local variables are
//! renamed by a labeling that depends only on the body's structure,
//! and its premises are sorted by their encoding under that labeling.
//!
//! The head's operands are fixed names (`this` and the concept's field
//! names) and are not renamed: the head is part of the identity. A
//! local is every other named variable.
//!
//! The labeling is colour refinement followed by individualisation:
//! each local starts with a colour summarising where it occurs (which
//! premise shape, under which parameter), and the colours are refined
//! by the colours of the locals each occurrence sits beside until the
//! partition stops splitting. Locals that still share a colour are
//! structurally interchangeable as far as refinement can tell; each
//! is tried first in turn, refined again, and the spelling with the
//! smallest encoding wins. The search is exhaustive up to a bound on
//! its leaves, so the result is the same for every spelling of a body
//! within that bound, and deterministic for a given spelling beyond
//! it.

use std::collections::{BTreeMap, BTreeSet};

use crate::error::TypeError;
use crate::premise::Premise;
use crate::reduce::ReduceSpec;
use crate::rule::deductive::rename::{Rename, rename_premises, rename_term, variables};
use crate::term::Term;
use crate::types::Any;

/// The most leaves the tie-breaking search visits before settling for
/// the best spelling found so far.
const SEARCH_BOUND: usize = 1024;

/// The placeholder every local is renamed to when a premise's shape is
/// taken, so the shape says where locals occur but not which.
const HOLE: &str = "?";

/// A body in its canonical spelling.
#[derive(Debug, Clone)]
pub(crate) struct Canonical {
    /// The premises renamed and sorted.
    pub premises: Vec<Premise>,
    /// The reduce clause with its inputs renamed, in head-field order.
    pub reduce: Vec<(String, ReduceSpec)>,
    /// What each local was renamed to.
    pub rename: Rename,
}

/// One place a local may occur: a premise or a reduce entry, with the
/// locals it names under their parameter keys.
struct Atom {
    /// The atom with every local replaced by the hole, encoded.
    shape: Vec<u8>,
    /// The locals it names, by parameter key, in key order.
    slots: Vec<(String, String)>,
}

/// The canonical spelling of `premises` and `reduce` under `fixed`
/// names, which are the head's operands and stay as they are. `None`
/// for a body the formal notation cannot express (one reading an
/// attribute through a raw scan, as a concept's implicit rule does):
/// such a rule has no encoding, hence no identity, and keeps the
/// spelling it was given.
pub(crate) fn canonicalize(
    fixed: &BTreeSet<String>,
    premises: &[Premise],
    reduce: &[(String, ReduceSpec)],
) -> Result<Option<Canonical>, TypeError> {
    let mut locals: BTreeSet<String> = variables(premises);
    for (_, spec) in reduce {
        if let Some(name) = spec.of.name() {
            locals.insert(name.to_string());
        }
    }
    let locals: Vec<String> = locals.difference(fixed).cloned().collect();
    let holes: Rename = locals
        .iter()
        .map(|name| (name.clone(), HOLE.to_string()))
        .collect();

    let mut atoms = Vec::with_capacity(premises.len() + reduce.len());
    let holed = rename_premises(premises, &holes)?;
    for (premise, shape) in premises.iter().zip(&holed) {
        let Some(shape) = encode_premise(shape) else {
            return Ok(None);
        };
        atoms.push(Atom {
            shape,
            slots: slots(premise.parameters().iter(), &locals),
        });
    }
    for (field, spec) in reduce {
        let shape = serde_ipld_dagcbor::to_vec(&(
            "reduce",
            field,
            ReduceSpec {
                apply: spec.apply,
                of: rename_term(&spec.of, &holes),
            },
        ))
        .map_err(|error| TypeError::TypeInference {
            reason: format!("encoding a reduce entry: {error}"),
        })?;
        atoms.push(Atom {
            shape,
            slots: slots(
                [("of".to_string(), spec.of.clone())]
                    .iter()
                    .map(|(key, term)| (key, term)),
                &locals,
            ),
        });
    }

    let names = label(&locals, &atoms, fixed, premises, reduce)?;
    Ok(Some(spell(premises, reduce, &names)?.1))
}

/// The names a labeling assigns the locals: the spelling with the
/// smallest encoding among those the search visits.
fn label(
    locals: &[String],
    atoms: &[Atom],
    fixed: &BTreeSet<String>,
    premises: &[Premise],
    reduce: &[(String, ReduceSpec)],
) -> Result<Rename, TypeError> {
    if locals.is_empty() {
        return Ok(Rename::new());
    }
    let colours = refine(initial(locals, atoms), locals, atoms);
    let mut best: Option<(Vec<u8>, Rename)> = None;
    let mut leaves = 0usize;
    search(
        colours,
        locals,
        atoms,
        fixed,
        premises,
        reduce,
        &mut best,
        &mut leaves,
    )?;
    Ok(best.map(|(_, names)| names).unwrap_or_default())
}

/// Individualise and refine: at a leaf every local has its own colour
/// and the spelling it induces is a candidate; elsewhere the first
/// shared colour class is split by trying each member first.
#[allow(clippy::too_many_arguments)]
fn search(
    colours: BTreeMap<String, Vec<u8>>,
    locals: &[String],
    atoms: &[Atom],
    fixed: &BTreeSet<String>,
    premises: &[Premise],
    reduce: &[(String, ReduceSpec)],
    best: &mut Option<(Vec<u8>, Rename)>,
    leaves: &mut usize,
) -> Result<(), TypeError> {
    if *leaves >= SEARCH_BOUND {
        return Ok(());
    }
    let classes = classes(&colours, locals);
    match classes.iter().find(|members| members.len() > 1) {
        None => {
            *leaves += 1;
            let names = names(&classes, fixed);
            let (encoded, _) = spell(premises, reduce, &names)?;
            if best.as_ref().is_none_or(|(known, _)| encoded < *known) {
                *best = Some((encoded, names));
            }
        }
        Some(tied) => {
            for member in tied {
                let mut split = colours.clone();
                let colour = split.get_mut(member).expect("every local is coloured");
                *colour = hash(&[b"!", colour.as_slice()]);
                let refined = refine(split, locals, atoms);
                search(
                    refined, locals, atoms, fixed, premises, reduce, best, leaves,
                )?;
                if *leaves >= SEARCH_BOUND {
                    break;
                }
            }
        }
    }
    Ok(())
}

/// Each local's starting colour: the shapes and keys it occurs under.
fn initial(locals: &[String], atoms: &[Atom]) -> BTreeMap<String, Vec<u8>> {
    locals
        .iter()
        .map(|local| {
            let mut occurrences: Vec<Vec<u8>> = atoms
                .iter()
                .flat_map(|atom| {
                    atom.slots
                        .iter()
                        .filter(|(_, name)| name == local)
                        .map(|(key, _)| hash(&[&atom.shape, key.as_bytes()]))
                })
                .collect();
            occurrences.sort();
            (local.clone(), hash_all(&occurrences))
        })
        .collect()
}

/// Refine colours by the colours of the locals each occurrence sits
/// beside, until the partition stops splitting. A colour only ever
/// folds its own history in, so classes never merge.
fn refine(
    mut colours: BTreeMap<String, Vec<u8>>,
    locals: &[String],
    atoms: &[Atom],
) -> BTreeMap<String, Vec<u8>> {
    let mut distinct = count(&colours);
    loop {
        let next: BTreeMap<String, Vec<u8>> = locals
            .iter()
            .map(|local| {
                let mut occurrences: Vec<Vec<u8>> = atoms
                    .iter()
                    .flat_map(|atom| {
                        atom.slots
                            .iter()
                            .filter(|(_, name)| name == local)
                            .map(|(key, _)| {
                                let mut parts: Vec<&[u8]> = vec![&atom.shape, key.as_bytes()];
                                let neighbours: Vec<Vec<u8>> = atom
                                    .slots
                                    .iter()
                                    .map(|(other_key, other)| {
                                        hash(&[other_key.as_bytes(), &colours[other]])
                                    })
                                    .collect();
                                for neighbour in &neighbours {
                                    parts.push(neighbour);
                                }
                                hash(&parts)
                            })
                    })
                    .collect();
                occurrences.sort();
                let mut parts: Vec<&[u8]> = vec![&colours[local]];
                for occurrence in &occurrences {
                    parts.push(occurrence);
                }
                (local.clone(), hash(&parts))
            })
            .collect();
        let refined = count(&next);
        colours = next;
        if refined == distinct {
            return colours;
        }
        distinct = refined;
    }
}

fn count(colours: &BTreeMap<String, Vec<u8>>) -> usize {
    colours.values().collect::<BTreeSet<_>>().len()
}

/// The colour classes in colour order, each listing its members in
/// name order.
fn classes(colours: &BTreeMap<String, Vec<u8>>, locals: &[String]) -> Vec<Vec<String>> {
    let mut by_colour: BTreeMap<&[u8], Vec<String>> = BTreeMap::new();
    for local in locals {
        by_colour
            .entry(colours[local].as_slice())
            .or_default()
            .push(local.clone());
    }
    by_colour.into_values().collect()
}

/// Canonical names in class order: `~0`, `~1`, ... with the tilde
/// repeated as often as it takes to miss every fixed name.
fn names(classes: &[Vec<String>], fixed: &BTreeSet<String>) -> Rename {
    let mut prefix = String::from("~");
    while fixed.iter().any(|name| {
        name.starts_with(&prefix) && name[prefix.len()..].bytes().all(|b| b.is_ascii_digit())
    }) {
        prefix.push('~');
    }
    classes
        .iter()
        .flatten()
        .enumerate()
        .map(|(index, local)| (local.clone(), format!("{prefix}{index}")))
        .collect()
}

/// The body under `names`: premises renamed and sorted by encoding,
/// reduce inputs renamed, and the whole encoded for comparison.
fn spell(
    premises: &[Premise],
    reduce: &[(String, ReduceSpec)],
    names: &Rename,
) -> Result<(Vec<u8>, Canonical), TypeError> {
    let renamed = rename_premises(premises, names)?;
    let mut keyed: Vec<(Vec<u8>, Premise)> = renamed
        .into_iter()
        .map(|premise| {
            let key = encode_premise(&premise).ok_or_else(|| TypeError::TypeInference {
                reason: "a premise that encoded stopped encoding under other names".to_string(),
            })?;
            Ok((key, premise))
        })
        .collect::<Result<_, TypeError>>()?;
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    let reduce: Vec<(String, ReduceSpec)> = reduce
        .iter()
        .map(|(field, spec)| {
            (
                field.clone(),
                ReduceSpec {
                    apply: spec.apply,
                    of: rename_term(&spec.of, names),
                },
            )
        })
        .collect();
    let mut encoded = Vec::new();
    for (key, _) in &keyed {
        encoded.extend_from_slice(&(key.len() as u64).to_be_bytes());
        encoded.extend_from_slice(key);
    }
    let folds = serde_ipld_dagcbor::to_vec(&reduce).map_err(|error| TypeError::TypeInference {
        reason: format!("encoding a reduce clause: {error}"),
    })?;
    encoded.extend_from_slice(&folds);
    Ok((
        encoded,
        Canonical {
            premises: keyed.into_iter().map(|(_, premise)| premise).collect(),
            reduce,
            rename: names.clone(),
        },
    ))
}

/// A premise's encoding: its polarity, then its proposition. `None`
/// for a premise the formal notation cannot express.
fn encode_premise(premise: &Premise) -> Option<Vec<u8>> {
    let (tag, proposition) = match premise {
        Premise::Assert(proposition) => (0u8, proposition),
        Premise::Unless(negation) => (1u8, &negation.0),
    };
    let mut bytes = vec![tag];
    bytes.extend(serde_ipld_dagcbor::to_vec(proposition).ok()?);
    Some(bytes)
}

/// The locals among `parameters`, by key, in key order.
fn slots<'a>(
    parameters: impl Iterator<Item = (&'a String, &'a Term<Any>)>,
    locals: &[String],
) -> Vec<(String, String)> {
    let mut slots: Vec<(String, String)> = parameters
        .filter_map(|(key, term)| {
            let name = term.name()?;
            locals
                .iter()
                .any(|local| local == name)
                .then(|| (key.clone(), name.to_string()))
        })
        .collect();
    slots.sort();
    slots
}

fn hash(parts: &[&[u8]]) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new();
    for part in parts {
        hasher.update(&(part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    hasher.finalize().as_bytes().to_vec()
}

fn hash_all(parts: &[Vec<u8>]) -> Vec<u8> {
    let parts: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    hash(&parts)
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use serde_json::{Value, json};

    use crate::rule::deductive::DeductiveRule;
    use crate::rule::deductive::descriptor::DeductiveRuleDescriptor;

    /// A concept of one entity-valued field.
    fn head(field: &str, attribute: &str) -> Value {
        json!({ "with": { field: { "the": attribute, "as": "Entity" } } })
    }

    /// `of knows is`, each a variable name.
    fn knows(of: &str, is: &str) -> Value {
        json!({
            "assert": head("knows", "social/knows"),
            "where": {
                "this": { "?": { "name": of } },
                "knows": { "?": { "name": is } }
            }
        })
    }

    fn rule(when: Vec<Value>) -> DeductiveRule {
        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
            "deduce": head("friend", "social/friend"),
            "when": when,
        }))
        .expect("descriptor parses");
        descriptor.compile().expect("rule compiles")
    }

    /// Calling a local `x` or `a` does not make another rule.
    #[dialog_common::test]
    fn it_identifies_a_rule_by_its_body_not_its_variable_names() {
        let x = rule(vec![knows("this", "x"), knows("x", "friend")]);
        let a = rule(vec![knows("this", "a"), knows("a", "friend")]);
        assert_eq!(x.this(), a.this());
        assert_eq!(x.canonical_descriptor(), a.canonical_descriptor());
    }

    /// Listing the premises in another order does not either.
    #[dialog_common::test]
    fn it_identifies_a_rule_regardless_of_premise_order() {
        let forward = rule(vec![knows("this", "x"), knows("x", "friend")]);
        let backward = rule(vec![knows("x", "friend"), knows("this", "x")]);
        assert_eq!(forward.this(), backward.this());
    }

    /// Two bodies with the same premises connected differently are two
    /// rules: a branch at the first hop is not a branch at the second.
    #[dialog_common::test]
    fn it_tells_apart_bodies_that_connect_their_locals_differently() {
        let early = rule(vec![
            knows("this", "x"),
            knows("x", "y"),
            knows("y", "friend"),
            knows("x", "z"),
        ]);
        let late = rule(vec![
            knows("this", "x"),
            knows("x", "y"),
            knows("y", "friend"),
            knows("y", "z"),
        ]);
        assert_ne!(early.this(), late.this());
    }

    /// Locals refinement cannot tell apart (two parallel paths) are
    /// still named the same way whichever is written first.
    #[dialog_common::test]
    fn it_breaks_symmetric_ties_the_same_way_for_every_spelling() {
        let xy = rule(vec![
            knows("this", "x"),
            knows("this", "y"),
            knows("x", "friend"),
            knows("y", "friend"),
        ]);
        let ba = rule(vec![
            knows("b", "friend"),
            knows("this", "b"),
            knows("a", "friend"),
            knows("this", "a"),
        ]);
        assert_eq!(xy.this(), ba.this());
    }

    /// The head's operands are fixed names, so swapping which local
    /// reaches the head is another rule.
    #[dialog_common::test]
    fn it_keeps_the_head_binding_out_of_the_renaming() {
        let direct = rule(vec![knows("this", "friend"), knows("friend", "x")]);
        let indirect = rule(vec![knows("this", "x"), knows("x", "friend")]);
        assert_ne!(direct.this(), indirect.this());
    }

    /// The author's spelling is what the rule stores and shows; the
    /// canonical one is what it hashes. Decoding the stored bytes gives
    /// back the same identity, which is what the content-address check
    /// on hydration relies on.
    #[dialog_common::test]
    fn it_stores_the_authored_spelling_under_the_canonical_identity() {
        let authored = rule(vec![knows("x", "friend"), knows("this", "x")]);
        let descriptor = authored.descriptor();
        let names: Vec<String> = descriptor
            .when
            .iter()
            .flat_map(|premise| {
                premise
                    .parameters()
                    .iter()
                    .filter_map(|(_, term)| term.name().map(String::from))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert!(
            names.contains(&"x".to_string()),
            "authored name kept: {names:?}"
        );
        assert_eq!(
            serde_json::to_value(&descriptor.when[0]).unwrap()["where"]["knows"]["?"]["name"],
            "friend",
            "authored order kept"
        );
        let canonical = authored.canonical_descriptor();
        assert_ne!(canonical, descriptor);
        let decoded = DeductiveRule::decode(&authored.encode()).expect("stored bytes decode");
        assert_eq!(decoded.this(), authored.this());
        assert_eq!(decoded.descriptor(), descriptor);
    }

    /// A fold over one of two interchangeable locals is the same fold
    /// whichever the author picked.
    #[dialog_common::test]
    fn it_identifies_a_reducing_rule_by_which_local_it_folds() {
        let fold = |input: &str| {
            let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
                "deduce": { "with": { "total": { "the": "payroll/total", "as": "UnsignedInteger" } } },
                "when": [
                    {
                        "assert": { "with": { "pays": { "the": "payroll/pays", "as": "UnsignedInteger" } } },
                        "where": { "this": { "?": { "name": "this" } }, "pays": { "?": { "name": "x" } } }
                    },
                    {
                        "assert": { "with": { "pays": { "the": "payroll/pays", "as": "UnsignedInteger" } } },
                        "where": { "this": { "?": { "name": "this" } }, "pays": { "?": { "name": "y" } } }
                    }
                ],
                "reduce": { "total": { "apply": "sum", "of": { "?": { "name": input } } } }
            }))
            .expect("descriptor parses");
            descriptor.compile().expect("rule compiles")
        };
        assert_eq!(fold("x").this(), fold("y").this());
    }
}
