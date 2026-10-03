//! The canonical spelling of a rule.
//!
//! Two authors writing the same rule name its variables differently,
//! including the head's fields, and list its premises in a different
//! order. A rule's identity is a hash of the rule, and everything
//! keyed by that identity (the entity its facts are stored under, the
//! plan cache, the body memo) should treat those two spellings as one
//! rule. So a rule compiles with a canonical spelling beside the one
//! it was given: every variable but `this` is renamed by a labeling
//! that depends only on the rule's structure, the head's fields are
//! re-keyed by the same labeling, and the premises are sorted by their
//! encoding under it.
//!
//! The head's fields are variables like any other: a field name is
//! only what ties a body variable to an attribute, and a concept's
//! identity already ignores it. What pins a head variable is the
//! attribute it derives, which the labeling sees as one more place
//! the variable occurs. `this` is the entity slot of every head triple
//! and stays `this`.
//!
//! The labeling is colour refinement followed by individualisation:
//! each variable starts with a colour summarising where it occurs
//! (which premise shape, under which parameter), and the colours are
//! refined by the colours of the variables each occurrence sits
//! beside until the partition stops splitting. Variables that still
//! share a colour are interchangeable as far as refinement can tell;
//! each is tried first in turn, refined again, and the spelling with
//! the smallest encoding wins. The search is exhaustive up to a bound
//! on its leaves, so the result is the same for every spelling of a
//! rule within that bound, and deterministic for a given spelling
//! beyond it.
//!
//! The rule evaluates under a working spelling that keeps the head's
//! field names as given, since a caller's query binds the head by
//! those names, and renames only the body's locals. Its identity
//! hashes the canonical spelling.

use std::collections::{BTreeMap, BTreeSet};

use crate::attribute::Relation;
use crate::concept::descriptor::{ConceptDescriptor, ConceptFieldDescriptor};
use crate::error::TypeError;
use crate::premise::Premise;
use crate::reduce::ReduceSpec;
use crate::rule::deductive::rename::{Rename, rename_premises, rename_term, variables};
use crate::term::Term;
use crate::types::Any;

/// The most leaves the tie-breaking search visits before settling for
/// the best spelling found so far.
const SEARCH_BOUND: usize = 1024;

/// The placeholder every variable is renamed to when a premise's
/// shape is taken, so the shape says where variables occur but not
/// which.
const HOLE: &str = "?";

/// What canonical names start with.
const PREFIX: &str = "~";

/// A rule respelled.
#[derive(Debug, Clone)]
pub(crate) struct Canonical {
    /// The working spelling: the body's locals renamed, the head's
    /// field names kept, the premises in their given order.
    pub premises: Vec<Premise>,
    /// The reduce clause under the working spelling, keyed by the
    /// given head field, in head-field order.
    pub reduce: Vec<(String, ReduceSpec)>,
    /// The canonical spelling, which the identity hashes.
    pub identity: Identity,
}

/// A rule in its canonical spelling.
#[derive(Debug, Clone, PartialEq)]
pub struct Identity {
    /// The head, its fields re-keyed by the labeling.
    pub conclusion: ConceptDescriptor,
    /// The premises renamed and sorted.
    pub premises: Vec<Premise>,
    /// The reduce clause re-keyed and renamed, in canonical field
    /// order.
    pub reduce: Vec<(String, ReduceSpec)>,
}

/// One place a variable may occur: a premise, a reduce entry or a
/// head field, with the variables it names under their keys.
struct Atom {
    /// The atom with every variable replaced by the hole, encoded.
    shape: Vec<u8>,
    /// The variables it names, by key, in key order.
    slots: Vec<(String, String)>,
}

/// What the labeling works over: the rule's parts, the variables to
/// label, and the head's fields with their key operands.
struct Rule<'a> {
    conclusion: &'a ConceptDescriptor,
    premises: &'a [Premise],
    reduce: &'a [(String, ReduceSpec)],
    /// Every variable the labeling names, in name order.
    locals: Vec<String>,
    /// Each head field's variable, with its key operand when the
    /// field is a keyed collection.
    fields: Vec<(String, Option<String>)>,
    atoms: Vec<Atom>,
}

/// The canonical spelling of a rule concluding `conclusion` from
/// `premises` and `reduce`. `None` for a body the formal notation
/// cannot express (one reading an attribute through a raw scan, as a
/// concept's implicit rule does): such a rule has no encoding, hence
/// no identity, and keeps the spelling it was given.
pub(crate) fn canonicalize(
    conclusion: &ConceptDescriptor,
    premises: &[Premise],
    reduce: &[(String, ReduceSpec)],
) -> Result<Option<Canonical>, TypeError> {
    let fields: Vec<(String, Option<String>)> = conclusion
        .with()
        .iter()
        .map(|(name, field)| {
            let key = match field.the() {
                Relation::Attribute(_) => None,
                Relation::Collection { .. } => Some(Relation::key_operand(name)),
            };
            (name.to_string(), key)
        })
        .collect();

    let mut locals: BTreeSet<String> = variables(premises);
    for (_, spec) in reduce {
        if let Some(name) = spec.of.name() {
            locals.insert(name.to_string());
        }
    }
    for (name, key) in &fields {
        locals.insert(name.clone());
        if let Some(key) = key {
            locals.insert(key.clone());
        }
    }
    locals.remove("this");
    let locals: Vec<String> = locals.into_iter().collect();
    let holes: Rename = locals
        .iter()
        .map(|name| (name.clone(), HOLE.to_string()))
        .collect();

    let mut atoms = Vec::with_capacity(premises.len() + reduce.len() + fields.len());
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
        let shape = encode(&(
            "reduce",
            ReduceSpec {
                apply: spec.apply,
                of: rename_term(&spec.of, &holes),
            },
        ))?;
        let terms = [
            ("of".to_string(), spec.of.clone()),
            ("field".to_string(), Term::<Any>::var(field)),
        ];
        atoms.push(Atom {
            shape,
            slots: slots(terms.iter().map(|(key, term)| (key, term)), &locals),
        });
    }
    for ((name, key), (_, field)) in fields.iter().zip(conclusion.with().iter()) {
        let shape = encode(&("head", field))?;
        let mut terms = vec![("is".to_string(), Term::<Any>::var(name))];
        if let Some(key) = key {
            terms.push(("key".to_string(), Term::<Any>::var(key)));
        }
        atoms.push(Atom {
            shape,
            slots: slots(terms.iter().map(|(key, term)| (key, term)), &locals),
        });
    }

    let rule = Rule {
        conclusion,
        premises,
        reduce,
        locals,
        fields,
        atoms,
    };
    let names = label(&rule)?;
    let (_, identity) = spell(&rule, &names)?;

    // The working spelling renames the locals the head does not bind,
    // under a prefix none of the head's names could be mistaken for.
    let heads: BTreeSet<&String> = rule
        .fields
        .iter()
        .flat_map(|(name, key)| std::iter::once(name).chain(key.iter()))
        .collect();
    let mut prefix = String::from(PREFIX);
    while heads.iter().any(|name| {
        name.starts_with(&prefix) && name[prefix.len()..].bytes().all(|b| b.is_ascii_digit())
    }) {
        prefix.push_str(PREFIX);
    }
    let working: Rename = names
        .iter()
        .filter(|(from, _)| !heads.contains(from))
        .map(|(from, to)| (from.clone(), format!("{prefix}{}", &to[PREFIX.len()..])))
        .collect();
    let premises = rename_premises(rule.premises, &working)?;
    let reduce = rule
        .reduce
        .iter()
        .map(|(field, spec)| {
            (
                field.clone(),
                ReduceSpec {
                    apply: spec.apply,
                    of: rename_term(&spec.of, &working),
                },
            )
        })
        .collect();
    Ok(Some(Canonical {
        premises,
        reduce,
        identity,
    }))
}

/// The names a labeling assigns the variables: the spelling with the
/// smallest encoding among those the search visits. A key operand is
/// named after its field, so the labeling's name for it is replaced.
fn label(rule: &Rule<'_>) -> Result<Rename, TypeError> {
    if rule.locals.is_empty() {
        return Ok(Rename::new());
    }
    let colours = refine(initial(rule), rule);
    let mut best: Option<(Vec<u8>, Rename)> = None;
    let mut leaves = 0usize;
    search(colours, rule, &mut best, &mut leaves)?;
    Ok(best.map(|(_, names)| names).unwrap_or_default())
}

/// Individualise and refine: at a leaf every variable has its own
/// colour and the spelling it induces is a candidate; elsewhere the
/// first shared colour class is split by trying each member first.
fn search(
    colours: BTreeMap<String, Vec<u8>>,
    rule: &Rule<'_>,
    best: &mut Option<(Vec<u8>, Rename)>,
    leaves: &mut usize,
) -> Result<(), TypeError> {
    if *leaves >= SEARCH_BOUND {
        return Ok(());
    }
    let classes = classes(&colours, &rule.locals);
    match classes.iter().find(|members| members.len() > 1) {
        None => {
            *leaves += 1;
            let names = names(&classes, rule);
            let (encoded, _) = spell(rule, &names)?;
            if best.as_ref().is_none_or(|(known, _)| encoded < *known) {
                *best = Some((encoded, names));
            }
        }
        Some(tied) => {
            for member in tied {
                let mut split = colours.clone();
                let colour = split.get_mut(member).expect("every variable is coloured");
                *colour = hash(&[b"!", colour.as_slice()]);
                let refined = refine(split, rule);
                search(refined, rule, best, leaves)?;
                if *leaves >= SEARCH_BOUND {
                    break;
                }
            }
        }
    }
    Ok(())
}

/// Each variable's starting colour: the shapes and keys it occurs
/// under.
fn initial(rule: &Rule<'_>) -> BTreeMap<String, Vec<u8>> {
    rule.locals
        .iter()
        .map(|local| {
            let mut occurrences: Vec<Vec<u8>> = rule
                .atoms
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

/// Refine colours by the colours of the variables each occurrence
/// sits beside, until the partition stops splitting. A colour only
/// ever folds its own history in, so classes never merge.
fn refine(mut colours: BTreeMap<String, Vec<u8>>, rule: &Rule<'_>) -> BTreeMap<String, Vec<u8>> {
    let mut distinct = count(&colours);
    loop {
        let next: BTreeMap<String, Vec<u8>> = rule
            .locals
            .iter()
            .map(|local| {
                let mut occurrences: Vec<Vec<u8>> = rule
                    .atoms
                    .iter()
                    .flat_map(|atom| {
                        atom.slots
                            .iter()
                            .filter(|(_, name)| name == local)
                            .map(|(key, _)| {
                                let neighbours: Vec<Vec<u8>> = atom
                                    .slots
                                    .iter()
                                    .map(|(other_key, other)| {
                                        hash(&[other_key.as_bytes(), &colours[other]])
                                    })
                                    .collect();
                                let mut parts: Vec<&[u8]> = vec![&atom.shape, key.as_bytes()];
                                parts.extend(neighbours.iter().map(Vec::as_slice));
                                hash(&parts)
                            })
                    })
                    .collect();
                occurrences.sort();
                let mut parts: Vec<&[u8]> = vec![&colours[local]];
                parts.extend(occurrences.iter().map(Vec::as_slice));
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

/// Canonical names in class order: `~0`, `~1`, ... Every variable but
/// `this` is renamed, so nothing is left for them to collide with. A
/// key operand takes its field's name with the key suffix, since the
/// head's encoding derives it from the field.
fn names(classes: &[Vec<String>], rule: &Rule<'_>) -> Rename {
    let mut names: Rename = classes
        .iter()
        .flatten()
        .enumerate()
        .map(|(index, local)| (local.clone(), format!("{PREFIX}{index}")))
        .collect();
    for (field, key) in &rule.fields {
        if let Some(key) = key {
            let canonical = Relation::key_operand(&names[field]);
            names.insert(key.clone(), canonical);
        }
    }
    names
}

/// The rule under `names`: head re-keyed, premises renamed and sorted
/// by encoding, reduce re-keyed and renamed, and the whole encoded
/// for comparison.
fn spell(rule: &Rule<'_>, names: &Rename) -> Result<(Vec<u8>, Identity), TypeError> {
    let fields: Vec<(String, ConceptFieldDescriptor)> = rule
        .conclusion
        .with()
        .iter()
        .map(|(name, field)| (names[name].clone(), field.clone()))
        .collect();
    let conclusion = ConceptDescriptor::try_from(fields)?;
    let renamed = rename_premises(rule.premises, names)?;
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
    let mut reduce: Vec<(String, ReduceSpec)> = rule
        .reduce
        .iter()
        .map(|(field, spec)| {
            (
                names[field].clone(),
                ReduceSpec {
                    apply: spec.apply,
                    of: rename_term(&spec.of, names),
                },
            )
        })
        .collect();
    reduce.sort_by(|a, b| a.0.cmp(&b.0));
    let mut encoded = encode(&conclusion)?;
    for (key, _) in &keyed {
        encoded.extend_from_slice(&(key.len() as u64).to_be_bytes());
        encoded.extend_from_slice(key);
    }
    encoded.extend(encode(&reduce)?);
    Ok((
        encoded,
        Identity {
            conclusion,
            premises: keyed.into_iter().map(|(_, premise)| premise).collect(),
            reduce,
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

fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, TypeError> {
    serde_ipld_dagcbor::to_vec(value).map_err(|error| TypeError::TypeInference {
        reason: format!("encoding a rule for its identity: {error}"),
    })
}

/// The labeled variables among `parameters`, by key, in key order.
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

    use crate::concept::query::ConceptRules;
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

    fn rule_headed(field: &str, when: Vec<Value>) -> DeductiveRule {
        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
            "deduce": head(field, "social/friend"),
            "when": when,
        }))
        .expect("descriptor parses");
        descriptor.compile().expect("rule compiles")
    }

    fn rule(when: Vec<Value>) -> DeductiveRule {
        rule_headed("friend", when)
    }

    /// Calling a local `x` or `a` does not make another rule.
    #[dialog_common::test]
    fn it_identifies_a_rule_by_its_body_not_its_variable_names() {
        let x = rule(vec![knows("this", "x"), knows("x", "friend")]);
        let a = rule(vec![knows("this", "a"), knows("a", "friend")]);
        assert_eq!(x.this(), a.this());
        assert_eq!(x.canonical_descriptor(), a.canonical_descriptor());
    }

    /// Nor does calling the head's field `friend` or `buddy`: the
    /// field name only ties a body variable to the attribute.
    #[dialog_common::test]
    fn it_identifies_a_rule_regardless_of_its_head_field_names() {
        let friend = rule_headed("friend", vec![knows("this", "x"), knows("x", "friend")]);
        let buddy = rule_headed("buddy", vec![knows("this", "x"), knows("x", "buddy")]);
        assert_eq!(friend.this(), buddy.this());
        assert_eq!(
            friend.conclusion().with().keys().collect::<Vec<_>>(),
            vec!["friend"],
            "the working spelling keeps the given field name"
        );
        assert_eq!(
            buddy.conclusion().with().keys().collect::<Vec<_>>(),
            vec!["buddy"]
        );
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

    /// Which variable reaches the head is structure, not naming:
    /// deriving the first hop is not deriving the second.
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
        assert!(
            canonical
                .deduce
                .with()
                .keys()
                .all(|name| name.starts_with('~')),
            "the canonical head is re-keyed: {:?}",
            canonical.deduce.with().keys().collect::<Vec<_>>()
        );
        let decoded = DeductiveRule::decode(&authored.encode()).expect("stored bytes decode");
        assert_eq!(decoded.this(), authored.this());
        assert_eq!(decoded.descriptor(), descriptor);
    }

    /// A fold over one of two interchangeable locals is the same fold
    /// whichever the author picked, and whatever the reduced field is
    /// called.
    #[dialog_common::test]
    fn it_identifies_a_reducing_rule_by_which_local_it_folds() {
        let fold = |field: &str, input: &str| {
            let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
                "deduce": { "with": { field: { "the": "payroll/total", "as": "UnsignedInteger" } } },
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
                "reduce": { field: { "apply": "sum", "of": { "?": { "name": input } } } }
            }))
            .expect("descriptor parses");
            descriptor.compile().expect("rule compiles")
        };
        assert_eq!(fold("total", "x").this(), fold("sum", "y").this());
    }

    /// A rule installed into a bundle whose concept spells the same
    /// attributes under other field names is respelled onto them, so
    /// the caller's bindings reach its head.
    #[dialog_common::test]
    fn it_respells_an_installed_rule_onto_the_bundles_field_names() {
        let buddy = rule_headed("buddy", vec![knows("this", "x"), knows("x", "buddy")]);
        let friend = rule_headed("friend", vec![knows("this", "x"), knows("x", "friend")]);
        let mut bundle = ConceptRules::new(friend.conclusion());
        bundle.install(buddy.clone());
        let installed = &bundle.installed()[0];
        assert_eq!(
            installed.conclusion().with().keys().collect::<Vec<_>>(),
            vec!["friend"]
        );
        assert_eq!(installed.this(), buddy.this(), "the same rule");
        assert!(
            installed
                .analysis()
                .premises()
                .flat_map(|premise| premise
                    .parameters()
                    .iter()
                    .map(|(_, term)| term.clone())
                    .collect::<Vec<_>>())
                .any(|term| term.name() == Some("friend")),
            "the body binds the head under the bundle's name"
        );
    }
}
