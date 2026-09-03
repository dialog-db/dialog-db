//! Projecting a deductive rule onto a concept its head overlaps.
//!
//! A rule derives one relation per head attribute, sharing `this`. A
//! concept selects relations. When a rule's head shares attributes with
//! a queried concept, the rule contributes rows to that concept whether
//! or not the two heads are the same: the shared attributes come
//! jointly from one rule row, the concept's remaining attributes from
//! wherever each is available. This module builds the rule that says
//! exactly that — the *projection* of the rule onto the concept — so
//! every consumer downstream (planning, the plan cache, the fixpoint,
//! dependency analysis, incremental maintenance) sees an ordinary rule
//! whose head is the concept being queried.
//!
//! See `notes/attribute-level-deduction.md` for the semantics and the
//! reasoning behind the shape.

use std::collections::{BTreeMap, BTreeSet};

use crate::artifact::Entity;
use crate::attribute::Relation;
use crate::attribute::query::AttributeQuery;
use crate::concept::descriptor::{ConceptDescriptor, ConceptFieldDescriptor};
use crate::concept::query::ConceptQuery;
use crate::error::TypeError;
use crate::negation::Negation;
use crate::optional::OptionalAttributeQuery;
use crate::parameters::Parameters;
use crate::premise::Premise;
use crate::proposition::Proposition;
use crate::rule::deductive::{DeductiveRule, field_conformance_premise, field_scan_premises};
use crate::term::Term;
use crate::types::{Any, Typed};

/// A map from a rule's operand names to a concept's, applied to the
/// rows a rule yields when the rule itself cannot be re-headed.
pub type Rename = BTreeMap<String, String>;

/// A rule projected onto a concept.
#[derive(Debug, Clone, PartialEq)]
pub struct Projected {
    /// The rule to evaluate. For a plain rule this concludes the
    /// target concept directly. For a reducing rule it is the source
    /// rule unchanged, since a fold groups by its own head fields, and
    /// `rename` maps its rows onto the target's operands.
    pub rule: DeductiveRule,
    /// Row-level operand renaming, empty when the rule concludes the
    /// target under the target's own names.
    pub rename: Rename,
}

/// Project `rule` onto `target`.
///
/// Returns `None` when the rule's head shares no attribute with the
/// target, when a reducing rule covers the target only partially, or
/// when the projection would bind a required target field from a
/// field the rule declares optional.
///
/// `derived` says whether some rule derives a target field; a target
/// field the rule does not cover is read through the single-attribute
/// concept over it when derived, so the remaining attributes resolve
/// recursively, and scanned directly otherwise.
pub fn project(
    rule: &DeductiveRule,
    target: &ConceptDescriptor,
    derived: &dyn Fn(&str, &ConceptFieldDescriptor) -> bool,
) -> Result<Option<Projected>, TypeError> {
    let head = rule.conclusion();

    // Shared attributes, matched by attribute identity: target field
    // name to the rule's head field name for the same attribute.
    let mut covered: Vec<(&str, &str)> = Vec::new();
    for (name, field) in target.with().iter() {
        let uri = field.to_uri();
        if let Some((head_name, _)) = head.with().iter().find(|(_, f)| f.to_uri() == uri) {
            covered.push((name, head_name));
        }
    }
    if covered.is_empty() {
        return Ok(None);
    }
    let complete = covered.len() == target.with().iter().len();

    // Operand renaming for the shared attributes, including the
    // operands a keyed collection field carries beside its own.
    let mut rename = Rename::new();
    for (name, head_name) in &covered {
        if name == head_name {
            continue;
        }
        rename.insert((*head_name).to_string(), (*name).to_string());
        if let Some((_, field)) = target.with().iter().find(|(n, _)| n == name)
            && let Relation::Collection { .. } = field.the()
        {
            rename.insert(
                Relation::key_operand(head_name),
                Relation::key_operand(name),
            );
            rename.insert(
                Relation::attribute_variable(head_name),
                Relation::attribute_variable(name),
            );
        }
    }

    // A fold groups by the rule's own non-reduced head fields, so a
    // reducing rule cannot be re-headed: it runs as itself and its
    // rows are renamed. Covering the target only partially would need
    // its groups joined with other derivations, which aggregate
    // maintenance does not define yet.
    if !rule.reduce().is_empty() {
        return Ok(complete.then(|| Projected {
            rule: rule.clone(),
            rename,
        }));
    }

    // The rule concludes the target exactly, under the target's own
    // names: it is its own projection. Keeping the rule intact keeps
    // its content identity, so plans cache as they always have.
    if complete && rename.is_empty() && head.this() == target.this() {
        return Ok(Some(Projected {
            rule: rule.clone(),
            rename,
        }));
    }

    // Body variables that would collide with a target operand they do
    // not bind are alpha-renamed out of the way.
    let premises: Vec<Premise> = rule.analysis().premises().cloned().collect();
    let mut taken: BTreeSet<String> = variables(&premises);
    taken.extend(head.operands());
    let reserved: BTreeSet<String> = target
        .operands()
        .chain(
            target
                .collections()
                .map(|(name, _)| Relation::attribute_variable(name)),
        )
        .collect();
    let mut map = rename.clone();
    let shared: BTreeSet<&str> = covered.iter().map(|(_, head_name)| *head_name).collect();
    for variable in taken.clone() {
        if variable != "this"
            && reserved.contains(&variable)
            && !shared.contains(variable.as_str())
            && !map.contains_key(&variable)
        {
            let fresh = fresh_name(&variable, &taken, &reserved, &map);
            taken.insert(fresh.clone());
            map.insert(variable, fresh);
        }
    }
    let mut premises = rename_premises(&premises, &map)?;

    // The target's remaining attributes, read through their own
    // single-attribute concept when derived, scanned otherwise; and
    // the target's conformance constraints on every field.
    let this = Term::<Entity>::var("this");
    for (name, field) in target.with().iter() {
        let is_covered = covered.iter().any(|(covered, _)| *covered == name);
        if !is_covered {
            if !field.is_optional() && field.the().attribute().is_some() && derived(name, field) {
                premises.push(derived_field_premise(name, field)?);
            } else {
                premises.extend(field_scan_premises(name, field, &this));
            }
        }
        premises.extend(field_conformance_premise(name, field));
    }

    match DeductiveRule::new(target.clone(), premises) {
        Ok(rule) => Ok(Some(Projected {
            rule,
            rename: Rename::new(),
        })),
        Err(TypeError::RequiredHeadFromOptional { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

/// The premise reading one target field through the single-attribute
/// concept over it, so every rule deriving that attribute contributes.
fn derived_field_premise(name: &str, field: &ConceptFieldDescriptor) -> Result<Premise, TypeError> {
    let single = ConceptDescriptor::try_from(vec![(name.to_string(), field.clone())])?;
    let mut terms = Parameters::new();
    terms.insert("this".to_string(), Term::<Any>::var("this"));
    terms.insert(name.to_string(), Term::<Any>::var(name));
    Ok(Premise::Assert(Proposition::Concept(ConceptQuery {
        terms,
        predicate: single,
    })))
}

/// Every named variable the premises mention.
pub fn variables(premises: &[Premise]) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for premise in premises {
        for (_, term) in premise.parameters().iter() {
            if let Some(name) = term.name() {
                names.insert(name.to_string());
            }
        }
    }
    names
}

/// A variable name derived from `base` that no set in play uses.
fn fresh_name(
    base: &str,
    taken: &BTreeSet<String>,
    reserved: &BTreeSet<String>,
    map: &Rename,
) -> String {
    let mut counter = 1usize;
    loop {
        let candidate = format!("{base}~{counter}");
        let used = taken.contains(&candidate)
            || reserved.contains(&candidate)
            || map.values().any(|value| *value == candidate);
        if !used {
            return candidate;
        }
        counter += 1;
    }
}

/// Rename the variables of each premise per `map`, leaving every other
/// term untouched. Names absent from the map are unchanged.
pub fn rename_premises(premises: &[Premise], map: &Rename) -> Result<Vec<Premise>, TypeError> {
    if map.is_empty() {
        return Ok(premises.to_vec());
    }
    premises
        .iter()
        .map(|premise| {
            Ok(match premise {
                Premise::Assert(proposition) => {
                    Premise::Assert(rename_proposition(proposition, map)?)
                }
                Premise::Unless(Negation(proposition)) => {
                    Premise::Unless(Negation(rename_proposition(proposition, map)?))
                }
            })
        })
        .collect()
}

fn rename_proposition(proposition: &Proposition, map: &Rename) -> Result<Proposition, TypeError> {
    Ok(match proposition {
        Proposition::Concept(query) => Proposition::Concept(ConceptQuery {
            terms: rename_parameters(&query.terms, map),
            predicate: query.predicate.clone(),
        }),
        Proposition::Attribute(query) => Proposition::Attribute(Box::new(AttributeQuery::new(
            rename_term(query.the(), map),
            rename_term(query.of(), map),
            rename_term(query.is(), map),
            rename_term(query.cause(), map),
            Some(query.cardinality()),
        ))),
        Proposition::OptionalAttribute(query) => {
            let inner = query.query();
            Proposition::OptionalAttribute(Box::new(OptionalAttributeQuery::new(
                rename_term(inner.the(), map),
                rename_term(inner.of(), map),
                rename_term(inner.is(), map),
                rename_term(inner.cause(), map),
                Some(inner.cardinality()),
            )))
        }
        // Formulas, resolvers and constraints are closed families of
        // term-carrying structs; they round-trip through the formal
        // notation, where every variable is a `{"?": {"name": ..}}`
        // node, so one walk renames them all.
        Proposition::Formula(_) | Proposition::Resolver(_) | Proposition::Constraint(_) => {
            let mut value =
                serde_json::to_value(proposition).map_err(|error| TypeError::TypeInference {
                    reason: format!("premise does not encode for renaming: {error}"),
                })?;
            rename_json(&mut value, map);
            serde_json::from_value(value).map_err(|error| TypeError::TypeInference {
                reason: format!("renamed premise does not decode: {error}"),
            })?
        }
    })
}

fn rename_parameters(parameters: &Parameters, map: &Rename) -> Parameters {
    let mut renamed = Parameters::new();
    for (key, term) in parameters.iter() {
        renamed.insert(key.clone(), rename_term(term, map));
    }
    renamed
}

fn rename_term<T>(term: &Term<T>, map: &Rename) -> Term<T>
where
    T: Typed,
    <T as Typed>::Descriptor: Clone,
    Term<T>: Clone,
{
    match term {
        Term::Variable {
            name: Some(name),
            descriptor,
        } => match map.get(name) {
            Some(renamed) => Term::Variable {
                name: Some(renamed.clone()),
                descriptor: descriptor.clone(),
            },
            None => term.clone(),
        },
        other => other.clone(),
    }
}

/// Rename every `{"?": {"name": <old>, ..}}` node in a formal-notation
/// value tree.
fn rename_json(value: &mut serde_json::Value, map: &Rename) {
    match value {
        serde_json::Value::Object(object) => {
            if let Some(serde_json::Value::Object(variable)) = object.get_mut("?")
                && let Some(serde_json::Value::String(name)) = variable.get("name")
                && let Some(renamed) = map.get(name)
            {
                variable.insert(
                    "name".to_string(),
                    serde_json::Value::String(renamed.clone()),
                );
            }
            for child in object.values_mut() {
                rename_json(child, map);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                rename_json(item, map);
            }
        }
        _ => {}
    }
}

/// Rename the operands of one derived row.
pub fn rename_row(
    row: &BTreeMap<String, crate::Value>,
    map: &Rename,
) -> BTreeMap<String, crate::Value> {
    if map.is_empty() {
        return row.clone();
    }
    row.iter()
        .map(|(key, value)| {
            (
                map.get(key).cloned().unwrap_or_else(|| key.clone()),
                value.clone(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::reduce::{Aggregator, ReduceSpec};
    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use crate::{Concept, Descriptor, Query};
    use anyhow::Result;
    use dialog_operator::helpers::{test_operator_with_profile, test_repo};
    use futures_util::TryStreamExt;

    mod staff {
        use crate::Attribute;

        /// A staff member's name (`staff/name`).
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("staff")]
        pub struct Name(pub String);

        /// A staff member's role (`staff/role`).
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("staff")]
        pub struct Role(pub String);

        /// A staff member's avatar (`staff/avatar`).
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("staff")]
        pub struct Avatar(pub String);
    }

    mod contractor {
        use crate::Attribute;

        /// A contractor's name (`contractor/name`).
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("contractor")]
        pub struct Name(pub String);

        /// A contractor's position (`contractor/position`).
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("contractor")]
        pub struct Position(pub String);
    }

    mod profile {
        use crate::Attribute;

        /// A profile's picture handle (`profile/handle`).
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("profile")]
        pub struct Handle(pub String);
    }

    mod family {
        use crate::Attribute;
        use crate::Entity;

        /// A parent edge (`family/parent`).
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("family")]
        pub struct Parent(pub Entity);

        /// An ancestor edge (`family/ancestor`).
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("family")]
        pub struct Ancestor(pub Entity);

        /// The parent an ancestor was reached through (`family/via`).
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("family")]
        pub struct Via(pub Entity);
    }

    mod payroll {
        use crate::Attribute;
        use crate::Entity;

        /// An employee's department (`payroll/dept`).
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("payroll")]
        pub struct Dept(pub Entity);

        /// An employee's salary (`payroll/salary`).
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("payroll")]
        pub struct Salary(pub u32);

        /// A department's total salary (`payroll/total`).
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("payroll")]
        pub struct Total(pub u32);
    }

    /// The rule head: an employee has a name and a role.
    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct Employee {
        pub this: Entity,
        pub name: staff::Name,
        pub role: staff::Role,
    }

    /// The rule body: contractors are the source of derived employees.
    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct Contractor {
        pub this: Entity,
        pub name: contractor::Name,
        pub position: contractor::Position,
    }

    /// A subset of the employee head, same field name.
    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct Named {
        pub this: Entity,
        pub name: staff::Name,
    }

    /// A subset of the employee head under a different field name.
    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct Labelled {
        pub this: Entity,
        #[dialog(rename = "label")]
        pub label: staff::Name,
    }

    /// The head of a second rule, deriving a different staff attribute.
    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct Pictured {
        pub this: Entity,
        pub avatar: staff::Avatar,
    }

    /// The body of the second rule.
    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct Profiled {
        pub this: Entity,
        pub handle: profile::Handle,
    }

    /// A concept spanning two rules' heads.
    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct Card {
        pub this: Entity,
        pub name: staff::Name,
        pub avatar: staff::Avatar,
    }

    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct HasParent {
        pub this: Entity,
        pub parent: family::Parent,
    }

    /// A recursive two-attribute head.
    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct Lineage {
        pub this: Entity,
        pub ancestor: family::Ancestor,
        pub via: family::Via,
    }

    /// A subset of the recursive head.
    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct HasAncestor {
        pub this: Entity,
        pub ancestor: family::Ancestor,
    }

    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct Salaried {
        pub this: Entity,
        pub dept: payroll::Dept,
        pub salary: payroll::Salary,
    }

    /// A reducing head: `this` is the department.
    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct DeptTotal {
        pub this: Entity,
        pub total: payroll::Total,
    }

    /// The reducing head under another field name.
    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct Payroll {
        pub this: Entity,
        #[dialog(rename = "sum")]
        pub sum: payroll::Total,
    }

    fn employee_from_contractor() -> DeductiveRule {
        DeductiveRule::new(
            Employee::descriptor().clone(),
            vec![
                Query::<Contractor> {
                    this: Term::var("this"),
                    name: Term::var("name"),
                    position: Term::var("role"),
                }
                .into(),
            ],
        )
        .expect("employee rule compiles")
    }

    /// Same derivation, but the body names the position variable
    /// `label` — the field name `Labelled` uses for the *name*
    /// attribute, so projecting onto `Labelled` must alpha-rename it.
    fn employee_from_contractor_colliding() -> DeductiveRule {
        DeductiveRule::new(
            Employee::descriptor().clone(),
            vec![
                Query::<Contractor> {
                    this: Term::var("this"),
                    name: Term::var("name"),
                    position: Term::var("label"),
                }
                .into(),
                crate::Constraint::from(crate::constraint::Equality::new(
                    Term::<Any>::var("role"),
                    Term::<Any>::var("label"),
                ))
                .into(),
            ],
        )
        .expect("employee rule compiles")
    }

    fn avatar_from_handle() -> DeductiveRule {
        DeductiveRule::new(
            Pictured::descriptor().clone(),
            vec![
                Query::<Profiled> {
                    this: Term::var("this"),
                    handle: Term::var("avatar"),
                }
                .into(),
            ],
        )
        .expect("avatar rule compiles")
    }

    fn lineage_rules() -> Vec<DeductiveRule> {
        // `via` is the parent in both rules; the base rule reaches
        // the ancestor directly, so its `via` is the ancestor itself.
        let base = DeductiveRule::new(
            Lineage::descriptor().clone(),
            vec![
                Query::<HasParent> {
                    this: Term::var("this"),
                    parent: Term::var("ancestor"),
                }
                .into(),
                crate::Constraint::from(crate::constraint::Equality::new(
                    Term::<Any>::var("via"),
                    Term::<Any>::var("ancestor"),
                ))
                .into(),
            ],
        )
        .expect("base compiles");
        let step = DeductiveRule::new(
            Lineage::descriptor().clone(),
            vec![
                Query::<HasParent> {
                    this: Term::var("this"),
                    parent: Term::var("via"),
                }
                .into(),
                Query::<Lineage> {
                    this: Term::var("via"),
                    ancestor: Term::var("ancestor"),
                    via: Term::blank(),
                }
                .into(),
            ],
        )
        .expect("step compiles");
        vec![base, step]
    }

    fn dept_total_rule() -> DeductiveRule {
        let mut reduce = BTreeMap::new();
        reduce.insert(
            "total".to_string(),
            ReduceSpec {
                apply: Aggregator::Sum,
                of: Term::<Any>::var("salary"),
            },
        );
        DeductiveRule::with_reduce(
            DeptTotal::descriptor().clone(),
            vec![
                Query::<Salaried> {
                    this: Term::var("employee"),
                    dept: Term::var("this"),
                    salary: Term::var("salary"),
                }
                .into(),
            ],
            reduce,
        )
        .expect("reducing rule compiles")
    }

    fn registry(rules: impl IntoIterator<Item = DeductiveRule>) -> RuleRegistry {
        let mut registry = RuleRegistry::new();
        for rule in rules {
            registry.register(rule).expect("rule registers");
        }
        registry
    }

    #[dialog_common::test]
    fn it_keeps_an_exact_head_rule_intact() {
        let rule = employee_from_contractor();
        let projected = project(&rule, Employee::descriptor(), &|_, _| true)
            .expect("projects")
            .expect("overlaps");
        assert_eq!(projected.rule, rule, "an exact head is its own projection");
        assert!(projected.rename.is_empty());
    }

    #[dialog_common::test]
    fn it_does_not_project_without_overlap() {
        let rule = employee_from_contractor();
        let projected = project(&rule, Profiled::descriptor(), &|_, _| false).expect("projects");
        assert!(projected.is_none(), "no shared attribute, no projection");
    }

    #[dialog_common::test]
    fn it_heads_a_projection_with_the_target() {
        let rule = employee_from_contractor();
        let projected = project(&rule, Labelled::descriptor(), &|_, _| false)
            .expect("projects")
            .expect("overlaps");
        assert_eq!(projected.rule.conclusion(), Labelled::descriptor());
        assert!(projected.rename.is_empty());
        let variables = variables(
            &projected
                .rule
                .analysis()
                .premises()
                .cloned()
                .collect::<Vec<_>>(),
        );
        assert!(
            variables.contains("label"),
            "the shared attribute's variable takes the target's field name"
        );
        assert!(!variables.contains("name"));
    }

    #[dialog_common::test]
    async fn it_projects_a_superset_rule_onto_a_subset_query() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        branch
            .transaction()
            .assert(Contractor {
                this: alice.clone(),
                name: contractor::Name("Alice".into()),
                position: contractor::Position("cryptographer".into()),
            })
            .commit()
            .perform(&operator)
            .await?;

        let source = TestEnv::new(&branch, &operator, registry([employee_from_contractor()]));

        let employees: Vec<Employee> = Query::<Employee>::default()
            .perform(&source)
            .try_collect()
            .await?;
        assert_eq!(
            employees,
            vec![Employee {
                this: alice.clone(),
                name: staff::Name("Alice".into()),
                role: staff::Role("cryptographer".into()),
            }],
            "the exact head is unchanged"
        );

        let named: Vec<Named> = Query::<Named>::default()
            .perform(&source)
            .try_collect()
            .await?;
        assert_eq!(
            named,
            vec![Named {
                this: alice.clone(),
                name: staff::Name("Alice".into()),
            }],
            "a subset of the head sees the derivation"
        );

        let labelled: Vec<Labelled> = Query::<Labelled>::default()
            .perform(&source)
            .try_collect()
            .await?;
        assert_eq!(
            labelled,
            vec![Labelled {
                this: alice,
                label: staff::Name("Alice".into()),
            }],
            "field names are the querying concept's, not the rule's"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_alpha_renames_colliding_body_variables() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        branch
            .transaction()
            .assert(Contractor {
                this: alice.clone(),
                name: contractor::Name("Alice".into()),
                position: contractor::Position("cryptographer".into()),
            })
            .commit()
            .perform(&operator)
            .await?;

        let source = TestEnv::new(
            &branch,
            &operator,
            registry([employee_from_contractor_colliding()]),
        );
        let labelled: Vec<Labelled> = Query::<Labelled>::default()
            .perform(&source)
            .try_collect()
            .await?;
        assert_eq!(
            labelled,
            vec![Labelled {
                this: alice,
                label: staff::Name("Alice".into()),
            }],
            "the body's own `label` variable must not capture the target's field"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_joins_attributes_derived_by_different_rules() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?; // name via the employee rule, avatar via the profile rule
        let bob = Entity::new()?; // name stored, avatar via the profile rule
        let carol = Entity::new()?; // avatar only: no card
        branch
            .transaction()
            .assert(Contractor {
                this: alice.clone(),
                name: contractor::Name("Alice".into()),
                position: contractor::Position("cryptographer".into()),
            })
            .assert(Profiled {
                this: alice.clone(),
                handle: profile::Handle("alice.png".into()),
            })
            .assert(staff::Name::of(bob.clone()).is("Bob"))
            .assert(Profiled {
                this: bob.clone(),
                handle: profile::Handle("bob.png".into()),
            })
            .assert(Profiled {
                this: carol.clone(),
                handle: profile::Handle("carol.png".into()),
            })
            .commit()
            .perform(&operator)
            .await?;

        let source = TestEnv::new(
            &branch,
            &operator,
            registry([employee_from_contractor(), avatar_from_handle()]),
        );
        let mut cards: Vec<Card> = Query::<Card>::default()
            .perform(&source)
            .try_collect()
            .await?;
        cards.sort_by(|a, b| a.name.0.cmp(&b.name.0));
        cards.dedup();
        assert_eq!(
            cards,
            vec![
                Card {
                    this: alice,
                    name: staff::Name("Alice".into()),
                    avatar: staff::Avatar("alice.png".into()),
                },
                Card {
                    this: bob,
                    name: staff::Name("Bob".into()),
                    avatar: staff::Avatar("bob.png".into()),
                },
            ],
            "attributes from different rules, and from storage, join on the entity"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_projects_a_recursive_head_onto_a_subset() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let a = Entity::new()?;
        let b = Entity::new()?;
        let c = Entity::new()?;
        branch
            .transaction()
            .assert(family::Parent::of(b.clone()).is(a.clone()))
            .assert(family::Parent::of(c.clone()).is(b.clone()))
            .commit()
            .perform(&operator)
            .await?;

        let registry = registry(lineage_rules());
        assert!(
            registry.is_recursive(&Lineage::descriptor().this())?,
            "the step rule reads its own head"
        );
        assert!(
            !registry
                .acquire(HasAncestor::descriptor())?
                .recursion()
                .is_some(),
            "a subset depends on the recursive head without joining its cycle"
        );

        let source = TestEnv::new(&branch, &operator, registry);
        let mut ancestors: Vec<(Entity, Entity)> = Query::<HasAncestor>::default()
            .perform(&source)
            .map_ok(|row| (row.this, row.ancestor.0))
            .try_collect()
            .await?;
        ancestors.sort();
        ancestors.dedup();
        let mut expected = vec![
            (b.clone(), a.clone()),
            (c.clone(), a.clone()),
            (c.clone(), b.clone()),
        ];
        expected.sort();
        assert_eq!(
            ancestors, expected,
            "the closure is visible through the subset"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_projects_a_reducing_rule_onto_a_subset() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let dept = Entity::new()?;
        let alice = Entity::new()?;
        let bob = Entity::new()?;
        branch
            .transaction()
            .assert(Salaried {
                this: alice,
                dept: payroll::Dept(dept.clone()),
                salary: payroll::Salary(100),
            })
            .assert(Salaried {
                this: bob,
                dept: payroll::Dept(dept.clone()),
                salary: payroll::Salary(50),
            })
            .commit()
            .perform(&operator)
            .await?;

        let source = TestEnv::new(&branch, &operator, registry([dept_total_rule()]));
        let totals: Vec<DeptTotal> = Query::<DeptTotal>::default()
            .perform(&source)
            .try_collect()
            .await?;
        assert_eq!(
            totals,
            vec![DeptTotal {
                this: dept.clone(),
                total: payroll::Total(150),
            }]
        );

        let payroll: Vec<Payroll> = Query::<Payroll>::default()
            .perform(&source)
            .try_collect()
            .await?;
        assert_eq!(
            payroll,
            vec![Payroll {
                this: dept,
                sum: payroll::Total(150),
            }],
            "a reducing rule's rows are renamed onto the querying concept"
        );
        Ok(())
    }
}
