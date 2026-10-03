//! A rule's heads, one per attribute.
//!
//! A deductive rule concludes a concept, which is a set of attributes.
//! Semantically the rule derives one relation per attribute of that
//! head, all from the same body: an entity either satisfies the body
//! and gets every attribute, or does not and gets none. The engine
//! resolves and evaluates rules in that form, so a concept selecting
//! any of those attributes sees the derivation, whether or not it is
//! the concept the rule was written against.
//!
//! See `notes/attribute-heads.md`.

use std::collections::BTreeMap;

use super::rename::{Rename, fresh_name, rename_premises, variables};
use crate::attribute::Relation;
use crate::concept::descriptor::{ConceptDescriptor, ConceptFieldDescriptor};
use crate::constraint::{Constraint, TypeOf};
use crate::error::TypeError;
use crate::premise::Premise;
use crate::proposition::Proposition;
use crate::reduce::ReduceSpec;
use crate::rule::deductive::{DeductiveRule, Origin};
use crate::term::Term;
use crate::type_system::{Primitive, Type as Kind};
use crate::types::Any;

/// One attribute of a rule's head, and the rule re-headed to derive
/// that attribute alone.
#[derive(Debug, Clone, PartialEq)]
pub struct Head {
    /// The head field as the source rule declared it.
    pub field: ConceptFieldDescriptor,
    /// The source rule concluding the attribute concept of `field`,
    /// its body's variables renamed onto that concept's operands.
    pub rule: DeductiveRule,
}

impl DeductiveRule {
    /// Whether this rule already concludes an attribute concept under
    /// its canonical operand name, in which case it is its own single
    /// head.
    pub fn is_attribute_headed(&self) -> bool {
        matches!(
            self.conclusion().attribute_field(),
            Some((name, _)) if name == ConceptDescriptor::VALUE
        )
    }

    /// The attribute concept this rule derives, when it is
    /// [attribute-headed](Self::is_attribute_headed).
    pub fn derived_attribute(&self) -> Option<&ConceptFieldDescriptor> {
        if self.is_attribute_headed() {
            self.conclusion().attribute_field().map(|(_, field)| field)
        } else {
            None
        }
    }

    /// This rule split into one rule per head attribute.
    ///
    /// A head field declared optional contributes a head only when the
    /// body binds it from a required source: the attribute relation
    /// holds values, never absences, and a rule whose body may leave
    /// the field absent derives nothing for it. A reducing rule's
    /// reduced field keeps its fold, grouped by the entity alone; its
    /// other fields are derived from the unfolded body, which is what
    /// the fold's grouping already made them.
    pub fn heads(&self) -> Result<Vec<Head>, TypeError> {
        if self.is_attribute_headed() {
            let (_, field) = self
                .conclusion()
                .attribute_field()
                .expect("attribute-headed rules have an attribute field");
            return Ok(vec![Head {
                field: field.clone(),
                rule: self.clone(),
            }]);
        }

        let premises: Vec<_> = self.analysis().premises().cloned().collect();
        let taken = variables(&premises);
        let mut heads = Vec::new();

        for (name, field) in self.conclusion().with().iter() {
            let mut map = Rename::new();
            // The body variable named after the head field becomes the
            // attribute concept's value operand; a body variable that
            // already uses that name, and is not the field, moves aside.
            let value = ConceptDescriptor::VALUE.to_string();
            if name != value {
                if taken.contains(&value) {
                    map.insert(value.clone(), fresh_name(&value, &taken, &map));
                }
                map.insert(name.to_string(), value.clone());
            }
            if let Relation::Collection { .. } = field.the() {
                let key = Relation::key_operand(name);
                let target = Relation::key_operand(&value);
                if key != target {
                    if taken.contains(&target) {
                        map.insert(target.clone(), fresh_name(&target, &taken, &map));
                    }
                    map.insert(key, target);
                }
            }

            // The head derives into the relation, whatever policy the
            // field it came from reads it under.
            let relation = ConceptFieldDescriptor::required(field.descriptor().clone().without_select());
            let conclusion = ConceptDescriptor::of_attribute(&relation);
            let mut body = rename_premises(&premises, &map)?;
            // An optional head field derives a value only where the
            // body bound one: the relation holds values, never
            // absences, so the value is narrowed to its present
            // shapes, which also drops the rows that lack it.
            if field.is_optional() && !self.reduce().iter().any(|entry| entry.field == name) {
                let present = field
                    .content_type()
                    .map(Kind::from)
                    .unwrap_or_else(|| Kind::from(Primitive::ALL))
                    .required();
                body.push(Premise::Assert(Proposition::Constraint(
                    Constraint::TypeOf(TypeOf::new(Term::<Any>::var(&value), present)),
                )));
            }
            let reduce: BTreeMap<String, ReduceSpec> = self
                .reduce()
                .iter()
                .filter(|entry| entry.field == name)
                .map(|entry| {
                    (
                        value.clone(),
                        ReduceSpec {
                            apply: entry.aggregator,
                            of: entry.input.clone(),
                        },
                    )
                })
                .collect();

            let rule = if reduce.is_empty() {
                DeductiveRule::new(conclusion, body)
            } else {
                DeductiveRule::with_reduce(conclusion, body, reduce)
            };
            match rule {
                Ok(rule) => {
                    // A plain head shares its source's body through the
                    // memo; a reducing head folds its own.
                    let rule = if self.reduce().is_empty() {
                        rule.with_origin(Origin {
                            rule: self.clone(),
                            value: name.to_string(),
                            key: matches!(field.the(), Relation::Collection { .. })
                                .then(|| Relation::key_operand(name)),
                        })
                    } else {
                        rule
                    };
                    heads.push(Head {
                        field: field.clone(),
                        rule,
                    })
                }
                Err(TypeError::RequiredHeadFromOptional { .. }) if field.is_optional() => {}
                Err(error) => return Err(error),
            }
        }

        Ok(heads)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::attribute::{AttributeDescriptor, Cardinality, Type};
    use crate::concept::query::ConceptQuery;
    use crate::parameters::Parameters;
    use crate::the;

    fn attribute(the: &str, cardinality: Cardinality) -> AttributeDescriptor {
        AttributeDescriptor::new(the.parse().unwrap(), the, cardinality, Some(Type::String))
    }

    fn concept(fields: &[(&str, &str)]) -> ConceptDescriptor {
        ConceptDescriptor::try_from(
            fields
                .iter()
                .map(|(name, the)| (*name, attribute(the, Cardinality::One)))
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    fn premise(predicate: ConceptDescriptor, terms: &[(&str, &str)]) -> Premise {
        let mut parameters = Parameters::new();
        for (slot, variable) in terms {
            parameters.insert(slot.to_string(), Term::<Any>::var(*variable));
        }
        Premise::Assert(Proposition::Concept(ConceptQuery {
            terms: parameters,
            predicate,
        }))
    }

    #[dialog_common::test]
    fn it_splits_a_concept_head_per_attribute() -> anyhow::Result<()> {
        let employee = concept(&[("name", "person/name"), ("role", "employee/role")]);
        let contractor = concept(&[("name", "person/name"), ("position", "contract/position")]);
        let rule = DeductiveRule::new(
            employee,
            vec![premise(
                contractor,
                &[("this", "this"), ("name", "name"), ("position", "role")],
            )],
        )?;

        let heads = rule.heads()?;
        assert_eq!(heads.len(), 2);

        for head in &heads {
            assert!(head.rule.is_attribute_headed());
            assert_eq!(
                head.rule.conclusion().this(),
                ConceptDescriptor::of_attribute(&head.field).this()
            );
            let operands: Vec<String> = head.rule.conclusion().operands().collect();
            assert_eq!(operands, vec!["this".to_string(), "is".to_string()]);
        }
        let names: Vec<_> = heads
            .iter()
            .map(|head| head.field.the().to_string())
            .collect();
        assert_eq!(names, vec!["person/name", "employee/role"]);
        Ok(())
    }

    #[dialog_common::test]
    fn it_returns_an_attribute_headed_rule_as_its_own_head() -> anyhow::Result<()> {
        let named = concept(&[("name", "person/name")]);
        let field = named.with().iter().next().unwrap().1.clone();
        let target = ConceptDescriptor::of_attribute(&field);
        let rule = DeductiveRule::new(
            target.clone(),
            vec![premise(named, &[("this", "this"), ("name", "is")])],
        )?;

        let heads = rule.heads()?;
        assert_eq!(heads.len(), 1);
        assert_eq!(heads[0].rule, rule);
        Ok(())
    }

    #[dialog_common::test]
    fn it_moves_a_body_variable_named_is_aside() -> anyhow::Result<()> {
        // A source head with a field named `is` beside the one being
        // split: in the `label` head that field is a body variable
        // named `is`, which the head's own value operand would capture.
        let labelled = concept(&[("label", "thing/label"), ("is", "thing/is")]);
        let named = concept(&[("name", "person/name"), ("is", "person/is")]);
        let rule = DeductiveRule::new(
            labelled,
            vec![premise(
                named,
                &[("this", "this"), ("name", "label"), ("is", "is")],
            )],
        )?;

        let heads = rule.heads()?;
        assert_eq!(heads.len(), 2);
        let label = heads
            .iter()
            .find(|head| head.field.the().to_string().contains("thing/label"))
            .expect("the label head");
        let body: Vec<_> = label.rule.analysis().premises().collect();
        let Premise::Assert(Proposition::Concept(query)) = body[0] else {
            panic!("expected the concept premise");
        };
        assert_eq!(query.terms.get("name").unwrap().name(), Some("is"));
        let aside = query.terms.get("is").unwrap().name().expect("named");
        assert_ne!(aside, "is", "the body variable moved out of the way");
        Ok(())
    }

    #[dialog_common::test]
    fn it_identifies_a_single_attribute_concept_with_its_attribute() {
        let named = concept(&[("name", "person/name")]);
        let field = named.with().iter().next().unwrap().1.clone();
        assert_eq!(
            named.this(),
            ConceptDescriptor::of_attribute(&field).this(),
            "field names do not take part in a concept's identity"
        );
        assert!(named.attribute_field().is_some());
        let employee = concept(&[("name", "person/name"), ("role", "employee/role")]);
        assert!(employee.attribute_field().is_none());
        let _ = the!("person/name");
    }
}
