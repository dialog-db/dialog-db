use super::dependencies::{AggregationViolation, ProgramAnalysis};
use crate::Entity;
use crate::EvaluationError;
use crate::attribute::The;
use crate::concept::descriptor::{ConceptDescriptor, ConceptFieldDescriptor};
use crate::concept::query::{ConceptRules, Exact, Installed, PlanCache};
use crate::rule::deductive::DeductiveRule;
use crate::rule::statement::Reach;
use crate::source::SelectRules;
use dialog_capability::Provider;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::sync::{Arc, RwLock};

/// Thread-safe registry of *deductive* rules, keyed by the relation
/// each rule derives into. Inductive rules
/// ([`InductiveRule`](crate::rule::InductiveRule)) have a
/// different lifecycle: they participate in transactions rather
/// than queries, and will be installed via a separate path in the
/// future.
///
/// A registered rule is split into [one rule per head
/// attribute](DeductiveRule::heads), each indexed under the [attribute
/// concept](ConceptDescriptor::of_attribute) it derives. When a
/// concept query needs rules, the registry returns a
/// [`ConceptRules`](crate::concept::query::ConceptRules) bundle: for
/// an attribute concept, the implicit scan plus every rule deriving
/// that attribute; for any other concept, its selecting rule alone,
/// which reads each derived attribute through its attribute concept
/// and every other attribute from stored facts.
///
/// Cloning a registry is cheap: the underlying maps are wrapped in
/// `Arc<RwLock<…>>` so all clones share the same rule set and caches.
#[derive(Debug, Clone, Default)]
pub struct RuleRegistry {
    /// The rules deriving each relation, keyed by the relation's
    /// `on:` entity: a rule derives into `(domain, name)`, and a read
    /// declares its own type, cardinality and pick over it. Every
    /// rule here is attribute-headed.
    heads: Arc<RwLock<HashMap<Entity, Vec<DeductiveRule>>>>,
    /// The order rules were registered in, by each head's identity: what
    /// the program analysis sets the newest rule of a cycle aside by.
    registered: Arc<RwLock<HashMap<Entity, u64>>>,
    /// Bundles assembled per queried concept, keyed by the concept's
    /// entity and cleared whenever the rule set changes.
    bundles: Arc<RwLock<HashMap<Entity, ConceptRules>>>,
    /// Lazily computed program-level dependency analysis (recursion
    /// and stratification), shared across clones and invalidated by
    /// [`register`](Self::register) / [`extend`](Self::extend).
    analysis: Arc<RwLock<Option<Arc<ProgramAnalysis>>>>,
}

/// The entity a field's relation is indexed under: the `derives`
/// index's key, shared by every attribute over that relation.
fn relation_key(field: &ConceptFieldDescriptor) -> Entity {
    Reach::of(field.the())
        .on_entity()
        .expect("a relation names an entity")
}

/// The index entities of every relation a field reads: one, or each
/// of a ranked chain.
fn relation_keys(field: &ConceptFieldDescriptor) -> Vec<Entity> {
    field
        .descriptor()
        .relations()
        .filter_map(|relation| Reach::of(relation).on_entity())
        .collect()
}

fn poisoned<E: Display>(error: E) -> EvaluationError {
    EvaluationError::Store(error.to_string())
}

/// `rule` reading every relation in `derived` through its attribute
/// concept where its body names the relation by an attribute premise
/// (see [`DeductiveRule::reading_derived`]), or the rule as it is.
fn reading_derived(
    rule: &DeductiveRule,
    derived: &HashSet<Entity>,
) -> Result<DeductiveRule, EvaluationError> {
    let reads_derived = |relation: &The| {
        Reach::of(relation)
            .on_entity()
            .is_some_and(|on| derived.contains(&on))
    };
    Ok(rule
        .reading_derived(&reads_derived)
        .map_err(poisoned)?
        .unwrap_or_else(|| rule.clone()))
}

impl RuleRegistry {
    /// Creates an empty rule registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a deductive rule, deduplicating by identity. The rule
    /// is indexed once per attribute its head carries.
    ///
    /// Registration is *unconditional* with respect to
    /// stratification: rules can be installed concurrently on
    /// multiple replicas and the merged set must converge, so
    /// whole-set properties (recursion, negation or aggregation
    /// through recursion) are checked by
    /// [`validate`](Self::validate) and at query time, never here.
    pub fn register(&mut self, rule: DeductiveRule) -> Result<(), EvaluationError> {
        if rule.try_this().is_none() {
            return Err(EvaluationError::RuleWithoutIdentity {
                concept: rule.conclusion().this().to_string(),
            });
        }
        let heads = rule
            .heads()
            .map_err(|error| EvaluationError::Store(error.to_string()))?;
        let mut index = self.heads.write().map_err(poisoned)?;
        let mut registered = self.registered.write().map_err(poisoned)?;
        let order = registered.len() as u64;
        for head in heads {
            if let Some(identity) = head.rule.try_this() {
                registered.entry(identity).or_insert(order);
            }
            let key = relation_key(&head.field);
            let rules = index.entry(key).or_default();
            if !rules.iter().any(|existing| existing.same(&head.rule)) {
                rules.push(head.rule);
            }
        }
        drop(registered);
        drop(index);
        self.invalidate()
    }

    /// When `rule` was registered: the order the analysis sets rules
    /// aside in.
    fn installed(&self, rule: &DeductiveRule) -> Result<Installed, EvaluationError> {
        let registered = self.registered.read().map_err(poisoned)?;
        Ok(rule
            .try_this()
            .and_then(|identity| registered.get(&identity).copied())
            .map(Installed::Registered)
            .unwrap_or(Installed::Builtin))
    }

    /// Whether some registered rule derives the attribute of `field`.
    pub fn derives(&self, field: &ConceptFieldDescriptor) -> Result<bool, EvaluationError> {
        let index = self.heads.read().map_err(poisoned)?;
        Ok(relation_keys(field)
            .iter()
            .any(|key| index.contains_key(key)))
    }

    /// The relations some registered rule derives into, by `on:` entity.
    fn derived(&self) -> Result<HashSet<Entity>, EvaluationError> {
        Ok(self
            .heads
            .read()
            .map_err(poisoned)?
            .keys()
            .cloned()
            .collect())
    }

    /// Assemble the rule bundle for `predicate` from the index.
    fn bundle(&self, predicate: &ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        let entity = predicate.this();
        if let Some(bundle) = self.bundles.read().map_err(poisoned)?.get(&entity) {
            return Ok(bundle.clone());
        }
        let bundle = if let Some((_, field)) = predicate.attribute_field() {
            // An attribute concept, under whatever field name the
            // caller spelled it: the bundle is built over the canonical
            // spelling every rule deriving it concludes, and the rules
            // are those deriving the field's relation, whatever type,
            // cardinality or pick the field reads it under.
            let canonical = ConceptDescriptor::of_attribute(field);
            let mut bundle = ConceptRules::new(&canonical);
            for rule in ConceptRules::chain_scans(field) {
                bundle.install(rule);
            }
            // A rule's body reads a relation some rule derives through
            // the attribute concept over it, so it sees the derived
            // candidates too.
            let derived = self.derived()?;
            let index = self.heads.read().map_err(poisoned)?;
            for key in relation_keys(field) {
                if let Some(rules) = index.get(&key) {
                    for rule in rules {
                        bundle.install_at(reading_derived(rule, &derived)?, self.installed(rule)?);
                    }
                }
            }
            drop(index);
            match self.exact(&canonical)? {
                Some(exact) => bundle.with_exact(exact),
                None => bundle,
            }
        } else {
            let derived = self.derived()?;
            // A field reads through its attribute concept when a rule
            // derives it, or when its pick is not the plain stored
            // read: either way its candidates are gathered and elected.
            let through = |field: &ConceptFieldDescriptor| {
                field.descriptor().reads_elected()
                    || relation_keys(field).iter().any(|key| derived.contains(key))
            };
            let reads_derived = predicate.with().iter().any(|(_, field)| through(field));
            if reads_derived {
                let selecting = DeductiveRule::selecting(predicate, &through)
                    .map_err(|error| EvaluationError::Store(error.to_string()))?;
                let bundle = ConceptRules::with_implicit(selecting, true, PlanCache::default());
                match self.exact(predicate)? {
                    Some(exact) => bundle.with_exact(exact),
                    None => bundle,
                }
            } else {
                ConceptRules::new(predicate)
            }
        };
        self.bundles
            .write()
            .map_err(poisoned)?
            .insert(entity, bundle.clone());
        Ok(bundle)
    }

    /// The covering rule for `predicate`, when exactly one source rule
    /// derives every derived attribute of it, no attribute-headed rule
    /// stands beside it, and none of them is a keyed collection.
    fn exact(&self, predicate: &ConceptDescriptor) -> Result<Option<Exact>, EvaluationError> {
        let index = self.heads.read().map_err(poisoned)?;
        let mut source: Option<DeductiveRule> = None;
        let mut attributes = Vec::new();
        for (_, field) in predicate.with().iter() {
            if field.descriptor().is_chain() {
                return Ok(None);
            }
            let key = relation_key(field);
            let Some(heads) = index.get(&key) else {
                continue;
            };
            let Some(attribute) = field.the().attribute() else {
                return Ok(None);
            };
            attributes.push(attribute);
            for head in heads {
                let Some(origin) = head.origin() else {
                    return Ok(None);
                };
                match &source {
                    None => source = Some(origin.rule.clone()),
                    Some(known) if known.same(&origin.rule) => {}
                    Some(_) => return Ok(None),
                }
            }
        }
        drop(index);
        let Some(rule) = source else { return Ok(None) };
        // The covering rule is the source's body as the bundle reads it:
        // a derived relation it names through an attribute premise is
        // read through the attribute concept here too.
        let rule = reading_derived(&rule, &self.derived()?)?;
        let covering = rule
            .covering(predicate)
            .map_err(|error| EvaluationError::Store(error.to_string()))?;
        Ok(covering.map(|rule| Exact { rule, attributes }))
    }

    /// Acquire rules for the given concept. Always returns a
    /// `ConceptRules`, whether or not any rule derives one of the
    /// concept's attributes.
    ///
    /// Runs the query-time dependency check over the concept's
    /// closure first: a closure folding inside a cycle fails with
    /// [`EvaluationError::AggregationThroughRecursion`], so such
    /// regions of the program fail exactly the queries that touch
    /// them. When the concept itself sits on a dependency cycle, the
    /// returned rules carry the program analysis so evaluation
    /// switches to the semi-naive fixpoint. A rule the analysis
    /// quarantined is left out of the returned rules.
    pub fn acquire(&self, predicate: &ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        let analysis = self.analysis()?;
        analysis.check(predicate)?;
        let rules = self.bundle(predicate)?.without(analysis.quarantined());
        Ok(
            if analysis.is_recursive(&ProgramAnalysis::node(predicate)) {
                rules.with_recursion(analysis)
            } else {
                rules
            },
        )
    }

    /// Merge every rule from `other` into this registry.
    ///
    /// Like [`register`](Self::register), merging is unconditional:
    /// the merged set may be ill-stratified, which
    /// [`validate`](Self::validate) reports and queries surface.
    pub fn extend(&mut self, other: &RuleRegistry) -> Result<(), EvaluationError> {
        let theirs = other.heads.read().map_err(poisoned)?;
        let mut ours = self.heads.write().map_err(poisoned)?;
        for (key, rules) in theirs.iter() {
            let existing = ours.entry(key.clone()).or_default();
            for rule in rules {
                if !existing.iter().any(|known| known.same(rule)) {
                    existing.push(rule.clone());
                }
            }
        }
        drop(ours);
        drop(theirs);
        // Rules merged in register after ours, in the order they were
        // registered there.
        let mut order: Vec<(u64, Entity)> = other
            .registered
            .read()
            .map_err(poisoned)?
            .iter()
            .map(|(identity, order)| (*order, identity.clone()))
            .collect();
        order.sort();
        let mut registered = self.registered.write().map_err(poisoned)?;
        let base = registered
            .values()
            .copied()
            .max()
            .map_or(0, |last| last + 1);
        for (theirs, identity) in order {
            registered.entry(identity).or_insert(base + theirs);
        }
        drop(registered);
        self.invalidate()
    }

    /// The current program analysis snapshot, computing it if the
    /// rule set changed since the last one.
    pub fn analysis(&self) -> Result<Arc<ProgramAnalysis>, EvaluationError> {
        if let Some(analysis) = self.analysis.read().map_err(poisoned)?.as_ref() {
            return Ok(analysis.clone());
        }
        // Every derived attribute is a node with its own bundle; the
        // concepts those bundles' bodies reference join the graph
        // through their selecting edges.
        let keys: Vec<Entity> = self
            .heads
            .read()
            .map_err(poisoned)?
            .keys()
            .cloned()
            .collect();
        let derived = self.derived()?;
        let mut entries: Vec<(Entity, ConceptRules)> = Vec::with_capacity(keys.len());
        for key in keys {
            let index = self.heads.read().map_err(poisoned)?;
            let Some(rules) = index.get(&key) else {
                continue;
            };
            let Some(first) = rules.first() else { continue };
            let mut bundle = ConceptRules::new(first.conclusion());
            for rule in rules {
                bundle.install_at(reading_derived(rule, &derived)?, self.installed(rule)?);
            }
            entries.push((key, bundle));
        }
        let analysis = Arc::new(ProgramAnalysis::analyze_with(
            entries.iter().map(|(entity, bundle)| (entity, bundle)),
            derived,
        ));
        *self.analysis.write().map_err(poisoned)? = Some(analysis.clone());
        Ok(analysis)
    }

    /// Every stratification violation in the current rule set.
    /// Callers decide what to do: surface as a warning after an
    /// install, refuse to proceed after a merge, or ignore and let
    /// queries fail individually. The absence tests the cycle
    /// pick governs are listed by
    /// [`ProgramAnalysis::absences`] on [`Self::analysis`].
    pub fn validate(&self) -> Result<Vec<AggregationViolation>, EvaluationError> {
        Ok(self.analysis()?.violations().to_vec())
    }

    /// Whether the concept participates in a dependency cycle in
    /// the current rule set.
    pub fn is_recursive(&self, concept: &Entity) -> Result<bool, EvaluationError> {
        Ok(self.analysis()?.is_recursive(concept))
    }

    fn invalidate(&self) -> Result<(), EvaluationError> {
        self.bundles.write().map_err(poisoned)?.clear();
        *self.analysis.write().map_err(poisoned)? = None;
        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<SelectRules> for RuleRegistry {
    async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        self.acquire(&input)
    }
}

#[cfg(test)]
mod tests {
    use crate::premise::reading;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::Term;
    use crate::attribute::{AttributeDescriptor, Cardinality, Relation, Type};
    use crate::the;

    fn person_concept() -> ConceptDescriptor {
        ConceptDescriptor::try_from([(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "person name",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap()
    }

    /// A rule deriving `concept`'s fields, by name, from the attributes
    /// `sources` names: a rule that does not read what it derives.
    fn deriving(concept: &ConceptDescriptor, sources: &[(&str, &str)]) -> DeductiveRule {
        let premises = sources
            .iter()
            .map(|(field, source)| {
                reading(
                    source.parse::<Relation>().expect("an attribute"),
                    Term::var("this"),
                    Term::var(*field),
                    None,
                )
            })
            .collect();
        DeductiveRule::new(concept.clone(), premises).expect("the rule compiles")
    }

    fn employee_concept() -> ConceptDescriptor {
        ConceptDescriptor::try_from([
            (
                "name",
                AttributeDescriptor::new(
                    the!("person/name"),
                    "person name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "role",
                AttributeDescriptor::new(
                    the!("employee/role"),
                    "employee role",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
        ])
        .unwrap()
    }

    #[dialog_common::test]
    async fn it_returns_implicit_rules_for_an_unseen_concept() {
        let registry = RuleRegistry::new();
        let descriptor = person_concept();
        let rules = Provider::<SelectRules>::execute(&registry, descriptor)
            .await
            .expect("acquire should succeed");
        assert!(
            rules.installed().is_empty(),
            "no rules installed, only implicit"
        );
    }

    #[dialog_common::test]
    async fn it_surfaces_a_registered_rule_through_the_provider() {
        let mut registry = RuleRegistry::new();
        let descriptor = person_concept();
        let rule = deriving(&descriptor, &[("name", "person/alias")]);
        registry.register(rule.clone()).unwrap();

        let rules = Provider::<SelectRules>::execute(&registry, descriptor.clone())
            .await
            .expect("acquire");
        assert_eq!(rules.installed().len(), 1);
        let installed = &rules.installed()[0];
        assert!(installed.is_attribute_headed());
        assert_eq!(installed.conclusion().this(), descriptor.this());
    }

    #[dialog_common::test]
    async fn it_indexes_a_rule_under_each_attribute_it_derives() {
        let mut registry = RuleRegistry::new();
        let employee = employee_concept();
        registry
            .register(deriving(
                &employee,
                &[("name", "person/alias"), ("role", "employee/title")],
            ))
            .unwrap();

        let (_, name) = person_concept()
            .with()
            .iter()
            .next()
            .map(|(n, f)| (n, f.clone()))
            .unwrap();
        assert!(registry.derives(&name).unwrap());

        let by_name = registry.acquire(&person_concept()).unwrap();
        assert_eq!(
            by_name.installed().len(),
            1,
            "the name head reaches the name concept"
        );

        let whole = registry.acquire(&employee).unwrap();
        assert!(
            whole.installed().is_empty(),
            "a concept with several attributes selects through its attribute concepts"
        );
    }

    #[dialog_common::test]
    async fn it_copies_entries_for_unseen_concepts_on_extend() {
        let descriptor = person_concept();
        let rule = deriving(&descriptor, &[("name", "person/alias")]);
        let mut src = RuleRegistry::new();
        src.register(rule.clone()).unwrap();

        let mut dst = RuleRegistry::new();
        dst.extend(&src).unwrap();
        assert_eq!(dst.acquire(&descriptor).unwrap().installed().len(), 1);
    }

    #[dialog_common::test]
    async fn it_merges_installed_rules_for_a_shared_concept_on_extend() {
        // Two registries with different rules for the same concept; extend
        // should produce a registry where both rules are installed.
        let descriptor = person_concept();
        let rule_a = deriving(&descriptor, &[("name", "person/alias")]);
        let rule_b = deriving(&descriptor, &[("name", "person/handle")]);
        assert_ne!(rule_a, rule_b);

        let mut a = RuleRegistry::new();
        a.register(rule_a.clone()).unwrap();
        let mut b = RuleRegistry::new();
        b.register(rule_b.clone()).unwrap();

        a.extend(&b).unwrap();
        let merged = a.acquire(&descriptor).unwrap();
        assert_eq!(merged.installed().len(), 2);
    }
}
