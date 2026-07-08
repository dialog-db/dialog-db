//! Program-level dependency analysis over the installed rule set.
//!
//! Rules live in the database and can be installed concurrently on
//! multiple replicas, each install fully valid in isolation, so
//! stratification is a *whole-set* property, not an install-time
//! one: [`RuleRegistry::register`](super::rule_registry::RuleRegistry::register)
//! accepts every rule unconditionally (replicas must converge on the
//! merged rule set regardless of stratifiability) and the merged set
//! is analyzed here, after the fact.
//!
//! The analysis builds the Apt-Blair-Walker dependency graph over
//! concepts: one node per concept, an edge from a rule's conclusion
//! to each concept its body references, tagged with the polarity of
//! the reference (`unless` premises are negative). Tarjan's
//! algorithm computes the strongly connected components; a concept
//! is *recursive* when its component is non-trivial, and a negative
//! edge inside a component is a stratification violation (no
//! stratified semantics exists for the program).
//!
//! Callers consume the analysis two ways:
//!
//! - [`RuleRegistry::validate`](super::rule_registry::RuleRegistry::validate)
//!   returns every [`NegationViolation`] in the program, for callers
//!   that want immediate feedback after an install or a merge.
//! - [`RuleRegistry::acquire`](super::rule_registry::RuleRegistry::acquire)
//!   runs the targeted [`ProgramAnalysis::check`] over the queried
//!   concept's dependency closure, so an ill-stratified or recursive
//!   region of the program fails the queries that touch it (and only
//!   those) with a structured error.

use crate::Entity;
use crate::concept::descriptor::ConceptDescriptor;
use crate::concept::query::ConceptRules;
use crate::error::EvaluationError;
use crate::negation::Negation;
use crate::premise::Premise;
use crate::proposition::Proposition;
use crate::rule::deductive::DeductiveRule;
use std::collections::{HashMap, HashSet, VecDeque};
use std::iter;

/// Polarity of a dependency edge: whether the rule body references
/// the concept positively (an ordinary premise) or under `unless`.
/// Negative edges inside a dependency cycle are stratification
/// violations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Polarity {
    /// The body asserts the concept.
    Positive,
    /// The body negates the concept (`unless`).
    Negative,
}

/// A stratification violation: some rule concluding `concept`
/// negates `negated`, and both live in the same dependency cycle,
/// so the negation reads a set the cycle itself is still deriving.
/// No stratified semantics exists for such a program.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NegationViolation {
    /// The concluding concept whose rule negates into its own cycle.
    pub concept: Entity,
    /// The negated concept inside the same cycle.
    pub negated: Entity,
}

/// The dependency edges a rule's body contributes: one per concept
/// premise, negative when the premise sits under `unless`. The
/// target's full descriptor rides along so concepts that never got
/// a registry entry still contribute their structural edges.
fn rule_edges(rule: &DeductiveRule) -> Vec<(ConceptDescriptor, Polarity)> {
    rule.analysis()
        .premises()
        .filter_map(|premise| match premise {
            Premise::Assert(Proposition::Concept(query)) => {
                Some((query.predicate.clone(), Polarity::Positive))
            }
            Premise::Unless(Negation(Proposition::Concept(query))) => {
                Some((query.predicate.clone(), Polarity::Negative))
            }
            _ => None,
        })
        .collect()
}

/// The dependency edges a concept contributes with no rules
/// installed: its implicit rule applies the target concept of every
/// concept-typed field, so each `conforms` target is a positive
/// edge.
fn structural_edges(descriptor: &ConceptDescriptor) -> Vec<(ConceptDescriptor, Polarity)> {
    descriptor
        .with()
        .iter()
        .filter_map(|(_, field)| field.conforms().map(|t| (t.clone(), Polarity::Positive)))
        .collect()
}

/// A snapshot of the program's dependency structure: edges,
/// strongly connected components, the recursive concept set, and
/// every stratification violation. Computed by
/// [`ProgramAnalysis::analyze`] from a registry's rule map and
/// cached until the next install.
#[derive(Clone, Debug, Default)]
pub struct ProgramAnalysis {
    /// Adjacency: concept -> the concepts its rules reference.
    edges: HashMap<Entity, Vec<(Entity, Polarity)>>,
    /// Concepts whose strongly connected component is non-trivial
    /// (more than one member, or a self-edge).
    recursive: HashSet<Entity>,
    /// Strongly connected component id per concept. Two recursive
    /// concepts with the same id are on the same cycle.
    component: HashMap<Entity, usize>,
    /// Every negative edge that lands inside its own component.
    violations: Vec<NegationViolation>,
}

/// The shape of a queried concept's dependency closure, as
/// classified by [`ProgramAnalysis::check`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Closure {
    /// No dependency cycle anywhere in the closure: ordinary
    /// top-down evaluation applies.
    Acyclic,
    /// The closure contains at least one cycle (all of them
    /// stratified): recursive concepts in it evaluate via the
    /// semi-naive fixpoint.
    Recursive,
}

impl ProgramAnalysis {
    /// Analyze the program formed by the given per-concept rule
    /// sets: every implicit and installed rule contributes edges,
    /// and concepts referenced by premises without a registry entry
    /// of their own contribute their structural (`conforms`) edges.
    pub fn analyze<'a>(entries: impl IntoIterator<Item = (&'a Entity, &'a ConceptRules)>) -> Self {
        let mut edges: HashMap<Entity, Vec<(Entity, Polarity)>> = HashMap::new();
        let mut pending: VecDeque<ConceptDescriptor> = VecDeque::new();

        for (entity, rules) in entries {
            let mut out = Vec::new();
            for rule in rules.rules() {
                for (target, polarity) in rule_edges(rule) {
                    out.push((target.this(), polarity));
                    pending.push_back(target);
                }
            }
            edges.insert(entity.clone(), out);
        }

        // Concepts referenced by premises but never registered still
        // constrain the graph through their embedded descriptors.
        while let Some(descriptor) = pending.pop_front() {
            let entity = descriptor.this();
            if edges.contains_key(&entity) {
                continue;
            }
            let mut out = Vec::new();
            for (target, polarity) in structural_edges(&descriptor) {
                out.push((target.this(), polarity));
                pending.push_back(target);
            }
            edges.insert(entity, out);
        }

        // Index the node set (keys plus any edge target) for Tarjan.
        // Sorted so component numbering and violation order are
        // deterministic regardless of hash-map iteration order.
        let mut nodes: Vec<Entity> = edges
            .iter()
            .flat_map(|(node, out)| {
                iter::once(node.clone()).chain(out.iter().map(|(target, _)| target.clone()))
            })
            .collect();
        nodes.sort();
        nodes.dedup();
        let index_of: HashMap<&Entity, usize> =
            nodes.iter().enumerate().map(|(i, e)| (e, i)).collect();
        let adjacency: Vec<Vec<usize>> = nodes
            .iter()
            .map(|node| {
                edges
                    .get(node)
                    .map(|out| out.iter().map(|(target, _)| index_of[target]).collect())
                    .unwrap_or_default()
            })
            .collect();

        let component = components(&adjacency);

        // A concept is recursive when its component has more than
        // one member, or when it has a self-edge.
        let mut component_size = vec![0usize; nodes.len()];
        for &c in &component {
            component_size[c] += 1;
        }
        let mut recursive = HashSet::new();
        for (i, node) in nodes.iter().enumerate() {
            let self_edge = adjacency[i].contains(&i);
            if component_size[component[i]] > 1 || self_edge {
                recursive.insert(node.clone());
            }
        }

        // A negative edge whose endpoints share a component negates
        // a set the cycle is still deriving.
        let mut violations = Vec::new();
        for node in &nodes {
            let Some(out) = edges.get(node) else { continue };
            for (target, polarity) in out {
                if *polarity == Polarity::Negative
                    && component[index_of[node]] == component[index_of[target]]
                {
                    violations.push(NegationViolation {
                        concept: node.clone(),
                        negated: target.clone(),
                    });
                }
            }
        }

        let component = nodes
            .iter()
            .enumerate()
            .map(|(i, node)| (node.clone(), component[i]))
            .collect();

        ProgramAnalysis {
            edges,
            recursive,
            component,
            violations,
        }
    }

    /// Every stratification violation in the program, in
    /// deterministic (concept-sorted) order.
    pub fn violations(&self) -> &[NegationViolation] {
        &self.violations
    }

    /// Whether the concept participates in a dependency cycle.
    pub fn is_recursive(&self, concept: &Entity) -> bool {
        self.recursive.contains(concept)
    }

    /// Whether the two concepts sit on the *same* dependency cycle:
    /// both recursive and in the same strongly connected component.
    /// This is the membership test the fixpoint evaluator uses to
    /// tell recursive occurrences (evaluated from the answer table)
    /// from base premises (evaluated top-down).
    pub fn in_same_cycle(&self, a: &Entity, b: &Entity) -> bool {
        self.recursive.contains(a)
            && self.recursive.contains(b)
            && match (self.component.get(a), self.component.get(b)) {
                (Some(x), Some(y)) => x == y,
                _ => false,
            }
    }

    /// Check the queried concept's dependency closure: an
    /// ill-stratified closure fails with
    /// [`EvaluationError::NegationThroughRecursion`]; otherwise the
    /// closure is classified [`Closure::Recursive`] when it contains
    /// a cycle (the fixpoint evaluator's cue) or [`Closure::Acyclic`]
    /// for ordinary top-down evaluation.
    ///
    /// Takes the descriptor rather than the entity because the
    /// queried concept may be unknown to the analysis (never
    /// registered); its embedded `conforms` targets seed the walk.
    pub fn check(&self, descriptor: &ConceptDescriptor) -> Result<Closure, EvaluationError> {
        // Closure over the analysis edges, seeded with the queried
        // concept's structural closure for the unregistered case.
        let mut order = Vec::new();
        let mut seen = HashSet::new();
        let mut structural = VecDeque::from([descriptor.clone()]);
        while let Some(descriptor) = structural.pop_front() {
            let entity = descriptor.this();
            if !seen.insert(entity.clone()) {
                continue;
            }
            order.push(entity.clone());
            if !self.edges.contains_key(&entity) {
                for (target, _) in structural_edges(&descriptor) {
                    structural.push_back(target);
                }
            }
        }
        let mut queue: VecDeque<Entity> = order.iter().cloned().collect();
        while let Some(entity) = queue.pop_front() {
            for (target, _) in self.edges.get(&entity).map(Vec::as_slice).unwrap_or(&[]) {
                if seen.insert(target.clone()) {
                    order.push(target.clone());
                    queue.push_back(target.clone());
                }
            }
        }

        for violation in &self.violations {
            if seen.contains(&violation.concept) {
                return Err(EvaluationError::NegationThroughRecursion {
                    concept: violation.concept.to_string(),
                    negated: violation.negated.to_string(),
                });
            }
        }
        if order.iter().any(|entity| self.recursive.contains(entity)) {
            Ok(Closure::Recursive)
        } else {
            Ok(Closure::Acyclic)
        }
    }
}

/// Iterative Tarjan: returns the component id per node index.
fn components(adjacency: &[Vec<usize>]) -> Vec<usize> {
    struct Frame {
        node: usize,
        edge: usize,
    }

    let n = adjacency.len();
    let mut index = vec![usize::MAX; n];
    let mut lowlink = vec![0usize; n];
    let mut on_stack = vec![false; n];
    let mut stack = Vec::new();
    let mut component = vec![usize::MAX; n];
    let mut next_index = 0;
    let mut next_component = 0;

    for start in 0..n {
        if index[start] != usize::MAX {
            continue;
        }
        index[start] = next_index;
        lowlink[start] = next_index;
        next_index += 1;
        stack.push(start);
        on_stack[start] = true;
        let mut frames = vec![Frame {
            node: start,
            edge: 0,
        }];

        while let Some(frame) = frames.last_mut() {
            let v = frame.node;
            if frame.edge < adjacency[v].len() {
                let w = adjacency[v][frame.edge];
                frame.edge += 1;
                if index[w] == usize::MAX {
                    index[w] = next_index;
                    lowlink[w] = next_index;
                    next_index += 1;
                    stack.push(w);
                    on_stack[w] = true;
                    frames.push(Frame { node: w, edge: 0 });
                } else if on_stack[w] {
                    lowlink[v] = lowlink[v].min(index[w]);
                }
            } else {
                frames.pop();
                if let Some(parent) = frames.last() {
                    lowlink[parent.node] = lowlink[parent.node].min(lowlink[v]);
                }
                if lowlink[v] == index[v] {
                    while let Some(w) = stack.pop() {
                        on_stack[w] = false;
                        component[w] = next_component;
                        if w == v {
                            break;
                        }
                    }
                    next_component += 1;
                }
            }
        }
    }

    component
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::attribute::{AttributeDescriptor, Cardinality, Type};
    use crate::concept::query::ConceptQuery;
    use crate::session::RuleRegistry;
    use crate::types::Any;
    use crate::{ConceptFieldDescriptor, Parameters, Term};

    /// A one-field concept in the given domain: `{domain}/name` as
    /// text. Distinct domains produce distinct concept identities.
    fn concept(domain: &str) -> ConceptDescriptor {
        ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(
                format!("{domain}/name").parse().expect("valid selector"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .expect("concept builds")
    }

    /// A rule concluding `conclusion` whose body asserts each
    /// `positive` concept (binding `this` and `name`) and negates
    /// each `negative` one (joined on `this`).
    fn rule(
        conclusion: &ConceptDescriptor,
        positive: &[&ConceptDescriptor],
        negative: &[&ConceptDescriptor],
    ) -> DeductiveRule {
        let mut premises: Vec<Premise> = Vec::new();
        for target in positive {
            let mut terms = Parameters::new();
            terms.insert("this".to_string(), Term::<Entity>::var("this").into());
            terms.insert("name".to_string(), Term::<Any>::var("name"));
            premises.push(Premise::Assert(Proposition::Concept(ConceptQuery {
                terms,
                predicate: (*target).clone(),
            })));
        }
        for target in negative {
            let mut terms = Parameters::new();
            terms.insert("this".to_string(), Term::<Entity>::var("this").into());
            premises.push(Premise::Unless(Negation(Proposition::Concept(
                ConceptQuery {
                    terms,
                    predicate: (*target).clone(),
                },
            ))));
        }
        DeductiveRule::new(conclusion.clone(), premises).expect("rule compiles")
    }

    /// The replica-merge scenario: `safe :- person, !blocked` and
    /// `blocked :- safe, banned` are each valid alone; together they
    /// form a cycle through negation. Both installs are accepted,
    /// validate() reports the violation, and only queries touching
    /// the cycle fail.
    #[dialog_common::test]
    fn it_accepts_and_reports_negation_through_recursion() {
        let person = concept("person");
        let banned = concept("banned");
        let safe = concept("safe");
        let blocked = concept("blocked");

        let mut registry = RuleRegistry::new();
        registry
            .register(rule(&safe, &[&person], &[&blocked]))
            .expect("install is unconditional");
        registry
            .register(rule(&blocked, &[&safe, &banned], &[]))
            .expect("install is unconditional");

        let violations = registry.validate().expect("validate");
        assert_eq!(
            violations,
            vec![NegationViolation {
                concept: safe.this(),
                negated: blocked.this(),
            }]
        );

        assert!(registry.is_recursive(&safe.this()).unwrap());
        assert!(registry.is_recursive(&blocked.this()).unwrap());
        assert!(!registry.is_recursive(&person.this()).unwrap());

        match registry.acquire(&safe) {
            Err(EvaluationError::NegationThroughRecursion { concept, negated }) => {
                assert_eq!(concept, safe.this().to_string());
                assert_eq!(negated, blocked.this().to_string());
            }
            other => panic!("expected NegationThroughRecursion, got {other:?}"),
        }
        assert!(
            matches!(
                registry.acquire(&blocked),
                Err(EvaluationError::NegationThroughRecursion { .. })
            ),
            "the whole cycle is poisoned"
        );
        assert!(
            registry.acquire(&person).is_ok(),
            "concepts outside the ill-stratified region still answer"
        );
    }

    /// A well-stratified recursive closure is rejected with a
    /// structured error until the fixpoint evaluator lands, rather
    /// than evaluated unboundedly.
    #[dialog_common::test]
    fn it_marks_recursive_closures_for_fixpoint_evaluation() {
        let same = concept("same");
        let mut registry = RuleRegistry::new();
        registry.register(rule(&same, &[&same], &[])).unwrap();

        assert!(registry.validate().unwrap().is_empty(), "stratified");
        assert!(registry.is_recursive(&same.this()).unwrap());
        let rules = registry.acquire(&same).expect("recursive concepts answer");
        assert!(
            rules.recursion().is_some(),
            "the rules carry the analysis so evaluation runs the fixpoint"
        );
    }

    /// Mutual recursion across two concepts and a transitive
    /// three-concept cycle are both detected, and each member's
    /// rules carry the recursion context.
    #[dialog_common::test]
    fn it_detects_mutual_and_transitive_recursion() {
        let a = concept("aaa");
        let b = concept("bbb");
        let c = concept("ccc");

        let mut mutual = RuleRegistry::new();
        mutual.register(rule(&a, &[&b], &[])).unwrap();
        mutual.register(rule(&b, &[&a], &[])).unwrap();
        assert!(mutual.is_recursive(&a.this()).unwrap());
        assert!(mutual.is_recursive(&b.this()).unwrap());
        assert!(mutual.acquire(&a).unwrap().recursion().is_some());
        let analysis = mutual.analysis().unwrap();
        assert!(analysis.in_same_cycle(&a.this(), &b.this()));

        let mut transitive = RuleRegistry::new();
        transitive.register(rule(&a, &[&b], &[])).unwrap();
        transitive.register(rule(&b, &[&c], &[])).unwrap();
        transitive.register(rule(&c, &[&a], &[])).unwrap();
        for concept in [&a, &b, &c] {
            assert!(transitive.is_recursive(&concept.this()).unwrap());
            assert!(transitive.acquire(concept).unwrap().recursion().is_some());
        }
        let analysis = transitive.analysis().unwrap();
        assert!(analysis.in_same_cycle(&a.this(), &c.this()));
        assert!(
            !analysis.in_same_cycle(&a.this(), &concept("ddd").this()),
            "concepts outside the cycle are not members"
        );
    }

    /// Ordered-variant style negation over acyclic concepts is
    /// well-stratified: no violations, queries proceed.
    #[dialog_common::test]
    fn it_passes_well_stratified_negation() {
        let contact = concept("contact");
        let email = concept("email");
        let phone = concept("phone");

        let mut registry = RuleRegistry::new();
        registry.register(rule(&contact, &[&email], &[])).unwrap();
        registry
            .register(rule(&contact, &[&phone], &[&email]))
            .unwrap();

        assert!(registry.validate().unwrap().is_empty());
        assert!(!registry.is_recursive(&contact.this()).unwrap());
        assert!(registry.acquire(&contact).is_ok());
    }

    /// The post-merge case: each registry is valid alone; extending
    /// one with the other closes a cycle through negation, and the
    /// merged analysis reports it.
    #[dialog_common::test]
    fn it_reports_violation_closed_by_merge() {
        let person = concept("person");
        let banned = concept("banned");
        let safe = concept("safe");
        let blocked = concept("blocked");

        let mut replica_a = RuleRegistry::new();
        replica_a
            .register(rule(&safe, &[&person], &[&blocked]))
            .unwrap();
        assert!(replica_a.validate().unwrap().is_empty());
        assert!(replica_a.acquire(&safe).is_ok(), "valid before the merge");

        let mut replica_b = RuleRegistry::new();
        replica_b
            .register(rule(&blocked, &[&safe, &banned], &[]))
            .unwrap();
        assert!(replica_b.validate().unwrap().is_empty());

        replica_a.extend(&replica_b).unwrap();
        assert_eq!(replica_a.validate().unwrap().len(), 1);
        assert!(matches!(
            replica_a.acquire(&safe),
            Err(EvaluationError::NegationThroughRecursion { .. })
        ));
    }

    /// Concept-typed fields contribute structural edges: a concept
    /// whose field conforms to a target participates in cycles the
    /// target's rules close, even when the outer concept itself has
    /// no registry entry.
    #[dialog_common::test]
    fn it_walks_conformance_edges_structurally() {
        let inner = concept("inner");
        let outer = ConceptDescriptor::try_from(vec![
            (
                "name".to_string(),
                ConceptFieldDescriptor::required(AttributeDescriptor::new(
                    "outer/name".parse().expect("valid selector"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                )),
            ),
            (
                "peer".to_string(),
                ConceptFieldDescriptor::conforming(
                    AttributeDescriptor::new(
                        "outer/peer".parse().expect("valid selector"),
                        "",
                        Cardinality::One,
                        Some(Type::Entity),
                    ),
                    inner.clone(),
                )
                .expect("entity-valued"),
            ),
        ])
        .unwrap();

        // inner's installed rule references outer, closing the
        // cycle outer -> inner -> outer.
        let mut registry = RuleRegistry::new();
        registry.register(rule(&inner, &[&outer], &[])).unwrap();

        assert!(
            registry
                .acquire(&outer)
                .expect("recursive concepts answer")
                .recursion()
                .is_some(),
            "the conformance cycle is visible from the unregistered end"
        );
        assert!(
            registry
                .acquire(&inner)
                .expect("recursive concepts answer")
                .recursion()
                .is_some(),
            "the cycle is visible from both ends"
        );
    }
}
