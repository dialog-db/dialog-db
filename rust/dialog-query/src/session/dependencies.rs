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
//! the reference (`unless` premises are negative, a set-widened read
//! of an attribute concept or of an optional field is optional, and
//! every positive premise of a *reducing* rule is aggregating — its
//! fold reads the complete relation). Tarjan's algorithm computes the
//! strongly connected components; a concept is *recursive* when its
//! component is non-trivial.
//!
//! Outside a component, negation, optional reads and elections have
//! stratified semantics: the premise reads a relation derived in full
//! before the rule runs. An election is a negation too: a read under a
//! choosing policy returns a candidate *and nothing better*. Inside a
//! component such a premise would read a set the cycle is still
//! deriving, which has no stratified meaning. A merge of rule sets
//! each fine on its own can close such a cycle, so it is never refused
//! at install: the analysis *quarantines* a rule of the cycle instead
//! ([`ProgramAnalysis::quarantined`]), and evaluation leaves it out.
//! The rule chosen is one whose reads in the cycle are all positive,
//! when one exists: the rule that fed a negation back into itself,
//! rather than the negation, which keeps the meaning it had before the
//! cycle formed. Among equals, the greatest identity, an order every
//! replica shares. The choice depends on the rules alone, so every
//! replica holding the same rules quarantines the same ones, and a
//! quarantine lifts by itself once a rule of the cycle is retracted.
//! An aggregating edge inside a component remains a violation: a fold
//! has no deterministic reading over a set still growing. A deductive
//! rule refuses `reduce` at compile time, so the violation is
//! unreachable for authored rules and kept for the structure.
//!
//! Callers consume the analysis three ways:
//!
//! - [`RuleRegistry::validate`](super::rule_registry::RuleRegistry::validate)
//!   returns every [`AggregationViolation`] in the program, for
//!   callers that want immediate feedback after an install or a merge.
//! - [`ProgramAnalysis::quarantined`] lists the rules set aside and the
//!   cycle each closed, for the same callers.
//! - [`RuleRegistry::acquire`](super::rule_registry::RuleRegistry::acquire)
//!   runs the targeted [`ProgramAnalysis::check`] over the queried
//!   concept's dependency closure, so a recursive region of the
//!   program is evaluated by the fixpoint and an aggregating one
//!   fails the queries that touch it (and only those).

use crate::Entity;
use crate::attribute::AttributeDescriptor;
use crate::concept::descriptor::{ConceptDescriptor, ConceptFieldDescriptor};
use crate::concept::query::ConceptRules;
use crate::error::EvaluationError;
use crate::negation::Negation;
use crate::premise::Premise;
use crate::proposition::Proposition;
use crate::rule::deductive::DeductiveRule;
use crate::rule::statement::Reach;
use std::collections::{HashMap, HashSet, VecDeque};
use std::iter;

/// Polarity of a dependency edge: whether the rule body references
/// the concept positively (an ordinary premise), under `unless`,
/// set-widened, or from a *reducing* rule whose fold must read the
/// complete relation. A negative or optional edge inside a dependency
/// cycle is an absence test the cycle policy governs; an aggregating
/// one is a stratification violation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Polarity {
    /// The body asserts the concept.
    Positive,
    /// The body negates the concept (`unless`).
    Negative,
    /// The body reads the concept set-widened: an attribute concept
    /// whose value term admits `Nothing`, or a selecting rule's read
    /// of an optional field, which yields the absent row where
    /// nothing matched.
    Optional,
    /// The body asserts the concept from a rule with a `reduce`
    /// clause: the rule's folds consume the premise's full
    /// relation, so the reference demands a complete lower stratum.
    Aggregating,
    /// The body reads the relation under a choosing policy (`last`,
    /// `top`, `max`, `min`): the candidate it returns, and nothing
    /// better, which negates the better candidates.
    Electing,
}

/// How a rule tests for absence: by negating a concept or by reading
/// it set-widened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Absence {
    /// An `unless` premise.
    Negated,
    /// A set-widened (optional) read.
    Optional,
    /// A read under a choosing policy.
    Elected,
}

/// A rule the analysis set aside: it closed a cycle through an absence
/// test or an election, which has no stratified meaning, so evaluation
/// leaves it out until a rule of the cycle is retracted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Quarantine {
    /// The rule's identity.
    pub rule: Entity,
    /// The concept the rule concludes.
    pub concept: Entity,
    /// The concepts of the cycle it closed, sorted.
    pub cycle: Vec<Entity>,
}

/// An absence test inside a dependency cycle: some rule concluding
/// `concept` negates, or reads set-widened, `target`, and both live
/// in the same cycle, so the test reads a set the cycle is still
/// deriving. The cycle policy evaluates it as satisfied (the negation
/// holds; the optional read yields the absent row as well), which
/// keeps the component positive and every program evaluable, and is
/// rarely what the rule's author meant. Reported, never refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AbsenceInCycle {
    /// The concluding concept whose rule tests absence in its own
    /// cycle.
    pub concept: Entity,
    /// The concept tested, inside the same cycle.
    pub target: Entity,
    /// How the rule tests it.
    pub absence: Absence,
}

/// A stratification violation: some *reducing* rule concluding
/// `concept` folds over `aggregated`, and both live in the same
/// dependency cycle, so the fold reads a relation the cycle itself
/// is still deriving. No stratified semantics exists for such a
/// program, and no cycle policy gives a fold a deterministic reading
/// over a set still growing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregationViolation {
    /// The concluding concept whose reducing rule folds into its
    /// own cycle.
    pub concept: Entity,
    /// The aggregated concept inside the same cycle.
    pub aggregated: Entity,
}

/// The dependency edges a rule's body contributes: one per concept
/// premise — negative when the premise sits under `unless`, optional
/// when it reads an attribute concept set-widened, aggregating when
/// the rule carries a `reduce` clause (its folds read each positive
/// premise's complete relation). The target's full descriptor rides
/// along so concepts that never got a registry entry still
/// contribute their structural edges.
fn rule_edges(rule: &DeductiveRule) -> Vec<(ConceptDescriptor, Polarity)> {
    let positive = if rule.reduce().is_empty() {
        Polarity::Positive
    } else {
        Polarity::Aggregating
    };
    rule.analysis()
        .premises()
        .filter_map(|premise| match premise {
            Premise::Assert(Proposition::Concept(query)) if query.widens() => {
                Some((query.predicate.clone(), Polarity::Optional))
            }
            Premise::Assert(Proposition::Concept(query)) => {
                let electing = positive == Polarity::Positive
                    && query.predicate.attribute_field().is_some_and(|(_, field)| {
                        !field.descriptor().is_chain() && field.descriptor().select().elects()
                    });
                Some((
                    query.predicate.clone(),
                    if electing {
                        Polarity::Electing
                    } else {
                        positive
                    },
                ))
            }
            Premise::Unless(Negation(Proposition::Concept(query))) => {
                Some((query.predicate.clone(), Polarity::Negative))
            }
            _ => None,
        })
        .collect()
}

/// The dependency edges a concept contributes with no rules of its own:
/// one edge to the attribute concept of every field whose attribute
/// `derived` holds, since the concept's selecting rule reads it there,
/// optional for an optional field and positive otherwise, plus its
/// structural edges.
fn selecting_edges(
    descriptor: &ConceptDescriptor,
    derived: &HashSet<Entity>,
) -> Vec<(ConceptDescriptor, Polarity)> {
    let mut edges = structural_edges(descriptor);
    if let Some((_, field)) = descriptor.attribute_field() {
        // A ranked chain elects among every relation it lists.
        if field.descriptor().is_chain() {
            for relation in field.descriptor().relations() {
                let single = AttributeDescriptor::over(
                    relation.clone(),
                    "",
                    field.cardinality(),
                    field.content_type(),
                );
                edges.push((
                    ConceptDescriptor::of_attribute(&ConceptFieldDescriptor::required(single)),
                    Polarity::Electing,
                ));
            }
        }
        return edges;
    }
    for (_, field) in descriptor.with().iter() {
        let attribute = ConceptDescriptor::of_attribute(field);
        if derived.contains(&ProgramAnalysis::node(&attribute)) {
            let polarity = if field.is_optional() {
                Polarity::Optional
            } else if field.descriptor().select().elects() {
                Polarity::Electing
            } else {
                Polarity::Positive
            };
            edges.push((attribute, polarity));
        }
    }
    edges
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
/// strongly connected components, the recursive concept set, the
/// absence tests the cycle policy governs, and every stratification
/// violation. Computed by
/// [`ProgramAnalysis::analyze`] from a registry's rule map and
/// cached until the next install.
#[derive(Clone, Debug, Default)]
pub struct ProgramAnalysis {
    /// Adjacency: node -> the nodes its rules reference. An attribute
    /// concept's node is its relation (see [`ProgramAnalysis::node`]).
    edges: HashMap<Entity, Vec<(Entity, Polarity)>>,
    /// The node of every concept the analysis saw, by the concept's
    /// own entity, so a question asked by concept entity reaches the
    /// relation's node.
    aliases: HashMap<Entity, Entity>,
    /// Concepts whose strongly connected component is non-trivial
    /// (more than one member, or a self-edge).
    recursive: HashSet<Entity>,
    /// Strongly connected component id per concept. Two recursive
    /// concepts with the same id are on the same cycle.
    component: HashMap<Entity, usize>,
    /// Every negative, optional or electing edge that lands inside its
    /// own component once the quarantined rules are left out: none,
    /// unless a cycle had no rule to quarantine.
    absences: Vec<AbsenceInCycle>,
    /// The rules set aside, in the order they were.
    quarantined: Vec<Quarantine>,
    /// Every aggregating edge that lands inside its own component.
    violations: Vec<AggregationViolation>,
    /// The attribute concepts some rule derives, by entity: what an
    /// unregistered concept's selecting rule reads through.
    derived: HashSet<Entity>,
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

/// One rule's dependency edges: the identity it can be quarantined by
/// (`None` for a concept's implicit rule), the node it concludes, and
/// the nodes its body reads.
struct RuleEdges {
    rule: Option<Entity>,
    concept: Entity,
    edges: Vec<(Entity, Polarity)>,
}

/// The dependency graph of the rules not quarantined, with its strongly
/// connected components.
struct Graph {
    edges: HashMap<Entity, Vec<(Entity, Polarity)>>,
    nodes: Vec<Entity>,
    index_of: HashMap<Entity, usize>,
    adjacency: Vec<Vec<usize>>,
    component: Vec<usize>,
}

impl Graph {
    fn of(
        rules: &[RuleEdges],
        selecting: &HashMap<Entity, Vec<(Entity, Polarity)>>,
        quarantined: &[Quarantine],
        concluded: &HashSet<Entity>,
    ) -> Self {
        let mut edges: HashMap<Entity, Vec<(Entity, Polarity)>> = concluded
            .iter()
            .map(|node| (node.clone(), Vec::new()))
            .collect();
        for rule in rules {
            if rule
                .rule
                .as_ref()
                .is_some_and(|id| quarantined.iter().any(|set_aside| set_aside.rule == *id))
            {
                continue;
            }
            edges
                .entry(rule.concept.clone())
                .or_default()
                .extend(rule.edges.iter().cloned());
        }
        for (node, out) in selecting {
            edges.insert(node.clone(), out.clone());
        }

        // Index the node set (keys plus any edge target) for Tarjan.
        // Sorted so component numbering and quarantine order are
        // deterministic regardless of hash-map iteration order.
        let mut nodes: Vec<Entity> = edges
            .iter()
            .flat_map(|(node, out)| {
                iter::once(node.clone()).chain(out.iter().map(|(target, _)| target.clone()))
            })
            .collect();
        nodes.sort();
        nodes.dedup();
        let index_of: HashMap<Entity, usize> = nodes
            .iter()
            .enumerate()
            .map(|(i, e)| (e.clone(), i))
            .collect();
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
        Graph {
            edges,
            nodes,
            index_of,
            adjacency,
            component,
        }
    }

    /// The members, sorted, of the first component (by its least node)
    /// that reads itself through a negation, an optional read or an
    /// election.
    fn first_unstratified(&self) -> Option<Vec<Entity>> {
        let mut unstratified: Option<usize> = None;
        for node in &self.nodes {
            let Some(out) = self.edges.get(node) else {
                continue;
            };
            let here = self.component[self.index_of[node]];
            let reads_itself = out.iter().any(|(target, polarity)| {
                matches!(
                    polarity,
                    Polarity::Negative | Polarity::Optional | Polarity::Electing
                ) && self.component[self.index_of[target]] == here
            });
            if reads_itself {
                unstratified = Some(here);
                break;
            }
        }
        let component = unstratified?;
        Some(
            self.nodes
                .iter()
                .enumerate()
                .filter(|(i, _)| self.component[*i] == component)
                .map(|(_, node)| node.clone())
                .collect(),
        )
    }
}

impl ProgramAnalysis {
    /// Analyze the program formed by the given per-concept rule
    /// sets: every implicit and installed rule contributes edges,
    /// and concepts referenced by premises without a registry entry
    /// of their own contribute their structural (`conforms`) edges.
    pub fn analyze<'a>(entries: impl IntoIterator<Item = (&'a Entity, &'a ConceptRules)>) -> Self {
        Self::analyze_with(entries, HashSet::new())
    }

    /// [`analyze`](Self::analyze), told which attribute concepts
    /// (by entity) some rule derives: a concept referenced by a premise
    /// but never resolved reads each such attribute through its
    /// attribute concept, and contributes that edge beside its
    /// structural ones.
    pub fn analyze_with<'a>(
        entries: impl IntoIterator<Item = (&'a Entity, &'a ConceptRules)>,
        derived: HashSet<Entity>,
    ) -> Self {
        // Each rule's edges, by the node it concludes. An installed rule
        // carries its identity, by which it can be quarantined; a
        // concept's implicit rule cannot be.
        let mut rules: Vec<RuleEdges> = Vec::new();
        let mut aliases: HashMap<Entity, Entity> = HashMap::new();
        let mut pending: VecDeque<ConceptDescriptor> = VecDeque::new();
        let mut concluded: HashSet<Entity> = HashSet::new();

        for (entity, bundle) in entries {
            concluded.insert(entity.clone());
            let installed = bundle.installed().len();
            let implicit = bundle.rules().count() - installed;
            for (position, rule) in bundle.rules().enumerate() {
                aliases.insert(rule.conclusion().this(), Self::node(rule.conclusion()));
                let mut out = Vec::new();
                for (target, polarity) in rule_edges(rule) {
                    let node = Self::node(&target);
                    aliases.insert(target.this(), node.clone());
                    out.push((node, polarity));
                    pending.push_back(target);
                }
                rules.push(RuleEdges {
                    rule: (position >= implicit).then(|| rule.try_this()).flatten(),
                    concept: entity.clone(),
                    edges: out,
                });
            }
        }

        // Concepts referenced by premises but never registered still
        // constrain the graph through their embedded descriptors.
        let mut selecting: HashMap<Entity, Vec<(Entity, Polarity)>> = HashMap::new();
        while let Some(descriptor) = pending.pop_front() {
            let entity = Self::node(&descriptor);
            if concluded.contains(&entity) || selecting.contains_key(&entity) {
                continue;
            }
            let mut out = Vec::new();
            for (target, polarity) in selecting_edges(&descriptor, &derived) {
                let node = Self::node(&target);
                aliases.insert(target.this(), node.clone());
                out.push((node, polarity));
                pending.push_back(target);
            }
            selecting.insert(entity, out);
        }

        // Quarantine, one rule at a time, until no cycle reads a set it
        // is still deriving through a negation, an optional read or an
        // election.
        let mut quarantined: Vec<Quarantine> = Vec::new();
        let graph = loop {
            let graph = Graph::of(&rules, &selecting, &quarantined, &concluded);
            let Some(cycle) = graph.first_unstratified() else {
                break graph;
            };
            let members: HashSet<&Entity> = cycle.iter().collect();
            let mut candidates: Vec<(&Entity, &Entity, bool)> = rules
                .iter()
                .filter(|rule| members.contains(&rule.concept))
                .filter_map(|rule| {
                    let id = rule.rule.as_ref()?;
                    if quarantined.iter().any(|set_aside| set_aside.rule == *id) {
                        return None;
                    }
                    let inner: Vec<Polarity> = rule
                        .edges
                        .iter()
                        .filter(|(target, _)| members.contains(target))
                        .map(|(_, polarity)| *polarity)
                        .collect();
                    if inner.is_empty() {
                        return None;
                    }
                    let positive = inner.iter().all(|polarity| *polarity == Polarity::Positive);
                    Some((id, &rule.concept, positive))
                })
                .collect();
            // A rule whose reads in the cycle are all positive first: the
            // one that fed the negation back into itself. Then the
            // greatest identity.
            candidates.sort_by(|a, b| (a.2, a.0).cmp(&(b.2, b.0)));
            let Some((rule, concept, _)) = candidates.pop() else {
                // A cycle with no rule to set aside: it stays, and its
                // absence tests are reported.
                break graph;
            };
            quarantined.push(Quarantine {
                rule: rule.clone(),
                concept: concept.clone(),
                cycle: cycle.clone(),
            });
        };

        let Graph {
            edges,
            nodes,
            index_of,
            adjacency,
            component,
        } = graph;

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

        // A negative, optional, electing or aggregating edge whose
        // endpoints share a component reads a set the cycle is still
        // deriving.
        let mut absences = Vec::new();
        let mut violations = Vec::new();
        for node in &nodes {
            let Some(out) = edges.get(node) else { continue };
            for (target, polarity) in out {
                if component[index_of[node]] != component[index_of[target]] {
                    continue;
                }
                let absence = match polarity {
                    Polarity::Negative => Absence::Negated,
                    Polarity::Optional => Absence::Optional,
                    Polarity::Electing => Absence::Elected,
                    Polarity::Aggregating => {
                        violations.push(AggregationViolation {
                            concept: node.clone(),
                            aggregated: target.clone(),
                        });
                        continue;
                    }
                    Polarity::Positive => continue,
                };
                absences.push(AbsenceInCycle {
                    concept: node.clone(),
                    target: target.clone(),
                    absence,
                });
            }
        }

        let component = nodes
            .iter()
            .enumerate()
            .map(|(i, node)| (node.clone(), component[i]))
            .collect();

        ProgramAnalysis {
            edges,
            aliases,
            recursive,
            component,
            absences,
            quarantined,
            violations,
            derived,
        }
    }

    /// The rules set aside because each closed a cycle through an
    /// absence test or an election, in the order they were. Evaluation
    /// leaves them out; see [`ConceptRules::without`].
    pub fn quarantined(&self) -> &[Quarantine] {
        &self.quarantined
    }

    /// Every stratification violation in the program, in
    /// deterministic (concept-sorted) order.
    pub fn violations(&self) -> &[AggregationViolation] {
        &self.violations
    }

    /// Every absence test inside its own dependency cycle, in
    /// deterministic (concept-sorted) order: what the cycle policy
    /// evaluates as satisfied, and what an authoring tool warns about.
    pub fn absences(&self) -> &[AbsenceInCycle] {
        &self.absences
    }

    /// The node a concept is analysed as: an attribute concept is its
    /// relation, `on:<domain>/<name>`, whatever type or policy it reads
    /// the relation under, since every rule deriving the relation and
    /// every read of it meet there; any other concept is itself.
    pub fn node(concept: &ConceptDescriptor) -> Entity {
        match concept.attribute_field() {
            // A ranked chain of relations is a concept of its own with
            // an edge to each relation (see `selecting_edges`).
            Some((_, field)) if !field.descriptor().is_chain() => Reach::of(field.the())
                .on_entity()
                .unwrap_or_else(|| concept.this()),
            _ => concept.this(),
        }
    }

    /// The node a question about `concept` is asked of: the concept's
    /// node when the analysis saw it, else the entity itself.
    fn key<'a>(&'a self, concept: &'a Entity) -> &'a Entity {
        self.aliases.get(concept).unwrap_or(concept)
    }

    /// Whether the concept participates in a dependency cycle.
    pub fn is_recursive(&self, concept: &Entity) -> bool {
        self.recursive.contains(self.key(concept))
    }

    /// Whether the two concepts sit on the *same* dependency cycle:
    /// both recursive and in the same strongly connected component.
    /// This is the membership test the fixpoint evaluator uses to
    /// tell recursive occurrences (evaluated from the answer table)
    /// from base premises (evaluated top-down).
    pub fn in_same_cycle(&self, a: &Entity, b: &Entity) -> bool {
        let (a, b) = (self.key(a), self.key(b));
        self.recursive.contains(a)
            && self.recursive.contains(b)
            && match (self.component.get(a), self.component.get(b)) {
                (Some(x), Some(y)) => x == y,
                _ => false,
            }
    }

    /// Check the queried concept's dependency closure: a closure
    /// that folds inside a cycle fails with
    /// [`EvaluationError::AggregationThroughRecursion`]; otherwise
    /// the closure is classified [`Closure::Recursive`] when it
    /// contains a cycle (the fixpoint evaluator's cue) or
    /// [`Closure::Acyclic`] for ordinary top-down evaluation. An
    /// absence test inside a cycle is not a failure: the cycle policy
    /// evaluates it (see [`Self::absences`]).
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
            let entity = Self::node(&descriptor);
            if !seen.insert(entity.clone()) {
                continue;
            }
            order.push(entity.clone());
            if !self.edges.contains_key(&entity) {
                for (target, _) in selecting_edges(&descriptor, &self.derived) {
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

        if let Some(aggregation) = self
            .violations
            .iter()
            .find(|aggregation| seen.contains(&aggregation.concept))
        {
            return Err(EvaluationError::AggregationThroughRecursion {
                concept: aggregation.concept.to_string(),
                aggregated: aggregation.aggregated.to_string(),
            });
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

    /// A negation inside a cycle is reported, not refused: the program
    /// has no stratification violation, the closure is recursive so
    /// the fixpoint evaluates it under the cycle policy, and the
    /// absence test names the rule's concept and the concept it
    /// negates.
    #[dialog_common::test]
    fn it_reports_an_absence_test_inside_a_cycle() {
        let a = concept("aaa");
        let b = concept("bbb");
        let c = concept("ccc");
        let mut registry = RuleRegistry::new();
        registry.register(rule(&a, &[&b], &[])).unwrap();
        registry.register(rule(&b, &[&c], &[&a])).unwrap();

        assert!(registry.validate().unwrap().is_empty(), "no violation");
        let analysis = registry.analysis().unwrap();
        assert_eq!(
            analysis.absences(),
            &[AbsenceInCycle {
                concept: ProgramAnalysis::node(&b),
                target: ProgramAnalysis::node(&a),
                absence: Absence::Negated,
            }],
            "an attribute concept is reported by its relation"
        );
        assert_eq!(
            analysis.check(&a).unwrap(),
            Closure::Recursive,
            "the closure evaluates by fixpoint"
        );
        assert!(
            registry.acquire(&b).unwrap().recursion().is_some(),
            "the negating member is a component member like any other"
        );
    }

    /// A negation between concepts on no common cycle is stratified
    /// and reported as nothing.
    #[dialog_common::test]
    fn it_reports_nothing_for_a_stratified_negation() {
        let a = concept("aaa");
        let b = concept("bbb");
        let mut registry = RuleRegistry::new();
        registry.register(rule(&a, &[&a], &[])).unwrap();
        registry.register(rule(&b, &[&b], &[&a])).unwrap();

        let analysis = registry.analysis().unwrap();
        assert!(analysis.absences().is_empty());
        assert_eq!(analysis.check(&b).unwrap(), Closure::Recursive);
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
