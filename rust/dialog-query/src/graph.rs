//! Graph shaping above ordinary concept matching.
//!
//! Shape names are local references, not concept identities. A cycle in shapes
//! therefore neither changes a concept's hash nor introduces recursive rules.
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{Cardinality, ConceptDescriptor, ConceptQuery, Entity, Parameters, Term, Value};

/// The values read under each field's pick. Missing scalar fields are omitted;
/// a missing many-valued field has an empty vector.
pub type AttributeBag = BTreeMap<String, Vec<Value>>;

/// A graph's named shapes. References are resolved within this document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GraphSchema {
    /// Named shapes, including every edge target.
    pub shapes: BTreeMap<String, GraphShape>,
}

/// The attributes to read on each reached entity, independently of conformance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphShape {
    /// Field names, types, picks, and requiredness.
    pub concept: ConceptDescriptor,
    /// Entity-valued fields to expand.
    #[serde(default)]
    pub edges: BTreeMap<String, GraphEdge>,
}

/// A typed edge. Matching constraints can remove roots; shaping constraints
/// can remove edge targets but never roots. Both become scan bindings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphEdge {
    /// The name of a shape in the enclosing schema.
    pub target: String,
    /// Target-field equalities applied only when this edge is on a root.
    #[serde(default)]
    pub matching: BTreeMap<String, Value>,
    /// Target-field equalities applied while shaping at every depth.
    #[serde(default)]
    pub shaping: BTreeMap<String, Value>,
}

/// An ordinary matching query followed by graph shaping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphQuery {
    /// The root matching stage. Its normal rule semantics are unchanged.
    pub root: ConceptQuery,
    /// Name of the root shape.
    pub shape: String,
    /// The reachable named shapes.
    pub schema: GraphSchema,
}

/// A normalized graph. An entity shared by many paths appears once; bags are
/// namespaced by shape so different field aliases cannot overwrite one another.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GraphResult {
    /// Entities selected by matching, in deterministic order.
    pub roots: Vec<Entity>,
    /// Entity -> shape -> attribute bag. Edges are entity values in the bags.
    pub nodes: BTreeMap<Entity, BTreeMap<String, AttributeBag>>,
}

/// A field-level explanation of a node's failure to conform.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConformanceIssue {
    /// Field that failed.
    pub field: String,
    /// Human-readable reason.
    pub reason: String,
}

/// Check a read-side bag. This same check is used by graph root-edge matching
/// and is available to applications checking nested, potentially incomplete bags.
/// It checks local fields; typed edges do not recursively impose a fixpoint.
pub fn check_bag(concept: &ConceptDescriptor, bag: &AttributeBag) -> Vec<ConformanceIssue> {
    let mut issues = Vec::new();
    for (name, field) in concept.with().iter() {
        let values = bag.get(name).map(Vec::as_slice).unwrap_or_default();
        if values.is_empty() && !field.is_optional() {
            issues.push(ConformanceIssue {
                field: name.into(),
                reason: "required field is missing".into(),
            });
        }
        if field.cardinality() == Cardinality::One && values.len() > 1 {
            issues.push(ConformanceIssue {
                field: name.into(),
                reason: "scalar field has more than one value".into(),
            });
        }
        for value in values {
            if let Some(expected) = field.content_type()
                && !expected.holds(value)
            {
                issues.push(ConformanceIssue {
                    field: name.into(),
                    reason: format!("value does not have type {expected:?}"),
                });
            }
        }
    }
    issues
}

/// A malformed graph schema or an evaluation error.
#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    /// A schema reference or constraint is invalid.
    #[error("invalid graph: {0}")]
    Invalid(String),
    /// Existing query evaluation failed.
    #[error(transparent)]
    Evaluation(#[from] crate::EvaluationError),
}

impl GraphQuery {
    /// Validate all names and types before issuing any reads.
    pub fn validate(&self) -> Result<(), GraphError> {
        let root = self
            .schema
            .shapes
            .get(&self.shape)
            .ok_or_else(|| GraphError::Invalid(format!("unknown root shape {}", self.shape)))?;
        if root.concept != self.root.predicate {
            return Err(GraphError::Invalid(
                "root query and root shape must describe the same concept".into(),
            ));
        }
        for (name, shape) in &self.schema.shapes {
            for (field, descriptor) in shape.concept.with().iter() {
                if descriptor.the().attribute().is_none() {
                    return Err(GraphError::Invalid(format!(
                        "keyed collection {name}.{field} needs an explicit keyed graph bag"
                    )));
                }
                if descriptor.conforms().is_some() {
                    return Err(GraphError::Invalid(format!(
                        "use a graph edge instead of embedded conformance on {name}.{field}"
                    )));
                }
            }

            for (field, edge) in &shape.edges {
                let descriptor = shape
                    .concept
                    .with()
                    .iter()
                    .find(|(key, _)| key == field)
                    .map(|(_, descriptor)| descriptor)
                    .ok_or_else(|| GraphError::Invalid(format!("unknown edge {name}.{field}")))?;
                if descriptor.content_type() != Some(crate::Type::Entity) {
                    return Err(GraphError::Invalid(format!(
                        "edge {name}.{field} must be entity-valued"
                    )));
                }
                let target = self.schema.shapes.get(&edge.target).ok_or_else(|| {
                    GraphError::Invalid(format!("unknown target shape {}", edge.target))
                })?;
                for key in edge.matching.keys().chain(edge.shaping.keys()) {
                    if !target.concept.with().iter().any(|(field, _)| field == key) {
                        return Err(GraphError::Invalid(format!(
                            "unknown constraint {}.{key}",
                            edge.target
                        )));
                    }
                }
                if !edge.matching.is_empty() && name != &self.shape {
                    return Err(GraphError::Invalid(format!(
                        "matching constraint on non-root edge {name}.{field}"
                    )));
                }
                if !edge.matching.is_empty() && self.schema.cyclic(name, &edge.target) {
                    return Err(GraphError::Invalid(format!(
                        "cyclic edge {name}.{field} can only shape results"
                    )));
                }
            }
        }
        Ok(())
    }
}

impl GraphSchema {
    /// Whether following this edge participates in a cycle. Such edges shape
    /// only: treating them as required conformance would derive an empty set.
    pub fn cyclic(&self, source: &str, target: &str) -> bool {
        let mut pending = vec![target];
        let mut seen = BTreeSet::new();
        while let Some(name) = pending.pop() {
            if name == source {
                return true;
            }
            if seen.insert(name)
                && let Some(shape) = self.shapes.get(name)
            {
                pending.extend(shape.edges.values().map(|edge| edge.target.as_str()));
            }
        }
        false
    }
}

impl GraphShape {
    /// Read one field through its normal attribute concept. `equal` is a scan
    /// binding, not a post-read filter. Derived attributes and picks use the
    /// ordinary query engine. No target conformance is imposed here.
    pub fn field_query(
        &self,
        field: &str,
        entity: &Entity,
        equal: Option<&Value>,
    ) -> Result<ConceptQuery, GraphError> {
        let descriptor = self
            .concept
            .with()
            .iter()
            .find(|(name, _)| *name == field)
            .map(|(_, descriptor)| descriptor)
            .ok_or_else(|| GraphError::Invalid(format!("unknown field {field}")))?;
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::constant(entity.clone()));
        terms.insert(
            ConceptDescriptor::VALUE.into(),
            equal
                .cloned()
                .map(Term::Constant)
                .unwrap_or_else(|| Term::var("value")),
        );
        Ok(ConceptQuery {
            predicate: ConceptDescriptor::of_attribute(descriptor),
            terms,
        }
        .conformed())
    }

    /// An empty bag, with many-valued fields represented explicitly.
    pub fn empty_bag(&self) -> AttributeBag {
        self.concept
            .with()
            .iter()
            .filter(|(_, field)| field.cardinality() == Cardinality::Many)
            .map(|(name, _)| (name.to_string(), Vec::new()))
            .collect()
    }
}

/// One attribute's old and new values. `None` means the field is omitted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphFieldDelta {
    /// Entity whose field changed.
    pub entity: Entity,
    /// Shape containing the field.
    pub shape: String,
    /// Field name.
    pub field: String,
    /// Previous values.
    pub before: Option<Vec<Value>>,
    /// Current values.
    pub after: Option<Vec<Value>>,
}

/// Changes in a normalized graph, independently of the paths reaching a node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct GraphDelta {
    /// Newly matching roots.
    pub roots_added: Vec<Entity>,
    /// Roots that no longer match.
    pub roots_removed: Vec<Entity>,
    /// Newly reached entity/shape bags (including empty bags).
    pub nodes_added: BTreeMap<Entity, BTreeMap<String, AttributeBag>>,
    /// Entity/shape pairs no longer reachable.
    pub nodes_removed: BTreeMap<Entity, Vec<String>>,
    /// Changed fields on retained entity/shape pairs.
    pub fields: Vec<GraphFieldDelta>,
}

impl GraphResult {
    /// Compare normalized results, emitting shared entity changes only once.
    pub fn delta(&self, next: &Self) -> GraphDelta {
        let roots_added = next
            .roots
            .iter()
            .filter(|id| !self.roots.contains(id))
            .cloned()
            .collect();
        let roots_removed = self
            .roots
            .iter()
            .filter(|id| !next.roots.contains(id))
            .cloned()
            .collect();
        let mut delta = GraphDelta {
            roots_added,
            roots_removed,
            ..GraphDelta::default()
        };
        for (entity, shapes) in &next.nodes {
            for (shape, bag) in shapes {
                if let Some(old) = self.nodes.get(entity).and_then(|shapes| shapes.get(shape)) {
                    let fields: BTreeSet<_> = old.keys().chain(bag.keys()).collect();
                    for field in fields {
                        if old.get(field) != bag.get(field) {
                            delta.fields.push(GraphFieldDelta {
                                entity: entity.clone(),
                                shape: shape.clone(),
                                field: field.clone(),
                                before: old.get(field).cloned(),
                                after: bag.get(field).cloned(),
                            });
                        }
                    }
                } else {
                    delta
                        .nodes_added
                        .entry(entity.clone())
                        .or_default()
                        .insert(shape.clone(), bag.clone());
                }
            }
        }
        for (entity, shapes) in &self.nodes {
            for shape in shapes.keys() {
                if !next
                    .nodes
                    .get(entity)
                    .is_some_and(|shapes| shapes.contains_key(shape))
                {
                    delta
                        .nodes_removed
                        .entry(entity.clone())
                        .or_default()
                        .push(shape.clone());
                }
            }
        }
        delta
    }
}

impl GraphDelta {
    /// Whether the graph is unchanged.
    pub fn is_empty(&self) -> bool {
        self.roots_added.is_empty()
            && self.roots_removed.is_empty()
            && self.nodes_added.is_empty()
            && self.nodes_removed.is_empty()
            && self.fields.is_empty()
    }
}

/// Backend used by graph shaping. Implementations can run ordinary queries or
/// retain their existing subscriptions; graph traversal itself is identical.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait GraphReader: dialog_common::ConditionalSend {
    /// Evaluate the ordinary matching stage.
    async fn roots(&mut self, query: &ConceptQuery) -> Result<Vec<Entity>, GraphError>;
    /// Read one field on one entity, optionally constrained by equality.
    async fn field(
        &mut self,
        shape: &str,
        field: &str,
        entity: &Entity,
        equal: Option<&Value>,
    ) -> Result<Vec<Value>, GraphError>;
}

async fn bag<R: GraphReader>(
    reader: &mut R,
    name: &str,
    shape: &GraphShape,
    entity: &Entity,
) -> Result<AttributeBag, GraphError> {
    let mut bag = shape.empty_bag();
    for (field, _) in shape.concept.with().iter() {
        let mut values = reader.field(name, field, entity, None).await?;
        values.sort_by(|left, right| match (left, right) {
            (Value::Float(left), Value::Float(right)) => left.total_cmp(right),
            _ => left.partial_cmp(right).unwrap_or(Ordering::Equal),
        });
        values.dedup();
        if !values.is_empty() {
            bag.insert(field.to_string(), values);
        }
    }
    Ok(bag)
}

async fn accepts<R: GraphReader>(
    reader: &mut R,
    shape: &str,
    entity: &Entity,
    constraints: &BTreeMap<String, Value>,
) -> Result<bool, GraphError> {
    for (field, equal) in constraints {
        if reader
            .field(shape, field, entity, Some(equal))
            .await?
            .is_empty()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

impl GraphQuery {
    /// Evaluate matching and then shaping through a reader. The visited set is
    /// scoped to one read; shared nodes and cycles never duplicate reads.
    pub async fn read<R: GraphReader>(&self, reader: &mut R) -> Result<GraphResult, GraphError> {
        self.validate()?;
        let root_shape = &self.schema.shapes[&self.shape];
        let candidates: BTreeSet<_> = reader.roots(&self.root).await?.into_iter().collect();
        let mut cached = BTreeMap::<(Entity, String), AttributeBag>::new();
        let mut roots = Vec::new();
        for entity in candidates {
            let root_bag = bag(reader, &self.shape, root_shape, &entity).await?;
            let mut matches = true;
            for (field, edge) in &root_shape.edges {
                let required = root_shape
                    .concept
                    .with()
                    .iter()
                    .any(|(name, descriptor)| name == field && !descriptor.is_optional());
                if self.schema.cyclic(&self.shape, &edge.target)
                    || (!required && edge.matching.is_empty())
                {
                    continue;
                }
                let target_shape = &self.schema.shapes[&edge.target];
                let mut any = false;
                for value in root_bag.get(field).into_iter().flatten() {
                    if let Value::Entity(target) = value {
                        if !accepts(reader, &edge.target, target, &edge.matching).await? {
                            continue;
                        }
                        let key = (target.clone(), edge.target.clone());
                        if !cached.contains_key(&key) {
                            cached.insert(
                                key.clone(),
                                bag(reader, &edge.target, target_shape, target).await?,
                            );
                        }
                        if check_bag(&target_shape.concept, &cached[&key]).is_empty() {
                            any = true;
                        }
                    }
                }
                if !any {
                    matches = false;
                    break;
                }
            }
            cached.insert((entity.clone(), self.shape.clone()), root_bag);
            if matches {
                roots.push(entity);
            }
        }
        let mut pending: Vec<_> = roots
            .iter()
            .map(|entity| (entity.clone(), self.shape.clone()))
            .collect();
        let mut visited = BTreeSet::new();
        let mut result = GraphResult {
            roots,
            nodes: BTreeMap::new(),
        };
        while let Some((entity, name)) = pending.pop() {
            if !visited.insert((entity.clone(), name.clone())) {
                continue;
            }
            let shape = &self.schema.shapes[&name];
            let mut values = match cached.remove(&(entity.clone(), name.clone())) {
                Some(bag) => bag,
                None => bag(reader, &name, shape, &entity).await?,
            };
            for (field, edge) in &shape.edges {
                if let Some(targets) = values.get_mut(field) {
                    let mut retained = Vec::new();
                    for value in targets.iter() {
                        if let Value::Entity(target) = value
                            && accepts(reader, &edge.target, target, &edge.shaping).await?
                        {
                            retained.push(value.clone());
                            pending.push((target.clone(), edge.target.clone()));
                        }
                    }
                    let omitted = retained.is_empty()
                        && shape.concept.with().iter().any(|(name, field_descriptor)| {
                            name == field && field_descriptor.cardinality() == Cardinality::One
                        });
                    *targets = retained;
                    if omitted {
                        values.remove(field);
                    }
                }
            }
            result.nodes.entry(entity).or_default().insert(name, values);
        }
        Ok(result)
    }

    /// Read a graph with the ordinary query engine's scope (including overlays).
    pub async fn perform<'a, Env: crate::Scope<'a>>(
        &'a self,
        env: &'a Env,
    ) -> Result<GraphResult, GraphError> {
        self.read(&mut ScopeReader {
            schema: &self.schema,
            env,
        })
        .await
    }
}

struct ScopeReader<'a, Env> {
    schema: &'a GraphSchema,
    env: &'a Env,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<'a, Env: crate::Scope<'a>> GraphReader for ScopeReader<'a, Env> {
    async fn roots(&mut self, query: &ConceptQuery) -> Result<Vec<Entity>, GraphError> {
        use crate::{Application, Output};
        Ok(query
            .clone()
            .perform(self.env)
            .try_vec()
            .await?
            .into_iter()
            .map(|row| row.entity().clone())
            .collect())
    }
    async fn field(
        &mut self,
        shape: &str,
        field: &str,
        entity: &Entity,
        equal: Option<&Value>,
    ) -> Result<Vec<Value>, GraphError> {
        use crate::{Application, Output};
        let query = self.schema.shapes[shape].field_query(field, entity, equal)?;
        let mut values = Vec::new();
        for row in query.perform(self.env).try_vec().await? {
            let value = row.value(ConceptDescriptor::VALUE)?;
            if !values.contains(&value) {
                values.push(value);
            }
        }
        Ok(values)
    }
}
