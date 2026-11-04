//! Reads guided by placement.
//!
//! A placement names the scope of an attribute, and a stack binds each
//! scope to the layers linked under it. A write routes by placement, so
//! the stack itself never puts a fact in a layer outside its scope. A
//! fact can still arrive there another way: a commit to the layer that
//! does not go through the stack, or a pull from a peer that holds no
//! such placement. Such a fact is a *divergence of placement*.
//!
//! A read of a stack meets each layer through a [`Guide`]. A row of an
//! attribute placed on a scope that a link binds is read only from the
//! layers linked under that scope. One placed on the default scope, or on
//! a scope that no link binds and that the bottom binds to its tree, and
//! one with no placement in a repository with a default scope, belongs to
//! the bottom, where a write of the attribute lands. One placed on any
//! other scope has no layer in the stack: a write of it is refused, and
//! no layer is read for it. The placement decides first, so a default
//! scope never overrides it. Every other row is a divergence: no read of the stack takes it, and the
//! stack reports it once, with the attribute and the layer, when a read
//! meets it (see [`Stack::divergences`](super::Stack::divergences)). An
//! attribute with no placement in a repository with no default scope is
//! read from every layer, as a stack reads with no guide.
//!
//! The machinery of Dialog DB, every attribute under `dialog.`, is read
//! from each layer: links, rules, and placements live in the layer that
//! holds them. The placements that guide a read are those of the bottom,
//! the lowest layer, which is the first that the top links when it links
//! layers side by side.
//!
//! A rule is a fact of the layer that holds it, and a read takes it only
//! when that layer may conclude its head ([`Guide::takes_rule`]). The layer
//! may conclude an attribute that it would be read from, and one that a
//! placement it holds itself sends to another layer of the stack: the
//! writers of the layer chose that. So a placement in the session store of
//! the bottom keeps a rule that concludes its namespace out of each layer
//! but the one under its scope. A rule that a query or a commit brings comes
//! from that query or that commit, and no layer holds it, so this does not
//! apply to it.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use dialog_artifacts::Entity;
use dialog_query::concept::descriptor::ConceptDescriptor;
use parking_lot::Mutex;

use crate::placement::{Bindings, ScopeMap, Target};

/// The most divergences a stack holds until its host takes them, and the
/// most it remembers having reported. Past either, the oldest goes: a
/// layer that a writer floods with misplaced facts costs a bounded amount
/// of memory, and a divergence reported long ago may be reported again.
const MOST_DIVERGENCES: usize = 1024;

/// A fact found in a layer outside the scope of its attribute's
/// placement. Reads skip it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Divergence {
    /// The attribute of the fact.
    pub attribute: String,
    /// The address entity of the layer that holds it (see
    /// [`Layer::address_entity`](super::Layer::address_entity)).
    pub layer: Entity,
    /// The scope that the attribute belongs to: the one its placement
    /// names, or the default scope when it has none. `None` when its
    /// placement names a scope that no link binds and the bottom binds to
    /// its tree.
    pub scope: Option<Entity>,
}

/// The divergences a stack found and its host has not taken yet. Clones
/// share them.
#[derive(Debug, Clone, Default)]
pub(crate) struct Divergences(Arc<Mutex<Found>>);

#[derive(Debug, Default)]
struct Found {
    /// Reported, in the order found, until the host takes them.
    pending: VecDeque<Divergence>,
    /// The attributes reported for each layer, with the scope they were
    /// reported under, so that each is reported once until its placement
    /// moves, and the order they were reported in, oldest first.
    seen: HashMap<Entity, HashSet<(String, Option<Entity>)>>,
    order: VecDeque<(Entity, (String, Option<Entity>))>,
}

impl Divergences {
    /// Records that a fact of `attribute` is in `layer`, outside `scope`,
    /// unless that attribute and layer were reported already under that
    /// scope. A read meets each such row, so the check comes first.
    fn report(&self, attribute: &str, layer: &Entity, scope: Option<&Entity>) {
        let mut found = self.0.lock();
        let key = (attribute.to_owned(), scope.cloned());
        if found
            .seen
            .get(layer)
            .is_some_and(|attributes| attributes.contains(&key))
        {
            return;
        }
        found
            .seen
            .entry(layer.clone())
            .or_default()
            .insert(key.clone());
        found.order.push_back((layer.clone(), key));
        if found.order.len() > MOST_DIVERGENCES
            && let Some((layer, attribute)) = found.order.pop_front()
            && let Some(attributes) = found.seen.get_mut(&layer)
        {
            attributes.remove(&attribute);
            if attributes.is_empty() {
                found.seen.remove(&layer);
            }
        }
        let divergence = Divergence {
            attribute: attribute.to_owned(),
            layer: layer.clone(),
            scope: scope.cloned(),
        };
        found.pending.push_back(divergence);
        if found.pending.len() > MOST_DIVERGENCES {
            found.pending.pop_front();
        }
        drop(found);
        tracing::warn!(
            attribute,
            layer = %layer,
            scope = scope.map(ToString::to_string),
            "a fact is in a layer outside the scope of its placement; no read sees it"
        );
    }

    /// Takes every divergence reported since the last take.
    pub(crate) fn take(&self) -> Vec<Divergence> {
        self.0.lock().pending.drain(..).collect()
    }
}

/// One layer of a guided read: its address, the scopes the stack binds
/// to it, and whether it is the bottom.
#[derive(Debug, Clone)]
pub(crate) struct GuidedLayer {
    pub(crate) address: Entity,
    pub(crate) scopes: HashSet<Entity>,
    pub(crate) bottom: bool,
}

/// How a read of a stack meets each of its layers. A composite carries
/// one for each tree line and each standalone ephemeral layer, in the
/// order the composite lists them.
#[derive(Debug, Clone)]
pub(crate) struct Guide {
    pub(crate) lines: Vec<GuidedLayer>,
    pub(crate) ephemerals: Vec<GuidedLayer>,
    /// Every scope some link of the stack binds.
    pub(crate) bound: HashSet<Entity>,
    /// The bindings of the bottom: a scope that no link binds belongs to
    /// the bottom only when the bottom binds it to its tree.
    pub(crate) bottom: Bindings,
    pub(crate) divergences: Divergences,
}

impl Guide {
    /// Whether a read takes a row of `attribute` from `layer`, under the
    /// placements `scopes`. A row it does not take is reported.
    pub(crate) fn admits(&self, layer: &GuidedLayer, scopes: &ScopeMap, attribute: &str) -> bool {
        let (admitted, scope) = self.verdict(layer, scopes, attribute);
        if !admitted {
            self.divergences.report(attribute, &layer.address, scope);
        }
        admitted
    }

    /// Whether a read can take a row of `attribute` from some layer of the
    /// stack. A read of an attribute that no layer can give reads nothing, so
    /// a write of it into a layer changes no answer.
    pub(crate) fn reaches(&self, scopes: &ScopeMap, attribute: &str) -> bool {
        self.lines
            .iter()
            .chain(&self.ephemerals)
            .any(|layer| self.verdict(layer, scopes, attribute).0)
    }

    /// Whether a read takes from `layer` a rule that concludes `head`: the
    /// layer may conclude each attribute of the head. A rule whose head spans
    /// scopes so reaches a read only from a layer that may conclude each one.
    /// A rule stays in a layer that may not conclude it, and it reaches no
    /// query and fires in no commit.
    pub(crate) fn takes_rule(
        &self,
        layer: &GuidedLayer,
        scopes: &ScopeMap,
        head: &ConceptDescriptor,
    ) -> bool {
        head.with().iter().all(|(_, field)| {
            let attribute = field.descriptor().the().to_string();
            self.verdict(layer, scopes, &attribute).0
                || (layer.bottom
                    && scopes.held_by_bottom(&attribute)
                    && scopes
                        .scope_of(&attribute)
                        .is_some_and(|scope| self.bound.contains(scope)))
        })
    }

    /// The layers and the placements that decide which rules a read takes, as
    /// a value that two reads can compare.
    pub(crate) fn rule_shape(&self, scopes: &ScopeMap) -> RuleShape {
        let layer = |layer: &GuidedLayer| {
            let mut bound: Vec<String> = layer.scopes.iter().map(ToString::to_string).collect();
            bound.sort();
            (layer.bottom, bound)
        };
        let mut bound: Vec<String> = self.bound.iter().map(ToString::to_string).collect();
        bound.sort();
        RuleShape {
            scopes: scopes.clone(),
            lines: self.lines.iter().map(layer).collect(),
            ephemerals: self.ephemerals.iter().map(layer).collect(),
            bound,
        }
    }

    /// Whether `layer` gives a row of `attribute`, with the scope that the
    /// attribute belongs to.
    fn verdict<'a>(
        &self,
        layer: &GuidedLayer,
        scopes: &'a ScopeMap,
        attribute: &str,
    ) -> (bool, Option<&'a Entity>) {
        if attribute.starts_with("dialog.") {
            return (true, None);
        }
        // The placement of the attribute decides first, so that a default
        // scope never overrides it.
        match (scopes.scope_of(attribute), scopes.default()) {
            (Some(scope), _) if self.bound.contains(scope) => {
                (layer.scopes.contains(scope), Some(scope))
            }
            // A scope that no link binds belongs to the bottom when it is
            // the default scope, or the bottom binds it to its tree, as a
            // write of the attribute lands there.
            (Some(scope), default)
                if Some(scope) == default || self.bottom.target(scope) == Some(Target::Tree) =>
            {
                (layer.bottom, None)
            }
            // Any other scope has no layer in this stack: a write of the
            // attribute is refused, and no layer is read for it.
            (Some(scope), _) => (false, Some(scope)),
            // With no placement, an attribute belongs to the default
            // scope, which is the bottom's.
            (None, Some(default)) => (layer.bottom, Some(default)),
            // No placement and no default: the stack reads the attribute
            // from every layer, as it reads an unguided composite.
            (None, None) => (true, None),
        }
    }
}

/// What the rules that a guided read takes depend on, besides the rule facts
/// of its layers: the placements, and the scopes of each layer. A rule set
/// assembled under one shape does not answer a read under another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuleShape {
    scopes: ScopeMap,
    lines: Vec<(bool, Vec<String>)>,
    ephemerals: Vec<(bool, Vec<String>)>,
    bound: Vec<String>,
}
