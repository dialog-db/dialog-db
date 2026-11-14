//! Reads guided by placement.
//!
//! A read of a session meets each layer through a [`Guide`]. A row of an
//! attribute placed on a layer is taken from that layer alone; a row of
//! an attribute with no placement is taken from the primary alone. Every
//! other row is a *divergence of placement*: no read of the session takes
//! it, and the session reports it once, with the attribute and the layer,
//! when a read meets it (see [`Session::divergences`]).
//!
//! The machinery of Dialog DB, every attribute under `dialog.`, is read
//! from each layer: rules and placements live in the layer that holds
//! them.
//!
//! [`Session::divergences`]: crate::Session::divergences

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use dialog_corpus::{Changes, Entity};
use parking_lot::Mutex;

use super::PlacementMap;

/// The most divergences a session holds until its host takes them, and
/// the most it remembers having reported. Past either, the oldest goes:
/// a layer that a writer floods with misplaced facts costs a bounded
/// amount of memory, and a divergence reported long ago may be reported
/// again.
const MOST_DIVERGENCES: usize = 1024;

/// A fact found in a layer other than the one its attribute is placed
/// on. Reads skip it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Divergence {
    /// The attribute of the fact.
    pub attribute: String,
    /// The layer that holds it.
    pub layer: Entity,
    /// The layer the attribute is placed on, or `None` for the primary.
    pub placed: Option<Entity>,
}

/// The divergences a session found and its host has not taken yet.
/// Clones share them.
#[derive(Debug, Clone, Default)]
pub(crate) struct Divergences(Arc<Mutex<Found>>);

#[derive(Debug, Default)]
struct Found {
    /// Reported, in the order found, until the host takes them.
    pending: VecDeque<Divergence>,
    /// The attributes reported for each layer, with the layer they were
    /// reported as placed on, so that each is reported once until its
    /// placement moves, and the order they were reported in, oldest
    /// first.
    seen: HashMap<Entity, HashSet<(String, Option<Entity>)>>,
    order: VecDeque<(Entity, (String, Option<Entity>))>,
}

impl Divergences {
    /// Records that a fact of `attribute` is in `layer` while placed on
    /// `placed`, unless that attribute and layer were reported already
    /// under that placement. A read meets each such row, so the check
    /// comes first.
    fn report(&self, attribute: &str, layer: &Entity, placed: Option<&Entity>) {
        let mut found = self.0.lock();
        let key = (attribute.to_owned(), placed.cloned());
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
            placed: placed.cloned(),
        };
        found.pending.push_back(divergence);
        if found.pending.len() > MOST_DIVERGENCES {
            found.pending.pop_front();
        }
        drop(found);
        tracing::warn!(
            attribute,
            layer = %layer,
            placed = placed.map(ToString::to_string),
            "a fact is in a layer other than the one its attribute is placed on; no read sees it"
        );
    }

    /// Takes every divergence reported since the last take.
    pub(crate) fn take(&self) -> Vec<Divergence> {
        self.0.lock().pending.drain(..).collect()
    }
}

/// One layer of a guided read: the entity that names it, and whether it
/// is the primary.
#[derive(Debug, Clone)]
pub(crate) struct GuidedLayer {
    pub(crate) entity: Entity,
    pub(crate) primary: bool,
}

/// How a read of a session meets each of its layers. A composite carries
/// one for each tree line and each memory layer, in the order the
/// composite lists them.
#[derive(Debug, Clone)]
pub(crate) struct Guide {
    pub(crate) lines: Vec<GuidedLayer>,
    pub(crate) memories: Vec<GuidedLayer>,
    pub(crate) divergences: Divergences,
    /// The declarations the session holds (see [`Session::declare`]),
    /// which a guided read resolves its placements over.
    ///
    /// [`Session::declare`]: crate::Session::declare
    pub(crate) declared: Changes,
}

impl Guide {
    /// Whether a read takes a row of `attribute` from `layer`, under the
    /// placements `map`: the layer it is placed on, or the primary when it
    /// is placed nowhere. The machinery under `dialog.` is read from every
    /// layer. A row the read does not take is reported.
    pub(crate) fn admits(&self, layer: &GuidedLayer, map: &PlacementMap, attribute: &str) -> bool {
        if self.placed(layer, map, attribute) {
            return true;
        }
        self.divergences
            .report(attribute, &layer.entity, map.layer_of(attribute));
        false
    }

    /// The decision of [`admits`](Self::admits) alone, with nothing
    /// reported: for a check made before any row is read.
    pub(crate) fn placed(&self, layer: &GuidedLayer, map: &PlacementMap, attribute: &str) -> bool {
        if attribute.starts_with("dialog.") {
            return true;
        }
        match map.layer_of(attribute) {
            Some(placed) => *placed == layer.entity,
            None => layer.primary,
        }
    }
}
