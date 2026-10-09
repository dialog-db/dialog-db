//! The cells a transaction wrote under a choosing pick, found by the
//! entity and attribute bounds of a read.

use std::collections::BTreeMap;
use std::ops::Bound;

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{ArtifactSelector, Entity, NameShape, Relation};

/// One entry per cell a write under a choosing pick asserted, kept
/// past its retraction: the cells a read of a range must settle. Kept
/// by attribute then entity and by entity then attribute, on their
/// spellings, so a read bounded on either (exactly or by prefix) ranges
/// to its cells without encoding a key: a cell is its attribute and
/// entity, and a read settles it whatever value it holds.
#[derive(Clone, Debug, Default)]
pub(crate) struct ElectingCells {
    by_attribute: BTreeMap<String, BTreeMap<String, (Relation, Entity)>>,
    by_entity: BTreeMap<String, BTreeMap<String, (Relation, Entity)>>,
}

/// The entries of `map` whose key is `exact`, starts with `prefix`, or
/// all of them, in key order.
fn keyed<'m, V>(
    map: &'m BTreeMap<String, V>,
    exact: Option<&str>,
    prefix: Option<&str>,
) -> Box<dyn Iterator<Item = &'m V> + 'm> {
    match (exact, prefix) {
        (Some(key), _) => Box::new(map.get(key).into_iter()),
        (None, Some(prefix)) => {
            let prefix = prefix.to_owned();
            Box::new(
                map.range::<str, _>((Bound::Included(prefix.as_str()), Bound::Unbounded))
                    .take_while(move |(key, _)| key.starts_with(&prefix))
                    .map(|(_, value)| value),
            )
        }
        (None, None) => Box::new(map.values()),
    }
}

impl ElectingCells {
    /// Keep the cell. Returns whether it was not kept before.
    pub(crate) fn insert(&mut self, the: &Relation, of: &Entity) -> bool {
        let entities = self
            .by_attribute
            .entry(the.as_str().to_owned())
            .or_default();
        if entities.contains_key(of.as_str()) {
            return false;
        }
        entities.insert(of.as_str().to_owned(), (the.clone(), of.clone()));
        self.by_entity
            .entry(of.as_str().to_owned())
            .or_default()
            .insert(the.as_str().to_owned(), (the.clone(), of.clone()));
        true
    }

    /// Whether no cell is kept.
    pub(crate) fn is_empty(&self) -> bool {
        self.by_attribute.is_empty()
    }

    /// The kept cells within the entity and attribute bounds of
    /// `selector`. A bound on the value is ignored: the cell's writes
    /// may succeed a claim of a value they do not share. Ranged on the
    /// attribute when the read bounds one exactly or by prefix, else on
    /// the entity when the read bounds one; a read bounding neither,
    /// or only an attribute's name or shape, visits every cell.
    pub(crate) fn within(
        &self,
        selector: &ArtifactSelector<Constrained>,
    ) -> Vec<(Relation, Entity)> {
        let admits_attribute = |the: &Relation| {
            selector
                .attribute_name()
                .is_none_or(|name| the.name() == name.as_str())
                && selector.name_shape().is_none_or(|shape| {
                    the.name()
                        .as_bytes()
                        .first()
                        .and_then(|&first| NameShape::classify(first))
                        == Some(shape)
                })
        };
        let the = selector.attribute().map(Relation::as_str);
        let the_prefix = selector.attribute_prefix();
        let of = selector.entity().map(Entity::as_str);
        let of_prefix = selector.entity_prefix();
        let cells: Box<dyn Iterator<Item = &(Relation, Entity)>> =
            if the.is_some() || the_prefix.is_some() || (of.is_none() && of_prefix.is_none()) {
                Box::new(
                    keyed(&self.by_attribute, the, the_prefix)
                        .flat_map(move |entities| keyed(entities, of, of_prefix)),
                )
            } else {
                Box::new(
                    keyed(&self.by_entity, of, of_prefix)
                        .flat_map(move |attributes| keyed(attributes, the, the_prefix)),
                )
            };
        cells
            .filter(|(the, _)| admits_attribute(the))
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    fn cell(the: &str, of: &str) -> (Relation, Entity) {
        (the.parse().expect("attribute"), of.parse().expect("entity"))
    }

    /// A read bounded on the attribute, its prefix, its name or its
    /// shape, on the entity or its prefix, or on nothing, finds the
    /// cells within its bounds; a bound on the value finds them all.
    #[dialog_common::test]
    fn it_finds_the_cells_within_a_reads_bounds() {
        let mut cells = ElectingCells::default();
        for (the, of) in [
            cell("person/name", "id:a"),
            cell("person/name", "id:b"),
            cell("person/age", "id:a"),
            cell("list/Mx", "id:a"),
            cell("other/name", "id:c"),
        ] {
            assert!(cells.insert(&the, &of));
        }
        let (the, of) = cell("person/name", "id:a");
        assert!(!cells.insert(&the, &of), "kept once");
        let within = |selector: ArtifactSelector<Constrained>| {
            let mut found = cells.within(&selector);
            found.sort();
            found
        };

        assert_eq!(
            within(ArtifactSelector::new().the(the.clone())),
            vec![cell("person/name", "id:a"), cell("person/name", "id:b")]
        );
        assert_eq!(
            within(ArtifactSelector::new().the(the.clone()).of(of.clone())),
            vec![cell("person/name", "id:a")]
        );
        assert_eq!(
            within(ArtifactSelector::new().of(of.clone())),
            vec![
                cell("list/Mx", "id:a"),
                cell("person/age", "id:a"),
                cell("person/name", "id:a")
            ]
        );
        assert_eq!(
            within(ArtifactSelector::new().the_starting_with("person/")),
            vec![
                cell("person/age", "id:a"),
                cell("person/name", "id:a"),
                cell("person/name", "id:b")
            ]
        );
        assert_eq!(
            within(
                ArtifactSelector::new()
                    .the_starting_with("person/")
                    .of(of.clone())
            ),
            vec![cell("person/age", "id:a"), cell("person/name", "id:a")]
        );
        assert_eq!(
            within(
                ArtifactSelector::new()
                    .of_starting_with("id:")
                    .with_name("name".parse::<dialog_artifacts::Name>().expect("name"))
            ),
            vec![
                cell("other/name", "id:c"),
                cell("person/name", "id:a"),
                cell("person/name", "id:b")
            ]
        );
        assert_eq!(
            within(
                ArtifactSelector::new()
                    .of(of.clone())
                    .with_name_shape(NameShape::Position)
            ),
            vec![cell("list/Mx", "id:a")]
        );
        assert_eq!(
            within(
                ArtifactSelector::new()
                    .with_name_shape(NameShape::Position)
                    .is(dialog_artifacts::Value::String("x".into()))
            ),
            vec![cell("list/Mx", "id:a")],
            "a shape alone visits every cell"
        );
        assert_eq!(
            within(ArtifactSelector::new().is(dialog_artifacts::Value::String("x".into()))).len(),
            5,
            "a value bound reads every cell"
        );
        assert!(within(ArtifactSelector::new().the(cell("person/born", "id:a").0)).is_empty());
        assert!(within(ArtifactSelector::new().of_starting_with("id:z")).is_empty());
        assert!(!cells.is_empty());
        assert!(ElectingCells::default().is_empty());
    }
}
