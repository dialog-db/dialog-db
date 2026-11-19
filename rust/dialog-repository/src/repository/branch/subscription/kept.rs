//! A subscription's kept answer, indexed by the entity each row is about.

use std::collections::BTreeMap;
use std::sync::OnceLock;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use dialog_corpus::Entity;
use dialog_query::Conclusion;

use super::Delta;

/// The answer a subscription keeps between polls, in the order a slice of
/// it has always been read in, and indexed by each row's entity
/// ([`Conclusion::this`]).
///
/// A maintained poll compares each entity it re-derived with the rows kept
/// for it. Looking those rows up by scanning the whole answer cost every
/// kept row per entity, so a change touching many entities (a list renamed,
/// and every to-do's row reads the list's name) cost the entities times the
/// rows. Through the index an entity's comparison reads that entity's rows
/// only.
///
/// The rows are held under the sequence they were kept in: a recompute
/// keeps its rows in the order it read them, and re-deriving an entity
/// drops its rows and keeps what it re-derived after every other row,
/// exactly as the list the answer was kept in before did. The slice
/// [`rows`](Self::rows) reads is built from them when it is first asked
/// for after a change.
///
/// Rows that compare equal are about the same entity (every conclusion's
/// equality counts its entity), so whether a row is kept is answered by the
/// rows kept under its own entity.
pub(super) struct Kept<T> {
    /// Every kept row, under the sequence it was kept in.
    rows: BTreeMap<u64, T>,
    /// The sequences of each entity's rows, in the order they were kept.
    entities: BTreeMap<Entity, Vec<u64>>,
    /// The sequence the next kept row is held under.
    next: u64,
    /// The rows in order, as a slice: built on the first read after a
    /// change, and dropped by the next change.
    slice: OnceLock<Vec<T>>,
    /// How many kept rows comparisons have read, for the tests that pin
    /// that a poll reads only the rows of the entities it re-derived.
    #[cfg(test)]
    pub(super) reads: AtomicUsize,
}

impl<T> Default for Kept<T> {
    fn default() -> Self {
        Kept {
            rows: BTreeMap::new(),
            entities: BTreeMap::new(),
            next: 0,
            slice: OnceLock::new(),
            #[cfg(test)]
            reads: AtomicUsize::new(0),
        }
    }
}

impl<T> Kept<T>
where
    T: Conclusion + PartialEq + Clone,
{
    /// Every kept row, in the order they were kept.
    pub(super) fn rows(&self) -> &[T] {
        self.slice
            .get_or_init(|| self.rows.values().cloned().collect())
    }

    /// The rows kept for `entity`, in the order they were kept.
    fn of<'k>(&'k self, entity: &Entity) -> impl Iterator<Item = &'k T> + 'k {
        self.entities
            .get(entity)
            .into_iter()
            .flatten()
            .filter_map(|sequence| self.rows.get(sequence))
            .inspect(|_| self.read())
    }

    /// Whether a row equal to `row` is kept.
    fn holds(&self, row: &T) -> bool {
        self.of(row.this()).any(|kept| kept == row)
    }

    /// Keep `row` after every row kept so far.
    fn push(&mut self, row: T) {
        let sequence = self.next;
        self.next += 1;
        self.entities
            .entry(row.this().clone())
            .or_default()
            .push(sequence);
        self.rows.insert(sequence, row);
    }

    /// Keep `rows` in place of everything kept, and say what changed: the
    /// rows of `rows` not kept before, in their order, and the rows kept
    /// before that `rows` does not hold, in the order they were kept.
    pub(super) fn replace(&mut self, rows: Vec<T>) -> Delta<T> {
        let mut next: BTreeMap<&Entity, Vec<&T>> = BTreeMap::new();
        for row in &rows {
            next.entry(row.this()).or_default().push(row);
        }
        let asserted = rows
            .iter()
            .filter(|row| !self.holds(row))
            .cloned()
            .collect();
        let retracted = self
            .rows
            .values()
            .filter(|row| !next.get(row.this()).is_some_and(|kept| kept.contains(row)))
            .cloned()
            .collect();
        *self = Kept {
            next: self.next,
            ..Kept::default()
        };
        for row in rows {
            self.push(row);
        }
        Delta {
            asserted,
            retracted,
        }
    }

    /// Keep `after` in place of the rows kept for `entity`, after every other
    /// row, pushing onto `asserted` each row of `after` not kept for it
    /// before and onto `retracted` each row kept for it that `after` does
    /// not hold, in the order they were kept.
    pub(super) fn rederive(
        &mut self,
        entity: &Entity,
        after: Vec<T>,
        asserted: &mut Vec<T>,
        retracted: &mut Vec<T>,
    ) {
        let before: Vec<&T> = self.of(entity).collect();
        for row in &after {
            if !before.contains(&row) {
                asserted.push(row.clone());
            }
        }
        for row in &before {
            if !after.contains(row) {
                retracted.push((*row).clone());
            }
        }
        if let Some(sequences) = self.entities.remove(entity) {
            for sequence in sequences {
                self.rows.remove(&sequence);
            }
        }
        for row in after {
            self.push(row);
        }
        self.slice = OnceLock::new();
    }

    /// Count one kept row read by a comparison.
    #[inline(always)]
    fn read(&self) {
        #[cfg(test)]
        self.reads.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::Kept;
    use crate::repository::branch::subscription::Delta;
    use dialog_corpus::Entity;
    use dialog_query::Conclusion;

    /// A row about an entity, with a value to tell its rows apart.
    #[derive(Clone, Debug, PartialEq)]
    struct Row {
        this: Entity,
        value: u8,
    }

    impl Conclusion for Row {
        fn this(&self) -> &Entity {
            &self.this
        }
    }

    /// The list the answer was kept in before it was indexed: a
    /// recompute compares and replaces the whole list, a re-derived
    /// entity filters its rows out of it and appends what it re-derived.
    #[derive(Default)]
    struct Listed(Vec<Row>);

    impl Listed {
        fn replace(&mut self, rows: Vec<Row>) -> Delta<Row> {
            let delta = Delta {
                asserted: rows
                    .iter()
                    .filter(|row| !self.0.contains(row))
                    .cloned()
                    .collect(),
                retracted: self
                    .0
                    .iter()
                    .filter(|row| !rows.contains(row))
                    .cloned()
                    .collect(),
            };
            self.0 = rows;
            delta
        }

        fn rederive(
            &mut self,
            entity: &Entity,
            after: Vec<Row>,
            asserted: &mut Vec<Row>,
            retracted: &mut Vec<Row>,
        ) {
            let before: Vec<Row> = self
                .0
                .iter()
                .filter(|row| row.this() == entity)
                .cloned()
                .collect();
            for row in &after {
                if !before.contains(row) {
                    asserted.push(row.clone());
                }
            }
            for row in &before {
                if !after.contains(row) {
                    retracted.push(row.clone());
                }
            }
            self.0.retain(|row| row.this() != entity);
            self.0.extend(after);
        }
    }

    /// Whatever recomputes and re-derivations a subscription runs, the
    /// indexed answer reads in the order the list did, and reports the
    /// same rows asserted and retracted in the same order, duplicate rows
    /// included.
    #[dialog_common::test]
    fn it_keeps_the_order_and_the_deltas_of_a_list() -> anyhow::Result<()> {
        let entities = (0..6)
            .map(|index| format!("id:entity-{index}").parse())
            .collect::<Result<Vec<Entity>, _>>()?;
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut below = |n: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % n as u64) as usize
        };
        let mut kept = Kept::default();
        let mut listed = Listed::default();
        for round in 0..400 {
            let rows = |below: &mut dyn FnMut(usize) -> usize, entity: Option<&Entity>| {
                (0..below(5))
                    .map(|_| Row {
                        this: entity
                            .cloned()
                            .unwrap_or_else(|| entities[below(entities.len())].clone()),
                        value: below(3) as u8,
                    })
                    .collect::<Vec<Row>>()
            };
            if below(8) == 0 {
                let next = rows(&mut below, None);
                assert_eq!(
                    kept.replace(next.clone()),
                    listed.replace(next),
                    "round {round}: a recompute's delta"
                );
            } else {
                let (mut ours, mut theirs) = ((Vec::new(), Vec::new()), (Vec::new(), Vec::new()));
                for _ in 0..1 + below(3) {
                    let entity = entities[below(entities.len())].clone();
                    let after = rows(&mut below, Some(&entity));
                    kept.rederive(&entity, after.clone(), &mut ours.0, &mut ours.1);
                    listed.rederive(&entity, after, &mut theirs.0, &mut theirs.1);
                }
                assert_eq!(ours, theirs, "round {round}: a maintained poll's delta");
            }
            if below(2) == 0 {
                assert_eq!(kept.rows(), listed.0.as_slice(), "round {round}: the rows");
            }
        }
        assert_eq!(kept.rows(), listed.0.as_slice());
        Ok(())
    }
}
