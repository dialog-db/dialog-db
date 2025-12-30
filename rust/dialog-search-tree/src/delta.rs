use parking_lot::Mutex;
use std::sync::Arc;

use hashbrown::HashMap;

/// A thread-safe accumulator of pending changes to tree nodes.
#[derive(Clone, Debug)]
pub struct Delta<K, V> {
    contents: Arc<Mutex<HashMap<K, V>>>,
}

impl<K, V> Delta<K, V>
where
    K: Clone + std::hash::Hash + PartialEq + Eq + std::fmt::Display,
    V: Clone,
{
    /// Creates an empty delta with no pending changes.
    pub fn zero() -> Self {
        Self {
            contents: Default::default(),
        }
    }

    /// Creates a new delta that contains a copy of this delta's contents.
    pub fn branch(&self) -> Self {
        Self {
            contents: Arc::new(Mutex::new(self.contents.lock().clone())),
        }
    }

    /// Adds a key-value pair to this delta.
    pub fn add(&mut self, key: K, value: V) {
        let mut contents = self.contents.lock();
        contents.insert(key.clone(), value);
    }

    /// Adds multiple key-value pairs to this delta.
    pub fn add_all<Entries>(&mut self, entries: Entries)
    where
        Entries: Iterator<Item = (K, V)>,
    {
        let mut contents = self.contents.lock();
        for (key, value) in entries {
            contents.insert(key.clone(), value);
        }
    }

    /// Removes a key from this delta.
    pub fn subtract(&mut self, key: &K) {
        let mut contents = self.contents.lock();
        contents.remove(key);
    }

    /// Removes multiple keys from this delta.
    pub fn subtract_all<'a, Keys>(&'a mut self, keys: Keys)
    where
        Keys: Iterator<Item = &'a K>,
    {
        let mut contents = self.contents.lock();
        for key in keys {
            contents.remove(key);
        }
    }

    /// Retrieves the value associated with a key from this delta.
    pub fn get(&self, key: &K) -> Option<V> {
        let contents = self.contents.lock();
        contents.get(key).cloned()
    }

    /// Drains all entries from this delta and returns them as an iterator.
    pub fn flush(&mut self) -> impl Iterator<Item = (K, V)> {
        std::mem::take(&mut *self.contents.lock()).into_iter()
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use super::*;
    use anyhow::Result;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    #[dialog_common::test]
    async fn it_creates_an_empty_delta() -> Result<()> {
        let delta = Delta::<u32, String>::zero();
        assert_eq!(delta.get(&1), None);
        assert_eq!(delta.get(&100), None);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_adds_a_single_entry() -> Result<()> {
        let mut delta = Delta::<u32, String>::zero();

        delta.add(1, "value1".to_string());
        assert_eq!(delta.get(&1), Some("value1".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_adds_multiple_entries() -> Result<()> {
        let mut delta = Delta::<u32, String>::zero();

        delta.add(1, "value1".to_string());
        delta.add(2, "value2".to_string());
        delta.add(3, "value3".to_string());

        assert_eq!(delta.get(&1), Some("value1".to_string()));
        assert_eq!(delta.get(&2), Some("value2".to_string()));
        assert_eq!(delta.get(&3), Some("value3".to_string()));
        assert_eq!(delta.get(&4), None);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_overwrites_existing_entries() -> Result<()> {
        let mut delta = Delta::<u32, String>::zero();

        delta.add(1, "value1".to_string());
        assert_eq!(delta.get(&1), Some("value1".to_string()));

        delta.add(1, "updated".to_string());
        assert_eq!(delta.get(&1), Some("updated".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_adds_all_entries_from_an_iterator() -> Result<()> {
        let mut delta = Delta::<u32, String>::zero();

        let entries = vec![
            (1, "value1".to_string()),
            (2, "value2".to_string()),
            (3, "value3".to_string()),
        ];

        delta.add_all(entries.into_iter());

        assert_eq!(delta.get(&1), Some("value1".to_string()));
        assert_eq!(delta.get(&2), Some("value2".to_string()));
        assert_eq!(delta.get(&3), Some("value3".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_subtracts_an_entry() -> Result<()> {
        let mut delta = Delta::<u32, String>::zero();

        delta.add(1, "value1".to_string());
        delta.add(2, "value2".to_string());

        assert_eq!(delta.get(&1), Some("value1".to_string()));
        assert_eq!(delta.get(&2), Some("value2".to_string()));

        delta.subtract(&1);

        assert_eq!(delta.get(&1), None);
        assert_eq!(delta.get(&2), Some("value2".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_subtracts_nonexistent_entry_without_error() -> Result<()> {
        let mut delta = Delta::<u32, String>::zero();

        delta.add(1, "value1".to_string());

        // Subtracting non-existent key should not panic
        delta.subtract(&999);

        assert_eq!(delta.get(&1), Some("value1".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_subtracts_all_entries_from_an_iterator() -> Result<()> {
        let mut delta = Delta::<u32, String>::zero();

        delta.add(1, "value1".to_string());
        delta.add(2, "value2".to_string());
        delta.add(3, "value3".to_string());
        delta.add(4, "value4".to_string());

        let keys_to_remove = vec![1, 3];
        delta.subtract_all(keys_to_remove.iter());

        assert_eq!(delta.get(&1), None);
        assert_eq!(delta.get(&2), Some("value2".to_string()));
        assert_eq!(delta.get(&3), None);
        assert_eq!(delta.get(&4), Some("value4".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_branches_to_create_independent_copy() -> Result<()> {
        let mut delta1 = Delta::<u32, String>::zero();

        delta1.add(1, "value1".to_string());
        delta1.add(2, "value2".to_string());

        let mut delta2 = delta1.branch();

        // Verify delta2 has the same contents initially
        assert_eq!(delta2.get(&1), Some("value1".to_string()));
        assert_eq!(delta2.get(&2), Some("value2".to_string()));

        // Modify delta2
        delta2.add(3, "value3".to_string());
        delta2.subtract(&1);

        // Verify delta1 is unchanged
        assert_eq!(delta1.get(&1), Some("value1".to_string()));
        assert_eq!(delta1.get(&2), Some("value2".to_string()));
        assert_eq!(delta1.get(&3), None);

        // Verify delta2 has the changes
        assert_eq!(delta2.get(&1), None);
        assert_eq!(delta2.get(&2), Some("value2".to_string()));
        assert_eq!(delta2.get(&3), Some("value3".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_flushes_all_entries() -> Result<()> {
        let mut delta = Delta::<u32, String>::zero();

        delta.add(1, "value1".to_string());
        delta.add(2, "value2".to_string());
        delta.add(3, "value3".to_string());

        let entries: Vec<(u32, String)> = delta.flush().collect();

        // Verify all entries were flushed
        assert_eq!(entries.len(), 3);
        assert!(entries.contains(&(1, "value1".to_string())));
        assert!(entries.contains(&(2, "value2".to_string())));
        assert!(entries.contains(&(3, "value3".to_string())));

        // Verify delta is now empty
        assert_eq!(delta.get(&1), None);
        assert_eq!(delta.get(&2), None);
        assert_eq!(delta.get(&3), None);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_flushes_an_empty_delta() -> Result<()> {
        let mut delta = Delta::<u32, String>::zero();

        let entries: Vec<(u32, String)> = delta.flush().collect();

        assert_eq!(entries.len(), 0);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_flushes_one_delta_without_affecting_branched_copy() -> Result<()> {
        let mut delta1 = Delta::<u32, String>::zero();

        delta1.add(1, "value1".to_string());
        delta1.add(2, "value2".to_string());
        delta1.add(3, "value3".to_string());

        // Branch creates an independent copy
        let delta2 = delta1.branch();

        // Verify both have the same initial data
        assert_eq!(delta1.get(&1), Some("value1".to_string()));
        assert_eq!(delta1.get(&2), Some("value2".to_string()));
        assert_eq!(delta1.get(&3), Some("value3".to_string()));

        assert_eq!(delta2.get(&1), Some("value1".to_string()));
        assert_eq!(delta2.get(&2), Some("value2".to_string()));
        assert_eq!(delta2.get(&3), Some("value3".to_string()));

        // Flush delta1
        let entries: Vec<(u32, String)> = delta1.flush().collect();
        assert_eq!(entries.len(), 3);

        // Verify delta1 is now empty
        assert_eq!(delta1.get(&1), None);
        assert_eq!(delta1.get(&2), None);
        assert_eq!(delta1.get(&3), None);

        // Verify delta2 still has all its data (unaffected by flush)
        assert_eq!(delta2.get(&1), Some("value1".to_string()));
        assert_eq!(delta2.get(&2), Some("value2".to_string()));
        assert_eq!(delta2.get(&3), Some("value3".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_mixed_operations() -> Result<()> {
        let mut delta = Delta::<u32, String>::zero();

        // Add some entries
        delta.add(1, "value1".to_string());
        delta.add(2, "value2".to_string());
        delta.add(3, "value3".to_string());

        // Update one
        delta.add(2, "updated2".to_string());

        // Remove one
        delta.subtract(&1);

        // Add more
        delta.add(4, "value4".to_string());

        // Verify final state
        assert_eq!(delta.get(&1), None);
        assert_eq!(delta.get(&2), Some("updated2".to_string()));
        assert_eq!(delta.get(&3), Some("value3".to_string()));
        assert_eq!(delta.get(&4), Some("value4".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_works_with_string_keys() -> Result<()> {
        let mut delta = Delta::<String, Vec<u8>>::zero();

        let key1 = "key1".to_string();
        let key2 = "key2".to_string();
        let value1 = vec![1, 2, 3, 4];
        let value2 = vec![5, 6, 7, 8];

        delta.add(key1.clone(), value1.clone());
        delta.add(key2.clone(), value2.clone());

        assert_eq!(delta.get(&key1), Some(value1));
        assert_eq!(delta.get(&key2), Some(value2));

        delta.subtract(&key1);
        assert_eq!(delta.get(&key1), None);
        assert_eq!(delta.get(&key2), Some(vec![5, 6, 7, 8]));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_large_number_of_entries() -> Result<()> {
        let mut delta = Delta::<u32, u32>::zero();

        // Add 1000 entries
        for i in 0..1000 {
            delta.add(i, i * 2);
        }

        // Verify some entries
        assert_eq!(delta.get(&0), Some(0));
        assert_eq!(delta.get(&500), Some(1000));
        assert_eq!(delta.get(&999), Some(1998));

        // Remove every other entry
        for i in (0..1000).step_by(2) {
            delta.subtract(&i);
        }

        // Verify removals
        assert_eq!(delta.get(&0), None);
        assert_eq!(delta.get(&1), Some(2));
        assert_eq!(delta.get(&500), None);
        assert_eq!(delta.get(&501), Some(1002));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_is_thread_safe_across_clones() -> Result<()> {
        let mut delta1 = Delta::<u32, String>::zero();
        delta1.add(1, "value1".to_string());

        // Clone the delta (Arc is cloned, not the inner data)
        let delta2 = delta1.clone();

        // Both should see the same data
        assert_eq!(delta1.get(&1), Some("value1".to_string()));
        assert_eq!(delta2.get(&1), Some("value1".to_string()));

        // Modifying one affects the other (shared Arc)
        delta1.add(2, "value2".to_string());
        assert_eq!(delta2.get(&2), Some("value2".to_string()));

        Ok(())
    }
}
