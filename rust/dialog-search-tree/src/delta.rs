use parking_lot::Mutex;
use std::sync::Arc;

use hashbrown::HashMap;

#[derive(Clone, Debug)]
pub struct Delta<K, V> {
    contents: Arc<Mutex<HashMap<K, V>>>,
}

impl<K, V> Delta<K, V>
where
    K: Clone + std::hash::Hash + PartialEq + Eq + std::fmt::Display,
    V: Clone,
{
    pub fn zero() -> Self {
        Self {
            contents: Default::default(),
        }
    }

    pub fn branch(&self) -> Self {
        Self {
            contents: Arc::new(Mutex::new(self.contents.lock().clone())),
        }
    }

    pub fn add(&mut self, key: K, value: V) {
        let mut contents = self.contents.lock();
        contents.insert(key.clone(), value);
        println!("+ {} ({})", &key, contents.len());
    }

    pub fn add_all<Entries>(&mut self, entries: Entries)
    where
        Entries: Iterator<Item = (K, V)>,
    {
        let mut contents = self.contents.lock();
        for (key, value) in entries {
            contents.insert(key.clone(), value);
            println!("+ {} ({})", &key, contents.len());
        }
    }

    pub fn subtract(&mut self, key: &K) {
        let mut contents = self.contents.lock();
        contents.remove(key);
        println!("- {} ({})", &key, contents.len());
    }

    pub fn subtract_all<'a, Keys>(&'a mut self, keys: Keys)
    where
        Keys: Iterator<Item = &'a K>,
    {
        let mut contents = self.contents.lock();
        for key in keys {
            contents.remove(key);
            println!("- {} ({})", &key, contents.len());
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let contents = self.contents.lock();
        contents.get(key).map(|value| value.clone())
    }

    pub fn flush(&mut self) -> impl Iterator<Item = (K, V)> {
        std::mem::take(&mut *self.contents.lock()).into_iter()
    }
}
