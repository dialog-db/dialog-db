//! Remembered outputs of formulas that ask for it.
//!
//! A formula is a pure function of its inputs, so what it produced once for
//! some inputs it produces again. Most formulas are cheaper to run than to
//! look up, so they are simply run. A formula that is not, verifying a
//! signature say, is marked `#[formula(cached)]` when it is derived, and the
//! engine keeps its outputs here: by the formula and its input values, in a
//! cache the environment holds, so whoever owns the environment decides how
//! long they are kept and can let them go.

use std::any::type_name;
use std::fmt;
use std::sync::Arc;

use dialog_common::{ConditionalSend, ConditionalSync, Held, Holds, held_key};
use serde::Serialize;

#[cfg(not(target_arch = "wasm32"))]
use sieve_cache::ShardedSieveCache as SieveCache;
#[cfg(target_arch = "wasm32")]
use sieve_cache::SieveCache;
#[cfg(target_arch = "wasm32")]
use std::{cell::RefCell, rc::Rc};

use crate::{EvaluationError, Value};

/// How many formula applications a cache remembers when no capacity is
/// named. The verified-record memo this replaces kept as many.
pub const FORMULA_CACHE_CAPACITY: usize = 4096;

/// The key an environment holds its formula cache under.
const HELD: &str = "dialog.formulas";

/// The outputs of formulas marked `#[formula(cached)]`, by formula and input
/// values. Bounded by how many applications it remembers; the least useful
/// are let go first.
///
/// Clones share the cache.
#[derive(Clone)]
pub struct FormulaCache {
    #[cfg(not(target_arch = "wasm32"))]
    entries: Arc<SieveCache<[u8; 32], Held>>,
    #[cfg(target_arch = "wasm32")]
    entries: Rc<RefCell<SieveCache<[u8; 32], Held>>>,
    capacity: usize,
}

impl fmt::Debug for FormulaCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FormulaCache")
            .field("capacity", &self.capacity)
            .field("entries", &self.len())
            .finish()
    }
}

impl Default for FormulaCache {
    fn default() -> Self {
        Self::new()
    }
}

impl FormulaCache {
    /// A cache remembering up to [`FORMULA_CACHE_CAPACITY`] applications.
    pub fn new() -> Self {
        Self::with_capacity(FORMULA_CACHE_CAPACITY)
    }

    /// A cache remembering up to `capacity` applications.
    pub fn with_capacity(capacity: usize) -> Self {
        // SAFETY of the expect: a zero capacity is raised to one.
        let cache = SieveCache::new(capacity.max(1)).expect("non-zero formula cache capacity");
        Self {
            #[cfg(not(target_arch = "wasm32"))]
            entries: Arc::new(cache),
            #[cfg(target_arch = "wasm32")]
            entries: Rc::new(RefCell::new(cache)),
            capacity,
        }
    }

    /// The formula cache `env` holds, made with the default capacity if it
    /// holds none yet.
    pub fn of<Env: Holds + ?Sized>(env: &Env) -> Self {
        env.held_or(&held_key::<Self>(HELD), &|| Arc::new(Self::new()))
            .downcast_ref::<Self>()
            .cloned()
            // Only if something else is held under the key: a cache of the
            // caller's own, which remembers for this evaluation alone.
            .unwrap_or_default()
    }

    /// Have `env` hold this cache, in place of any it held.
    pub fn hold<Env: Holds + ?Sized>(&self, env: &Env) {
        env.hold(held_key::<Self>(HELD), Arc::new(self.clone()));
    }

    /// How many applications the cache may remember.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// How many applications the cache remembers now.
    pub fn len(&self) -> usize {
        #[cfg(not(target_arch = "wasm32"))]
        let len = self.entries.len();
        #[cfg(target_arch = "wasm32")]
        let len = self.entries.borrow().len();
        len
    }

    /// Whether the cache remembers nothing.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Forgets everything.
    pub fn clear(&self) {
        #[cfg(not(target_arch = "wasm32"))]
        self.entries.clear();
        #[cfg(target_arch = "wasm32")]
        self.entries.borrow_mut().clear();
    }

    /// The outputs of formula `F` for `inputs`, remembered or computed with
    /// `compute` and remembered: what a formula derived with
    /// `#[formula(cached)]` resolves through.
    ///
    /// `inputs` are the formula's input values in a fixed order; with the
    /// formula's type they are the whole key, so two formulas never share an
    /// entry. A computation that fails is not remembered.
    pub fn outputs<F>(
        &self,
        inputs: &[Value],
        compute: impl FnOnce() -> Result<Vec<F>, EvaluationError>,
    ) -> Result<Arc<Vec<F>>, EvaluationError>
    where
        F: 'static + ConditionalSend + ConditionalSync,
    {
        let key = key::<F>(inputs)?;
        if let Some(outputs) = self
            .get(&key)
            .and_then(|held| held.downcast_ref::<Arc<Vec<F>>>().cloned())
        {
            return Ok(outputs);
        }
        let outputs = Arc::new(compute()?);
        let held: Held = Arc::new(outputs.clone());
        self.insert(key, held);
        Ok(outputs)
    }

    fn get(&self, key: &[u8; 32]) -> Option<Held> {
        #[cfg(not(target_arch = "wasm32"))]
        let held = self.entries.get(key);
        #[cfg(target_arch = "wasm32")]
        let held = self.entries.borrow_mut().get(key).cloned();
        held
    }

    fn insert(&self, key: [u8; 32], held: Held) {
        #[cfg(not(target_arch = "wasm32"))]
        self.entries.insert(key, held);
        #[cfg(target_arch = "wasm32")]
        self.entries.borrow_mut().insert(key, held);
    }
}

/// The key of formula `F` applied to `inputs`: the hash of the formula's
/// name and the canonical encoding of the values.
fn key<F>(inputs: &[Value]) -> Result<[u8; 32], EvaluationError> {
    #[derive(Serialize)]
    struct Application<'a> {
        formula: &'a str,
        inputs: &'a [Value],
    }
    let encoded = serde_ipld_dagcbor::to_vec(&Application {
        formula: type_name::<F>(),
        inputs,
    })
    .map_err(|error| EvaluationError::Serialization {
        message: format!("formula inputs do not encode: {error}"),
    })?;
    Ok(*blake3::hash(&encoded).as_bytes())
}
