//! Per-rule plan cache, keyed by `(rule identity, adornment)`.
//!
//! Planning a deductive rule for a binding pattern is a pure function
//! of `(the rule body, the adornment)` — proven name-independent (see
//! `it_plans_independently_of_caller_variable_names` in the
//! [`rules`](super::rules) tests). So a single rule's planned
//! [`Conjunction`] can be memoized across *every* query that uses it,
//! regardless of which concept assembled it or which layer surfaced
//! it.
//!
//! This matters for the layered resolution model: each query
//! re-assembles a concept's [`ConceptRules`](super::ConceptRules) from
//! its layers (a fresh instance every time), so the per-instance plan
//! map on `ConceptRules` is cold on each query. A shared [`PlanCache`]
//! — keyed on the rule's content-addressed identity
//! ([`DeductiveRule::try_this`]), which is stable across re-assembly —
//! carries the planned [`Conjunction`] across queries, so the expensive
//! part (planning) is paid once per `(rule, adornment)`.
//!
//! # Ownership
//!
//! A [`PlanCache`] is *not* a process global. It is held by the durable
//! storage handle that owns the rules it caches plans for (a branch in
//! `dialog-repository`, beside its node and rule caches) and handed to
//! each assembled [`ConceptRules`](super::ConceptRules). A
//! `ConceptRules` built without one (the in-memory registry, tests) gets
//! a fresh private cache via [`PlanCache::default`]. Lifecycle follows
//! the owner: drop the branch, drop its cached plans.
//!
//! # Why content addressing makes this correct
//!
//! The key is `(rule.try_this(), adornment)`. `try_this()` is a content
//! hash of the rule body, so a cached [`Conjunction`] is *never* stale:
//! a different rule body is a different key. Entries are only ever
//! evicted to bound memory, never for correctness — eviction just
//! re-pays planning, it cannot return a wrong plan.

use crate::artifact::Entity;
use crate::concept::query::adornment::Adornment;
use crate::planner::Conjunction;
use crate::rule::deductive::DeductiveRule;

#[cfg(not(target_arch = "wasm32"))]
use sieve_cache::ShardedSieveCache as SieveCache;
#[cfg(target_arch = "wasm32")]
use sieve_cache::SieveCache;
use std::fmt;
#[cfg(target_arch = "wasm32")]
use std::{cell::RefCell, rc::Rc};

/// Capacity bound on distinct `(rule, adornment)` plan entries. SIEVE
/// eviction reclaims the coldest entry past this point. Content
/// addressing means eviction only re-pays planning, never returns a
/// wrong plan, so this is a memory bound, not an invalidation knob.
const CAPACITY: usize = 4096;

type Key = (Entity, Adornment);

/// A shared, bounded cache of planned rule [`Conjunction`]s keyed by
/// `(content-addressed rule identity, adornment)`.
///
/// Mirrors the in-house `dialog_search_tree::Cache` shape: a
/// `ShardedSieveCache` on native (internally synchronized, values shared
/// via `Arc`) and an `Rc<RefCell<SieveCache>>` on wasm. Cloning a
/// `PlanCache` shares the same underlying store, so a branch hands a
/// clone to every [`ConceptRules`](super::ConceptRules) it assembles and
/// they all read and fill one cache.
#[derive(Clone)]
pub struct PlanCache {
    #[cfg(not(target_arch = "wasm32"))]
    cache: SieveCache<Key, Conjunction>,
    #[cfg(target_arch = "wasm32")]
    cache: Rc<RefCell<SieveCache<Key, Conjunction>>>,
}

impl fmt::Debug for PlanCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[cfg(not(target_arch = "wasm32"))]
        let len = self.cache.len();
        #[cfg(target_arch = "wasm32")]
        let len = self.cache.borrow().len();
        f.debug_struct("PlanCache").field("entries", &len).finish()
    }
}

impl Default for PlanCache {
    fn default() -> Self {
        // SAFETY: `SieveCache::new` only errors on a zero capacity.
        let cache = SieveCache::new(CAPACITY).expect("non-zero plan-cache capacity");
        Self {
            #[cfg(not(target_arch = "wasm32"))]
            cache,
            #[cfg(target_arch = "wasm32")]
            cache: Rc::new(RefCell::new(cache)),
        }
    }
}

impl PlanCache {
    /// The planned [`Conjunction`] for `rule` under `adornment`,
    /// computing and caching it on a miss. `plan` is only called on a
    /// miss, so the caller can build the scope lazily.
    ///
    /// A rule with no content-addressed identity (the implicit rule, or
    /// any rule built from raw attribute queries — see
    /// [`DeductiveRule::try_this`]) is planned directly and never cached:
    /// such a rule has no stable key, and it is cheap to plan anyway.
    pub(crate) fn get_or_plan<F>(
        &self,
        rule: &DeductiveRule,
        adornment: Adornment,
        plan: F,
    ) -> Conjunction
    where
        F: FnOnce() -> Conjunction,
    {
        let Some(identity) = rule.try_this() else {
            return plan();
        };
        let key = (identity, adornment);

        if let Some(hit) = self.get(&key) {
            return hit;
        }

        let planned = plan();
        self.insert(key, planned.clone());
        planned
    }

    fn get(&self, key: &Key) -> Option<Conjunction> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.cache.get(key)
        }
        #[cfg(target_arch = "wasm32")]
        {
            self.cache.borrow_mut().get(key).cloned()
        }
    }

    fn insert(&self, key: Key, value: Conjunction) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.cache.insert(key, value);
        }
        #[cfg(target_arch = "wasm32")]
        {
            self.cache.borrow_mut().insert(key, value);
        }
    }
}
