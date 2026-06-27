//! Process-global plan cache, keyed by `(rule identity, adornment)`.
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
//! its layers, so a per-instance plan cache would never be reused. The
//! global cache keys on the rule's content-addressed identity
//! ([`DeductiveRule::this`]), which is stable across re-assembly, so the
//! expensive part — planning — is paid once per `(rule, adornment)` for
//! the life of the process.
//!
//! # Why content addressing makes this correct
//!
//! The key is `(rule.this(), adornment)`. `rule.this()` is a content
//! hash of the rule body, so a cached [`Conjunction`] is *never* stale:
//! a different rule body is a different key. Entries only ever need
//! eviction for memory, never for correctness; the cap below is a plain
//! bound, not an invalidation mechanism.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::artifact::Entity;
use crate::concept::query::adornment::Adornment;
use crate::planner::Conjunction;
use crate::rule::deductive::DeductiveRule;

/// Soft cap on distinct `(rule, adornment)` entries. Reaching it clears
/// the cache wholesale — content addressing means this only re-pays
/// planning, never returns a wrong plan. The cap is generous: real
/// workloads have few rules and few adornments per rule.
const CAPACITY: usize = 4096;

type Key = (Entity, Adornment);

fn cache() -> &'static Mutex<HashMap<Key, Conjunction>> {
    static CACHE: OnceLock<Mutex<HashMap<Key, Conjunction>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The planned [`Conjunction`] for `rule` under `adornment`, computing
/// and caching it on a miss. `plan` is only called on a miss, so the
/// caller can build the scope lazily.
///
/// A rule with no content-addressed identity (the implicit rule, or any
/// rule built from raw attribute queries — see
/// [`DeductiveRule::try_this`]) is planned directly and never cached:
/// such a rule has no stable key, and it is cheap to plan anyway.
pub(crate) fn get_or_plan<F>(rule: &DeductiveRule, adornment: Adornment, plan: F) -> Conjunction
where
    F: FnOnce() -> Conjunction,
{
    let Some(identity) = rule.try_this() else {
        return plan();
    };
    let key = (identity, adornment);

    if let Ok(map) = cache().lock()
        && let Some(hit) = map.get(&key)
    {
        return hit.clone();
    }

    let planned = plan();

    if let Ok(mut map) = cache().lock() {
        if map.len() >= CAPACITY {
            map.clear();
        }
        map.insert(key, planned.clone());
    }

    planned
}
