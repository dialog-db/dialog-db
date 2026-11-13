//! An inductive rule as commit-time induction holds it: the rule, and
//! the plans its seeds were given.
//!
//! A seeded firing binds a changed row into a premise and evaluates the
//! rule's body with that binding fixed, under a plan ordered for the
//! names the seed binds. A commit of 500 commands seeds a rule 500
//! times with one set of names, and planned it 500 times.
//!
//! A plan is a function of the rule and of the set of names bound, and
//! of nothing else. `Planner::plan` is handed the rule's premises, the
//! types analysis inferred and the scope. It stamps the types on the
//! premises by variable name (`apply_types`), then picks, step by step,
//! the cheapest premise that is feasible under the names bound so far,
//! the first of equals in the order the rule lists them. Feasibility
//! and cost are asked of the premise and the bound names alone
//! (`Premise::feasible`, `Premise::estimate`): no value a seed binds,
//! no store and no statistic is read, so two seeds that bind the same
//! names get the same plan whatever values they bind. The ranges a
//! conjunction estimates before it merges are read when a plan is
//! evaluated, not when it is made.
//!
//! So the plans are kept with the rule, by the set of names bound. They
//! live as long as the decoded rule does, in the cache the environment
//! bounds, and a rule keeps at most [`PLANS`] of them.

use dialog_query::{Conjunction, Environment, InductiveRule};
use parking_lot::RwLock;

/// The most plans one rule keeps. A rule has a scope for each way its
/// premises are seeded, a handful; a scope past the bound is planned
/// each time, as every scope was before.
pub(crate) const PLANS: usize = 32;

/// An inductive rule decoded from its stored bytes, with the plans its
/// body was given so far, by the names bound.
#[derive(Debug)]
pub(crate) struct Induced {
    rule: InductiveRule,
    plans: RwLock<Vec<(Environment, Conjunction)>>,
}

impl Induced {
    /// `rule`, with no plan kept yet.
    pub(crate) fn new(rule: InductiveRule) -> Self {
        Induced {
            rule,
            plans: RwLock::new(Vec::new()),
        }
    }

    /// The rule.
    pub(crate) fn rule(&self) -> &InductiveRule {
        &self.rule
    }

    /// The plan of the rule's body for `scope`, the names already bound:
    /// the one kept for that set of names, or a new one, kept for the
    /// next seed that binds them.
    ///
    /// In this crate's tests a kept plan is checked, every time it is
    /// handed out, against the plan made afresh for the same scope.
    pub(crate) fn plan(&self, scope: &Environment) -> Conjunction {
        #[cfg(test)]
        if probe::is_off() {
            probe::planned();
            return self.rule.plan(scope);
        }
        if let Some((_, plan)) = self.plans.read().iter().find(|(known, _)| known == scope) {
            #[cfg(test)]
            {
                probe::kept();
                assert_eq!(
                    *plan,
                    self.rule.plan(scope),
                    "a kept plan must be the plan made afresh for its scope"
                );
            }
            return plan.clone();
        }
        #[cfg(test)]
        probe::planned();
        let plan = self.rule.plan(scope);
        let mut plans = self.plans.write();
        if plans.len() < PLANS && !plans.iter().any(|(known, _)| known == scope) {
            plans.push((scope.clone(), plan.clone()));
        }
        plan
    }

    /// How many plans are kept.
    #[cfg(test)]
    pub(crate) fn kept(&self) -> usize {
        self.plans.read().len()
    }
}

/// In tests: keeping plans turned off, so the same commits can be run
/// both ways, and counts of the plans made and the plans reused, so a
/// test can tell that plans were kept.
///
/// `DIALOG_TEST_PLANS=off` keeps none for a whole run of the tests, to
/// see which of them hold the keeping up.
#[cfg(test)]
pub(crate) mod probe {
    use std::cell::Cell;
    use std::env;
    use std::sync::OnceLock;

    /// Whether `DIALOG_TEST_PLANS` asks for no plan to be kept, read
    /// once.
    fn asked_off() -> bool {
        static OFF: OnceLock<bool> = OnceLock::new();
        *OFF.get_or_init(|| env::var("DIALOG_TEST_PLANS").is_ok_and(|asked| asked == "off"))
    }

    // Per thread: the test runtime drives each test on one thread.
    thread_local! {
        static OFF: Cell<bool> = const { Cell::new(false) };
        static PLANNED: Cell<usize> = const { Cell::new(0) };
        static KEPT: Cell<usize> = const { Cell::new(0) };
    }

    /// Stop keeping plans on this thread, or start again.
    pub(crate) fn off(off: bool) {
        OFF.with(|cell| cell.set(off));
    }

    pub(super) fn is_off() -> bool {
        OFF.with(Cell::get) || asked_off()
    }

    pub(super) fn planned() {
        PLANNED.with(|cell| cell.set(cell.get() + 1));
    }

    pub(super) fn kept() {
        KEPT.with(|cell| cell.set(cell.get() + 1));
    }

    /// How many plans were made and how many kept ones were handed out
    /// on this thread since the last call.
    pub(crate) fn take() -> (usize, usize) {
        (
            PLANNED.with(|cell| cell.replace(0)),
            KEPT.with(|cell| cell.replace(0)),
        )
    }
}

#[cfg(test)]
mod tests;
