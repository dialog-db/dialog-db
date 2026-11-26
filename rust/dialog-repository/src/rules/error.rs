//! Why a rule a branch holds is not evaluated: a rule that did not
//! compile here.
//!
//! A rule installed through the API compiles before it is written, so a
//! rule that does not compile reaches a branch only from elsewhere: a
//! replica on another release, another implementation, or a writer of
//! raw `dialog.rule/*` facts. Such a rule works as if it were not there,
//! and is answered under `dialog.rule/error` with its [`RuleError`].
//!
//! Most kinds are the rule's own, fixed for its bytes under this
//! release; a cycle depends on which other rules the branch holds, and
//! lifts once a rule of the cycle is retracted
//! ([`RuleError::depends_on_other_rules`]).

use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::sync::LazyLock;

use dialog_corpus::{Change, Changes, Entity, Relation, Selector, Value};
use dialog_query::rule::body::{BodyError, FORMAT, Stamp, TERMS_FORMAT};
use dialog_query::session::Quarantine;
use dialog_query::{DeductiveRule, InductiveRule};

use crate::rules::source_attr;
use crate::{CommitError, RuleCache, Staged};

/// Why a rule a branch holds is not evaluated there.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuleError {
    /// The body is in a format older than this release reads as it
    /// stands, and does not decode in it: `Branch::upgrade_rules`
    /// rewrites it.
    Older {
        /// The format the body names.
        format: u64,
    },
    /// The body is in a format newer than this release reads: a newer
    /// release reads it.
    Newer {
        /// The format the body names.
        format: u64,
    },
    /// The body is a rule, stored under an entity it does not hash to.
    Forged,
    /// The bytes decode to no rule body.
    Undecodable {
        /// Why they do not.
        reason: String,
    },
    /// The body decodes and does not compile: the analysis refuses it.
    IllTyped {
        /// What the analysis refused.
        reason: String,
    },
    /// The rule compiles, and closes a cycle through an absence test with
    /// the other rules the branch holds, which has no stratified meaning:
    /// the program analysis sets it aside.
    Cycle {
        /// The concept the rule concludes.
        concept: Entity,
        /// The concepts of the cycle it closed, sorted.
        cycle: Vec<Entity>,
    },
}

impl RuleError {
    /// The kind, as the diagnostic under `dialog.rule/error` begins.
    pub fn code(&self) -> &'static str {
        match self {
            RuleError::Older { .. } => "older",
            RuleError::Newer { .. } => "newer",
            RuleError::Forged => "forged",
            RuleError::Undecodable { .. } => "undecodable",
            RuleError::IllTyped { .. } => "ill-typed",
            RuleError::Cycle { .. } => "cycle",
        }
    }

    /// Whether the error depends on which other rules the branch holds:
    /// a cycle does, and lifts once a rule of it is retracted. Every
    /// other kind is the rule's own, fixed for its bytes under this
    /// release.
    pub fn depends_on_other_rules(&self) -> bool {
        matches!(self, RuleError::Cycle { .. })
    }

    /// The reason `dialog.rule/invalid` answers for a rule's own error,
    /// spelled as that relation always spelled it: a body that does not
    /// compile reads as undecodable there. `None` for a cycle, which that
    /// relation does not list.
    pub(crate) fn invalid_reason(&self) -> Option<String> {
        match self {
            RuleError::IllTyped { reason } => Some(format!("undecodable: {reason}")),
            RuleError::Cycle { .. } => None,
            own => Some(own.to_string()),
        }
    }
}

impl From<&Quarantine> for RuleError {
    fn from(quarantine: &Quarantine) -> Self {
        RuleError::Cycle {
            concept: quarantine.concept.clone(),
            cycle: quarantine.cycle.clone(),
        }
    }
}

impl Display for RuleError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}: ", self.code())?;
        match self {
            RuleError::Older { format } => write!(
                f,
                "the body is in format {format}, which `upgrade_rules` rewrites to {TERMS_FORMAT}"
            ),
            RuleError::Newer { format } => write!(
                f,
                "the body is in format {format}, and this release reads up to {FORMAT}"
            ),
            RuleError::Forged => {
                f.write_str("the body does not hash to the identity it is stored under")
            }
            RuleError::Undecodable { reason } | RuleError::IllTyped { reason } => {
                f.write_str(reason)
            }
            RuleError::Cycle { concept, cycle } => {
                write!(f, "concluding {concept} closes a cycle through")?;
                for concept in cycle {
                    write!(f, " {concept}")?;
                }
                f.write_str(" with an absence test, so the rule is set aside")
            }
        }
    }
}

impl Error for RuleError {}

/// A stored body read as the rule it is: deductive or inductive.
#[derive(Clone, Debug)]
pub(crate) enum Hydrated {
    /// A deductive rule.
    Deductive(DeductiveRule),
    /// An inductive rule.
    Inductive(InductiveRule),
}

/// What the body `bytes` stored under `entity` is on this release: the
/// rule, or why it is none, judged once whichever way it falls. A body
/// in a format older than the current reads when it decodes as it
/// stands under its identity, and is otherwise left for the upgrade.
pub(crate) fn judge(entity: &Entity, bytes: &[u8]) -> Result<Hydrated, RuleError> {
    // The stamp is read once and handed to both readers: reading it
    // skips the whole rule.
    let stamp = Stamp::of(bytes);
    let format = stamp.format();
    if let Some(format) = format.filter(|format| *format > FORMAT) {
        return Err(RuleError::Newer { format });
    }
    let deductive = match hydrate_deductive(stamp) {
        Ok(rule) if rule.stored_as(entity) => return Ok(Hydrated::Deductive(rule)),
        other => other,
    };
    let inductive = match InductiveRule::read_stamped(stamp) {
        Ok(rule) if rule.stored_as(entity) => return Ok(Hydrated::Inductive(rule)),
        other => other,
    };
    if let Some(format) = format.filter(|format| *format < TERMS_FORMAT) {
        return Err(RuleError::Older { format });
    }
    match (deductive, inductive) {
        (Ok(_), _) | (_, Ok(_)) => Err(RuleError::Forged),
        (Err(BodyError::IllTyped(reason)), _) | (_, Err(BodyError::IllTyped(reason))) => {
            Err(RuleError::IllTyped { reason })
        }
        (Err(BodyError::Undecodable(reason)), Err(_)) => Err(RuleError::Undecodable { reason }),
    }
}

/// Refuse a commit writing a rule body that is no rule here: every
/// `dialog.rule/source` the commit's writes `written` assert, bound for
/// the primary or into a layer, is judged under the entity it is written
/// under, and the first that is no rule is refused with its error. A
/// rule installed through the API compiles before it is written, so this
/// refuses only what is written as a plain fact. A body that arrives from
/// elsewhere is not written through a commit, and is not refused: it is
/// inert where it is read. Judged through the line's rule `cache`, so the
/// body a commit judged is the one its first read finds.
pub(crate) fn refuse_invalid_rules(written: &Staged, cache: &RuleCache) -> Result<(), CommitError> {
    // A commit that writes no rule body pays for a look and nothing
    // more: no selector, no scan, no list.
    let source = source();
    let primary = if written.holds_attribute(source) {
        written.scan(&Selector::new().the(source.clone()))
    } else {
        Vec::new()
    }
    .into_iter()
    .map(|fact| (fact.of, fact.is));
    let shares = written.shares().iter().flat_map(|(_, share)| {
        share.iter().filter_map(|(of, the, change)| match change {
            Change::Assert(is, _) if the == source => Some((of.clone(), is.clone())),
            _ => None,
        })
    });
    for (rule, body) in primary.chain(shares.collect::<Vec<_>>()) {
        refuse_body(rule, &body, cache)?;
    }
    Ok(())
}

/// [`refuse_invalid_rules`] over writes a commit derived rather than
/// staged: what the rules it fires write, round by round. An inductive
/// rule whose head writes `dialog.rule/source` would otherwise plant a
/// body that is no rule through a transaction that stages only innocent
/// facts. A round that writes no rule body is walked once and builds
/// nothing.
pub(crate) fn refuse_derived_invalid_rules(
    derived: &Changes,
    cache: &RuleCache,
) -> Result<(), CommitError> {
    let source = source();
    let primary = derived.iter().map(|(of, the, change)| (of, the, change));
    let layered = derived
        .iter_in()
        .map(|(_, of, the, change)| (of, the, change));
    for (of, the, change) in primary.chain(layered) {
        if let Change::Assert(is, _) = change
            && the == source
        {
            refuse_body(of.clone(), is, cache)?;
        }
    }
    Ok(())
}

/// The relation a rule body is stored under, built once.
fn source() -> &'static Relation {
    static SOURCE: LazyLock<Relation> = LazyLock::new(source_attr);
    &SOURCE
}

/// Refuse `body` written under `rule` unless it is a rule there.
fn refuse_body(rule: Entity, body: &Value, cache: &RuleCache) -> Result<(), CommitError> {
    let judged = match body {
        Value::Bytes(bytes) => cache.judged(&rule, bytes).err(),
        other => Some(RuleError::Undecodable {
            reason: format!("the body is {other:?}, not bytes"),
        }),
    };
    match judged {
        Some(reason) => Err(CommitError::InvalidRule { rule, reason }),
        None => Ok(()),
    }
}

/// The deductive rule `bytes` hold, in the `hydrate_rule` span: one per
/// body decoded as a deductive rule, which the cost matrix counts.
#[tracing::instrument(skip_all, name = "hydrate_rule")]
fn hydrate_deductive(stamp: Stamp<'_>) -> Result<DeductiveRule, BodyError> {
    DeductiveRule::read_stamped(stamp)
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::{Hydrated, RuleError, judge};
    use crate::RuleCache;
    use dialog_corpus::Entity;
    use dialog_query::DeductiveRule;
    use dialog_query::rule::DeductiveRuleDescriptor;

    /// `doc/tag(x) := label :- doc/label(x) = label`.
    fn rule() -> DeductiveRule {
        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": { "tag": { "the": "doc/tag", "as": "text:" } } },
            "when": [{
                "assert": { "with": { "label": { "the": "doc/label", "as": "text:" } } },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "label": { "?": { "name": "tag" } }
                }
            }]
        }))
        .expect("a descriptor");
        descriptor.compile().expect("the rule compiles")
    }

    /// A body is judged a rule under its identity and forged under any
    /// other; bytes that are no body are undecodable.
    #[dialog_common::test]
    fn it_judges_a_body_by_what_it_is() {
        let rule = rule();
        let bytes = rule.encode();
        assert!(matches!(
            judge(&rule.this(), &bytes),
            Ok(Hydrated::Deductive(_))
        ));
        let elsewhere: Entity = "rule:elsewhere".parse().expect("an entity");
        assert!(matches!(judge(&elsewhere, &bytes), Err(RuleError::Forged)));
        assert!(matches!(
            judge(&elsewhere, &[0xff, 0x00]),
            Err(RuleError::Undecodable { .. })
        ));
    }

    /// A rule cache remembers a body that is no rule as it remembers one
    /// that is: the second read is the first's verdict, from the cache.
    #[dialog_common::test]
    fn it_remembers_a_body_that_is_no_rule() {
        let cache = RuleCache::default();
        let garbage: Entity = "rule:garbage".parse().expect("an entity");
        let bytes = [0xff, 0x00];
        assert!(cache.failure(&garbage, &bytes).is_none());
        let first = cache.judged(&garbage, &bytes).err();
        assert!(matches!(first, Some(RuleError::Undecodable { .. })));
        assert_eq!(cache.failure(&garbage, &bytes), first);
        assert_eq!(cache.judged(&garbage, &bytes).err(), first);

        let rule = rule();
        let body = rule.encode();
        assert!(matches!(
            cache.judged(&rule.this(), &body),
            Ok(Hydrated::Deductive(_))
        ));
        assert!(cache.body(&rule.this(), &body).is_some());
        assert!(cache.failure(&rule.this(), &body).is_none());
    }

    /// The diagnostic begins with the kind, and only a cycle depends on
    /// which other rules a branch holds.
    #[dialog_common::test]
    fn it_names_the_kind_first() {
        let cycle = RuleError::Cycle {
            concept: "on:test/q".parse().expect("an entity"),
            cycle: vec!["on:test/p".parse().expect("an entity")],
        };
        assert!(cycle.to_string().starts_with("cycle: "));
        assert!(cycle.depends_on_other_rules());
        assert_eq!(cycle.invalid_reason(), None);
        let ill_typed = RuleError::IllTyped {
            reason: "refused".into(),
        };
        assert_eq!(ill_typed.to_string(), "ill-typed: refused");
        assert!(!ill_typed.depends_on_other_rules());
        assert_eq!(
            ill_typed.invalid_reason().as_deref(),
            Some("undecodable: refused")
        );
    }
}
