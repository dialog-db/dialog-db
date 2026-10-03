//! Sharing one evaluation of a rule body among every head of the rule.
//!
//! A rule concluding several attributes is resolved as one rule per
//! attribute, each over the same body. A concept selecting several of
//! those attributes would evaluate that body once per attribute: the
//! first time in full, every later time as a probe per entity. The
//! [`Recall`] step instead evaluates the *source* rule's body once per
//! query and binding of `this`, remembers its rows on the query's
//! [`Memo`], and projects each head's attribute out of the remembered
//! rows. The memo is per query, so it never outlives the facts it
//! was computed from.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures_util::TryStreamExt;

use crate::Binding;
use crate::artifact::Value;
use crate::attribute::Relation;
use crate::concept::descriptor::ConceptDescriptor;
use crate::concept::query::ConceptQuery;
use crate::error::EvaluationError;
use crate::parameters::Parameters;
use crate::planner::Conjunction;
use crate::selection::{Match, Selection};
use crate::term::Term;
use crate::try_stream;
use crate::types::Any;

/// The rows a rule body yielded, kept for the rest of the query: for
/// every rule, the rows under a free `this`, and the rows per bound
/// `this`. A bound lookup is answered from the free rows when those
/// were computed, by indexing them once, so a head evaluated with the
/// entity bound after another head scanned the whole relation never
/// probes the body again.
#[derive(Debug, Default)]
pub struct Memo {
    rules: Mutex<HashMap<Vec<u8>, Remembered>>,
}

/// What one rule's body yielded so far in this query.
#[derive(Debug, Default)]
struct Remembered {
    /// Every row, when the body ran with `this` free.
    all: Option<Arc<Vec<Match>>>,
    /// Rows per `this`: computed by a bound run, or indexed from `all`.
    by_this: HashMap<Vec<u8>, Arc<Vec<Match>>>,
    /// Whether `by_this` holds every entity of `all`.
    indexed: bool,
}

impl Memo {
    /// The remembered rows for `rule` under `this`, if any: empty `this`
    /// asks for the rows of the free run.
    pub fn recall(&self, rule: &[u8], this: &[u8]) -> Option<Arc<Vec<Match>>> {
        let mut rules = self.rules.lock().expect("memo lock");
        let remembered = rules.get_mut(rule)?;
        if this.is_empty() {
            return remembered.all.clone();
        }
        if let Some(rows) = remembered.by_this.get(this) {
            return Some(rows.clone());
        }
        let all = remembered.all.clone()?;
        if !remembered.indexed {
            let mut groups: HashMap<Vec<u8>, Vec<Match>> = HashMap::new();
            for row in all.iter() {
                if let Ok(Binding::Present(value)) = row.lookup(&Term::<Any>::var("this"))
                    && let Ok(key) = encode(&value)
                {
                    groups.entry(key).or_default().push(row.clone());
                }
            }
            for (key, rows) in groups {
                remembered
                    .by_this
                    .entry(key)
                    .or_insert_with(|| Arc::new(rows));
            }
            remembered.indexed = true;
        }
        // Indexed, and still absent: the free run had no row for this
        // entity, which is an answer.
        Some(
            remembered
                .by_this
                .get(this)
                .cloned()
                .unwrap_or_else(|| Arc::new(Vec::new())),
        )
    }

    /// Remember `rows` as what `rule` yields under `this` (every row of
    /// the free run when `this` is empty).
    pub fn remember(&self, rule: Vec<u8>, this: Vec<u8>, rows: Arc<Vec<Match>>) {
        let mut rules = self.rules.lock().expect("memo lock");
        let remembered = rules.entry(rule).or_default();
        if this.is_empty() {
            remembered.all = Some(rows);
        } else {
            remembered.by_this.insert(this, rows);
        }
    }
}

/// An evaluation environment that may carry a per-query [`Memo`]. An
/// environment without one evaluates every head's body itself, which
/// is correct and merely repeats work.
pub trait BodyMemo {
    /// This query's memo, if the environment keeps one.
    fn memo(&self) -> Option<&Memo>;
}

/// A head of a rule, evaluated by recalling the source rule's body:
/// the step a concept's rule bundle plans for a rule re-headed onto
/// one of its attributes.
#[derive(Debug, Clone)]
pub struct Recall {
    /// The identity the memo keys on: the source rule's.
    pub rule: Vec<u8>,
    /// The source body, planned with `this` bound iff `bound`.
    pub body: Arc<Conjunction>,
    /// Whether the scope binds `this`, so each input row is evaluated
    /// (and remembered) for its own entity.
    pub bound: bool,
    /// The source body's operand for the head attribute's value.
    pub value: String,
    /// The source body's operand for the head attribute's key, for a
    /// keyed collection.
    pub key: Option<String>,
    /// The attribute concept this head derives.
    pub attribute: ConceptDescriptor,
}

impl PartialEq for Recall {
    /// Two recalls are the same step when they recall the same rule's
    /// body under the same binding for the same head; the planned body
    /// follows from those.
    fn eq(&self, other: &Self) -> bool {
        self.rule == other.rule
            && self.bound == other.bound
            && self.value == other.value
            && self.key == other.key
            && self.attribute == other.attribute
    }
}

impl Recall {
    /// The source body's rows for one input: recalled from the memo under
    /// the input's `this` when the step is bound (or the free run when it
    /// is not), else computed and remembered. `this` is the bound entity
    /// the rows were asked for, when the step is bound.
    pub async fn rows_for<'a, Env>(
        &self,
        this: Option<&Value>,
        env: &'a Env,
    ) -> Result<Arc<Vec<Match>>, EvaluationError>
    where
        Env: crate::Scope<'a>,
    {
        let key = match this {
            Some(value) => encode(value)?,
            None => Vec::new(),
        };
        if let Some(rows) = env.memo().and_then(|memo| memo.recall(&self.rule, &key)) {
            return Ok(rows);
        }
        let mut seed = Match::new();
        if let Some(value) = this {
            seed.bind(&Term::<Any>::var("this"), value.clone())?;
        }
        let rows: Vec<Match> = self
            .body
            .as_ref()
            .clone()
            .evaluate(seed.seed(), env)
            .try_collect()
            .await?;
        let rows = Arc::new(rows);
        if let Some(memo) = env.memo() {
            memo.remember(self.rule.clone(), key, rows.clone());
        }
        Ok(rows)
    }

    /// The premise this step stands for when a plan is read back: a
    /// read of the attribute concept it derives.
    pub fn premise(&self) -> ConceptQuery {
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::var("this"));
        terms.insert(
            ConceptDescriptor::VALUE.to_string(),
            Term::<Any>::var(ConceptDescriptor::VALUE),
        );
        if self.key.is_some() {
            let key = Relation::key_operand(ConceptDescriptor::VALUE);
            terms.insert(key.clone(), Term::<Any>::var(&key));
        }
        ConceptQuery {
            terms,
            predicate: self.attribute.clone(),
        }
    }

    /// Evaluate: per input row, recall or compute the source body's rows
    /// for its `this`, and extend the row with the head's value (and
    /// key) from each, keeping the facts each row cites.
    pub fn evaluate<'a, Env, M: Selection + 'a>(
        self,
        selection: M,
        env: &'a Env,
    ) -> impl Selection + 'a
    where
        Env: crate::Scope<'a>,
    {
        let this_term = Term::<Any>::var("this");
        let value_term = Term::<Any>::var(ConceptDescriptor::VALUE);
        let key_term = Term::<Any>::var(Relation::key_operand(ConceptDescriptor::VALUE));
        try_stream! {
            for await input in selection {
                let input = input?;
                let this = if self.bound {
                    match input.lookup(&this_term) {
                        Ok(Binding::Present(value)) => Some(value),
                        // A row that pins `this` absent, or never bound
                        // it where the plan expected it, matches nothing.
                        _ => continue,
                    }
                } else {
                    None
                };
                let rows = self.rows_for(this.as_ref(), env).await?;
                for row in rows.iter() {
                    let mut extension = input.clone();
                    if !self.bound {
                        match row.lookup(&this_term) {
                            Ok(Binding::Present(value)) => {
                                if extension.bind(&this_term, value).is_err() {
                                    continue;
                                }
                            }
                            _ => continue,
                        }
                    }
                    match row.lookup(&Term::<Any>::var(&self.value)) {
                        Ok(Binding::Present(value)) => {
                            if extension.bind(&value_term, value).is_err() {
                                continue;
                            }
                        }
                        _ => continue,
                    }
                    if let Some(key) = &self.key {
                        match row.lookup(&Term::<Any>::var(key)) {
                            Ok(Binding::Present(value)) => {
                                if extension.bind(&key_term, value).is_err() {
                                    continue;
                                }
                            }
                            _ => continue,
                        }
                    }
                    extension.adopt_citations(row);
                    yield extension;
                }
            }
        }
    }
}

fn encode(value: &Value) -> Result<Vec<u8>, EvaluationError> {
    serde_ipld_dagcbor::to_vec(value).map_err(|error| EvaluationError::Store(error.to_string()))
}
