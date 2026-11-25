//! Applying a document to a branch: the one way its forms are run.
//!
//! [`apply`] takes what [`check`](crate::check) made of a document and
//! runs it on a branch as one transaction:
//!
//! - Each write is staged in the order written. A write with a `when`
//!   reads the branch as it stood before the document: it does not see
//!   what the forms before it wrote.
//! - A command a form issues, and what the inductive rules it fires
//!   write, land at commit, after every form has run.
//! - The document commits once. A refusal or an error on the way writes
//!   nothing. When another writer moves the branch first, the document
//!   is run again over the head that won, its `when`s read again.
//! - Queries, selects and views are answered after the commit, so each
//!   reads what the whole document left, in the order written.
//!
//! An embedder that drives [`Step`]s itself takes these on itself.

use dialog_capability::Provider;
use dialog_effects::archive::Import;
use dialog_effects::authority::Attest;
use dialog_effects::blob::Import as BlobImport;
use dialog_effects::blob::Size as BlobSize;
use dialog_effects::memory::Publish;
use dialog_query::ConceptConclusion;
use dialog_query::error::EvaluationError;
use dialog_query::query::Output as _;
use dialog_repository::{Branch, CommitError, Revision, Transaction, Transient};

use crate::live::{Live, LiveError, Reader};
use crate::view::tree::Rendered;
use crate::{Checked, Diagnostic, Error, Step, Syntax, Write};

/// What a document is applied through: what a [`Reader`] reads through,
/// and what a commit publishes through.
pub trait Writer:
    Reader
    + Provider<BlobSize>
    + Provider<BlobImport>
    + Provider<Import>
    + Provider<Publish>
    + Provider<Attest>
{
}

impl<T> Writer for T where
    T: Reader
        + Provider<BlobSize>
        + Provider<BlobImport>
        + Provider<Import>
        + Provider<Publish>
        + Provider<Attest>
{
}

/// What applying a document made.
#[derive(Debug, Clone)]
pub struct Applied {
    /// The revision the branch stands at after the document: the one it
    /// committed, or the one it stood at when the document wrote
    /// nothing.
    pub revision: Option<Revision>,
    /// Each query's rows and each select's answers, in the order the
    /// document asks them.
    pub answers: Vec<Syntax>,
    /// Each view the document asks for, rendered once, in the order
    /// asked.
    pub rendered: Vec<Rendered>,
    /// What there is to say about the document that refuses nothing:
    /// its warnings.
    pub diagnostics: Vec<Diagnostic>,
}

/// Why a document was not applied, or was applied and not answered.
#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    /// A form of the document is refused: nothing of it was applied. The
    /// diagnostic says which, and where.
    #[error("{}", .0.message)]
    Refused(Box<Diagnostic>),
    /// What a match of a write's `when` made is refused: nothing of the
    /// document was applied.
    #[error("{}", crate::syntax::told(&.0.to_string()))]
    Notation(#[from] Error),
    /// Dialog-DB failed to read a write's `when`: nothing of the
    /// document was applied.
    #[error(transparent)]
    Evaluation(#[from] EvaluationError),
    /// The commit failed: nothing of the document was applied.
    #[error(transparent)]
    Commit(#[from] CommitError),
    /// The document was applied, and a query, a select or a view of it
    /// could not be answered after the commit.
    #[error("the document was applied, and then {error}")]
    Unanswered {
        /// The revision the document left the branch at.
        revision: Option<Revision>,
        /// Why it was not answered.
        error: LiveError,
    },
}

/// Apply `checked`, what [`check`](crate::check) made of a document, to
/// `branch`: its writes as one transaction, staged in the order written,
/// and then its queries, selects and views, answered from what the
/// commit left. See the [module docs](self).
///
/// A document [`check`](crate::check) refused is
/// [`ApplyError::Refused`], with the first refusal, and nothing of it
/// is applied.
pub async fn apply<E: Writer>(
    branch: &Branch,
    checked: &Checked,
    env: &E,
) -> Result<Applied, ApplyError> {
    if let Some(refused) = checked.refusals().next() {
        return Err(ApplyError::Refused(Box::new(refused.clone())));
    }
    let steps = &checked.steps;
    branch
        .transact(|transaction| staged(transaction, branch, steps, env))
        .perform(env)
        .await?;
    branch.refresh(env).await.map_err(CommitError::from)?;
    let revision = branch.revision();
    let unanswered = |error: LiveError| ApplyError::Unanswered {
        revision: revision.clone(),
        error,
    };
    let live = Live::new(branch.clone());
    let mut answers = Vec::new();
    let mut rendered = Vec::new();
    for step in steps {
        match step {
            Step::Query(query) => {
                // A query's own rule, when it has several premises, is
                // laid over the branch for the query alone: nothing of
                // it is written.
                let mut layer = branch.query();
                if let Some(rule) = &query.rule {
                    layer = layer.with(rule.clone());
                }
                let rows: Vec<ConceptConclusion> = layer
                    .select(query.select.clone())
                    .perform(env)
                    .try_vec()
                    .await
                    .map_err(|error| unanswered(error.into()))?;
                for row in &rows {
                    answers.push(query.row(row).map_err(|error| unanswered(error.into()))?);
                }
            }
            Step::Select(selection) => {
                answers.extend(live.select(selection, env).await.map_err(unanswered)?);
            }
            Step::View(plan) => {
                rendered.push(
                    live.render(plan, plan.given.as_ref(), env)
                        .await
                        .map_err(unanswered)?,
                );
            }
            _ => {}
        }
    }
    Ok(Applied {
        revision,
        answers,
        rendered,
        diagnostics: checked.diagnostics.clone(),
    })
}

/// `transaction` with every write of `steps` staged, in order, each
/// write with a `when` reading `branch` as it stood before the document.
async fn staged<'a, E: Writer>(
    mut transaction: Transaction<&'a Branch>,
    branch: &Branch,
    steps: &[Step],
    env: &E,
) -> Result<Transaction<&'a Branch>, ApplyError> {
    for step in steps {
        transaction = match step {
            Step::Write(write) => {
                let rows = matches(branch, write, env).await?;
                for row in &rows {
                    let statement = write.statement(row)?;
                    transaction = if write.retract {
                        transaction.retract(statement)
                    } else {
                        transaction.assert(statement)
                    };
                }
                transaction
            }
            Step::Assert(statement) => transaction.assert(statement.clone()),
            Step::Retract(statement) => transaction.retract(statement.clone()),
            Step::Issue(statement) => transaction.dispatch(statement.clone()),
            // A rule is kept on the branch, one that holds a struct too.
            Step::Deduce(rule) => transaction.assert((**rule).clone()),
            Step::Induce(rule) => transaction.assert((**rule).clone()),
            Step::Transient(concept) => transaction.assert(Transient(concept.clone())),
            Step::Query(_) | Step::Select(_) | Step::View(_) => transaction,
        };
    }
    Ok(transaction)
}

/// Every match of `write`'s `when`, read from `branch` as it stood
/// before the document, with the rule its `when` reads through, when it
/// has more than one premise, laid over that read alone.
async fn matches<E: Writer>(
    branch: &Branch,
    write: &Write,
    env: &E,
) -> Result<Vec<ConceptConclusion>, ApplyError> {
    let mut layer = branch.query();
    if let Some(rule) = &write.query.rule {
        layer = layer.with(rule.clone());
    }
    Ok(layer
        .select(write.query.select.clone())
        .perform(env)
        .try_vec()
        .await?)
}
