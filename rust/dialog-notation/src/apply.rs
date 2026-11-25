//! Applying a document to a branch: the one way its forms are run.
//!
//! [`apply`] takes what [`check`](crate::check) made of a document and
//! runs it on a branch as one transaction, its forms a sequence:
//!
//! - Each write is staged in the order written, and each write with a
//!   `when` reads the branch as the forms before it left it, through
//!   the transaction it is part of: what they asserted and retracted,
//!   and what the rules they installed derive. A write under
//!   `cardinality: :one` reads as the commit will leave its cell.
//! - Every match of a `when` is read before any of them is written, so
//!   a write does not see itself.
//! - A command a form issues, and what the inductive rules it fires
//!   write, land at commit, after every form has run: a later `when` in
//!   the same document reads the command itself, through the relations
//!   of the layer commands are on, but not what rules make of it. What
//!   a rule writes lands in the commit the forms' writes do, and under
//!   `cardinality: :one` succeeds the newest claim, one a form of the
//!   same document wrote too.
//! - The document commits once. A refusal or an error on the way writes
//!   nothing. When another writer moves the branch first, the document
//!   is run again over the head that won, its `when`s read again.
//! - Queries, selects and views are answered after the commit, so each
//!   reads what the whole document left, form by form: each form's
//!   answer is its own, and one that fails hides no other.
//!
//! It comes in layers. [`stage`] stages a document into a transaction
//! the caller holds, on a branch or through a stack ([`Stage`]), reads
//! each write's `when` through it, and reports what each form did
//! ([`DocumentReport`]); it never commits. [`answer`] answers the
//! document's queries, selects and views from a branch once the caller
//! has committed. [`apply`] is the two on a branch: it opens the
//! transaction, stages, commits once and answers.
//!
//! An embedder that drives [`Step`]s itself takes these on itself: each
//! [`Write`](crate::Write) has to be read through the transaction the
//! steps before it are staged in, and the answers read after the
//! commit.

use dialog_capability::Provider;
use dialog_effects::archive::Import;
use dialog_effects::authority::Attest;
use dialog_effects::blob::Import as BlobImport;
use dialog_effects::blob::Size as BlobSize;
use dialog_effects::memory::Publish;
use std::sync::{Mutex, PoisonError};

use dialog_corpus::{Entity, Statement};
use dialog_query::ConceptConclusion;
use dialog_query::concept::descriptor::ConceptStatement;
use dialog_query::error::EvaluationError;
use dialog_query::query::Output as _;
use dialog_repository::{
    Branch, CommitError, Revision, StackTransaction, Transaction, TransactionQuery, Transient,
};

use crate::live::{Live, LiveError, Reader};
use crate::view::tree::Rendered;
use crate::written::Segment;
use crate::{Checked, Diagnostic, Error, Step, Syntax};

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
#[derive(Debug)]
pub struct Applied {
    /// The revision the branch stands at after the document: the one it
    /// committed, or the one it stood at when the document wrote
    /// nothing.
    pub revision: Option<Revision>,
    /// What each form that asks answers, in the order written: each
    /// form's own, so one that fails hides no other.
    pub answers: Answers,
    /// What there is to say about the document that refuses nothing:
    /// its warnings.
    pub diagnostics: Vec<Diagnostic>,
    /// What each form of the document staged, as [`stage`] says it.
    pub report: DocumentReport,
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
}

mod sealed {
    /// What only the crate implements [`Stage`](super::Stage) for.
    pub trait Sealed {}
    impl Sealed for dialog_repository::Transaction<&dialog_repository::Branch> {}
    impl Sealed for dialog_repository::StackTransaction<&dialog_repository::Branch> {}
}

/// What a document is staged into: a transaction the caller builds, on a
/// branch or through a stack, that reads back what is staged in it.
/// Implemented for [`Transaction`] and [`StackTransaction`] on a branch,
/// and for nothing else: a stage that cannot read what is staged in it
/// could not read a write's `when`.
pub trait Stage: sealed::Sealed + Sized {
    /// `statement` asserted.
    fn assert<S: Statement>(self, statement: S) -> Self;
    /// `statement` retracted.
    fn retract<S: Statement>(self, statement: S) -> Self;
    /// `statement`, a command, issued: it holds for the commit.
    fn dispatch<S: Statement>(self, statement: S) -> Self;
    /// A read of the branch with what is staged in it.
    fn query(&self) -> TransactionQuery<'_>;
}

impl Stage for Transaction<&Branch> {
    fn assert<S: Statement>(self, statement: S) -> Self {
        Transaction::assert(self, statement)
    }
    fn retract<S: Statement>(self, statement: S) -> Self {
        Transaction::retract(self, statement)
    }
    fn dispatch<S: Statement>(self, statement: S) -> Self {
        Transaction::dispatch(self, statement)
    }
    fn query(&self) -> TransactionQuery<'_> {
        Transaction::<&Branch>::query(self)
    }
}

impl Stage for StackTransaction<&Branch> {
    fn assert<S: Statement>(self, statement: S) -> Self {
        StackTransaction::assert(self, statement)
    }
    fn retract<S: Statement>(self, statement: S) -> Self {
        StackTransaction::retract(self, statement)
    }
    fn dispatch<S: Statement>(self, statement: S) -> Self {
        StackTransaction::dispatch(self, statement)
    }
    fn query(&self) -> TransactionQuery<'_> {
        StackTransaction::<&Branch>::query(self)
    }
}

/// What [`stage`] staged of a document, form by form.
#[derive(Debug, Clone, Default)]
pub struct DocumentReport {
    /// A report for each form read, in the order written.
    pub forms: Vec<FormReport>,
}

/// What one form of a document did.
#[derive(Debug, Clone)]
pub struct FormReport {
    /// Which form, counted from 0 among the forms read.
    pub form: usize,
    /// Its refusal, when it was refused: it staged nothing. A form with
    /// none was kept. In a document checked whole (`Checked::whole`),
    /// a form refused with the document holds the document's first
    /// refusal.
    pub refused: Option<Diagnostic>,
    /// What it staged, in order.
    pub did: Vec<Did>,
}

/// One thing a form did.
#[derive(Debug, Clone)]
pub enum Did {
    /// Facts asserted.
    Asserted(ConceptStatement),
    /// Facts retracted.
    Retracted(ConceptStatement),
    /// A command issued.
    Issued(ConceptStatement),
    /// What was installed, and the entity it is stored under: `None`
    /// for a rule Dialog-DB gives no identity, which the commit refuses.
    Installed(Installation, Option<Entity>),
    /// A write's `when` read. Each match follows as the
    /// [`Asserted`](Did::Asserted) or [`Retracted`](Did::Retracted) it
    /// made, so nothing follows when nothing matched.
    Read,
    /// A query, a select or a view, answered after the commit
    /// ([`answer`]).
    Asked,
    /// A rule, a concept or a query written again: it is the same as the
    /// one the form named here came to, installed once, at that form.
    Repeats(usize),
}

/// What a form installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Installation {
    /// A rule that derives: a concept's, a query's, a derived attribute's.
    Deduction,
    /// A rule that writes when its premises come to hold.
    Induction,
    /// A concept marked as a command's: its facts hold for one round.
    Transient,
}

/// Stage `checked`, what [`check`](crate::check) or
/// [`check_forms`](crate::check_forms) made of a document, into
/// `transaction`, the caller's: its writes in order, each write with a
/// `when` read through `transaction` as the forms before it left it,
/// with the rule its `when` reads through laid over that read alone.
/// It never commits: the caller commits, or drops the transaction to
/// write nothing, and may then [`answer`] the document's questions.
///
/// It refuses no form itself: it stages the steps `checked` holds and
/// says each form's refusal beside it, so a document checked form by
/// form keeps the forms that were not refused. A document [`check`]
/// refused holds no step and stages nothing. An error reading a `when`
/// is returned, with nothing more staged.
///
/// [`check`]: crate::check
pub async fn stage<T: Stage, E: Reader>(
    mut transaction: T,
    checked: &Checked,
    env: &E,
) -> Result<(T, DocumentReport), ApplyError> {
    let mut forms: Vec<FormReport> = (0..read(checked))
        .map(|form| FormReport {
            form,
            refused: None,
            did: Vec::new(),
        })
        .collect();
    for refused in checked.refusals() {
        if let Some(Segment::Item(at)) = refused.path.first()
            && let Some(report) = forms.get_mut(*at)
            && report.refused.is_none()
        {
            report.refused = Some(refused.clone());
        }
    }
    // A document checked whole is refused whole: each form is refused
    // with it, by its own refusal or the first of the document.
    if checked.whole
        && let Some(first) = checked.refusals().next()
    {
        for report in forms.iter_mut().filter(|report| report.refused.is_none()) {
            report.refused = Some(first.clone());
        }
    }
    for (at, earlier) in &checked.repeats {
        if let Some(report) = forms.get_mut(*at) {
            report.did.push(Did::Repeats(*earlier));
        }
    }
    for (step, at) in checked.steps.iter().zip(&checked.step_forms) {
        let mut did = Vec::new();
        transaction = match step {
            Step::Write(write) => {
                let mut query = transaction.query();
                if let Some(rule) = &write.query.rule {
                    query = query.with_rule(rule.clone());
                }
                let rows: Vec<ConceptConclusion> = query
                    .select(write.query.select.clone())
                    .perform(env)
                    .try_vec()
                    .await?;
                did.push(Did::Read);
                for row in &rows {
                    let statement = write.statement(row)?;
                    transaction = if write.retract {
                        did.push(Did::Retracted(statement.clone()));
                        transaction.retract(statement)
                    } else {
                        did.push(Did::Asserted(statement.clone()));
                        transaction.assert(statement)
                    };
                }
                transaction
            }
            Step::Assert(statement) => {
                did.push(Did::Asserted(statement.clone()));
                transaction.assert(statement.clone())
            }
            Step::Retract(statement) => {
                did.push(Did::Retracted(statement.clone()));
                transaction.retract(statement.clone())
            }
            Step::Issue(statement) => {
                did.push(Did::Issued(statement.clone()));
                transaction.dispatch(statement.clone())
            }
            // A rule is kept on the branch, one that holds a struct too.
            Step::Deduce(rule) => {
                did.push(Did::Installed(Installation::Deduction, rule.try_this()));
                transaction.assert((**rule).clone())
            }
            Step::Induce(rule) => {
                did.push(Did::Installed(Installation::Induction, rule.try_this()));
                transaction.assert((**rule).clone())
            }
            Step::Transient(concept) => {
                did.push(Did::Installed(
                    Installation::Transient,
                    Some(concept.clone()),
                ));
                transaction.assert(Transient(concept.clone()))
            }
            Step::Query(_) | Step::Select(_) | Step::View(_) => {
                did.push(Did::Asked);
                transaction
            }
        };
        if let Some(report) = forms.get_mut(*at) {
            report.did.extend(did);
        }
    }
    Ok((transaction, DocumentReport { forms }))
}

/// How many forms `checked` read: every form a step, a refusal, a repeat
/// or the text's own notes name.
fn read(checked: &Checked) -> usize {
    let noted = checked
        .written
        .notes
        .keys()
        .filter_map(|path| match path.as_slice() {
            [Segment::Item(at)] => Some(*at),
            _ => None,
        });
    let refused =
        checked
            .diagnostics
            .iter()
            .filter_map(|diagnostic| match diagnostic.path.first() {
                Some(Segment::Item(at)) => Some(*at),
                _ => None,
            });
    let repeated = checked.repeats.iter().map(|(at, _)| *at);
    noted
        .chain(refused)
        .chain(repeated)
        .chain(checked.step_forms.iter().copied())
        .max()
        .map_or(0, |last| last + 1)
}

/// What a document's queries, selects and views answer, form by form.
#[derive(Debug, Default)]
pub struct Answers {
    /// Each form that asks, in the order written.
    pub forms: Vec<FormAnswer>,
}

/// What one form that asks answers.
#[derive(Debug)]
pub struct FormAnswer {
    /// Which form, counted from 0 among the forms read, as
    /// [`FormReport::form`] counts it.
    pub form: usize,
    /// Its answer, or why it has none: its own, whatever the other
    /// forms answer.
    pub answered: Result<Answer, LiveError>,
}

/// What a form that asks answers.
#[derive(Debug, Clone)]
pub enum Answer {
    /// A query's rows or a select's answers.
    Rows(Vec<Syntax>),
    /// A view, rendered once.
    Rendered(Vec<Rendered>),
}

/// The answers to the queries, selects and views of `checked`, form by
/// form, read from `branch` as it stands: after the caller committed
/// what [`stage`] staged, what the whole document left. A form that
/// cannot be answered says why in its own entry, and the forms after it
/// are answered still.
pub async fn answer<E: Reader>(branch: &Branch, checked: &Checked, env: &E) -> Answers {
    let live = Live::new(branch.clone());
    let mut answers = Answers::default();
    for (step, at) in checked.steps.iter().zip(&checked.step_forms) {
        let answered = match step {
            Step::Query(query) => rows(branch, query, env).await.map(Answer::Rows),
            Step::Select(selection) => live.select(selection, env).await.map(Answer::Rows),
            Step::View(plan) => live
                .rendered(plan, plan.given.as_ref(), env)
                .await
                .map(|rendered| Answer::Rendered(vec![rendered])),
            _ => continue,
        };
        // A form of several steps that ask is one entry: its answers
        // together, or its first error.
        match answers.forms.last_mut() {
            Some(last) if last.form == *at => {
                last.answered = match (
                    std::mem::replace(&mut last.answered, Ok(Answer::Rows(Vec::new()))),
                    answered,
                ) {
                    (Ok(Answer::Rows(mut before)), Ok(Answer::Rows(more))) => {
                        before.extend(more);
                        Ok(Answer::Rows(before))
                    }
                    (Ok(Answer::Rendered(mut before)), Ok(Answer::Rendered(more))) => {
                        before.extend(more);
                        Ok(Answer::Rendered(before))
                    }
                    (Err(error), _) | (Ok(_), Err(error)) => Err(error),
                    (Ok(before), Ok(_)) => Ok(before),
                };
            }
            _ => answers.forms.push(FormAnswer {
                form: *at,
                answered,
            }),
        }
    }
    answers
}

/// The rows `query` answers on `branch`, its own rule, when it has
/// several premises, laid over the branch for it alone.
async fn rows<E: Reader>(
    branch: &Branch,
    query: &crate::Query,
    env: &E,
) -> Result<Vec<Syntax>, LiveError> {
    let mut layer = branch.query();
    if let Some(rule) = &query.rule {
        layer = layer.with(rule.clone());
    }
    let found: Vec<ConceptConclusion> = layer
        .select(query.select.clone())
        .perform(env)
        .try_vec()
        .await?;
    Ok(found
        .iter()
        .map(|row| query.row(row))
        .collect::<Result<_, Error>>()?)
}

/// Apply `checked`, what [`check`](crate::check) made of a document, to
/// `branch`: [`stage`] it in a transaction of the branch, commit once,
/// built again over the head that won when another writer moves the
/// branch first, and [`answer`] it from what the commit left: once it
/// has committed it errs no more, and a form that cannot be answered
/// says so in [`Applied::answers`]. See the
/// [module docs](self).
///
/// A document with a refusal is [`ApplyError::Refused`], with the first
/// refusal, and nothing of it is applied: a document is applied whole.
pub async fn apply<E: Writer>(
    branch: &Branch,
    checked: &Checked,
    env: &E,
) -> Result<Applied, ApplyError> {
    if let Some(refused) = checked.refusals().next() {
        return Err(ApplyError::Refused(Box::new(refused.clone())));
    }
    let report: Mutex<DocumentReport> = Mutex::new(DocumentReport::default());
    branch
        .transact(|transaction| {
            let report = &report;
            async move {
                let (transaction, made) = stage(transaction, checked, env).await?;
                *report.lock().unwrap_or_else(PoisonError::into_inner) = made;
                Ok::<_, ApplyError>(transaction)
            }
        })
        .perform(env)
        .await?;
    branch.refresh(env).await.map_err(CommitError::from)?;
    let revision = branch.revision();
    let answers = answer(branch, checked, env).await;
    Ok(Applied {
        revision,
        answers,
        diagnostics: checked.diagnostics.clone(),
        report: report.into_inner().unwrap_or_else(PoisonError::into_inner),
    })
}
