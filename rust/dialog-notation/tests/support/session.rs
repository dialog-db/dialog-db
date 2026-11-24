//! A branch of a repository kept in memory, and the syntax submitted
//! to it so far, which the tests share.
//!
//! A notebook is the one writer of its branch, so it publishes with the
//! call that fails when a commit loses a race, which is deprecated for
//! `Branch::transact`, the one that runs again.

#![allow(deprecated)]

use anyhow::Result;
use dialog_notation::live::{Live, LiveError};
use dialog_notation::{Analyzer, Diagnostic, Step, check};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_peer::{Peer, Session};
use dialog_query::ConceptConclusion;
use dialog_query::query::Output as _;
use dialog_repository::{Branch, Transient};
use dialog_storage::provider::storage::VolatileSpace;

/// A branch and the syntax submitted to it so far.
pub struct Notebook {
    pub operator: Peer<VolatileSpace, Session>,
    pub branch: Branch,
    analyzer: Analyzer,
}

impl Notebook {
    pub async fn open() -> Result<Self> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        Ok(Notebook {
            operator,
            branch,
            analyzer: Analyzer::new()?,
        })
    }

    /// Submit a document: one transaction for what it writes, installs
    /// and issues, then its queries against the result. The report is
    /// every row, as written, sorted.
    pub async fn submit(&mut self, text: &str) -> Result<(Vec<String>, Vec<Diagnostic>)> {
        // The three phases: text to trees, trees to analyzed trees, an
        // analyzed tree to what Dialog-DB runs. A document with a
        // refusal in it is refused whole. The first one is the error,
        // said with where it is.
        let checked = check(&mut self.analyzer, text);
        if let Some((refused, error)) = checked
            .refusals()
            .find_map(|refused| Some((refused, refused.refusal.clone()?)))
        {
            let place = match refused.span {
                Some(span) => format!(
                    "line {}, column {}",
                    span.start.line + 1,
                    span.start.column + 1
                ),
                None => "somewhere in the document".into(),
            };
            return Err(anyhow::Error::new(error).context(place));
        }
        let diagnostics = checked.diagnostics;
        let mut transaction = self.branch.transaction();
        let mut writes = false;
        let mut queries = Vec::new();
        let mut selections = Vec::new();
        let mut views = Vec::new();
        {
            for step in checked.steps {
                // A write with a `when` runs once: each match of the
                // `when`, as the branch stood, makes it. Nothing of it
                // is installed.
                if let Step::Write(write) = &step {
                    let mut layer = self.branch.query();
                    if let Some(rule) = &write.query.rule {
                        layer = layer.with(rule.clone());
                    }
                    let rows: Vec<ConceptConclusion> = layer
                        .select(write.query.select.clone())
                        .perform(&self.operator)
                        .try_vec()
                        .await?;
                    for row in &rows {
                        let statement = write.statement(row)?;
                        transaction = if write.retract {
                            transaction.retract(statement)
                        } else {
                            transaction.assert(statement)
                        };
                        writes = true;
                    }
                    continue;
                }
                transaction = match step {
                    Step::Assert(statement) => transaction.assert(statement),
                    Step::Retract(statement) => transaction.retract(statement),
                    Step::Issue(statement) => transaction.dispatch(statement),
                    // A rule is kept on the branch, one that holds a
                    // struct too.
                    Step::Deduce(rule) => transaction.assert(*rule),
                    Step::Induce(rule) => transaction.assert(*rule),
                    Step::Transient(concept) => transaction.assert(Transient(concept)),
                    Step::Query(query) => {
                        queries.push(query);
                        continue;
                    }
                    Step::Select(selection) => {
                        selections.push(selection);
                        continue;
                    }
                    Step::Write(_) => continue,
                    // A query whose answer is markup is maintained where it
                    // is rendered; asked here, it is rendered once.
                    Step::View(plan) => {
                        views.push(plan);
                        continue;
                    }
                };
                writes = true;
            }
        }
        if writes {
            transaction
                .commit()
                .publish()
                .perform(&self.operator)
                .await?;
            self.branch.refresh(&self.operator).await?;
        }
        let mut report = Vec::new();
        for query in queries {
            // The query's own rule, when it has several premises, is
            // laid over the branch for the query alone: nothing of it is
            // written.
            let mut layer = self.branch.query();
            if let Some(rule) = &query.rule {
                layer = layer.with(rule.clone());
            }
            let rows: Vec<ConceptConclusion> = layer
                .select(query.select.clone())
                .perform(&self.operator)
                .try_vec()
                .await?;
            for row in &rows {
                report.push(query.row(row)?.to_string());
            }
        }
        // A query asked, and a query whose answer is markup rendered
        // once: the driver asks every query their rows come from, with
        // the rules each reads laid over the branch for it alone.
        let live = Live::new(self.branch.clone());
        for selection in selections {
            for answer in live
                .select(&selection, &self.operator)
                .await
                .map_err(unwrapped)?
            {
                report.push(answer.to_string());
            }
        }
        for plan in views {
            let tree = live
                .render(&plan, plan.given.as_ref(), &self.operator)
                .await
                .map_err(unwrapped)?;
            for root in &tree.roots {
                report.push(
                    dialog_notation::Rendered {
                        roots: vec![root.clone()],
                    }
                    .hiccup(),
                );
            }
        }
        // Sorted, so a report reads the same whatever order the rows came
        // in, and never deduplicated: an answer given twice is seen twice.
        report.sort();
        Ok((report, diagnostics))
    }
}

/// The notation's own error where the driver failed with one, so a
/// refusal reads as it does when nothing is driven.
fn unwrapped(error: LiveError) -> anyhow::Error {
    match error {
        LiveError::Notation(error) => anyhow::Error::new(error),
        other => anyhow::Error::new(other),
    }
}
