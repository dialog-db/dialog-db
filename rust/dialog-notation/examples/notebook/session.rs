//! A branch of a repository kept in memory, and the syntax submitted
//! to it so far. The example and the tests share it.

use anyhow::Result;
use dialog_notation::{Diagnostic, Expander, Step, compile};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_peer::{Peer, Session};
use dialog_query::ConceptConclusion;
use dialog_query::query::Output as _;
use dialog_repository::{Branch, Transient};
use dialog_storage::provider::storage::VolatileSpace;

/// A branch and the syntax submitted to it so far.
pub struct Notebook {
    operator: Peer<VolatileSpace, Session>,
    branch: Branch,
    expander: Expander,
}

impl Notebook {
    pub async fn open() -> Result<Self> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        Ok(Notebook {
            operator,
            branch,
            expander: Expander::new()?,
        })
    }

    /// Submit a document: one transaction for what it writes, installs
    /// and issues, then its queries against the result. The report is
    /// every row, as written, sorted.
    pub async fn submit(&mut self, text: &str) -> Result<(Vec<String>, Vec<Diagnostic>)> {
        let (kernel, diagnostics) = self.expander.submit(text)?;
        let mut transaction = self.branch.transaction();
        let mut writes = false;
        let mut queries = Vec::new();
        for rule in &kernel {
            for step in compile(rule)? {
                writes |= !matches!(step, Step::Query(_));
                transaction = match step {
                    Step::Assert(statement) => transaction.assert(statement),
                    Step::Retract(statement) => transaction.retract(statement),
                    Step::Issue(statement) => transaction.dispatch(statement),
                    Step::Deduce(rule) => transaction.assert(*rule),
                    Step::Induce(rule) => transaction.assert(*rule),
                    Step::Transient(concept) => transaction.assert(Transient(concept)),
                    Step::Query(query) => {
                        queries.push(query);
                        transaction
                    }
                };
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
            let layer = match &query.rule {
                Some(rule) => self.branch.with(rule.clone()),
                None => self.branch.query(),
            };
            let rows: Vec<ConceptConclusion> = layer
                .select(query.select.clone())
                .perform(&self.operator)
                .try_vec()
                .await?;
            for row in &rows {
                report.push(query.row(row)?.to_string());
            }
        }
        report.sort();
        report.dedup();
        Ok((report, diagnostics))
    }
}
