//! A branch of a repository kept in memory, and the syntax submitted
//! to it so far. The example and the tests share it.

use anyhow::Result;
use dialog_notation::{Analyzer, Diagnostic, Error, Step, check};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_peer::{Peer, Session};
use dialog_query::query::Output as _;
use dialog_query::{ConceptConclusion, DeductiveRule};
use dialog_repository::{Branch, Transient};
use dialog_storage::provider::storage::VolatileSpace;

/// A branch and the syntax submitted to it so far.
pub struct Notebook {
    pub operator: Peer<VolatileSpace, Session>,
    pub branch: Branch,
    analyzer: Analyzer,
    /// The rules Dialog-DB cannot keep yet: a rule that holds a struct
    /// has no stored form. The notebook holds them itself and lays them
    /// over the branch for every query, so they last as long as it does
    /// and no longer.
    held: Vec<DeductiveRule>,
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
            held: Vec::new(),
        })
    }

    /// How many rules the notebook holds because Dialog-DB cannot keep
    /// them.
    pub fn held(&self) -> usize {
        self.held.len()
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
        {
            for step in checked.steps {
                transaction = match step {
                    Step::Assert(statement) => transaction.assert(statement),
                    Step::Retract(statement) => transaction.retract(statement),
                    Step::Issue(statement) => transaction.dispatch(statement),
                    // A rule Dialog-DB has no stored form for is held
                    // here in place of being kept on the branch.
                    Step::Deduce(rule) => {
                        if rule.storable().is_err() {
                            self.held.push(*rule);
                            continue;
                        }
                        transaction.assert(*rule)
                    }
                    // A rule that writes runs when a transaction
                    // commits, which a rule laid over a branch for a
                    // query never does. So one Dialog-DB cannot keep is
                    // refused, and an assertion has no error to return,
                    // so it is asked first.
                    Step::Induce(rule) => {
                        rule.storable()
                            .map_err(|refusal| Error::Unsupported(refusal.to_string()))?;
                        transaction.assert(*rule)
                    }
                    Step::Transient(concept) => transaction.assert(Transient(concept)),
                    Step::Query(query) => {
                        queries.push(query);
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
            // The rules held here, and the query's own when it has
            // several premises, are laid over the branch for the query
            // alone: nothing of them is written.
            let mut layer = self.branch.query();
            for rule in self.held.iter().chain(&query.rule) {
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
        report.sort();
        report.dedup();
        Ok((report, diagnostics))
    }
}
