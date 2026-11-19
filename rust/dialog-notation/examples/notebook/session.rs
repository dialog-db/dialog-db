//! A branch of a repository kept in memory, and the syntax submitted
//! to it so far. The example and the tests share it.
//!
//! A notebook is the one writer of its branch, so it publishes with the
//! call that fails when a commit loses a race, which is deprecated for
//! `Branch::transact`, the one that runs again.

#![allow(deprecated)]

use anyhow::Result;
use dialog_notation::live::{Live, LiveError, written};
use dialog_notation::{Analyzer, Diagnostic, Step};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_peer::{Peer, Session};
use dialog_query::ConceptConclusion;
use dialog_query::query::Output as _;
use dialog_repository::{Branch, Transient};
use dialog_storage::provider::storage::VolatileSpace;

/// A notebook over the storage space `$space`: a branch of a repository,
/// and an analyzer that knows every definition the branch keeps.
macro_rules! notebook {
    ($(#[$meta:meta])* $name:ident, $space:ty) => {
$(#[$meta])*
pub struct $name {
    /// How the last document submitted was analyzed from the branch.
    pub rounds: dialog_notation::live::Rounds,
    pub operator: Peer<$space, Session>,
    pub branch: Branch,
    pub repository: std::sync::Arc<dialog_repository::Repository<dialog_credentials::Credential>>,
    analyzer: Analyzer,
}

#[allow(dead_code)]
impl $name {
    /// A notebook on `branch`, its analyzer loaded from what the branch
    /// keeps: the definitions every earlier document wrote.
    pub async fn on(
        operator: Peer<$space, Session>,
        repository: std::sync::Arc<dialog_repository::Repository<dialog_credentials::Credential>>,
        branch: Branch,
    ) -> Result<Self> {
        // Whose the definitions are: the authority the branch is opened
        // under, the DID the peer writes as.
        let mine = Live::new(branch.clone())
            .authority(&operator)
            .await
            .map_err(unwrapped)?;
        // Nothing is loaded: what a document reaches is read from the
        // branch as it is analyzed.
        let analyzer = Analyzer::new()?.with_namespace(mine);
        Ok($name {
            rounds: Default::default(),
            operator,
            branch,
            repository,
            analyzer,
        })
    }

    /// Another notebook on the branch `name` of the same repository.
    pub async fn branch(&self, name: &str) -> Result<Self> {
        let branch = self
            .repository
            .branch(name)
            .open()
            .perform(&self.operator)
            .await?;
        Self::on(self.operator.clone(), self.repository.clone(), branch).await
    }

    /// Forget every definition submitted, as a notebook opened anew
    /// would: what a later document reaches is read from the branch.
    pub async fn reload(&mut self) -> Result<()> {
        self.branch.refresh(&self.operator).await?;
        let mine = self.analyzer.namespaces().mine().clone();
        self.analyzer = Analyzer::new()?.with_namespace(mine);
        Ok(())
    }

    /// The analyzer, with every definition submitted so far.
    pub fn analyzer(&self) -> &Analyzer {
        &self.analyzer
    }

    /// Submit a document: one transaction for what it writes, installs
    /// and issues, then its queries against the result. The report is
    /// every row, as written, sorted.
    pub async fn submit(&mut self, text: &str) -> Result<(Vec<String>, Vec<Diagnostic>)> {
        // The three phases: text to trees, trees to analyzed trees, an
        // analyzed tree to what Dialog-DB runs. A document with a
        // refusal in it is refused whole. The first one is the error,
        // said with where it is.
        let (checked, rounds) = Live::new(self.branch.clone())
            .analyze(&mut self.analyzer, text, &self.operator)
            .await
            .map_err(unwrapped)?;
        self.rounds = rounds;
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
        // What the document defines and names is written with what it
        // writes: each definition, its source map and its bindings, each
        // petname, each name taken away, and the rule `Name` is derived by.
        let kept = written(&checked)?;
        let diagnostics = checked.diagnostics;
        let mut transaction = self.branch.transaction();
        let mut writes = false;
        for statement in kept.asserted {
            transaction = transaction.assert(statement);
            writes = true;
        }
        for statement in kept.retracted {
            transaction = transaction.retract(statement);
            writes = true;
        }
        if let Some(rule) = kept.rule {
            transaction = transaction.assert(rule);
        }
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
        report.sort();
        report.dedup();
        Ok((report, diagnostics))
    }
}
    };
}

notebook!(
    /// A branch and the syntax submitted to it so far, in memory.
    Notebook,
    VolatileSpace
);

impl Notebook {
    pub async fn open() -> Result<Self> {
        let (operator, profile) = test_session_with_peer().await;
        let repository = test_repo(&operator, &profile).await;
        let branch = repository.branch("main").open().perform(&operator).await?;
        Notebook::on(operator, std::sync::Arc::new(repository), branch).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
notebook!(
    /// A notebook whose repository is kept on the filesystem, in a
    /// directory of its own.
    #[allow(dead_code)]
    FileNotebook,
    dialog_storage::NativeTempSpace
);

#[cfg(not(target_arch = "wasm32"))]
#[allow(dead_code)]
impl FileNotebook {
    pub async fn open() -> Result<Self> {
        use dialog_capability::Subject;
        use dialog_peer::helpers::{open_peer, test_owned, unique_name};
        use dialog_repository::RepositoryExt as _;
        let storage = test_owned(dialog_storage::provider::Storage::temp()).await;
        let peer = open_peer(
            storage,
            dialog_effects::storage::Location::profile(unique_name("notebook")),
        )
        .await?;
        let operator = peer
            .session(b"notebook")
            .space(peer.state())
            .allow(Subject::any())
            .await?;
        let repository = peer
            .space(unique_name("repo"))
            .open()
            .perform(&operator)
            .await?;
        let branch = repository.branch("main").open().perform(&operator).await?;
        FileNotebook::on(operator, std::sync::Arc::new(repository), branch).await
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
