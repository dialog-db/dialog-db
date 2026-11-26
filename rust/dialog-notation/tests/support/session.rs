//! A branch of a repository kept in memory, and the syntax submitted
//! to it so far, which the tests share.
//!
//! A notebook is the one writer of its branch, so it publishes with the
//! call that fails when a commit loses a race, which is deprecated for
//! `Branch::transact`, the one that runs again.

#![allow(deprecated)]

use anyhow::Result;
use dialog_notation::live::LiveError;
use dialog_notation::{Analyzer, Answer, ApplyError, Diagnostic, apply, check};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_peer::{Peer, Session};
use dialog_repository::Branch;
use dialog_storage::provider::storage::VolatileSpace;

/// Panics where `message`, said of the document `text`, names what the
/// notation made and the author did not write: a variable the compiler
/// made, `?[2]` or `?v[1]`, or a name in the notation's own namespace,
/// `dialog.n`. What the document itself says is the author's.
pub fn spoken(text: &str, message: &str) {
    let leaks = leaks(message);
    let unwritten: Vec<&String> = leaks
        .iter()
        .filter(|leak| !text.contains(leak.as_str()))
        .collect();
    assert!(
        unwritten.is_empty(),
        "{message:?} names {unwritten:?}, which the author did not write"
    );
}

/// The names in `message` the notation makes: each variable with a
/// bracketed tag, and each name under `dialog.n`.
pub fn leaks(message: &str) -> Vec<String> {
    let chars: Vec<char> = message.chars().collect();
    let mut found = Vec::new();
    let named = |c: char| c.is_alphanumeric() || "-_./*+!<>=".contains(c);
    let mut at = 0;
    while at < chars.len() {
        if chars[at] == '?' {
            let mut end = at + 1;
            while end < chars.len() && named(chars[end]) {
                end += 1;
            }
            if end < chars.len() && chars[end] == '[' {
                let close = chars[end..]
                    .iter()
                    .position(|c| *c == ']')
                    .map(|at| end + at);
                let stop = close.map_or(chars.len(), |close| close + 1);
                found.push(chars[at..stop].iter().collect());
                at = stop;
                continue;
            }
            at = end;
            continue;
        }
        at += 1;
    }
    // An identity Dialog-DB gives a rule or a concept, `concept:8w8P…`,
    // and the record it prints of one, `{this: Entity,v: Any,}`.
    for marker in ["concept:", "rule:"] {
        let mut rest = message;
        while let Some(at) = rest.find(marker) {
            let tail = &rest[at + marker.len()..];
            let id: String = tail
                .chars()
                .take_while(char::is_ascii_alphanumeric)
                .collect();
            if id.len() >= 32 {
                found.push(format!("{marker}{id}"));
            }
            rest = &tail[id.len()..];
        }
    }
    if message.contains("{this: ") {
        found.push("{this: …}".into());
    }
    let mut rest = message;
    while let Some(at) = rest.find("dialog.n") {
        let tail = &rest[at..];
        let end = tail
            .find(|c: char| !(c.is_alphanumeric() || ".-_/".contains(c)))
            .unwrap_or(tail.len());
        let name = &tail[..end];
        // `dialog.node.*` is the engine's, not the notation's.
        if name == "dialog.n" || name.starts_with("dialog.n.") || name.starts_with("dialog.n/") {
            found.push(name.to_string());
        }
        rest = &tail[end.max(1)..];
    }
    found
}

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
    /// every row, as written, sorted. Whatever refuses it is in the
    /// author's terms.
    pub async fn submit(&mut self, text: &str) -> Result<(Vec<String>, Vec<Diagnostic>)> {
        let submitted = self.submitted(text).await;
        match &submitted {
            Ok((_, diagnostics)) => diagnostics
                .iter()
                .for_each(|diagnostic| spoken(text, &diagnostic.message)),
            Err(error) => spoken(text, &format!("{error:#}")),
        }
        submitted
    }

    /// [`submit`](Self::submit), with no check of what it is told: the
    /// document checked and applied by the crate's own driver,
    /// [`apply`], and the answers said as text.
    pub async fn submitted(&mut self, text: &str) -> Result<(Vec<String>, Vec<Diagnostic>)> {
        let checked = check(&mut self.analyzer, text);
        let applied = match apply(&self.branch, &checked, &self.operator).await {
            Ok(applied) => applied,
            Err(ApplyError::Refused(refused)) => {
                // The first refusal is the error, said with where it is.
                let place = match refused.span {
                    Some(span) => format!(
                        "line {}, column {}",
                        span.start.line + 1,
                        span.start.column + 1
                    ),
                    None => "somewhere in the document".into(),
                };
                let error = match refused.refusal {
                    Some(error) => anyhow::Error::new(error),
                    None => anyhow::anyhow!(refused.message),
                };
                return Err(error.context(place));
            }
            Err(error) => return Err(unwrapped(error)),
        };
        let mut report: Vec<String> = Vec::new();
        for form in applied.answers.forms {
            match form.answered.map_err(live)? {
                Answer::Rows(rows) => report.extend(rows.iter().map(ToString::to_string)),
                Answer::Rendered(trees) => {
                    for tree in &trees {
                        for root in &tree.roots {
                            report.push(
                                dialog_notation::Rendered {
                                    roots: vec![root.clone()],
                                }
                                .hiccup(),
                            );
                        }
                    }
                }
            }
        }
        // Sorted, so a report reads the same whatever order the rows came
        // in, and never deduplicated: an answer given twice is seen twice.
        report.sort();
        Ok((report, applied.diagnostics))
    }
}

/// The notation's own error where the driver failed with one, so a
/// refusal reads as it does when nothing is driven.
fn unwrapped(error: ApplyError) -> anyhow::Error {
    match error {
        ApplyError::Notation(error) => anyhow::Error::new(error),
        other => anyhow::Error::new(other),
    }
}

/// The notation's own error where a form was not answered with one.
fn live(error: LiveError) -> anyhow::Error {
    match error {
        LiveError::Notation(error) => anyhow::Error::new(error),
        other => anyhow::Error::new(other),
    }
}
