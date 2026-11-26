//! What a query left out, as it runs.
//!
//! A rule a branch holds that did not compile here works as if it were
//! not there: a query whose derivation reaches it answers without it.
//! The query's [`Report`] says so. It is filled as the query runs and
//! can be read at any point: a rule is left out when the rules of a
//! concept are assembled, before the first row that concept gives, so a
//! report read when a row arrives already lists the rules left out of
//! what produced it. Nothing has to be drained to read it, and what was
//! reported stays reported when the query's stream is dropped early.
//!
//! [`Report::entries`] is what was reported so far;
//! [`Report::stream`] delivers each entry as it is reported, from the
//! first, and ends when the query's stream ends or is dropped. The
//! query's own stream is unchanged: it yields its rows, and the report
//! rides beside it on the [`Answered`] that wraps it.
//!
//! Only rules a query's derivation reaches are reported: a rule a
//! branch holds that no concept of the query reads is not.

use std::mem::take;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

use dialog_corpus::Entity;
use dialog_query::EvaluationError;
use futures_util::{Stream, TryStreamExt as _};
use pin_project_lite::pin_project;

use crate::RuleError;

/// One thing a query left out.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry {
    /// A rule the query's derivation reaches that did not compile here,
    /// left out of the query.
    RuleSkipped {
        /// The rule's entity.
        rule: Entity,
        /// The relation it would have concluded, as the `on:` entity the
        /// rule is indexed under.
        concludes: Entity,
        /// Why it did not compile.
        reason: RuleError,
    },
}

/// What a query left out, so far: shared by the query and every reader,
/// readable at any point while the query runs and after.
#[derive(Clone, Debug, Default)]
pub struct Report {
    shared: Arc<Shared>,
}

/// What a report's handles share. The query's end is a flag, and the
/// lock is taken at the end only when a reader is waiting, so a query
/// whose report nobody streams pays for neither.
#[derive(Debug, Default)]
struct Shared {
    inner: Mutex<Inner>,
    finished: AtomicBool,
    waited: AtomicBool,
}

#[derive(Debug, Default)]
struct Inner {
    entries: Vec<Entry>,
    waiting: Vec<Waker>,
}

impl Report {
    fn inner(&self) -> MutexGuard<'_, Inner> {
        self.shared
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Every entry reported so far, in the order reported.
    pub fn entries(&self) -> Vec<Entry> {
        self.inner().entries.clone()
    }

    /// Whether nothing was reported so far.
    pub fn is_empty(&self) -> bool {
        self.inner().entries.is_empty()
    }

    /// Each entry as it is reported: every entry reported so far, then
    /// each one reported after, ending once the query's stream ends or
    /// is dropped.
    pub fn stream(&self) -> ReportStream {
        ReportStream {
            report: self.clone(),
            next: 0,
        }
    }

    /// Report `entry`, unless it was reported already: a rule a query
    /// reaches by several of its concepts is reported once.
    pub(crate) fn record(&self, entry: Entry) {
        let waiting = {
            let mut inner = self.inner();
            if inner.entries.contains(&entry) {
                return;
            }
            inner.entries.push(entry);
            take(&mut inner.waiting)
        };
        waiting.into_iter().for_each(Waker::wake);
    }

    /// Mark the query done: a report stream ends after the entries
    /// reported so far.
    fn finish(&self) {
        self.shared.finished.store(true, Ordering::Release);
        if !self.shared.waited.load(Ordering::Acquire) {
            return;
        }
        let waiting = take(&mut self.inner().waiting);
        waiting.into_iter().for_each(Waker::wake);
    }
}

/// A [`Report`]'s entries as they are reported. See [`Report::stream`].
#[derive(Debug)]
pub struct ReportStream {
    report: Report,
    next: usize,
}

impl Stream for ReportStream {
    type Item = Entry;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Entry>> {
        let next = self.next;
        let mut inner = self.report.inner();
        if let Some(entry) = inner.entries.get(next).cloned() {
            drop(inner);
            self.next += 1;
            return Poll::Ready(Some(entry));
        }
        if self.report.shared.finished.load(Ordering::Acquire) {
            return Poll::Ready(None);
        }
        self.report.shared.waited.store(true, Ordering::Release);
        // A reader polled again before anything is reported waits once:
        // the list holds a waker per reader task, not one per poll.
        let waker = context.waker();
        if !inner.waiting.iter().any(|known| known.will_wake(waker)) {
            inner.waiting.push(waker.clone());
        }
        drop(inner);
        // The query may have ended between the check and the wait, after
        // reporting an entry: poll again, and read what is there.
        if self.report.shared.finished.load(Ordering::Acquire) {
            context.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

/// Marks a report done when the query it rides with ends or is dropped.
#[derive(Debug)]
struct Finish(Report);

impl Drop for Finish {
    fn drop(&mut self) {
        self.0.finish();
    }
}

pin_project! {
    /// A query's rows, with the [`Report`] of what it left out. A stream
    /// of the rows as the query's own was, so every caller that reads
    /// rows reads them as before.
    pub struct Answered<S> {
        #[pin]
        rows: S,
        finish: Finish,
    }
}

impl<S> Answered<S> {
    /// `rows`, reporting into `report`. Inlined, so the rows are built
    /// in place: a query's stream is large, and a copy of it per query
    /// shows in a point read.
    #[inline(always)]
    pub(crate) fn new(rows: S, report: Report) -> Self {
        Self {
            rows,
            finish: Finish(report),
        }
    }

    /// What the query left out, so far: readable at any point, before
    /// the first row, between rows, and after the last.
    ///
    /// The query runs as its rows are polled, so a report read before
    /// the rows are first polled is empty: nothing has been looked at
    /// yet. Read it once a row arrives, once the rows end, or through
    /// [`Report::stream`], which waits for what is reported.
    pub fn report(&self) -> Report {
        self.finish.0.clone()
    }
}

impl<S, T> Answered<S>
where
    S: Stream<Item = Result<T, EvaluationError>>,
{
    /// Every row, and the report: a convenience that drains the rows.
    /// The report is readable without it, at any point.
    pub async fn try_report(self) -> Result<(Vec<T>, Report), EvaluationError> {
        let report = self.report();
        let rows = self.try_collect().await?;
        Ok((rows, report))
    }
}

impl<S: Stream> Stream for Answered<S> {
    type Item = S::Item;

    #[inline(always)]
    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<S::Item>> {
        let this = self.project();
        let polled = this.rows.poll_next(context);
        if let Poll::Ready(None) = polled {
            this.finish.0.finish();
        }
        polled
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use std::pin::Pin;
    use std::task::Context;

    use futures_util::Stream as _;
    use futures_util::task::noop_waker;

    use super::Report;

    /// A reader polled over and over while nothing is reported keeps one
    /// waker, so its wait does not grow with the number of polls.
    #[dialog_common::test]
    fn it_keeps_one_waker_per_reader_however_often_it_polls() {
        let report = Report::default();
        let mut stream = report.stream();
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        for _ in 0..100 {
            assert!(Pin::new(&mut stream).poll_next(&mut context).is_pending());
        }
        assert_eq!(report.inner().waiting.len(), 1);
    }
}
