//! Exact counts of what a scenario did: the archive blocks it moved and
//! the engine steps it took, read from the tracing spans the engine
//! emits (`resolve_bundle`, `hydrate_rule`, `settle_cell`, `commit`,
//! ...). A span is counted when it is created, once per call.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use dialog_peer::helpers::Tally;
use tracing::Subscriber;
use tracing::span::{Attributes, Id};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

type Spans = Arc<Mutex<BTreeMap<&'static str, u64>>>;

/// Span counts by name, shared with the subscriber that fills them.
#[derive(Clone, Default)]
pub struct Counters {
    spans: Spans,
}

impl Counters {
    /// Install the counting subscriber as the process's default. One
    /// process runs one scenario, so the counts are that scenario's.
    pub fn install() -> Self {
        let counters = Self::default();
        let layer = CountingLayer {
            spans: counters.spans.clone(),
        };
        tracing_subscriber::registry()
            .with(layer)
            .try_init()
            .expect("the counting subscriber installs once per process");
        counters
    }

    /// Every count so far: `span.<name>` per span, and the archive
    /// blocks and bytes `tally` has seen.
    pub fn snapshot(&self, tally: &Tally) -> BTreeMap<String, u64> {
        let mut snapshot: BTreeMap<String, u64> = self
            .spans
            .lock()
            .expect("span counts")
            .iter()
            .map(|(name, count)| (format!("span.{name}"), *count))
            .collect();
        snapshot.insert("archive.get".into(), tally.reads() as u64);
        snapshot.insert("archive.get.bytes".into(), tally.read_bytes() as u64);
        snapshot.insert("archive.put".into(), tally.writes() as u64);
        snapshot.insert("archive.put.bytes".into(), tally.write_bytes() as u64);
        snapshot
    }
}

/// What happened between two snapshots: `after` less `before`, with a
/// span first seen in `after` counted whole.
pub fn since(
    before: &BTreeMap<String, u64>,
    after: &BTreeMap<String, u64>,
) -> BTreeMap<String, u64> {
    after
        .iter()
        .map(|(name, count)| {
            let earlier = before.get(name).copied().unwrap_or(0);
            (name.clone(), count.saturating_sub(earlier))
        })
        .collect()
}

struct CountingLayer {
    spans: Spans,
}

impl<S> Layer<S> for CountingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, _attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            *self
                .spans
                .lock()
                .expect("span counts")
                .entry(span.name())
                .or_insert(0) += 1;
        }
    }
}
