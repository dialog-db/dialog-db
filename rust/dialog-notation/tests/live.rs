//! The driver, `dialog_notation::live`, when a poll fails part way: a
//! read of the branch fails at each place a poll reads in turn, and the
//! polls after it give what the branch holds, with nothing lost of what
//! the subscriptions reported before the failure.

#![allow(deprecated)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use dialog_capability::{Command, Fork, Provider, Resume};
use dialog_common::{Held, Holds};
use dialog_corpus::tree::CorpusNode;
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::blob::Load as BlobLoad;
use dialog_effects::blob::Read as BlobRead;
use dialog_effects::memory::Resolve;
use dialog_notation::live::Live;
use dialog_notation::{Analyzer, Plan, Selection, Step, compile, parse};
use dialog_repository::RemoteSite;
use dialog_search_tree::{DialogSearchTreeError, nodes};

#[path = "support/session.rs"]
mod session;
use session::Notebook;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// An env that is the notebook's, but for one read of a node, the one
/// it is armed for, which fails.
#[derive(Clone)]
struct Flaky<E> {
    env: E,
    /// How many node reads to let through before the one that fails;
    /// `usize::MAX` for none.
    left: Arc<AtomicUsize>,
    /// Whether the armed read has failed.
    failed: Arc<AtomicUsize>,
}

impl<E> Flaky<E> {
    fn new(env: E) -> Self {
        Flaky {
            env,
            left: Arc::new(AtomicUsize::new(usize::MAX)),
            failed: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Fail the read after `passing` more.
    fn arm(&self, passing: usize) {
        self.failed.store(0, Ordering::SeqCst);
        self.left.store(passing, Ordering::SeqCst);
    }

    fn disarm(&self) -> bool {
        self.left.store(usize::MAX, Ordering::SeqCst);
        self.failed.load(Ordering::SeqCst) > 0
    }
}

macro_rules! delegate {
    ($($command:ty),* $(,)?) => {
        $(
            impl<E: Provider<$command>> Provider<$command> for Flaky<E> {
                fn execute(&self, input: <$command as Command>::Input)
                    -> Resume<'_, <$command as Command>::Output> {
                    self.env.execute(input)
                }
            }
        )*
    };
}

delegate!(
    BlobRead,
    Get,
    Put,
    Resolve,
    Identify,
    ArchiveLoad,
    nodes::Write<CorpusNode>,
    nodes::Preload<CorpusNode>,
    BlobLoad,
    dialog_corpus::Preload,
    dialog_corpus::Speculation,
    Fork<RemoteSite, Resolve>,
);

impl<E: Provider<nodes::Read<CorpusNode>>> Provider<nodes::Read<CorpusNode>> for Flaky<E> {
    fn execute(
        &self,
        input: nodes::Read<CorpusNode>,
    ) -> Resume<'_, Result<Option<CorpusNode>, DialogSearchTreeError>> {
        let left = self.left.load(Ordering::SeqCst);
        if left == 0 {
            self.left.store(usize::MAX, Ordering::SeqCst);
            self.failed.fetch_add(1, Ordering::SeqCst);
            return Resume::now(Err(DialogSearchTreeError::Node(
                "a read made to fail".into(),
            )));
        }
        if left != usize::MAX {
            self.left.store(left - 1, Ordering::SeqCst);
        }
        self.env.execute(input)
    }
}

impl<E: Holds> Holds for Flaky<E> {
    fn held(&self, key: &str) -> Option<Held> {
        self.env.held(key)
    }

    fn hold(&self, key: String, handle: Held) {
        self.env.hold(key, handle)
    }
}

const TAGGED: &str = r#"
    (concept (task ?t title: ?title)
      (xyz.task/title of: ?t is: ?title as: :text cardinality: :one))
    (concept (task/tag ?t tag: ?g)
      (xyz.task/tag of: ?t is: ?g as: :text cardinality: :many))
    (query (task/chip ?t tag: ?g is: ?chip)
      (task/tag ?t tag: ?g)
      (view ?chip [:span {:class "tag"} ?g]))
    (query (task/row ?t is: ?row)
      (task ?t title: ?title)
      (task/chip ?t is: (collect ?chips))
      (view ?row [:li ?title ?chips]))
    (query (task/card ?t is: ?card)
      (task ?t title: ?title)
      (task/tag ?t tag: (collect ?tags))
      (is ?card {:title ?title :tags ?tags}))"#;

/// The one step `asked` compiles to.
fn step(asked: &str) -> Result<Step> {
    let text = format!("{TAGGED}\n{asked}");
    let analysis = Analyzer::new()?.analyze(&parse(&text)?);
    for tree in &analysis.analyzed {
        if let [step] = compile(tree)?.as_slice()
            && matches!(step, Step::View(_) | Step::Select(_))
        {
            return Ok(step.clone());
        }
    }
    Err(anyhow::anyhow!("nothing asked: {:?}", analysis.diagnostics))
}

fn plan() -> Result<Plan> {
    match step("(select (task/row ?t is: ?row))")? {
        Step::View(plan) => Ok(*plan),
        _ => Err(anyhow::anyhow!("no plan")),
    }
}

fn selection() -> Result<Selection> {
    match step("(select (task/card ?t is: ?card))")? {
        Step::Select(selection) => Ok(*selection),
        _ => Err(anyhow::anyhow!("no selection")),
    }
}

/// A notebook with the concepts and one task with a tag.
async fn tasks() -> Result<Notebook> {
    let mut notebook = Notebook::open().await?;
    let concepts = dialog_notation::read(TAGGED)?
        .iter()
        .filter(|form| form.head() == Some("concept"))
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    notebook.submit(&concepts).await?;
    notebook
        .submit(
            r#"(assert (task task:1 title: "Buy milk"))
               (assert (task/tag task:1 tag: "errand"))"#,
        )
        .await?;
    Ok(notebook)
}

/// What changes both layers of the row, and the card.
const CHANGE: &str = r#"(assert (task task:3 title: "Water plants"))
    (assert (task/tag task:3 tag: "home"))
    (assert (task/tag task:1 tag: "dairy"))"#;

/// How many places a poll may fail at that are tried.
const PLACES: usize = 200;

/// A view watched: whichever read of the poll after a change fails, the
/// polls after it leave the tree as the branch renders it. A read that
/// fails does not always fail the poll; one that does is counted, and
/// some must.
#[dialog_common::test]
async fn it_loses_nothing_of_a_view_when_a_poll_fails() -> Result<()> {
    let mut failures = 0;
    for passing in 0..PLACES {
        let mut notebook = tasks().await?;
        let env = Flaky::new(notebook.operator.clone());
        let live = Live::new(notebook.branch.clone());
        let mut watched = live.watch(plan()?, None)?;
        watched.poll(&env).await?;
        notebook.submit(CHANGE).await?;
        env.arm(passing);
        let failed = watched.poll(&env).await.is_err();
        if !env.disarm() {
            break;
        }
        failures += usize::from(failed);
        watched.poll(&env).await?;
        watched.poll(&env).await?;
        let rendered = live.render(watched.plan(), None, &env).await?;
        assert_eq!(
            watched.tree().hiccup(),
            rendered.hiccup(),
            "after a read failed with {passing} let through"
        );
    }
    assert!(failures > 0, "no poll was made to fail");
    Ok(())
}

/// A selection kept standing: whichever read of the poll after a change
/// fails, a poll after it gives the answers the branch gives.
#[dialog_common::test]
async fn it_loses_nothing_of_a_selection_when_a_poll_fails() -> Result<()> {
    let mut failures = 0;
    for passing in 0..PLACES {
        let mut notebook = tasks().await?;
        let env = Flaky::new(notebook.operator.clone());
        let live = Live::new(notebook.branch.clone());
        let selection = selection()?;
        let mut standing = live.standing(selection.clone())?;
        let mut answers = standing.poll(&env).await?;
        notebook.submit(CHANGE).await?;
        env.arm(passing);
        let polled = standing.poll(&env).await;
        if !env.disarm() {
            break;
        }
        match polled {
            Ok(Some(now)) => answers = Some(now),
            Ok(None) => {}
            Err(_) => failures += 1,
        }
        for _ in 0..2 {
            if let Some(now) = standing.poll(&env).await? {
                answers = Some(now);
            }
        }
        let mut given: Vec<String> = answers
            .unwrap_or_default()
            .iter()
            .map(ToString::to_string)
            .collect();
        let mut selected: Vec<String> = live
            .select(&selection, &env)
            .await?
            .iter()
            .map(ToString::to_string)
            .collect();
        given.sort();
        selected.sort();
        assert_eq!(
            given, selected,
            "after a read failed with {passing} let through"
        );
    }
    assert!(failures > 0, "no poll was made to fail");
    Ok(())
}
