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
      (is ?card {:title ?title :tags ?tags}))
    (query (task/pair ?t is: ?pair)
      (task ?t title: ?title)
      (task/tag ?t tag: ?g)
      (is ?pair {:title ?title :tag ?g}))
    (query (task/flat ?t is: ?row)
      (task ?t title: ?title)
      (task/tag ?t tag: ?g)
      (view ?row [:li ?title [:span {:class "tag"} ?g]]))"#;

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

/// A view whose layer joins two reads, and so has a rule of its own.
fn flat() -> Result<Plan> {
    match step("(select (task/flat ?t is: ?row))")? {
        Step::View(plan) => Ok(*plan),
        _ => Err(anyhow::anyhow!("no plan")),
    }
}

/// A selection whose query joins two reads, and so has a rule.
fn pairs() -> Result<Selection> {
    match step("(select (task/pair ?t is: ?pair))")? {
        Step::Select(selection) => Ok(*selection),
        _ => Err(anyhow::anyhow!("no selection")),
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

/// The rules a watch and a selection kept standing lay on the branch's
/// session overlay are taken off when the last that holds them is
/// dropped: two watches of one view share their rules, and the one left
/// is kept current after the other is dropped. A rule on the overlay the
/// driver did not lay stays.
#[dialog_common::test]
async fn it_takes_off_the_overlay_what_it_laid_when_the_last_holder_goes() -> Result<()> {
    let mut notebook = tasks().await?;
    let overlay = notebook.branch.overlay().clone();
    let before = overlay.len();
    let live = Live::new(notebook.branch.clone());
    let first = live.watch(flat()?, None)?;
    let laid = overlay.len();
    assert!(laid > before, "a watch lays its rules");
    let rules = (0..flat()?.layers.len())
        .filter_map(|layer| flat().ok()?.query(layer, None).ok()?.rule)
        .count();
    let mut second = live.watch(flat()?, None)?;
    assert_eq!(
        overlay.len(),
        laid + rules,
        "the rules two watches share are laid once, and each is held by both"
    );
    second.poll(&notebook.operator).await?;
    drop(first);
    assert_eq!(overlay.len(), laid, "the rules the second holds stay");
    notebook.submit(CHANGE).await?;
    second.poll(&notebook.operator).await?;
    let rendered = live.render(second.plan(), None, &notebook.operator).await?;
    assert_eq!(second.tree().hiccup(), rendered.hiccup());
    drop(second);
    assert_eq!(
        overlay.len(),
        before,
        "the last watch dropped takes its rules off"
    );

    let standing = live.standing(pairs()?)?;
    assert!(
        overlay.len() > before,
        "a standing selection lays its rules"
    );
    drop(standing);
    assert_eq!(overlay.len(), before, "and takes them off");

    // A rule of the view laid by someone else before the watch.
    let plan = flat()?;
    let theirs = (0..plan.layers.len())
        .filter(|layer| !plan.layers[*layer].conditions.is_empty())
        .find_map(|layer| plan.query(layer, None).ok()?.rule)
        .ok_or_else(|| anyhow::anyhow!("a layer with a rule"))?;
    overlay.assert(theirs)?;
    let with_theirs = overlay.len();
    drop(live.watch(plan, None)?);
    assert_eq!(
        overlay.len(),
        with_theirs,
        "a rule the driver did not lay stays"
    );
    Ok(())
}

/// Two `Live`s made apart on one branch, each watching one view, share
/// the rules it lays: the first dropped, the second is still kept
/// current; the second dropped, the overlay is as it was before either.
#[dialog_common::test]
async fn it_keeps_a_rule_two_lives_hold_until_both_let_go() -> Result<()> {
    let mut notebook = tasks().await?;
    let overlay = notebook.branch.overlay().clone();
    let before = overlay.len();
    let first = Live::new(notebook.branch.clone());
    let second = Live::new(notebook.branch.clone());
    let mut one = first.watch(flat()?, None)?;
    one.poll(&notebook.operator).await?;
    let evaluated = one.recomputes();
    let mut other = second.watch(flat()?, None)?;
    other.poll(&notebook.operator).await?;
    // The second only writes that it holds the rule, beside it: a
    // subscription already standing evaluates nothing again for it.
    assert!(one.poll(&notebook.operator).await?.is_empty());
    assert_eq!(one.recomputes(), evaluated, "a hold costs a poll nothing");
    drop(one);
    drop(first);
    notebook.submit(CHANGE).await?;
    other.poll(&notebook.operator).await?;
    let rendered = second
        .render(other.plan(), None, &notebook.operator)
        .await?;
    assert_eq!(
        other.tree().hiccup(),
        rendered.hiccup(),
        "the second still has the rule the first laid"
    );
    drop(other);
    assert_eq!(overlay.len(), before, "both gone, nothing is left laid");
    Ok(())
}

/// Two documents laid on one branch whose report relations are named
/// alike, after two sites, are refused where the second is laid: the two
/// would answer as one. Names are shortened until two sites meet.
#[dialog_common::test]
async fn it_refuses_a_report_two_sites_would_derive_alike() -> Result<()> {
    let notebook = tasks().await?;
    dialog_notation::name_sites_with(0);
    let pairs = pairs();
    let other = step(
        "(query (task/other ?t is: ?pair)
           (task/tag ?t tag: ?g)
           (task ?t title: ?title)
           (is ?pair {:tag ?g :title ?title :other true}))
         (select (task/other ?t is: ?pair))",
    );
    dialog_notation::name_sites_with(12);
    let (pairs, Step::Select(other)) = (pairs?, other?) else {
        return Err(anyhow::anyhow!("no selection"));
    };
    let live = Live::new(notebook.branch.clone());
    let kept = live.standing(pairs)?;
    let refused = live.standing(*other);
    assert!(
        matches!(
            refused,
            Err(dialog_notation::live::LiveError::Notation(
                dialog_notation::Error::Clash(..)
            ))
        ),
        "{:?}",
        refused.err()
    );
    drop(kept);
    Ok(())
}

/// A poll of a standing selection that fails where its answers are put
/// together, after its rows moved, gives nothing; the next poll gives
/// the answers as the branch stands, though no row moved since.
#[dialog_common::test]
async fn it_answers_after_a_poll_failed_putting_the_answers_together() -> Result<()> {
    let mut notebook = tasks().await?;
    let live = Live::new(notebook.branch.clone());
    let selection = selection()?;
    let mut standing = live.standing(selection.clone())?;
    let answers = |answers: Vec<dialog_notation::Syntax>| -> Vec<String> {
        let mut said: Vec<String> = answers.iter().map(ToString::to_string).collect();
        said.sort();
        said
    };
    standing.poll(&notebook.operator).await?;
    notebook.submit(CHANGE).await?;
    standing.fail_next_answer();
    assert!(standing.poll(&notebook.operator).await.is_err());
    let now = standing
        .poll(&notebook.operator)
        .await?
        .ok_or_else(|| anyhow::anyhow!("the move was lost"))?;
    assert_eq!(
        answers(now),
        answers(live.select(&selection, &notebook.operator).await?)
    );
    Ok(())
}
