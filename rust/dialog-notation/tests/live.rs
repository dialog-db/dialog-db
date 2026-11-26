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
      (task/chip ?t is: (distinct ?chips))
      (view ?row [:li ?title ?chips]))
    (query (task/card ?t is: ?card)
      (task ?t title: ?title)
      (task/tag ?t tag: (distinct ?tags))
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

/// Laying a rule is all or nothing: a write that fails part way, after
/// the rule and before its marks or its holder, leaves nothing laid on
/// the overlay, and laying again afterwards works.
#[dialog_common::test]
async fn it_leaves_nothing_laid_when_laying_fails_part_way() -> Result<()> {
    let notebook = tasks().await?;
    let overlay = notebook.branch.overlay().clone();
    let before = overlay.len();
    let live = Live::new(notebook.branch.clone());
    for writes in 1..=3 {
        dialog_notation::fail_laying_after(Some(writes));
        let failed = live.standing(pairs()?);
        dialog_notation::fail_laying_after(None);
        assert!(
            matches!(failed, Err(dialog_notation::live::LiveError::Rule(_))),
            "after {writes} writes: {:?}",
            failed.err()
        );
        assert_eq!(
            overlay.len(),
            before,
            "after {writes} writes, nothing is left laid"
        );
    }
    let kept = live.standing(pairs()?)?;
    assert!(overlay.len() > before);
    drop(kept);
    assert_eq!(overlay.len(), before);
    // A selection that lays more than one rule, failing on a later one
    // after the first is laid whole: the first comes off with the lease
    // the failure drops.
    let both = || match step(
        r#"(query (task/both ?t is: ?both)
           (task ?t title: ?title)
           (task/tag ?t tag: "errand")
           (task/pair ?t is: ?pair)
           (is ?both {:title ?title :pair ?pair}))
         (select (task/both ?t is: ?both))"#,
    )? {
        Step::Select(selection) => Ok(*selection),
        _ => Err(anyhow::anyhow!("no selection")),
    };
    let rules: std::collections::BTreeSet<_> = both()?
        .queries
        .iter()
        .filter_map(|query| query.rule.as_ref().map(|rule| rule.this()))
        .collect();
    assert!(rules.len() > 1, "{rules:?}");
    // Laying fails at its first write, then its second, and so on, until
    // a lay makes all its writes: each that failed left nothing laid,
    // those that failed on the second rule after the first was laid
    // whole among them.
    let mut failed_at = 0;
    for writes in 0.. {
        dialog_notation::fail_laying_after(Some(writes));
        let laid = live.standing(both()?);
        dialog_notation::fail_laying_after(None);
        match laid {
            Ok(kept) => {
                drop(kept);
                break;
            }
            Err(dialog_notation::live::LiveError::Rule(_)) => failed_at = writes,
            Err(other) => return Err(other.into()),
        }
        assert_eq!(
            overlay.len(),
            before,
            "after {writes} writes, nothing is left laid"
        );
    }
    // One rule takes four writes: the rule, that it is laid, what it
    // derives and its holder.
    assert!(
        failed_at >= 4,
        "failed only within the first rule: {failed_at}"
    );
    assert_eq!(overlay.len(), before);
    Ok(())
}

/// What the notation lays it takes off with its last holder, even where
/// an embedder asserted the same rule after it was laid: the rule is one
/// fact on the overlay, and it is the notation's. An embedder that
/// asserts a rule before the notation lays it keeps it, as the notation
/// only holds a rule it finds there. This pins what `Live` documents.
#[dialog_common::test]
async fn it_takes_off_a_rule_an_embedder_asserted_after_it_was_laid() -> Result<()> {
    let notebook = tasks().await?;
    let overlay = notebook.branch.overlay().clone();
    let before = overlay.len();
    let live = Live::new(notebook.branch.clone());
    let rule = pairs()?
        .queries
        .iter()
        .find_map(|query| query.rule.clone())
        .ok_or_else(|| anyhow::anyhow!("a query with a rule"))?;
    // Asserted after: taken off with the last holder.
    let kept = live.standing(pairs()?)?;
    overlay.assert(rule.clone())?;
    drop(kept);
    assert_eq!(overlay.len(), before, "the rule went with the last holder");
    // Asserted before: held, never laid, so kept.
    overlay.assert(rule.clone())?;
    let asserted = overlay.len();
    let kept = live.standing(pairs()?)?;
    drop(kept);
    assert_eq!(
        overlay.len(),
        asserted,
        "the embedder's rule is still there"
    );
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
    // What it says names no relation the notation made.
    let said = refused
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(session::leaks(&said).is_empty(), "{said}");
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
    let failed = standing.poll(&notebook.operator).await;
    assert!(
        matches!(
            failed,
            Err(dialog_notation::live::LiveError::Notation(
                dialog_notation::Error::Refused(_)
            ))
        ),
        "{:?}",
        failed.err()
    );
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

/// `Plan::row` of a layer the plan has no such index for is a typed
/// error, not a panic, and a match is read as a row of every column.
#[dialog_common::test]
async fn it_refuses_a_row_of_a_layer_the_plan_has_not() -> Result<()> {
    use dialog_query::query::Output as _;
    let notebook = tasks().await?;
    let plan = flat()?;
    let layer = (0..plan.layers.len())
        .find(|layer| !plan.layers[*layer].conditions.is_empty())
        .ok_or_else(|| anyhow::anyhow!("a layer with conditions"))?;
    let query = plan.query(layer, None)?;
    let mut asked = notebook.branch.query();
    if let Some(rule) = &query.rule {
        asked = asked.with(rule.clone());
    }
    let found = asked
        .select(query.select.clone())
        .perform(&notebook.operator)
        .try_vec()
        .await?;
    let conclusion = found.first().ok_or_else(|| anyhow::anyhow!("a match"))?;
    let row = plan.row(layer, &query, conclusion, None)?;
    assert_eq!(row.len(), plan.layers[layer].row.len(), "{row:?}");
    let beyond = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        plan.row(plan.layers.len() + 3, &query, conclusion, None)
    }));
    assert!(
        matches!(beyond, Ok(Err(dialog_notation::Error::Refused(_)))),
        "{:?}",
        beyond.map(|row| row.err())
    );
    Ok(())
}

/// What Dialog-DB says when it fails to evaluate is its own text, which
/// may name a derived attribute by its hash; a standing selection and a
/// watched view say it in the author's terms, from the names of what
/// they hold.
#[dialog_common::test]
async fn it_tells_an_evaluation_error_in_the_authors_terms() -> Result<()> {
    let notebook = Notebook::open().await?;
    let checked = dialog_notation::check(
        &mut dialog_notation::Analyzer::new()?,
        "(concept (list ?l name: ?n)
           (xyz.list/name of: ?l is: ?n as: :text cardinality: :one))
         (rule (list/label ?l ?n) (list ?l name: ?n))
         (query (list/card ?l is: ?card) (list/label ?l ?n) (is ?card {:label ?n}))
         (select (list/card ?l is: ?card))",
    );
    let selection = checked
        .steps()
        .iter()
        .find_map(|step| match step {
            Step::Select(selection) => Some((**selection).clone()),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("a selection: {:?}", checked.diagnostics()))?;
    let hash = format!("{:?}", checked.steps())
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '.'))
        .find(|part| part.starts_with("dialog.n.c."))
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("no derived relation"))?;
    let live = Live::new(notebook.branch.clone());
    let standing = live.standing(selection)?;
    let error = dialog_notation::live::LiveError::Evaluation(
        dialog_query::EvaluationError::UnboundVariable {
            variable_name: format!("{hash}/is"),
        },
    );
    // Display is Dialog-DB's text, the hash in it.
    assert!(error.to_string().contains(&hash), "{error}");
    let told = standing.told(&error);
    assert!(told.contains("list/label"), "{told}");
    assert!(!told.contains("dialog.n.c."), "{told}");
    Ok(())
}

/// A watched view tells an evaluation error in the author's terms too,
/// by the derived attributes its layers read: a view over one.
#[dialog_common::test]
async fn it_tells_an_evaluation_error_of_a_view_in_the_authors_terms() -> Result<()> {
    let notebook = Notebook::open().await?;
    let text = "(concept (list ?l name: ?n)
           (xyz.list/name of: ?l is: ?n as: :text cardinality: :one))
         (rule (list/label ?l ?n) (list ?l name: ?n))
         (query (list/chip ?l is: ?chip) (list/label ?l ?n) (view ?chip [:span ?n]))
         (select (list/chip ?l is: ?chip))";
    let analysis = Analyzer::new()?.analyze(&parse(text)?);
    let plan = analysis
        .analyzed
        .iter()
        .find_map(|tree| match compile(tree).ok()?.as_slice() {
            [Step::View(plan)] => Some((**plan).clone()),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("a view: {:?}", analysis.diagnostics))?;
    let hash = format!("{plan:?}")
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '.' || c == '/'))
        .find_map(|part| {
            part.strip_prefix("dialog.n.c/")
                .and_then(|written| written.rsplit_once('/'))
                .map(|(concept, field)| (concept.to_string(), field.to_string()))
        })
        .ok_or_else(|| anyhow::anyhow!("no derived attribute in {plan:?}"))?;
    let live = Live::new(notebook.branch.clone());
    let watched = live.watch(plan, None)?;
    let error = dialog_notation::live::LiveError::Evaluation(
        dialog_query::EvaluationError::UnboundVariable {
            variable_name: format!("{}/is", derived_hash(&hash.0, &hash.1)?),
        },
    );
    let told = watched.told(&error);
    assert!(told.contains("list/label"), "{told}");
    assert!(!told.contains("dialog.n.c."), "{told}");
    Ok(())
}

/// Dialog-DB's name for the derived attribute `field` of `concept`, as
/// a check of a document that derives it shows it in its steps.
fn derived_hash(concept: &str, field: &str) -> Result<String> {
    let checked = dialog_notation::check(
        &mut Analyzer::new()?,
        &format!(
            "(concept ({concept} ?l name: ?n)
               (xyz.probe/name of: ?l is: ?n as: :text cardinality: :one))
             (rule ({concept}/{field} ?l ?n) ({concept} ?l name: ?n))"
        ),
    );
    format!("{:?}", checked.steps())
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '.'))
        .find(|part| part.starts_with("dialog.n.c."))
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("no derived relation: {:?}", checked.diagnostics()))
}

/// A selection kept standing follows writes through a concept: two
/// claims of a `cardinality: :one` field answer the newest, and a retract
/// of the entity alone, which retracts every claim, leaves it answering
/// nothing of that entity, as a select asked once does.
#[dialog_common::test]
async fn it_keeps_a_selection_standing_through_a_retract_of_every_claim() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let checked = dialog_notation::check(
        &mut Analyzer::new()?,
        "(concept (p ?t n: ?n) (xyz.p/n of: ?t is: ?n as: :text cardinality: :one))
         (query (p/name ?t is: ?n) (p ?t n: ?n))
         (select (p/name ?t is: ?n))",
    );
    let selection = checked
        .steps()
        .iter()
        .find_map(|step| match step {
            Step::Select(selection) => Some((**selection).clone()),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("a selection: {:?}", checked.diagnostics()))?;
    notebook
        .submit(
            "(concept (p ?t n: ?n) (xyz.p/n of: ?t is: ?n as: :text cardinality: :one))
             (query (p/name ?t is: ?n) (p ?t n: ?n))",
        )
        .await?;
    let live = Live::new(notebook.branch.clone());
    let mut standing = live.standing(selection.clone())?;
    let env = notebook.operator.clone();
    let said = |answers: Vec<_>| {
        let mut said: Vec<String> = answers.iter().map(ToString::to_string).collect();
        said.sort();
        said
    };
    notebook.submit(r#"(assert (xyz.p x:1 n: "a"))"#).await?;
    notebook.submit(r#"(assert (xyz.p x:1 n: "b"))"#).await?;
    notebook.submit(r#"(assert (xyz.p x:2 n: "kept"))"#).await?;
    let mut answers = standing.poll(&env).await?.unwrap_or_default();
    assert_eq!(
        said(answers.clone()),
        said(live.select(&selection, &env).await?)
    );
    assert_eq!(answers.len(), 2, "{answers:?}");
    notebook.submit("(retract (p x:1))").await?;
    if let Some(now) = standing.poll(&env).await? {
        answers = now;
    }
    assert_eq!(
        said(answers.clone()),
        said(live.select(&selection, &env).await?)
    );
    assert_eq!(answers.len(), 1, "{answers:?}");
    assert!(said(answers)[0].contains("kept"));
    Ok(())
}
