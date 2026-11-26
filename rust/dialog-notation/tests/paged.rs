//! A spike: a long list rendered as a window, only the rows whose order
//! key falls in the window's bounds, the window moved by a command a
//! front end posts on scroll. The window is defined once, as a macro of
//! the notation, `tests/fixtures/paged.clj`, and two lists of different
//! concepts and key types use it with one form each.
//! `design/macros/paged-views-spike.md` says what it found.

#![allow(deprecated)]

use std::time::Instant;

use anyhow::Result;
use dialog_notation::live::Live;
use dialog_notation::{Analyzer, Patch, Plan, Step, compile, parse};

#[path = "support/session.rs"]
mod session;
use session::Notebook;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// The paged list, written once.
const PAGED: &str = include_str!("fixtures/paged.clj");

/// Two lists, each one `paged` form: to-dos by title, messages by time.
const LISTS: &str = r#"
(concept (todo ?t title: ?title)
  (xyz.todo/title of: ?t is: ?title as: :text cardinality: :one))

(paged todo/page rows: todo/paged read: (todo ?t title: ?title) entity: ?t key: ?title
       window: window/text scroll: window/scroll-text
       row: [:li {:data-key ?title} ?title])

(concept (message ?m at: ?at body: ?body)
  (xyz.message/at of: ?m is: ?at as: :integer cardinality: :one)
  (xyz.message/body of: ?m is: ?body as: :text cardinality: :one))

(paged message/page rows: message/paged read: (message ?m at: ?at body: ?body) entity: ?m key: ?at
       window: window/integer scroll: window/scroll-integer
       row: [:p {:data-key ?at} ?body])
"#;

/// The plan of the view `asked` answers, after the definitions.
fn plan_of(asked: &str) -> Result<Plan> {
    let text = format!("{PAGED}\n{LISTS}\n{asked}");
    let analysis = Analyzer::new()?.analyze(&parse(&text)?);
    let refused: Vec<String> = analysis
        .diagnostics
        .iter()
        .filter_map(|diagnostic| diagnostic.refusal.as_ref().map(ToString::to_string))
        .collect();
    anyhow::ensure!(refused.is_empty(), "{refused:?}");
    for tree in &analysis.analyzed {
        if let [Step::View(plan)] = compile(tree)?.as_slice() {
            return Ok((**plan).clone());
        }
    }
    Err(anyhow::anyhow!(
        "{asked} is no query whose answer is markup"
    ))
}

async fn write(notebook: &mut Notebook, text: &str) -> Result<()> {
    let (_, diagnostics) = notebook.submit(text).await?;
    anyhow::ensure!(diagnostics.is_empty(), "{diagnostics:?}");
    Ok(())
}

async fn opened() -> Result<Notebook> {
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, &format!("{PAGED}\n{LISTS}")).await?;
    Ok(notebook)
}

/// How many instances patches bring and take away.
fn moved(patches: &[Patch]) -> (usize, usize) {
    patches.iter().fold((0, 0), |(i, r), patch| match patch {
        Patch::Insert { .. } => (i + 1, r),
        Patch::Remove { .. } => (i, r + 1),
        _ => (i, r),
    })
}

/// One definition, two lists: each shows the rows of its own concept
/// whose key is in its window, in key order, a text order and an
/// integer one.
#[dialog_common::test]
async fn it_pages_two_lists_by_one_definition() -> Result<()> {
    let mut notebook = opened().await?;
    write(
        &mut notebook,
        r#"(assert (todo todo:a title: "a"))
           (assert (todo todo:d title: "d"))
           (assert (todo todo:c title: "c"))
           (assert (todo todo:b title: "b"))
           (assert (message message:3 at: 30 body: "three"))
           (assert (message message:1 at: 10 body: "one"))
           (assert (message message:2 at: 20 body: "two"))
           (assert (window/scroll-text window: window:1 lower: "b" upper: "d"))
           (assert (window/scroll-integer window: window:2 lower: 15 upper: 100))"#,
    )
    .await?;
    let (todos, _) = notebook
        .submit("(select (todo/page window:1 is: ?p))")
        .await?;
    assert_eq!(
        todos,
        [r#"[:ol [:li {:data-key "b"} "b"] [:li {:data-key "c"} "c"]]"#]
    );
    let (messages, _) = notebook
        .submit("(select (message/page window:2 is: ?p))")
        .await?;
    assert_eq!(
        messages,
        [r#"[:ol [:p {:data-key "20"} "two"] [:p {:data-key "30"} "three"]]"#]
    );
    Ok(())
}

/// The window kept live: a fact inside it is one row inserted, a fact
/// outside it changes nothing, and the scroll command moves it, taking
/// away the rows it leaves and bringing the ones it reaches.
#[dialog_common::test]
async fn it_moves_the_window_by_the_scroll_command_and_stays_live() -> Result<()> {
    let mut notebook = opened().await?;
    write(
        &mut notebook,
        r#"(assert (todo todo:a title: "a"))
           (assert (todo todo:b title: "b"))
           (assert (todo todo:c title: "c"))
           (assert (todo todo:d title: "d"))
           (assert (todo todo:e title: "e"))
           (assert (window/scroll-text window: window:1 lower: "a" upper: "c"))"#,
    )
    .await?;
    let live = Live::new(notebook.branch.clone());
    let mut watched = live.watch(plan_of("(select (todo/page window:1 is: ?p))")?, None)?;
    watched.poll(&notebook.operator).await?;
    assert_eq!(
        watched.tree().hiccup(),
        r#"[:ol [:li {:data-key "a"} "a"] [:li {:data-key "b"} "b"]]"#
    );

    write(&mut notebook, r#"(assert (todo todo:bb title: "bb"))"#).await?;
    let inside = watched.poll(&notebook.operator).await?;
    assert_eq!(moved(&inside), (1, 0), "{inside:?}");
    assert_eq!(
        watched.tree().hiccup(),
        r#"[:ol [:li {:data-key "a"} "a"] [:li {:data-key "b"} "b"] [:li {:data-key "bb"} "bb"]]"#
    );

    write(&mut notebook, r#"(assert (todo todo:z title: "z"))"#).await?;
    let outside = watched.poll(&notebook.operator).await?;
    assert!(outside.is_empty(), "{outside:?}");

    write(
        &mut notebook,
        r#"(assert (window/scroll-text window: window:1 lower: "c" upper: "e"))"#,
    )
    .await?;
    let scrolled = watched.poll(&notebook.operator).await?;
    assert_eq!(moved(&scrolled), (2, 3), "{scrolled:?}");
    assert_eq!(
        watched.tree().hiccup(),
        r#"[:ol [:li {:data-key "c"} "c"] [:li {:data-key "d"} "d"]]"#
    );
    Ok(())
}

/// The cost of one window move over a list of 10000 rows (or
/// `PAGED_ROWS`), a window of 50: printed, not gated.
#[dialog_common::test]
async fn it_measures_a_window_move_over_many_rows() -> Result<()> {
    let rows: usize = option_env!("PAGED_ROWS")
        .and_then(|rows| rows.parse().ok())
        .unwrap_or(10_000);
    let mut notebook = opened().await?;
    let started = Instant::now();
    for chunk in (0..rows).collect::<Vec<_>>().chunks(1000) {
        let text: String = chunk
            .iter()
            .map(|n| format!("(assert (message message:{n} at: {n} body: \"m{n}\"))\n"))
            .collect();
        write(&mut notebook, &text).await?;
    }
    write(
        &mut notebook,
        "(assert (window/scroll-integer window: window:1 lower: 0 upper: 50))",
    )
    .await?;
    let loaded = started.elapsed();
    let live = Live::new(notebook.branch.clone());
    let mut watched = live.watch(plan_of("(select (message/page window:1 is: ?p))")?, None)?;
    let started = Instant::now();
    watched.poll(&notebook.operator).await?;
    let opened = started.elapsed();

    let mut moves = Vec::new();
    for step in 1..=5 {
        let lower = step * rows / 10;
        write(
            &mut notebook,
            &format!(
                "(assert (window/scroll-integer window: window:1 lower: {lower} upper: {}))",
                lower + 50
            ),
        )
        .await?;
        let started = Instant::now();
        let patches = watched.poll(&notebook.operator).await?;
        moves.push(started.elapsed());
        assert_eq!(moved(&patches), (50, 50), "step {step}");
    }

    write(
        &mut notebook,
        r#"(assert (message message:outside at: 99999 body: "far"))"#,
    )
    .await?;
    let started = Instant::now();
    let outside = watched.poll(&notebook.operator).await?;
    let untouched = started.elapsed();
    assert!(outside.is_empty(), "{outside:?}");
    println!(
        "rows {rows}: loaded in {loaded:?}, first poll {opened:?}, window moves {moves:?}, \
         a fact outside the window {untouched:?}"
    );
    Ok(())
}

/// A macro may hold a query that collects, since a query is where
/// `distinct` stands. A macro whose template is a rule that collects is
/// still refused, as that rule written out is.
#[dialog_common::test]
async fn it_refuses_a_macro_whose_rule_collects() -> Result<()> {
    let mut notebook = opened().await?;
    let refused = notebook
        .submit(
            "(rule (syntax (rule (xyz.out ?l n: (distinct ?x)) (todo ?l title: ?x)))
               (expand (folding)))",
        )
        .await
        .map_err(|error| error.downcast::<dialog_notation::Error>());
    assert!(
        matches!(&refused, Err(Ok(dialog_notation::Error::Refused(why)))
            if why.contains("distinct stands only in a query")),
        "{refused:?}"
    );
    Ok(())
}
