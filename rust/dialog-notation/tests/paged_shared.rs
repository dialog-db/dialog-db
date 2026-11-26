//! A spike beside `tests/paged.rs`: paging through one shared relation
//! instead of a macro. Each list derives its rows into `xyz.list` with a
//! rule, one rule says which rows a window shows, and one query pages
//! over it for whichever list a window names. `fixtures/paged-shared.clj`
//! holds what is written once, `fixtures/paged-shared-lists.clj` the two
//! lists. `design/macros/paged-views-spike.md` says what it found.

#![allow(deprecated)]

use anyhow::Result;
use dialog_notation::live::{Live, Watched};
use dialog_notation::{Analyzer, Patch, Plan, Step, compile, parse};

#[path = "support/session.rs"]
mod session;
use session::Notebook;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

const SHARED: &str = include_str!("fixtures/paged-shared.clj");
const LISTS: &str = include_str!("fixtures/paged-shared-lists.clj");

const ROWS: &str = r#"
(assert (todo todo:a title: "a"))
(assert (todo todo:b title: "b"))
(assert (todo todo:c title: "c"))
(assert (todo todo:d title: "d"))
(assert (message message:1 at: 10 body: "one"))
(assert (message message:2 at: 20 body: "two"))
(assert (message message:3 at: 30 body: "three"))
(assert (window window:1 list: list:todos lower: "b" upper: "d"))
(assert (window window:2 list: list:messages lower: 15 upper: 100))
"#;

fn plan_of(asked: &str) -> Result<Plan> {
    let text = format!("{SHARED}\n{LISTS}\n{asked}");
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
    write(&mut notebook, &format!("{SHARED}\n{LISTS}")).await?;
    write(&mut notebook, ROWS).await?;
    Ok(notebook)
}

fn entity(uri: &str) -> Result<dialog_corpus::Value> {
    match parse(uri)?.as_slice() {
        [dialog_notation::Syntax::Constant(value)] => Ok(value.clone()),
        other => Err(anyhow::anyhow!("{other:?} is no entity")),
    }
}

fn moved(patches: &[Patch]) -> (usize, usize) {
    patches.iter().fold((0, 0), |(i, r), patch| match patch {
        Patch::Insert { .. } => (i + 1, r),
        Patch::Remove { .. } => (i, r + 1),
        _ => (i, r),
    })
}

/// One page query, written once, pages both lists: a text order and an
/// integer one, each window naming its list. Every row looks alike: it
/// shows the label its list derived.
#[dialog_common::test]
async fn it_pages_two_lists_by_one_query_over_a_shared_relation() -> Result<()> {
    let mut notebook = opened().await?;
    let (todos, _) = notebook
        .submit("(select (window/page window:1 is: ?p))")
        .await?;
    assert_eq!(
        todos,
        [r#"[:ol [:li {:data-key "b"} "b"] [:li {:data-key "c"} "c"]]"#]
    );
    let (messages, _) = notebook
        .submit("(select (window/page window:2 is: ?p))")
        .await?;
    assert_eq!(
        messages,
        [r#"[:ol [:li {:data-key "20"} "two"] [:li {:data-key "30"} "three"]]"#]
    );
    Ok(())
}

/// A list that draws its rows itself writes a row query and a page
/// query of its own over the shared rows a window shows.
#[dialog_common::test]
async fn it_draws_a_lists_own_rows_over_the_shared_relation() -> Result<()> {
    let mut notebook = opened().await?;
    let (messages, _) = notebook
        .submit("(select (message/page window:2 is: ?p))")
        .await?;
    assert_eq!(
        messages,
        [concat!(
            r#"[:div [:p {:data-key "20"} [:time "20"] " " "two"] "#,
            r#"[:p {:data-key "30"} [:time "30"] " " "three"]]"#
        )]
    );
    Ok(())
}

/// Kept live through the shared relation: a fact inside the window is
/// one row inserted, a fact outside it no patch, and the scroll command
/// moves the window.
#[dialog_common::test]
async fn it_moves_a_shared_window_and_stays_live() -> Result<()> {
    let mut notebook = opened().await?;
    let live = Live::new(notebook.branch.clone());
    let mut watched = live.watch(
        plan_of("(select (window/page ?w is: ?p))")?,
        Some(entity("window:1")?),
    )?;
    watched.poll(&notebook.operator).await?;
    let shown = |watched: &Watched| watched.tree().hiccup();
    assert_eq!(
        shown(&watched),
        r#"[:ol [:li {:data-key "b"} "b"] [:li {:data-key "c"} "c"]]"#
    );

    write(&mut notebook, r#"(assert (todo todo:bb title: "bb"))"#).await?;
    let inside = watched.poll(&notebook.operator).await?;
    assert_eq!(moved(&inside), (1, 0), "{inside:?}");

    write(&mut notebook, r#"(assert (todo todo:z title: "z"))"#).await?;
    let outside = watched.poll(&notebook.operator).await?;
    assert!(outside.is_empty(), "{outside:?}");

    write(
        &mut notebook,
        r#"(assert (message message:4 at: 25 body: "another list"))"#,
    )
    .await?;
    let other = watched.poll(&notebook.operator).await?;
    assert!(other.is_empty(), "{other:?}");

    write(
        &mut notebook,
        r#"(assert (window/scroll window: window:1 lower: "c" upper: "e"))"#,
    )
    .await?;
    let scrolled = watched.poll(&notebook.operator).await?;
    assert_eq!(moved(&scrolled), (1, 2), "{scrolled:?}");
    assert_eq!(
        shown(&watched),
        r#"[:ol [:li {:data-key "c"} "c"] [:li {:data-key "d"} "d"]]"#
    );
    Ok(())
}
