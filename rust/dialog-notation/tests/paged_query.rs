//! A spike beside `tests/paged.rs`: the window in the query. Each list
//! joins `list/item` with one monotone rule and names the query that
//! draws its rows. One query, written once, pages whichever list a
//! window names, and no rule reads the window. The test plays the
//! renderer: it keeps the window, and draws each row by the view the
//! row names. `fixtures/paged-query.clj` holds what is written once,
//! `fixtures/paged-query-lists.clj` the two lists.
//! `design/macros/paged-views-spike.md` says what it found.

#![allow(deprecated)]

use anyhow::Result;
use dialog_notation::live::{Live, Watched};
use dialog_notation::{Analyzer, Patch, Plan, Step, compile, parse};

#[path = "support/session.rs"]
mod session;
use session::Notebook;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

const PAGED: &str = include_str!("fixtures/paged-query.clj");
const LISTS: &str = include_str!("fixtures/paged-query-lists.clj");

const ROWS: &str = r#"
(assert (todo todo:a title: "a"))
(assert (todo todo:b title: "b"))
(assert (todo todo:c title: "c"))
(assert (todo todo:d title: "d"))
(assert (message message:1 at: 10 body: "one"))
(assert (message message:2 at: 20 body: "two"))
(assert (message message:3 at: 30 body: "three"))
(assert (window window:1 list: list:todos from: "b" to: "d"))
(assert (window window:2 list: list:messages from: 15 to: 100))
"#;

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

fn entity(uri: &str) -> Result<dialog_corpus::Value> {
    match parse(uri)?.as_slice() {
        [dialog_notation::Syntax::Constant(value)] => Ok(value.clone()),
        other => Err(anyhow::anyhow!("{other:?} is no entity")),
    }
}

async fn write(notebook: &mut Notebook, text: &str) -> Result<()> {
    let (_, diagnostics) = notebook.submit(text).await?;
    anyhow::ensure!(diagnostics.is_empty(), "{diagnostics:?}");
    Ok(())
}

async fn opened() -> Result<Notebook> {
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, &format!("{PAGED}\n{LISTS}")).await?;
    write(&mut notebook, ROWS).await?;
    Ok(notebook)
}

async fn page(notebook: &mut Notebook, window: &str) -> Result<String> {
    let (answers, _) = notebook
        .submit(&format!("(select (list/page {window} is: ?p))"))
        .await?;
    match answers.as_slice() {
        [one] => Ok(one.clone()),
        other => Err(anyhow::anyhow!("{other:?} is not one page")),
    }
}

/// The value of each `name` attribute in `hiccup`, in order.
fn attribute<'a>(hiccup: &'a str, name: &str) -> Vec<&'a str> {
    let marker = format!(":{name} \"");
    hiccup
        .match_indices(&marker)
        .filter_map(|(at, _)| {
            let rest = &hiccup[at + marker.len()..];
            rest.find('"').map(|end| &rest[..end])
        })
        .collect()
}

fn moved(patches: &[Patch]) -> (usize, usize) {
    patches.iter().fold((0, 0), |(i, r), patch| match patch {
        Patch::Insert { .. } => (i + 1, r),
        Patch::Remove { .. } => (i, r + 1),
        _ => (i, r),
    })
}

/// One query pages both lists, a text order and an integer one: each
/// window shows the members of its own list whose key is in its range,
/// in key order, each a place naming the member and its view.
#[dialog_common::test]
async fn it_pages_two_lists_by_one_query() -> Result<()> {
    let mut notebook = opened().await?;
    assert_eq!(
        page(&mut notebook, "window:1").await?,
        concat!(
            r#"[:ol {:data-list "list:todos"} "#,
            r#"[:list-row {:subject "todo:b" :data-key "b" :data-view "todo/row"}] "#,
            r#"[:list-row {:subject "todo:c" :data-key "c" :data-view "todo/row"}]]"#
        )
    );
    assert_eq!(
        page(&mut notebook, "window:2").await?,
        concat!(
            r#"[:ol {:data-list "list:messages"} "#,
            r#"[:list-row {:subject "message:2" :data-key "20" :data-view "message/row"}] "#,
            r#"[:list-row {:subject "message:3" :data-key "30" :data-view "message/row"}]]"#
        )
    );
    Ok(())
}

/// The renderer draws each row by the view the row names, which the
/// rule that joined the member to the list chose: the to-dos as items,
/// the messages as paragraphs.
#[dialog_common::test]
async fn it_draws_each_row_by_the_view_its_list_names() -> Result<()> {
    let mut notebook = opened().await?;
    let mut drawn = Vec::new();
    for window in ["window:1", "window:2"] {
        let page = page(&mut notebook, window).await?;
        for (subject, view) in attribute(&page, "subject")
            .into_iter()
            .zip(attribute(&page, "data-view"))
        {
            let (row, _) = notebook
                .submit(&format!("(select ({view} {subject} is: ?row))"))
                .await?;
            drawn.extend(row);
        }
    }
    assert_eq!(
        drawn,
        [
            r#"[:li "b"]"#,
            r#"[:li "c"]"#,
            r#"[:p [:time "20"] " " "two"]"#,
            r#"[:p [:time "30"] " " "three"]"#,
        ]
    );
    Ok(())
}

/// Kept live: a member added inside the window is one row inserted, one
/// outside it or in another list no patch, and the renderer moving the
/// window, a fact it writes and no rule reads, takes away the rows it
/// leaves and brings the ones it reaches.
#[dialog_common::test]
async fn it_moves_the_window_the_renderer_keeps_and_stays_live() -> Result<()> {
    let mut notebook = opened().await?;
    let live = Live::new(notebook.branch.clone());
    let mut watched = live.watch(
        plan_of("(select (list/page ?w is: ?p))")?,
        Some(entity("window:1")?),
    )?;
    watched.poll(&notebook.operator).await?;
    let keys = |watched: &Watched| {
        attribute(&watched.tree().hiccup(), "data-key")
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    };
    assert_eq!(keys(&watched), ["b", "c"]);

    write(&mut notebook, r#"(assert (todo todo:bb title: "bb"))"#).await?;
    let inside = watched.poll(&notebook.operator).await?;
    assert_eq!(moved(&inside), (1, 0), "{inside:?}");
    assert_eq!(keys(&watched), ["b", "bb", "c"]);

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
        r#"(assert (window window:1 list: list:todos from: "c" to: "e"))"#,
    )
    .await?;
    let moved_to = watched.poll(&notebook.operator).await?;
    assert_eq!(moved(&moved_to), (1, 2), "{moved_to:?}");
    assert_eq!(keys(&watched), ["c", "d"]);
    Ok(())
}

/// The bounds cannot be the query's own parameters, given by the one who
/// asks: a parameter that is only compared is bound by no premise, and
/// the rule a layer compiles to concludes it.
#[dialog_common::test]
async fn it_refuses_bounds_that_are_only_compared() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, PAGED).await?;
    let refused = notebook
        .submit(
            r#"(query (list/span ?l from: ?lo to: ?hi is: ?span)
                 (list/item ?r list: ?l key: ?k view: ?v)
                 (<= ?lo : ?k)
                 (< ?k : ?hi)
                 (view ?span [:li ?k]))
               (select (list/span list:todos from: "b" to: "d" is: ?s))"#,
        )
        .await;
    assert!(
        matches!(&refused, Err(error) if format!("{error:#}").contains("do not bind what its head concludes under ?lo")),
        "{refused:?}"
    );
    Ok(())
}

/// The window kept in the stack's `session:` layer, which never syncs:
/// the renderer writes its bounds there, the layer of the page that
/// reads `list/item` in the window is subscribed over the stack, and a
/// move of the bounds there moves the rows it answers. The primary
/// holds no window, and the crate's driver, which subscribes through
/// the branch, sees none.
#[dialog_common::test]
async fn it_moves_a_window_kept_in_the_session_layer() -> Result<()> {
    use dialog_corpus::Statement as _;
    use dialog_query::the;
    use dialog_repository::session_layer;

    let mut notebook = Notebook::open().await?;
    write(&mut notebook, &format!("{PAGED}\n{LISTS}")).await?;
    let rows: String = ROWS
        .lines()
        .filter(|line| !line.contains("(window "))
        .collect::<Vec<_>>()
        .join("\n");
    write(&mut notebook, &rows).await?;

    let window = entity("window:1")?;
    let plan = plan_of("(select (list/page ?w is: ?p))")?;
    let layer = plan.layers.len() - 1;
    let query = plan.query(layer, Some(&window))?;
    if let Some(rule) = &query.rule {
        notebook.branch.overlay().assert(rule.clone())?;
    }

    let stack = notebook.branch.stack().perform(&notebook.operator).await?;
    let list = entity("list:todos")?;
    let session = session_layer;
    let at = |from: &str, to: &str| {
        let of = match &window {
            dialog_corpus::Value::Entity(of) => Ok(of.clone()),
            other => Err(anyhow::anyhow!("{other:?} is no entity")),
        }?;
        let list = match &list {
            dialog_corpus::Value::Entity(list) => list.clone(),
            other => return Err(anyhow::anyhow!("{other:?} is no entity")),
        };
        anyhow::Ok((
            the!("xyz.window/list")
                .of(of.clone())
                .is(list)
                .at(session()),
            the!("xyz.window/from")
                .of(of.clone())
                .is(from.to_string())
                .at(session()),
            the!("xyz.window/to")
                .of(of)
                .is(to.to_string())
                .at(session()),
        ))
    };
    let (l, f, t) = at("b", "d")?;
    stack
        .assert(l)
        .assert(f)
        .assert(t)
        .perform(&notebook.operator)
        .await?;

    let mut reader = stack.subscribe(query.select.clone());
    stack.settle(&notebook.operator).await?;
    reader.poll();
    let members = |results: &[_]| {
        ["todo:a", "todo:b", "todo:c", "todo:d"]
            .into_iter()
            .filter(|member| {
                results
                    .iter()
                    .any(|row| format!("{row:?}").contains(&format!("this: Held({member})")))
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(members(&reader.results()), ["todo:b", "todo:c"]);

    let (_, from, to) = at("b", "d")?;
    let (_, f, t) = at("c", "e")?;
    stack
        .assert(f)
        .assert(t)
        .retract(from)
        .retract(to)
        .perform(&notebook.operator)
        .await?;
    stack.settle(&notebook.operator).await?;
    let moved = reader.poll();
    assert!(!moved.is_empty(), "the move is delivered");
    assert_eq!(members(&reader.results()), ["todo:c", "todo:d"]);

    let (primary, _) = notebook
        .submit("(select (window ?w list: ?l from: ?lo to: ?hi))")
        .await?;
    assert!(
        primary.is_empty(),
        "the primary holds no window: {primary:?}"
    );
    let live = Live::new(notebook.branch.clone());
    let mut watched = live.watch(plan, Some(window))?;
    watched.poll(&notebook.operator).await?;
    assert_eq!(
        watched.tree().hiccup(),
        "",
        "the driver reads through the branch and sees no session: window"
    );
    Ok(())
}
