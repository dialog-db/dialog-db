//! Definitions kept on a branch, and loaded from it.
//!
//! A document's concepts, macros and queries are written to the branch
//! it is submitted to, each compiled with every reference resolved, under
//! the entity its canonical form makes, with a source map beside it and
//! the binding that puts it under its name and signature in the space. A
//! notebook opened anew on the branch loads them and answers with
//! nothing submitted again.

#![allow(deprecated)]

use anyhow::Result;
use dialog_notation::definition;
use dialog_notation::live::Live;
use dialog_notation::{Analyzer, Error, check};

#[path = "../examples/notebook/session.rs"]
mod session;
use session::Notebook;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// A concept, a macro over it, a query and a view, and a to-do.
const DOCUMENT: &str = r#"
    (concept (todo ?t title: ?title)
      (xyz.todo/title of: ?t is: ?title as: :text cardinality: :one))
    (rule (syntax (todo ?t title: ?x)) (expand (task ?t ?x)))
    (query (todo/card ?t is: ?card)
      (todo ?t title: ?title)
      (is ?card {:title ?title}))
    (query (todo/row ?t is: ?row)
      (todo ?t title: ?title)
      (view ?row [:li ?title]))
    (assert (todo todo:1 title: "Buy milk"))
"#;

/// What is asked of the document: a query, a macro and a view.
const ASKED: [&str; 3] = [
    "(select (todo/card ?t is: ?card))",
    "(select (task ?t ?x))",
    "(select (todo/row ?t is: ?row))",
];

const ANSWERED: [&str; 3] = [
    r#"(todo/card todo:1 is: {:title "Buy milk"})"#,
    r#"(xyz.todo todo:1 title: "Buy milk")"#,
    r#"[:li "Buy milk"]"#,
];

/// Submit the document, ask it, open the analyzer anew from the branch,
/// and ask again: a selection, a macro and a view answer as they did.
macro_rules! round_trip {
    ($notebook:expr) => {{
        let mut notebook = $notebook;
        let (_, diagnostics) = notebook.submit(DOCUMENT).await?;
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
        for (asked, answered) in ASKED.iter().zip(ANSWERED) {
            assert_eq!(notebook.submit(asked).await?.0, vec![answered], "{asked}");
        }
        notebook.reload().await?;
        assert_eq!(
            notebook.analyzer().definitions().count(),
            0,
            "nothing is loaded"
        );
        for (asked, answered) in ASKED.iter().zip(ANSWERED) {
            assert_eq!(
                notebook.submit(asked).await?.0,
                vec![answered],
                "{asked}, from a fresh analyzer"
            );
        }
        // A fresh analyzer with nothing loaded knows none of them: a
        // name with a `/` that nothing defines applies no relation.
        let mut bare = Analyzer::new()?;
        let refused = check(&mut bare, ASKED[0]);
        assert!(
            refused.refusals().any(|refused| matches!(
                &refused.refusal,
                Some(Error::Refused(why)) if why.contains("applies no relation")
            )),
            "{:?}",
            refused.diagnostics
        );
    }};
}

/// The definitions the branch binds `name` to, read from it.
async fn bound_to(notebook: &Notebook, name: &str) -> Result<Vec<String>> {
    Ok(Live::new(notebook.branch.clone())
        .bindings(&definition::range(name), &notebook.operator)
        .await?
        .into_iter()
        .map(|binding| binding.definition.to_string())
        .collect())
}

/// On the volatile provider.
#[dialog_common::test]
async fn it_answers_from_a_fresh_analyzer_on_the_same_branch() -> Result<()> {
    round_trip!(Notebook::open().await?);
    Ok(())
}

/// On the filesystem provider, which a browser does not have: there the
/// volatile provider's test,
/// `it_answers_from_a_fresh_analyzer_on_the_same_branch`, covers it.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn it_answers_from_a_fresh_analyzer_on_the_filesystem() -> Result<()> {
    round_trip!(session::FileNotebook::open().await?);
    Ok(())
}

/// What a small document writes: the definition under its entity, with
/// its kind; a source map that says what was written and what each name
/// resolved to; and one binding of the space, its key the name, a space
/// and the signature.
#[dialog_common::test]
async fn it_writes_a_definition_its_source_map_and_its_binding() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(DOCUMENT).await?;
    let kept = Live::new(notebook.branch.clone())
        .kept(&notebook.operator)
        .await?;
    let card = kept
        .bindings
        .iter()
        .find(|binding| binding.name == "todo/card")
        .expect("todo/card is bound")
        .clone();
    assert_eq!(card.signature, "query (is:)");
    let repository = Live::new(notebook.branch.clone()).repository()?;
    assert_eq!(
        card.repository, repository,
        "bound in the branch's repository"
    );
    assert_eq!(card.key(), format!("todo/card {repository} query (is:)"));
    let stored = kept
        .definitions
        .iter()
        .find(|kept| kept.entity == card.definition)
        .expect("the query is kept");
    assert_eq!(stored.kind, definition::Kind::Query);
    // Kept canonical and with no name: its own is `@`, its variables
    // named by the labelling, and the concept it reads written out as
    // the attribute it is.
    let form = stored.form.to_string();
    assert!(form.contains("(@ ?v") && !form.contains("?card"), "{form}");
    assert!(
        !form.contains("todo/card") && !form.contains("(todo "),
        "{form}"
    );
    assert!(form.contains("xyz.todo"), "{form}");
    // `Name` is derived on the branch, by a rule over the bindings.
    let names = notebook
        .submit(r#"(select (syntax.name ?e is: "todo/card" definition: ?d))"#)
        .await?
        .0;
    assert_eq!(
        names,
        vec![format!(
            r#"(syntax.name {} definition: {} is: "todo/card")"#,
            card.entity()?,
            card.definition
        )]
    );
    Ok(())
}

/// Two branches that define one thing, each spelled its own way, keep it
/// under one entity.
#[dialog_common::test]
async fn it_keeps_one_definition_under_one_entity_on_two_branches() -> Result<()> {
    let mut one = Notebook::open().await?;
    let mut other = Notebook::open().await?;
    one.submit(DOCUMENT).await?;
    other
        .submit(
            r#"(concept (todo ?e title: ?named)
                 (xyz.todo/title of: ?e is: ?named as: :text cardinality: :one))
               (query (todo/card ?e is: ?made)
                 (is ?made {:title ?named})
                 (todo ?e title: ?named))"#,
        )
        .await?;
    assert_eq!(
        bound_to(&one, "todo").await?,
        bound_to(&other, "todo").await?
    );
    assert_eq!(
        bound_to(&one, "todo/card").await?,
        bound_to(&other, "todo/card").await?
    );
    Ok(())
}

/// Two spellings of one definition write one value under its entity:
/// its canonical form. Each source map keeps its own spelling.
#[dialog_common::test]
async fn it_writes_the_canonical_form_whatever_the_spelling() -> Result<()> {
    let spelled = [
        r#"(concept (todo ?t title: ?title)
             (xyz.todo/title of: ?t is: ?title as: :text cardinality: :one))
           (query (todo/card ?t is: ?card) (todo ?t title: ?title) (is ?card {:title ?title}))"#,
        r#"(concept (todo ?e title: ?named)
             (xyz.todo/title of: ?e is: ?named as: :text cardinality: :one))
           (query (todo/card ?e is: ?made) (is ?made {:title ?named}) (todo ?e title: ?named))"#,
    ];
    let mut forms = Vec::new();
    let mut written = Vec::new();
    for text in spelled {
        let mut notebook = Notebook::open().await?;
        notebook.submit(text).await?;
        let kept = Live::new(notebook.branch.clone())
            .kept(&notebook.operator)
            .await?;
        let mut kept: Vec<(String, String)> = kept
            .definitions
            .iter()
            .map(|kept| (kept.entity.to_string(), kept.form.to_string()))
            .collect();
        kept.sort();
        forms.push(kept);
        let checked = check(&mut Analyzer::new()?, text);
        written.push(
            checked
                .defined
                .iter()
                .map(|made| made.source.written.to_string())
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(forms[0], forms[1]);
    assert_ne!(written[0], written[1], "each source map keeps its spelling");
    assert!(written[1][0].contains("?named"), "{:?}", written[1]);
    Ok(())
}

/// `(unname todo/card)` takes every binding of `todo/card` away: its
/// binding is retracted, its definition stays kept, and the name is then
/// bound to nothing: read again, it is what it is with no binding, the
/// derived attribute `card` of the concept `todo`, which nothing
/// concludes.
#[dialog_common::test]
async fn it_takes_a_name_away() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(DOCUMENT).await?;
    let live = Live::new(notebook.branch.clone());
    let kept = live.kept(&notebook.operator).await?;
    let card = kept
        .bindings
        .iter()
        .find(|binding| binding.name == "todo/card")
        .cloned()
        .expect("todo/card is bound");
    notebook.submit("(unname todo/card)").await?;
    let after = live.kept(&notebook.operator).await?;
    assert!(!after.bindings.contains(&card), "the binding is retracted");
    assert_eq!(after.definitions, kept.definitions, "the definitions stay");
    let (answers, _) = notebook.submit(ASKED[0]).await?;
    assert!(answers.is_empty(), "{answers:?}");
    let unknown = notebook
        .submit("(unname nonesuch)")
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&unknown, Err(Ok(Error::Unknown(_)))),
        "{unknown:?}"
    );
    Ok(())
}

/// A pull brings what another branch defined, and the analyzer, which
/// reads what each document reaches, answers by it with no refresh.
#[dialog_common::test]
async fn it_sees_a_definition_a_pull_brought() -> Result<()> {
    let mut main = Notebook::open().await?;
    main.submit(DOCUMENT).await?;
    let mut other = main.branch("other").await?;
    let before = other.submit(ASKED[0]).await;
    let before = before.map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&before, Err(Ok(Error::Refused(why))) if why.contains("applies no relation")),
        "nothing is defined on the other branch yet: {before:?}"
    );
    other
        .branch
        .pull(dialog_repository::Download::MinimalRanges)
        .from(&main.branch)
        .perform(&other.operator)
        .await?;
    assert_eq!(other.submit(ASKED[0]).await?.0, vec![ANSWERED[0]]);
    Ok(())
}

/// `cdata/chore`, defined in the space.
const CHORE: &str =
    "(concept (cdata/chore ?t title: ?title) (abc.task/name of: ?t is: ?title as: :text))";

/// A name the space binds to nothing is refused, naming the other names
/// that end as it does, and nothing of the document is written; the path
/// it named answers.
#[dialog_common::test]
async fn it_refuses_a_name_bound_to_nothing_and_writes_nothing() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(DOCUMENT).await?;
    notebook.submit(CHORE).await?;
    notebook.reload().await?;
    let live = Live::new(notebook.branch.clone());
    let kept = live.kept(&notebook.operator).await?;
    let revision = notebook.branch.revision();
    let refused = notebook
        .submit(
            r#"(assert (todo todo:2 title: "Call mum"))
               (assert (chore task:2 title: "Whose?"))"#,
        )
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&refused, Err(Ok(Error::Unbound(_)))),
        "{refused:?}"
    );
    let Err(Ok(Error::Unbound(unbound))) = &refused else {
        unreachable!()
    };
    assert_eq!(unbound.name, "chore");
    let said: Vec<String> = unbound
        .suggestions
        .iter()
        .map(ToString::to_string)
        .collect();
    assert_eq!(said, vec!["cdata/chore (concept (title:))"]);
    assert_eq!(notebook.branch.revision(), revision, "nothing is written");
    assert_eq!(live.kept(&notebook.operator).await?, kept);

    notebook
        .submit(r#"(assert (cdata/chore task:1 title: "Theirs"))"#)
        .await?;
    assert_eq!(
        notebook.submit("(select (abc.task ?t name: ?x))").await?.0,
        vec![r#"(abc.task task:1 name: "Theirs")"#]
    );
    Ok(())
}

/// Several rules concluding one concept are no several definitions:
/// reading the concept reads what each concludes.
#[dialog_common::test]
async fn it_reads_every_rule_that_concludes_one_concept() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (seen ?t text: ?x) (xyz.seen/text of: ?t is: ?x as: :text))
               (rule (seen ?t text: ?x) (xyz.a ?t text: ?x))
               (rule (seen ?t text: ?x) (xyz.b ?t text: ?x))
               (assert (xyz.a a:1 text: (:text "from a")))
               (assert (xyz.b b:1 text: (:text "from b")))"#,
        )
        .await?;
    assert_eq!(
        notebook.submit("(select (seen ?t text: ?x))").await?.0,
        vec![
            r#"(xyz.seen a:1 text: "from a")"#,
            r#"(xyz.seen b:1 text: "from b")"#,
        ]
    );
    Ok(())
}

/// What is stored on the branch for the analysis below: a concept, a
/// query and a second query that calls it, a macro and a second macro
/// the first expands to only once its holes are filled, and two
/// definitions nothing asks for.
const STORED: &str = r#"
    (concept (todo ?t title: ?title)
      (xyz.todo/title of: ?t is: ?title as: :text cardinality: :one))
    (concept (spare ?s text: ?x) (xyz.spare/text of: ?s is: ?x as: :text))
    (concept (other ?o text: ?x) (xyz.other/text of: ?o is: ?x as: :text))
    (query (todo/label ?t is: ?label)
      (todo ?t title: ?title)
      (is ?label ?title))
    (query (todo/badge ?t is: ?badge)
      (todo/label ?t is: ?label)
      (is ?badge {:label ?label}))
    (rule (syntax (todo ?t title: ?x)) (expand (task ?t ?x)))
    (rule (syntax (task ?t ... ?rest)) (expand (chore ?t ... ?rest)))
    (assert (todo todo:1 title: "Buy milk"))
"#;

/// A document that uses the concept, the calling query and the first
/// macro.
const USING: &str = r#"
    (select (todo ?t title: ?x))
    (select (todo/badge ?t is: ?badge))
    (select (chore ?t ?x))
"#;

/// A notebook with nothing in memory analyzes a document whose
/// definitions are all on the branch, reading in rounds only what it
/// reaches: the concept, the query it asks (whose callee is inlined in
/// it) and the two macros, the second asked for once the first is
/// expanded. Of the seven stored, the two spares and the callee are not
/// read.
macro_rules! reached {
    ($notebook:expr) => {{
        let mut notebook = $notebook;
        notebook.submit(STORED).await?;
        notebook.reload().await?;
        assert_eq!(notebook.analyzer().definitions().count(), 0);
        let (answers, _) = notebook.submit(USING).await?;
        assert_eq!(
            answers,
            vec![
                r#"(todo/badge todo:1 is: {:label "Buy milk"})"#,
                r#"(xyz.todo todo:1 title: "Buy milk")"#,
            ]
        );
        let rounds = notebook.rounds;
        assert_eq!(rounds.definitions, 4, "{rounds:?}");
        assert_eq!(rounds.rounds, 3, "{rounds:?}");
        let known: Vec<String> = notebook
            .analyzer()
            .names()
            .bindings()
            .map(|binding| binding.name)
            .collect();
        // Only the ranges of the names reached are read.
        assert!(
            !known.iter().any(|name| name == "spare" || name == "other"),
            "{known:?}"
        );
    }};
}

#[dialog_common::test]
async fn it_reads_only_the_definitions_a_document_reaches() -> Result<()> {
    reached!(Notebook::open().await?);
    Ok(())
}

/// On the filesystem provider, which a browser does not have: there
/// `it_reads_only_the_definitions_a_document_reaches` covers it.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn it_reads_only_the_definitions_a_document_reaches_on_the_filesystem() -> Result<()> {
    reached!(session::FileNotebook::open().await?);
    Ok(())
}

/// A definition in the session's overlay, which no commit keeps, and
/// one in the durable tree are both read: the overlay's query calls the
/// durable one.
macro_rules! layered {
    ($notebook:expr) => {{
        let mut notebook = $notebook;
        notebook
            .submit(
                r#"(concept (todo ?t title: ?title)
                     (xyz.todo/title of: ?t is: ?title as: :text cardinality: :one))
                   (query (todo/label ?t is: ?label) (todo ?t title: ?title) (is ?label ?title))
                   (assert (todo todo:1 title: "Buy milk"))"#,
            )
            .await?;
        // The calling query, made where nothing is written, and laid on
        // the overlay alone.
        let mut writer = notebook.analyzer().clone();
        let checked = check(
            &mut writer,
            "(query (todo/badge ?t is: ?badge) (todo/label ?t is: ?label) (is ?badge {:label ?label}))",
        );
        let [badge] = checked.defined.as_slice() else {
            panic!("one definition: {:?}", checked.diagnostics);
        };
        for statement in definition::statements(badge)? {
            notebook.branch.overlay().assert(statement)?;
        }
        notebook.reload().await?;
        let (answers, _) = notebook
            .submit("(select (todo/badge ?t is: ?badge))")
            .await?;
        assert_eq!(
            answers,
            vec![r#"(todo/badge todo:1 is: {:label "Buy milk"})"#]
        );
    }};
}

#[dialog_common::test]
async fn it_reads_definitions_from_the_overlay_and_the_durable_tree() -> Result<()> {
    layered!(Notebook::open().await?);
    Ok(())
}

/// On the filesystem provider; in a browser,
/// `it_reads_definitions_from_the_overlay_and_the_durable_tree` covers
/// it.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn it_reads_definitions_from_the_overlay_and_the_durable_tree_on_the_filesystem() -> Result<()>
{
    layered!(session::FileNotebook::open().await?);
    Ok(())
}

/// A name nothing holds is unknown once the rounds stop asking, and a
/// name bound to nothing that another path ends as is refused naming it,
/// both read from the branch by an analyzer that held nothing.
#[dialog_common::test]
async fn it_refuses_what_the_rounds_do_not_find() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(DOCUMENT).await?;
    notebook.submit(CHORE).await?;
    notebook.reload().await?;
    let unknown = notebook
        .submit("(select (nonesuch ?t title: ?x))")
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&unknown, Err(Ok(Error::Unknown(_)))),
        "{unknown:?}"
    );
    notebook.reload().await?;
    let unbound = notebook
        .submit("(select (chore ?t title: ?x))")
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&unbound, Err(Ok(Error::Unbound(_)))),
        "{unbound:?}"
    );
    Ok(())
}

/// What the suggestions read from the branch are: the names of the
/// space under a prefix, one range.
#[dialog_common::test]
async fn it_suggests_names_read_from_the_branch() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(DOCUMENT).await?;
    notebook.submit(CHORE).await?;
    let live = Live::new(notebook.branch.clone());
    let said: Vec<String> = live
        .suggest("", &notebook.operator)
        .await?
        .iter()
        .map(ToString::to_string)
        .collect();
    assert!(
        said.contains(&"cdata/chore (concept (title:))".to_string()),
        "{said:?}"
    );
    assert!(
        said.contains(&"todo/card (query (is:))".to_string()),
        "{said:?}"
    );
    let todo: Vec<String> = live
        .suggest("todo/c", &notebook.operator)
        .await?
        .iter()
        .map(ToString::to_string)
        .collect();
    assert_eq!(todo, vec!["todo/card (query (is:))"]);
    let cdata: Vec<String> = live
        .suggest("cdata/", &notebook.operator)
        .await?
        .iter()
        .map(ToString::to_string)
        .collect();
    assert_eq!(cdata, vec!["cdata/chore (concept (title:))"]);
    Ok(())
}

/// How many rounds each sketch takes, submitted in order to a notebook
/// that holds nothing of the documents before it: each reads what it
/// reaches from the branch. Printed for the report; bounded here.
#[dialog_common::test]
async fn it_analyzes_each_sketch_in_a_few_rounds() -> Result<()> {
    let sketches: [(&str, &str); 5] = [
        ("core.clj", include_str!("../sketches/core.clj")),
        ("notebook.clj", include_str!("../sketches/notebook.clj")),
        (
            "notebook-views.clj",
            include_str!("../sketches/notebook-views.clj"),
        ),
        ("table.clj", include_str!("../sketches/table.clj")),
        (
            "table-views.clj",
            include_str!("../sketches/table-views.clj"),
        ),
    ];
    let mut notebook = Notebook::open().await?;
    for (name, text) in sketches {
        notebook.reload().await?;
        notebook.submit(text).await?;
        let rounds = notebook.rounds;
        println!(
            "{name}: {} rounds, {} definitions and {} bindings read",
            rounds.rounds, rounds.definitions, rounds.bindings
        );
        assert!(
            rounds.rounds < dialog_notation::live::ROUNDS,
            "{name}: {rounds:?}"
        );
    }
    Ok(())
}

/// A name's bindings are one range of the value index: reading `todo`'s
/// among 10 or 1000 bindings of other names, many of them its neighbours
/// in key order (`todo-0`, `todo/0`, `todox0`), the scan of the key gives
/// `todo`'s one key either way, with nothing after it to filter.
#[dialog_common::test]
async fn it_reads_a_names_bindings_as_one_range() -> Result<()> {
    async fn read(unrelated: usize) -> Result<(Vec<String>, usize, usize)> {
        let mut notebook = Notebook::open().await?;
        notebook.submit(DOCUMENT).await?;
        let card = Live::new(notebook.branch.clone())
            .bindings(&definition::range("todo/card"), &notebook.operator)
            .await?;
        let mut transaction = notebook.branch.transaction();
        for at in 0..unrelated {
            let name = match at % 3 {
                0 => format!("todo-{at}"),
                1 => format!("todo/{at}"),
                _ => format!("todox{at}"),
            };
            transaction =
                transaction.assert(definition::binding_statement(&definition::Binding {
                    name,
                    repository: card[0].repository.clone(),
                    signature: "concept (title:)".into(),
                    definition: card[0].definition.clone(),
                })?);
        }
        transaction
            .commit()
            .publish()
            .perform(&notebook.operator)
            .await?;
        notebook.branch.refresh(&notebook.operator).await?;
        // The scan alone, with nothing after it to filter: what the index
        // range gives.
        use dialog_query::query::Output as _;
        let scanned = notebook
            .branch
            .query()
            .select(dialog_notation::live::binding_range(&definition::range(
                "todo",
            ))?)
            .perform(&notebook.operator)
            .try_vec()
            .await?
            .len();
        let found: Vec<String> = Live::new(notebook.branch.clone())
            .bindings(&definition::range("todo"), &notebook.operator)
            .await?
            .into_iter()
            .map(|binding| binding.key())
            .collect();
        let every = Live::new(notebook.branch.clone())
            .bindings("", &notebook.operator)
            .await?
            .len();
        Ok((found, scanned, every))
    }
    let (few, few_scanned, few_all) = read(10).await?;
    let (many, many_scanned, many_all) = read(1000).await?;
    println!(
        "keys the range scan gives: {few_scanned} of {few_all} bindings, {many_scanned} of {many_all}"
    );
    assert_eq!(few.len(), 1);
    assert!(
        few[0].starts_with("todo did:") && few[0].ends_with(" concept (title:)"),
        "{few:?}"
    );
    assert_eq!(many.len(), 1, "{many:?}");
    assert_eq!(
        (few_scanned, many_scanned),
        (1, 1),
        "the scan gives the range alone"
    );
    assert!(many_all > 1000, "{many_all}");
    Ok(())
}

/// Two replicas bind one key to two definitions. After a pull one is
/// elected, the key reads as it; the other is still kept and reached by
/// its hash.
#[dialog_common::test]
async fn it_elects_one_definition_of_a_key_two_replicas_bound() -> Result<()> {
    let mut main = Notebook::open().await?;
    main.submit(DOCUMENT).await?;
    let mut other = main.branch("other").await?;
    other
        .submit("(concept (todo ?t title: ?title) (abc.task/name of: ?t is: ?title as: :text))")
        .await?;
    let ours = bound_to(&main, "todo").await?;
    let theirs = bound_to(&other, "todo").await?;
    assert_ne!(ours, theirs);
    other
        .branch
        .pull(dialog_repository::Download::MinimalRanges)
        .from(&main.branch)
        .perform(&other.operator)
        .await?;
    let elected = bound_to(&other, "todo").await?;
    assert_eq!(elected.len(), 1, "one key, one definition: {elected:?}");
    let kept = Live::new(other.branch.clone())
        .kept(&other.operator)
        .await?;
    let both: Vec<String> = [&ours[0], &theirs[0]]
        .into_iter()
        .filter(|entity| {
            kept.definitions
                .iter()
                .any(|kept| &kept.entity.to_string() == *entity)
        })
        .cloned()
        .collect();
    assert_eq!(both.len(), 2, "both definitions are kept");
    let loser = if elected[0] == ours[0] {
        &theirs[0]
    } else {
        &ours[0]
    };
    let hash = loser.split_once(':').map(|(_, hash)| hash).unwrap_or(loser);
    other.reload().await?;
    other
        .submit(&format!(
            r#"(assert (todo#{} todo:7 title: "By hash"))"#,
            &hash[..10]
        ))
        .await?;
    let (domain, attribute) = if loser == &ours[0] {
        ("xyz.todo", "title")
    } else {
        ("abc.task", "name")
    };
    assert_eq!(
        other
            .submit(&format!("(select ({domain} todo:7 {attribute}: ?x))"))
            .await?
            .0,
        vec![format!(r#"({domain} todo:7 {attribute}: "By hash")"#)]
    );
    Ok(())
}

/// The read of a name's bindings is planned as one range: the prefix of
/// its keys is stamped on the value the scan of `syntax.binding/key`
/// reads, which the value index is ranged by.
#[test]
fn it_plans_a_names_bindings_as_a_range_of_the_value_index() -> Result<()> {
    use dialog_query::{Premise, Proposition};
    let scan = dialog_notation::live::binding_range(&definition::range("todo"))?;
    let premise: Premise = scan.into();
    let steps = dialog_query::Planner::from(vec![premise])
        .plan(&dialog_query::Environment::new())?
        .steps;
    let first = steps.first().expect("a step").as_premise();
    let Premise::Assert(Proposition::Attribute(scan)) = &first else {
        panic!("the step scans the key: {first:?}");
    };
    assert!(format!("{scan:?}").contains("syntax.binding/key"));
    let prefix = scan
        .is()
        .kind()
        .and_then(|kind| kind.refinement().and_then(|refined| refined.prefix.clone()));
    assert_eq!(prefix.as_deref(), Some("todo "));
    Ok(())
}

/// Their `todo`: same name and labels as ours, over other attributes.
const THEIR_TODO: &str =
    "(concept (todo ?t title: ?title) (abc.task/name of: ?t is: ?title as: :text))";

/// A notebook of our repository and one of theirs, on the same peer,
/// theirs known to ours as `theirs`, with their `todo` received into our
/// tree; with `ours_first`, ours defines its `todo` before theirs
/// arrives, otherwise after.
async fn two_repositories(ours_first: bool) -> Result<(Notebook, Notebook)> {
    let mut ours = Notebook::open().await?;
    let mut theirs = ours.repository("theirs").await?;
    theirs.submit(THEIR_TODO).await?;
    if ours_first {
        ours.submit(DOCUMENT).await?;
    }
    ours.receive(&theirs, THEIR_TODO).await?;
    if !ours_first {
        ours.submit(DOCUMENT).await?;
    }
    let repository = Live::new(theirs.branch.clone()).repository()?;
    ours.knowing.push(("theirs".into(), repository));
    ours.reload().await?;
    Ok((ours, theirs))
}

/// A name is a repository's. Their bindings in our tree sit beside ours
/// and override none of them: a bare `todo` is ours, `@theirs/todo` is
/// theirs, whichever arrived first.
#[dialog_common::test]
async fn it_resolves_a_bare_name_in_this_repository_and_another_by_its_name() -> Result<()> {
    for ours_first in [true, false] {
        let (mut ours, theirs) = two_repositories(ours_first).await?;
        let bound = Live::new(ours.branch.clone())
            .bindings(&definition::range("todo"), &ours.operator)
            .await?;
        assert_eq!(
            bound.len(),
            2,
            "both repositories' todo in one tree: {bound:?}"
        );
        let mine = Live::new(ours.branch.clone()).repository()?;
        let their = Live::new(theirs.branch.clone()).repository()?;
        assert_ne!(mine, their);
        ours.submit(
            r#"(assert (todo todo:9 title: "Ours"))
               (assert (@theirs/todo task:9 title: "Theirs"))"#,
        )
        .await?;
        assert_eq!(
            ours.submit("(select (xyz.todo ?t title: ?x))").await?.0,
            vec![
                r#"(xyz.todo todo:1 title: "Buy milk")"#,
                r#"(xyz.todo todo:9 title: "Ours")"#,
            ],
            "ours_first: {ours_first}"
        );
        assert_eq!(
            ours.submit("(select (abc.task ?t name: ?x))").await?.0,
            vec![r#"(abc.task task:9 name: "Theirs")"#],
            "ours_first: {ours_first}"
        );
    }
    Ok(())
}

/// `@nobody/…`, where this notebook knows no repository as `nobody`, is
/// its own refusal, and nothing is written.
#[dialog_common::test]
async fn it_refuses_a_repository_it_does_not_know() -> Result<()> {
    let (mut ours, _) = two_repositories(true).await?;
    let revision = ours.branch.revision();
    let refused = ours
        .submit(r#"(assert (@nobody/todo task:1 title: "x"))"#)
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&refused, Err(Ok(Error::NoRepository(who))) if who == "nobody"),
        "{refused:?}"
    );
    assert_eq!(ours.branch.revision(), revision, "nothing is written");
    Ok(())
}

/// A bare name the current repository does not bind, which another
/// repository this notebook knows does, is refused naming
/// `@theirs/chore`, and nothing is written.
#[dialog_common::test]
async fn it_names_another_repositorys_definition_of_a_name_not_bound_here() -> Result<()> {
    let mut ours = Notebook::open().await?;
    ours.submit(DOCUMENT).await?;
    let theirs = ours.repository("theirs").await?;
    let chore = "(concept (chore ?c title: ?title) (abc.chore/title of: ?c is: ?title as: :text))";
    ours.receive(&theirs, chore).await?;
    ours.knowing.push((
        "theirs".into(),
        Live::new(theirs.branch.clone()).repository()?,
    ));
    let revision = ours.branch.revision();
    let refused = ours
        .submit(r#"(assert (chore task:1 title: "x"))"#)
        .await
        .map_err(|error| error.downcast::<Error>());
    let Err(Ok(Error::Unbound(unbound))) = &refused else {
        panic!("refused as unbound: {refused:?}");
    };
    let said: Vec<String> = unbound
        .suggestions
        .iter()
        .map(ToString::to_string)
        .collect();
    assert_eq!(said, vec!["@theirs/chore (concept (title:))"]);
    assert_eq!(ours.branch.revision(), revision, "nothing is written");
    Ok(())
}

/// A query written against `@theirs/todo` keeps what it meant: once this
/// notebook no longer knows `theirs`, the query still answers, while
/// `@theirs/todo` written anew is refused.
#[dialog_common::test]
async fn it_keeps_what_another_repositorys_name_meant() -> Result<()> {
    let (mut ours, _) = two_repositories(true).await?;
    ours.submit(
        r#"(query (task/title ?t is: ?title) (@theirs/todo ?t title: ?title) (is ?title ?title))
           (assert (@theirs/todo task:1 title: "Theirs"))"#,
    )
    .await?;
    ours.knowing.clear();
    ours.reload().await?;
    assert_eq!(
        ours.submit("(select (task/title ?t is: ?x))").await?.0,
        vec![r#"(task/title task:1 is: "Theirs")"#]
    );
    let refused = ours
        .submit("(select (@theirs/todo ?t title: ?x))")
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&refused, Err(Ok(Error::NoRepository(who))) if who == "theirs"),
        "{refused:?}"
    );
    Ok(())
}
