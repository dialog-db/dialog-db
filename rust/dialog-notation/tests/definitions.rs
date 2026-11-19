//! Definitions kept on a branch, and loaded from it.
//!
//! A document's concepts, macros and queries are written to the branch
//! it is submitted to, each compiled with every reference resolved, under
//! the entity its canonical form makes, with a source map beside it and
//! the bindings that put it under its name in the local namespace. A
//! notebook opened anew on the branch loads them and answers with
//! nothing submitted again.

#![allow(deprecated)]

use anyhow::Result;
use dialog_notation::definition::{self, Binding, Defined};
use dialog_notation::live::Live;
use dialog_notation::{Analyzer, Error, check};
use std::str::FromStr;

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
/// resolved to; and two bindings, the name in mine, the authority the
/// notebook's operator acts for, and the signature in the name's namespace.
#[dialog_common::test]
async fn it_writes_a_definition_its_source_map_and_its_bindings() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(DOCUMENT).await?;
    let kept = Live::new(notebook.branch.clone())
        .kept(&notebook.operator)
        .await?;
    let mine = notebook.analyzer().namespaces().mine().clone();
    assert!(mine.as_str().starts_with("did:"), "{mine}");
    let card = notebook
        .analyzer()
        .namespaces()
        .resolve("todo/card")
        .map_err(anyhow::Error::new)?;
    let [card] = card.as_slice() else {
        panic!("one todo/card: {card:?}");
    };
    assert_eq!(card.namespace, mine);
    assert_eq!(card.signature, "query (is:)");
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
    let named = definition::name_space(&mine, "todo/card")?;
    assert!(kept.bindings.contains(&Binding {
        namespace: mine.clone(),
        key: "todo/card".into(),
        referent: named.clone(),
    }));
    assert!(kept.bindings.contains(&Binding {
        namespace: named,
        key: "query (is:)".into(),
        referent: card.definition.clone(),
    }));
    // `Name` is derived on the branch, by a rule over the bindings.
    let names = notebook
        .submit(r#"(select (syntax.name ?e is: "todo/card" definition: ?d))"#)
        .await?
        .0;
    assert_eq!(
        names,
        vec![format!(
            r#"(syntax.name {} definition: {} is: "todo/card")"#,
            definition::binding_entity(
                &definition::name_space(&mine, "todo/card")?,
                "query (is:)"
            )?,
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
    let entities = |notebook: &Notebook, name: &str| -> Result<Vec<String>> {
        Ok(notebook
            .analyzer()
            .namespaces()
            .resolve(name)?
            .into_iter()
            .map(|row| row.definition.to_string())
            .collect())
    };
    assert_eq!(entities(&one, "todo")?, entities(&other, "todo")?);
    assert_eq!(entities(&one, "todo/card")?, entities(&other, "todo/card")?);
    let mine = |notebook: &Notebook| notebook.analyzer().namespaces().mine().clone();
    assert_ne!(mine(&one), mine(&other), "two authorities, two namespaces");
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

/// `(name cdata did:…)` binds a petname in mine, and `@cdata/…` walks
/// through it; `(unname todo)` takes `todo` away from mine, which is then
/// refused as unknown, and its definition stays kept.
#[dialog_common::test]
async fn it_binds_a_petname_and_takes_a_name_away() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(DOCUMENT).await?;
    let made = theirs(&mut notebook).await?;
    let mine = notebook.analyzer().namespaces().mine().clone();
    notebook
        .submit(&format!("(name ours {})", made.namespace))
        .await?;
    let live = Live::new(notebook.branch.clone());
    let kept = live.kept(&notebook.operator).await?;
    assert!(kept.bindings.contains(&Binding {
        namespace: mine.clone(),
        key: "ours".into(),
        referent: made.namespace.clone(),
    }));
    notebook
        .submit(r#"(assert (@ours/chore task:3 title: "Ours"))"#)
        .await?;
    assert_eq!(
        notebook
            .submit("(select (abc.task task:3 name: ?x))")
            .await?
            .0,
        vec![r#"(abc.task task:3 name: "Ours")"#]
    );

    let card = kept
        .bindings
        .iter()
        .find(|binding| binding.namespace == mine && binding.key == "todo/card")
        .cloned()
        .expect("todo/card is bound");
    notebook.submit("(unname todo/card)").await?;
    let after = live.kept(&notebook.operator).await?;
    assert!(!after.bindings.contains(&card), "the binding is retracted");
    assert_eq!(after.definitions, kept.definitions, "the definitions stay");
    let refused = notebook
        .submit(ASKED[0])
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&refused, Err(Ok(Error::Refused(why))) if why.contains("applies no relation")),
        "{refused:?}"
    );
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

/// A definition of another namespace, as a branch both write to would
/// hold it: `chore`, which mine does not define, and `theirs` in mine
/// bound to their namespace.
async fn theirs(notebook: &mut Notebook) -> Result<Defined> {
    let theirs = dialog_corpus::Entity::from_str("did:key:theirs")?;
    let mut analyzer = Analyzer::new()?.with_namespace(theirs.clone());
    let checked = check(
        &mut analyzer,
        "(concept (chore ?t title: ?title) (abc.task/name of: ?t is: ?title as: :text))",
    );
    let made = checked.defined[0].clone();
    let mut transaction = notebook.branch.transaction();
    for statement in definition::statements(&made)? {
        transaction = transaction.assert(statement);
    }
    transaction = transaction.assert(definition::binding_statement(&Binding {
        namespace: notebook.analyzer().namespaces().mine().clone(),
        key: "theirs".into(),
        referent: theirs,
    })?);
    transaction
        .commit()
        .publish()
        .perform(&notebook.operator)
        .await?;
    notebook.reload().await?;
    Ok(made)
}

/// A bare name is mine and nothing else. One mine does not hold is
/// refused, naming the `@theirs/chore` that is, and nothing of the
/// document is written; written `@theirs/chore`, it is theirs.
#[dialog_common::test]
async fn it_refuses_a_bare_name_that_is_not_mine_and_writes_nothing() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(DOCUMENT).await?;
    let made = theirs(&mut notebook).await?;
    let live = Live::new(notebook.branch.clone());
    let kept = live.kept(&notebook.operator).await?;
    let revision = notebook.branch.revision();
    let refused = notebook
        .submit(
            r#"(assert (todo todo:2 title: "Call mum"))
               (assert (chore task:2 title: "Mine?"))"#,
        )
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&refused, Err(Ok(Error::Elsewhere(_)))),
        "{refused:?}"
    );
    let Err(Ok(Error::Elsewhere(elsewhere))) = &refused else {
        unreachable!()
    };
    assert_eq!(elsewhere.name, "chore");
    let [suggestion] = elsewhere.suggestions.as_slice() else {
        panic!("one suggestion: {elsewhere:?}");
    };
    assert_eq!(suggestion.to_string(), "@theirs/chore (concept (title:))");
    assert_eq!(suggestion.definition, made.definition.entity);
    assert_eq!(notebook.branch.revision(), revision, "nothing is written");
    assert_eq!(live.kept(&notebook.operator).await?, kept);

    notebook
        .submit(r#"(assert (@theirs/chore task:1 title: "Theirs"))"#)
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
            .namespaces()
            .bindings()
            .map(|binding| binding.key)
            .collect();
        // Mine's own keys are listed, to find its petnames; the spares'
        // signatures, and so their definitions, are never read.
        assert!(
            !known.iter().any(|key| key == "concept (text:)"),
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
        let mine = notebook.analyzer().namespaces().mine().clone();
        let mut writer = notebook.analyzer().clone();
        let checked = check(
            &mut writer,
            "(query (todo/badge ?t is: ?badge) (todo/label ?t is: ?label) (is ?badge {:label ?label}))",
        );
        let [badge] = checked.defined.as_slice() else {
            panic!("one definition: {:?}", checked.diagnostics);
        };
        assert_eq!(badge.namespace, mine);
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
/// bare name only a petname's namespace holds is refused naming it,
/// both read from the branch by an analyzer that held nothing.
#[dialog_common::test]
async fn it_refuses_what_the_rounds_do_not_find() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(DOCUMENT).await?;
    theirs(&mut notebook).await?;
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
    let elsewhere = notebook
        .submit("(select (chore ?t title: ?x))")
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&elsewhere, Err(Ok(Error::Elsewhere(_)))),
        "{elsewhere:?}"
    );
    Ok(())
}

/// What the suggestions read from the branch are: the names that start
/// so, in mine and where a petname of mine points.
#[dialog_common::test]
async fn it_suggests_names_read_from_the_branch() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(DOCUMENT).await?;
    theirs(&mut notebook).await?;
    let mine = notebook.analyzer().namespaces().mine().clone();
    let live = Live::new(notebook.branch.clone());
    let said: Vec<String> = live
        .suggest(&mine, "", &notebook.operator)
        .await?
        .iter()
        .map(ToString::to_string)
        .collect();
    assert!(
        said.contains(&"@theirs/chore (concept (title:))".to_string()),
        "{said:?}"
    );
    assert!(
        said.contains(&"todo/card (query (is:))".to_string()),
        "{said:?}"
    );
    let todo: Vec<String> = live
        .suggest(&mine, "todo/c", &notebook.operator)
        .await?
        .iter()
        .map(ToString::to_string)
        .collect();
    assert_eq!(todo, vec!["todo/card (query (is:))"]);
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

/// Mine is the authority the branch is opened under: the one every
/// revision the notebook commits names as its authority.
#[dialog_common::test]
async fn it_binds_in_the_authority_the_branch_is_opened_under() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(DOCUMENT).await?;
    let mine = notebook.analyzer().namespaces().mine().clone();
    assert_eq!(
        Live::new(notebook.branch.clone())
            .authority(&notebook.operator)
            .await?,
        mine
    );
    use dialog_query::query::Output as _;
    use dialog_query::{Query, Term};
    use dialog_repository::schema;
    let revisions: Vec<schema::Revision> = notebook
        .branch
        .query()
        .select(Query::<schema::Revision> {
            this: Term::var("this"),
            branch: Term::var("branch"),
            issuer: Term::var("issuer"),
            authority: Term::var("authority"),
            edition: Term::var("edition"),
        })
        .perform(&notebook.operator)
        .try_vec()
        .await?;
    assert!(!revisions.is_empty());
    assert!(
        revisions
            .iter()
            .all(|revision| revision.authority.0 == mine),
        "the revisions' authority is mine"
    );
    Ok(())
}
