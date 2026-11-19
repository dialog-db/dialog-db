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
            4,
            "the concept, the macro and the two queries are loaded"
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
/// resolved to; and two bindings, the name in mine, the profile the
/// branch is read under, and the signature in the name's namespace.
#[dialog_common::test]
async fn it_writes_a_definition_its_source_map_and_its_bindings() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(DOCUMENT).await?;
    let kept = Live::new(notebook.branch.clone())
        .kept(&notebook.operator)
        .await?;
    assert!(kept.mine.as_str().starts_with("did:"), "{}", kept.mine);
    let card = notebook
        .analyzer()
        .namespaces()
        .resolve("todo/card")
        .map_err(anyhow::Error::new)?;
    let [card] = card.as_slice() else {
        panic!("one todo/card: {card:?}");
    };
    assert_eq!(card.namespace, kept.mine);
    assert_eq!(card.signature, "query (is:)");
    let stored = kept
        .definitions
        .iter()
        .find(|kept| kept.entity == card.definition)
        .expect("the query is kept");
    assert_eq!(stored.kind, definition::Kind::Query);
    // Kept with no name: its own is `@`, and the concept it reads is
    // written out as the attribute it is.
    let form = stored.form.to_string();
    assert!(form.contains("(@ ?t is: ?card)"), "{form}");
    assert!(
        !form.contains("todo/card") && !form.contains("(todo "),
        "{form}"
    );
    assert!(form.contains("xyz.todo"), "{form}");
    let named = definition::name_space(&kept.mine, "todo/card")?;
    assert!(kept.bindings.contains(&Binding {
        namespace: kept.mine.clone(),
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
                &definition::name_space(&kept.mine, "todo/card")?,
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
    assert_ne!(mine(&one), mine(&other), "two profiles, two namespaces");
    Ok(())
}

/// A pull brings what another branch defined, and the analyzer, told to
/// take what the branch now keeps, answers by it.
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
    other.refreshed().await?;
    assert_eq!(other.submit(ASKED[0]).await?.0, vec![ANSWERED[0]]);
    Ok(())
}

/// A definition of another namespace, as a branch both write to would
/// hold it: `todo` of the same labels as mine, over other attributes.
async fn theirs(notebook: &mut Notebook) -> Result<Defined> {
    let mut analyzer =
        Analyzer::new()?.with_namespace(dialog_corpus::Entity::from_str("did:key:theirs")?);
    let checked = check(
        &mut analyzer,
        "(concept (todo ?t title: ?title) (abc.task/name of: ?t is: ?title as: :text))",
    );
    let made = checked.defined[0].clone();
    let mut transaction = notebook.branch.transaction();
    for statement in definition::statements(&made)? {
        transaction = transaction.assert(statement);
    }
    transaction
        .commit()
        .publish()
        .perform(&notebook.operator)
        .await?;
    notebook.reload().await?;
    Ok(made)
}

/// `todo` alone is every namespace's: two definitions of one signature,
/// mine and another's, are refused, both listed, and nothing of the
/// document is written. `@theirs/todo`, once `theirs` in mine names
/// their namespace, is theirs alone.
#[dialog_common::test]
async fn it_refuses_a_name_two_namespaces_define_and_writes_nothing() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(DOCUMENT).await?;
    let made = theirs(&mut notebook).await?;
    let live = Live::new(notebook.branch.clone());
    let kept = live.kept(&notebook.operator).await?;
    let revision = notebook.branch.revision();
    let refused = notebook
        .submit(
            r#"(assert (todo todo:2 title: "Call mum"))
               (concept (card ?c is: ?x) (xyz.card/is of: ?c is: ?x))"#,
        )
        .await
        .map_err(|error| error.downcast::<Error>());
    let Err(Ok(Error::Several(several))) = &refused else {
        panic!("refused as several: {refused:?}");
    };
    assert!(matches!(&refused, Err(Ok(Error::Several(_)))));
    assert_eq!(several.name, "todo");
    let mut listed: Vec<&str> = several
        .candidates
        .iter()
        .map(|(namespace, _)| namespace.as_str())
        .collect();
    listed.sort();
    let mut namespaces = vec!["did:key:theirs", kept.mine.as_str()];
    namespaces.sort();
    assert_eq!(listed, namespaces);
    assert!(
        several
            .candidates
            .iter()
            .any(|(_, definition)| definition == made.definition.entity.as_str())
    );
    assert_eq!(notebook.branch.revision(), revision, "nothing is written");
    assert_eq!(live.kept(&notebook.operator).await?, kept);

    // A petname says whose: `theirs` in mine, bound to their namespace.
    let mut transaction = notebook.branch.transaction();
    transaction = transaction.assert(definition::binding_statement(&Binding {
        namespace: kept.mine.clone(),
        key: "theirs".into(),
        referent: dialog_corpus::Entity::from_str("did:key:theirs")?,
    })?);
    transaction
        .commit()
        .publish()
        .perform(&notebook.operator)
        .await?;
    notebook.reload().await?;
    notebook
        .submit(r#"(assert (@theirs/todo task:1 title: "Theirs"))"#)
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
