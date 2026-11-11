//! Documents in the notation, run on a real branch.
//!
//! Each test opens a repository, submits documents, and reads the
//! report back. Nothing here stands in for Dialog-DB: a concept
//! application is a concept query, a rule is a rule the branch stores
//! and fires, and a command is a dispatched transient.

use anyhow::Result;
use dialog_notation::Error;

#[path = "../examples/notebook/session.rs"]
mod session;
use session::Notebook;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// The report of a document.
async fn report(notebook: &mut Notebook, text: &str) -> Result<Vec<String>> {
    Ok(notebook.submit(text).await?.0)
}

const TODO: &str = r#"
    (concept (todo ?t list: ?l text: ?x state: ?s)
      (xyz.todo ?t list: (:last (:entity ?l))
                   text: (:last (:text ?x))
                   state: (:last (:entity ?s)))
      description: "Something to do, on a list")

    (rule (assert! (todo ?t list: ?l text: ?x state: :open))
      (when (add (todo ?t list: ?l text: ?x)))
      description: "Adding a to-do opens it")

    (rule (assert! (todo ?t state: :done))
      (when (complete (todo ?t))))
"#;

/// A concept is defined, written through by a rule a command fires, and
/// read back through the same concept.
#[dialog_common::test]
async fn it_defines_a_concept_and_runs_a_command_through_it() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook.submit(TODO).await?;
    assert!(diagnostics.is_empty());

    notebook
        .submit(
            r#"(add (todo todo:1 list: list:home text: "Buy milk"))
               (add (todo todo:2 list: list:home text: "Call mum"))
               (add (todo todo:3 list: list:work text: "Ship it"))"#,
        )
        .await?;

    assert_eq!(
        report(&mut notebook, "(query (todo ?t text: ?x state: ?s))").await?,
        vec![
            r#"(xyz.todo todo:1 text: "Buy milk" state: :open)"#,
            r#"(xyz.todo todo:2 text: "Call mum" state: :open)"#,
            r#"(xyz.todo todo:3 text: "Ship it" state: :open)"#,
        ]
    );

    notebook.submit("(complete (todo todo:2))").await?;
    assert_eq!(
        report(
            &mut notebook,
            "(query (todo ?t text: ?x list: list:home state: :open))"
        )
        .await?,
        vec![r#"(xyz.todo todo:1 list: list:home text: "Buy milk" state: :open)"#],
        "a row has the order the concept gives its fields"
    );

    // The command held for one round and is not on the branch.
    assert!(
        report(&mut notebook, "(query (command.add.todo ?c of: ?t))")
            .await?
            .is_empty()
    );
    Ok(())
}

/// `increment` is defined nowhere but in the rule that reads it. The
/// count is kept with `assert!`, so it stays.
#[dialog_common::test]
async fn it_counts_with_a_command_a_rule_defines_by_reading_it() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook
        .submit(
            "(assert! (xyz.counter counter:a count: (:last (:natural 1))))
             (rule (assert! (xyz.counter ?c count: (:last (:natural (+ ?n : 1)))))
               (when (increment (xyz.counter ?c))
                     (xyz.counter ?c count: (:last (:natural ?n)))))",
        )
        .await?;
    assert!(diagnostics.is_empty());

    notebook
        .submit("(increment (xyz.counter counter:a))")
        .await?;
    notebook
        .submit("(increment (xyz.counter counter:a))")
        .await?;
    assert_eq!(
        report(&mut notebook, "(query (xyz.counter ?c count: (:last ?n)))").await?,
        vec!["(xyz.counter counter:a count: 3)"]
    );

    // A command no rule reads is issued all the same, and said so.
    let (_, diagnostics) = notebook
        .submit("(incremnt (xyz.counter counter:a))")
        .await?;
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0].message, "no rule reads this command");
    assert_eq!(
        report(&mut notebook, "(query (xyz.counter ?c count: (:last ?n)))").await?,
        vec!["(xyz.counter counter:a count: 3)"]
    );
    Ok(())
}

/// A concept need not be about the entity its facts are of: a list's
/// items are the to-dos that name it. It reads from either side, and a
/// write through it lands on the to-do.
#[dialog_common::test]
async fn it_reads_and_writes_a_backlink_through_a_concept() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (list-item ?l todo: ?t)
                 (xyz.todo ?t list: (:last (:entity ?l))))
               (assert! (list-item list:home todo: todo:1))
               (assert! (list-item list:home todo: todo:2))
               (assert! (list-item list:work todo: todo:3))"#,
        )
        .await?;

    assert_eq!(
        report(&mut notebook, "(query (list-item list:home todo: ?t))").await?,
        vec![
            "(xyz.todo todo:1 list: list:home)",
            "(xyz.todo todo:2 list: list:home)",
        ]
    );
    assert_eq!(
        report(&mut notebook, "(query (list-item ?l todo: todo:3))").await?,
        vec!["(xyz.todo todo:3 list: list:work)"]
    );
    Ok(())
}

/// A concept of two reads: an author has a name in one domain and an
/// address in another. Both must hold for the concept to, and its
/// absence is the absence of the two together.
#[dialog_common::test]
async fn it_joins_the_reads_a_concept_holds_and_negates_them_as_one() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (author ?p name: ?n email: ?e)
                 (io.person ?p name: (:text ?n))
                 (io.contact ?p email: (:text ?e)))
               (assert! (author person:ada name: "Ada" email: "ada@example.com"))
               (assert! (io.person person:bob name: (:text "Bob")))"#,
        )
        .await?;

    assert_eq!(
        report(&mut notebook, "(query (author ?p name: ?n))").await?,
        vec![r#"(io.person person:ada name: "Ada")"#],
        "a read the query names no field of says nothing in the report"
    );
    assert_eq!(
        report(
            &mut notebook,
            "(query (io.person ?p name: ?n)
                   (when (io.person ?p name: (:text ?n)))
                   (unless (author ?p)))"
        )
        .await?,
        vec![r#"(io.person person:bob name: "Bob")"#]
    );
    Ok(())
}

/// A rule with a bare head derives: nothing is stored, and the relation
/// holds while its premises do. A query of several premises keeps the
/// values of a row together.
#[dialog_common::test]
async fn it_derives_a_relation_and_reports_rows_of_several_premises() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(assert! (shop.item item:tea name: (:text "Tea") price: (:natural 4)))
               (assert! (shop.item item:pot name: (:text "Pot") price: (:natural 30)))
               (assert! (shop.item item:cup name: (:text "Cup") price: (:natural 9)))

               (rule (shop.offer ?i name: ?n price: ?p)
                 (when (shop.item ?i name: (:text ?n) price: (:natural ?p))
                       (< ?p : 10))
                 description: "What costs less than ten")"#,
        )
        .await?;

    assert_eq!(
        report(&mut notebook, "(query (shop.offer ?i name: ?n))").await?,
        vec![
            r#"(shop.offer item:cup name: "Cup")"#,
            r#"(shop.offer item:tea name: "Tea")"#,
        ]
    );
    assert_eq!(
        report(
            &mut notebook,
            "(query (shop.pair ?a with: ?b total: (+ ?p : ?q))
                   (when (shop.offer ?a price: ?p)
                         (shop.offer ?b price: ?q)
                         (< ?p : ?q)))"
        )
        .await?,
        vec!["(shop.pair item:tea with: item:cup total: 13)"]
    );
    Ok(())
}

/// One rule, two heads of different kinds: moving a to-do writes where
/// it is and takes back where it was.
#[dialog_common::test]
async fn it_asserts_and_retracts_from_one_rule() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(rule (assert! (xyz.todo ?t list: ?to))
                     (retract! (xyz.todo ?t list: ?from))
                 (when (move (xyz.todo ?t list: ?to))
                       (xyz.todo ?t list: ?from)))
               (assert! (xyz.todo todo:1 list: list:home))"#,
        )
        .await?;
    notebook
        .submit("(move (xyz.todo todo:1 list: list:work))")
        .await?;
    assert_eq!(
        report(&mut notebook, "(query (xyz.todo ?t list: ?l))").await?,
        vec!["(xyz.todo todo:1 list: list:work)"]
    );
    Ok(())
}

/// A relation derived from itself: two rules of one head, the second
/// reading what both conclude.
#[dialog_common::test]
async fn it_derives_a_relation_from_itself() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            "(assert! (fam.parent person:ann of: person:bea))
             (assert! (fam.parent person:bea of: person:cal))
             (assert! (fam.parent person:cal of: person:dee))

             (rule (fam.ancestor ?a of: ?c)
               (when (fam.parent ?a of: ?c)))
             (rule (fam.ancestor ?a of: ?c)
               (when (fam.parent ?a of: ?b)
                     (fam.ancestor ?b of: ?c)))",
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(query (fam.ancestor ?a of: person:dee))").await?,
        vec![
            "(fam.ancestor person:ann of: person:dee)",
            "(fam.ancestor person:bea of: person:dee)",
            "(fam.ancestor person:cal of: person:dee)",
        ]
    );
    Ok(())
}

/// A document that installs a rule and issues the command it reads:
/// the rule fires in the transaction that installs it.
#[dialog_common::test]
async fn it_fires_a_rule_in_the_document_that_installs_it() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (report, diagnostics) = notebook
        .submit(
            "(assert! (xyz.counter counter:a count: (:last (:natural 1))))
             (rule (assert! (xyz.counter ?c count: (:last (:natural (+ ?n : 1)))))
               (when (increment (xyz.counter ?c))
                     (xyz.counter ?c count: (:last (:natural ?n)))))
             (increment (xyz.counter counter:a))
             (query (xyz.counter ?c count: (:last ?n)))",
        )
        .await?;
    assert!(diagnostics.is_empty());
    assert_eq!(report, vec!["(xyz.counter counter:a count: 2)"]);
    Ok(())
}

/// What the notation allows and Dialog-DB cannot yet run is said, not
/// approximated.
#[dialog_common::test]
async fn it_says_what_is_not_supported_yet() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(TODO).await?;
    let issued_by_a_rule = notebook
        .submit(
            "(rule (complete (todo ?t))
               (when (archive (xyz.list ?l)) (todo ?t list: ?l)))",
        )
        .await;
    assert!(matches!(
        issued_by_a_rule.map_err(|error| error.downcast::<Error>()),
        Err(Ok(Error::Unsupported(_)))
    ));
    Ok(())
}
