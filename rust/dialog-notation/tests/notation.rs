//! Documents in the notation, run on a real branch.
//!
//! Each test opens a repository, submits documents, and reads the
//! report back. Nothing here stands in for Dialog-DB: a concept
//! application is a concept query, a rule is a rule the branch stores
//! and fires, and a command is a dispatched transient.

use anyhow::Result;
use dialog_corpus::Value;
use dialog_notation::{Error, Syntax, read};
use dialog_query::concept::descriptor::ConceptStatement;
use dialog_query::query::Output as _;
use dialog_query::{Any, AttributeDescriptor, ConceptConclusion, ConceptQuery, Term};
use serde_json::json;

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

    ; A write says every field, so one that changes the state alone
    ; writes the relation, not the concept.
    (rule (assert! (xyz.todo ?t state: (:last (:entity :done))))
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
        report(&mut notebook, "(query (command.add.todo ?c about: ?t))")
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
    // What must not hold may stand inside `when`: the same rule.
    assert_eq!(
        report(
            &mut notebook,
            "(query (io.person ?p name: ?n)
               (when (io.person ?p name: (:text ?n))
                     (unless (author ?p))))"
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
            "(assert! (fam.parent person:ann child: person:bea))
             (assert! (fam.parent person:bea child: person:cal))
             (assert! (fam.parent person:cal child: person:dee))

             (rule (fam.ancestor ?a descendant: ?c)
               (when (fam.parent ?a child: ?c)))
             (rule (fam.ancestor ?a descendant: ?c)
               (when (fam.parent ?a child: ?b)
                     (fam.ancestor ?b descendant: ?c)))",
        )
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(query (fam.ancestor ?a descendant: person:dee))"
        )
        .await?,
        vec![
            "(fam.ancestor person:ann descendant: person:dee)",
            "(fam.ancestor person:bea descendant: person:dee)",
            "(fam.ancestor person:cal descendant: person:dee)",
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

/// A rule issues a command, which another rule reads in the round
/// after: archiving a list completes what is on it.
#[dialog_common::test]
async fn it_lets_a_rule_issue_a_command_another_rule_reads() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook
        .submit(&format!(
            "{TODO}
             (rule (complete (todo ?t))
               (when (archive (xyz.list ?l))
                     (todo ?t list: ?l)))"
        ))
        .await?;
    assert!(diagnostics.is_empty());
    notebook
        .submit(
            r#"(add (todo todo:1 list: list:home text: "Buy milk"))
               (add (todo todo:2 list: list:home text: "Call mum"))
               (add (todo todo:3 list: list:work text: "Ship it"))"#,
        )
        .await?;

    notebook.submit("(archive (xyz.list list:home))").await?;
    assert_eq!(
        report(&mut notebook, "(query (todo ?t text: ?x state: :open))").await?,
        vec![r#"(xyz.todo todo:3 text: "Ship it" state: :open)"#]
    );
    // Neither command is on the branch.
    assert!(
        report(
            &mut notebook,
            "(query (command.complete.todo ?c about: ?t))"
        )
        .await?
        .is_empty()
    );
    Ok(())
}

/// An entity made from what a rule binds is the entity the same values
/// make anywhere: a rule and a write at the top level name one note.
#[dialog_common::test]
async fn it_derives_one_entity_from_the_same_values() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(rule (assert! (xyz.note (:id xyz.note text: ?x) text: (:text ?x)))
                 (when (jot (xyz.note text: ?x))))"#,
        )
        .await?;
    notebook.submit(r#"(jot (xyz.note text: "hi"))"#).await?;
    notebook
        .submit(r#"(assert! (xyz.note (:id xyz.note text: "hi") text: (:text "hi")))"#)
        .await?;
    notebook.submit(r#"(jot (xyz.note text: "ho"))"#).await?;

    let notes = report(&mut notebook, "(query (xyz.note ?n text: ?x))").await?;
    assert_eq!(notes.len(), 2, "{notes:?}");
    assert!(notes.iter().all(|note| note.starts_with("(xyz.note id:")));
    Ok(())
}

/// Two commands in one document share a round, so what they write to
/// one `last` field is concurrent. The field elects one, and which one
/// does not depend on the order the commands were written in.
#[dialog_common::test]
async fn it_elects_one_of_two_writes_of_one_round_whatever_their_order() -> Result<()> {
    let add = r#"(add (todo todo:1 list: list:home text: "Buy milk"))"#;
    let complete = "(complete (todo todo:1))";
    let mut elected = Vec::new();
    for document in [[add, complete], [complete, add]] {
        let mut notebook = Notebook::open().await?;
        notebook.submit(TODO).await?;
        notebook.submit(&document.join("\n")).await?;
        assert_eq!(
            report(&mut notebook, "(query (xyz.todo ?t state: ?s))").await?,
            vec![
                "(xyz.todo todo:1 state: :done)",
                "(xyz.todo todo:1 state: :open)",
            ],
            "both writes are in the cell"
        );
        let last = report(&mut notebook, "(query (xyz.todo ?t state: (:last ?s)))").await?;
        assert_eq!(last.len(), 1, "one of them is elected: {last:?}");
        elected.push(last);
    }
    assert_eq!(elected[0], elected[1]);
    Ok(())
}

/// What a name with two referents can be read as today. Written under
/// `last`, a referent written after another replaces it, and two written
/// in one round, which is what two replicas writing apart come to, both
/// stand. Read with no pick, the first is one row and the second is two.
/// So a name that was re-pointed and a name two definitions claim at
/// once are told apart by what is already stored.
#[dialog_common::test]
async fn it_tells_a_name_that_was_repointed_from_one_claimed_twice() -> Result<()> {
    let rules = "(rule (assert! (xyz.name ?n referent: (:last (:entity concept:a))))
                   (when (first (xyz.name ?n))))
                 (rule (assert! (xyz.name ?n referent: (:last (:entity concept:b))))
                   (when (second (xyz.name ?n))))";
    let first = "(first (xyz.name name:todo))";
    let second = "(second (xyz.name name:todo))";
    let every = "(query (xyz.name ?n referent: ?r))";

    let mut repointed = Notebook::open().await?;
    repointed.submit(rules).await?;
    repointed.submit(first).await?;
    repointed.submit(second).await?;
    assert_eq!(
        report(&mut repointed, every).await?,
        vec!["(xyz.name name:todo referent: concept:b)"]
    );

    let mut twice = Notebook::open().await?;
    twice.submit(rules).await?;
    twice.submit(&format!("{first}\n{second}")).await?;
    assert_eq!(
        report(&mut twice, every).await?,
        vec![
            "(xyz.name name:todo referent: concept:a)",
            "(xyz.name name:todo referent: concept:b)",
        ]
    );
    assert_eq!(
        report(&mut twice, "(query (xyz.name ?n referent: (:last ?r)))")
            .await?
            .len(),
        1
    );
    Ok(())
}

/// One call, two ways to write it: the primary with no label, and under
/// `of:`. A concept and a relation read the same through each, and a
/// write lands the same. A struct in the primary's place is the primary,
/// a value, and not the labelled parts written another way.
#[dialog_common::test]
async fn it_reads_a_call_the_same_however_its_primary_is_written() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(TODO).await?;
    notebook
        .submit(
            r#"(add (todo todo:1 list: list:home text: "Buy milk"))
               (assert! (xyz.note text: (:text "by label") of: note:1))
               (assert! (xyz.note note:2 text: (:text "by position")))"#,
        )
        .await?;

    let milk = vec![r#"(xyz.todo todo:1 text: "Buy milk")"#];
    for query in [
        "(query (todo ?t text: ?x))",
        "(query (todo text: ?x of: ?t))",
    ] {
        assert_eq!(report(&mut notebook, query).await?, milk, "{query}");
    }
    assert_eq!(
        report(&mut notebook, "(query (xyz.note of: ?n text: ?x))").await?,
        vec![
            r#"(xyz.note note:1 text: "by label")"#,
            r#"(xyz.note note:2 text: "by position")"#,
        ]
    );

    let record = notebook
        .submit(r#"(assert! (xyz.note {of: note:3 text: (:text "by record")}))"#)
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(record, Err(Ok(Error::Unsupported(_)))),
        "{record:?}"
    );

    // Beside a part with no label, `of:` would be the primary twice.
    let twice = notebook
        .submit("(query (xyz.note ?n of: ?m))")
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(matches!(twice, Err(Ok(Error::Refused(_)))));
    Ok(())
}

/// A form is held by a branch today: as a `record:` value, under the
/// entity its reference names. What comes back reads as the form that
/// went in, and names the same entity.
#[dialog_common::test]
async fn it_keeps_a_form_on_a_branch_under_the_entity_that_names_it() -> Result<()> {
    let notebook = Notebook::open().await?;
    let form = read(TODO)?.remove(0);
    let syntax = Syntax::try_from(&form)?;
    let entity = syntax.entity()?;

    let is: AttributeDescriptor =
        serde_json::from_value(json!({ "the": "syntax.form/is", "as": "record:" }))?;
    notebook
        .branch
        .transaction()
        .assert(ConceptStatement {
            this: entity.clone(),
            with: vec![is.resolve(Value::Record(syntax.encode()?))?],
        })
        .commit()
        .publish()
        .perform(&notebook.operator)
        .await?;
    notebook.branch.refresh(&notebook.operator).await?;

    let select: ConceptQuery = serde_json::from_value(json!({
        "assert": { "with": { "is": { "the": "syntax.form/is", "as": "record:" } } },
        "where": { "this": { "entity:": entity.to_string() }, "is": { "?": { "name": "is" } } }
    }))?;
    let found: Vec<ConceptConclusion> = notebook
        .branch
        .query()
        .select(select)
        .perform(&notebook.operator)
        .try_vec()
        .await?;
    assert_eq!(found.len(), 1);
    let Value::Record(bytes) = found[0]
        .source()
        .lookup(&Term::<Any>::var("is"))?
        .content()?
    else {
        panic!("a record");
    };
    let kept = Syntax::decode(&bytes)?;
    assert_eq!(kept, syntax);
    assert_eq!(kept.entity()?, entity);
    assert_eq!(read(&kept.to_string())?[0].reference()?, form.reference()?);
    Ok(())
}

/// What a form as a record rests on: a symbol with no domain is a value
/// today, a variable or an operator spelled as one included, and it is
/// not an attribute a claim can be made under.
#[dialog_common::test]
async fn it_holds_a_symbol_with_no_domain_as_a_value_and_not_as_an_attribute() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit("(assert! (xyz.form form:1 head: /todo var: /?x op: /+))")
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(query (xyz.form ?f head: ?h var: ?v op: ?o))"
        )
        .await?,
        vec!["(xyz.form form:1 head: /todo var: /?x op: /+)"]
    );

    let claim = notebook
        .submit(r#"(assert! (xyz.form form:1 /text: "x"))"#)
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(matches!(claim, Err(Ok(Error::Refused(_)))));
    Ok(())
}

/// What is not allowed is refused, and what Dialog-DB cannot yet run is
/// said, not approximated.
#[dialog_common::test]
async fn it_refuses_and_says_what_is_not_supported() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(TODO).await?;
    let why = |result: Result<(Vec<String>, Vec<dialog_notation::Diagnostic>)>| {
        result.map_err(|error| error.downcast::<Error>())
    };

    // A write through a concept says every field.
    let partial = notebook
        .submit("(rule (assert! (todo ?t state: :done)) (when (finish (todo ?t))))")
        .await;
    assert!(matches!(why(partial), Err(Ok(Error::Refused(_)))));

    // A variable in `unless` is one `when` binds.
    let unbound = notebook
        .submit(
            "(query (xyz.todo ?t text: ?x)
               (when (xyz.todo ?t text: ?x))
               (unless (xyz.todo ?t list: ?l)))",
        )
        .await;
    assert!(matches!(why(unbound), Err(Ok(Error::Refused(_)))));

    // A formula inside `unless` has nowhere to go.
    let formula = notebook
        .submit(
            "(query (xyz.counter ?c count: ?n)
               (when (xyz.counter ?c count: ?n))
               (unless (xyz.counter ?c count: (+ ?n : 1))))",
        )
        .await;
    assert!(matches!(why(formula), Err(Ok(Error::Unsupported(_)))));
    Ok(())
}
