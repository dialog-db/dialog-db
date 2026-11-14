//! Documents in the notation, run on a real branch.
//!
//! Each test opens a repository, submits documents, and reads the
//! report back. Nothing here stands in for Dialog-DB: a concept
//! application is a concept query, a rule is a rule the branch stores
//! and fires, and a command is a dispatched transient.

use anyhow::Result;
use dialog_corpus::Value;
use dialog_notation::{Error, Signature, Syntax, read};
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
                   state: (:last (:entity ?s))))

    ; Adding a to-do opens it.
    (rule (assert! (todo ?t list: ?l text: ?x state: :open))
      (add (todo ?t list: ?l text: ?x)))

    ; A write says every field, so one that changes the state alone
    ; writes the relation, not the concept.
    (rule (assert! (xyz.todo ?t state: (:last (:entity :done))))
      (complete (todo ?t)))
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
        report(&mut notebook, "(query (todo ?t text: ?x state: ?s ...))").await?,
        vec![
            r#"(xyz.todo todo:1 state: :open text: "Buy milk")"#,
            r#"(xyz.todo todo:2 state: :open text: "Call mum")"#,
            r#"(xyz.todo todo:3 state: :open text: "Ship it")"#,
        ]
    );

    notebook.submit("(complete (todo todo:2))").await?;
    assert_eq!(
        report(
            &mut notebook,
            "(query (todo ?t text: ?x list: list:home state: :open))"
        )
        .await?,
        vec![r#"(xyz.todo todo:1 list: list:home state: :open text: "Buy milk")"#],
        "a row lists its fields by label: a tree keeps no order of them"
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
               (increment (xyz.counter ?c))
               (xyz.counter ?c count: (:last (:natural ?n))))",
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
        report(&mut notebook, "(query (author ?p name: ?n ...))").await?,
        vec![r#"(io.person person:ada name: "Ada")"#],
        "a read the query names no field of says nothing in the report"
    );
    assert_eq!(
        report(
            &mut notebook,
            "(query (io.person ?p name: ?n)
                   (io.person ?p name: (:text ?n))
                   (unless (author ?p ...)))"
        )
        .await?,
        vec![r#"(io.person person:bob name: "Bob")"#]
    );
    // What must not hold may stand anywhere among the premises: the
    // same rule.
    assert_eq!(
        report(
            &mut notebook,
            "(query (io.person ?p name: ?n)
               (io.person ?p name: (:text ?n))
               (unless (author ?p ...)))"
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

               ; What costs less than ten.
               (rule (shop.offer ?i name: ?n price: ?p)
                 (shop.item ?i name: (:text ?n) price: (:natural ?p))
                 (< ?p : 10))"#,
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
                   (shop.offer ?a price: ?p)
                   (shop.offer ?b price: ?q)
                   (< ?p : ?q))"
        )
        .await?,
        vec!["(shop.pair item:tea total: 13 with: item:cup)"]
    );
    Ok(())
}

/// A warning and what the compiler refuses both say where in the text
/// they are about.
#[dialog_common::test]
async fn it_says_where_a_warning_and_a_refusal_are() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let (_, diagnostics) = notebook
        .submit("(assert! (xyz.todo todo:1 text: \"x\"))\n  (ad (xyz.todo todo:1 text: \"y\"))")
        .await?;
    let [warning] = diagnostics.as_slice() else {
        panic!("one warning: {diagnostics:?}");
    };
    let span = warning.span.expect("the warning has a place");
    assert_eq!((span.start.line, span.start.column), (1, 2));
    assert_eq!((span.end.line, span.end.column), (1, 34));

    let refused = notebook
        .submit("\n\n  (query (xyz.a ?a n: ?n) (xyz.a ?a n: ?n) (:like ?n : \"x\"))")
        .await
        .expect_err("the compiler has no counterpart for it");
    assert!(
        refused.to_string().contains("line 3, column 3"),
        "{refused:#}"
    );
    assert!(matches!(
        refused.downcast::<Error>(),
        Ok(Error::Unsupported(_))
    ));

    // A refusal by the analyzer says where the part it is about is.
    let refused = notebook
        .submit("(rule (xyz.seen ?a n: ?n)\n  (xyz.a ?a n: ?n)\n  (frobnicate ?a))")
        .await
        .expect_err("nothing says what frobnicate is");
    assert!(
        refused.to_string().contains("line 3, column 3"),
        "{refused:#}"
    );
    assert!(matches!(refused.downcast::<Error>(), Ok(Error::Unknown(_))));
    Ok(())
}

/// What the comment above a rule says is kept as a fact about the rule,
/// so a rule is found by what it is for.
#[dialog_common::test]
async fn it_finds_a_rule_by_what_the_comment_above_it_says() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"; How a shop prices things.

               ; What costs less than ten.
               (rule (shop.offer ?i name: ?n price: ?p)
                 (shop.item ?i name: (:text ?n) price: (:natural ?p))
                 (< ?p : 10))

               ; Restocking sets the count.
               (rule (assert! (shop.stock ?i count: (:last (:natural ?n))))
                 (restock (shop.item ?i count: ?n)))

               (rule (shop.dear ?i name: ?n)
                 (shop.item ?i name: (:text ?n) price: (:natural ?p))
                 (> ?p : 100))"#,
        )
        .await?;
    let said = report(
        &mut notebook,
        r#"(query (syntax.meta ?rule description: ?text))"#,
    )
    .await?;
    assert_eq!(said.len(), 2, "{said:?}");
    assert!(said.iter().all(|row| row.starts_with("(syntax.meta rule:")));
    assert!(
        said.iter()
            .any(|row| row.ends_with(r#"description: "What costs less than ten.")"#))
    );
    assert!(
        said.iter()
            .any(|row| row.ends_with(r#"description: "Restocking sets the count.")"#))
    );
    Ok(())
}

/// A rule has one head. Two written as one rule are refused, and
/// nothing the document says is written, the fact beside the rule
/// included.
#[dialog_common::test]
async fn it_refuses_to_assert_and_retract_from_one_rule() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let refused = notebook
        .submit(
            r#"(assert! (xyz.todo todo:1 list: list:home))
               (rule (all (assert! (xyz.todo ?t list: ?to))
                          (retract! (xyz.todo ?t list: ?from)))
                 (move (xyz.todo ?t list: ?to))
                 (xyz.todo ?t list: ?from))"#,
        )
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(&refused, Err(Ok(Error::Refused(why))) if why.contains("a rule for each")),
        "{refused:?}"
    );
    assert_eq!(
        report(&mut notebook, "(query (xyz.todo ?t list: ?l))").await?,
        Vec::<String>::new()
    );
    Ok(())
}

/// Moving a to-do writes where it is and takes back where it was: two
/// rules of one head, each with the premises it needs. A round reads
/// one state, so both see where the to-do was. A to-do on no list is
/// still put on one, since the write does not wait on what only the
/// retraction needs.
#[dialog_common::test]
async fn it_asserts_and_retracts_from_two_rules_of_one_head() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(rule (assert! (xyz.todo ?t list: ?to))
                 (move (xyz.todo ?t list: ?to)))
               (rule (retract! (xyz.todo ?t list: ?from))
                 (move (xyz.todo ?t list: ?to))
                 (xyz.todo ?t list: ?from))
               (assert! (xyz.todo todo:1 list: list:home))"#,
        )
        .await?;
    notebook
        .submit("(move (xyz.todo todo:1 list: list:work))")
        .await?;
    notebook
        .submit("(move (xyz.todo todo:2 list: list:work))")
        .await?;
    assert_eq!(
        report(&mut notebook, "(query (xyz.todo ?t list: ?l))").await?,
        vec![
            "(xyz.todo todo:1 list: list:work)",
            "(xyz.todo todo:2 list: list:work)"
        ]
    );
    Ok(())
}

/// The same move with its condition given a name: one rule issues a
/// command that carries what the condition bound, and two rules of one
/// head read it a round later, in the same commit.
#[dialog_common::test]
async fn it_asserts_and_retracts_on_a_command_a_rule_issues() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(rule (moved (xyz.todo ?t list: ?to before: ?from))
                 (move (xyz.todo ?t list: ?to))
                 (xyz.todo ?t list: ?from))
               (rule (assert! (xyz.todo ?t list: ?to))
                 (moved (xyz.todo ?t list: ?to)))
               (rule (retract! (xyz.todo ?t list: ?from))
                 (moved (xyz.todo ?t before: ?from)))
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
               (fam.parent ?a child: ?c))
             (rule (fam.ancestor ?a descendant: ?c)
               (fam.parent ?a child: ?b)
               (fam.ancestor ?b descendant: ?c))",
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
               (increment (xyz.counter ?c))
               (xyz.counter ?c count: (:last (:natural ?n))))
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
               (archive (xyz.list ?l))
               (todo ?t list: ?l ...))"
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
        report(&mut notebook, "(query (todo ?t text: ?x state: :open ...))").await?,
        vec![r#"(xyz.todo todo:3 state: :open text: "Ship it")"#]
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
                 (jot (xyz.note text: ?x)))"#,
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

/// Two definitions under one name, whose labels both fit what a query
/// writes: the query reports through each, and a row says which relation
/// it is of. A write says every field, so it is one definition's.
#[dialog_common::test]
async fn it_reports_through_every_definition_a_query_fits() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(concept (todo ?t title: ?x done: ?d)
                 (xyz.todo ?t title: (:last (:text ?x)) done: (:last ?d)))
               (concept (todo ?t title: ?x pinned: ?p)
                 (xyz.note ?t title: (:last (:text ?x)) pinned: (:last ?p)))
               (assert! (todo todo:1 title: "Buy milk" done: false))
               (assert! (todo note:1 title: "Call Ada" pinned: true))"#,
        )
        .await?;
    assert_eq!(
        report(&mut notebook, "(query (todo ?t title: ?x ...))").await?,
        vec![
            r#"(xyz.note note:1 title: "Call Ada")"#,
            r#"(xyz.todo todo:1 title: "Buy milk")"#,
        ]
    );
    assert_eq!(
        report(&mut notebook, "(query (todo ?t pinned: ?p ...))").await?,
        vec!["(xyz.note note:1 pinned: true)"]
    );
    Ok(())
}

/// What a name is kept as: not a name and an entity only, but a name,
/// what is named, and the parameters it is applied with. A definition is
/// kept under the entity its signature names, with `last`, so two
/// definitions of other parameters stand side by side under one name and
/// a later one of the same parameters replaces. Each definition holds
/// its own names for the fields, whatever attributes they are of.
#[dialog_common::test]
async fn it_keeps_definitions_on_a_branch_under_their_signatures() -> Result<()> {
    let notebook = Notebook::open().await?;
    let definitions = [
        "(concept (todo ?t title: ?x) (xyz.todo ?t text: (:last (:text ?x))))",
        "(concept (todo ?t name: ?x) (xyz.todo ?t text: (:last (:text ?x))))",
        // The first, corrected: the same signature.
        "(concept (todo ?t title: ?x) (xyz.todo ?t title: (:last (:text ?x))))",
    ];
    let name: AttributeDescriptor =
        serde_json::from_value(json!({ "the": "syntax.name/is", "as": "text:" }))?;
    let definition: AttributeDescriptor =
        serde_json::from_value(json!({ "the": "syntax.name/definition", "as": "record:" }))?;

    for text in definitions {
        let syntax = Syntax::try_from(&read(text)?[0])?;
        let Syntax::Application(_, operand) = &syntax else {
            panic!("an application");
        };
        let signature = Signature::of(&operand.items[0], "concept").expect("a signature");
        notebook
            .branch
            .transaction()
            .assert(ConceptStatement {
                this: signature.entity()?,
                with: vec![
                    name.resolve(Value::String(signature.name.clone()))?,
                    definition.resolve(Value::Record(syntax.encode()?))?,
                ],
            })
            .commit()
            .publish()
            .perform(&notebook.operator)
            .await?;
    }
    notebook.branch.refresh(&notebook.operator).await?;

    // Everything named `todo`, by the name alone.
    let select: ConceptQuery = serde_json::from_value(json!({
        "assert": { "with": {
            "is": { "the": "syntax.name/is", "as": "text:" },
            "definition": { "the": "syntax.name/definition", "as": "record:" }
        } },
        "where": {
            "this": { "?": { "name": "this" } },
            "is": "todo",
            "definition": { "?": { "name": "definition" } }
        }
    }))?;
    let found: Vec<ConceptConclusion> = notebook
        .branch
        .query()
        .select(select)
        .perform(&notebook.operator)
        .try_vec()
        .await?;
    let mut kept = Vec::new();
    for conclusion in &found {
        let Value::Record(bytes) = conclusion
            .source()
            .lookup(&Term::<Any>::var("definition"))?
            .content()?
        else {
            panic!("a record");
        };
        kept.push(Syntax::decode(&bytes)?.to_string());
    }
    kept.sort();
    assert_eq!(
        kept,
        vec![
            "(concept (todo ?t name: ?x) (xyz.todo ?t text: (:last (:text ?x))))",
            "(concept (todo ?t title: ?x) (xyz.todo ?t title: (:last (:text ?x))))",
        ]
    );
    Ok(())
}

/// A struct is a value like 5: written as a field's value it is kept,
/// and comes back as the struct it was. With a variable in it, it is a
/// pattern: a query takes the kept struct apart by it, and one that does
/// not have its shape is no match.
#[dialog_common::test]
async fn it_keeps_a_struct_and_takes_it_apart_by_a_pattern() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(assert! (xyz.note note:1 about: {author: "Ada" tags: ["math" "engines"] pinned: true}))
               (assert! (xyz.note note:2 about: {author: "Grace"}))"#,
        )
        .await?;

    // Whole, under a variable.
    assert_eq!(
        report(&mut notebook, "(query (xyz.note ?n about: ?about))").await?,
        vec![
            r#"(xyz.note note:1 about: {author: "Ada" pinned: true tags: ["math" "engines"]})"#,
            r#"(xyz.note note:2 about: {author: "Grace"})"#,
        ]
    );
    // Taken apart: a map pattern with no rest is the map of exactly its
    // entries, so only the second note has this shape.
    assert_eq!(
        report(&mut notebook, "(query (xyz.note ?n about: {author: ?who}))").await?,
        vec![r#"(xyz.note note:2 about: {author: "Grace"})"#]
    );
    assert_eq!(
        report(
            &mut notebook,
            r#"(query (xyz.note ?n about: {author: ?who tags: [?first ?second] pinned: true}))"#
        )
        .await?,
        vec![r#"(xyz.note note:1 about: {author: "Ada" pinned: true tags: ["math" "engines"]})"#]
    );
    // With a rest it is a map of at least its entries, and the rest is
    // the others, as a struct: both notes have an author.
    assert_eq!(
        report(
            &mut notebook,
            "(query (xyz.note ?n about: {author: ?who ... ?others}))"
        )
        .await?,
        vec![
            r#"(xyz.note note:1 about: {author: "Ada" pinned: true tags: ["math" "engines"]})"#,
            r#"(xyz.note note:2 about: {author: "Grace"})"#,
        ]
    );
    // A blank takes what is there and binds nothing; the row still says
    // what was there.
    assert_eq!(
        report(
            &mut notebook,
            "(query (xyz.note ?n about: {author: _ tags: [?first _] pinned: _}))"
        )
        .await?,
        vec![r#"(xyz.note note:1 about: {author: "Ada" pinned: true tags: ["math" "engines"]})"#]
    );
    // A constant struct in a query is the value, whatever order it is
    // written in.
    assert_eq!(
        report(
            &mut notebook,
            r#"(query (xyz.note ?n about: {pinned: true tags: ["math" "engines"] author: "Ada"}))"#
        )
        .await?
        .len(),
        1
    );

    // A query of several premises is a rule laid over the branch for
    // the query alone, struct and all: nothing of it is stored.
    notebook
        .submit(r#"(assert! (xyz.person person:ada name: "Ada"))"#)
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(query (xyz.person ?p name: ?who)
               (xyz.note ?n about: {author: ?who tags: _ pinned: _})
               (xyz.person ?p name: ?who))",
        )
        .await?,
        vec![r#"(xyz.person person:ada name: "Ada")"#]
    );

    // Dialog-DB cannot keep a rule that holds a struct: a stored rule
    // has no way to write one yet. The notebook holds the rule itself
    // and lays it over the branch for every query after.
    notebook
        .submit("(rule (xyz.by ?n author: ?who) (xyz.note ?n about: {author: ?who}))")
        .await?;
    assert_eq!(notebook.held(), 1);
    assert_eq!(
        report(&mut notebook, "(query (xyz.by ?n author: ?who))").await?,
        vec![r#"(xyz.by note:2 author: "Grace")"#]
    );
    // A query of several premises reads through the held rule.
    notebook
        .submit(r#"(assert! (xyz.person person:grace name: "Grace"))"#)
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(query (xyz.person ?p name: ?who)
               (xyz.by ?n author: ?who) (xyz.person ?p name: ?who))",
        )
        .await?,
        vec![r#"(xyz.person person:grace name: "Grace")"#]
    );

    // A rule that writes runs when a transaction commits, where a rule
    // held for queries is never seen. So one that holds a struct is
    // refused until Dialog-DB can keep it.
    let writing = notebook
        .submit(
            "(rule (assert! (xyz.by ?n author: ?who))
               (xyz.note ?n about: {author: ?who}))",
        )
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(writing, Err(Ok(Error::Unsupported(_)))),
        "{writing:?}"
    );
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
                   (first (xyz.name ?n)))
                 (rule (assert! (xyz.name ?n referent: (:last (:entity concept:b))))
                   (second (xyz.name ?n)))";
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
        "(query (todo ?t text: ?x ...))",
        "(query (todo text: ?x of: ?t ...))",
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
    assert!(matches!(record, Err(Ok(Error::Refused(_)))), "{record:?}");

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

/// A kept form is a composite value, so a query takes it apart by a
/// pattern like any struct: the forms that apply a name are found by
/// the shape of an application, with no code that knows what a form is.
#[dialog_common::test]
async fn it_finds_a_kept_form_by_a_pattern_over_its_tree() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let is: AttributeDescriptor =
        serde_json::from_value(json!({ "the": "syntax.form/is", "as": "record:" }))?;
    let mut entities = Vec::new();
    for text in [
        "(todo ?t)",
        "(todo ?t state: :open)",
        "(done ?t)",
        "[todo ?t]",
    ] {
        let syntax = Syntax::try_from(&read(text)?[0])?;
        entities.push(syntax.entity()?.to_string());
        notebook
            .branch
            .transaction()
            .assert(ConceptStatement {
                this: syntax.entity()?,
                with: vec![is.resolve(syntax.value()?)?],
            })
            .commit()
            .publish()
            .perform(&notebook.operator)
            .await?;
    }
    notebook.branch.refresh(&notebook.operator).await?;

    // Every application of `todo`, whatever it is applied to. A
    // reference holds its name as a symbol of no domain, written
    // `/todo`. The tuple that starts with `todo` applies nothing.
    let mut applies = report(
        &mut notebook,
        "(query (syntax.form ?f is: {type: :application
                                     operator: {type: :reference name: /todo}
                                     operand: _}))",
    )
    .await?;
    applies.sort();
    let mut expected = vec![
        format!(
            "(syntax.form {} is: {{operand: {{items: '({{name: \"t\" type: :variable}}) terms: {{}} type: :parameters}} operator: {{name: /todo type: :reference}} type: :application}})",
            entities[0]
        ),
        format!(
            "(syntax.form {} is: {{operand: {{items: '({{name: \"t\" type: :variable}}) terms: {{state: {{type: :constant value: :open}}}} type: :parameters}} operator: {{name: /todo type: :reference}} type: :application}})",
            entities[1]
        ),
    ];
    expected.sort();
    assert_eq!(applies, expected);

    // Taken further apart: the applications with no labelled part, by
    // the shape of their parameters, whatever name they apply.
    let bare = report(
        &mut notebook,
        "(query (syntax.form ?f is: {type: :application
                                     operator: {type: :reference name: ?name}
                                     operand: {type: :parameters items: ?items terms: {}}}))",
    )
    .await?;
    assert_eq!(bare.len(), 2, "{bare:?}");
    assert!(bare.iter().any(|row| row.contains(&entities[0])));
    assert!(bare.iter().any(|row| row.contains(&entities[2])));

    // A tuple is no application, so it has no such shape, and it has
    // one of its own.
    let tuples = report(
        &mut notebook,
        "(query (syntax.form ?f is: {type: :tuple items: ?items}))",
    )
    .await?;
    assert_eq!(tuples.len(), 1, "{tuples:?}");
    assert!(tuples[0].contains(&entities[3]));

    // With a rest, a pattern says only what it asks about: every kept
    // form that applies a name, whatever else its nodes hold.
    let named = report(
        &mut notebook,
        "(query (syntax.form ?f is: {operator: {name: ?name ... ?reference} ... ?node}))",
    )
    .await?;
    assert_eq!(named.len(), 3, "{named:?}");

    // The parts of a node are a list, which `( )` takes apart and
    // `[ ]`, a tuple, does not: the kept tuple of two parts.
    let pair = "(query (syntax.form ?f is: {type: :tuple items: '(_ _)}))";
    let pairs = report(&mut notebook, pair).await?;
    assert_eq!(pairs.len(), 1, "{pairs:?}");
    assert!(pairs[0].contains(&entities[3]));
    assert_eq!(
        report(
            &mut notebook,
            "(query (syntax.form ?f is: {type: :tuple items: [_ _]}))",
        )
        .await?,
        Vec::<String>::new()
    );
    // And by a run: every application whose first part is a variable.
    let first = report(
        &mut notebook,
        "(query (syntax.form ?f is: {operand: {items: '({type: :variable name: ?variable} ... ?more)
                                               ... ?parameters}
                                     ... ?node}))",
    )
    .await?;
    assert_eq!(first.len(), 3, "{first:?}");
    Ok(())
}

/// A list is `'(1 2 3)` where a value stands: quoted, as in lisp, so
/// that it is not read as applying `1`. `... ?run` after its items is
/// the ones after them, and `...` alone takes them and binds nothing. A
/// list comes back written as it went in.
#[dialog_common::test]
async fn it_keeps_a_list_and_takes_it_apart_by_its_first_items() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit(
            r#"(assert! (xyz.note note:1 tags: '("math" "engines" "looms")))
               (assert! (xyz.note note:2 tags: '("math")))
               (assert! (xyz.note note:3 tags: ["math" "engines"]))
               (assert! (xyz.note note:4 about: {tags: '("math" {by: "Ada"})}))"#,
        )
        .await?;

    // Whole, under a variable: a list is printed `( )` and a tuple `[ ]`.
    let mut whole = report(&mut notebook, "(query (xyz.note ?n tags: ?tags))").await?;
    whole.sort();
    assert_eq!(
        whole,
        vec![
            r#"(xyz.note note:1 tags: '("math" "engines" "looms"))"#,
            r#"(xyz.note note:2 tags: '("math"))"#,
            r#"(xyz.note note:3 tags: ["math" "engines"])"#,
        ]
    );
    // A list pattern with no run is the list of exactly its items.
    assert_eq!(
        report(&mut notebook, "(query (xyz.note ?n tags: '(?only)))").await?,
        vec![r#"(xyz.note note:2 tags: '("math"))"#]
    );
    // With a run it is a list that starts with them.
    assert_eq!(
        report(
            &mut notebook,
            "(query (xyz.note ?n tags: '(?first ?second ... ?more)))"
        )
        .await?,
        vec![r#"(xyz.note note:1 tags: '("math" "engines" "looms"))"#]
    );
    let mut starts = report(
        &mut notebook,
        r#"(query (xyz.note ?n tags: '("math" ... ?more)))"#,
    )
    .await?;
    starts.sort();
    assert_eq!(
        starts,
        vec![
            r#"(xyz.note note:1 tags: '("math" "engines" "looms"))"#,
            r#"(xyz.note note:2 tags: '("math"))"#,
        ]
    );
    // A list is not a tuple, nor a tuple a list.
    assert_eq!(
        report(&mut notebook, "(query (xyz.note ?n tags: [?a ?b]))").await?,
        vec![r#"(xyz.note note:3 tags: ["math" "engines"])"#]
    );
    // A list with no variable in it is the value, and what a row prints
    // reads back as it.
    assert_eq!(
        report(
            &mut notebook,
            r#"(query (xyz.note ?n tags: '("math" "engines" "looms")))"#
        )
        .await?,
        vec![r#"(xyz.note note:1 tags: '("math" "engines" "looms"))"#]
    );
    // Inside a struct, and a struct inside it.
    assert_eq!(
        report(
            &mut notebook,
            "(query (xyz.note ?n about: {tags: '(_ {by: ?who})}))"
        )
        .await?,
        vec![r#"(xyz.note note:4 about: {tags: '("math" {by: "Ada"})})"#]
    );

    // A list has no labelled part.
    let labelled = notebook
        .submit("(query (xyz.note ?n tags: '(?first then: ?second)))")
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(
        matches!(labelled, Err(Ok(Error::Refused(_)))),
        "{labelled:?}"
    );
    // Quoted, a list may start with anything, a keyword too. Not
    // quoted, a `( )` where a value stands is refused, with what to
    // write.
    notebook
        .submit("(assert! (xyz.note note:5 tags: '(:open :done)))")
        .await?;
    assert_eq!(
        report(&mut notebook, "(query (xyz.note ?n tags: '(:open ...)))").await?,
        vec!["(xyz.note note:5 tags: '(:open :done))"]
    );
    let bare = notebook
        .submit(r#"(query (xyz.note ?n tags: ("math" "engines")))"#)
        .await
        .map_err(|error| error.downcast::<Error>());
    assert!(matches!(bare, Err(Ok(Error::Refused(_)))), "{bare:?}");
    Ok(())
}

/// What a form as a record rests on: a symbol with no domain is a value
/// today, a variable or an operator spelled as one included, and it is
/// not an attribute a claim can be made under.
#[dialog_common::test]
async fn it_holds_a_symbol_with_no_domain_as_a_value_and_not_as_an_attribute() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .submit("(assert! (xyz.form form:1 head: /todo op: /+ var: /?x))")
        .await?;
    assert_eq!(
        report(
            &mut notebook,
            "(query (xyz.form ?f head: ?h var: ?v op: ?o))"
        )
        .await?,
        vec!["(xyz.form form:1 head: /todo op: /+ var: /?x)"]
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
        .submit("(rule (assert! (todo ?t state: :done)) (finish (todo ?t)))")
        .await;
    assert!(matches!(why(partial), Err(Ok(Error::Refused(_)))));

    // A variable in `unless` is one a premise binds.
    let unbound = notebook
        .submit(
            "(query (xyz.todo ?t text: ?x)
               (xyz.todo ?t text: ?x)
               (unless (xyz.todo ?t list: ?l)))",
        )
        .await;
    assert!(matches!(why(unbound), Err(Ok(Error::Refused(_)))));

    // A formula inside `unless` has nowhere to go.
    let formula = notebook
        .submit(
            "(query (xyz.counter ?c count: ?n)
               (xyz.counter ?c count: ?n)
               (unless (xyz.counter ?c count: (+ ?n : 1))))",
        )
        .await;
    assert!(matches!(why(formula), Err(Ok(Error::Unsupported(_)))));
    Ok(())
}
