//! The parser against the worked examples in the tonk design, and
//! against the behaviours Ubiquity's own `test_parser2.js` pinned down.

use std::collections::BTreeMap;

use dialog_lingo::{
    Argument, Candidate, Context, Grammar, Memory, Noun, Parse, Registry, Selection, Value, Verb,
    parse,
};

fn candidate(entity: &str, label: &str) -> Candidate {
    Candidate {
        entity: entity.into(),
        label: label.into(),
    }
}

fn argument(role: &str, field: &str, noun: Noun, label: &str) -> Argument {
    Argument {
        role: role.into(),
        field: field.into(),
        noun,
        label: label.into(),
    }
}

fn concept(id: &str) -> Noun {
    Noun::Concept(id.into())
}

/// Three real tonk commands: expelling a member, and two renames that
/// share the word "rename" and differ in what they rename.
fn registry() -> Registry {
    Registry {
        defaults: Default::default(),
        verbs: vec![
            Verb {
                id: "member/expel".into(),
                names: vec!["expel".into(), "remove member".into()],
                arguments: vec![argument(
                    "object",
                    "member",
                    concept("member/account"),
                    "member",
                )],
            },
            Verb {
                id: "notebook/retitle".into(),
                names: vec!["rename".into(), "retitle".into()],
                arguments: vec![
                    argument("object", "subject", concept("notebook/named"), "notebook"),
                    argument("goal", "title", Noun::Text, "title"),
                ],
            },
            Verb {
                id: "tonk/rename-repository".into(),
                names: vec!["rename".into()],
                arguments: vec![
                    argument("object", "subject", concept("tonk/repository"), "space"),
                    argument("goal", "name", Noun::Text, "name"),
                ],
            },
        ],
        candidates: BTreeMap::from([
            (
                "member/account".into(),
                vec![
                    candidate("did:key:alice", "Alice"),
                    candidate("did:key:alicia", "Alicia"),
                    candidate("did:key:bob", "Bob"),
                ],
            ),
            (
                "notebook/named".into(),
                vec![
                    candidate("id:roadmap", "Roadmap"),
                    candidate("id:todo", "to do list"),
                ],
            ),
            (
                "tonk/repository".into(),
                vec![candidate("did:key:space", "Budget")],
            ),
        ]),
    }
}

/// The palette opened on the Roadmap notebook.
fn on_roadmap() -> Context {
    Context {
        selection: None,
        this: Some(Selection {
            text: "Roadmap".into(),
            entity: Some("id:roadmap".into()),
        }),
    }
}

fn english(input: &str, context: &Context) -> Vec<Parse> {
    parse(
        &Grammar::english(),
        &registry(),
        &Memory::default(),
        context,
        input,
        10,
    )
}

fn value(parse: &Parse, field: &str) -> Option<Value> {
    parse
        .arguments
        .iter()
        .find(|filled| filled.field == field)
        .and_then(|filled| filled.value.clone())
}

fn entity(id: &str) -> Option<Value> {
    Some(Value::Entity(id.into()))
}

fn text(value: &str) -> Option<Value> {
    Some(Value::Text(value.into()))
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-9
}

#[test]
fn it_reads_an_anaphor_as_the_entity_the_page_shows() {
    let parses = english("rename this to Q3 plan", &on_roadmap());
    let top = &parses[0];
    assert_eq!(top.verb, "notebook/retitle");
    assert_eq!(value(top, "subject"), entity("id:roadmap"));
    assert_eq!(value(top, "title"), text("Q3 plan"));
    // m = 1.0 (exact verb) × 1.2 (anaphor); object 1.0, text goal 0.3:
    // 1.2 + 1.0·1.2 + 0.3·1.2.
    assert!(close(top.score, 2.76), "score {}", top.score);
    assert_eq!(top.display_text(), "rename [Roadmap] to [Q3 plan]");
}

#[test]
fn it_lets_the_noun_decide_between_commands_sharing_a_word() {
    // "this" is a notebook, so the space rename finds no object and dies.
    let parses = english("rename this to Q3 plan", &on_roadmap());
    assert!(
        parses
            .iter()
            .all(|parse| !(parse.verb == "tonk/rename-repository"
                && value(parse, "subject").is_some()
                && value(parse, "name") == text("Q3 plan")))
    );

    // Naming the space picks the space rename.
    let parses = english("rename budget to Q3", &on_roadmap());
    assert_eq!(parses[0].verb, "tonk/rename-repository");
    assert_eq!(value(&parses[0], "subject"), entity("did:key:space"));
    assert_eq!(value(&parses[0], "name"), text("Q3"));
}

#[test]
fn it_matches_an_object_against_candidate_labels() {
    let parses = english("expel ali", &Context::default());
    assert_eq!(parses[0].verb, "member/expel");
    assert_eq!(value(&parses[0], "member"), entity("did:key:alice"));
    assert_eq!(value(&parses[1], "member"), entity("did:key:alicia"));
    assert!(parses[0].score > parses[1].score);
}

#[test]
fn it_matches_multi_word_verbs_and_their_suffixes() {
    let parses = english("remove member bob", &Context::default());
    assert_eq!(parses[0].verb, "member/expel");
    assert_eq!(value(&parses[0], "member"), entity("did:key:bob"));

    let parses = english("member bob", &Context::default());
    assert_eq!(parses[0].verb, "member/expel");
    assert_eq!(value(&parses[0], "member"), entity("did:key:bob"));
}

#[test]
fn it_suggests_verbs_for_a_bare_noun() {
    let parses = english("alice", &Context::default());
    let top = &parses[0];
    assert_eq!(top.verb, "member/expel");
    assert_eq!(top.input, None);
    assert_eq!(value(top, "member"), entity("did:key:alice"));
    // Noun-first: m = 0.3 × (1 − 0.7 / (1 + 0 uses)) = 0.09.
    assert!(close(top.score, 0.09 + 1.0 * 0.09), "score {}", top.score);
}

#[test]
fn it_fills_an_empty_argument_with_the_entity_the_page_shows() {
    let parses = english("ren", &on_roadmap());
    let notebook = parses
        .iter()
        .find(|parse| parse.verb == "notebook/retitle")
        .expect("the notebook rename is offered");
    let subject = notebook
        .arguments
        .iter()
        .find(|filled| filled.field == "subject")
        .unwrap();
    assert_eq!(subject.value, entity("id:roadmap"));
    assert!(!subject.given);
    assert!(!notebook.is_complete());
    assert_eq!(notebook.display_text(), "rename [Roadmap] to (title)");
}

#[test]
fn it_ranks_what_was_chosen_before() {
    let mut memory = Memory::default();
    memory.remember(Some("ren"), "tonk/rename-repository");
    let parses = parse(
        &Grammar::english(),
        &registry(),
        &memory,
        &on_roadmap(),
        "ren",
        10,
    );
    assert_eq!(parses[0].verb, "tonk/rename-repository");
}

#[test]
fn it_ranks_a_specific_noun_above_arbitrary_text() {
    // Ubiquity's testSortSpecificNounsBeforeArbTextParser2.
    let registry = Registry {
        defaults: Default::default(),
        verbs: vec![
            Verb {
                id: "mumble".into(),
                names: vec!["mumble".into()],
                arguments: vec![argument("object", "stuff", Noun::Text, "stuff")],
            },
            Verb {
                id: "wash".into(),
                names: vec!["wash".into()],
                arguments: vec![argument("object", "dog", concept("dog"), "dog")],
            },
        ],
        candidates: BTreeMap::from([(
            "dog".into(),
            vec![
                candidate("dog:beagle", "beagle"),
                candidate("dog:husky", "husky"),
            ],
        )]),
    };
    let context = Context {
        selection: Some(Selection {
            text: "beagle".into(),
            entity: None,
        }),
        this: None,
    };
    let parses = parse(
        &Grammar::english(),
        &registry,
        &Memory::default(),
        &context,
        "",
        10,
    );
    assert_eq!(parses[0].verb, "wash");
    assert_eq!(parses[1].verb, "mumble");
}

#[test]
fn it_parses_japanese_with_the_same_commands() {
    let mut registry = registry();
    registry.verbs[1].names = vec!["名前変更".into()];
    let parses = parse(
        &Grammar::japanese(),
        &registry,
        &Memory::default(),
        &Context {
            selection: None,
            this: Some(Selection {
                text: "Roadmap".into(),
                entity: Some("id:roadmap".into()),
            }),
        },
        "これを「Q3計画」に名前変更",
        10,
    );
    let top = &parses[0];
    assert_eq!(top.verb, "notebook/retitle");
    assert_eq!(value(top, "subject"), entity("id:roadmap"));
    assert_eq!(value(top, "title"), text("「Q3計画」"));
}
