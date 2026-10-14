# Fixtures captured from the release on `main`

`main-120fba8.json` is what `main` at `120fba8` stores: the facts it
writes to install two rules (a deductive `org/salary := org/bonus` and
an inductive `derived/tag := doc/title`), and the identities it gives
two attributes and two concepts. The migration tests in
`../migration.rs` replay these facts onto a branch of this release, so
they test what a replica that ran `main` actually holds rather than
this release's encoding of the same rules.

It was produced by the program below, added as a workspace member
`rust/capture` of a `main` checkout and run with `cargo run -p capture`.

```rust
use dialog_artifacts::{Changes, Instruction, Value};
use dialog_query::rule::{DeductiveRuleDescriptor, InductiveRule};
use dialog_query::{AttributeDescriptor, Cardinality, ConceptDescriptor, Statement, Type};
use serde_json::{Value as Json, json};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn value(value: &Value) -> Json {
    match value {
        Value::Bytes(bytes) => json!({ "bytes": hex(bytes) }),
        Value::Entity(entity) => json!({ "entity": entity.to_string() }),
        Value::String(text) => json!({ "string": text }),
        Value::Boolean(flag) => json!({ "boolean": flag }),
        Value::UnsignedInt(n) => json!({ "unsigned": n.to_string() }),
        other => panic!("not captured: {other:?}"),
    }
}

fn facts(statement: impl Statement) -> Json {
    let mut changes = Changes::new();
    statement.assert(&mut changes);
    let mut facts: Vec<Json> = changes
        .into_instructions()
        .into_iter()
        .map(|instruction| match instruction {
            Instruction::Assert(fact) | Instruction::Replace(fact) => json!({
                "the": fact.the.to_string(),
                "of": fact.of.to_string(),
                "is": value(&fact.is),
            }),
            Instruction::Retract(_) => panic!("an install retracts nothing"),
        })
        .collect();
    facts.sort_by_key(|fact| fact.to_string());
    Json::Array(facts)
}
fn main() {
    let salary = AttributeDescriptor::new(
        "org/salary".parse().unwrap(),
        "",
        Cardinality::One,
        Some(Type::UnsignedInt),
    );
    let tag = AttributeDescriptor::new(
        "org/tag".parse().unwrap(),
        "",
        Cardinality::Many,
        Some(Type::String),
    );
    let concept = ConceptDescriptor::try_from(vec![("salary", salary.clone()), ("tag", tag.clone())])
        .unwrap();
    let stage: ConceptDescriptor = serde_json::from_value(json!({
        "with": { "target": { "the": "cmd.stage/target", "as": "Entity" } }
    }))
    .unwrap();

    // org/salary(x) := bonus :- org/bonus(x) = bonus
    let salary_from_bonus: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": { "with": {
            "salary": { "the": "org/salary", "as": "UnsignedInteger" }
        }},
        "when": [{
            "assert": { "with": {
                "bonus": { "the": "org/bonus", "as": "UnsignedInteger" }
            }},
            "where": {
                "this": { "?": { "name": "this" } },
                "bonus": { "?": { "name": "salary" } }
            }
        }]
    }))
    .unwrap();
    let salary_from_bonus = salary_from_bonus.compile().unwrap();

    // assert! derived/tag(x) := t when doc/title(x) = t
    let tagger: InductiveRule = serde_json::from_value(json!({
        "assert!": {
            "with": { "tag": { "the": "derived/tag", "as": "Text" } }
        },
        "when": [{
            "assert": {
                "with": { "title": { "the": "doc/title", "as": "Text" } }
            },
            "where": {
                "this": { "?": { "name": "this" } },
                "title": { "?": { "name": "tag" } }
            }
        }]
    }))
    .unwrap();

    let out = json!({
        "release": "main 120fba8",
        "attributes": {
            "org/salary one UnsignedInteger": salary.to_uri(),
            "org/tag many Text": tag.to_uri(),
        },
        "concepts": {
            "{salary: org/salary one UnsignedInteger, tag: org/tag many Text}": concept.this().to_string(),
            "{target: cmd.stage/target Entity}": stage.this().to_string(),
        },
        "rules": {
            "salary_from_bonus": {
                "entity": salary_from_bonus.this().to_string(),
                "facts": facts(&salary_from_bonus),
            },
            "tagger": {
                "entity": tagger.this().to_string(),
                "facts": facts(&tagger),
            },
        },
    });
    println!("{}", serde_json::to_string_pretty(&out).unwrap());
}
```
