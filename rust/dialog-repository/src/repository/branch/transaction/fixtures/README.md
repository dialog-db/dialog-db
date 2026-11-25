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
use dialog_corpus::{Changes, Instruction, Value};
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

# Fixtures captured from the `v0.4.0` release

`v0.4.0.json` is what the `v0.4.0` release, which read every attribute
under a pick, stores: the facts it writes to install seven rules, and
the identities it gives three attributes and two concepts. The rules
read their attributes under `last` (spelled or implied), under `all`,
under `last` over a listed domain, under `max`, under `top` (a listed
`as` without a pick), and over a chain of relations; `tagger` is an
inductive rule whose head is read under `all`. The tests in
`../pick_migration.rs` replay these facts onto a branch of this
release.

It was produced by the program below, added as a workspace member
`rust/capture` of a `v0.4.0` checkout (its `Cargo.toml` depends on
`dialog-corpus`, `dialog-query` and `serde_json`) and run with
`cargo run -p capture`.

```rust
use dialog_corpus::{Changes, Instruction, Statement, Value};
use dialog_query::rule::{DeductiveRuleDescriptor, InductiveRule};
use dialog_query::{AttributeDescriptor, ConceptDescriptor};
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
        Value::Symbol(symbol) => json!({ "symbol": symbol.to_string() }),
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
            Instruction::Assert(fact, _) => json!({
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

fn attribute(spec: Json) -> AttributeDescriptor {
    serde_json::from_value(spec).unwrap()
}

fn deductive(spec: Json) -> Json {
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(spec).unwrap();
    let rule = descriptor.compile().unwrap();
    json!({ "entity": rule.this().to_string(), "facts": facts(&rule) })
}

fn main() {
    let salary = json!({ "the": "org/salary", "as": "natural:" });
    let tag = json!({ "the": "org/tag", "as": "text:", "pick": "all:" });
    let state = json!({ "the": "job/state", "as": ["todo", "done"], "pick": "last:" });
    let ranked = json!({ "the": "job/state", "as": ["todo", "done"] });
    let top_salary = json!({ "the": "org/salary", "as": "natural:", "pick": "max:" });
    let contact = json!({ "the": ["user/email", "user/phone"], "as": "text:" });

    let concept: ConceptDescriptor =
        serde_json::from_value(json!({ "with": { "salary": salary, "tag": tag } })).unwrap();
    let stage: ConceptDescriptor = serde_json::from_value(json!({
        "with": { "target": { "the": "cmd.stage/target", "as": "entity:" } }
    }))
    .unwrap();

    // org/salary(x) := bonus :- org/bonus(x) = bonus
    let salary_from_bonus = deductive(json!({
        "deduce": { "with": { "salary": salary } },
        "when": [{
            "assert": { "with": { "bonus": { "the": "org/bonus", "as": "natural:" } } },
            "where": {
                "this": { "?": { "name": "this" } },
                "bonus": { "?": { "name": "salary" } }
            }
        }]
    }));

    // org/tag(x) := t :- doc/label(x) = t, every tag kept
    let tag_from_label = deductive(json!({
        "deduce": { "with": { "tag": tag } },
        "when": [{
            "assert": { "with": { "label": { "the": "doc/label", "as": "text:", "pick": "all:" } } },
            "where": {
                "this": { "?": { "name": "this" } },
                "label": { "?": { "name": "tag" } }
            }
        }]
    }));

    // job/state(x) := s :- job/raw(x) = s, read as the newest of a listed domain
    let state_from_raw = deductive(json!({
        "deduce": { "with": { "state": state } },
        "when": [{
            "assert": { "with": { "raw": { "the": "job/raw", "as": "text:" } } },
            "where": {
                "this": { "?": { "name": "this" } },
                "raw": { "?": { "name": "state" } }
            }
        }]
    }));

    // org/top(x) := s :- org/salary(x) read under max = s
    let top_from_salary = deductive(json!({
        "deduce": { "with": { "top": { "the": "org/top", "as": "natural:" } } },
        "when": [{
            "assert": { "with": { "salary": top_salary } },
            "where": {
                "this": { "?": { "name": "this" } },
                "salary": { "?": { "name": "top" } }
            }
        }]
    }));

    // job/flag(x) := s :- job/state(x) read under top = s
    let flag_from_ranked_state = deductive(json!({
        "deduce": { "with": { "flag": { "the": "job/flag", "as": "text:" } } },
        "when": [{
            "assert": { "with": { "state": ranked } },
            "where": {
                "this": { "?": { "name": "this" } },
                "state": { "?": { "name": "flag" } }
            }
        }]
    }));

    // user/reach(x) := c :- the first of user/email, user/phone = c
    let reach_from_contact = deductive(json!({
        "deduce": { "with": { "reach": { "the": "user/reach", "as": "text:" } } },
        "when": [{
            "assert": { "with": { "contact": contact } },
            "where": {
                "this": { "?": { "name": "this" } },
                "contact": { "?": { "name": "reach" } }
            }
        }]
    }));

    // assert! derived/tag(x) := t when doc/title(x) = t
    let tagger: InductiveRule = serde_json::from_value(json!({
        "assert!": {
            "with": { "tag": { "the": "derived/tag", "as": "text:", "pick": "all:" } }
        },
        "when": [{
            "assert": {
                "with": { "title": { "the": "doc/title", "as": "text:" } }
            },
            "where": {
                "this": { "?": { "name": "this" } },
                "title": { "?": { "name": "tag" } }
            }
        }]
    }))
    .unwrap();

    let out = json!({
        "release": "v0.4.0",
        "attributes": {
            "org/salary last natural": attribute(salary).to_uri(),
            "org/tag all text": attribute(tag).to_uri(),
            "job/state last [todo, done]": attribute(state).to_uri(),
        },
        "concepts": {
            "{salary: org/salary last natural, tag: org/tag all text}": concept.this().to_string(),
            "{target: cmd.stage/target entity}": stage.this().to_string(),
        },
        "rules": {
            "salary_from_bonus": salary_from_bonus,
            "tag_from_label": tag_from_label,
            "state_from_raw": state_from_raw,
            "top_from_salary": top_from_salary,
            "flag_from_ranked_state": flag_from_ranked_state,
            "reach_from_contact": reach_from_contact,
            "tagger": {
                "entity": tagger.this().to_string(),
                "facts": facts(&tagger),
            },
        },
    });
    println!("{}", serde_json::to_string_pretty(&out).unwrap());
}
```

`main-f8f2655.json` is what `main` at `f8f2655` stores for a deductive
rule whose head field is declared `integer:` and whose body reads it
from a `text:` field: a rule this release refuses when it compiles. The
tests in `../../head_type_tests.rs` install these facts as that release
did, past the check, and pin that the rule is inert and listed as
invalid. It was produced by the program below, added as a workspace
member `rust/capture` of a checkout of `f8f2655`.

```rust
use dialog_corpus::{Changes, Instruction, Value};
use dialog_query::Statement;
use dialog_query::rule::DeductiveRuleDescriptor;
use serde_json::{Value as Json, json};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn value(value: &Value) -> Json {
    match value {
        Value::Bytes(bytes) => json!({ "bytes": hex(bytes) }),
        Value::Entity(entity) => json!({ "entity": entity.to_string() }),
        Value::Symbol(symbol) => json!({ "symbol": symbol.to_string() }),
        Value::String(text) => json!({ "string": text }),
        other => panic!("not captured: {other:?}"),
    }
}

fn main() {
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": { "with": { "value": { "the": "note/body", "as": "integer:", "cardinality": "many" } } },
        "when": [{
            "assert": { "with": { "value": { "the": "source/body", "as": "text:", "cardinality": "many" } } },
            "where": {
                "this": { "?": { "name": "this" } },
                "value": { "?": { "name": "value" } }
            }
        }]
    }))
    .unwrap();
    let rule = descriptor.compile().unwrap();
    let mut changes = Changes::new();
    (&rule).assert(&mut changes);
    let mut facts: Vec<Json> = changes
        .into_instructions()
        .into_iter()
        .map(|instruction| match instruction {
            Instruction::Assert(fact, _) => json!({
                "the": fact.the.to_string(),
                "of": fact.of.to_string(),
                "is": value(&fact.is),
            }),
            Instruction::Retract(_) => panic!("an install retracts nothing"),
        })
        .collect();
    facts.sort_by_key(|fact| fact.to_string());
    let out = json!({
        "rules": {
            "text-into-integer": { "entity": rule.this().to_string(), "facts": facts }
        }
    });
    println!("{}", serde_json::to_string_pretty(&out).unwrap());
}
```
