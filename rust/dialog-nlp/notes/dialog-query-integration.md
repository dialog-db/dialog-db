# Integration with dialog-query

This document describes how the NLP parser maps onto dialog-query's primitives
when fully integrated. The sketch code in `src/` implements the algorithm
directly; this document shows how it would work as facts, rules, and queries.

---

## All Registrations Are Facts

### Verb registration

```rust
// Attributes
mod verb {
    #[derive(Attribute, Clone)]
    pub struct Name(pub String);

    #[derive(Attribute, Clone)]
    pub struct Description(pub String);

    #[derive(Attribute, Clone)]
    #[cardinality(many)]
    pub struct Alias(pub String);
}

// Concept
#[derive(Concept, Debug, Clone)]
pub struct Verb {
    pub this: Entity,
    pub name: verb::Name,
    pub description: verb::Description,
}

// Registering the "translate" verb:
let translate = Entity::new()?;
let mut tx = session.edit();
tx.assert(Verb {
    this: translate.clone(),
    name: verb::Name("translate".into()),
    description: verb::Description("Translate text between languages".into()),
});
tx.assert(With { this: translate.clone(), has: verb::Alias("trans".into()) });
session.commit(tx).await?;
```

### Argument slot registration

```rust
mod argument {
    #[derive(Attribute, Clone)]
    pub struct VerbRef(pub Entity);

    #[derive(Attribute, Clone)]
    pub struct Role(pub String);

    #[derive(Attribute, Clone)]
    pub struct NounTypeRef(pub Entity);

    #[derive(Attribute, Clone)]
    pub struct Required(pub bool);
}

#[derive(Concept, Debug, Clone)]
pub struct ArgumentSlot {
    pub this: Entity,
    pub verb: argument::VerbRef,
    pub role: argument::Role,
    pub noun_type: argument::NounTypeRef,
    pub required: argument::Required,
}

// Register "object: text (required)" for translate verb
let obj_slot = Entity::new()?;
tx.assert(ArgumentSlot {
    this: obj_slot,
    verb: argument::VerbRef(translate.clone()),
    role: argument::Role("object".into()),
    noun_type: argument::NounTypeRef(text_noun_type.clone()),
    required: argument::Required(true),
});
```

### Noun type registration

```rust
mod noun_type {
    #[derive(Attribute, Clone)]
    pub struct Label(pub String);

    #[derive(Attribute, Clone)]
    pub struct Description(pub String);
}

#[derive(Concept, Debug, Clone)]
pub struct NounType {
    pub this: Entity,
    pub label: noun_type::Label,
    pub description: noun_type::Description,
}

// Registering the "language" noun type:
let language_nt = Entity::new()?;
tx.assert(NounType {
    this: language_nt.clone(),
    label: noun_type::Label("language".into()),
    description: noun_type::Description("A human language".into()),
});

// The lookup table entries are also facts:
mod known_language {
    #[derive(Attribute, Clone)]
    pub struct Name(pub String);

    #[derive(Attribute, Clone)]
    pub struct Code(pub String);
}

#[derive(Concept, Debug, Clone)]
pub struct KnownLanguage {
    pub this: Entity,
    pub name: known_language::Name,
    pub code: known_language::Code,
}

let spanish = Entity::new()?;
tx.assert(KnownLanguage {
    this: spanish,
    name: known_language::Name("spanish".into()),
    code: known_language::Code("es".into()),
});
// ... more languages ...
```

---

## Noun Recognition as Derivation Rules

The key insight: each noun type's recognizer is a `DeductiveRule` installed
in the session. When the parser queries for NounMatch concepts, the rule
engine automatically applies the recognizer.

```rust
// --- The NounMatch concept (derived by rules) ---

mod noun_match {
    #[derive(Attribute, Clone)]
    pub struct NounTypeLabel(pub String);

    #[derive(Attribute, Clone)]
    pub struct InputText(pub String);

    #[derive(Attribute, Clone)]
    pub struct Value(pub String);

    #[derive(Attribute, Clone)]
    pub struct Confidence(pub f64);
}

#[derive(Concept, Debug, Clone)]
pub struct NounMatch {
    pub this: Entity,
    pub noun_type: noun_match::NounTypeLabel,
    pub input_text: noun_match::InputText,
    pub value: noun_match::Value,
    pub confidence: noun_match::Confidence,
}

// --- Recognizer rule for language noun type ---
// This rule says: a NounMatch can be derived when a Segment's text
// joins against a KnownLanguage's name.

fn recognize_language(nm: Match<NounMatch>) -> impl When {
    (
        Match::<Segment> {
            this: Term::var("segment"),
            text: nm.input_text.clone(),  // join: segment text = input text
            role: Term::var("role"),
        },
        Match::<KnownLanguage> {
            this: Term::var("entry"),
            name: nm.input_text.clone(),  // join: known language name = text
            code: nm.value.clone(),       // bind: code becomes the value
        },
        // The noun_type and confidence are fixed by this rule:
        nm.noun_type.is(Term::from("language".to_string())),
        nm.confidence.is(Term::from(1.0)),
    )
}

// Install the recognizer rule:
let session = session.install(recognize_language)?;
```

### Selection as a noun

```rust
// Rule: when the segment text is "this" and there's a selection,
// produce a NounMatch from the selection.

fn selection_as_text(nm: Match<NounMatch>) -> impl When {
    (
        Match::<Segment> {
            this: Term::var("segment"),
            text: Term::from("this".to_string()),
            role: Term::var("role"),
        },
        Match::<With<input::Selection>> {
            this: Term::var("session"),
            has: nm.value.clone(),  // selection text becomes the value
        },
        nm.noun_type.is(Term::from("text".to_string())),
        nm.input_text.is(Term::from("this".to_string())),
        nm.confidence.is(Term::from(0.8)),
    )
}
```

---

## Verb Matching as a Rule

```rust
mod verb_match {
    #[derive(Attribute, Clone)]
    pub struct VerbRef(pub Entity);

    #[derive(Attribute, Clone)]
    pub struct Quality(pub f64);

    #[derive(Attribute, Clone)]
    pub struct TokenPosition(pub u32);
}

#[derive(Concept, Debug, Clone)]
pub struct VerbMatch {
    pub this: Entity,
    pub verb: verb_match::VerbRef,
    pub quality: verb_match::Quality,
    pub position: verb_match::TokenPosition,
}

// Rule: exact verb match
fn match_verb_exact(vm: Match<VerbMatch>) -> impl When {
    (
        Match::<Token> {
            this: Term::var("token"),
            value: Term::var("name"),
            position: vm.position.clone(),
        },
        Match::<Verb> {
            this: vm.verb.clone(),
            name: Term::var("name"),  // join: token value = verb name
        },
        vm.quality.is(Term::from(1.0)),
    )
}

// Rule: exact alias match
fn match_verb_alias(vm: Match<VerbMatch>) -> impl When {
    (
        Match::<Token> {
            this: Term::var("token"),
            value: Term::var("alias"),
            position: vm.position.clone(),
        },
        Match::<With<verb::Alias>> {
            this: vm.verb.clone(),
            has: Term::var("alias"),  // join: token value = verb alias
        },
        vm.quality.is(Term::from(1.0)),
    )
}

// Install both rules — they're OR alternatives for deriving VerbMatch:
let session = session
    .install(match_verb_exact)?
    .install(match_verb_alias)?;
```

---

## Candidate Assembly as a Rule

```rust
mod candidate {
    #[derive(Attribute, Clone)]
    pub struct VerbRef(pub Entity);

    #[derive(Attribute, Clone)]
    pub struct Score(pub f64);
}

#[derive(Concept, Debug, Clone)]
pub struct Candidate {
    pub this: Entity,
    pub verb: candidate::VerbRef,
    pub score: candidate::Score,
}

// This is where it gets interesting — the candidate assembly rule
// would need to join VerbMatch + NounMatches and compute a score.
// This is complex in pure Datalog; it may need a Formula or
// aggregation support.
```

---

## Discovery — The Parser Queries Dialog

The parser doesn't have a hardcoded registry. It discovers everything by
querying the session:

```rust
// "What verbs are available?"
let verbs = Match::<Verb> {
    this: Term::var("verb"),
    name: Term::var("name"),
    description: Term::var("desc"),
}.query(&session).try_vec().await?;

// "What noun types are registered?"
let nouns = Match::<NounType> {
    this: Term::var("noun"),
    label: Term::var("label"),
    description: Term::var("desc"),
}.query(&session).try_vec().await?;

// "What arguments does the translate verb expect?"
let args = Match::<ArgumentSlot> {
    this: Term::var("slot"),
    verb: Term::from(translate_entity.clone()),
    role: Term::var("role"),
    noun_type: Term::var("noun_type"),
    required: Term::var("required"),
}.query(&session).try_vec().await?;

// "What language entries are known?"
let langs = Match::<KnownLanguage> {
    this: Term::var("lang"),
    name: Term::var("name"),
    code: Term::var("code"),
}.query(&session).try_vec().await?;
```

### Dynamic extension

Because everything is facts + rules, extending the parser is just asserting
new facts and installing new rules:

```rust
// Add a new verb at runtime:
let mut tx = session.edit();
tx.assert(Verb {
    this: Entity::new()?,
    name: verb::Name("summarize".into()),
    description: verb::Description("Summarize text".into()),
});
session.commit(tx).await?;

// Add a new noun type by installing a recognizer rule:
let session = session.install(recognize_contact)?;

// The parser automatically discovers these on the next parse.
```

---

## Effectful Rules — Verb Execution

The existing dialog-query rule system is purely deductive. Verb execution
requires an extension: **effectful rules** that produce Effect descriptors.

### Proposed design

```rust
/// An effectful rule has the same structure as a DeductiveRule but its
/// conclusion is an Effect rather than a Concept derivation.
///
/// When: premises are satisfied (parsed sentence, resolved arguments)
/// Then: produce an Effect descriptor

pub trait VerbHandler: Send + Sync {
    /// The verb name this handler responds to.
    fn verb(&self) -> &str;

    /// Given resolved arguments, produce an effect.
    fn handle(&self, args: &ResolvedArguments) -> Result<Effect, NlpError>;
}

// Installation:
impl<S: Store> Session<S> {
    pub fn install_verb_handler<H: VerbHandler + 'static>(
        mut self,
        handler: H,
    ) -> Self {
        self.verb_handlers.insert(handler.verb().to_string(), Box::new(handler));
        self
    }
}

// The translate handler:
struct TranslateHandler;

impl VerbHandler for TranslateHandler {
    fn verb(&self) -> &str { "translate" }

    fn handle(&self, args: &ResolvedArguments) -> Result<Effect, NlpError> {
        let text = args.get(&SemanticRole::Object)
            .ok_or(NlpError::MissingArgument {
                verb: "translate".into(),
                role: "object".into(),
            })?;
        let target = args.get(&SemanticRole::Goal)
            .ok_or(NlpError::MissingArgument {
                verb: "translate".into(),
                role: "goal".into(),
            })?;

        Ok(Effect::Custom {
            name: "translate".into(),
            params: HashMap::from([
                ("text".into(), text.to_string()),
                ("target_language".into(), target.to_string()),
            ]),
        })
    }
}
```

### Capability gating

Effects are executed through the capability system. A verb handler's effect
must be authorized by the caller's capability chain:

```rust
// The capability hierarchy for NLP verb execution:
//
// Subject (DID)
//   └─ NlpVerbs (attenuation)
//        └─ VerbPolicy::new("translate") (policy)
//             └─ Execute::new(params) (effect)

let capability = Subject::from(did)
    .attenuate(NlpVerbs)
    .attenuate(VerbPolicy::new("translate"))
    .invoke(Execute::new(params));
```

This ensures that callers can only execute verbs they're authorized for,
and effects are auditable.

---

## Summary: What's a Fact, What's a Rule, What's a Formula

| Thing                | dialog-query primitive | Nature       |
|----------------------|-----------------------|--------------|
| Verb name/desc       | Concept (facts)       | Data         |
| Verb aliases         | Attribute (facts)     | Data         |
| Argument slots       | Concept (facts)       | Data         |
| Noun type name/desc  | Concept (facts)       | Data         |
| Known value tables   | Concept (facts)       | Data         |
| Role markers         | Concept (facts)       | Data         |
| Noun recognizers     | DeductiveRule         | Logic        |
| Verb matching        | DeductiveRule         | Logic        |
| Candidate assembly   | DeductiveRule         | Logic        |
| Scoring              | Formula               | Computation  |
| Tokenization         | Formula               | Computation  |
| Verb handlers        | VerbHandler (new)     | Effect       |
| Effect execution     | Capability chain      | Authorization|
| Input text/selection | Attribute (facts)     | Data         |
