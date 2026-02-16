# Dialog NLP — Natural Language Parser via Dialog Discovery

A natural language command parser inspired by [Mozilla Ubiquity](https://github.com/mozilla/ubiquity),
built on dialog-db's fact store, rule system, and query engine.

## Core Thesis

Instead of a hardcoded command registry, the parser **discovers** verbs and nouns
through dialog itself. Verbs and nouns are facts and concepts in the triple store.
Noun types are derivation rules. Verb execution produces effects. The entire NLP
pipeline is expressed as queries and rule evaluation over facts.

---

## Ubiquity → Dialog Mapping

| Ubiquity Concept       | Dialog Equivalent                                       |
|------------------------|---------------------------------------------------------|
| Verb (command)         | Concept + Effect handler; facts describe its schema     |
| Noun type              | Derivation rule(s) that extract typed values from text  |
| Semantic role          | Attribute on argument slots (object, goal, source, ...) |
| Verb registry          | Query over verb concepts in the fact store              |
| Noun type registry     | Query over noun-type concepts / installed rules         |
| Parser suggestion      | Derived `Candidate` concepts, scored and ranked         |
| Command execution      | Effect invocation via capability system                 |
| Text input             | Fact: `{ the: "input/text", of: <session>, is: "..." }`|
| Selection              | Fact: `{ the: "input/selection", of: <page>, is: "..."}`|

---

## Architecture Layers

```
┌──────────────────────────────────────────────────────┐
│                    Input Layer                        │
│  text input, selection, context → asserted as facts   │
└────────────────────────┬─────────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────────┐
│                  Tokenizer Layer                      │
│  Formula: split input into Token facts                │
└────────────────────────┬─────────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────────┐
│             Verb Recognition Layer                    │
│  Query verb concepts; match tokens to verb names      │
│  → derives VerbMatch facts                            │
└────────────────────────┬─────────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────────┐
│           Argument Segmentation Layer                 │
│  Use preposition markers to split remaining tokens    │
│  into argument segments with semantic roles           │
│  → derives ArgumentSegment facts                      │
└────────────────────────┬─────────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────────┐
│             Noun Resolution Layer                     │
│  Apply noun-type derivation rules to each segment     │
│  Each noun type has recognizer rules that may or may  │
│  not match; matching produces typed NounMatch facts   │
│  → derives NounMatch facts with confidence scores     │
└────────────────────────┬─────────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────────┐
│            Sentence Assembly Layer                    │
│  Combine VerbMatch + NounMatches into Candidate       │
│  sentences; check argument type compatibility         │
│  → derives Candidate concepts                         │
└────────────────────────┬─────────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────────┐
│              Scoring / Ranking Layer                  │
│  Formula: score each Candidate by verb match quality, │
│  argument completeness, noun confidence, frequency    │
│  → derives RankedCandidate with composite score       │
└────────────────────────┬─────────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────────┐
│              Execution Layer                          │
│  Top-ranked candidate's verb is invoked as an Effect  │
│  via the capability system; resolved noun values are  │
│  passed as effect parameters                          │
└──────────────────────────────────────────────────────┘
```

---

## Key Design Decisions

### 1. Nouns Are Derivation Rules

A noun type is not a passive type tag — it is a **set of derivation rules** that
actively extract and validate typed values from text fragments. When you register a
noun type, you install one or more rules into the session. The parser discovers
available noun types by querying which rules are installed.

Example: a "language" noun type is a rule that:
- Takes a text fragment as input
- Matches against known language names/codes ("spanish", "es", "español")
- Derives a `Language { name, code }` concept with a confidence score

Both the input text and the derived concept are facts. The selection on a page is
also a fact that noun rules can operate on — enabling "translate this" where "this"
refers to the current selection.

### 2. Verbs Are Effectful Concepts

A verb is a concept in the store that describes a command:
- Its trigger name(s)
- Its argument slots with semantic roles and expected noun types
- A handler that produces an effect when invoked

The verb schema is data (facts). The verb handler is installed as a function that
receives resolved arguments and returns an effect. This separates the "what commands
exist" (queryable facts) from "what commands do" (installed handlers).

### 3. Semantic Roles as First-Class Attributes

Argument slots are tagged with semantic roles (object, goal, source, location,
instrument, time). The parser uses role-associated prepositions to segment input:

```
"translate hello to spanish from english"
         ↓         ↓          ↓
      [object]   [goal]    [source]
```

Roles are attributes on argument-slot entities, making them queryable and extensible.

### 4. The Parser Pipeline Is a Rule Cascade

Each layer of the parser is a set of rules that derive new facts from the previous
layer's output. This makes the pipeline:
- **Inspectable**: you can query intermediate results at any layer
- **Extensible**: add rules to any layer without modifying the pipeline
- **Composable**: layers are independent rule sets that chain through facts

### 5. Scoring Uses Formulas

Candidate scoring is a pure computation (Formula) that combines factors:
- Verb match quality (exact > prefix > substring)
- Argument completeness (all required args filled)
- Noun confidence (how well each noun type matched)
- Usage frequency (historical preference)

Formulas are side-effect-free, so scoring is a query, not a mutation.

---

## Noun Type in Detail

A noun type consists of:

1. **Registration facts**: `NounType { label, description }` concept in the store
2. **Recognizer rules**: installed in the session, derive `NounMatch` from text input
3. **Suggester** (optional): rules that produce default suggestions when no input matches

### Recognizer Pattern

A recognizer rule maps text to a typed value. The simplest form is a lookup table
(e.g., language names → codes). More complex forms use formulas for pattern matching.

```
Rule: recognize_language
  Given: Segment { text: ?text, role: ?role }
  When:  KnownLanguage { name: ?text, code: ?code }  // lookup fact
  Then:  NounMatch { text: ?text, noun_type: "language", value: ?code, confidence: 1.0 }
```

Or using a formula for fuzzy matching:

```
Rule: recognize_language_fuzzy
  Given: Segment { text: ?text, role: ?role }
  When:  FuzzyMatch { input: ?text, against: "language-names", result: ?name, score: ?conf }
         KnownLanguage { name: ?name, code: ?code }
  Then:  NounMatch { text: ?text, noun_type: "language", value: ?code, confidence: ?conf }
```

### Selection as Implicit Noun

The current selection is a fact. Noun recognizers can match against it:

```
Rule: selection_as_text
  Given: InputSelection { text: ?selection }
  Then:  NounMatch { text: ?selection, noun_type: "text", value: ?selection, confidence: 0.8 }
```

---

## Verb Type in Detail

A verb consists of:

1. **Registration facts**: `Verb { name, description }` concept, plus `ArgumentSlot`
   facts for each parameter
2. **Handler**: a function `fn(ResolvedArguments) -> Effect` installed in the session
3. **Aliases** (optional): additional trigger names as facts

### Argument Slots

Each argument slot is an entity with attributes:
- `Role`: semantic role (object, goal, source, ...)
- `NounType`: reference to the expected noun type entity
- `Required`: whether the argument must be filled
- `Prepositions`: words that introduce this argument ("to", "from", "in", ...)
- `Default`: optional default derivation rule

### Effect Production

When a verb is executed, it produces an `Effect` value that can be invoked through
the capability system. The verb handler doesn't execute side effects directly — it
returns a description of the effect to perform. This keeps the parser/resolver pure
and the effect execution controlled by capabilities.

---

## Discovery Protocol

Because verbs, nouns, and their relationships are all facts, discovery is a query:

```rust
// Discover all available verbs
let verbs = Match::<Verb> {
    this: Term::var("verb"),
    name: Term::var("name"),
    description: Term::var("desc"),
}.query(&session).try_vec().await?;

// Discover noun types
let nouns = Match::<NounType> {
    this: Term::var("noun"),
    label: Term::var("label"),
    description: Term::var("desc"),
}.query(&session).try_vec().await?;

// Discover arguments for a specific verb
let args = Match::<ArgumentSlot> {
    this: Term::var("slot"),
    verb: Term::from(translate_verb_entity),
    role: Term::var("role"),
    noun_type: Term::var("noun_type"),
    required: Term::var("required"),
}.query(&session).try_vec().await?;
```

New verbs/nouns can be added at runtime by asserting facts and installing rules.
The parser automatically picks them up on the next parse because it queries the
live store.

---

## Open Questions

1. **Async noun resolution**: Some noun types may need async operations (e.g.,
   querying a contact database). How does this interact with the streaming query
   model?

2. **Verb composition**: Can verbs be chained? ("translate this and email to Bob")
   Ubiquity intentionally avoided multi-clause commands. We may want to stay simple
   initially.

3. **Learning/adaptation**: Can the scoring system learn from user choices over time?
   This could be modeled as frequency facts that the scorer queries.

4. **Localization**: Ubiquity's Parser 2 used abstract semantic roles to support
   multiple languages. The same approach works here since roles are attributes, and
   preposition→role mappings are facts that can vary by locale.
