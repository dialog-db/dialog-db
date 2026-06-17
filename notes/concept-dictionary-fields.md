# Dictionary fields on concepts via `..` suffix

Status: design note, not yet implemented.

## Idea

A concept's schema lists fields. Most fields bind to a single attribute
(`the: domain/name`, `as: <type>`). Some fields should instead bind to
*every* attribute under a domain — a dictionary keyed by the name half.

The rule: **any field name ending in `..` is a dictionary field**. The
`..` suffix toggles the field's interpretation. Nothing else changes:
`as:` still describes the value type, `cardinality:` still describes
multiplicity, and the field appears alongside ordinary named fields.

```yaml
db:concept!: &concept
  description:
    the: dialog.meta/description
    as: text
  with..:
    domain: dialog.concept.with
    as: AttributeDescriptor
```

`description` is a regular named field bound to `dialog.meta/description`.
`with..` is a dictionary field that collects every `dialog.concept.with/<x>`
attribute on the entity, exposed under the field name `with` with the
trailing `..` stripped (or kept; see "Bikeshed" below). The field's
value is a dictionary keyed by `<x>` whose entries are typed by `as:`.

## Why suffix and not keyword

Earlier sketches reserved `with..` as a keyword paired with `with`. That
imported new vocabulary and only supported one dictionary per concept.
The suffix rule is strictly less special:

- It's not a new keyword. It's a *spelling rule* on field names.
- A concept can have any number of dictionary fields — `with..`,
  `prefs..`, `views..`. Each picks its own domain.
- There's no collision question between named and dictionary fields,
  because each field has a distinct name (the trailing `..` is part of
  the name in the schema document).
- Ordinary named fields remain the only "non-dictionary" mechanism.
  `maybe` (optional) is orthogonal and works the same way for either
  kind of field.

The Concept-of-Concept bootstrap falls out: Concept's own schema is

```yaml
db:concept!: &concept
  description:
    the: dialog.meta/description
    as: text
  with..:
    domain: dialog.concept.with
    as: AttributeDescriptor
```

`with..` is a dictionary of `AttributeDescriptor` entities, keyed by
each entry's field name. Self-referential and well-founded because
the dictionary type is structural, not bespoke.

## Field shape

A dictionary field's schema entry looks like:

```yaml
<name>..:
  domain: <symbol>          # required: the attribute domain to scan
  as: <type-or-concept>     # required: per-value type
  cardinality: one | many   # optional, default one; per-entry, not per-dict
```

`domain:` replaces `the:` — instead of pinning to a single
`domain/name`, the field claims the whole `domain/*` prefix. Everything
else (`as:`, `cardinality:`, optionality) means exactly what it means
on a regular field, just applied per dictionary entry.

`as:` accepts:
- A primitive type (`text`, `entity`, `u64`, ...), giving
  `Dict<Symbol, T>` where `T` is that scalar.
- A concept reference, giving `Dict<Symbol, Entity>` where each entity
  is constrained to be an instance of that concept. The concept-name
  in `as:` is the same lift that lets ordinary fields reference
  concepts; it's not specific to dictionary fields.

Cardinality is per entry, not per dictionary:
- `one`: each (entity, name) pair has at most one value; the
  dictionary's per-key type is `T`.
- `many`: each key can have multiple values; per-key type is `Vec<T>`.

Empty dictionaries are always allowed. Absence is not separately
marked; a missing key simply has no entry.

## Rust surface

A concept struct field for a dictionary entry is a `HashMap`:

```rust
#[derive(Concept, Debug, Clone)]
pub struct ConceptInstance {
    pub this: Entity,
    pub description: Description,
    #[concept(domain = "dialog.concept.with")]
    pub with: HashMap<Symbol, Entity>,
}
```

The macro recognizes the `domain` attribute as the directory marker.
The corresponding schema field name is `with..`; the Rust field name
loses the trailing `..` (see "Bikeshed"). The value type informs `as:`
in the descriptor, and `Symbol` keys are required.

## Storage semantics

Storage is unchanged from today. A dictionary entry is just an
ordinary attribute `<domain>/<name>` on the entity. The dictionary
abstraction lives entirely in how the schema reads and writes those
attributes.

## Assert semantics

On `push_statements`, a dictionary field iterates its map and emits
one `AttributeStatement` per entry:

- `the` = `<domain>/<map-key>` (joined as a `Symbol` pair),
- `of`  = the entity,
- `is`  = the map value,
- `cardinality` = the descriptor's cardinality,
- `cause` = `None`.

Each entry is indistinguishable from a hand-written required attribute
under the same domain. Empty maps emit no statements.

This is symmetric with `Option<N>` realize: required fields emit 1
statement, optional fields emit 0 or 1, dictionary fields emit N.

## Query semantics

A dictionary field requires a prefix scan over the attribute index,
constrained to the entity and the dictionary's domain. This is what
the directory branch's `ArtifactSelector::with_domain(domain_symbol)`
already supports — domain-only is `Constrained` and selects a
contiguous range.

The rule body emitted by `#[derive(Concept)]` for a dictionary field
is a single domain-bound `AttributeQuery` premise plus a collector
projection that groups the resulting rows by (entity, name half) and
folds them into the field's `HashMap`. This is a new shape for the
query pipeline: today every `with`/`maybe` slot binds to a single
value per row, but a dictionary field binds to an aggregation.

Two implementation strategies:

- **Side query**: leave the main pipeline scalar; the macro emits a
  post-step that runs the prefix scan for each conclusion's `this`
  before constructing the struct. Cheap to build, but O(rows × scan)
  work.
- **Multi-valued binding**: extend the binding type to carry an
  aggregation slot. The query engine performs the grouping inline,
  one row per `this`. Bigger churn on the binding type, but the right
  asymptotic.

Side query first; multi-valued binding when usage demands it.

## Pull-named single-key access

Pulling a specific key by name is unchanged: it's an ordinary
attribute fetch on the entity (`<domain>/<key>`). Schema authors who
want "give me view `x`" don't need to materialize the whole dictionary
— they just query the attribute directly. The dictionary
materialization is for cases where the whole prefix is wanted.

The view system falls naturally into this: a `views..` field with
`domain: "view"` materializes every named view; a single
`branch.entity(this).get("view/x")` reads one without touching the
dictionary.

## Optionality

A dictionary field is "optional" in the trivial sense that an empty
dictionary is always representable. There's no `Option<HashMap<_,_>>`
case — the absence of any entries *is* the empty state. If a concept
wants to distinguish "no directory" from "empty directory", that's an
extra signal (some other named field saying so), not something the
dictionary mechanism encodes.

This means `maybe..` as a field name is fine syntactically — it just
means a dictionary field that lives in the optional half of the
concept descriptor's storage — but the practical difference from
`with..` is minimal, since empty maps already model absence. The
distinction may still be useful for concepts that distinguish
"definitely no entries" from "haven't checked yet"; that's a
schema-author choice.

## Bikeshed: keeping or stripping the `..` in Rust

The schema document carries `with..` as the field name. Two options
for the Rust side:

- Strip the suffix: Rust field is `with`, schema is `with..`. The
  trailing `..` is purely a schema-document signal. Rust code reads
  cleanly.
- Keep the suffix: Rust field is `with..` (illegal in Rust today,
  would need a rename via the derive macro). More faithful to the
  schema text, but requires an attribute like
  `#[concept(name = "with..")]` on every dictionary field.

Strip is cleaner; the `..` is a schema-language artifact, not data.

## Conflict rules

- Two fields cannot claim overlapping attribute coverage. A named
  field with `the: "dialog.concept.with/title"` and a dictionary
  field with `domain: "dialog.concept.with"` both claim the
  `dialog.concept.with/title` attribute. The schema validator rejects
  the combination; pick one.
- Two dictionary fields with the same `domain:` are also rejected.
- Two dictionary fields with disjoint domains coexist freely.

## What this unblocks

The directory branch's storage and selector groundwork
(`ArtifactSelector::with_domain` for prefix scans) is the prerequisite.
This note specifies the schema-layer and query-layer mechanics on top.

The Concept-of-Concept bootstrap is the main payoff: once dictionary
fields exist and `as:` accepts concept references, Concept's own
schema is expressible as a concept, and so is every meta-concept
built on top — views, lenses, registries — without any of them
needing a privileged position in the schema language.
