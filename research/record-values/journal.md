# Record values: journal

What we tried, what it measured, and what we decided, newest last. The
[topic](../record-values.md) says why and sets the bars.

## Where things stand

**Status: Exploring.** See the [research index](../README.md) for what the
statuses mean.

- **Built:** nothing.
- **Decided:**
  - A record is an atomic value.
  - It carries no type tag. Its fields are typed on read by attributes,
    as claims are, and an attribute with no `as:` takes any value.
  - Fields are named by attributes and destructured as `{lat ?lat}`.
  - A field may hold several values, so a record is a set of pairs.
  - A field may hold an entity, which is followed by an ordinary join, or
    a nested record, which is destructured.
  - Selecting an attribute finds only claims the replica stores. A
    record's fields are never claims: they are reached by destructuring a
    bound record, never by selection, and a record cannot stand where an
    entity is expected (`of`).
  - Equality follows tokens as key comparison does, resolving them across
    sources, and the wire carries the canonical form.
  - Repetition is expected mostly across records, and shared through the
    replica's tokens.
- **Decided, continued:** in destructuring, a field is named by an
  attribute, which brings its pick and its type. `last` over a field
  holding several values elects as `last` does between values one
  revision wrote: the greater value.
- **Open:**
  - the surface of destructuring: a pattern in the notation, a formula
    over a path, or both;
  - documents that grow (options in the third entry);
  - whether an attribute can require its records to satisfy a concept;
  - how a reader tells today's dag-cbor records from the new layout.
- **Next:** a census of the records in tonk-team's history: how many, their
  sizes, and how much repeats across them.
- **Blocked:** neither dataset is in this session's container.

## Datasets

- **tonk-team:** the team space's pushed history, the 9,070 blocks #733
  fetched (568.5 MiB). Its records are revision records and tonk's view
  bindings and embeds. Not committed; read from a copy.
- **Stack Exchange:** the `retrocomputing` replay (`DIALOG_SE_CSV`). Its
  only records are revision records, one per commit, so it measures those
  at scale and nothing else.

## 2026-11-06 — Framing: what a record is, and where sharing lives

**Question.** What should a record value be, given a placeholder, three
ad hoc dag-cbor uses, and the single-file archive's tokens?

**Method.** Read `notes/record-value.md`, `Value::Record`'s keying
(`ordvalue.rs`, `key.rs`), every writer of a record in dialog and tonk,
#636 and #733, and the fressian and schemaboi documentation.

**Results.**

- Every record today is dag-cbor, and each writer invents its own type
  tag: the attribute for revisions, a `kind` field for tonk's bindings.
- Fressian, schemaboi and FlexBuffers each let the writer choose an
  encoding, so none is canonical as it stands. Fressian's cache and
  schemaboi's schema depend on the stream or file they sit in.
- A record's bytes are hashed, sent in blocks and signed, so they must
  stand alone. Sharing across records belongs to the file, as it does for
  nodes.

**Decision.** A record names its type and has one canonical, standalone
encoding; equality is of those bytes, and token ids compare them inside a
replica. Field names live in the record, written once, with later
occurrences referring back. Where the shared strings sit (one region or
interleaved) and whether references are offsets is for the spike. The bars
in the topic are proposed and await the maintainer.

## 2026-11-06 — Equality follows tokens, and fields are attributes

**Question.** What a record's canonical form has to carry, once the
replica shares tokens across records, and how its fields are named.

**Method.** Discussion; nothing measured.

**Results.** As the maintainer stated them:

- Equality follows the pattern of key comparison: records read over the
  same source compare by their token references, records from different
  sources resolve their tokens, and the wire carries the resolved,
  canonical form.
- Repetition is expected across records, not within one.
- Fields are named by attributes: in `{lat ?lat}`, `lat` is an attribute
  reference, not a free name.
- Records are flat for now; nesting may come later.

Consequences raised, not yet decided:

- `Hash` cannot take the same-source shortcut: equal records from
  different sources must hash alike, so it runs over the canonical form,
  computed once and kept with the value.
- A sorted run of attribute and value pairs is an EAV run without its
  entity. Laid out as key tails, a record would compare, find a field and
  split into file tokens with the code and the tokens keys already use.
- Such a record is a concept instance without `this`. A concept's identity
  is the sorted set of its attributes' identities (`notes/concept.md`), so
  the attributes may already name the record's type.
- The alternative is claims on an entity identified by the hash of its
  pairs: atomic through the claim that refers to it, every field indexed,
  at (n+1) claims in three orderings per write, the cost #636 measured.

**Decision.** The four results above are settled. They replace the first
entry's string table inside the record and its "later occurrences
referring back": sharing moves from the record to the replica's tokens.
Not decided: whether attributes name the type, value or entity, and
many-valued attributes.

Commits: this entry.

## 2026-11-06 — Fields are typed on read, records are values, a field holds a set

**Question.** How a record is typed, whether it is a value or claims on an
entity, and whether a field holds one value or several.

**Method.** Discussion. Read how dialog types attributes
(`notes/concept.md`, `AttributeDescriptor` in
`dialog-query/src/attribute/descriptor.rs`, `type_system.rs`) and what
happens to concurrent values (`notes/version-control.md`).

**Results.**

- Dialog stores no type beyond each value's own type byte. An attribute
  is a relation read under an optional type and a pick
  (`AttributeDescriptor::content_type` is an `Option`); two attributes over
  one relation under different types are two attributes. Concepts are
  read, not written: one entity satisfies any number of them.
- So a record needs no type tag. A field stores its relation and its value
  with the value's type, as a claim does, and an attribute's `as:` reads
  it. A record satisfies whichever concepts its fields satisfy. Tonk's
  `kind` check becomes reading by concept; whether that refuses foreign
  data as reliably is a hypothesis.
- A record is an atomic value (the maintainer). The alternative, claims on
  an entity whose identity is the hash of the record's pairs, is set
  aside, and with it indexed fields.
- A field may hold several values: a record is a set of pairs, sorted,
  without duplicates.
- Corrections to the previous entry: flat is preferred, not decided, and
  repetition inside a record is possible, only not expected to be common.
  A field value is any dialog value and a record is one, so a record nests
  unless that is forbidden; what is undecided is whether destructuring and
  import recurse.

Raised, not decided:

- `last` has no meaning inside a record: its pairs have no causal order.
  `all`, `max`, `min` and `top` read a set of pairs as they read claims. A
  `last` attribute over a field holding two values needs a rule.
- Documents that grow, such as automerge. As one value, every edit writes
  the whole document, every version is kept, and concurrent edits both
  survive while `last` shows one (`notes/version-control.md`: concurrent
  values "both survive ... until a later write that has seen both cites
  and supersedes them"). As a set of changes under an `all` attribute, a
  write costs one change, a merge between replicas is a union of the sets
  (which is automerge's own merge), and a snapshot that retracts the
  changes it covers leaves concurrent changes standing, since a
  retraction removes only the claims its author saw. The second needs no
  record at all. #388, which put automerge in records, was closed as
  "reconsidered".

**Decision.** Records are atomic values with no type tag, typed on read
by attributes; a field holds a set. This replaces the first entry's "a
record names its type". Open: nesting, `last` over a set, documents that
grow.

Commits: this entry.

## 2026-11-06 — Destructuring is not selection

**Question.** What "fields are not indexed" means, whether records nest,
and how reading a field relates to selecting an attribute.

**Method.** Discussion. Read how the `of` slot is typed
(`dialog-query/src/attribute/query/all.rs`) and how the unifier treats a
conflict (`type_system/unifier.rs`).

**Results.** As the maintainer stated them:

- Selecting `geo/lat` finds nothing unless the replica stores `geo/lat`
  claims. Holding a record with a `geo/lat` field does not change that.
- `{ location: { geo/lat: ?l } }` destructures: it reads a field out of a
  record already bound. Binding a record to `?p` and then selecting
  `geo/lat` `of: ?p` is not allowed, because selection must work with its
  entity unbound, and a record's fields are not in the indexes it scans.
- A field may hold an entity, whose own claims are found by an ordinary
  join, or a nested record, which is destructured. Records nest.
- A formula reading a record by path (jq notation was suggested) is an
  alternative surface; it is still destructuring, not selection.

Found in the code:

- `of` is typed `entity` (`all.rs`), and the unifier refuses an empty meet
  (`unifier.rs`). A record-typed binding used as `of` is therefore refused
  when the rule is compiled. When the binding is untyped, it is refused
  only when the row is read, presumably as no row; that is a hypothesis.
- "Fields are not indexed", in the previous entry, says the same as the
  first result: only the whole record is a value in the indexes, found by
  equality.

**Decision.** Records nest. Destructuring reads a bound record and never
scans; selection sees only stored claims. This replaces the earlier
"flat preferred". Not decided: whether an attribute reference brings its
type and pick into destructuring, and the surface of destructuring.

Commits: this entry.

## 2026-11-06 — `last` inside a record elects by value

**Question.** What a `last` attribute reads from a record field holding
several values.

**Method.** Discussion. Read `ArtifactView::elect`
(`dialog-artifacts/src/artifacts/artifact.rs`) and `value_beats`
(`dialog-artifacts/src/artifacts/update.rs`).

**Results.**

- The maintainer: `last` can elect inside a record as queries already do.
- `elect` decides in three tiers: the revision's standing (edition, then
  the hash of the version), the cause, then the value, the greater
  winning (`value_beats`: the values' own order, their encoding's
  otherwise). The hash orders revisions, not values.
- Every pair of a record was written by one revision, so the first two
  tiers tie and `last` falls to the value. Inside a record, `last` reads as
  `max` does, which is what it already reads between two values one
  revision asserts. Concurrent records still compete as whole claims, by
  revision.

**Decision.** In destructuring, a field is named by an attribute, which
brings its pick and its type; `last` over several values elects the
greater. The type applying is inferred from the pick applying, as it
does for a concept's field, and is to be confirmed. This replaces the
earlier proposal to refuse `last` over several values.

Commits: this entry.

## 2026-11-06 — The topic, revised to the decisions so far

**Question.** What the topic states, now that the first entry's framing
is replaced.

**Method.** Rewrote [the topic](../record-values.md) from the entries
above. Nothing measured.

**Results.** The topic now has a question and a hypothesis (records as
sorted pairs laid out as key tails, sharing the replica's tokens), the
decisions above, and four variants against main. The string table inside
a record, the type tag and the candidate encodings B and C (CBOR with
stringref, a canonical FlexBuffers profile) are gone. One question is
new: with no type tag, a reader still has to tell today's dag-cbor
records from the new layout.

**Decision.** The bars changed with the hypothesis: a size bar on the
wire, allowed 25% over dag-cbor because attributes are full relation
names, and a size bar in the file. They were proposed, never set, and no
result has been seen, so nothing was moved after a result. They remain
the maintainer's to set.

Commits: this entry.

## How to reproduce

Nothing to run yet.
