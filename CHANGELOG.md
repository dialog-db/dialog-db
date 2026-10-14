# Changelog

What changed, what it may break, how to move across, and why we did it.
Entries are grouped by the change a reader has to understand, not by
commit; each links the pull request that landed it and the note that
argues it in full. Newest first.

Versions follow [semver](https://semver.org) over two surfaces: the Rust
API, and what a replica stores and syncs. While dialog is 0.x, a change
that breaks either one bumps the minor version, and anything else bumps
the patch. The whole workspace shares one version. `0.1.0` is the commit
tagged `tonk-2026-10-03`, the last of the date tags. [RELEASING.md](./RELEASING.md)
says how to cut a release.

## Unreleased

### The node cache is bounded by bytes and shared between scopes

[#573](https://github.com/dialog-db/dialog-db/pull/573)

`NodeCache` is its own type. It holds up to a budget of stored node
bytes (`NODE_CACHE_BUDGET`, 128 MiB, unless built with another), evicts
by SIEVE, allocates nothing until a node is kept, and serves a node
larger than it can keep without keeping it. A handle has a `Scope` and
is answered only with nodes its scope holds: `scoped` hands out a handle
on the same cache for another scope, and `release` forgets what a scope
held. A node two scopes hold is kept once.

What this changes for you:

- `NodeCache` is no longer an alias of `Cache`. Build it with
  `NodeCache::new()` or `NodeCache::with_budget(bytes)`.
- A cache that held 2,048 nodes of any size now holds 128 MiB of node
  bytes, whatever their count. What a node derives from its bytes
  (decoded keys, key hashes, its summary) is not charged, so resident
  memory can run to about twice the budget.
- `Cache<K, V>` is unchanged and still serves the count-bounded caches of
  small values.

Why: nodes differ in size by orders of magnitude, so a count bounded
nothing; and a cache that repositories share must never answer one with
a block only another holds.

### Measurement counters are compiled in by an `audit` feature

[#570](https://github.com/dialog-db/dialog-db/pull/570)

The search tree counted every hash its shaping paths asked for, and the
volatile archive looked each block up before storing it to count
duplicates, in every build. Both now exist only with the `audit` feature
of `dialog-search-tree`, which turns on `dialog-storage`'s.

What this changes for you:

- `dialog_search_tree::audit::report` and
  `dialog_storage::dup_audit::drain_sets` exist only with the `audit`
  feature. The public counter statics are gone.
- `Volatile` no longer reads a block before storing it unless the
  feature is on.

Why: measurement plumbing should cost nothing in a build that does not
measure.

### The hashes a tree is shaped by are kept by what owns the bytes, not in per-thread memos

[#569](https://github.com/dialog-db/dialog-db/pull/569), fixes
[#566](https://github.com/dialog-db/dialog-db/issues/566)

The boundary rules decide from the blake3 of a key or a separator, and
ask again on every regroup. Two thread-local memos answered them: every
thread that committed filled its own and kept it for the thread's life,
with no way to see, bound or release it. A full one held about 46 MiB at
257-byte keys, per thread. Now an entry keeps the hash of its key, a
separator its own, and a stored node the hashes of its keys and its
piece summary, each computed on first ask and dropped with its owner. A
node a persist seals is kept in the tree's node cache. Trees are shaped
byte for byte as before; nothing stored changes.

What this changes for you:

- An `Entry` is built with `Entry::new(key, value)`. It keeps its key's
  hash, so replace an entry whose key changes rather than assigning the
  key in place.
- `Link::separator` is a `Separator`, which derefs to `[u8]` and is
  built `From<Vec<u8>>`. `Link` no longer derives its rkyv form.
- `Distribution::rank`, `seam_rank` and `leaf_cut` take a `Hashed<'_>`:
  the bytes with wherever their hash is kept. `Hashed::from(&bytes)`
  wraps bytes with none.
- `TransientNode::persist` and `persist_mut` take the node cache the
  sealed nodes are kept in.
- A process that commits on many threads no longer grows a memo per
  thread.

Why: a memo with no owner cannot be bounded or released, and one per
thread multiplies whatever it holds by the size of the thread pool.

## 0.3.0 (2026-10-13)

### Every number is keyed in one band, and a branch an earlier release wrote is migrated in one pass

[#593](https://github.com/dialog-db/dialog-db/pull/593),
[identity, storage and notation](./notes/identity-storage-notation.md#numbers-one-band-typed-views)

A tree's manifest now sets `value_encoding` to the number band: every
number is keyed in one band of the value index, in dialog-number's
decimal encoding, the encoding identity already used. `3`, `3.0` and
the natural `3` are one value in the index as they are in identity, and
a range over a numeric attribute is one scan rather than three. The
manifest field is critical, so a release before the band refuses a
migrated tree by name rather than misreading it.

A branch an earlier release wrote is read, never extended:
`Branch::migrate` rewrites its head's tree under the band
(`migrate_storage`) and then re-installs its rules (`upgrade_rules`),
one call for everything an earlier release stored. The rewrite is
deterministic, so two replicas that migrate the same head produce the
same tree, and a pull that meets a head an old client wrote rewrites
it in memory before merging, so the old client's writes are not lost.

What this changes for you:

- A commit on a branch whose head an earlier release laid out fails with
  `CommitError::NeedsMigration` and writes nothing. Run
  `Branch::migrate` (or `migrate_storage`) first; `Branch::needs_migration`
  says whether a branch is in that state, and `storage_encoding` which
  layout its head has.
- A number read back raw (an `ArtifactSelector` select, a `Value` match)
  comes back as the narrowest type that holds it: a natural when it is
  whole and not negative, an integer when whole and negative, a float
  otherwise. Code matching `Value::UnsignedInt` on counts and timestamps
  still sees them; code matching `Value::Float(5.0)` on a stored `5.0`
  now sees `UnsignedInt(5)`. A typed read (a concept field, a derived
  struct) views the number as the type it declares, unchanged.
- An `ArtifactSelector` range bound reads each number through the lens
  its type names before comparing, as a field declared with that type
  reads it: an integer bound sees `34.5` as `34`, a natural bound does not
  see `-3`, a float bound sees every number as it is, and
  `across_numbers()` compares every number as it is for any bound.
  Before, a bound admitted only numbers written with its own type; a
  natural bound `>= 20` now finds a stored `30.0`.
- An exact number, in a selector's `is(..)` or a query's constant, is the
  range of that one number read through the same lens: `is(4)` as an
  integer finds the stored `4`, `4.0` and `4.3`; `is(4.0)` as a float
  finds the number `4` alone. This holds on the cardinality-one election
  too, which compared the constant with the stored value by type and so
  missed `+41` against a stored natural `41`.
- A cardinality-one assertion of a value its cell already reads as,
  through the lens of the asserted value's type, is a no-op
  (`Value::reads_as`): asserting the integer `4` over a stored `4.3`
  writes nothing, since an integer reader observes `4` already; asserting
  `5` supersedes it.
- A float is the decimal it prints as, in memory as in the band:
  `Value::viewed_as` reads `1e30` through an integer lens as the natural
  `10^30`, not the float's binary value, and `Value::same_number` says
  two values are one number exactly when the band keys them as one, so
  `2^64` as a float is the natural `18446744073709552000` and not
  `2^64`. Before, the in-memory views and the stored identity disagreed
  past `2^53`.
- Rows a query derives are one row per number: two rules deriving the
  natural `3` and the float `3.0` for one relation yield one row, through
  a union and through a fixpoint alike, as two such facts are one fact in
  storage; an `all` pick tells values apart the same way
  (`Value::identity_key`, the band's encoding of a number). Before, the
  rows were keyed by type as well, and a `number:` reader saw both.
- A number constant finds its matches on every path: the cardinality-one
  lookup with the entity unbound compared the constant with the elected
  claim by type, and a variable bound to a stored `4.3` refused the
  constant `4` that arrived after it; both now read through the lens
  (`Value::reads_as`), in either order.
- A concept write (`ConceptDescriptor::edit`, the derive's builders)
  writes each field under the field's pick, as an attribute statement
  spelling that pick does: a `last` field's write succeeds the claim its
  cell holds, and is the no-op above when the cell already reads as the
  written value. `Attribution` carries the field's `pick` in place of its
  `cardinality`. Before, every concept write was an `all` write, so a
  cardinality-one field accumulated a claim per write; reads elected the
  newest, so they read right while the cell grew.
- The band and the identity carry a number of any size (dialog-number
  keys any decimal), but `Value` does not yet: it holds `u128`, `i128`
  and `f64`, so no wider number enters, and a stored number wider than
  `u128` reads back as the nearest float. A `Value` variant holding a
  `dialog_number::Number`, with the fixed-width types as views over it,
  is planned for the next cycle; it changes no key and no identity, so
  nothing stored now will need rewriting for it.
- `ValueDataType::holds` reads the same for every number type: a type
  holds a number its view reads unchanged, so `float:` holds the natural
  `3` and `natural:` the float `3.0`, as `float:64` and `natural:8`
  already did; `natural:` still refuses `2.5` and `-1`. Before, an
  unsized type held values of its own type only.
- `tree::rekey::rekey_tree` and `rekey` rewrite a tree or a key between
  encodings; `Branch::rekey_storage(0)` lays a branch out the earlier
  way, for tests of the migration.
- A pull that merges a head an earlier release wrote rewrites it in
  memory first, so both sides share one encoding and the old client's
  writes merge in. A pull that fast-forwards adopts such a head as it is,
  reading nothing, and the branch then needs migrating like any other.

Why: identity and storage should agree on what a number is, a numeric
range should be one scan, and the move should happen once, with the
rule and identity upgrades, rather than as a second migration later.

### A selection reads a bare constant as the type its field declares

[#593](https://github.com/dialog-db/dialog-db/pull/593)

`select(ConceptQuery)` now conforms each constant to the type its field
declares, as compiling a rule already did for a premise's constants: a
bare text `"home:"` under a `text:` field is the text, not an entity,
and a bare `5` under `integer:` is a signed integer. A constant `this`
is an entity.

What this changes for you:

- A query whose constant was written bare in JSON matches the claims
  the field holds. Before, `"url": "home:"` under a `text:` field read
  as the entity `home:` and matched nothing.
- Every evaluation of a `ConceptQuery` conforms its constants, through
  `select`, a plan's concept premise, or the query's own `evaluate`.
  Before, only `select` did, and a plan built by an embedder read `+41`
  under a `natural:` field as a signed integer that matched nothing.
- A constant in a field with no declared type still reads as the type
  its spelling suggests.
- A bare text constant shaped like a URL (`https://example.com`) is
  still read as an entity first, and comes back as the URL's normalized
  text (`https://example.com/`). Write such a constant tagged,
  `{"text:": "https://example.com"}`, until the reader takes every bare
  string as text.

Why: a constant written bare has no type of its own; the slot it fills
does, and reading it any other way makes a query miss what it names.

### Rule bodies store the canonical spelling

[#593](https://github.com/dialog-db/dialog-db/pull/593),
[identity, storage and notation](./notes/identity-storage-notation.md#body-format-3)

A rule's `dialog.rule/source` body is now written in format 3: the
rule's canonical spelling, the one its identity hashes, in the dag-cbor
envelope earlier formats used. The body holds exactly what the identity
holds, so every spelling of one rule stores the same bytes, and a
rule's body is found by encoding the rule.

What this changes for you:

- `DeductiveRule::decode` and `InductiveRule::decode` of a format 3
  body give the rule in its canonical spelling: locals and head fields
  named `~0`, `~1`, …, a premise's fields `#0`, `#1`, …, premises
  sorted, no descriptions. A query still binds the head by its own
  concept's field names. The names and descriptions an author wrote
  are not stored.
- Installing another spelling of an installed rule stores no second
  body, and retracting either spelling removes the rule's body.
  Before, two spellings stored two bodies under one entity, and
  retracting one left the other.
- A rule with a constant no body can carry is refused when it compiles
  with `TypeError::UnencodableConstant`, naming the constant: a NaN or
  infinite float, a natural above `u64::MAX` or an integer outside
  `i64`. Before, such a rule compiled and panicked when stored.
- `Branch::upgrade_rules()` also rewrites every body of an earlier
  format into format 3, and reports those already under their identity
  in `RulesUpgraded::rewritten`.

Why: a stored rule should be one record that is a function of the
rule, so equal rules store equal bytes and a retraction finds what an
install wrote. The authored spelling is display data, and storing it
beside every rule measured at 35% to 75% more instructions per
install; it waits on a cheaper form.

### NaN is not a value

[#593](https://github.com/dialog-db/dialog-db/pull/593),
[identity, storage and notation](./notes/identity-storage-notation.md#decided)

A NaN float is refused wherever a value enters dialog, rather than
stored as a number that is not equal to itself. Infinities stay
values.

What this changes for you:

- Deserializing a `Value` from a NaN fails with "NaN is not a value",
  bare or tagged (`{"float:": NaN}` in dag-cbor), as does parsing the
  string `float:NaN` into a `Value`. No type `holds` a NaN, so a typed
  write refuses it, and a commit that would write one by a raw assertion
  fails with `CommitError::Artifact(InvalidValue("NaN is not a value"))`
  before anything lands.
- `float/parse` of `NaN` is no row.
- A float formula whose result would be NaN (`0.0 / 0.0`,
  `inf - inf`) yields no row, as integer division by zero already did.
- A rule constant of NaN is refused when the rule compiles (see above).

Why: a database value needs an equality it can join and retract by.
The absence of a claim already means "no number"; a value that exists
only to say so, and that IEEE says is unequal to itself, had to be
special-cased in equality, in the `float:32` view and in the number
encoding to behave at all.

### Attributes can declare `number:` and fixed-width number types

[#593](https://github.com/dialog-db/dialog-db/pull/593),
[identity, storage and notation](./notes/identity-storage-notation.md#number-types)

An attribute may declare `as: number:`, which holds every number, or a
fixed-width number type: `natural:8` to `natural:128`, `integer:8` to
`integer:128`, `float:32` and `float:64`, one for each Rust number.
`natural:`, `integer:` and `float:` stay as they were, without a width.
A fixed-width type is a view over the stored number. On read, it coerces
where it only loses precision and leaves out what it cannot hold:
through `natural:8`, a stored `9.75` reads as `9`, and `300` and `-1`
are not rows. A write refuses a value the type cannot hold. Values are
still stored as naturals, integers and floats, as before.

What this changes for you:

- `#[derive(Attribute)]` on a fixed-width Rust number declares its
  sized type: `struct Level(u8)` declares `natural:8`, and `f32`
  declares `float:32`. The identities of these attributes, and of the
  concepts and rules that use them, differ from those declaring
  `natural:`, `integer:` or `float:`. `usize` still declares `natural:`,
  because it has no width common to every target.
- `Typed::DECLARED` gives the type an attribute over a Rust type
  declares. `TypeDescriptor::TYPE`, and the kind of a `Term<u8>`, are
  unchanged.
- `ValueDataType` gains `Number`, `U8` to `U128`, `I8` to `I128`, `F32`
  and `F64`. No stored value has these types: they type attributes and
  query slots. `ValueDataType::holds(&value)` says whether a type admits
  a value, and `Value::viewed_as(kind)` reads a value through one.
- The query type system gains `Refinement::views`, sent as `"views"`
  on the wire.
- A view reads the same on every target: rounding to `f32` is pinned
  bit for bit on native and WebAssembly.

Why: a Rust field of `u8` cannot hold `300`, and an app should be able
to say so in the schema rather than fail on read. `number:` gives an
attribute that holds numbers of every type, which JSON already writes
as naturals, integers and floats.

### Rules are identified by merkle references

[#593](https://github.com/dialog-db/dialog-db/pull/593),
[identity, storage and notation](./notes/identity-storage-notation.md#plan)

A rule's identity is now the merkle reference of its canonical
spelling under `rule:`, not the BLAKE3 hash of its dag-cbor. The head
is its concept by reference with each field bound to its canonical
variable; `when` and `unless` are sets of premises; a concept premise
is its concept by reference with each binding keyed by the field it
binds (attribute and options), not by a field name; other premises
and reduce clauses are their wire form read as a merkle structure,
with constants identified by value.

What this changes for you:

- Every rule identity differs from 0.2.0's and from 0.3.0-rc.1's.
  `Branch::upgrade_rules()` re-installs stored rules under the new
  identities.
- An attribute's description no longer changes the identity of a rule
  that uses it, as it did when rules embedded their concepts' wire
  form.
- A rule's numeric constants are identified by value: a rule with the
  constant `5` written as a natural, an integer or a float is one
  rule.

Why: a rule's identity should depend on what it means, and the
concepts it uses should enter by reference, so a rule is the same
whether they are embedded or linked.

### Attributes and concepts are identified by merkle references

[#593](https://github.com/dialog-db/dialog-db/pull/593),
[identity, storage and notation](./notes/identity-storage-notation.md#identity-merkle-reference-v2),
[merkle-reference v2](./rust/merkle-reference/spec.md)

An attribute's and a concept's identity is now a merkle reference
(`merkle-reference`, a new crate in this workspace), not the BLAKE3
hash of their dag-cbor. Types are named by URI; the crate defines only
the shapes of composites (`list:`, `map:`, `set:`), and dialog names
its own scalar types (`text:`, `boolean:`, `number:`, `entity:`,
`symbol:`, `record:`). A definition is hashed as a map from field
names under its own type (`the:`, `concept:`), and a concept hashes its
attributes' and its conformance targets' references, so it is the same
whether they are embedded or linked. The entities keep their shape,
`the:<base58>` and `concept:<base58>`, with other digits.

What this changes for you:

- Every attribute and concept identity differs from 0.2.0's and from
  0.3.0-rc.1's. Rule identities change too (see above).
- `AttributeDescriptor::hash()` and `ConceptDescriptor::hash()` are
  `reference()`, returning a `merkle_reference::Reference`.
  `to_cbor_bytes()` is gone from both. `AttributeDescriptor::parse_uri`
  returns a `Reference`.
- `Value::reference()` gives a value's identity. Every number has one
  `number:` type and is identified by its value alone, in the encoding
  of the new `dialog-number` crate (SQLite4's decimal key encoding): a
  natural `5`, an integer `5` and a float `5.0` are one value, a float
  is the decimal it prints as (`1e23` is 10²³), and `-0.0` is `0`. An
  attribute's `as:` list hashes values this way.
  The value index still keeps naturals, integers and floats apart, so
  until it moves to one band, two attributes that differ only by
  listing `5` or `5.0` share an identity but read different claims.
- `dialog_artifacts::identity` and `dialog_query::identity` hold the
  types dialog hashes under.

Why: identity should be a function of what a definition means, cheap
to compute, and independent of where its parts are stored. This is the
first step of moving every identity to merkle references in one
migration.

### Constants name their type by an entity

[notation](./notes/notation.md#constants)

A value in a binary encoding is written as `{"<type entity>": payload}`:
`{"text:": "foo:"}`, `{"entity:": "foo:"}`, `{"integer:": 5}`.

What this changes for you:

- Rule bodies and identities: text, an entity and a symbol of the same
  spelling, or a natural and a signed integer of the same number, are
  different constants. A committed rule matching text shaped like a URI
  derives again.
- Rule bodies are format 2. Formats 0 and 1 are still read, and a
  constant in a field with a declared type takes that type.
- `Changes` batches keep each value's type through dag-cbor. Integers
  used to come back as floats.
- JSON writes a value bare where the bare form reads back as the same
  value, and tagged otherwise. It reads both forms.
- `Value::untagged()` gives the bare form, for encodings that must keep
  an earlier spelling. `Value::conform(kind)` converts a value to a
  type that reads it.

Why: an encoding that leaves out a constant's type makes the meaning
depend on how its spelling looks, and lets two different rules share
one identity.

### Picks are named by entities

[glossary](./notes/glossary.md#pick)

A pick is named by an entity, as a type is: `last:`, `all:`, `top:`,
`max:`, `min:`.

What this changes for you:

- Descriptors write `"pick": "all:"`, and the 0.2.0 spelling `"all"`
  is still read. Batches serialize `Pick` the same way.
- Attribute identities hash the entity, so every attribute, concept
  and rule identity differs from 0.2.0's.
- `Pick::uri()` returns the entity. `name()` and `Display` are
  unchanged.
- The derive takes `#[pick(last|all|max|min)]`. `#[cardinality(..)]` no
  longer compiles, and the error names its replacement.

Why: a notation that resolves every bare word needs `pick: all` to name
something. Doing this before anything ships on 0.2.0 means identities
change only once.

### Compatibility and migration

- From pre-0.2: unchanged, `Branch::upgrade_rules()` lands on this
  release's identities.
- From 0.2.0 (nothing shipped on it): `upgrade_rules()` re-installs its
  rules but does not move its transient markers.
- 0.2.0 cannot read descriptors this release writes. Stored claims are
  not rewritten.
- Rule bodies from 0.2.0 or earlier wrote constants bare. On upgrade,
  each constant takes its field's declared type. A constant in an
  untyped field keeps the type its spelling suggests. Text shaped like
  a URL may come back with its URL normalization (`https://a` becomes
  `https://a/`).
- Every rule with a constant, and every attribute with an `as:` list,
  has a new identity. `upgrade_rules()` re-installs those rules.
- Every attribute and concept has a new identity, from 0.2.0 and from
  0.3.0-rc.1 alike. Nothing stored is rewritten: definitions recorded
  under their old entities (tonk's `db.attribute/*` and concept facts)
  need re-recording under the new ones, as tonk's `upgrade_definitions`
  does for pre-0.2 replicas.
- An attribute derived over a fixed-width Rust number (`u8` to `u128`,
  `i8` to `i128`, `f32`, `f64`) has a new identity, as do the concepts
  and rules that use it. Its stored claims are not rewritten, but they
  read differently: a stored number of another type now reads through
  the view (a float `9.75` under a `u8` field reads as `9`, where it was
  no row), and a value the Rust type cannot hold is left out, where it
  failed the read. 0.2.0 cannot read the sized types.
- Values in tonk's dag-cbor and IPLD conversions come out tagged. Use
  `Value::untagged()` where an identity minted from values has to stay
  as it was.
- Rule bodies are written in format 3, the canonical spelling. Bodies
  of formats 0 to 2 are still read, and `Branch::upgrade_rules()`
  rewrites them in the same pass that re-keys rules; until it runs, a
  body an earlier release wrote is read as before and a retraction of
  the rule it holds leaves it behind. 0.2.0 refuses format 3 bodies by
  name.
- A stored NaN from an earlier release is not rewritten. It reads as it
  did; nothing new can be written beside it.
- Every tree an earlier release wrote keys each number type in its own
  band. It is read as before, and `Branch::migrate` (or
  `migrate_storage`) rewrites it under the one band, facts and history
  alike, as a new revision; until then a commit on it fails with
  `NeedsMigration`. A pull that merges rewrites a legacy head in memory;
  one that fast-forwards adopts it as it is. 0.2.0 and earlier refuse a migrated
  tree: its manifest carries the critical field `value_encoding`
  (`0x05`), which they do not know. An embedder runs `Branch::migrate`
  once per branch when it opens it; it rewrites the whole tree, so the
  cost is one pass over the branch's entries.

## 0.2.0 (2026-10-09)

### Relations, types and picks have one name each

[#592](https://github.com/dialog-db/dialog-db/pull/592),
[glossary](./notes/glossary.md)

The words the code, the notation and the notes use are now the same. A
**relation** is the named set of claims `the` denotes. An **attribute**
is a relation qualified by a value type and a pick. A **pick** is which
of an entity's claims a read returns. A **type** is named by an entity,
`text:` rather than `Text`. Stored rule bodies name the format they are
written in.

What this changes for you:

- `dialog_artifacts::Attribute` is `dialog_artifacts::Relation`.
  `DialogArtifactsError::InvalidAttribute` and `ReservedAttribute` are
  `InvalidRelation` and `ReservedRelation`, and `ATTRIBUTE_LENGTH` is
  `RELATION_LENGTH`.
- In `dialog_query`, the validated relation name the `the!` macro
  builds is `Relation` (it was `The`), and what an attribute's `the`
  holds, one relation or a keyed collection, is the enum `The`
  (`The::Relation(..)`, `The::Collection { .. }`; it was `Relation`,
  with an `Attribute` variant). `dialog_query::ArtifactsAttribute` is
  `ArtifactsRelation`, and `Reach::Attribute` is `Reach::Relation`.
- One type, `dialog_artifacts::Pick` (`Last`, `All`, `Top(..)`, `Max`,
  `Min`), replaces both `dialog_artifacts::Policy` and
  `dialog_query::Select`. `AttributeDescriptor::select()` and
  `with_select(..)` are `pick()` and `with_pick(..)`;
  `without_select()` and `select_error()` are `without_pick()` and
  `pick_error()`. `TypeError::SelectPolicy` is `TypeError::UnfitPick`
  and `CommitError::Policy` is `CommitError::Succession`.
- A descriptor writes `pick:` where it wrote `select:`. `select:` is
  still read.
- A value type is written as its entity: `text:`, `integer:`,
  `natural:`, `float:`, `boolean:`, `bytes:`, `entity:`, `symbol:`,
  `record:`. `Text`, `SignedInteger`, `UnsignedInteger` and the other
  earlier names are still read and never written. `ValueDataType::uri`,
  `legacy_name` and `named` convert between them. Its `Display` is
  unchanged.
- A stored rule body is `{ format: 1, rule: <descriptor> }`.
  `dialog_query::rule::body::FORMAT` is the format this release writes.
  A bare body reads as format 0, and a body in a later format is
  refused with an error that names its format.

Why: three names (attribute, `The`, `Relation`) were used for the set
of claims `the` names, and "attribute" also meant the typed, picked
read of it, so the code could not say which one it meant. Types spelled
as strings were the one identifier in a descriptor that was not an
entity. A stored rule body had no way to say what shape it was in, so a
later release could only guess.

### Writes succeed the claim the pick elects

[#583](https://github.com/dialog-db/dialog-db/pull/583)

A write through an attribute read under `last`, `max`, `min` or `top`
no longer replaces its cell. It succeeds one claim: the one a read under
the same pick returns. The tree elects that claim among the cell's
stored claims in the same descent that writes the value, retracts it,
and records the new claim with the elected claim's versions as its
cause, as a replacement did. Every other claim in the cell stays. Where
a rule derives the relation, the transactor settles the write first
against the derived candidates the tree cannot see, through the
commit's own view. A write under `all` appends, as before.

The guarantee this gives is transactional. A transaction is a commit
that has not been flushed: querying it returns what querying the
committed result would, every write on top is squashed into that one
commit, and an assertion succeeds whatever a reader at that point
would have observed. `all` is the one pick that asserts without
succeeding anything.

What this changes for you:

- A cardinality-one write used to retract every prior claim of its
  cell (the former `Instruction::Replace`). It now retracts the one a `last` read
  would have returned. An older concurrent claim that lost the
  election stays live. No `last` read can tell the difference; an
  `all` read over the same relation can, and will now see it.
- If the read elects a candidate a rule derives, nothing is retracted:
  a derived candidate is not a claim. The write lands beside it and
  competes under the pick.
- Writing a value a read under the pick already returns writes
  nothing: the revision's tree does not move. Writing a value the cell
  holds as a claim the read does not return succeeds the claim it does
  return, and folds the commit's version into the held claim, so the
  written value stands past the claim it succeeded and a read returns
  it. Under `last` the write is the newest fact of the cell, as a
  last-writer-wins write is.
- Every write carries its pick. `Change::Assert(Value, Pick)` and
  `Instruction::Assert(Artifact, Pick)` are the one write form, beside
  `Retract`; `Change::Replace`, `Instruction::Replace`, `Change::Succeed`
  and `Instruction::Succeed` are gone. `dialog_artifacts::Pick` is the
  pick an attribute is read under (`Last`, `All`, `Top(..)`, `Max`,
  `Min`); `Update::associate` takes it as its fourth argument, and
  `associate_unique` and `succeed` are gone with the variants they
  wrote. A write under `All` appends; any other pick succeeds the
  claim a read under it returns. A `Changes` batch serializes each
  assertion with its pick, a shape older readers do not know. Batches
  are not stored or sent between replicas today, so this bites only
  code that encodes a batch itself.
- The reset `Replace` performed, retracting every claim of the cell
  whatever a reader observed, has no spelling any more. A caller that
  wants it retracts what it observed under `all` and asserts.
- `Branch::commit` and `Snapshot::commit`, which took a stream of
  instructions, are no longer public: they ran no induction and saw no
  derived candidate, so a write through them could land a value
  without succeeding what a reader observed. Every write goes through
  a transaction. A caller holding instructions collects them into a
  batch (`Changes` implements `FromIterator<Instruction>`) and
  integrates it: `branch.transaction().integrate(changes).commit()
  .publish()`; an empty commit is `branch.transaction().commit()
  .allow_empty().publish()`.
- What a write observes is the line and the writes before it in its
  own transaction, in order. A cell's writes are replayed in that order
  at commit: a write under `all` after a write under `last` stands
  beside it, and a write under `max` after an `all` write succeeds the
  staged claim it elects. An inductive rule's head observes the round
  view it fired on.
- A transaction reads what its commit will leave. Reads and the commit
  share one settlement: the log is settled in order against the line
  and the writes before each one, the first read settles what is
  logged so far, a later read or the commit settles only the writes
  logged since, and the result is kept while the lines read stay the
  same. So the claim a write succeeds is already gone from
  `transaction.query()`, and what a read showed is what lands.
  Two `last` writes of one cell in one transaction leave the later one
  alone, never both and never a hash-decided one, and a staged claim a
  later write retires or retracts leaves no tombstone: it never
  reached the line.
  A staged write stands at the edition the commit will mint, equal to
  every other write of the transaction, as the claims of one commit
  are; what order the transaction wrote them in is settlement's
  business alone, never a reader's.
- One departure remains: commit-time induction runs at commit, so what
  an inductive rule would derive from the transaction's writes is not
  in `transaction.query()` yet. That was true before this change.
- The session overlay is the newest facts. An overlay row stands
  past the edition the next commit mints, above every committed claim
  and every staged write, so a `last` read returns it over a committed
  claim of the same cell (it used to lose to any committed claim, and
  could only shadow one by retracting it first), and `max`, `min` and
  `top` rank it with the rest. A commit never takes an overlay row
  back: a succession the read resolves to an overlay row retires
  nothing and stands beside it, as beside a candidate a rule derives,
  and a transaction reads the overlay above its own writes. A
  transaction that retracts an overlay row takes it off the overlay
  when the commit publishes, so the row stays gone after the commit as
  it was in the transaction's reads. A cell the overlay holds is
  settled by the transactor, not the tree.
- Concurrent `last` writes settle by commit. Candidates of equal
  edition order by version hash before value, in the tree, the
  transactor and every read alike, so when two replicas' commits meet,
  one commit's writes win every cell they share, never some cells each.

Why: `Replace` encoded one pick, last-writer-wins, in the write path,
while reads had grown four. A write is a claim that succeeds what the
attribute currently stands for, and what it stands for is the pick's
to say. Making the write follow the read is what lets a relation be
read one way and written another: declare two attributes over it.

### A derived value stands by the fact that bound it

[#583](https://github.com/dialog-db/dialog-db/pull/583)

Under `last`, a value a rule derives competes with stored claims by
standing. Its standing is now that of the fact that bound its value,
carried across every concept boundary with the value. It used to be
the newest fact the rule's body touched, so an unrelated input landing
could make a derived value look fresher than the stored claim it was
competing with. A value a formula computes cites no fact of its own
and keeps the old behaviour.

A recursive relation's values stand the same way. Each row of the
fixpoint stands as the newest of its derivations, carried from row to
row as the fixpoint runs; a row derived again from a newer fact
re-enters the next round, so what it derives stands as new. A
subscription that maintains the fixpoint across polls keeps the
standings too: a deletion re-derives a suspect row at the newest
derivation that survives it.

### `unless`, optional premises and elections are stratified; a rule closing a cycle through one is quarantined

[#583](https://github.com/dialog-db/dialog-db/pull/583), detail in
[`notes/attribute-heads.md`](./notes/attribute-heads.md).

A deductive rule admits `unless` and optional premises. A read under a
ranked pick (`top`, a relation chain, `max`, `min`) is treated the
same way: it returns a candidate *and nothing better*, which negates
the better candidates, so a ranked fallback is an absence test however
it is spelled. A read under `last` is too outside a cycle; inside one
it reads every candidate and elects at the cycle's exit (see below). Outside a recursive component all of these are stratified:
the premise reads a relation derived in full before the rule runs, and
rules can derive, decide and derive again from the decision.

Inside a component, such a premise would read a relation the cycle is
still deriving, which has no stratified meaning. Merging rule sets that
are each fine can close such a cycle, so the program is never refused.
Instead the program analysis *quarantines* one rule of the cycle, and
evaluation leaves it out:

- It sets aside the rule of the cycle installed last. Every read was
  well defined before that rule arrived, so setting it aside keeps
  them as they were. A committed rule is ordered by the commit that
  indexed it (edition, then version hash, as a `last` election orders
  claims); a rule not yet committed (a session's, a transaction's, a
  query's own) is newer than every commit. A `RuleRegistry` orders
  rules by registration.
- Among rules installed together, it takes the cycle's plainest
  absence test (an `unless` or an optional read first, then a ranked
  election) and sets aside the rule inside the cycle that
  derives what that test reads; when the test's own rule derives what
  it tests, that rule.
- Then the greatest rule identity.

The order comes from the commits, which every replica sees the same,
so replicas holding the same history quarantine the same rule, however
the rules reached them. A quarantine lifts by itself once a rule of
the cycle is retracted. A plain recursion under `all`, such as an
ancestor closure, is positive and unaffected.

What this changes for you:

- A query reads the rules a branch sets aside as
  `dialog.rule/quarantined`: one row per rule, of the rule's identity,
  valued with the concept it concludes. It is never stored; the branch
  answers it from the rules its layers hold (committed, session,
  staged and the query's own), so it changes as rules are installed
  and retracted. Facts written under the attribute are not read.
- `ProgramAnalysis::quarantined()` lists the rules set aside, each with
  the concept it concludes and the cycle it closed.
  `ConceptRules::without` is how a bundle leaves them out, and
  `ConceptRules::install_at` installs a rule with when it was
  installed (`Installed`).
- A recursive rule may read its own relation under `last`: inside the
  cycle the read sees every candidate the fixpoint derives, and its
  readers elect the newest once the cycle is derived in full. A
  notebook positioning a run of inserted blocks from each block's
  successor relies on this. A read under a ranked pick (`top`, a
  relation chain, `max`, `min`) inside its own cycle is an absence
  test and is quarantined: a default chain is "this, else the
  default", which has no stratified meaning while "this" is still
  being derived. Inheritance down a hierarchy through a ranked choice
  (a node's own value, else its parent's) is the case this rules out;
  lattice-valued recursion could admit it later.
- `EvaluationError::NegationThroughRecursion` is gone, and the cycle
  pick that replaced it on this branch is gone too.
  `ProgramAnalysis::absences()` now lists only what a cycle with no
  rule to set aside still holds.
- `TypeError::NegationInOpenRule` is gone. It existed on this branch
  only.
- `reduce` in a deductive rule is still refused
  (`TypeError::ReduceInOpenRule`). A fold has no reading over a set
  that is still growing. Put the fold in a query, a subscription or an
  inductive rule.

Why: rules are installed as facts and merged from several replicas, so
whatever set arrives has to be evaluable and mean one thing everywhere.
Refusing negation outright threw away every stratified negation to
prevent the unstratified ones; reading a negation inside a cycle as
holding (the cycle policy) answered, but answered wrongly, ignoring
even stored facts the negation was written against. Quarantine keeps
every query answering and every answer a stratified one, and reports
what it set aside.

What is guaranteed: every program evaluates; evaluation is
deterministic, and replicas with the same history agree; what a query returns
is the stratified answer of the program minus the quarantined rules.
What is not: that a rule's derived set only grows as rules land. A
negation can lose derivations when a rule starts deriving what it
tests; that is what negation means.

### Picks choose members; aggregators are gone from `pick`

[#583](https://github.com/dialog-db/dialog-db/pull/583)

`pick` is one of `last`, `all`, `top`, `max`, `min`. Each returns
members of the candidate set, which is what lets a rule inside a cycle
read the set and a reader outside read the choice without disagreeing
about what the relation holds. `sum`, `count`, `count-distinct` and
`avg` are not picks and have been removed, with the carrier
distinction (`PolicyInOpenRule`) that existed to fence them. They were
introduced on this branch and never released. Use `reduce` in a query,
subscription or inductive rule for folds.

Other changes to picks on the same branch, for a reader who did not
follow it:

- An attribute is a relation (what `the` names) read under a type and
  a pick. Two picks over one relation are two attributes, with
  distinct identities. A field is a concept slot holding an attribute,
  and can be optional. These are the words the code and the notes use.
- Cardinality is derived from the pick: `all` is many, every other
  pick is one. `cardinality: one` and `cardinality: many` are read as
  the older spellings of `last` and `all`, and never written. Tonk's
  notation emits `pick: all` where it said `many` and nothing where
  it said `one`.
- A list is a ranked choice. `as: [a, b]` ranks values, `the: [x, y]`
  ranks relations, best first; either implies `top`. With the entity
  bound, a `top` over listed relations stops at the first relation
  that offers a candidate. Tonk's `among:` is gone; write the list
  under `as:`.
- A rule body naming a derived relation through a raw attribute
  premise (Rust API only; the notation always writes concept premises)
  now reads the derived candidates as well as the stored facts,
  negated and optional premises included.

### Rules derive attributes; concepts select them

[#580](https://github.com/dialog-db/dialog-db/pull/580), detail in
[`notes/attribute-heads.md`](./notes/attribute-heads.md).

A deductive rule's head is a set of attribute triples, and a concept
head is sugar for one triple per field. A rule contributes to the
relation each attribute names, and any concept reading that relation
sees the derived values beside the stored ones. Before this, a rule
deriving `Employee { name, role }` was visible only to a query for
`Employee`; a query for `Named { name }` did not see the name.

What this changes for you:

- A subset concept now sees derivations. If a concept's result set
  grew after upgrading, this is why.
- Installing a rule writes `dialog.rule/derives` facts, one per head
  attribute, beside `source`, `conclusion` and `reads`. Discovery
  reads `derives`; `conclusion` is kept for tooling and for rules
  installed before the index existed, which keep resolving.
- A rule's identity is the hash of its canonical spelling, not of the
  bytes you wrote: variables are renamed by structure and premises are
  ordered. The field names a premise binds a concept's fields under
  are renamed by structure too, so two premises reading the same
  attributes under different field names are one premise. Two authors
  writing one rule under different names install one rule. A body
  stored under any other entity, the older byte-hash identity
  included, is inert on every read and at commit until
  `Branch::upgrade_rules` re-installs it (see below).
- Every rule has an identity, including one a program registers with a
  `RuleRegistry`: a rule whose premises cannot be spelled canonically
  (a raw `AttributeQuery` scan) is refused
  (`EvaluationError::RuleWithoutIdentity`). Write the premise as a
  concept or attribute read, as an installed rule would.
- The dependency graph, the fixpoint and the registry key an attribute
  concept by its relation, so every read of a relation, under any type
  or pick, meets the rules deriving it.

Why: whatever is true of an asserted fact must be true of a derived
one. Asserting `Employee` writes two attribute facts and nothing else;
deriving it must land in the same place.

### Compatibility and migration

What a replica holds from the release before this branch, and what
happens to it.

- Types are hashed as entities, so the attribute, concept and rule
  identities this release computes differ from the earlier release's
  for every typed attribute, beyond the change picks made.
  `Branch::upgrade_rules()` re-installs every rule under its identity
  and moves the transient markers, as below; `dialog_query::migration`
  still computes the earlier identities from the earlier type names.
  Nothing is rewritten until the upgrade runs.
- The earlier release cannot read a descriptor this one writes: it
  knows `Text`, not `text:`, and it does not know the rule body
  envelope. A body stored by the earlier release, bare and with the
  earlier type names, is read by this one.
- Stored claims are unchanged: a claim is its attribute, entity,
  value and cause, with the versions that carry it. A cell the older
  release wrote through a cardinality-one replacement holds one claim,
  and a `last` read returns it, as before. Nothing is rewritten.
- Stored rules decode. A rule is stored as the dag-cbor of its
  descriptor, and the older descriptor spells `cardinality: one` or
  `many` and a single relation under `the`; both read as `last` and
  `all` over that relation. Re-encoded, a rule never writes the older
  spelling back.
- Rules installed by the older release are inert until upgraded. Such
  a rule sits under the hash of its stored bytes, with its conclusion
  and its body but no `derives` fact. No read looks for it there, so
  no read pays for rules that may not exist. `Branch::upgrade_rules()
  .perform(env)` reads the branch's rule bodies, retracts the facts of
  every rule stored under an entity that is not its identity, and
  installs it again under its identity, in one commit. It is
  idempotent, and two replicas upgrading concurrently converge, since
  a rule's identity is a function of the rule. It needs the branch's
  `dialog.rule/` range, not a full replica. A replica still on the
  older release can write such rules again and sync brings them in;
  they stay inert until the upgrade runs again. When to run it, once
  or after every pull, is the embedder's decision.
- Every attribute and concept identity changes: an attribute's
  identity hashes the relations it ranks, its pick and its ranked
  values beside its domain, name and type, so two reads of one
  relation under different picks are two attributes. The upgrade
  moves the `dialog.concept/transient` markers keyed by the older
  identities, for the concepts the branch's rules conclude.
  `dialog_query::migration` reproduces the older identities, for an
  application that keyed facts of its own by them.
- Library YAML in the older spelling parses: `cardinality:` is read as
  `pick: all` for `many` and the default `last` for `one`, and a
  `pick:` beside it wins. tonk's standard libraries carry a seed
  version that is the hash of their text, so a worker on this release
  sees the mismatch on first start, retracts the installed library's
  facts by the provenance it recorded, and installs the new one.
- The older release cannot read what this one writes. A rule written
  here may list relations or values under `the` or `as`, which the
  older descriptor refuses, and carries `pick`, which the older
  descriptor ignores, reading a `max` as `last`. A batch of changes
  serializes each assertion with its pick. Upgrade every replica of
  a repository together, or upgrade readers before writers: a replica
  on the older release pulls such a branch, but a read that meets a
  rule it cannot decode fails with the decoding error.
- A descriptor's JSON no longer spells `cardinality`. It writes
  `pick` where the pick says more than the lists imply (a ranked
  `the` or `as` list reads as `top`, a plain attribute as `last`), so
  a plain `last` field carries neither key. A program that told a
  one-valued field apart by `cardinality: one` reads `pick` instead:
  a field is one-valued unless it says `pick: all`. tonk's template
  planner did this; a reader rebuilding a descriptor from stored facts
  has to keep the pick and the ranked list beside the cardinality,
  or a `top` field comes back as `last`.
- Programs the older release refused run. `unless` and optional
  premises inside a recursive component were an error
  (`NegationThroughRecursion`); the analysis now quarantines a rule of
  such a cycle and every query answers, without the rule.
