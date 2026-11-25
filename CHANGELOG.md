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

### A rule can be laid over one query of a transaction

`TransactionQuery::with_rule` lays a deductive rule over a single query
of a transaction's view: the rule reads the claims the branch holds and
the ones the transaction stages, a staged cardinality-one replace as the
commit will leave the cell, and nothing of it is staged or written. A
transaction's view stays closed to more fact sources; a rule adds no
fact, only a way of reading the facts there are.

What this changes for you:

- New: `TransactionQuery::with_rule(DeductiveRule)`, chainable before
  `select`. Each `Transaction::query()` starts with no rule laid over
  it, so one query's rules are never another's.
- Nothing else: a transaction query with no rule laid over it reads as
  before.

Why: a write computed from a read in the middle of a transaction, such
as a notation document's `when`, may read through a rule of its own, a
query of several premises. It had to read the branch without the
transaction's own writes, or stage the rule and take it back before the
commit.

### A typed read bound to a constant its type does not read answers nothing

A field that declares a type reads only the values of that type, so a
value of another type is no fact of the field. A cardinality-one read
whose entity was unbound and whose value was bound to a constant found
the facts holding that constant by the constant alone, and answered one
whatever its type: a field declared `text:` bound to the integer `-3`
answered the stored `-3`, and a field that picks `max` over `natural:`
bound to `"hello"` answered the stored `"hello"`. Each fact such a read
finds is now read through the field's type, as the read with its value
unbound reads it.

What this changes for you:

- A read through a typed field whose value is bound to a constant the
  type does not read answers nothing, under every cardinality and
  through a pick, with the entity bound or unbound. Under cardinality
  one with the entity unbound it answered the facts holding the
  constant.
- A constant of another number type than the stored number still finds
  it: a `float:` field bound to the natural `3` finds a stored `3`.
- A read whose value is unbound, and a read through a field that
  declares no type, answer as before.

Why: a field's type is part of which attribute it reads, so binding the
value must not widen what the read answers.

### A typed read of a relation a rule deduces reads it as it reads a stored one

[#866](https://github.com/dialog-db/dialog-db/pull/866)

A field that declares a type reads a stored attribute through that
type: a value of another type is no fact of the field, and a number is
read as the field's number type. A rule concludes its relation whatever
type its head declares, and the values it concluded reached a typed read
unread: a field declared `integer:` over a relation a rule fills with
text answered the text. Every value a rule concludes is now read through
the field's declared type, as the stored scan reads a stored one, when
one rule answers the read, when stored and deduced values meet, and
through the fixpoint of a recursive relation. The type is applied before
the cardinality-one election on deduced reads, as the caller-variable
lens does; the stored scan elects first.

What this changes for you:

- A read of a deduced relation through a field of a type answers
  nothing for a deduced value of another type. It answered the value.
- A deduced number read through a field of another number type comes
  back as that type, as a stored one does: a natural read as `float:` is
  a float, a fraction read as `natural:` is floored, and a negative is
  not a natural. It came back as the rule concluded it.
- A read of a deduced relation whose value is bound to a number finds a
  value of another number type that is the same number, as a stored read
  does: a `float:` field bound to the natural `3` finds a deduced `3`,
  and a `natural:` field bound to `3.0` finds it too. It missed it. A
  number constant still finds no value that is not a number.
- A cardinality-one read of a deduced relation whose newest value is of
  a type the field does not read answers the newest value it does read.
  The same read of a stored relation answers nothing, as before.
- A read through a field that declares no type, and a read of a concept
  of several fields, answer as before.

Why: a field's type is part of which attribute it reads, so a read must
not answer differently because a rule rather than a write put the value
there.

### A replica reads the revision records it wrote from the buffers above the leaves

[#864](https://github.com/dialog-db/dialog-db/pull/864)

A commit reads its parent's revision record to carry the skip links
along, and a stack reads a durable layer's revision from its record. A
commit writes its record into a buffer of the tree, in an index node,
and only a flush moves it into the leaf it belongs in, so a record a
replica wrote a moment ago sits above a leaf the replica may never have
read. A branch remembers the records it mints in memory, which hid this
until the process restarted. A durable layer's line remembers nothing
from one write to the next, so the next write into the layer read its
own record through that leaf. The readers of a revision's record now
look in the buffers on the route to it first, read from what the replica
holds, and take a record there that verifies. Two valid records cannot
share a version, so the leaf has nothing better to offer.

What this changes for you:

- After a restart, a replica's first commit, and its stack's open and
  first write into a durable layer, read the record of the replica's own
  last revision without a fetch. Before, they fetched the leaf beneath
  it when the replica had never held it.
- A second write into a durable layer reads the first's record without
  a fetch.
- `dialog_search_tree::PersistentTree::first_buffered` is new. It offers
  a callback the newest buffered op of each key in a range, along the
  route to the range's start, reading only nodes the replica holds and
  no leaf, and returns the first answer the callback gives.
- A buffered record that does not verify, one whose value spilled out of
  its key, or one a newer op retracted is never taken from a buffer: the
  read falls back to the walk to the leaf, as before.

Why: an editor that answered a keystroke at once must answer the next at
once, after a restart too, and the record of its own last revision is
the one thing every commit on that revision reads.

### A claim one round of induction both retracts and asserts holds afterwards

[#863](https://github.com/dialog-db/dialog-db/pull/863)

Induction runs in rounds, and every rule a round triggers reads the
state before the round. A rule that retracts the old value of a field
beside a rule that asserts the new one is how a rename or a record
replacement is written. When the old and the new value coincided in a
round, the claim was lost: the retraction of a held claim was news and
was written, while the assertion of it was no news and was dropped. A
round now settles its writes once every rule has fired: a retraction a
`retract!` rule wrote of a claim another rule of the same round asserts
again is withdrawn, so the claim keeps the row it had and nothing is
written for it. The outcome does not depend on the order the rules
fire in.

What this changes for you:

- A rule retracting `title = ?old` beside a rule asserting
  `title = ?new` for one entity, with `?old` and `?new` bound to the
  same value, leaves the title. Before, the entity lost it.
- A rule retracting a record, every field named, beside a rule
  asserting it with one field changed, leaves the record with that
  field changed. Before, only the changed field was left.
- The same holds for a cardinality-many attribute and for a head
  landing in a layer (`in:`).
- A commit that used to fail now succeeds: an `assert!` rule and a
  `retract!` rule of one claim, both watching its attribute, that meet
  in a round with the claim held. Before, the claim flipped round after
  round and the commit failed with `CommitError::InductionDivergence`.
  Now the round writes nothing for the claim, and the commit settles
  with it held.
- Unchanged: a retraction with no assertion beside it removes the
  claim, an assertion with none beside it adds it, and a write under
  cardinality one still succeeds the claim it replaces, so a round
  asserting `A`, asserting `B` and retracting `A` of a cardinality-one
  cell leaves `B`, as before.
- Unchanged: a transaction's own writes are a sequence and are applied
  in the order written, so retracting a claim and then asserting it in
  one transaction leaves it, and asserting it and then retracting it
  removes it.

Why: retracting the old value and asserting the new one is how a rule
states a change, and it must not lose data when the two coincide.

### The first commit after a `QueriedRanges` pull reads from the replica's own blocks

[#861](https://github.com/dialog-db/dialog-db/pull/861)

A commit reads its parent's revision record to carry the skip links
along. Before a pull, that parent is the replica's own last revision,
whose record it wrote. After a pull that adopted another device's head,
it is that head, whose record the replica never read. A record is kept
at a key hashed from its version, so whether it fell in a leaf the
replica held was chance, and the first commit after a pull usually
waited on the remote. In a long history that was nearly every time. A
pull asked for `Download::QueriedRanges` now also brings the record of
the head it adopts. Separately, a range scan whose range ended at the
last key of a leaf read the leaf after it as well, and now stops at a
separator past its end.

What this changes for you:

- A pull with `Download::QueriedRanges` that adopts a head fetches the
  leaf holding that head's revision record, when the replica does not
  hold it already. At most one leaf, and its path, per pull.
- The first commit after such a pull answers from the replica's own
  blocks. Before, it fetched that leaf.
- Such a pull can now fail where it used to land: with
  `DownloadError::Snapshot` when the remote cannot serve that one leaf,
  or when every record kept at the head's slot fails verification.
  Before, the pull landed and the next commit failed on the same read.
  A head with no record at all, from history written before records
  existed, is adopted as before.
- A range scan (`PersistentTree::stream_range` and everything built on
  it) no longer reads the subtree after a range that ends at the last
  key of a subtree.

Why: `QueriedRanges` exists so that an editor that answered one
keystroke at once does not take a round trip for the next, and the
record of a head is the one thing every commit on that head reads.

### A rule whose head has a field named `this` is refused, not a panic

[#860](https://github.com/dialog-db/dialog-db/pull/860)

`this` is what every query and rule calls the entity, and a rule's
identity names every head field but the entity. A head field named
`this` made compiling the rule panic looking up its name.

What this changes for you:

- `DeductiveRuleDescriptor::compile` returns
  `TypeError::ReservedField { field }` for a concluded field named
  `this`. `TypeError` has the new variant, so a `match` over
  `TypeError` with no wildcard arm needs one more arm.

Why: an embedder can build such a descriptor without the notation, and
a descriptor a caller can build must not take the process down.

### Every identity generation a release computed is named, and the current one is a constant

[#859](https://github.com/dialog-db/dialog-db/pull/859).

`dialog_query::migration` now says, for attributes, concepts and rules,
which identity each release computed and what it hashed, and exports a
function for each generation an embedder can reproduce, named by its
generation, beside `IDENTITY_GENERATION`, the generation this release
computes. Generation 0 is 0.1.0's (`attribute_uri_v0`,
`concept_identity_v0`), generation 1 is 0.3.0 to 0.5.0's
(`attribute_uri_v1`, `concept_identity_v1`), and generation 2 is this
release's (`attribute_uri_v2`, `concept_identity_v2`,
`rule_identity_v2`). 0.2.0 computed identities of its own, which nothing
reproduces; no embedder is known to hold them. Rule identities of
generations 0 and 1 are not exported: no fact holds a rule's identity as
a value, and `Branch::upgrade_rules` finds an earlier rule by the entity
it is stored under.

What this changes for you:

- New: `migration::IDENTITY_GENERATION` (2), `attribute_uri_v2`,
  `concept_identity_v2`, `rule_identity_v2` (taking a `DeductiveRule` or
  an `InductiveRule` through `migration::AnyRule`, and returning `None`
  for a rule with no stored form). They return what `to_uri`, `this` and
  `try_this` return today, and keep returning it when a later release
  moves identities.
- New: `migration::picked_attribute_uri_v1` and
  `picked_attribute_reference_v1`, the identity 0.3.0 to 0.5.0 gave an
  attribute read under `max`, `min` or `top` (a listed `as` with no pick
  read as `top`) on its own, not through a concept. It was computed
  privately for `concept_reference_v1` and is now public.
- An embedder that writes dialog identities into its own facts records
  `IDENTITY_GENERATION` beside them, and when a release reports another,
  rewrites each value from the earlier generation's identity to the
  current one. The module documentation has the table of releases and
  generations, and an example.
- The documentation of `attribute_uri_v0` and `concept_identity_v0` said
  they reproduce 0.2's identities. They reproduce 0.1.0's; the functions
  compute what they always did.

Why: an embedder rekeys its own facts when dialog's identities move, and
had no way to tell that they had moved, nor a name for each generation
it had to compute the old identity in. A test now pins this
generation's identities beside the constant, so moving one means bumping
it.

### A rule that holds a constant map or list reads back from its stored body

[#855](https://github.com/dialog-db/dialog-db/pull/855)

A stored rule body writes a constant map or list as its canonical
bytes, and the reader of a rule body buffered each term through a
form that holds no bytes. So a rule holding a composite constant, as a
term or as a pattern with no variable in it (which is that constant),
was written in format 4 and then refused on every read: `decode`
failed with "invalid type: byte array", a branch listed the rule in
`dialog.rule/invalid` as undecodable, and it never fired. The reader
now reads such bytes as the composite they are. Only a composite is
written as a byte string: a bytes constant is written as an array of
integers under `bytes:`, so it reads back as bytes, and a bytes constant
and a composite of the same bytes are two rules, of two bodies. That was
also why format 5 refused a rule holding a constant map or list inside a
pattern; it is stored now, every rule that compiles has a body, and the
refusal is gone.

What this changes for you:

- `DeductiveRule::decode` and `InductiveRule::decode` read a body that
  holds a composite constant, at any depth, as the rule that wrote it.
  A branch fires such a rule instead of listing it in
  `dialog.rule/invalid`.
- A rule holding a constant map or list inside a pattern is stored in
  format 5: `try_encode` returns its body, and asserting it into a
  transaction, a batch, the session overlay or a query layer stages that
  body.
- Gone, as nothing produces them (none was released):
  `DeductiveRule::storable`, `InductiveRule::storable`,
  `TypeError::UnstorablePattern`, `Update::refuse`, `Update::hold`,
  `Refusal`, `Changes::refused`, `CommitError::Refused` and
  `DialogCorpusError::Refused`. An `Update` implementor implements the
  methods it did before #778. A query layer no longer keeps rules in
  memory beside its changes: it reads every rule from its stored body.
- Bytes in a rule body that are no map or list are refused when the body
  is decoded, with an error, as any malformed body is.

Why: composite values (#843) made a map or a list a constant a rule can
hold, and a rule that can be built and run should run once it is stored.

### `max` and `min` over a listed type rank by the listing, least first

[#858](https://github.com/dialog-db/dialog-db/pull/858).

A listed `as` is a scale written least first, `[low, medium, high]`. A
concept field picking `max` over an attribute that lists its values now
reads the claim holding the last listed value present, and `min` the
first, whatever the values' own order; before, both compared the values,
so listed symbols ranked by their bytes. A value the listing does not
hold loses to every listed one, claims holding one value fall to the
newest, and a cell holding no listed value reads its newest claim, as
under `top`. Over an attribute that lists nothing, `max` and `min` are
unchanged. `top` reads as it did: the first listed value present, which
is now also what `min` reads.

What this changes for you:

- A field with `pick: max` or `pick: min` over a listed attribute reads
  a different claim wherever the listing's order and the values' order
  disagree: over `[low, medium, high]`, `max` reads `high` where it read
  `medium`, and `min` reads `low` where it read `high`. A subscription or
  a rule reading such a field follows.
- `max` and `min` over a listing are accepted whatever the listed values'
  type: `TypeError::SelectiveAggregate`'s "type has no order" refusal
  applies only to an attribute that lists nothing.
- `Selective::compares` takes the listing (`compares(listed)`) and is
  false for `max` and `min` over one.
- Deprecated: `pick: top` (`Selective::Top`). It reads the first listed
  value present, which is `min` over the same list; a list written least
  first is read with `max`. It is still read and keeps its identity, is
  marked deprecated in `notes/notation/schema.json`, and goes in the next
  version.
- Removed: `Selective::bounds`, which nothing called.
- `ReduceEntry::try_listed` builds an entry ranking by a listing, and
  `ReduceEntry::orders_by_standing` says whether a fold over it is
  evaluated afresh: true for a `max` or `min` over a listing, as for
  `top`.

Why: a list written top to bottom read as "best first" only for `top`,
and `max` over the same list, the reading a scale invites, ranked by
bytes nobody chose. A listing written least first gives `max` and `min`
the order its author wrote.

### A tree names the subject whose nodes it reads, and holds no reader

[#785](https://github.com/dialog-db/dialog-db/pull/785),
[design/tree-node-reads.md](./design/tree-node-reads.md), a follow-up to
[#755](https://github.com/dialog-db/dialog-db/pull/755); the
[journal](./design/tree-node-reads/journal.md) records the two decisions
and the inventory behind them.

A tree is a root and a `Reading`: the subject whose nodes it reads, the
priority its hydrations rank at, and whether it reads only this replica.
Every operation is handed the environment it reads through and performs
`nodes::Read` and `nodes::Preload` on it; nothing in the tree crate keeps
an environment. The node requests name a subject instead of a catalog,
and which block catalog a node is kept in, when it is kept as a block, is
the translation layer's business.

What this changes for you:

- `Accessor` is gone. `Reading` carries the subject (`Reading { subject,
  priority, local }`, `Reading::new(subject)`, `.ranked(priority)`,
  `.locally()`), builds the requests (`read(&hash)`, `preload(&hash)`), and
  reads a node a tree links to with `node(env, &hash)`, where absence is
  an error. `Accessor::new(env, &catalog).get_node(&hash)` becomes
  `tree.reading().node(env, &hash)`, or `Reading::new(subject).node(env,
  &hash)`; a forced local read becomes `nodes::Read::new(subject,
  hash).locally().perform(env)`.
- The free `dialog_search_tree::load_node(env, &catalog, &hash)` is gone:
  `nodes::Read::new(subject, hash).perform(env)` is the same read, `None`
  for a node nobody can reach.
- `nodes::Read`, `nodes::Preload` and `nodes::Write` carry `subject:
  Subject` where they carried `catalog: CatalogScope`, and their `new`
  takes the subject. `Read::load()` still hands the translation layer the
  block load, in the subject's index catalog, and
  `dialog_search_tree::nodes::blocks_of(&subject)` names that catalog for
  a store that answers the node requests over blocks of its own, as the
  single-file archive (`dialog-chronicle`) does.
- The trees take a `Subject` where they took a `CatalogScope`:
  `PersistentTree::empty_in`, `empty_with_manifest` and `at`,
  `TransientTree::new`, `empty_with_manifest`, `with_manifest` and
  `stitch`, `HitchhikerTree::empty`, and above them
  `TreeHistory::from_root` and `empty` and
  `dialog_corpus::tree::distribution::capture`. `catalog()` is
  `subject()` on each tree and on `CorpusWriter`;
  `PersistentTree::reading()` returns `&Reading`, and `TransientTree`
  has one too.
- An edit reads as the tree it was opened from, and the tree it persists
  reads that way too. `tree.locally().edit()` reads only this replica and
  fails with `DialogSearchTreeError::Node` on a node it lacks instead of
  hydrating it; `tree.ranked(priority).edit()` ranks its hydrations at
  that priority; `HitchhikerTree::open` inherits the same way. Before,
  every edit read on demand and could hydrate, and the tree it persisted
  read on demand whatever it was opened from.
- For tests, `dialog_search_tree::helpers::test_subject()` names the test
  trees' subject and `Blocks::subject()` a block store's;
  `test_catalog()` is the block catalog beneath them.

Nothing a replica stores or syncs changes: a node is still kept as a block
in its subject's `index` catalog, and no data is rewritten.

Why: after #755 the `Accessor` no longer owned the cache, and what was
left bundled an environment with a catalog and a reading policy, which
put a storage handle back into the tree's vocabulary and gave callers
outside the crate a second way to read a node. The catalog name was
always `index`, so the part of it that mattered was the subject; an
environment serving several subjects (a peer, a query over lines of
several repositories, a push reading one tree locally and forwarding
another's blocks) routes, caches and hydrates by it. The edit's reading
policy was dropped by accident, so a local tree could hydrate the nodes
its edit rewrote.

### A standing fold steps a group's value instead of folding the group again

[#856](https://github.com/dialog-db/dialog-db/pull/856). The design it follows is
`design/fold-maintenance/design-fold-state.md` (#837), approach 2.

`Branch::subscribe_fold` folded a group again over all its kept input rows
whenever one of them moved: one to-do changed among 10,000 in one list cost
the 10,000. Where every entry of the fold is `count`, `count-distinct`,
`sum`, `min` or `max`, a group that has moved once now keeps a running value
(`dialog_query::reduce::Running`): a count, an integer sum, the chosen
extreme, or for `count-distinct` each distinct value with the number of rows
holding it. A poll takes the rows it added and took away into it. A group is
still folded again where a running value cannot be exact: a float sum, the
last row holding a `min` or `max` taken away, a value that does not order, a
moved group row, a group of no input rows or of two group rows, and `avg`.
`last` and `top` are evaluated afresh as before.

What this changes for you:

- Nothing in the answers: every row and every delta is the one folding the
  group again gives. This changes what a poll costs, not what it reports.
- `FoldSubscription::refolds` counts fewer groups; `FoldSubscription::steps`
  is new and counts the groups given by their running value.
- New: `Reduce::runs` and `Running` in `dialog_query::reduce`.

Why: a view keeps a count on a fold, and a change to one row has to cost
that row, not the group it is in.

### A rule that holds a pattern is stored, in rule body format 5

Stacked on [#778](https://github.com/dialog-db/dialog-db/pull/778); the
design is in [design/patterns/design-rule-identity.md](design/patterns/design-rule-identity.md).

A rule that holds a map or a list of terms needs a stored body that can
write a pattern, which format 4 has no way to do. Rule body format 5
writes one. Where a parameter holds a map or a list, the
body writes it under the one key `match` as a composite value is written
for a human reader: a map as its entries, each a pair of a key and a
pattern, and a list as its items, with what takes the rest beside them
under `...`. Inside a pattern a map or a list is `{"map": ..}` or
`{"list": ..}` again, a key is any value a map holds as a key (text, a
symbol, a number), and a constant is written under the name of its type.
Every pattern has one encoding, its entries in the order the map holds
them:

```json
{ "match": { "map": [[{ "text:": "title" }, { "?": { "name": "heading" } }]], "...": { "?": {} } } }
{ "match": { "list": [{ "?": { "name": "first" } }], "...": { "?": { "name": "more" } } } }
```

A body is written in the lowest format that can say it: format 5 for a
rule that holds a pattern, and format 4, byte for byte as before, for
every other rule. This departs from the stamping note of the earlier
draft (#799), which wrote every body in the newest format: a rule of
terms means what it did, so it keeps its bytes, an embedder's stored
rules are not migrated a second time for patterns, and a rule already
stored is still found and retracted by encoding it. A pattern rule is now installed like any rule: it
commits, is read back from the branch, fires, replicates, and is held in
the session overlay, so a subscription or a fold over the concept it
concludes stays current. Rule identity does not move: a rule of terms
keeps its entity and its body, and a pattern rule keeps the name it had.

What this changes for you:

- Asserting a rule that holds a pattern into a transaction, a batch, the
  session overlay or a query layer stages its `source` body with its
  index facts, and committing it or applying it to an ephemeral line
  lands it. A map pattern keyed by any value is stored, and so, since the
  entry above, is a pattern that holds a constant map or list.
- `Parameters` and `Pattern` serialize a map or a list of terms under
  `match`, and read it back, in JSON and dag-cbor. A `ConceptQuery` writes a name that holds a pattern as that
  pattern in its `where`, where it wrote the variable standing for the
  whole value, and reads it back: a negated premise keeps its pattern in
  the concept application, so a rule negating one was written without it.
- `dialog_query::rule::body::FORMAT` is 5, the latest format this release
  reads and the one a pattern rule is written in.
  `rule::body::TERMS_FORMAT` is 4, the format of a rule that holds no
  pattern, and `rule::body::format_for` picks between them.
- A body stamped with a format below 5 that holds a pattern is refused when
  it is decoded: no release wrote one.
- A constant inside a pattern is held to what a stored body carries, as a
  constant elsewhere in a rule is: an integer past 64 bits or a float that
  is not finite fails the rule with `TypeError::UnencodableConstant`.

Why: the notation compiles a query of more than one column into a rule
whose conclusion builds the row as one record with a map pattern. Such a
rule could not be committed, held in the session overlay or subscribed to,
so the notation's cards, tables and measurements had nothing to stand on.

### A parameter can hold a pattern: a map or a list of terms, taken apart or built

[#778](https://github.com/dialog-db/dialog-db/pull/778), on the composite
value of [#843](https://github.com/dialog-db/dialog-db/pull/843). The design
is in [design/patterns.md](design/patterns.md).

A premise's parameter held one variable or one constant. It can now hold a
`Pattern`: a term, or a map or a list of patterns, with an optional rest (the
entries a map pattern does not name, bound as a map) or run (the items after
a list pattern's, bound as a list). Where the value is bound, the pattern
takes it apart and binds the variables inside it; where every variable
inside it is bound, the pattern builds the value. A map pattern with no rest
matches a map of exactly its entries, and a list pattern with no run a list
of exactly its items. The planner counts a pattern bound when every variable
inside it is, and inference types a slot that holds one as a composite and
each leaf as its term is typed. A rule that holds a pattern has an identity,
and rule body format 5 (the entries above) stores it as any rule.

What this changes for you:

- `Equality.this` and `Equality.is` are a `Pattern` where they were a
  `Term<Any>`. `Equality::new` takes terms as before, and a `Pattern`
  compares with a `Term<Any>` and answers `name` and `is_constant`.
- New: `Pattern`, `MapPattern` (`MapPattern::new` refuses a key given
  twice, as a map holds it, with `MapPatternError::DuplicateKey`),
  `ListPattern`, and on `Parameters` `set` (a pattern under a name),
  `shape`, `pattern`, `shapes`, `has_shapes`, `leaves`, `lowered`, `plain`
  and `map_terms`. `get` and `iter` still answer terms: under a name that
  holds a pattern, a variable that stands for the whole value.

Why: a rule could not say a value by its shape, so a command could not be
given a record, an entity could not be derived from one, and a syntax tree
could not be taken apart in a rule. The pattern is the notation's struct
with variables in it, one thing both ways.

### A value can be a composite: a map, a record or a list, held as one value and read in place

[#843](https://github.com/dialog-db/dialog-db/pull/843).

From the record values research ([#748](https://github.com/dialog-db/dialog-db/pull/748)),
replacing the draft [#812](https://github.com/dialog-db/dialog-db/pull/812);
the layout and its measurements are in
[design/composite-value.md](design/composite-value.md).

A value that is compound but atomic (a location's latitude and longitude, a
table a view resolved, a syntax tree) had nowhere to go but an opaque
`record:`, each writer inventing its own encoding and its own tag. A
composite is a value type of its own, written and replaced whole like any
value. It is a map, whose keys are any values but composites (text the
usual one) and whose key may hold several values, or a list, whose items
keep their order and repeats. A record is the map whose every key is an
attribute: the same kind, told by its keys, as a natural is a number. Two
composites are equal exactly when they hold the same content, whatever
order it was given in, and the natural `3` and the float `3.0` are one value
in a composite as anywhere.

A composite has one canonical encoding: a table of its atoms, every
scalar it and the composites nested in it hold, keys and values alike,
once, sorted by their bytes; then its body, in which every scalar is a
reference to its place in the table (a slot that starts with type byte
26, reserved for that and for nothing else) and a nested composite is its
own body, which says its size. A sequence of more than `SCAN_LIMIT` items
carries a table of offsets. Bytes that are not that encoding are refused,
so equal composites have equal bytes and a composite is keyed in the
tree by them. A `Composite` is a view, its buffer shared and the place of
its body, so a nested composite, a field or an item is read where it
sits: the length, a field by key, an item by index and iteration decode
only what they return. Its identity is its merkle reference: for a map, a
map under `composite:` from each key's reference to its value's (or to
the `set:` of its values'); for a list, the `list:` of its items'
references. No part of the layout enters it.

An atom is a scalar in the bytes a tree key holds it in, so the
single-file archive (`dialog-chronicle`) keeps a composite in a key as
the token ids of its atoms and its body, and each atom as the token a
key holding that scalar alone names: a text a thousand composites hold
is stored once. A composite whose key holds only a prefix of it (one
that spilled) is kept whole, and a composite is kept as its atoms only
when that rebuilds the very key it was given. Blocks, sync and identity
are unchanged: the atoms are shared inside the file only.

What this changes for you:

- `Value` has a new variant, `Composite`, keyed with type byte 22.
  `ValueDataType` (the query layer's `Type`) has `Composite`
  (`composite:`, any composite, the band every composite is keyed under),
  and in its band `Map` (`composite:map`), `CompositeRecord`
  (`composite:record`) and `List` (`composite:list`), which type
  slots and never keys, as the sized number types do. A composite value's
  `data_type()` is `Map` or `List`. A `match` over either
  enum needs arms. `Value::Record` and `record:` are unchanged, identities
  included.
- Build with `Composite::map`, `Composite::record` (`Composite::new` is the
  same) and `Composite::list`; read with `len`, `kind`, `is_record`, `get`
  and `first` by key, `field` by attribute, `item` by index, `entries`,
  `items` and `fields`, and `first_view` and `item_view` for a value whose
  text borrows from the buffer. `Composite::from_bytes` refuses any bytes
  but the canonical ones with a `CompositeError` saying why;
  `Composite::to_bytes` gives them; `Composite::reference` gives the
  identity. Refused on construction: a composite as a key
  (`CompositeError::CompositeKey`), NaN (`NotANumber`), nesting past
  `MAX_DEPTH`, 128 (`TooDeep`), and bytes past 4 GiB (`TooLarge`).
- `Composite::split_atoms` splits a composite's bytes into its atoms and
  its body, and `Composite::join_atoms` writes them back, for a store
  that keeps scalars once. `ValueDataType::ATOM_REFERENCE` (26) is the
  reserved reference byte; no type takes it. `key_value_is_spilled` says
  whether a key holds its value only as a prefix.
- A composite says its layout in the high four bits of its first byte;
  this release writes layout 0, so no byte of its encoding moves, and
  `Composite::from_bytes` refuses any other layout by name,
  `CompositeError::UnsupportedLayout`.
- `ValueView` has a `Composite` variant: a scanned row holding a composite
  binds in place, as every other value does.
- An attribute declared `as: composite:`, `composite:map`,
  `composite:record` or `composite:list` holds what its type says; a write
  of a list where a record is declared is refused. The query layer's
  `Primitive` gives maps and lists a bit each (a record is on the map's),
  `Primitive::ALL` includes both, and the wire names them `map` and `list`.
  A query cannot yet read a field out of a composite it bound; that comes
  with destructuring.
- In JSON a composite is `{"composite:map": {"map": [[key, value], ...]}}`
  or `{"composite:list": {"list": [...]}}`, its entries in canonical order;
  in a binary format, its canonical bytes; in a CSV export, `composite` with
  its bytes in base58.
- `ValueDataType::max()`, the largest type a key carries, is now
  `Composite`, so a scan bound from it covers composites.

Why: a value that has to change as a whole was either split into claims,
which concurrent writers can leave half updated, or packed into opaque
bytes no query could read and no two writers spelled alike. The notation
keeps its syntax trees as maps and lists, and a composite that is read
without decoding all of it is what lets a rule reach one node of one.

### A block `append` acknowledged stays readable while threads of one process append to one file

[#851](https://github.com/dialog-db/dialog-db/pull/851)

Since 0.5.0, when several threads of one process appended to one
Chronicle file while sync points freed extents and claims reused them, a
block `append` had acknowledged could be missing afterwards: `get`
returned `None` for it, on the handle that wrote it and after reopening
the file. A thread gathering extents to fold in, on a snapshot taken
before one of them was retired, took that extent as abandoned. A claim in
the writer's turn then reused the same extent from the free list and
handed it to a writer, because it marked the extent busy without looking
whether a thread of this process held it already, and its lock on the
extent's byte succeeded since POSIX locks belong to the process. When the
gathering thread let go, it unmarked the extent and unlocked its byte
under the writer. Another thread could then fold the extent in and retire
it while the writer still wrote to it, and the writer gave the retired
extent back to be written again: what went into it after that was listed
by no slot.

A thread of a process now takes an extent only if no other thread of the
process has it, idle or busy, so the thread that marked an extent and
took its byte is the only one that lets go of either. The claim passes
over an extent a stale gather holds and takes the next free one, or a new
one at the file's end.

What this changes for you:

- Appends from many threads of one process to one file no longer lose
  blocks. Nothing in the API changes.
- Another process can no longer take an extent a writer of this process
  holds, which the unlock above allowed: two processes could have written
  one extent.
- A claim that finds the only free extent held by a stale gather grows
  the file by one extent instead of waiting for it.

Why: an extent's owner byte cannot keep threads of one process apart, so
the process's own record of who has each extent has to, and it has to
refuse a second taker rather than let two share one mark that either can
erase.

### A rule over an attribute a deductive rule derives from what a commit writes fires for what is derived

[#791](https://github.com/dialog-db/dialog-db/pull/791), following
[#802](https://github.com/dialog-db/dialog-db/pull/802).

Since 0.5.0 an inductive rule is evaluated seeded, with each row of a
commit bound into the premises that read it, unless what it reads
changed derivedly. An attribute counted as derived only when no row of
the commit wrote it. So when a commit wrote an attribute that a
deductive rule also derives from something the commit touched, the rule
over it was seeded with the rows written and missed what was derived
from them: the kind of an event mirroring the one written, a value
derived beside the one written at the same entity, or a value derived
from another attribute written in the same commit, at another entity.
Such an attribute now counts as derived, written or not, and a rule
premised on it is evaluated in full.

What this changes for you:

- A rule premised on an attribute that a deductive rule derives from
  what a commit touched fires for every match the commit made, derived
  ones included. The firings 0.5.0 missed now happen.
- Evaluated in full, such a rule also fires for matches the commit did
  not make: a command is issued again for every entity it matches, and
  a durable head is written again where it differs from what the
  entity holds, over a later write by hand. This is what a rule over an
  attribute that changed only derivedly already did.
- A commit writing such an attribute costs what a full evaluation of
  the rule costs. One row written under a rule over 1,000 events whose
  kind a deductive rule restates from itself: 283M instructions in
  0.5.0, 379M now; over 10,000 events, 2.79G and 3.76G. Without the
  deductive rule the same commit costs 4.3M and 5.6M: reading a derived
  attribute already evaluated the deductive rule over the relation.
- Rules over attributes no deductive rule derives from what a commit
  touched are seeded as before.

Why: a seed finds the matches a row is part of, at its value and its
entity, and a fact derived from the row is another fact. Narrowing the
full evaluation to deductive rules that can derive for another entity
than the one they read is not sound: a value derived at the entity
read, or derived from another attribute written beside it, is missed
as well.

### A concept read copies no inner evaluation, and a concept's descriptor clones without copying its fields

[#806](https://github.com/dialog-db/dialog-db/pull/806)

Every evaluation of a concept built a stream of about 12.5 KB, because it
held the inner evaluation of both of its arms (rows sorted on the entity,
and rows in rule order) inline, side by side, at 5.8 KB each. Its caller
moved the stream when it was returned and again when it boxed it, so a
concept read copied about 25 KB before it read a row. The inner
evaluation is now boxed, and the stream is under 1 KB. A
concept descriptor's fields are now shared, so the clones a query makes of
it on every evaluation (into its plan, its rule lookup, its renamed
premises) no longer copy every field name and attribute.

What this changes for you:

- A point read of a concept costs 16 to 26% fewer instructions: the
  `read-point` perf scenario goes from 23.0 M to 17.1 M, `rule-point` from
  51.6 M to 39.8 M, `rule-recursive` from 801.6 M to 636.1 M. Scans and
  commits do not move.
- No API, result, stored fact or wire format changes.

Why: copying that stream was 28% of the instructions of a single-entity
read of a rule-derived concept, more than the read's tree scans.
### A poll scopes its diff to the cover as it stands, copying none of it

[#844](https://github.com/dialog-db/dialog-db/pull/844).

A subscription's cover was kept sorted and disjoint, but every poll
merged its fact and rule ranges into a new list to scope the diff, the
diff checked that list's order twice, and the poll then dropped it: a
cover of ten thousand ranges cost tens of thousands of range copies,
comparisons and frees per poll, whatever changed. The cover is now kept
as one shared sorted list (`dialog_search_tree::KeyRanges`), and the
poll's diff borrows it as it stands
(`PersistentTree::differentiate_covered`). A range a re-derivation
records again, which the cover already holds, changes nothing.

What this changes for you:

- Nothing in the API of a subscription. A poll's delta, its order and
  `results()` read as they did.
- A poll does work in the log of its cover, not its size: one changed
  fact under a cover of ten thousand ranges costs what it does under a
  hundred.
- `dialog_search_tree` adds `KeyRanges`, a set of key ranges kept sorted
  and disjoint as ranges are added, and `PersistentTree::differentiate_covered`,
  a scoped difference over a union of them.

Why: after #838 the diff searched its scope, so what a poll over a large
answer still paid in the size of its cover was building, copying and
dropping that scope.

#### Compatibility and migration

- A rule that holds a composite constant: one of terms is written in
  format 4 byte for byte as before, and one holding the constant inside a
  pattern is written in format 5. Only the reader changed: a body that
  read before reads as it did, and nothing is rewritten. No release has
  stored a composite, so no older release meets such a body. Nothing for
  an embedder to run.
- `max` and `min` over a listed type: nothing stored is rewritten and no
  identity moves, an attribute's, a concept's or a rule's: a stored list
  keeps its order and `top` keeps its name. What a stored concept field
  with `pick: max` or `pick: min` over a listed attribute reads changes,
  as above. An older release reading the same descriptor ranks by value.
  A stored field with `pick: top` reads as before; `top` is deprecated
  and goes in the next version, so such a field is to be rewritten as
  `min` over the same list, which changes that concept's identity.
  Nothing is rewritten automatically. An embedder runs nothing.
- Rule body format 5: what a replica holds is unchanged. A rule that
  holds no pattern is written in format 4 as before, under the entity it
  had, so nothing is rewritten on upgrade, and `Branch::upgrade_rules`
  leaves a format 4 or format 5 body as it is (it still rewrites a body
  below format 4, into format 4). A rule that holds a pattern is written in
  format 5. A release that reads up to format 4, which is every build of
  `main` before this one, does not read a format 5 body: it lists the rule
  in `dialog.rule/invalid` as newer, the rule is inert there (it derives
  nothing and fires at no commit), and no read fails over it; the rule's
  facts replicate untouched, and the rule fires once that replica
  upgrades. 0.5.0 reads up to format 3, so it treats format 4 and format 5
  bodies alike. Nothing an embedder has to run.
- Patterns: a rule that holds one is stored in format 5 (the entry on
  format 5 above); nothing else stored changes. Nothing an embedder has
  to run.
- Nothing stored or synced changes.


### A stored rule that reads a collection entry with its key and value blank reads back

[#835](https://github.com/dialog-db/dialog-db/pull/835), reported in
[#834](https://github.com/dialog-db/dialog-db/issues/834).

A concept application that reads a collection and binds neither the key
nor the value of its entries, `entry: {the: _, is: _}`, writes the entry
as `{}`, both halves left out as blanks are. Reading it back took `{}`
for a constant and failed, so the rule's stored body did not decode and
the rule could not be loaded from a branch:
`rule hydrate: dag-cbor decode failed: an empty map names no type`. It now
reads `{}` under a collection field as that entry.

What this changes for you:

- A rule holding such a read loads from the branch it was installed on,
  under the identity it was installed with. The error above is gone for
  it.

Why: the writer and the reader of a concept application disagreed on one
form of an entry, so a rule that compiled had a body nothing could read.

#### Compatibility and migration

- Nothing is rewritten and no rule's bytes or identity change: bodies an
  earlier release stored with `{}` already hold what this one reads.
  An earlier release cannot read such a body, as it could not before.


### A read that names a derived value no longer fails with an unbound variable

Refs [#793](https://github.com/dialog-db/dialog-db/issues/793)

A read of a relation some rule derives tests the value its caller
names against what the relation's cardinality keeps; the value never
seeds the rule's body. The body was still planned as if it did, so a
premise reading the value could be ordered ahead of the one binding
it, and the read failed with `Unbound variable "is"`.

What this changes for you:

- A query, a negation (`unless`) or an inductive rule that reads a
  derived relation with both its entity and its value given now
  answers where the rule's body compares or computes with that value.
  A commit that failed with `Commit-time induction failed: rule body:
  Unbound variable "is"` goes through.
- No answer that was returned before differs.

Why: the plan and the row it runs on have to agree on what is bound.
A rule's body is now planned for the terms that seed it.

### A rule body a branch cannot read is skipped, and listed with why

Refs [#793](https://github.com/dialog-db/dialog-db/issues/793)

A stored rule body is whatever some peer wrote: one in a format an
older release stored, one in a format this release does not know yet,
bytes that are no rule, or a rule's body under an entity it does not
hash to. None of them is evaluated, and no query fails over one: a
query that met an undecodable body used to fail with `rule hydrate`,
so one bad rule arriving by merge stopped every read of its relation.

A branch now answers `dialog.rule/invalid`: one row per stored body it
does not read, of the rule's entity, valued with the reason. The reason
begins with its kind:

- `older:` the body is in a format before this release's, which
  `Branch::upgrade_rules` rewrites;
- `newer:` the body is in a format after this release's, which a newer
  release reads;
- `undecodable:` the bytes do not decode as a rule, followed by what
  the decoder said;
- `forged:` the body decodes, and does not hash to the entity it is
  stored under.

The body's format is probed before it is decoded, so the first two are
told apart from the third. Like `dialog.rule/quarantined`, the relation
is never stored: it is answered from the rule bodies the branch's
layers hold, and facts written under it are not read.

What this changes for you:

- A query over a relation some undecodable rule body would derive
  answers from the rules that do read, where it failed.
- An application lists the rules a branch skipped by querying
  `dialog.rule/invalid`, as it lists the rules set aside by querying
  `dialog.rule/quarantined`. `dialog_query::rule::statement::invalid_attr`
  names the attribute.

Why: rules are replicated facts, and a merge must never be able to stop
a program that worked before it. Skipping a rule is only safe if the
skip can be seen.

### A concept field reads one claim with a `pick`, and its cardinality follows from the pick

Refs [#793](https://github.com/dialog-db/dialog-db/issues/793), detail in
[`notes/aggregation.md`](./notes/aggregation.md) ("Selective
aggregators").

An aggregator is selective when its result is always one of its
inputs, as `max` and `min` are; a computing aggregator (`count`,
`count-distinct`, `sum`, `avg`) produces a value none of its inputs
holds. A concept field's `pick` names a selective one, which chooses
one of its relation's claims per entity: `last` the newest, `max` the
greatest value, `min` the least, and `top` the value listed first in
the attribute's `as`, which ranks its values best first as 0.4 did. The
attribute says what a write does, the pick what a read returns, so a
ranking field reads a cardinality-many attribute, whose cell keeps every
claim to rank:

```json
{ "with": { "status": {
  "the": "account/status",
  "as": ["suspended", "active", "registered"],
  "pick": "top"
} } }
```

A field's cardinality is derived from its pick unless it is written:
`last`, or no pick, is cardinality one, and `all`, `max`, `min` and
`top` are many. Written beside a pick it has to be one the pick can
read: `"pick": "last", "cardinality": "many"` keeps every claim and
reads the newest, and `"cardinality": "one"` beside any other pick is
refused. An attribute on its own takes `pick: last` or `pick: all` in
place of its cardinality, and refuses a ranking pick.

Writing `active` to an account that holds `suspended` adds a claim and
does not displace it; un-suspending is retracting `suspended`. The
aggregator ranks stored and derived claims alike, and among values it
holds equal it falls back on the order a cardinality-one read elects by.

What this changes for you:

- `ConceptFieldDescriptor` has a pick, set with `picking` and read with
  `pick` and `ranking`; `"pick": "top"` on the wire. `last` over a
  cardinality-one attribute is what the field reads anyway and declares
  nothing. A field refuses a pick its attribute cannot be read under
  (`TypeError::SelectiveAggregate`): `max`, `min` or `top` over
  cardinality one, `top` over an attribute that lists no values, `max`
  or `min` over a type with no order, or any aggregator on a keyed
  collection.
- `#[derive(Concept)]` takes `#[dialog(pick = last | max | min)]`
  on a field. `top` is refused at compile time, since
  `#[derive(Attribute)]` cannot declare a listed `as`; a ranking
  aggregator over a `#[cardinality(one)]` attribute fails when the
  concept's descriptor is first built.
- A concept's identity hashes each field's aggregator beside its
  optionality and conformance, so a field reading `top` and one reading
  without an aggregator over the same attribute are two concepts, and a
  one-field concept with an aggregator is not the attribute's relation.
- `dialog_query::selective::{Selective, Ranking}` are new, and
  `ReduceEntry::selects` is `is_selective`.
- A listed `as` is ordered: `[registered, active, suspended]` and
  `[suspended, active, registered]` are two types, and the second ranks
  `suspended` best.
- `top` is no longer an aggregator a rule's `reduce` spells, and
  `ReduceSpec` and `ReduceEntry` lose `among`: a ranked read is a field
  with a selective aggregator.
- A read of a field over stored claims chooses in the scan, as a
  cardinality-one read does. A field over a relation some rule derives
  folds the relation's claims, which is set aside like any fold if the
  field's concept closes a cycle through the relation.

Why: a rule concluding a ranked value into a relation of its own put
that value in competition with the relation's stored claims, so a
status set by hand outranked the fold until a newer state arrived. A
field with a selective aggregator reads the one relation the claims are
in, and a write through it adds to that relation. The ordering is the
type's, so a comparison in a rule body and the aggregator agree on it.

### An attribute is a relation, a type and a cardinality, and a ranked read is a concept field's pick

Refs [#793](https://github.com/dialog-db/dialog-db/issues/793) and
[#759](https://github.com/dialog-db/dialog-db/issues/759), detail in
[`notes/glossary.md`](./notes/glossary.md) and
[`notes/notation.md`](./notes/notation.md).

An attribute no longer reads under a ranking pick. It is a relation, a
value type and a cardinality, `one` (the default) or `many`, which
`pick: last` and `pick: all` also spell. Under one a read returns the
cell's newest claim, built in, recursion included, and a write succeeds
that claim: what `last` did. Under many a read returns every claim and
a write adds one: what `all` did.

```json
{ "the": "person/email", "as": "text:", "cardinality": "many" }
```

No attribute reads under `max`, `min` or `top` any more, and `the` names
one relation: a list of relations, ranked, is gone. A read that ranks a
cell's claims is a concept field picking `max`, `min` or `top` (see the
previous entry). A listed `as: [a, b]` stays, as an enumerated type
ordered best first.

An attribute spelled with a ranking pick, `select`, the entity spelling
0.3 to 0.5 wrote (`"pick": "top:"`), or a list of relations under
`the` is refused by name, so a definition in the format those releases
wrote is never read with another meaning. A rule
those releases stored is inert until `Branch::upgrade_rules` rewrites
it (see Compatibility and migration).

What this changes for you:

- `dialog_corpus::Pick` and `dialog_query::Pick` are gone.
  `dialog_corpus::Cardinality`, re-exported as `dialog_query::Cardinality`,
  takes their place on the write path: `Change::Assert(Value,
  Cardinality)`, `Instruction::Assert(Fact, Cardinality)` and
  `Update::associate(.., Cardinality)`. `Cardinality::One` succeeds the
  newest claim and `Cardinality::Many` adds. `Pick::newer` is
  `Cardinality::newer`, the one order the tree, a transaction's
  settlement, a cardinality-one read and the selective aggregators share, and
  `Cardinality::elect` is what `Pick::Last.elect` was. A batch spells
  the cardinality `one` or `many`, and reads the picks 0.3 to 0.5 wrote
  there (`last` and `max`, `min`, `top` as one, `all` as many).
- `Cardinality::estimate` is `dialog_query::estimate_cost(cardinality,
  ..)`.
- `AttributeDescriptor` loses `pick`, `with_pick`, `pick_error`,
  `with_then`, `relations`, `is_chain`, `reads_elected`,
  `scan_cardinality`, `writes`, `adding` and `adds`. `cardinality` is
  what both a read and a write go by, `with_cardinality` sets it, and
  `without_pick` is `plain`. `Attribution::pick` is `cardinality`, and
  `AttributeStatement` (`DynamicAttributeExpression`) has no `pick`
  field: its `cardinality` says how it writes, many when it is `None`.
  `TypeError::UnfitPick` is gone.
- `#[derive(Attribute)]` reads `#[cardinality(one)]` (the default) or
  `#[cardinality(many)]`, which `#[pick(last)]` and `#[pick(all)]` also
  say. A ranking `#[pick(..)]` no longer compiles on an attribute, and
  says to put the pick on the concept field.
- A query over `{ "the": .., "pick": "max:" }` fails to parse. Read a
  field `{ "the": .., "pick": "max" }`.
- `Election` elects by cardinality alone; `Election::elect_claims`,
  `ConceptRules::chain_scans` and `ConceptRules::ranks` are gone.
- An attribute's identity hashes its relation, its cardinality, its type
  and its listed values, where it hashed the list of relations it read
  and its pick. Every attribute's identity moved, and every concept's
  and every rule's with it. `dialog_query::migration` reproduces the
  identities of 0.3 to 0.5 (`attribute_reference_v1`,
  `attribute_uri_v1`, `concept_reference_v1`, `concept_identity_v1`).
- A rule body is stored in format 4, which spells cardinalities.
- `RulesUpgraded::unmapped` lists the rules the upgrade left inert, with
  why (`dialog_query::migration::Unmapped`), and
  `dialog_query::migration::rewrite_body` is the rewrite it runs.
  `dialog_query::rule::body::format_of` reads the format a stored body
  names.
- A stored rule body this release does not decode, a body spelling
  picks or one in a later format, is inert on a read as it already was
  at commit: a query that finds it no longer fails with `rule hydrate`.

Why: a pick made one attribute decide both how a relation is read and
what a write to it retires, so the relation's contents depended on how
each writer read it, and a ranked read forced a succeeding write. A
cardinality says only how many claims a cell holds. How to rank them is
the reading concept field's selective aggregator, which reads the relation as it is and
can be changed without rewriting it.

### A ranked field over a derived relation picks among its candidates, and a cycle through one is set aside

Refs [#793](https://github.com/dialog-db/dialog-db/issues/793), detail in
[`notes/aggregation.md`](./notes/aggregation.md).

A concept field with a `pick` may read a relation rules derive: it
picks among every candidate the rules offer and every stored claim.
The value it picks stands as the fact it was picked from, a derived
candidate as the newest fact its derivation consumed, so `last` over a
derived relation reads the newest derivation.

A deductive rule an author writes still takes no `reduce`
(`TypeError::ReduceInOpenRule`), as since
[#580](https://github.com/dialog-db/dialog-db/pull/580). A computed
value (`count`, `sum`, `avg`) is none of the claims it was computed
from, so concluded into an attribute it would compete with the claims
stored there; it belongs in a query that folds
([#839](https://github.com/dialog-db/dialog-db/pull/839)), and is still
refused in a deductive rule.

What this changes for you:

- A concept whose ranked field closes a cycle through the relation it
  reads is not evaluated in the cycle: the rule of the cycle installed
  last is quarantined, and `Quarantine::tested` carries
  `Absence::Folded`. The queries touching such a cycle used to fail
  (`AggregationThroughRecursion`), which remains for a cycle with no
  rule to set aside.
- `max` and `min` over values that compare equal pick the one from the
  newest fact, where they picked the one of the smaller encoding;
  values that also stand equal still fall to the encoding.
- A subscription over a concept with a ranked field on a derived
  relation is maintained per entity: a change picks again for the
  entities whose candidates it touches, and no other. Inside a
  recursion it is still recomputed.
- A concept of one field that picks, over a relation rules derive
  outside any cycle, is answered by gathering the relation's
  candidates and picking among them, without a selecting rule and a
  fold between, and a subscription over one discovers what a change
  touches over that relation. `ConceptDescriptor::picked_relation`
  names it.
- `Aggregator::Last` is new and `ReduceEntry::is_selective` tells a
  picking fold from a computing one. A sum of naturals is a natural.
- A query that folds with `last` or `top`
  (`Aggregator::orders_by_standing`) is evaluated afresh, under a
  standing fold, on each poll that moved one of its rows: a row the
  subscription keeps carries no standing, which those two choose by.
  Every other fold is still maintained per group.

Why: a status ranked among the cases several rules derive is the read
tonk's library needed, and it has to answer the same on every replica
whatever order the candidates arrive in.

### A rule deciding on a cardinality-one value inside its own cycle is set aside

Refs [#793](https://github.com/dialog-db/dialog-db/issues/793), detail in
[`notes/attribute-heads.md`](./notes/attribute-heads.md).

Inside a recursive component a cardinality-one read sees every
candidate the fixpoint derives, and readers elect the newest once the
component is derived in full. That is exact for a rule that passes the
value along: each candidate it derives stands by the one it carried, so
the election at the exit picks what a stratified read would have. It is
not for a rule that decides on the value. Such a rule fires for a
candidate no reader elects, and nothing at the exit takes its
conclusion back: with `b` reading `own` over a losing `end`, a rule
concluding a label for `a` from `x/label(next) = "end"` concluded it,
though every read of `b` returned `own`. The same body under another
head, outside the cycle, concluded nothing.

A cardinality-one read whose value the rule decides on is now an
absence test, so a rule closing a cycle through one is quarantined. A
rule decides on a value when the read is pinned to a constant, or when
the value, or anything a formula or an equality computes from it, is
compared, joined on by another read, negated over, or checked against a
constant. A rule passes a value along when it reaches the head and
nothing else, directly or through formulas.

What this changes for you:

- A rule that reads its own relation under cardinality one, of another
  entity, and decides on the value read is set aside, and listed by
  `ProgramAnalysis::quarantined` and `dialog.rule/quarantined`. A query
  over the relation returns what the remaining rules derive. Before, it
  returned that rule's conclusions too, the ones drawn from losing
  candidates among them.
- So is a rule deciding on a field of a concept of several fields,
  where the field is of cardinality one or declares a selective
  aggregator: the concept elects the field's claim, and the premise
  tests that election. A rule deriving a relation from the claim a
  field's aggregator elects of that same relation closes its cycle
  through a fold, and is set aside as one.
- A rule carrying a value down a chain, or computing from it (a
  position from a successor's position, a depth from a parent's), is
  unaffected.
- A rule rewriting the cell it reads, `x/status(e) := "todo" :-
  x/status(e) = "triage"`, is unaffected: it reads its own relation of
  its own entity, and its conclusion succeeds the value it read.
- The same rule outside a cycle is unaffected: there the read is of a
  relation derived in full.
- To decide on a value inside a recursion, conclude into another
  relation, which takes the decision out of the cycle, or make the
  decision in an inductive rule, which runs at commit over the settled
  value.
- A quarantine says what it was made over. `Quarantine::tested` names
  the cycle's plainest absence test: the rule making it (`by`), the
  relation tested (`target`) and how (`absence`). `Tested` is new, and
  `Absence::Decided` is a cardinality-one read the rule decides on.
  `Absence::Elected`, a ranked read, is gone with ranked reads. Code
  building a `Quarantine` by its fields names the new one.
- `Polarity::Electing` carries `decides`, and no longer `ranked`.

Why: a rule set has to mean one thing whatever else is installed. The
same body gave a different answer when an unrelated rule closed a cycle
around it, with nothing reported. Setting the rule aside is reported,
and names the rule.

### Compatibility and migration

- A claim one round of induction both retracts and asserts: nothing
  stored changes shape, no identity moves and nothing is rewritten on
  upgrade. What induction writes differs: such a claim keeps its row,
  where an older release retracted it. An older release reads what
  this one writes. An older replica committing under the same rules
  still drops such claims in its own commits until it upgrades; what it
  pulls from a newer one simply holds them. An embedder runs nothing.
- Composites: nothing an existing space holds is rewritten or read
  differently, and opaque `record:` values keep their encoding and identity.
  No release up to and including 0.5.0 wrote a composite (the draft #812 was
  never released), so there is nothing to migrate. A space that stores a
  composite cannot be read by an older release, which takes a key with type
  byte 22 for corruption: once a replica writes one, every replica reading
  that space needs this release. Nothing an embedder has to run.
  A single-file archive keeps a composite's atoms as tokens of their own;
  a file this release writes holding a composite is read by no older
  release, as the space is not. Nothing in a file an older release wrote
  is rewritten.
- A rule over an attribute a deductive rule derives from what a commit
  writes (#791): nothing stored changes shape and nothing is rewritten on
  upgrade, and an older release reads everything this one writes. What
  induction writes can differ: the firings 0.5.0 missed happen from
  the first commit after the upgrade, and are not replayed for commits
  made before it. An embedder runs nothing.
- A rule an older release stored spells its attributes under picks (0.2.0 to 0.5.0) or under the older `cardinality` (0.1.0), and sits under an
  identity that hashed them. This release reads a rule body only in its
  own format and only under the identity it gives it now,
  so every such rule is inert: it derives nothing, fires at no commit,
  and no read fails over it. Nothing is rewritten on upgrade.
  `Branch::upgrade_rules` (or `Branch::migrate`) is what an embedder
  runs: it rewrites each rule whose attributes read under `last` or
  `all` (spelled, or implied by a plain attribute) into cardinality one
  or many, asserts it in format 4 under its identity now, and retracts
  every rule fact the older release wrote for it. An attribute read
  under `max`, `min` or `top` (including a listed `as` without a pick,
  which read as `top`) is rewritten into a field with that pick over a
  cardinality-many attribute, a `top`'s list kept in its order, best
  first. A rule
  over a chain of relations has no rewriting with the same meaning: it
  is left as it is, inert, and listed in `RulesUpgraded::unmapped` with
  why. Its author replaces it with a rule deriving one relation from
  each, read by a field with a selective aggregator. When to run the upgrade,
  once after a full replication or on every pull, is the embedder's
  decision, as before.
- Identity generations: nothing stored changes, no identity is
  computed differently and nothing is rewritten on upgrade. An older
  release reads everything this one writes as it did. An embedder runs
  nothing for this; the constant and the generation 2 functions are for
  the rewrite it runs when identities move.
- The upgrade also moves a `dialog.concept/transient` marker keyed by a
  concept's 0.3 to 0.5 identity, as it moves one keyed by its 0.1.0
  identity. An application that keyed its own facts by an attribute or
  concept identity moves them with `dialog_query::migration`.
- An older release cannot read what this one writes: a rule body in
  format 4 is refused by name, and a rule stored under its new identity
  is inert there, as an older release's rules are here. A batch this
  release encodes spells `one` and `many`, which an older release does
  not read; this release reads the batches an older one encoded.
- What the tree stores is unchanged: a cell's claims, and how a
  cardinality-one write elects the claim it succeeds, are as `last`
  left them. A write a 0.3 to 0.5 replica made under `max`, `min` or
  `top` succeeded the claim that pick elected; those claims stay as
  written. A field the upgrade rewrote to read with that pick's aggregator
  reads the same claim; any other read under cardinality one returns the
  cell's newest.
- A rule the analysis sets aside for deciding on a cardinality-one value
  in its own cycle stays installed and is evaluated again once the cycle
  it closed is gone. Replicas on releases either side of this one can
  answer a query over such a cycle differently.


## 0.5.0 (2026-11-19)

### A subscription's poll costs the entities it touched, not its whole answer

[#838](https://github.com/dialog-db/dialog-db/pull/838).

A subscription keeps a range of its cover per entity it read, and a
poll gates the tree difference by that cover. Every node, buffered
operation and changed entry the difference walked was tested against
every range, so one changed fact under a cover of ten thousand ranges
cost tens of thousands of comparisons per node. The scope is now sorted
and merged once and each test is a binary search. A maintained poll
then compared each re-derived entity with the kept answer by scanning
every kept row; the kept answer is now indexed by entity, so the
comparison reads that entity's rows only.

What this changes for you:

- Nothing in the API. A poll's delta, its order and `results()` read as
  they did.
- A poll over a large cover, and a poll that re-derives many entities of
  a large answer, does work in the touched entities and the log of the
  cover, where it did work in their product.

Why: a subscription's answer grows with the data it watches, and the
cost of a poll that changed one fact grew with the square of it.

#### Compatibility and migration

- Nothing stored or synced changes.

### An ephemeral line can be read as it stood, at no cost to take: `Ephemeral::frozen`

[#841](https://github.com/dialog-db/dialog-db/pull/841).

`Ephemeral::frozen` returns a `Frozen`: the line's facts, tombstones and
revision as they stood when it was taken, answering the reads the line
answers (`scan`, `select`, `export`, `len`, `is_empty`, `revision`) and
never changing after. The line now holds its facts in chunks shared by
reference count, so taking one copies nothing, and a write to the line
while one is held copies the chunk it changes rather than every fact.

What this changes for you:

- New: `Ephemeral::frozen() -> Frozen`, and the `Frozen` type.
- Nothing a read of a live line answers changes.

Why: a subscription's poll is to read one fixed state of every layer,
so that subscriptions of one view agree and a channel's facts, which
last one tick, are read by a retry as they stood. Until now the state of
a session overlay or a memory layer at an earlier point was had only by
copying the whole line and undoing its instants.

#### Compatibility and migration

- Nothing stored or synced changes, and nothing is rewritten.

### A query can fold, and a fold can be kept current

[#839](https://github.com/dialog-db/dialog-db/pull/839).

Decided in [dialog-db/dialog-db#830](https://github.com/dialog-db/dialog-db/pull/830)
(a fold is a query type, not a rule) and
[dialog-db/dialog-db#837](https://github.com/dialog-db/dialog-db/pull/837)
(how it is kept current).

`Fold::new(groups, inputs, reduce)` is a query in `dialog-query` that
folds the rows of `inputs` with a `Reduce`, one row for each row of
`groups`, joined on the grouping fields of the `Reduce`. A group no input
joins is folded over inputs all absent, so `count` gives 0 for it. A fold
is asked and subscribed to as any query is; it has no name and installs
no rule, so an authored deductive rule still cannot fold.

What this changes for you:

- New: `Fold` and `FoldConclusion`, a folded row with the value of each
  grouping field and each fold.
- New: `Branch::subscribe_fold(fold)` and `Stack::fold_subscription(fold)`
  give a `FoldSubscription`, polled as a `Subscription` is and giving
  `Delta<FoldConclusion>`. It keeps a subscription of each of the fold's
  two queries and the rows they report, and folds again in memory only the
  groups a change moved a row of: a group whose row changed reports the
  old row retracted and the new one asserted, and what it reports adds up
  to what asking the fold whole gives. Both inner subscriptions are polled
  at one state of the branch.
- `Branch::subscribe(fold)` works too, and asks the whole fold again on
  each poll that touches what it read; `Branch::subscribe_fold` is the
  one that keeps it current.
- New: `Application::bindings`, with a default of `None`: the bindings a
  conclusion was realized from. Attribute queries, concept queries and
  folds give them, and a `FoldSubscription` needs them of both its
  queries; one over a query that gives none fails to poll.
- New: `Fold::groups`, `Fold::inputs`, `Fold::key`, `Fold::fold_groups`
  (the fold of a set of groups, which asking a fold and a
  `FoldSubscription` both fold with) and `Reduce::fold_rows`.

Why: a rule cannot fold, and a fold asked again whole on every change
folds every group for a change to one. A query that folds needs no name
and no stored value beside its own, and kept current it costs the two
inner polls and the groups a change moved.

#### Compatibility and migration

- Nothing stored or synced changes: a fold is a query asked in memory and
  nothing of it is written. Nothing is rewritten on upgrade, and an older
  release reads what this one writes.

### A concept read binds stored values in place, and reads optional fields in one pass

[#825](https://github.com/dialog-db/dialog-db/pull/825). Research: [zero-copy answers](research/zero-copy-answers.md)

A concept read used to turn every row it scanned into owned values (an
entity with a hash of its URI, an attribute, a value, a claim) and copy
them again into each joined row. It now binds the scanned row in place
and makes a value only when something reads it as one; a join compares
the stored bytes. A concept with optional fields used to be read by
probing every optional field once per entity; its optional fields are
now read once each, in entity order, beside the required ones, and an
entity a field lacks binds `Absent` as before.

What this changes for you:

- **List reads and subscriptions are faster.** On tonk-team at 10×, the
  issue list reads in 82 ms where it took 288, the activity list in 106
  where it took 486, and their subscription polls in 23 and 46 ms where
  they took 939 and 1,398. Results are the same, row for row.
- **New reads that do not hydrate:** `Match::view` and
  `ConceptConclusion::view` return a `ValueView` borrowed from the stored
  row; `Match::absent` answers without making the value;
  `Conclusion::this_uri` (with a default) and
  `ConceptConclusion::this_uri` give the entity's URI as stored.
  `dialog_corpus` gains `ValueView`, `FactView::checked` and `CheckedRow`.
- Nothing stored or synced changes.

Why: copying values was most of a concept read's cost after the scan,
and a concept with an optional field, which most of tonk's are, was
never merged, so its list read probed every field of every entity.

### A coverage entry spills nothing

[#833](https://github.com/dialog-db/dialog-db/pull/833).

A coverage key carries a hash of the covered value in the spilled form,
whatever the value's size, so claims can be matched by it; no value is
stored under that hash. `tree/entry` read it as a spilled value, so an
inspector offered to read a value that could never be read, and waited
on it.

What this changes for you:

- `tree/entry` yields no `spill` for an entry at a coverage key, and
  `inspect::entries` no `spill` in its `EntrySummary`.
- `dialog/key-part` reads a coverage key as its index, origin, edition,
  entity and attribute and a part of kind `reference`, `value:<hash>`,
  where it read a value type of `Bytes` and a `spill` part.

Why: a spill names bytes a reader can fetch; a coverage key's hash names
none.

#### Compatibility and migration

- Nothing stored changes.

### An index node's buffered operations are its entries, and a separator reads as the key it is

[#832](https://github.com/dialog-db/dialog-db/pull/832).

An index node buffers writes headed for its children (`novelty`), and
`tree/entry` gave no rows for an index, so a query could count them but
not read them. A separator was split into its tag and one `prefix` part
of lossy text, which for a history separator was the origin's raw bytes,
and a forced seam's separator, the last key before the seam padded with
zero bytes past the separator bound, read as a key whose value was the
padding.

What this changes for you:

- `tree/entry` of an index node yields one row per operation it
  buffers, with a new slot `span` naming the span the operation rides,
  as `tree/span` names it. `span` is absent on a segment's entry. `at`
  counts the node's buffered operations; `rank` is absent on them.
- `dialog_corpus::inspect::entries` returns an index node's buffered
  operations, and `EntrySummary` has a new field `span: Option<u64>`.
- `dialog/separator-part` reads a separator field by field as far as it
  reaches, as `dialog/key-part` reads a key: `entity`, `attribute`,
  `vtype` and `value` parts in the ordering's order, the value-type byte
  on its own, and a Bytes or Record value as hex. A history or coverage
  separator yields its `origin` (hex) and `edition`. The `prefix` part is
  gone.
- A separator that is a whole key reads as that key. A forced seam's
  separator reads as the key before the seam and a part of kind
  `padding`.

Why: an inspector shows what a node holds, and the operations an index
buffers and where its boundaries fall are part of that.

#### Compatibility and migration

- Nothing stored changes: both read blocks as they are.

### A revision is named by `revision:`, not by a `did:key:`

[#829](https://github.com/dialog-db/dialog-db/pull/829).

A revision's entity was `did:key:z6Mk<base58(hash)>`: the shape of a key
DID around a blake3 hash of the revision's version. It read as a
principal and was none, and nothing could tell a revision from a branch,
a replica or an operator by its name.

It is now `revision:<base58(hash)>`, over the same hash
(`Version::entity`, `Revision::entity`). A commit writes the revision's
signed record under that entity, `dialog.branch/revision` carries it, and
the built-in `Revision`, `RevisionParent` and `RevisionAncestor` concepts
yield it for `this`, `parent` and `ancestor`.

What this changes for you:

- A revision entity you read from a query starts with `revision:`. Code
  that told revisions apart by parsing `did:key:z6Mk` has to change; an
  entity compared whole needs nothing.
- `Version::entity_did` is `Version::entity_uri`. `Version::legacy_entity`
  and `Version::legacy_entity_uri` give the name a revision had before,
  and `Version::legacy_of` turns the current name into it.
- A new formula, `dialog/revision-slot`, takes a revision entity as `of`
  and yields as `is` each entity its record may be stored under: the
  entity itself, then its older name.
- A fact your space wrote about a revision under its older name (a
  comment on a commit, say) still names the older entity, and no longer
  joins the `this` of `Revision`. Join through `dialog/revision-slot`,
  or rewrite those facts.

Why: an entity's scheme is what a reader, a rule and a view have to tell
kinds of thing apart, and a node (`node:`) and an asset (`asset:`)
already say what they are. A name shaped like a key invites treating it
as one.

#### Compatibility and migration

- A replica written by an older release holds each revision's record
  under `did:key:z6Mk…`. Nothing is rewritten on upgrade: this release
  reads a record under either name (the history reader, the built-in
  revision concepts and a stack's durable layers all do) and writes new
  ones under `revision:`, so one tree can hold both. An embedder runs
  nothing.
- A durable layer's pointer, the `dialog.branch/revision` of
  `memory:<name>` stored in the primary's tree, names its revision by
  the name the release that wrote it used. This release follows a
  pointer an older release wrote, by either name, and opens the layer;
  the layer's next move points the primary at the new revision by its
  `revision:` name and retires the older claim, as any move does.
- An older release cannot find a record written under `revision:`, so
  it cannot read the history of what this release committed, nor follow
  a durable layer's pointer this release wrote. Upgrade every replica of
  a space before any of them commits with this release.

### A tree node is a handle, and five resolvers read it

[#829](https://github.com/dialog-db/dialog-db/pull/829), argued in [design/tree-exploration.md](./design/tree-exploration.md).

The seven `tree/*` resolvers each loaded a node's block and decoded one
part of it, so a query asking three things of one node decoded it three
times. A segment's entries were split across three of them (`tree/key`,
`tree/entry`, `tree/blob`) and paired by position. A node was named by
the base58 string of its hash, which is not an entity, so a rule could
not derive a concept whose rows were nodes.

A node is now opaque to a query, which holds its handle: the entity
`node:<base58(hash)>` (`Entity::from_node`, `Entity::node_hash`).
`dialog.branch/tree` carries the root's handle and `tree/span` yields
each child's. A resolver takes the handle as `of`, asks the environment
for the decoded node (`nodes::Read`, as the tree's own reads do), and
yields one part of it. The environment decodes a node once and keeps it.

```text
tree/node   what the node is, and the format it was written under
tree/span   an index node's children, with what the node holds about each
tree/entry  a segment node's entries, each whole
tree/block  the node's block as stored
tree/value  a spilled value's bytes, by its asset: entity
```

What this changes for you:

- `dialog/key-part` and `dialog/separator-part` yield `part`, each
  component's own entity (`part:<base58(blake3(key))>/<at>`), so a rule
  can derive one row per component. A query asking either formula
  names `part` among its terms too.
- `tree/key`, `tree/blob` and `tree/manifest` are gone, with
  `TreeKeyQuery`, `TreeBlobQuery` and `TreeManifestQuery`. A key and its
  rank, and a blob record's fields, are slots of `tree/entry`; the
  manifest's fields are slots of `tree/node`. `tree/block` is new.
- `of` is an entity: a `node:` for every resolver but `tree/value`,
  which takes an `asset:`. A base58 string or raw bytes, which `of`
  used to take, match nothing.
- `dialog.branch/tree` is an entity (`branch::Tree(Entity)`), not text.
  A concept that reads it declares the field as an entity; the hash is
  `tree.0.node_hash()`.
- `tree/span` yields `node` as a `node:` entity, and a new slot `span`:
  the span's own entity, `span:<base58(node)>/<position>`. `tree/entry`
  yields `entry`, `entry:<base58(node)>/<position>`. A rule deriving a
  concept of spans or of entries has an entity for each row.
- `tree/entry` yields `spill` and `blob` as `asset:` entities. A slot
  that does not apply to a row is absent: `spill` when the value is
  inline (it was an empty string), `origin` and `edition` when the
  write was unversioned (they were empty and 0), the blob slots on any
  entry that is no blob record, and the claim slots on one that is.
- Slot names are kebab-case: `max-separator`, `inline-n`,
  `spill-prefix`, `max-segment`, `frame-ceiling-factor`,
  `anchor-selector`. `fanout_n` is `fanout-exponent`. `tree/node` also
  yields `entry-overhead`, `key-overhead` and `link-overhead`, which no
  resolver did.
- `RESOLVER_COST` is 50,000, from 200. Where a rule's body has a fact
  that decides whether a node is read, the planner now asks the fact
  first, whether or not the caller gave the node.
- `dialog_corpus::inspect` has a reading of each part that takes a
  decoded node (`summary`, `spans`, `keys`, `entries`, `blob_records`,
  `manifest`) beside the `inspect_*` functions that take bytes.
- A resolver still requires its handle. A rule whose body is a resolver
  alone is refused at install as before; a rule that names its nodes by
  a fact installs, and reads those nodes and no others.

Why: a query should be able to read everything a node holds without the
embedder decoding blocks for it, and the reading and decoding of a node
belong to the environment, which already does both for the tree. A
handle that is an entity is what a rule needs to make a node, a span or
an entry the subject of a concept.

#### Compatibility and migration

- Nothing stored changes. A branch's `dialog.branch/tree` is derived
  when a query runs and never written; it now reads as a `node:`
  entity. A durable layer's pointer does write `dialog.branch/tree`, of
  `memory:<name>` in the primary's tree, and still writes it as the
  tree's base58 text, so the same attribute reads as an entity of a
  branch and as text of a layer: a rule reading a layer's pointer reads
  text. Resolver rows are read off blocks. An older release reads
  everything this one writes. A rule stored on a branch that uses `tree/key`,
  `tree/blob` or `tree/manifest`, a snake_case manifest slot, or a
  base58 string as `of`, or that reads `dialog.branch/tree` as text, no
  longer compiles or no longer matches, and has to be re-authored.

### A stack learns what its replica holds of a tree by command

[#829](https://github.com/dialog-db/dialog-db/pull/829), argued in [design/tree-exploration.md](./design/tree-exploration.md).
Built on the stack's providers
([#816](https://github.com/dialog-db/dialog-db/pull/816)).

A tree resolver fetches a node the replica lacks, so that what a query
answers does not depend on what happened to be local. That left a
consumer no way to know, before it asked, whether asking for a node
would fetch it.

`dialog_repository::holdings` has two commands for that and the
providers that answer them (`holdings::provide(&stack, env)`):

- `dialog.node.probe/node`, the node to ask about, is answered with
  `dialog.node.probed/node` and `dialog.node.probed/local`: whether the
  replica holds the node, with nothing fetched.
- `dialog.node.fetch/node`, the node to bring local, is answered with
  `dialog.node.fetched/node` and `dialog.node.fetched/local`: whether
  the replica holds it, having tried.

What this changes for you:

- `holdings::{provide, providers, probe, fetch}` and the attribute names
  beside them are new. A stack they are not registered with reports
  these commands as unhandled, like any command no provider claims.
- An answer is written into `control:`, so it is a command for one tick
  and nothing keeps it. A space keeps it with a rule, and where the
  rule's attribute is placed is where it is kept: on a layer of the
  session it is read and subscribed to through the stack, never
  committed or synced, and gone with the session. A space with no such
  rule gets nothing.
- A command whose value is not a `node:` entity, and a read that fails,
  fail the command: it gets a `dialog.command/failed` row with the
  reason.

Why: whether a block is held changes with no commit, so a query step
that reported it would answer the same query differently from one
moment to the next. As a command's answer captured by a rule, it enters
a query as a fact of a layer the query has pinned.
### A provider claims the commands whose rows satisfy its concept, and a command sees the rows of its tick

[#823](https://github.com/dialog-db/dialog-db/pull/823). Refs
tonk-labs/tonk#1318, issues 9 and 10.

A provider now claims a landed command when the command's rows satisfy
its concept, the test decoding the concept from them would pass: every
required field of the concept has a row of the field's attribute and,
when the field declares a type, a value of that type. Optional fields are
not needed, and rows of other attributes do not disqualify. Every
provider whose concept a command satisfies runs, in registration order,
each writing its own `answered` or `failed` row. One provider can answer
several concepts (`Provider::new(&current, handler).also(&legacy)`); it
runs at most once per command, and its `answered` row names the first
of its concepts the command satisfies. The rows of the `control:` entity
itself that land in a tick are the tick's own facts (who asked, and
whatever an embedder stamps on the loopback): they are no command, and
each command carries them as `Command::tick`.

What this changes for you:

- A provider no longer claims a command because one of its rows has an
  attribute of the provider's concept. A command that carries one shared
  attribute (a time stamp, an email) and lacks the concept's other
  required fields is no longer that provider's; a command no concept is
  satisfied by is `unhandled`, where before the first provider sharing
  an attribute ran.
- A command whose rows satisfy several providers' concepts runs every
  one of them, in registration order, where before only the first
  registered ran. It gets one `dialog.command/answered` row per provider
  that answered and one `dialog.command/failed` row per provider that
  failed (both are now cardinality-many), and `EffectReport::answered`
  and `EffectReport::failed` name it once per provider. A rule copying
  `answered` should read it under `pick: all`.
- `Provider::also(&concept)` adds a concept to a provider, and
  `Provider::concepts()` lists them; `Provider::concept()` is the first.
  Registering a provider writes one `dialog.provider/command` row per
  concept no provider registered before, and replaces the first
  registered provider that shares a concept with it.
- `InvocationReport::commands()` and `Effect::commands()` leave out the
  rows of the `control:` entity itself, so they are never a command,
  never `unhandled` and never marked `dialog.command/invoked`.
  `InvocationReport::tick()`, `Effect::tick()` and `Command::tick()`
  return them.

Why: three of tonk's command groups share an attribute
(`dom.event/time-stamp`, `elements.email/value`, `current-target/value`),
so the provider of the wrong one ran: CheckEmail answered
RegisterAccount, ProfileRename renamed a profile with a space's name.
Tonk's own registry ran every handler whose decode succeeded, once per
handler, which is the rule now. And tonk writes the asking client as a
row of `control:` for the tick, which no provider could read and every
effect reported as an unhandled command.

#### Compatibility and migration

- Nothing stored or synced changes: the rule is applied in memory when
  an effect runs, and the lifecycle and tick rows live in `control:` for
  one tick. Nothing is rewritten on upgrade, and an older release reads
  what this one writes.

### A stack transaction's publish reports like an invocation, so its commands reach the stack's providers

[#821](https://github.com/dialog-db/dialog-db/pull/821).

`Stack::transaction()` now returns a `StackTransaction`, the
stack-bound counterpart of a branch `Transaction`, and its publish
returns an `InvocationReport`, the type `Invocation::perform` returns:
the revision the publish left the primary at, the ticks its induction
ran, and the commands it landed on `control:`, with `effect()` to run
the stack's providers over them. That holds for the one-step
`.commit().publish().perform(&env)` and for a staged chain:
`.commit().perform(&env)` returns a `StackBatch`, whose next links are
stack transactions too, and its `publish().perform(&env)` reports the
commands of every link. A publish that loses its compare-and-swap
still fails with `PublishError::VersionMismatch`, lands nothing and
reports nothing; it is not re-run. `Branch::transaction()` is
unchanged, and its publish still returns a `Revision`.

What this changes for you:

- A stack transaction's publish returns an `InvocationReport`, not a
  `Revision`: `let revision = stack.transaction()...publish().perform(&env).await?`
  becomes `let report = ...; report.revision()`, and the same for a
  staged chain's `batch.publish()`. This is breaking for callers that
  read the revision.
- `Stack::transaction()` returns `StackTransaction<&Branch>`, its
  `commit()` a `StackCommit`, its staged batch a `StackBatch` and their
  publishes `StackPublish` and `StackBatchPublish`. They carry the
  methods of the branch types they wrap (`assert`, `retract`,
  `dispatch`, `integrate`, `query`, `allow_empty`, `canonicalize`,
  `amend`, `version`, `revision`, `induced`, `shares`, `claims`,
  `select`); a signature that named `Transaction<&Branch>` for a stack
  transaction names `StackTransaction<&Branch>`. Their `perform` takes
  a `StackEnv`, as `Invocation::perform` does.
- `report.effect().perform(&env)` after a stack transaction runs the
  providers over its commands, as after an invocation; before, a
  command asserted through a stack transaction landed on `control:`
  but no report named it, so no provider answered it.
- `dialog-stack` re-exports the five new types.

Why: tonk's evaluate route and its "absent branch" transact path build
the transaction themselves, resolved against a head they chose, and
published it with `.commit().publish()`, which answered a `Revision`
and nothing more, so a command asserted that way was never answered by
a provider. Only its route through `Stack::assert(..).perform(&env)`
got a report. A stack transaction's publish now reports what it landed
the same way, so every route runs the providers.

#### Compatibility and migration

- Nothing stored or synced changes: the publish writes what it wrote
  before, and the report is computed in memory from the staged chain.
  Nothing is rewritten on upgrade, and an older release reads what this
  one writes.

### A stack shares one evaluation of a query among its readers, and delivers to them after a dispatch

[#819](https://github.com/dialog-db/dialog-db/pull/819).

`Stack::subscribe(query)` now registers the stack's subscription to a
query, or attaches to the one it holds, and returns a `Subscriber`: a
second reader of the same query reads the same evaluation instead of
running its own. A subscription is keyed by `QueryHash`, a new hash of
the query's canonical encoding in `dialog-query`, and its type. A
dispatch through the stack that published or wrote into a memory layer
schedules a poll; `Stack::settle(&env)` polls every subscription once
when one is scheduled, delivers each reader the `Delta` the poll made,
and clears the mark, so the writes of one request cost one
evaluation. `Effect::perform` settles after its answer dispatch. The
evaluation is still dialog's demand-gated `Subscription`, unchanged.

What this changes for you:

- `Stack::subscribe` returns a `Subscriber<Q>`, not a `Subscription<Q>`,
  and asks `Q: Serialize`. The unshared evaluation it returned is now
  `Stack::subscription(query)`; `Branch::subscribe` is unchanged.
- `Subscriber` has `poll()` (the deltas delivered since the last read,
  without waiting), `next().await` (waits for a delivery), `hash()`,
  `results()` (the whole current set), `evaluated()`, `recomputes()`,
  `maintenances()` and `close()`. A reader's first delivery is the whole
  result set as `asserted`; each later one is what a poll changed, and a
  poll that changed nothing delivers nothing.
- `Stack::settle`, `Stack::schedule_poll`, `Stack::poll_scheduled` and
  `Stack::subscriptions_held` are new. A write the stack does not see,
  into `Branch::overlay()` among them, is scheduled with
  `schedule_poll`. A sync that moved the primary, `Stack::channel`
  creating a channel and `Stack::provide` schedule one too; a write into
  a channel does not.
- A subscription lives as long as a reader of it: the stack holds it
  weakly, and it goes when its last `Subscriber` is dropped or closed.
- `Stack::settle` and `Effect::perform` need a `Clone` environment.
- `Effect::perform` settles the stack, with or without commands to
  perform. A failed poll does not fail the effect: it is reported by
  `EffectReport::unsettled()` and stays scheduled.
- `dialog_query::QueryHash` is new: `QueryHash::of(&query)` hashes the
  query's DAG-CBOR encoding with its fresh variables (`__<n>`) named
  again by where they first occur, so two queries built alike hash
  alike.
- `dialog-stack` re-exports `Subscriber`, `Subscription`, `Delta` and
  `QueryHash`, and `dialog-repository` re-exports `QueryHash`.

Why: tonk kept this registry, the fan-out and the poll scheduling in a
reactive layer of its own over dialog, keyed by a query hash it
computed itself. They are generic: one evaluation per query, delivery
to every reader, and polls scheduled by writes and drained once per
turn belong beside the stack's channels, so an embedder keeps only its
transport.

#### Compatibility and migration

- Nothing stored or synced changes: subscriptions and their deliveries
  live in memory, and the stack writes no fact for them.
- A caller of `Stack::subscribe` that polled the returned
  `Subscription` itself calls `Stack::subscription` instead, or moves
  to the `Subscriber` and `Stack::settle`.

### The stack has a crate of its own, `dialog-stack`, which re-exports it from `dialog-repository`

[#818](https://github.com/dialog-db/dialog-db/pull/818), argued in
[notes/layers.md](./notes/layers.md) ("Decisions of 2026-11-15",
point 1).

The maintainer decided that the stack gets a separate crate later and
its name now. `dialog-stack` is that crate's seam: it re-exports the
stack from `dialog-repository` under one roof, and its README is the
crate's front page, a guide for an embedder, with every example
compiled as a doc test. No code moved. When it does, an embedder that
imports from `dialog_stack` changes no import.

What this changes for you:

- `dialog-stack` is a new workspace crate, released with the others at
  the workspace version. It re-exports `Stack`, `OpenStack`,
  `StackEnv`, `OpenStackEnv`, `SyncStack`, `SyncEnv`, `SyncError`,
  `Layer`, `LayerType`, `LayerDeclaration`, `Durable`, `Ephemeral`,
  `Channel`, `Arrivals`, `Invocation`, `Attempt`, `InvocationReport`,
  `MAX_RERUNS`, `Effect`, `EffectReport`, `Provider`, `Command`,
  `Failure`, `Backup`, `LayerBackup`, `CommitError`, `Download`, the
  layer names (`MEMORY`, `SESSION`, `CONTROL`, `memory_layer`,
  `session_layer`, `control_layer`) and the attribute functions of the
  facts a stack writes (`layer_type_attr`, `layer_branch_attr`,
  `revision_attr`, `selection_attr`, `invoked_attr`, `answered_attr`,
  `failed_attr`, `unhandled_attr`, `progress_attr`, `provided_attr`).
- Nothing in `dialog-repository` changes: every name above is still
  exported there.

Why: the stack is moving into its own crate, and an embedder (tonk is
the first) should take its imports from where the stack will live, so
the move does not touch them. The README gives the stack one place
that explains it from opening to backup.

#### Compatibility and migration

- Nothing stored or synced changes. `dialog_repository::Stack` and
  `dialog_stack::Stack` are the same type, as is every other name the
  crate re-exports, so the two imports mix freely.

### A stack's `session:` is its own store, apart from `Branch::overlay`, and `Stack::backup` and `OpenStack::restore` carry it to another stack

[#817](https://github.com/dialog-db/dialog-db/pull/817), argued in
[notes/layers.md](./notes/layers.md) ("Decisions of 2026-11-15",
point 8).

A stack's own layer, `session:`, was the primary's session overlay,
`Branch::overlay()`, so everything the stack records (revision rows,
`dialog.layer/*` facts, the selection, `dialog.provider/command`
discovery) landed in the store an embedder keeps its own session facts
in, and an embedder that snapshots, clears or prunes the overlay had to
filter dialog's rows out by hand. A stack now mints its own store for
`session:` when it opens. `Branch::overlay()` is the embedder's: the
stack writes nothing into it, and a read through the stack binds its
rows to the primary, `memory:`. `session:` is the stack's own rows and
the branch metadata (`dialog.branch/*`, `dialog.replica/*`,
`dialog.peer/*`), and holds one row per cell: the primary's
`dialog.branch/revision` is the metadata's row alone, where the stack
used to record a second one that read differently between a head move
and the next dispatch. `Stack::backup()` takes `session:` and every
declared session layer as a serializable `Backup`, and
`branch.stack().restore(backup)` opens a stack holding them again.

What this changes for you:

- `Backup` and `LayerBackup` (`Serialize + Deserialize + Clone +
  Debug`), `Stack::backup()` and `OpenStack::restore(backup)` are new. A
  backup holds each session layer's rows (tombstones too), the revision
  it stood at, the primary's branch entity, and the revision the stack
  last recorded the primary at. It leaves out channels, durable layers
  and `Branch::overlay()`. Restoring replays the rows, records again the
  layers that stood where their rows said, and leaves a layer that had
  moved unrecorded, like a primary committed or pulled through another
  handle while the backup was away, to be the first dispatch's stimulus,
  so a rule premised on its revision fires once. Providers' discovery
  rows and channels created at run time are not restored: register and
  create them again. A layer the schema no longer declares is dropped
  and the rest is restored.
- `CommitError::ForeignBackup` is new: a backup restored on a stack of
  another branch, refused before any layer is built, with nothing
  written.
- `Branch::overlay()` holds none of dialog's rows: `Stack::layer` of
  `session_layer()` returns the stack's own store, a write
  `.at(session_layer())` through a stack lands there, and
  `Stack::channel` and `Stack::provide` write there. A row of the
  overlay is read through a stack as `in: memory:`, not `session:`. A
  branch read or committed without a stack is unchanged.
- A second stack opened on a branch has a `session:` of its own, with
  its own revision rows, selection and created channels; it no longer
  sees what the first one wrote there. Restore a backup to carry them.
- The stack records no `dialog.branch/revision` row of the primary in
  `session:`: the metadata's row is the one, reading the head the read
  stands at. A rule premised on it needs no `dialog.layer/branch` join
  to tell a stack from a plain branch, and fires once per move. Since
  the metadata row is read on a plain branch too, such a rule fires
  when installed on one; install it through the stack.
- Tonk's overlay operations map onto the stack: `export_overlays` /
  `import_overlays` is `stack.backup()` / `branch.stack().restore(..)`;
  `retain_overlay_entities` and `clear_overlay` stay
  `Branch::overlay().retain_entities` and `Branch::overlay().clear()`,
  which now meet only the embedder's rows and leave `session:` intact.

Why: the embedder's session store and dialog's bookkeeping were one
cell, so neither could be exported, cleared or pruned without the
other, and the primary's revision cell held two rows. Tonk needed to
carry a session across workers, which `backup` and `restore` provide.

#### Compatibility and migration

- Nothing stored or synced changes, and nothing is rewritten:
  `session:` and a `Backup` are process-local, held and carried by the
  embedder. An embedder that filtered `dialog.*` rows out of
  `Branch::overlay()` can stop; one that read its overlay rows through a
  stack with `in: session:` reads them with `in: memory:`, or writes
  them `.at(session_layer())` through the stack instead.

### A stack transacts through a closure re-run on a moved head, an invocation reports what it landed, and providers run as a separate effect

[#816](https://github.com/dialog-db/dialog-db/pull/816), argued in
[notes/layers.md](./notes/layers.md) ("Decisions of 2026-11-15",
points 6 and 7).

`Stack::transact` took the facts to write, so a write computed from a
read was published against whatever head the dispatch re-ran on. It now
takes a closure, run once per attempt against an `Attempt` that reads
the stack at the head the attempt begins at and stages writes and
commands; a lost compare-and-swap runs the closure again from the new
head and drops what the lost attempt staged. The public verb is
*invoke*: nothing in the API is "dispatched" any more (a dispatch is
the unit of publication an invocation runs). `Invocation::perform`
returns an `InvocationReport` instead of the bare revision: the head it
left the primary at, the ticks it ran and the commands it landed on
`control:`'s outside end. Its `effect()` is the next step, performed
when the embedder chooses (a worker's `waitUntil`): it runs the
providers registered with `Stack::provide` over those commands, writes
their answers in one more dispatch with each command's lifecycle rows,
and says whether that dispatch landed commands of its own. Every change
in a command's life is a row of the command's entity in `control:` for
one tick, which a rule copies into a layer that keeps it, and a
registered provider is recorded in `session:`.

What this changes for you:

- `Stack::transact(|tx| async move { .. })` replaces
  `Stack::transact(facts)`: the closure returns `Result<(),
  CommitError>`, is `ConditionalSend + ConditionalSync + 'static`, and
  its future `ConditionalSend`. `Attempt` (`query`, `assert`,
  `retract`, `invoke`, `number`, `stack`) is new; `.at(layer)` on a
  statement places it as before. `Stack::assert(facts)` is the
  one-statement shorthand, for writes with no read.
- `Invocation::dispatch` is renamed `Invocation::invoke`.
- `Invocation::perform` returns `InvocationReport` (`revision()`,
  `ticks()`, `commands()`, `effect()`) instead of `Revision`: take
  `report.revision()` where the revision was used. `commands()` leaves
  out lifecycle rows.
- `CommitError::Read` (a read in a transact body failed, converted from
  `EvaluationError`) and `CommitError::Aborted(reason)` (the body gave
  up) are new; either fails the invocation with nothing written.
- `Stack::provide(Provider)`, `Provider::new(&concept, handler)`,
  `Command` (`entity`, `rows`, `progress`), `Failure`, `Effect`
  (`perform`, `commands`, `is_empty`) and `EffectReport` (`invocation`,
  `answered`, `failed`, `unhandled`, `remaining`, `effect`) are new. A
  provider claims a command whose rows carry an attribute of its
  concept; the first registered claiming one runs. Its answer is
  written as a transact body's writes are, and a statement it places
  `.at(control_layer())` is a command the next effect runs. A provider
  that fails answers with the failure and does not stop the others; a
  command no provider claims is answered `unhandled`, not an error.
  `Invocation::perform` runs no provider.
- Lifecycle rows, each of the command's entity in `control:` for one
  tick: `dialog.command/invoked` (the tick of its dispatch it is
  stimulus of, from 1, written beside the command's own rows),
  `dialog.command/answered` (the provider's concept),
  `dialog.command/failed` (the reason), `dialog.command/unhandled`
  (`true`), all three by the effect's answer dispatch, and
  `dialog.command/progress` (`Command::progress`). Their attributes
  are exported (`invoked_attr` and the rest).
- `session: dialog.provider/command <concept>`, cardinality-many, is
  written when a concept is first provided; providing it again
  replaces the handler and writes nothing (`provided_attr`).
- The corpus write gate admits `dialog.command/*` and
  `dialog.provider/*` from application writes, as it admits
  `dialog.layer/*`.

Why: a write that depends on a read must be computed again when the head
it read moved, which only a closure can be. An embedder answering
commands in a request cannot block the response on every provider, so
running them is a step it performs later, and it needs to know whether
anything is left; a command's life written as rows lets rules keep what
an embedder wants to show, and the discovery rows say what this
process can perform.

#### Compatibility and migration

- Nothing stored or synced changes: the lifecycle rows live one tick on
  `control:`, which is process-local, and the discovery rows are in
  `session:`, which is too. A tree written by this release holds them
  only if an application asserts them durably, which an older release
  refuses with `ReservedRelation`.

### A channel is a layer of its own type, with two ends, whose rows live one tick

[#815](https://github.com/dialog-db/dialog-db/pull/815), argued in
[notes/layers.md](./notes/layers.md) ("Decisions of 2026-11-15",
point 5).

A channel was an `Ephemeral` layer like a session layer: a table whose
rows stayed until its consumer retracted them, with an instant chain a
reader followed. It is now a `Channel`, simpler than `Ephemeral`: it
keeps no table, and a row of it lives one tick. It has two ends. What
the outside end sends (`Channel::send`) is read by the next dispatch
through the stack at its first tick, as rows of the channel, and by no
tick after; a send while a dispatch runs is the next dispatch's. What a
dispatch writes into the channel, `.at(channel)` or through a rule head
naming it, is the next tick's stimulus inside the dispatch, and once its
publish has won lands on the outside end as one tick of a log, each
tick a sequence and its rows. A lost publish lands nothing. `control:`
is the built-in channel, the loopback: `Stack::invoke` sends its
command into it and runs a dispatch, and what lands on its outside end
is what the stack's providers read. The log keeps every tick some
reader has not passed and drops a tick every reader has; while no
reader exists, it keeps the latest tick alone. A channel is any entity
but one under `memory:` or `session:`, and `Stack::channel` creates one
at run time.

What this changes for you:

- `Channel` is new (`dialog_repository::Channel`): `send`,
  `send_changes`, `entity`, `sequence`, `revision` (a hash over the
  sequence of the last tick that landed), `is_waiting`.
- `Stack::channel(&entity)` is new: it returns the stack's channel of
  that name, the loopback for `control_layer()`, creating one if the
  stack holds none and asserting `<entity> dialog.layer/type channel:`
  into `session:` as one instant. It returns `Result<Channel,
  CommitError>`: a name under `memory:` or `session:` is refused with
  `CommitError::InvalidPlacement` and nothing is written. `layers()`
  lists a channel it created after the durable layers.
- `Stack::layer(&entity)` returns `Option<Layer<'_>>` instead of
  `Option<&Ephemeral>`: `Layer::Ephemeral(&Ephemeral)` for `session:`
  and the declared session layers, `Layer::Durable(Durable)` (its
  `name()` and the `revision()` it stands at) for `memory:` and the
  declared durable layers, and `Layer::Channel(Channel)` for
  `control:` and every other channel. `Layer::ephemeral()`,
  `durable()` and `channel()` pick one out.
- A channel is no longer required to be named `control:<name>`: any
  entity outside `memory:` and `session:` names one. A declared
  channel named under either is refused at open with
  `CommitError::InvalidPlacement`.
- The rows-until-retracted model is gone: a channel's rows are no rows
  of any layer after their tick, a query through the stack reads none
  of them, and a consumer retracts nothing. A reader built late reads
  every tick the log holds, once.
- `Arrivals` keeps its API (`poll`, `next`, `rows`, `next_rows`,
  `layer`); it reads the channel's log from its position, and
  `Stack::arrivals` returns `None` for a layer that is no channel.
- `Stack::invoke` and `Invocation::invoke` (`Invocation::dispatch`
  before #816) send into `control:`,
  and every dispatch takes what each channel's outside end was sent;
  what a caller observes of `invoke` is unchanged. A dispatch that
  fails puts back what the channels were sent, not its own commands.
- A channel's `dialog.layer/revision` row in `session:` names the
  channel's revision, the base58 of the hash over its last tick's
  sequence, and moves when a tick lands in it.

Why: a channel is state of one tick with a transport, so the place it
lives is a layer with two ends, and the simplest one: nothing to
rewind, nothing to retract. A log read from each reader's own position
gives a reader that was not reading every tick, which the retained
rows gave before, without keeping rows in a layer a query reads.

#### Compatibility and migration

- Nothing stored or synced changes: a channel's declaration is still
  `dialog.layer/type channel:` on its entity, and its rows were never
  stored. A schema declaring `control:<name>` channels keeps working.
  A schema declaring a channel under `memory:` or `session:`, which no
  earlier build could have used as a channel either, now fails the
  open with `CommitError::InvalidPlacement`.

### A durable layer is the tree the primary points at, with no head of its own, and a pull merges a layer two replicas moved

[#814](https://github.com/dialog-db/dialog-db/pull/814), argued in
[notes/layers.md](./notes/layers.md) ("Decisions of 2026-11-15",
point 4).

A durable layer `memory:<name>` was a branch with a head of its own
beside the primary's pointer to it, so the two could disagree: a
partial publish moved one and not the other, and a pull could race
them. Now the primary's pointer is the only head. The layer is the
tree named by two facts of the layer's own entity in the primary,
`memory:<name> dialog.branch/revision <revision entity>` and
`memory:<name> dialog.branch/tree "<tree hash, base58>"`, written
together under `last`. A dispatch that writes into the layer mints
the layer's next tree on the one the primary pointed at, uploads its
blocks to the local store, and names it in the same commit of the
primary: one compare-and-swap, the primary's. A lost one leaves the
minted tree an orphan nothing names, and the dispatch re-runs from the
new head, reading the layer where that head points it. The pointer is
keyed by the layer's entity because a branch entity is derived from
the replica, so a pointer keyed by one is never the same cell on two
replicas.

`Stack::sync` pulls and pushes the primary alone. Its pull downloads
the trees the incoming primary points the layers at, to the
`Download` given, before the primary's head moves, as it downloads
the primary's own tree, so a replica that goes offline after a pull
reads every layer. When the pull merged two primaries that each moved
a layer, the pointer cells hold a claim of each; rather than `last`
picking one, the sync replays this replica's changes to the layer
since the two met onto the other's tree, as a branch pull rebases
local commits: our writes win on the cells they touch (a write that
succeeded a cell's claims replaces theirs there), theirs stand
elsewhere. The merged tree is minted locally and the primary pointed
at it alone. The push uploads the pointed trees' blocks before the
primary's head.

What this changes for you:

- `LayerType::Branch` carries no entity: declare a durable layer with
  `LayerDeclaration::new("memory:notes".parse()?, LayerType::Branch)`,
  which writes `dialog.layer/type branch:` alone. `LayerType` is
  `Copy`. `LayerType::branch()` is gone.
- `Stack::durable(name)` and `Stack::durables()` are gone: a durable
  layer has no branch to hand out, and no upstreams of its own; the
  primary's `dialog.branch/pull` and `dialog.branch/push` carry it.
  `Stack::layers()` still lists it, and `Stack::layer` still answers
  `None` for it.
- `Stack::branch_of(name)` answers for `memory:` alone; `None` for a
  durable layer.
- The `dialog.layer/branch` refusal of a durable layer naming another
  branch's entity, and the acceptance of one naming another replica's,
  are gone with the fact. A durable layer named outside `memory:`
  still fails the open with `CommitError::InvalidPlacement`.
- A write into a durable layer no longer commits on a branch `<name>`:
  no such branch is opened or written. A rule premised on the layer's
  `dialog.branch/revision` reads the fact of the layer's entity.
- A `Stack::query` reads a durable layer at the tree the primary
  pointed it at when the stack last read the primary (at open, after
  each dispatch and sync through it, and before a dispatch that finds
  the primary moved). A transaction from `Stack::transaction` whose
  primary moved since the stack last read the pointers is refused with
  `PublishError::VersionMismatch`, after reading them again, so its
  retry builds on fresh ones.
- `SyncError::Upstreams` and `SyncError::Pull` and `SyncError::Push`
  now speak of the primary alone; `SyncError::Commit` also covers
  minting a merged layer.
- The corpus write gate admits `dialog.branch/tree` from an application
  write, beside `dialog.branch/revision`.

Why: two heads for one layer can disagree, and nothing can say which
is right; one head cannot. A pull of the primary already downloads
what it adopts before moving, so the trees it points at are part of
that download, and a merge of the primary that leaves two pointers has
to rebase the layer the way a branch pull rebases commits, not drop
one replica's writes.

#### Compatibility and migration

- A primary written by an earlier pre-release build of this line
  holds `dialog.branch/revision` facts of its layer branches' entities,
  and those branches have heads. This build reads the pointer facts of
  the layer's own entity and ignores the branch heads: such a primary
  opens its durable layers empty until a dispatch writes into them,
  and the rows on the old branches stay there, unread. Nothing is
  rewritten. A build from before this change refuses a durable layer
  declared by this one, which names no `dialog.layer/branch`, with
  `CommitError::InvalidPlacement` when it opens a stack.
- Nothing already released is affected: no release carries durable
  layers.

### A `Session` is a `Stack`, every stack lists and records its three implied layers from open, and a layer is declared by its type

[#813](https://github.com/dialog-db/dialog-db/pull/813), argued in
[notes/layers.md](./notes/layers.md) ("Decisions of 2026-11-15",
points 1 to 3).

The layers an embedder opens on a branch are a `Stack`, opened with
`branch.stack()`: `schema::Session` (`db:session`) already names
something else. Every stack has three layers whatever its schema
says, `memory:` (the primary), `session:` (its own) and `control:`
(the loopback), and now lists all three; their revision rows exist
from the moment it opens, written with the facts that describe the
three layers. A layer is declared by its type, a bare-scheme entity,
`dialog.layer/type session:`, `channel:` or `branch:`, in place of
`dialog.layer/store "memory"`, `"control"` or `"durable"`. A branch's
revision is the meta branch's `dialog.branch/revision`, naming the
revision entity: the primary's row in `session:` is that attribute of
the primary's branch entity (the branch metadata's row, since the entry
above that separates `session:` from `Branch::overlay`), the one the meta branch derives from
`(replica, name)`, where it was `dialog.layer/revision` of `memory:`
with the tree's hash as text. A durable layer's pointer facts are the
same attribute of the layer's own entity (see the entry above).

What this changes for you:

- Renamed: `Session` to `Stack`, `OpenSession` to `OpenStack`,
  `OpenSessionEnv` to `OpenStackEnv`, `SessionEnv` to `StackEnv`,
  `SyncSession` to `SyncStack`, `Branch::session()` to
  `Branch::stack()`, and the module `session` to `stack`
  (`dialog_repository::stack::invoke`, `stack::arrivals`,
  `stack::sync`). `Store` is `LayerType`: `Store::Memory` is
  `LayerType::Session`, `Store::Control` is `LayerType::Channel`, and
  `Store::Durable` is `LayerType::Branch`. `LayerDeclaration`'s
  `store` field is `kind`.
  `CommitError::UnboundLayer` says "stack" where it said "session".
- `LayerDeclaration::new(layer, kind)` writes `dialog.layer/type`, under
  `last`. `layer_type_attr()` and `layer_branch_attr()` are new, the
  latter naming the primary's branch entity of `memory:`;
  `LayerType::entity` is the type's bare-scheme entity. `dialog.layer/store` is no longer
  read: a schema declaring its layers that way opens a stack with the
  three implied layers alone.
- `Stack::branch_of(layer)` is new: the branch entity of `memory:`.
- `Stack::layers()` lists `memory:`, `session:` and `control:` first,
  then the session layers and channels the schema declares, then its
  durable layers. It used to list `session:` first and leave the other
  two out.
- Opening a stack writes, as one instant of `session:`, the revision
  row of the primary, `session:`, `control:` and each declared memory
  layer, and the implied layers' facts: `control: dialog.layer/type
  channel:`, `session: dialog.layer/type session:`, `memory:
  dialog.layer/type branch:` and `memory: dialog.layer/branch <the
  primary's branch entity>`. Each stack writes them into a `session:`
  of its own, which a second stack on the branch does not share. A
  rule premised on a layer's revision therefore no longer fires on a
  stack's first dispatch; it fires when the layer moved since the row
  was written.
- The primary's revision row is `<the primary's branch entity>
  dialog.branch/revision <the revision's entity>` in `session:`, the
  branch metadata's row of that cell, in place of `memory: dialog.layer/revision "<tree, base58>"`, and the
  stimulus a dispatch takes of it changes the same way. A memory
  layer's row (`session:`, `control:`, a declared session layer or
  channel) stays `<layer> dialog.layer/revision "<chain hash,
  base58>"`: an in-memory layer's revision has no entity form yet.
- A durable layer's pointer fact in the primary is
  `dialog.branch/revision` naming the revision's entity, in place of
  `memory:<name> dialog.layer/revision "<tree, base58>"` (its form is
  in the entry above). `session:` holds no row for a durable layer any
  more: the primary's pointer fact is the record. A rule premised on a
  durable layer's revision reads the pointer fact.
- `control:` is recorded like any memory layer, after the dispatch's
  commands land in it: a dispatch whose commands land there rewrites
  `control:`'s row, as one instant of `session:`.
- `OpenStackEnv` also requires `Provider<Identify>`: the stack derives
  the primary's branch entity from the operator's profile when it
  opens. Every env that attests already provides it.
- The corpus write gate admits `dialog.branch/revision` from an
  application write, as it admits `dialog.layer/*`: the pointer fact is
  written by the stack's dispatch.

Why: the thing an embedder opens is a stack of layers, and naming it
a session collided with the session the schema already has. A layer's
revision row is where it stands, so it exists as soon as the stack
does. A revision is the meta branch's `dialog.branch/revision`, the
fact a pull and a push already read, rather than a second vocabulary
of layer facts and base58 text.

#### Compatibility and migration

- The `dialog.layer/store` declarations and the `dialog.layer/revision`
  pointer facts that earlier pre-release builds of this line stored
  are no longer read: a primary holding them opens a stack with the
  three implied layers alone, and its durable layers' pointer facts
  are written afresh under `dialog.branch/revision` by the next
  dispatch that moves them. Re-declare the layers with
  `LayerDeclaration` to keep them. Nothing is rewritten. A build from
  before this change reads the new declarations as facts it does not
  understand and opens no layer from them.
- Nothing already released is affected: no release carries layers, and
  0.4.0 and earlier read every fact this change writes as an ordinary
  one. The rows a stack writes into `session:` are process-local and
  never stored or synced.

### A stack writes a layer's revision row only when the layer moved, and `Stack::sync` writes the rows its pulls moved

[#810](https://github.com/dialog-db/dialog-db/pull/810).

A stack's own layer, `session:`, holds a revision row for each of its
layers. Every dispatch used to rewrite all of them
after it published, so `session:` minted an instant on every dispatch
even when no layer had moved, and its own row moved every time, since
the recording itself moved it. Now a dispatch writes only the rows of
the layers whose revision is not the one `session:` holds, and writes
nothing when none moved. Writing the rows is no longer a move of
`session:`: its own row names the revision it stood at before the
recordings that followed its last change, and moves only when
something was written into it. `Stack::sync` did not write the rows
at all; it now writes those its pulls moved, and after its pushes
those the pushes moved, through a dispatch that takes them as
stimulus.

What this changes for you:

- A dispatch that writes nothing into `session:` and moves no other
  layer (an empty transaction) writes no revision row and mints no
  instant in `session:`. One that moves the primary alone rewrites the
  primary's row alone. A command landing in `control:` moves that
  layer, so its row is rewritten (see the entry above).
- A rule premised on the `dialog.layer/revision` of `session:` fires on
  a stack's first dispatch and when something wrote into `session:`
  outside a dispatch, no longer on every dispatch after the first.
- A rule premised on the primary's `dialog.branch/revision` in
  `session:`, or on a durable layer's pointer fact, fires when a
  `Stack::sync` pull moved it, as it does when a commit did. A sync that moves nothing dispatches nothing.

Why: a revision row says where a layer stands, so it changes when the
layer moves, by a commit, a pull or a push, and not on every dispatch.
A reader of `session:`'s instants, and a rule premised on a revision,
then sees one change per move.

#### Compatibility and migration

- Nothing stored or synced changes, and nothing is rewritten: the rows
  live in `session:`, which is process-local. `session:` mints fewer
  instants.

### A stack's transaction lands its commands when it publishes, a stack opens through a read-only environment, and a whole layer is a legal scan

[#809](https://github.com/dialog-db/dialog-db/pull/809).

Three gaps an embedder met moving onto stacks. A transaction a
stack hands out (`Stack::transaction`), committed and published
through the transaction API, never landed what it dispatched in
`control:`: only `Invocation::perform` did. Now its publish lands them,
as an invocation's does. Opening a stack asked for an environment
that can commit, though opening only reads the primary and opens
branches; it now asks for what a read needs. And a scan whose every
term is a variable but a constant `in` was refused as an empty
selector; a constant layer now constrains a scan, so it reads every row
of that layer, and `Arrivals` can yield the arrived facts themselves,
without a query.

What this changes for you:

- A transaction from `Stack::transaction`, published with
  `.commit().publish().perform(env)` or staged and then
  `TransactionBatch::publish`, lands the commands it dispatched (with
  `Transaction::dispatch`, or written `.at(control_layer())`) and the
  ones its rules dispatched in the stack's `control:` once the publish
  wins, as one instant gone in the next, and wakes every `Arrivals`
  waiting. A publish that loses its compare-and-swap lands nothing. It
  is still not re-run on a moved head; `Stack::invoke` and
  `Stack::transact` are.
- `OpenStackEnv` is new: the providers opening a stack and reading
  through it need, with nothing that commits. `OpenStack::perform`,
  `Arrivals::poll` and `Arrivals::next` are bounded on it rather than on
  `StackEnv`, which now extends it with what a commit needs (`Attest`,
  `Publish`, `Import`, blob import and size). Every environment that
  met the old bounds meets the new ones.
- A scan with a constant `in` and every other term a variable, an
  `AttributeQuery` with `.with_layer(Term::from(layer))`, reads every
  row of that layer: of a memory layer, `control:` or a channel, its
  rows alone; of the primary or a durable layer, the whole tree, which
  is legal and costs what reading the whole tree costs. Before, it
  failed with `EvaluationError::EmptySelector`. A scan with nothing
  constant at all, the layer included, still does.
- `Selector::only_in(layer)` is new: a selector the layer alone
  constrains. A tree reads it as its whole entity index.
- `Arrivals::rows` and `Arrivals::next_rows` are new: every fact the
  layer's assertions since the last read assert, each once, oldest
  first, with no query; `Stack::arrivals_rows(layer)` builds a reader
  for them. Rows and `poll` read the same arrivals, so a row one yields
  the other does not. `Arrivals` takes its query type as a default
  parameter, `Arrivals<Q = ()>`.

Why: an embedder resolves its commands with its own transactions, so
the commands a stack's transaction dispatched must reach the
stack's providers whichever way it was published; it opens branches
with an environment that only reads, and the stack beside them needs
no more; and its providers tell a command apart by the attributes its
entity carries, so they need every row of the arrived instant, not one
concept's.

#### Compatibility and migration

- Nothing stored or synced changes, and nothing is rewritten:
  `control:` is process-local, and a scan of a whole layer reads what
  is already stored.

### A durable layer is a tree the primary points at, and `Stack::sync` carries it with the primary

[#808](https://github.com/dialog-db/dialog-db/pull/808), argued in
[notes/layers.md](./notes/layers.md) (points 1, 11 and 13, and
"Durable layers" under "Open"); the model is the one
[#814](https://github.com/dialog-db/dialog-db/pull/814) settled (see
the first entry above).

A stack's layers were all in memory. A layer can now be durable,
declared in the primary as `memory:<name> dialog.layer/type branch:`
(`LayerType::Branch`): a tree the primary points at. A stack reads it
as a line whose rows bind `in` to `memory:<name>`. A write `in:
memory:<name>`, a rule head whose descriptor names it, and a write
while it is selected are minted as the layer's next tree as part of
the dispatch, and nothing of them lives in memory. The primary records
the tree each durable layer stands at as pointer facts of the layer's
entity, `dialog.branch/revision <its revision's entity>` and
`dialog.branch/tree "<its tree, base58>"`, under `last`, written by
the dispatch that moved the layer, so the primary's history says which
tree of each layer goes with each of its own, and the primary's
compare-and-swap is the layer's. `Stack::sync` pulls the primary,
downloading the trees it points at, and pushes it, uploading them.

What this changes for you:

- `LayerType::Branch` is new; `LayerDeclaration::new("memory:notes".parse()?,
  LayerType::Branch)` declares one. A durable layer named under another
  store (`session:notes`) or with no name (`memory:`) fails the
  stack's open with `CommitError::InvalidPlacement`.
- `Stack::layers` lists the durable layers after the memory layers.
  `Stack::layer` answers `None` for a durable layer, which holds no
  memory cell.
- A dispatch that writes a durable layer publishes on the primary,
  with the pointer facts, even when it wrote nothing else there. A
  rule premised on a durable layer's pointer fact fires when the
  dispatch that moved the layer writes it.
- `CommitError::UnboundLayer` refuses a write into `memory:<name>` the
  primary does not declare, before anything is minted.
- `Stack::sync(download)` and `SyncStack`, `SyncEnv` and
  `SyncError` are new. A sync names its `Download`, as every pull does.
- A query through a stack, `Stack::query`, a transaction through
  it, and `Stack::subscribe` read the durable layers too. A
  subscription re-evaluates when a durable layer moved.

Why: a device-local or replicated table beside the primary is a layer,
not a separate mechanism, and its history has to be tied to the
primary's, the way a submodule pointer ties a submodule to its
superproject, so one revision of the primary names the whole
composite.

#### Compatibility and migration

- `dialog.layer/type branch:` and the pointer facts
  `dialog.branch/revision` and `dialog.branch/tree` of the layer's
  entity are new stored facts in the primary, replicated with it; the
  layer's rows are in the tree they name, synced with the primary.
  0.4.0 and earlier know no layer declarations and read them as
  ordinary facts, and read the primary without the layer's rows.
  Nothing already stored is rewritten, and nothing has to be run.
- What a durable layer syncs to is the primary's upstreams. The
  declaration carries no upstream.

### `control:` is the layer a command lives in for one tick, a channel is a layer whose rows live one tick, and a provider reads what arrives in either

[#807](https://github.com/dialog-db/dialog-db/pull/807), argued in
[notes/layers.md](./notes/layers.md) (points 7 to 18, and "Open").

A command used to be a fact apart: dispatched into a transaction, read
by the rules of one tick, then kept by the stack in a command feed
that handlers and readers drained. It is now a row of a layer.
`control:`, the loopback every stack has, holds what a dispatch
writes into it for one tick: a command dispatched, a write
`.at(control_layer())`, and a rule head landing there. Each is the
next tick's stimulus, read as a row of `control:`, and is gone after
that tick. Once a dispatch has published, its commands stand in the
stack's `control:` layer for one instant and leave it in the next, so
the layer's instants are every command, in order, once, and nothing of
them stays. A channel is a layer for a consumer outside the invoke
loop or the process; since #815 (see its entry above),
its rows live one tick too, and what a dispatch writes into it lands
on its outside end as one tick of a log every reader reads in full. A
provider reads either through `Stack::arrivals`, which yields each
row of its query that arrived, once, and answers by invoking.

What this changes for you:

- A rule premise whose descriptor names `in: control:` reads the
  tick's commands alone; one binding `in` to a variable binds a
  command's to `control:`. A transaction's query reads its dispatched
  transients as rows of `control:` too. A premise naming no layer reads
  them as before.
- A rule head cell whose descriptor names `in: control:`, and a write
  `.at(control_layer())`, are dispatches: the next tick's stimulus, in
  no layer and never committed, whether or not the concept carries the
  `dialog.concept/transient` marker. A command head (one whose concept
  carries the marker) lands its cells that name no layer in `control:`
  as before, and now lands a cell that names another layer, a channel
  say, in that layer, where before the layer it named was ignored.
- `LayerType::Channel` is new: `LayerDeclaration::new(entity,
  LayerType::Channel)` declares a channel, stored as `dialog.layer/type
  channel:`. A stack builds it as a `Channel` (see #815 below):
  a write into it is the next tick's stimulus, read as a row of the
  channel, and lands on its outside end once the dispatch has
  published. Declaring bare `control:` builds nothing apart.
- `Stack::arrivals(layer, query)` and `Arrivals` are new: a reader
  of what arrives in `control:` or a channel. `poll` yields the rows of
  the query the ticks landed since the last read make, evaluated over
  those rows alone; `next` waits for the stack's next dispatch while
  there are none. `Stack::layer(&control_layer())` hands out the
  loopback, which holds nothing between dispatches.
- `Stack::commands()`, `Commands`, `Dispatched`, `Lost` and
  `FEED_CAPACITY` are gone: read `control:` with
  `Stack::arrivals(&control_layer(), query)` instead, or a channel with
  `Stack::arrivals(&channel, query)`. A reader loses nothing: the log
  keeps every tick some reader has not passed.
- `Handler`, `HandlerError`, `Answering`, `Stack::handle` and
  `MAX_ANSWERS` are gone: a provider reads its commands with
  `Stack::arrivals` and answers with `Stack::invoke`, a dispatch
  with its own compare-and-swap. `Invocation::perform` publishes once
  and no longer chains answers. Since #816 a provider is registered
  with `Stack::provide` and run by a report's effect, a step apart
  from the invocation; `Stack::arrivals` stays for a consumer that
  reads the log itself.
- `CommitError::AnswerDivergence` and `CommitError::Handler` are gone:
  nothing runs an answer inside an invocation, so neither can happen.
  A provider's own failure is the provider's to report.

Why: a command is state of one tick, so the place it lives is a layer,
and observing it is reading that layer, not a mechanism apart. A
channel's log keeps every tick for a reader that was not polling, where
a view polled after a one-tick row came and went sees nothing. Commands reach `control:`
only after their dispatch has published, so a dispatch re-run from a
moved head re-runs no effect, and an answer needs no isolation of its
own.

#### Compatibility and migration

- A channel's declaration, `dialog.layer/type channel:` on the
  channel's entity, is a new stored value, replicated with the
  primary like every layer declaration. 0.4.0 and earlier know no
  layer declarations and read it as an ordinary fact. A build of this
  release line from before this change opens no stack on a primary
  that declares a channel: it refuses the declaration with
  `CommitError::InvalidPlacement`. Nothing already stored is
  rewritten, and nothing has to be run.
- Commands were never stored and still are not: `control:` and every
  channel are process-local, as memory layers are.

### A memory layer mints one instant per tick that changed it

[#805](https://github.com/dialog-db/dialog-db/pull/805), argued in
[notes/layers.md](./notes/layers.md) (point 12).

A dispatch through a stack runs in ticks: the transaction's own
writes are the first, and each induction round after it is one more.
A memory layer used to take a dispatch's writes as one instant holding
the merged result, so a reader of its instants could not see the states
the rules saw in between. It now takes each tick's writes as an
instant of its own, in tick order. The instants are still minted only
once the primary's commit publishes: a dispatch is one unit of
publication, and a refused publish shows in no layer.

What this changes for you:

- A dispatch whose rules write a memory layer in several rounds mints
  that many instants there, and `Ephemeral::since` yields each
  intermediate state in order. A tick that writes no layer mints
  nothing in it.
- `TransactionBatch::shares` returns the shares tick by tick,
  `&[Vec<(Entity, Changes)>]`, one entry per tick that wrote a layer,
  in place of one merged entry per layer.
- An induction round now reads what the rounds before it wrote into a
  layer, as that layer will hold it once the ticks land: a rule
  premised on a memory layer's row fires on an earlier round's head
  there, and a head under `last` succeeds the layer's own row of the
  cell. Before, a round read only the layer as the dispatch began, so
  such a rule did not fire within the dispatch.
- The transaction's own writes into a layer are read the same way and
  are stimulus of the first round: a rule premised on the layer's row
  fires on them in the same dispatch. Before, they were routed away
  before induction and no rule saw them until the next dispatch.
- A write under `last` into `session:`, the transaction's own or a
  rule's head, succeeds the row the layer already holds within the
  dispatch, so a chain of rules over `session:` runs as one over a
  declared layer does. Before, the held row stood above the write and
  the next round read it instead.
- A maintained subscription over a stack reads the several instants
  in order and still equals a fresh evaluation.

Why: a subscriber, or a rule premised on a layer's revision, should see
the ticks the rules ran through, so a maintained view equals a fresh
evaluation after any sequence of ticks.

#### Compatibility and migration

- Nothing stored or synced changes, and nothing is rewritten: memory
  layers are process-local. A stack's memory layers mint more
  instants per dispatch, which a reader pinned to an older sequence
  sees as more instants, not a different state.

### Every stack has its own layer, `session:`, which records the layers' revisions and which layer is selected

[#803](https://github.com/dialog-db/dialog-db/pull/803), argued in
[notes/layers.md](./notes/layers.md) ("The `session:` layer").

A stack now holds its own layer, `session:`, whether or not the
primary's schema declares it: a process-local store of the stack's
own, apart from `Branch::overlay`. After each dispatch publishes, the
stack writes into it, under `last`, the `dialog.layer/revision` hash
of the instant chain of each declared memory layer and of `session:`
itself; the primary's revision is read there too, as the branch
metadata's `dialog.branch/revision` of its branch entity. Of the same rows, as the layers stood when
the dispatch began, those the stack has not recorded are the
dispatch's stimulus, so a rule premised on a layer's revision fires
when the layer moved (and on the first dispatch, which finds nothing
recorded). A row
`session: dialog.session/layer <layer>` selects a layer: while one is
selected, a write with no layer named, and a rule head whose descriptor
names none, lands in it instead of the primary. Machinery under
`dialog.` (rules, layer declarations, the revision rows) never follows
the selection.

What this changes for you:

- `Stack::layers()` lists `session:` first, and `Stack::layer` of
  `session_layer()` returns the stack's own store. A declaration of
  `session:` in the schema no longer builds a second store.
- `Stack::select(Option<Entity>)` and `Stack::selected()` are new.
  A selection takes effect from the next dispatch; selecting a layer the
  stack does not hold makes a commit that would land a write in it
  fail with `CommitError::UnboundLayer`, and nothing is written.
- `revision_attr()` (`dialog.layer/revision`) and `selection_attr()`
  (`dialog.session/layer`) are new.
- Every dispatch through a stack now mints one instant in `session:`
  and runs induction even when it writes nothing else. A commit on a
  branch outside a stack writes no revision rows and ignores the
  selection.

Why: the stack's own layer is addressed like any other layer, so a
layer's revision becomes a row a rule or a subscription
reads like any other, and the layer a write lands in becomes data a
rule or the embedder sets, the way an image editor's stroke lands on
the selected layer.

#### Compatibility and migration

- The `session:` rows are process-local: they live in the stack's own
  store, which is never committed or synced. Nothing stored or synced
  changes, and nothing is rewritten.

### A stack's handlers answer a dispatch's commands

[#797](https://github.com/dialog-db/dialog-db/pull/797), argued in
[notes/layers.md](./notes/layers.md) ("Handlers").

A command is scratch: it lives one tick and lands on no layer, so
nothing could react to it with an effect. Now a stack holds
handlers (`Stack::handle`), built into dialog or registered by the
embedder through the same `Handler` trait. Once a dispatch has
published, every command of it, the one invoked and the ones its rules
dispatched in any tick, is offered to each handler; what they answer is
dispatched as the next tick's stimulus, where a rule captures what it
wants of the answer on the layer its attribute is placed on, and a
subscription through the stack sees the capture. The chain ends when
no handler answers.

What this changes for you:

- `Handler`, `HandlerError`, `Answering` (a handler over a function) and
  `Stack::handle` are new. A handler sees commands as `Changes` and
  answers with `Changes`; an answer names the command it answers by that
  command's entity.
- `Invocation::perform` now runs the handlers after each publish and
  chains their answers, so one `invoke` can publish several times. It
  still returns the head the last dispatch left the primary at.
- `Stack::commands()` is new: a reader of the stack's command feed,
  the control channel. Each `poll` yields every dispatch since the last,
  in order and once, as `Dispatched { sequence, revision, commands }`.
  A consumer outside the stack's process, or one that answers in its
  own time, reads it and answers by invoking through the stack; the
  feed holds `FEED_CAPACITY` dispatches, and a reader it moved past is
  told with `Lost` and starts over with `catch_up`.
- `CommitError::AnswerDivergence` is new: handlers that keep answering
  are stopped after `MAX_ANSWERS` dispatches. `CommitError::Handler` is
  new: a handler's failure fails the invocation. Both come after the
  dispatch they follow has published; what it wrote stands.
- A stack with no handler behaves as before.

Why: an embedder's command handlers (tonk's command providers) and
dialog's own effects, such as fetching a tree node a page asked for,
need one way to observe a command and answer it, and the answer has to
enter the stack as a fact on a pinned layer rather than as something
a query could observe outside any revision. Running handlers after the
publish keeps a dispatch a function of head and stimulus, so a lost
compare-and-swap re-runs no effect.

### A row has a layer, `in`, and a stack reads and writes its layers as one

[#795](https://github.com/dialog-db/dialog-db/pull/795) and
[#800](https://github.com/dialog-db/dialog-db/pull/800), argued in
[notes/layers.md](./notes/layers.md).

Where a fact lies is now a dimension of the row, beside `the`, `of` and
`is`: its layer, `in`. An attribute descriptor may name it (`in:`), a
constant layer or a variable a concept field binds; a statement may
name it for every fact it writes (`Statement::at`). A read with `in`
unbound reads every layer and binds each row's layer as `in` in the
match, `memory:` for the primary's rows; one with `in` bound reads that
layer alone; a write lands in the layer named, the primary with none. A
claim carries no layer: a reader that asks where a row lies names `in`
and reads the binding. `LayerDeclaration` (`dialog.layer/type`) declares a layer
beside the primary, backed for now by memory. `Branch::stack()` opens
a `Stack` over the primary: it mints a memory layer for every declared
layer, reads the composite, and commits a transaction by routing each
write to its layer. `Stack::invoke` drives it: one dispatch is one
publication, re-run from the new head when the head moved under it.
The primary's share is minted and published as before; a layer's share
is applied only after the publish wins its compare-and-swap, so a lost
head leaves no layer changed. A rule head whose descriptor names a
layer lands there, keeping its pick, and succeeds no claim of the
primary. The layers every stack has are named: `memory:` is the
primary, `session:` the stack's own process-local store, `control:`
the loopback a command lands in.

What this changes for you:

- `AttributeDescriptor::in_layer` and `layer`, the descriptor's `in` on
  the wire, `Attribution::layer`, and `Statement::at` are new. `Claim`
  is unchanged: the layer a row was read from is the match's `in`
  binding, bound only when the scan names `in:`. `AttributeQuery::with_layer` reads a layer by a term.
  `Selector::in_layer` reads one layer of a store that reads several,
  and `SelectIn` reads every row with the layer it came from, for a
  scan that binds `in`; a `Select` reads the rows bare, as before, and
  a `FactView` carries no layer.
- `Changes` keeps writes into layers apart (`associate_in`,
  `dissociate_in`, `take_shares`, `iter_in`, `into_layer`), serialized
  under an `in` key beside `facts` and `assets`. `into_instructions`
  yields the writes bound for the primary alone.
- `Branch::stack()`, `OpenStack::declare`, `Stack::{query,
  transaction, layer, layers, declare, declared}` are new. A branch
  read or committed directly, with no stack, behaves as before, and
  a write `.at(session:)` through it lands in the branch's overlay
  (through a stack, in the stack's own `session:`).
- `Stack::invoke(command)` and `Stack::transact(facts)` drive a
  session (since #816, `Stack::transact` takes a closure and
  `Stack::assert(facts)` takes the facts): each returns an
  `Invocation` whose `perform` commits and publishes as one unit, and re-runs the whole dispatch from the new
  head when the publish loses its compare-and-swap, up to `MAX_RERUNS`
  times before reporting `PublishError::VersionMismatch`. A dispatch
  that writes only to memory layers publishes nothing and returns the
  unchanged head.
- `Stack::subscribe(query)` is new: a standing query over the primary
  and every layer the stack holds. A commit to the primary, a share a
  dispatch lands in a memory layer, and a write to the primary's session
  overlay each propagate as a result delta on the next poll, maintained
  incrementally from the layer's instants as an overlay change is. A
  maintained subscription through a stack equals a fresh evaluation
  after any sequence of dispatches (pinned by a seeded churn test).
- `CommitError::UnboundLayer` is new: a write named for a layer the
  stack does not hold is refused before anything is settled or
  minted. `CommitError::InvalidPlacement` is new: a layer declaration
  whose value names no store is refused.
- The corpus write gate admits `dialog.layer/*` from an application
  write, as it admits `dialog.rule/*`.

Why: local state that moves at frame rate, replicated state and
ephemeral state peers learn of are different layers, not different
kinds of fact. A row's layer being a term lets one set of rules act
across layers and choose where a write lands from data, the way a
stroke lands on the layer an editor's tool is on, and lets a stack
recreate every layer from the primary's declarations when it opens.

#### Compatibility and migration

- A replica that declares no layer stores and syncs exactly what it did.
  Layer declarations are ordinary facts under `dialog.layer/`; an older
  release reads them as facts it does not understand and ignores them.
  A batch of `Changes` that writes into a layer serializes with an `in`
  key an older release does not read; one that writes into none
  serializes as before. Nothing is rewritten.

### A rule that leaves a premise's subject unnamed fires for the entity a commit wrote

[#802](https://github.com/dialog-db/dialog-db/pull/802), fixes
[#801](https://github.com/dialog-db/dialog-db/issues/801)

Commit-time induction starts a rule from each row a commit changed. It
bound the row's value to the premise's field, and the row's entity to
the variable the premise gives `this`. A premise that gave `this` no
variable left the entity unbound, so the premise was read at every
entity holding the value written, and the rule fired for entities the
commit did not touch. Such a premise is now seeded with its subject
named, in a variable nothing else in the rule reads, so the seed binds
the entity the row is about.

What this changes for you:

- **A rule whose premise names no `this` fires for fewer entities.**
  Writing one field of one entity no longer fires the rule for every
  entity that holds the same value in that field. A command is no
  longer issued again for them, and a durable head under `last` no
  longer writes its value again over what was written since. It fires
  for what the same rule with its subject named fired for already.
- A commit that writes one field of an entity and a commit that writes
  two now fire such a rule for the same entities. Before, the two
  fields together found the entity written and the one field found
  every holder of its value.
- A rule fired in full is not seeded and is unchanged: a rule installed
  over existing state, one whose premise changed through a deductive
  rule, and one enabled by a retraction under `unless` fire for every
  match, as before.
- Nothing stored changes shape. A rule that relied on being fired for
  every holder of a value has to be given a premise that says so.

Why: a row is a fact of one entity, and the matches it newly makes are
those where the premise is read at that entity. Reading by value alone
found matches the commit did not make and fired for them again.

### A tree write sizes its segments before filling them

When a write regroups a window of entries into segments, each segment
is now moved out in one step at its final size, and a merged stretch
is allocated at its final size, where both grew entry by entry. The
trees written are the same.

What this changes for you:

- A commit that rewrites many entries copies far less: the perf
  scenario that succeeds a standing claim on 500 entities retires 26%
  fewer instructions, and its reallocations start from 50 MB in all
  where they started from 530 MB.

Why: a growing vector is copied each time it outgrows its allocation,
and a write regroups large windows many times over.

### A tree reads a node from its block or from rows a store keeps it in

[#733](https://github.com/dialog-db/dialog-db/pull/733),
[design](./research/single-file-format/design.md)

A node's body is now read through views, `IndexView` and `SegmentView`, that
either a block or a store's rows back. A store that keeps nodes as rows
implements `NodeRows`, and answers the tree's `nodes::Read` with nodes
over them (see the entry on the tree asking its environment for nodes).
Such a node is read in place: keys compare part by part, values
are read as archives where the store keeps them, and no block is built,
hashed or validated. `Chronicle` is such a source.

What this changes for you:

- `NodeBody::Index` and `NodeBody::Segment` hold `IndexView` and
  `SegmentView` instead of `&ArchivedIndex` and `&ArchivedSegment`;
  `PersistentNode::as_index` and `as_segment` return the views. The views
  have the archived types' methods. `index.novelty` is `index.buffers()`,
  a buffer's `child` is `child()`, its `count` is `count()`, and its
  `polarity` is `is_assert(at)`.
- `PersistentNode::buffer` returns `Result<&Buffer, _>`: a node read from
  rows builds its block the first time it is asked, and that can fail.
- A segment's `keys()` returns a `KeyCursor`, which has `next_key` as
  `StreamingLeaf` does.
- Nothing changes for a tree read through a store that keeps blocks.

Why: a store that keeps nodes compactly need not rebuild a block for every
read. Read in place, the single-file archive's cold reads match the `fs`
archive's, where rebuilding made them about 2× slower, and its cached nodes
take about a third of the memory of cached blocks.

### A transaction that loses its branch's race runs again over the head that won

[#733](https://github.com/dialog-db/dialog-db/pull/733),
[design](./design/file-writers.md)

`Branch::transact(|transaction| async { ... })` takes the code that builds
a transaction instead of the transaction. Each try hands it a fresh
transaction on the branch's head as it is then, commits what it returns,
and publishes that compare-and-swapped against the head it read. When
another writer moved the head first, the head that won is read and the
code runs again: its queries read over the winner, so a write computed
from a read is computed again, as in a software transactional memory. It
tries as many times as a commit does (64) before returning
`VersionMismatch`.

What this changes for you:

- New: `Branch::transact(build).perform(&env)`, with `allow_empty()` and
  `canonicalize()`; `build` returns `Result<Transaction, E>` where
  `E: From<CommitError>`, and an error from it ends the transaction with
  nothing written.
- Deprecated: `branch.transaction()...commit().publish().perform(&env)`.
  It keeps failing with `CommitError::Publish(PublishError::VersionMismatch)`
  on a lost race, as it did. Move to `Branch::transact`, which builds the
  same transaction inside the closure.
- `Branch::induce` runs its rules again over the head that won.
- A `TransactionBatch` its caller staged (`.commit().perform(..)`, then
  `.publish()`) is still refused on a lost race: its caller read what it
  staged on. The repository's own commits (delegations, migrations) apply
  their instructions again over the winner, as before.
- `dialog_corpus::Instruction` is `Clone`.

Why: processes writing one branch concurrently should not each catch and
retry every commit; and retrying the instructions a transaction computed
reapplies writes derived from reads of a head that lost, where running the
transaction again computes them over the head that won.

### A space can keep its memory cells in its file, branch heads included

[#733](https://github.com/dialog-db/dialog-db/pull/733),
[design](./design/file-writers.md)

`Chronicle` provides the memory effects (`Resolve`, `Publish`,
`Retract`, `List`), so a `Space` can name it as its memory as well as its
archive: `Space<Chronicle, Chronicle, ...>` keeps a space's blocks and
its cells, branch heads among them, in one file. A cell is kept as a
log: each update appends a record with the cell's value, in the writer's
turn, and the cells' own index files the cell at its newest record. The record names the writer's extents that
still hold blocks, and recovery takes it as whole only with those
extents' frames, so a power loss can undo the last updates (synced at the
next sync point), never leave a head naming what is gone.

What this changes for you:

- A `Space` whose memory is a `Chronicle` keeps no `memory` directory:
  its cells are in `archive.dialog`.
- A cell's edition is the BLAKE3 hash of its value, as under `fs`, so a
  `Publish` names the same version either way, and publishing the value a
  cell already holds succeeds whatever edition the writer expected, as on
  every memory store.
- A cell's update takes the writer's turn for the append of one small
  record: an update waits while another process holds the turn, for its
  own appends' laying out and writing. Splitting and encoding an append, folding extents in and
  syncing are done before or after the turn, not in it.
  A `Resolve` takes no lock, and reads the newest update another process
  made.
- A value of any size is kept in its record.
- `List` walks the cells' own index, not the blocks'.

Why: a space that is one file is one thing to copy, back up and keep
consistent, and a head kept apart from the blocks it names was a second
thing to keep in step with them. Written in place, a head could reach the
disk before the blocks it names, which the power-loss tests showed.

### Writers to one space's file do not wait on one another

[#733](https://github.com/dialog-db/dialog-db/pull/733),
[design](./design/file-writers.md)

A writer to a `Chronicle` tries the file's turn without waiting. With
it, it appends as before. Without it, it writes its blocks whole into an
extent of its own, space it claimed in an earlier turn, and they read back
at once, in its process and in others. The next writer to get the turn,
usually the same one after its commit, turns those blocks into frames and
tokens, and the extent is freed once a sync point makes that durable. A
process that holds the turn keeps an extent ready for itself; a process
that opens the file with the turn free leaves one no process owns, and
takes one back as it closes, so there is one for each process that has the
file open. A process with none takes one of those, or one whose writer
died, so a process that has never held the turn writes without waiting for
it too. A writer waits for the turn only to claim an extent when there is
none to take, or for a catalog the file has not numbered.

What this changes for you:

- Processes writing one space (separate branches, or the same one) no
  longer queue behind each other's appends, as they did not on the `fs`
  store either.
- The writer's turn is a lock on one byte of the file rather than on the
  whole file, on every platform.
- `Options::extent` sets how many bytes of blocks an extent holds (1 MiB
  by default). Each process that writes keeps one extent claimed while it
  has the file open, and each process that has it open leaves one for
  another; the last process to close the file retires them and cuts free
  extents that end the file off it. One left before blocks were appended
  stays free mid-file and is the next one claimed.
- Claiming an extent writes its header and grows the file, without
  writing the extent's bytes: an unused extent is a hole on file systems
  that keep sparse files.
- `Options::leave_extent` (on by default) is whether opening leaves an
  extent for another process; a process that only reads can turn it off
  and leave the file's length alone.
- Opening the file no longer waits for another process's turn: the slot
  it opens from was written in this boot, so there is nothing to repair.
  After a reboot, or for a new file, opening still takes the turn.
- A file reached by two names in one process (a hard link, a symbolic
  link) is one handle, as a path reached two ways already was: two would
  both hold the writer's turn.
- A file whose making a power loss cut short opens, made again: one no
  longer than its header, whose first bytes were never written or are the
  format's with no durable slot. Any other file that is not an archive is
  still refused.
- A writer thread taking the turn just as another thread of its process
  let it go no longer lets another process take it under it: the file's
  lock is let go before the process's own. Two writers could append at
  once before.
- A block another process holds only in its extent is written again into
  the writer's own, so a head it commits never names a block another
  process has not yet folded into the file.
- The file starts with a 16-byte header: `DIALOG`, the byte `0xB6` (not
  valid UTF-8 on its own, so a file read as text fails at once), the
  format version, 1, and zeros, reserved. A file written by an earlier
  build of `dialog-chronicle` does not open.

Why: many processes work on separate branches of one space at once, and on
main they never wait on one another to write. A file whose every append
took one turn made them wait, which would rule the file out wherever such
processes run.

### A space can keep its blocks in one file

[#733](https://github.com/dialog-db/dialog-db/pull/733),
[design](./research/single-file-format/design.md)

The new `dialog-chronicle` crate adds `Chronicle`, an archive
provider that keeps a space's blocks in one append-only file instead of a
file per block. It answers `archive::Get`, `Put` and `Import` like the
`fs` provider, and opens from a space's `Location` (`Resource<Location>`),
so a `Space` can take it as its archive and keep memory, credentials and
blobs where they were. A node of the artifact tree is kept as a frame:
rows of ids into the file's tokens (each distinct entity, attribute, value
and datum once, and each revision's version once, so a history record
shares its fact's tokens), written against the node it supersedes. A frame is only
written once it realizes to the exact block; anything else is kept whole.
A read realizes the block. Native only.

What this changes for you:

- Nothing, unless you choose it: `Space<Chronicle, FileSystem,
  FileSystem, FileSystem, FileSystem>` is a space whose blocks live in
  `archive.dialog` in the space's directory. The tree, queries and sync
  work through it unchanged.
- Any number of processes can open one file. Writers take turns on a
  lock on one byte of it, and a writer that finds the turn taken writes
  into an extent of its own instead (see the entry on writers above);
  readers take no lock. A file this process already has open is shared,
  not opened twice.
- The file is synced after an append once 64 appends have come since
  the last sync or 2 seconds have passed, on `Chronicle::sync`, on the
  last close, and by a sync thread once appends have waited 2 seconds
  for a sync (`Options::sync_in_background`, on by default), so a commit
  no other append follows is durable within about three seconds. The
  thread is one per process, for every file it has open, and wakes once a
  second. Dropping a handle waits for it only when that handle may be the
  last. That is weaker than the `fs` provider, which syncs every
  block it writes; call `Chronicle::sync` where a commit must be durable
  before anything else happens. A power loss loses at most the appends
  since the last sync, and never leaves the file damaged: opening goes back to the newest state
  that is whole. The slot that state is read from is kept twice, and a
  sync writes over the older copy, so a sync torn by power loss leaves
  the previous copy whole.
- Every frame is checked before it is written: one that does not realize
  to the exact block it stands for (a node another peer wrote in another
  layout, say) is kept whole, never written as rows that read back
  different bytes.
- A file shorter than its durable slot names (cut short by something
  other than the archive, such as a copy that did not finish) does not
  open, where it used to be filled back out with zeros and opened as an
  older state.
- A cell's update that names blocks still in its writer's extent names
  their frames by a checksum, so a power loss that keeps the extent's new
  count but loses a frame no longer leaves the cell naming a block the
  file does not hold: an extent claimed again can still hold a whole frame
  from its earlier claim in the same place.
- What a sync point frees is taken up by a writer only once the durable
  slot that frees it is on the disk, and a sync point made in the writer's
  turn waits for one made without it, so two never write the same durable
  copy.
- Every write (`Put`, `Import`, the tree's `nodes::Write`, and a cell's
  `Publish` and `Retract`) runs on tokio's blocking threads when a tokio
  runtime runs the caller: it can wait for the writer's turn another
  process holds and make a sync point, and holds up none of the runtime's
  other tasks while it does. Reads (`Get`, `Resolve`, `List`, the tree's
  node reads) answer at once from the file's mapping.
- An append of more than 32 Ki blocks is committed in parts, so the index
  pages one commit frees fit the slot that lists them.
- An open of a file that has to be recovered holds up no open or close of
  another file in the process. Two opens of one file in a process wait
  for each other, by its path and, once it exists, by the file, so the
  second shares the first's handle even when the first made the file.
- An extent a process left behind when it closed or died is written into
  again by the next writer that needs one, instead of a new one, so a
  file opened and closed many times does not grow a dead extent each
  time.
- Opening reads small slots at the head of the file and nothing else.
  The file's index is in the file, so a reader holds a few tens of KB,
  not an index that grows with the file; its pages are written over once
  nothing reads them. The format is still changing, and a file written
  now may not open in the release.
- `Options::frames = false` keeps every block whole.
- The new `dialog-migration` crate holds migrations an application runs
  before it opens a replica, each behind a feature of its own, and the
  `dialog-migrate` binary that runs them. A migration is a `Step` that finds
  its work by looking at the replica, so it is safe to run on every start:
  `Migrations::plan` says what the steps would do, `Migrations::run` does
  it, and an interrupted run picks up where it stopped. The `chronicle`
  step (`chronicle::IntoChronicle`) moves every space of a replica the
  `fs` provider wrote into the `archive.dialog` a file-archive space reads,
  writing it under another name and renaming it once complete, or, for a
  space that has an `archive.dialog` already, into that file through
  `Chronicle` (so what it holds stays, and a process with it open goes on
  using it), and then writing `archive.dialog.migrated`. A space is
  migrated when that marker is there, not when it has an `archive.dialog`
  (opening a `Chronicle` creates one). With `remove_source` it deletes
  the `fs` archive after, unless a block's bytes did not match its name
  or a file in it was not a block (`chronicle::strays`): then it fails and
  keeps the `fs` archive. It moves blocks only: a space's cells, branch
  heads among them, stay in its `memory` directory, so a migrated space
  names `Chronicle` as its archive and not as its memory. It is a
  conversion an application opts into, not an upgrade every replica
  needs. Every block
  reads back as the bytes it was; nodes in an older layout are kept whole.
  `dialog-migrate [--dry-run] [--step <id>] [--remove-source] <replica>`
  runs the steps; `--source`, `--target` and `--catalog` move one store.

Why: a replica's history costs a file per block per version, which is
most of what a replica weighs. Replaying 4096 Stack Exchange commits, the
`fs` archive takes about 1.2 GiB and the file 13 MiB.

### Compatibility and migration

- Nothing an existing space holds changes or is read differently for the
  single-file archive: it is a new provider a space opts into. A space
  opened with `Chronicle` does not read an `fs` archive's directory of
  blocks, and the reverse; moving a space between the two means copying
  its blocks with `Import`. The blocks are the same bytes in both, so an
  older release reads any block this one stores.
- A `Chronicle` file is format 1, the first released; one written by an
  earlier, unreleased build of `dialog-chronicle` does not open, and is
  moved again from the replica it came from with `dialog-migrate`. Nothing
  else stored or synced changes: blocks, their hashes and what a replica
  syncs are the same.

## 0.4.0 (2026-11-14)

### A prefetch hint a query took but did not finish is resumed by the next query

[#770](https://github.com/dialog-db/dialog-db/pull/770), argued in
[notes/fetch-scheduler.md](./notes/fetch-scheduler.md) ("A hint outlives
the driver that took it").

Every evaluation (a query, a subscription, a query inside a transaction)
drives the env's queue of prefetch hints while it is polled, walking each
hinted range so its blocks are local before a read asks for them. A hint
it took lived only in that evaluation: when its stream ended or was
dropped, an unfinished walk was dropped with it and no later evaluation
saw the hint again. Now an evaluation that ends with walks unfinished
hands their hints back to the queue, and whichever evaluation runs next
resumes them. A resumed walk starts again from the root, but what earlier
attempts fetched is already local, so it gets further each time.

What this changes for you:

- On a replica whose remote is slow relative to its queries, a hinted
  range is fetched in full across the queries that follow, where before
  it stayed partly remote and the range's own read fetched the rest.
- `PreloadQueue::pending()` counts a hint a dropped evaluation handed
  back; it no longer drops to zero the moment a driver takes a hint whose
  walk never ends inside that evaluation.
- A hint handed back goes through the same path as a new one: refused
  with a zero `FetchBudget`, not enqueued twice while the same selector is
  pending, and behind fresher hints under the same 1,024-per-rank bound.

Why: speculation should be driven by whichever consumer is running, not
lost with the one that popped it; on wasm a query regularly ended before
its walk did, so the hint it existed for was wasted.

### A read's warm walk picks children by the read's own routing, and decodes no buffer

Refs [#762](https://github.com/dialog-db/dialog-db/issues/762).

A query on a branch whose repository tracks a remote warms the ranges it
is about to read with a scoped walk of the tree
(`Traversable::traverse_available_within`). At each index node the walk
decoded the write buffer of every link to see whether a buffered op fell
in its scope. It now keeps exactly the children a read of the scope
descends into, found by binary search on the node's separators
(`ArchivedIndex::route` and `children_within`), with no buffer decoded and
no separator rebuilt. A write buffers each op on the link that same rule
picks, so the children kept are the ones that can hold in-scope content,
buffered or stored.

What this changes for you:

- A view read on a member that tracks a remote and holds every block
  costs a third fewer instructions (the `query-member` perf scenario:
  181.8 M to 120.2 M), and no longer grows with how many links hold
  buffered ops.
- A scoped walk (a warm walk, or a download `within` a scope) may keep
  child 0 of a node for a scope below its first separator, one path to the
  leftmost leaf it did not keep before, because a read of that scope
  descends there too.
- `dialog_search_tree::helpers::buffer_decodes()` counts the novelty
  buffers the calling thread has decoded, for tests.

Why: on a member whose blocks are all local, the walk was most of a view
read, and its cost rose with every change that left more ops buffered.

### A typed variable reads the values of its type through a concept, as it does through a scan

[#782](https://github.com/dialog-db/dialog-db/pull/782),
[design/typed-read-of-untyped-field.md](./design/typed-read-of-untyped-field.md).

A variable's type says which values it reads, and an attribute's type is
part of which attribute it is. A scan of an attribute into a variable
typed as text passes over a number stored under it, and so does a read of
a recursive concept. A read of a concept that is not recursive did not: a
value of another type under a field that declares no type failed the
whole query with `Variable ?x of kind String cannot bind UnsignedInt(7)`.
It now takes such a value for a fact of another attribute, so every way
a concept is read agrees.

What this changes for you:

- A query that reads a concept field into a typed variable no longer
  fails when the field holds a value of another type for some entity. It
  yields the rows whose value the variable reads.
- A variable that reads a type or nothing (`Option<T>`) is bound absent
  for an entity whose only values are of another type, as it is for an
  entity with no value. A value of the variable's type that disagrees
  with what the variable is already bound to is still no row.
- Where a pick elects one of an entity's values (`max:`, `top:`, a field
  a rule derives), a typed variable elects among the values of its type.
  A greater value of another type no longer takes the place of the one
  the variable reads.

Why: which of a concept's rows a caller's terms match should not depend
on whether a rule makes the concept recursive, which the caller cannot
see. `Match::bind` already says a value a type does not read is a
mismatch that "callers that filter rows treat as a non-match"; the plain
concept read was the one caller that did not.

### A rule seeded by several rows of one entity is evaluated once when they agree

[#763](https://github.com/dialog-db/dialog-db/pull/763), [#762](https://github.com/dialog-db/dialog-db/issues/762)

Commit-time induction evaluated a seeded rule once per changed row a
premise names. Rows of one entity that each bind one distinct field of
the premise, a single attribute read under `last`, are now bound into
one evaluation first. When it finds any match, it finds every match the
rows find one by one: a seeded read yields a row's value only when the
round view elects it, so every field then reads its row's value either
way. When it finds none (an overlay row, a derived candidate or another
claim stands above one of the rows, or the rest of the body fails), the
rows are seeded one by one, as before. Every other row seeds alone.

What this changes for you:

- What induction writes is unchanged. The matches of one firing keep
  the order seeding the rows one by one finds them in, so when two of
  them write one transient cell under `last`, the same one wins. In
  this crate's tests every seeded firing also seeds its rows one by one,
  writes those matches the same way, and checks that the novelty and
  transients are the same.
- A command whose event names several fields of a premise evaluates the
  rule once. On a tonk-shaped replica, a warm command's median fell from
  6.9–8.7 ms to 4.7–5.9 ms; 1,861 of 1,900 groups found their match
  together while building and pulling, 200 of 220 in warm commands.

Why: an event's fields are one binding; evaluating it once per field
repeated the same joins.

### A rule seeded by several rows of one change writes its head once

[#761](https://github.com/dialog-db/dialog-db/pull/761)

Commit-time induction evaluates a rule once for each changed row a
premise names, with that row bound. A command whose event carries
several of a premise's fields seeds the rule once per field, and every
evaluation finds the same binding. Each wrote the rule's head again: a
`set assignee` event of eight fields wrote its assignee and its activity
record eight times over, and each copy seeded the next round's rules
again. A firing now writes each distinct head once.

What this changes for you:

- What a commit stores is unchanged: the copies were the same claims,
  and a set holds a claim once.
- A commit that fires rules writes fewer ops into the tree. On a
  tonk-shaped replica, a warm command wrote 177–198 ops before and 26–33
  now, and its median time fell from 12.7–14.1 ms to 7.1–9.2 ms.

Why: the copies were most of what a warm command wrote, so they filled
the tree's root buffer on nearly every commit, and each overflow
rewrote the leaves it sheds into.

### A pull says how much it downloads, and can bring along only what the replica holds

[#775](https://github.com/dialog-db/dialog-db/pull/775),
[design/held-across-pull.md](./design/held-across-pull.md).

A pull that adopts an upstream head by root (a fast-forward, a rival
merge settled, a reset brought forward) reads none of the head's tree, so
every leaf the upstream changed is held by reference once the head lands,
whether the replica held it before or not. A read or a write the replica
had answered from its own blocks then waits on the peer that holds the
block, and fails without one: a page rendered from a device's own store
did not render offline after a sync it had no part in, and an edit that
had been answered at once took a round trip after every pull. Whether a
pull did that was decided by a modifier (`pull().download()`) that was
easy to leave out, and was left out.

How much a pull brings local is now an argument of the pull, with no
default: `branch.pull(Download::…)`. `Download` is an enum:

- `Download::MinimalRanges`: nothing. The head is adopted by reference,
  as a bare `pull()` did.
- `Download::QueriedRanges`: what the replica holds of its head, as the
  head it would adopt has it. New. `PersistentTree::follow(held, env)`
  walks the two trees in step and reads a node of the adopted one,
  fetching what the replica lacks, only where the keys it is routed meet
  those of a leaf the replica holds.
- `Download::Operational`: the entity, attribute and value orders and the
  blob index, as `pull().download().operational()` did.
- `Download::Ranges(scope)`: the key ranges given, as
  `pull().download().within(scope)` did.
- `Download::Everything`: as `pull().download()` did.

Whatever it names is fetched before the head moves, by `perform` and by
`prepare` alike, and a download that fails leaves the head where it was.

A pull can also be made again when a commit overtakes it.
`Pull::retry(Limit)` makes a pull that lost its publish merge onto the
head the commit left and publish that, `Limit::Times(n)` more times or
`Limit::Unlimited`.

What this changes for you:

- `Branch::pull` takes a `Download`. Write `pull(Download::MinimalRanges)`
  where you wrote `pull()`, to keep what it did.
- `Pull::download`, `PullDownload` and its `within` and `operational` are
  gone: the extent is the pull's argument. `PreparedPull` is already
  downloaded when `prepare` hands it back, so a caller that ran a
  download between `prepare` and `commit` drops it.
- `Branch::download` takes a `Download` too, and its command struct is
  `DownloadBranch` (it was `Download`, the name the enum now has).
  `download().operational()` is `download(Download::Operational)`,
  `download().within(scope)` is `download(Download::Ranges(scope))`, a
  bare `download()` is `download(Download::Everything)`, and `.of(revision)`
  is unchanged.
- `Branch::replicate` takes a `Download`, which every pull it makes uses.
- `Pull::perform` and `Pull::prepare` are bounded by `PullEnv`, which is
  `ResolveEnv` and `Provider<Fork<RemoteSite, blob::Read>>`: a download
  that brings blobs along reads them from the upstream's site. Every
  environment that could run a pull download satisfies it.
- With `Download::QueriedRanges`, every key a replica reads or writes
  from its own blocks before a pull it reads or writes from them after
  it. A replica that holds every leaf of its head ends holding every leaf
  of the head it adopts. A range it never read stays by reference however
  much of it changed, and a replica with no head, or one that holds none
  of its head's leaves, fetches nothing.
- `Download::QueriedRanges` brings tree nodes, not what they point at: a
  spilled value or a blob that a changed leaf newly references is fetched
  when it is read.
- A replica's first write to a key it only read still fetches the leaves
  of the orders it never read that key through. A write reads the leaves
  it writes to, in the entity and attribute orders and in history.
- The head stays where it was while a download runs, and reads and
  commits go on against it. A commit that lands meanwhile makes the pull
  lose its publish, as before. `Pull::retry` makes it again; without it
  the pull fails with `PublishError::VersionMismatch`, the head where the
  commit put it.
- A `Download::QueriedRanges` that cannot fetch a node fails with
  `DownloadError::Tree`, a new variant.
- New: `PersistentTree::follow` in `dialog-search-tree`; `Download`,
  `DownloadBranch`, `PullEnv`, `Pull::retry`, `Limit` and
  `DownloadError::Tree` in `dialog-repository`.

Why: a partial replica's promise is that what it read it can read again
without the network. Adopting by root keeps the cost of a pull independent
of what the upstream changed, and breaks that promise each time the
upstream changes something the replica read. Bounding the fetch by what
the replica holds keeps both. Which of the two a branch wants is not
something a library can guess, and a modifier that is easy to miss made
the cheap answer the one a caller got by not deciding: an argument makes
every pull say. A pull is retried by the pull because a replica that keeps
writing while it syncs would otherwise have every caller write the same
loop.

### `wbg-pool` gives every test's tab focus

[#774](https://github.com/dialog-db/dialog-db/pull/774).

`wbg-pool` runs each test in a tab of its own, many at once, and Chrome
shows only the latest tab of a window. The rest are hidden: their
`visibilityState` is `hidden`, `hasFocus()` is false, and animation frames
and `ResizeObserver` deliveries are held back. `wbg-pool` now turns on
focus emulation for a tab as it opens it, so each page acts as the focused,
active one.

What this changes for you:

- A wasm test that waits on an animation frame, a `ResizeObserver`, focus
  or visibility passes beside other tests as it does alone. Run eight at
  once in headless Chromium and 15 of 24 tabs passed before; all 24 do now.
- A tab that cannot be given focus is closed and its run fails with
  `failed to give a test's tab focus`, where the tab would have run without
  it.
- A dependent that carries this as a patch against `wbg-pool` (tonk's
  `wbg-pool-focus-tabs.patch`) drops it when it builds `wbg-pool` from this
  release.

Why: a test that depended on rendering failed or hung only in a full run,
with nothing in its output to say its tab was in the background. Chrome's
flags against backgrounding (`--disable-renderer-backgrounding` and its
kin) do not change it; focus emulation does.

### A provisioned wasm test is given its address when it runs

[#774](https://github.com/dialog-db/dialog-db/pull/774).

A test that takes a provisioned address (`#[dialog_common::test]` with a
parameter) runs under `web-integration-tests` as a native test that starts
the service and a wasm test that uses it. The wasm test read the address
from `option_env!`, so it was compiled into the test binary. It now reads
it when it runs: the native test names the variable in `WBG_POOL_PASS_ENV`,
and `wbg-pool` defines the variables named there on
`globalThis.__wbg_pool_env` in the page or worker the suite runs in.

What this changes for you:

- A package's wasm test binary is built once for all of its provisioned
  tests, where every test rebuilt and relinked it. Nothing in a test
  changes.
- The wasm side needs the `wbg-pool` of this release as its runner. Under
  an older `wbg-pool`, or `wasm-bindgen-test-runner`, a provisioned wasm
  test fails with `Missing PROVISIONED_SERVICE_ADDRESS environment
  variable`. A dependent that builds `wbg-pool` from this repository takes
  both together; patches it carries against `wbg-pool`'s `shim.rs`,
  `daemon/harness.rs`, `daemon/server.rs` or the scripts under `daemon/js`
  may need rebasing.
- `dialog_common::helpers::address` reads the passed variable on
  `wasm32-unknown-unknown` and the process environment elsewhere.
  `dialog_common::helpers::PASSED_VARIABLES` is the name of the variable
  that lists what to pass.
- `wbg-pool` passes any variable named in `WBG_POOL_PASS_ENV`, not only
  this one (see its README).

Why: cargo tracks a variable read with `option_env!` as a build input, and
every provisioned test has its own address. Each test of
`dialog-repository` therefore rebuilt and relinked the crate's wasm test
binary, 82 seconds a test on a CI runner, and the cross-target job spent
over an hour of its 79 minutes of tests there. Two tests of one package
could also build over each other's binary.

### A read reads one revision, and a transaction reads and commits against the one it opened at

[#757](https://github.com/dialog-db/dialog-db/issues/757),
[#758](https://github.com/dialog-db/dialog-db/pull/758),
[design/query-isolation.md](./design/query-isolation.md)

A query performs many selects on its environment: a conjunction looks
its second premise up per row of the first, a resolver re-reads a
reference per outer row. A transaction performs many queries, and a
commit's settlement and induction read the line many times over. Each
of those reads took its tree root from the branch's head cell as it
stood at that moment, and that cell is shared by every handle of the
branch, so a commit made through another handle while a read ran moved
the root under it: the read saw some of the line at one revision and
the rest at another, and could yield rows no revision held. Every unit
of isolation now pins its line when it begins (a query, a select, one
poll of a subscription, a transaction from open to commit), and every
scan it performs reads that revision. Inside the crate a read source is
a pinned `View` and the live `&Branch` or `&Snapshot` is reachable only
by what needs the current head (a commit's CAS, a subscription deciding
whether to re-evaluate), so a read that follows the head cannot be
written. A query over a `Snapshot` was already pinned, and is
unchanged.

What this changes for you:

- A `Branch::query()`, `Branch::select()`, `Branch::claims().select()`,
  a subscription's poll, and the induction and succession reads a
  commit makes each see one revision throughout. A commit that lands on
  the branch while one of them runs is seen by the next read, not by
  the rest of this one.
- A `Transaction` reads the revision its line stood at when it was
  opened, in every query it runs, and its commit builds on that
  revision. If the head moved meanwhile (a commit through another
  handle of the branch, a pull, a commit through the same handle of a
  snapshot), `commit().perform()` and `commit().publish().perform()`
  are refused with `CommitError::Publish(PublishError::VersionMismatch)`
  and nothing of the transaction lands: open a new transaction on the
  fresh head. Before, such a transaction silently built on the moved
  head, reading one revision and committing against another. The
  publish still CAS's the head cell against the version the transaction
  read, so the refusal holds under a race too; the check at commit is
  only the early word.
- `Transaction<L>` requires `L: Line`, a new sealed trait implemented
  for `&Branch`, `&Snapshot` and `TransactionBatch`. Code generic over
  the line (`fn f<L>(t: Transaction<L>)`) adds the bound. The lifetime of
  `Transaction::query()`'s `TransactionQuery` is now that of the borrow
  of the transaction, not of the branch.
- The heads a read settlement records (`ReadObservation`) are the
  revisions the transaction pinned, and the edition its writes stand at
  is that head's successor, whatever the head is by the time it commits.
- A line's session overlay (`Branch::overlay`) is not pinned: it is
  read live, as before.
- `SourceRef`, the crate's internal borrowed line, is gone; `View` and
  `Live` replace it. It was never public.

Why: a read's answer should be a function of one state of the branch,
and a transaction's commit should apply what the transaction read.
Reading each scan against whatever the head happened to be made both
depend on the timing of unrelated commits, which no caller can reason
about.

### The corpus crate is `dialog-corpus`, and it asks its environment for everything it reads

[#768](https://github.com/dialog-db/dialog-db/pull/768), decided in
[design/tree-node-reads](./design/tree-node-reads.md); the spilled-value
read is argued in
[design-spilled-values.md](./design/tree-node-reads/design-spilled-values.md).

The crate that held the data model and its indexes, `dialog-corpus`, is
`dialog-corpus`: the corpus is the body of claims a branch holds, in the
search tree. Its storage side is named after it, and the crate
no longer depends on a store crate. What it reads beside its nodes, a
value spilled out of its key, it asks its environment for through
`blob::Load`, performed in one place and checked against the reference;
nothing in the crate reads a store by type. The word "artifact" is
retired: what the model called an artifact is a fact, a claim with its
cause.

What this changes for you:

- The crate is `dialog-corpus`; paths that began `dialog_corpus::`
  begin `dialog_corpus::`. `tree::Corpus` is `tree::Corpus`,
  `CorpusExt` is `CorpusExt`, `CorpusCache` is `CorpusCache`,
  `BufferedCorpus` is `BufferedCorpus`, `CorpusWriter` is
  `CorpusWriter` and `DialogCorpusError` is `DialogCorpusError`.
- The model side is renamed the same way. `Fact` is `Fact`,
  `FactView` is `FactView`, `FactViewStream` is `FactViewStream`,
  `Selector` is `Selector`, `fact_index_keys` and
  `fact_index_keys_with` are `fact_index_keys` and
  `fact_index_keys_with`, and every `from_fact`, `to_fact` and
  `for_fact` is `from_fact`, `to_fact` and `for_fact`. The module that
  held them, `artifacts`, is `model`. In `dialog-query` the re-export
  module `artifact` is `corpus`, and its aliases `ArtifactsRelation` and
  `ArtifactTypeError` are `CorpusRelation` and `CorpusTypeError`. `Fact`
  prints as `Fact` in its `Debug` and `Display` forms.
- The nightly fuzz soak is `soak:facts`, run by `--test fact_fuzz`, and
  reads `DIALOG_FACT_FUZZ_SEEDS` and `DIALOG_FACT_FUZZ_OPS` where it read
  `DIALOG_ARTIFACT_FUZZ_*`.
- `dialog-corpus` does not depend on `dialog-storage`. `Blake3Hash` is
  still `[u8; 32]` and is defined in the crate; the `HashType` re-export
  is gone (it is `dialog_storage::HashType`). `IndexRoot`, a CBOR wrapper
  nothing used, is removed. `From<DialogStorageError> for
  DialogCorpusError` is gone: a storage error reaches the corpus as an
  `ArchiveError`, a `BlobError` or the tree's error, each of which still
  converts. `DialogSearchTreeError` implements `AuthorizationFailure` and
  has `rejection()`, so a decision made beneath the tree is read without
  naming the store's error type.
- `ArchiveDelta::block` and `ArchiveDelta::flush_blocks` are gone, and
  `DeltaOverlay` no longer answers `archive::Load`, `archive::Get` or
  `Holds`: it answers the node layer's `Read`, `Preload` and `Write` for
  `CorpusNode`, and `blob::Load` for a value the batch staged. A caller
  that shipped a batch's nodes as blocks takes them with `flush_nodes()`
  and writes them with `nodes::Write`, as a commit does.
- `Provider<blob::Load>` is no longer asked of an environment by an
  operation that never reaches a spilled value: `CorpusExt::select_data`,
  `format_manifest`, `record` and `estimate`; `BlobIndexExt::get_blob`,
  `has_blob` and `list_blobs`, and `blob_changes`; a buffered batch's
  `record` and `seal`; `rekey_tree`; `history::context_in` and
  `region_entry`; `spilled_refs`; `merge::screen_history`. It stays on
  `apply`, `select_record`, `scan`, `write_instructions`, the buffered
  applies, `observe_revisions`, `TreeHistory` and, through
  `select_record`, `content_size` and `asset_size`. A caller that carried
  the bound only to satisfy one of the former can drop it.

Why: a crate named for artifacts no longer said what it held, and a crate
that embodies the model should not depend on a store. #755 made the tree
ask its environment for nodes; this makes the layer above it ask for
everything else the same way, so what a store is and where it is stay the
environment's business.

### A tree asks its environment for nodes, and the environment keeps them

[design/tree-node-reads.md](./design/tree-node-reads.md), planned in
[#753](https://github.com/dialog-db/dialog-db/pull/753).

A search tree no longer reads blocks and decodes them, nor finds the node
cache for itself. It performs three commands on the environment it is
given, defined in `dialog_search_tree::nodes`: `Read` (the node a name
names, from anywhere or from this replica alone), `Preload` (make a node
cheap to read soon) and `Write` (the nodes a persist sealed, to store and
keep readable). The environment answers however it keeps nodes. A store
that keeps blocks (`Volatile`, `FileSystem`, `IndexedDb`) answers through
the translation layer in the tree crate, which loads, checks and decodes;
`Storage` routes a request to the space the catalog's subject is mounted
as, and `Space` hands it to its archive; a peer answers from the node cache
it holds, then its storage, then a hydration from the subject's routes, and
keeps what it read or wrote. A store that keeps nodes in a form of its own
implements the three commands and is reached the same way.

What this changes for you:

- A request is written at the node it is about: `nodes::Read<N>`,
  `nodes::Preload<N>` and `nodes::Write<N>` for an `N: nodes::Node`, a
  sealed trait `PersistentNode<Key, Value>` implements. A tree operation's
  environment bound is spelled out, `Env:
  Provider<nodes::Read<PersistentNode<Key, Value>>> +
  Provider<nodes::Preload<PersistentNode<Key, Value>>> + ConditionalSync`,
  with no alias trait in between, instead of `Provider<archive::Load> + Holds + ConditionalSync` (and `+
  Provider<archive::Get>` where a difference was computed). `Holds`, `Load`
  and `Get` are no longer asked of a tree's environment.
- `dialog_corpus::tree::CorpusNode` names the node of the corpus, the
  tree over claims (`PersistentNode<Key, State<Datum>>`), and an
  environment a corpus is read through is bounded as
  `Provider<nodes::Read<CorpusNode>> + Provider<nodes::Preload<CorpusNode>>`,
  one that is written through as `Provider<nodes::Write<CorpusNode>>`.
  There is no alias trait for either: a signature says which requests it
  performs. "Corpus" is the name the crate's storage side will carry when
  `dialog-corpus` is renamed; everything new here uses it.
- `TransientTree::persist(delta)`, `HitchhikerTree::persist(delta)`,
  `HitchhikerTree::persist_mut(delta)` and `Node::into_link(delta,
  manifest)` take no environment and no cache, and the delta they fill is
  a `Delta<Blake3Hash, PersistentNode<Key, Value>>`: the sealed nodes, not
  their blocks. A persist keeps nothing in any cache; the environment keeps
  the nodes when the commit performs `Write`. `ArchiveDelta::nodes()`
  replaces `blocks()`, `node(hash)` and `flush_nodes()` are new, and
  `block(hash)` and `flush_blocks()` still hand out blocks to a caller that
  ships them. A batch that reads a node it sealed before the flush gets
  the staged node back from its `DeltaOverlay`.
- `Accessor::new(env, catalog)` asks nothing of the environment;
  `Accessor::cache`, `cached` and `keeps` are gone, and `load_node` needs
  `Provider<nodes::Read<PersistentNode<Key, Value>>>` and
  `Provider<nodes::Preload<PersistentNode<Key, Value>>>` of its environment.
- An environment that answers `Load` and holds handles answers the node
  requests through `dialog_search_tree::reads_nodes_through_blocks!` and
  `writes_nodes_through_blocks!`; one with a store beneath it and a
  hydration uses `dialog_repository::read_node_or_hydrate`,
  `preload_node_or_hydrate` and `write_nodes`. `Peer`, `AccessEnv`, the
  query session, `DeltaOverlay`, `Counting`, `Metered`, `Storage`, `Space`
  and the three stores answer them; `PeerSpace` requires them of a space.
- A difference's boundary side reads locally through the node layer
  instead of performing `Get`: the environment's cache is consulted, which
  changes no answer, since a scope's cache holds only what its archive has
  produced or a successful write stored.
- The query session no longer decodes and caches the blocks it loads; a
  node read through it reaches the peer's cache. An integration opens its
  read-ahead window once per batch of pending changes instead of when a
  path misses the cache, since nothing outside the environment can tell.
- A node the environment holds is answered at once, with nothing
  allocated: the translation layer, `read_node_or_hydrate`, the overlay
  and the query session return `Resume::now` on a hit (see the entry
  below). Under `dialog-perf` the point-read scenarios are within 0.4% of
  before and the commit scenarios 0.6% to 1.9% cheaper; the design topic's
  journal has the table.

Why: the tree was written before the capability model and only partly
moved onto it, so what it needed from the world was not expressed as
capabilities. It performed `Load` for bytes and then did a provider's work
itself: found a cache through a lookup that could come back empty,
checked, decoded and kept nodes, while other code kept them again its own
way. A store that keeps nodes in a form of its own (the single-file
archive of #733) could only reach the tree through a side channel. With
nodes as the tree's requests, each concern sits with whoever owns it, and
a store's byte representation is not the tree's concern.

### A provider answers at once or later, and a caller cannot tell

[#755](https://github.com/dialog-db/dialog-db/pull/755), argued in the
journal of [design/tree-node-reads.md](./design/tree-node-reads.md).

`Provider::execute` is no longer an `async fn` under `async_trait`. It
returns a `Resume<'_, Output>`: either the output at once
(`Resume::now`), or a future that produces it (`Resume::later`). A caller
awaits either one the same way, so the change is invisible where a
command is performed; what it changes is that a provider holding the
answer (a cache that has the value, a check that fails before any I/O)
hands it back without allocating a future, and a provider standing in
front of another hands back what it was handed.

What this changes for you:

- A `Provider<C>` impl is written one of two ways. Under
  `#[dialog_capability::provider]`, `async fn execute(&self, input) ->
  Output` as before: the attribute replaces the `async_trait` pair, and
  the body becomes the future. Without it, `fn execute(&self, input) ->
  Resume<'_, Output>`, returning `Resume::now` on the paths that have the
  answer and `Resume::later(async move { .. })` on the rest. A forwarding
  impl returns what the inner provider returned, boxing nothing.
- `#[derive(Provider)]` forwards without boxing, and no longer asks
  `ConditionalSend` of the command's input or `ConditionalSync` of the
  struct.
- `Resume` implements `Future` and `Unpin`, and offers `is_now`,
  `into_now` (the answer, or the future that waits for it) and `map`.
  `dialog_capability::Pending<'a, T>` names the boxed future.
- A generic provider whose future awaits another `execute` and holds its
  output needs `C::Output: ConditionalSend`, since a `Resume` is `Send`
  only when its output is; `async_trait` hid that bound inside the box.
- `nodes::Ready` and `nodes::held_node`'s role as its answer are gone from
  `dialog_search_tree`, and so is the `Nodes<Key, Value>` alias trait: a tree
  operation bounds on `Provider<Read<PersistentNode<Key, Value>>> +
  Provider<Preload<PersistentNode<Key, Value>>> + ConditionalSync` itself. `Read::through_blocks`,
  `through_loader` and `through_held_blocks`, `Preload::through_blocks`
  and `through_held_blocks`, `Write::through_blocks` and
  `through_held_blocks`, every request's `perform`, and
  `dialog_repository::read_node_or_hydrate`, `preload_node_or_hydrate`
  and `write_nodes` return a `Resume` instead of being `async fn`;
  `through_loader` takes an `FnOnce(Load) -> impl Future` instead of an
  `AsyncFnOnce`.
- `dialog_common::provider` (the test provisioning attribute) is
  unchanged; in `dialog-macros` it is now `provision`.

Why: the cost of crossing a provider boundary was a boxed future per
call, paid even when the provider had the answer in hand. The first fix
for the tree's node reads was a side door beside the capability model,
which was a symptom of the model: a provider could only ever answer
later. Answering now or later in the one trait keeps every answer inside
the model, with sync and async providers written against the same
signature, and takes the box off every hit in the workspace, not just the
tree's.

### Two claims that stand equal are told apart by the hash of their values

[#752](https://github.com/dialog-db/dialog-db/pull/752)

A cell read under `last` elects its newest claim: the greater standing
(the version, then the cause, then the succession). Two claims stand
equal when one revision wrote both under one cause, which is what two
writes to one cell in one transaction are. Between those, the election
fell to the values and the greater won. It now falls to the hash of each
value's identity key (`Value::tiebreak`), the greater hash winning. Every
place that elects does the same: the tree, a commit's settlement, a
concept read, and `top` among values of one rank.

What this changes for you:

- Where one transaction writes two values to a cell under `last`, or
  under `top` at one rank, the value a read returns may change: it is the
  one whose hash is greater, not the greater value. Claims at different
  standings, which is every concurrent edit between revisions, elect as
  before.
- `max` and `min` elect as before: by value, then between equal values
  by standing. Equal values hash alike, so the hash never decides there.
- Replicas on releases either side of this one can show different values
  for such a cell. Nothing a replica stores is rewritten, and the claims
  are the same on both; only which one a read returns differs.
- `Value::tiebreak` is new.

Why: a tie broken by value favours some values over others, so a writer
can choose the value that wins (the greater number, `true` over `false`).
The hash favours none, as the version's hash already does between
concurrent revisions. It came up while designing record values, whose
values all share one version, so every tie among them reaches this rule
([#748](https://github.com/dialog-db/dialog-db/pull/748)).

### A file store's write lock is never taken by two writers at once

[#741](https://github.com/dialog-db/dialog-db/pull/741), on [#723](https://github.com/dialog-db/dialog-db/pull/723)

A native file store guards each cell's compare-and-swap with a lock file.
It was a PID lock taken through `pidlock`, whose acquire creates the file
and writes the PID after; a second writer arriving in between read no PID,
removed the file as corrupt, and took the lock too. Both then compared
against the same version and both wrote, so the later write replaced the
earlier one and each reported success: a commit racing a pull of the same
branch could vanish without an error. Two writers that both found a dead
holder's lock could also both remove it and both take it.

The lock is still a file, `{cell}.lock`, now holding `{pid} {token}`: the
holder's process and a random token naming that one hold. A lock file is
held unless proven otherwise, so one that is empty or does not read as
`pid token` is held and never deleted; the one exception is a bare PID,
the previous release's lock file, read as that PID with token `0`. A writer writes its content to a
temporary file first and hard-links it into place, which fails if a lock
file is there, so a lock file never exists without its content; on a
filesystem without hard links it creates the file and writes it after,
and a writer arriving in between is refused. A lock naming a process
that is not running is recovered by exactly one writer, which first
links it to `{cell}.lock.stale.{token}` (a name only one writer can
create) and checks the file still names the gone holder before removing
it.

What this changes for you:

- Concurrent writers of one store, in one process or several, no longer
  lose a write to each other. A writer that finds the lock held is
  refused with `FileSystemError::Lock` (`Concurrent write in progress`),
  as before.
- A crashed writer's lock is recovered by the next writer, once the
  crashed process is gone. Whether it is gone is asked of the OS in the
  writer's own PID namespace, so writers sharing one store must share a
  PID namespace. If the crashed writer's PID has been given to another
  process, its lock looks held until the lock file is deleted by hand;
  so does a crashed writer's lock on a filesystem without hard links.
- A lock file that is empty or does not read as `pid token` (or as a
  bare PID) is held, whoever wrote it, and has to be deleted by hand if
  no writer is running.
- Lock files are removed on release, by the writer that holds them
  only: a writer whose lock file now names another holder leaves it.
- Cells whose names differ only in extension (`a.x`, `a.y`) get a lock
  each, instead of sharing `a.lock`.
- `pidlock` is no longer a dependency.

Why: a write lock that two writers can hold at once does not guard the
compare-and-swap, and the writes it loses are reported as successes, so
nothing downstream can notice or repair them. The lock stays a file
rather than an OS lock (`flock`), which errors or is not enforced on
network and FUSE filesystems that stores live on.
### An environment bounds every cache it holds, and spilled values share one budget

[#742](https://github.com/dialog-db/dialog-db/pull/742)

`HeldCaches::builder()` returns a `CacheBudget` that sets the size of
every cache an environment holds for its repositories: tree nodes and
spilled values in bytes, the rest in entries. Spilled values are now kept
in one cache for the whole environment, under one byte budget, instead of
one 8 MiB cache per repository. A snapshot taken through the environment
and the caches a transaction batch keeps to itself take their sizes from
the same budget. Nothing configured, every cache keeps the size it had.

What this changes for you:

- `HeldCaches::builder().nodes(bytes).spills(bytes).entries(n).rules(n)
  .plans(n).formulas(n).build()` builds caches that keep to each cap.
  `entries` bounds each repository's causal verdicts, contexts, revision
  records and rewritten trees; `rules` bounds the shared rule content
  (heads, bodies, selecting rules) and `plans` the query plans. `HeldCaches::new()` and
  `with_budget(bytes)` are shorthands for the defaults
  (`CacheBudget::default()`), and `HeldCaches::limits()` reads the budget
  back.
- Memory for spilled values no longer grows with the number of open
  repositories: the default 8 MiB (`SPILL_CACHE_BUDGET`) is now the total.
  `HeldCaches::release(repository)` no longer drops spilled values, since
  one cache holds them for every repository by content; they age out under
  the budget, and `clear()` drops them.
- `HeldCaches::bytes()` now counts spilled values as well as tree nodes.
  `spill_bytes()` reads the spilled share, and `entries()` counts what the
  entry-bounded caches hold (a count, not bytes).
- `Repository::snapshot_in(env, revision)` takes a cold snapshot sized by
  the caches `env` holds, reading spilled values, rule content and plans
  through them. `Repository::snapshot(revision)` keeps the default sizes.
- A cache bounded by entries now holds at most the capacity it was given
  on native targets, however small. Below 2048 entries a sharded cache
  used to keep at least one entry per shard, up to 16, so a capacity under
  16 held more than asked. The default sizes keep all 16 shards.
- New accessors: `CausalityCache::with_capacity`, `ContextCache::with_capacity`,
  `PlanCache::with_capacity` (with `PLAN_CACHE_CAPACITY`), `len` on these and
  on `dialog_search_tree::Cache` (with `capacity` and the now public
  `CACHE_CAPACITY`), and `budget`, `bytes`, `len` and `clear` on `SpillCache`.

Why: an embedder running in a fixed memory limit, such as a Cloudflare
Durable Object with 128 MB for everything, could bound only the tree
nodes. The spill budget multiplied with each repository opened, and every
other cache was fixed when it was made (#706).

### A repository an earlier release left opens, whatever layout its registry is in

Opening or loading a repository that predates layout versioning carries
its remote and upstream cells over into facts in its registry branch
(`meta`). A registry an earlier release wrote is laid out from before the
number band, and a commit into such a tree is refused with
`CommitError::NeedsMigration`. Nothing moved the registry first, since
nothing outside dialog-repository writes it, so the open failed with
`OpenRepositoryError::Upgrade` / `LoadRepositoryError::Upgrade` on every
attempt, and the repository could not be opened at all. Every write to a
registry or a host's contacts now moves the branch to this release's
layout first, when it needs it, and applies the changes on the rewrite.

What this changes for you:

- A repository last written by a release before 0.3.0 opens instead of
  failing with "The branch must be migrated before it is written".
- Upstream changes (`set_upstream`) and contact writes on such a branch
  succeed the same way, where they failed with `NeedsMigration`.
- An embedder no longer needs to migrate a registry itself; it could not,
  since the open that fails is the one that would hand it the branch.

Why: the open-time upgrade is the only thing that writes a registry an
earlier release left, and it ran before any embedder could migrate it.

### dialog-storage reads a browser file without `wasm-streams`

[#744](https://github.com/dialog-db/dialog-db/pull/744)

The web file system provider read a `File` through the `wasm-streams`
crate's `ReadableStream` adapter. That crate exports its adapters through
`#[wasm_bindgen]` under names that carry no version, so two versions of it
in one wasm module are duplicate symbols at link time. The workspace had
pinned it to 0.5 to match the copy `reqwest` brings in through `iroh`
(#521), while an embedder linking the Cloudflare `worker` crate needs 0.6,
and no version satisfied both. The provider now reads the stream through
web-sys's default reader and the crate is gone from dialog-storage.

What this changes for you:

- dialog-storage no longer depends on `wasm-streams`, so an embedder may
  link whichever version its other dependencies need.
- Nothing stored or synced changes, and the reader's behaviour is the
  same: chunks arrive as they are read, and dropping the reader before the
  end cancels the stream.

Why: a dependency that can only ever be one version across a whole wasm
module should not be pinned by a library that uses one call of it.

### A pull or a push that meets another revision under its own head's version is refused

[#707](https://github.com/dialog-db/dialog-db/pull/707), fixing [#705](https://github.com/dialog-db/dialog-db/issues/705)

Two replicas writing with one key on one branch share an origin, since
an origin derives from the branch and the issuer. When both build on the
same head, each mints the next edition of it over its own tree: two
revisions under one version. Every context that holds one claims the
other, so the second writer's pull took the first's head as already seen
and merged nothing, and its push then replaced that head. Both writers
were told their write synced, one write was gone, and the two replicas
kept different trees under one version for good. A pull or a push now
compares the upstream's head with the branch's own head first, and when
they are the same version over different trees, refuses with an error
that says so.

What this changes for you:

- `PullError::WriterRacedItself { branch, ours, theirs }` and
  `PushError::WriterRacedItself { branch, ours, theirs }` are new. A pull
  or a push that meets the rival head returns one and writes nothing: not
  the branch's head, not its sync base, not the upstream. A sync loop
  that pulls on `PushError::NonFastForward` and pushes again now stops
  with this error instead of reporting the write synced.
- The refused write is not lost: written again on a replica that has
  not written (one that pulls the upstream fresh), it lands on top of the
  rival. Writers that need to write at once each need a key of their
  own, which is what dialog's model has always assumed.
- A head that wrote first and a sync with no rival behave as before. The
  check compares two heads already in hand, so it reads nothing.
- Still not detected: a replica that minted two or more revisions on the
  shared head before it syncs holds a head above the rival's edition,
  and nothing on either head tells the rival from its own earlier
  revision. One write per sync per key, as the stateless workers of #705
  do, is caught.

Why: concurrent writers sharing one identity are outside the model, but
breaking that rule cost a write without an error. A refusal the writer
can see is the least the replica owes it; keeping both writes by a merge
cannot be done soundly, since each head's context already claims the
other's write and the merge would screen it out as removed.

### `ConditionalSync` means `Sync`, not `Send + Sync`

[#734](https://github.com/dialog-db/dialog-db/pull/734)

`dialog_common::ConditionalSync` was `Send + Sync` on native targets, so
a `ConditionalSync` bound quietly asked for `Send` as well, and a bound
that needed only `Sync` could not say so. It is now `Sync`, matching its
name and `dialog_ucan_core::ConditionalSync`; `ConditionalSend` is the
bound for `Send`. On wasm both remain no bound at all.

What this changes for you:

- A generic bounded only by `ConditionalSync` is no longer known to be
  `Send` on native. Where your code moves it into a `Send` future, sends
  an `Arc` of it to a task, or stores it in a type that must be `Send`,
  write `ConditionalSend + ConditionalSync`.
- These traits now name both bounds as supertraits, so what implements
  them and what a bound on them implies is unchanged:
  `dialog_corpus::{ValueType, History}`, `dialog_did_web::Fetch`,
  `dialog_remote_ucan::Presented`, `dialog_peer_iroh::{Channel, Connect}`
  and the `dialog_peer` helpers' `Meter`.
- A store a tree reads through by value asks for `ConditionalSend` as
  well, since it is moved into the read's future: the `S` of the
  `dialog_corpus::CorpusExt` and `BlobIndexExt` methods,
  `TreeHistory`, `blob_changes` and `spilled_refs`, and of the query
  builder's `execute`, `execute_owned` and `estimate` in
  `dialog_repository`. An env passed by reference (`perform(&env)`)
  still asks only for `ConditionalSync`.
- `dialog_query::merge_join` and `multi_merge_join` ask for
  `ConditionalSend + ConditionalSync` on their key function and key, and
  the `Resolver` and `Revocations` of `dialog_peer_iroh::accept` ask for
  both, as they always needed.

Why: a bound named `Sync` that also demands `Send` hides what code
actually needs, and makes a type that is `Sync` but not `Send` impossible
to accept through it.

### A pull that loses its publish forgets the head it minted

[#704](https://github.com/dialog-db/dialog-db/pull/704)

A pull remembered its merged head's causal context at prepare time,
before publishing it. A merge prepared on a stale view of the head mints
the version of the commit already landed on it (a version is the origin
and its edition, so two revisions minted on one parent by one origin
share it), so when its publish lost, its context stayed in the memo under
that commit's version: the commit read as having seen the upstream, and
the next merge screened the upstream's claims out as removed. The context
now enters the memo only once the head is published, as a commit's does.

What this changes for you:

- A branch whose pull loses the race to a commit through another handle
  no longer drops the upstream's claims on its next merge. Nothing stored
  changes: the memo is in memory, and a pull that lands is remembered as
  before.
- `PreparedPull::commit` still fails such a race with
  `PullError::Publish(PublishError::VersionMismatch)`; the caller refreshes
  and pulls again, as before.

Why: the memo speaks for heads that landed, and a context remembered
before the publish speaks for one that may never land.

### Distinct members inserted between the same neighbours get distinct positions

[#729](https://github.com/dialog-db/dialog-db/pull/729).

A position's fractional tail comes from a bias derived from the member's
identity. The bias was the first six base62 digits of the identity bytes,
and base62 is a big-endian re-encoding, so those digits depend only on the
identity's leading bytes and its length. Every `did:key:z6Mk…` entity
shares both, so every one of them got the same bias (`13lS7E`, or
`FvVVmY` one character shorter). The bias is now taken from the identity's
BLAKE3 digest.

What this changes for you:

- `Bias::derive` returns a different bias for the same identity, so
  `dialog_corpus::position::insert` and the `dialog/position` formula
  derive different positions than before for the same member and
  neighbours. Derivation is still deterministic: every replica on this
  release derives the same position for the same insert.
- Two different members inserted into the same slot used to derive the
  same position, so a relation that keeps one member per position kept
  only one of them. They now land apart.

Why: the bias exists so distinct members disperse while the same member
converges. Read off the identity's head it converged for everything, so
concurrent inserts into one slot collided instead of interleaving.

### A tree reads its catalog's blocks through `Load` on the env, and the env hydrates

[#735](https://github.com/dialog-db/dialog-db/pull/735), after
[#730](https://github.com/dialog-db/dialog-db/pull/730); argued in
[notes/tree-reads-as-effects.md](./notes/tree-reads-as-effects.md).

A search tree is now a root under a catalog, and it reads a node by
performing the new `dialog_effects::archive::Load` command on the
environment it is given: the block its `Get` names, from the local
archive, or else hydrated from where the environment knows the subject's
blocks live. A spilled value is read the same way through
`dialog_effects::blob::Load`. The environment holds the checked-node
cache (`NodeCache::of(env, subject)`) and the hydration routes
(`dialog_network::Routes`, which a branch registers when it opens), so a
peer answers every tree read the same way, sharing in-flight hydrations
through its `Hydrate` scheduler. The store wrappers that bridged the old
subject-less `LoadBlock` to the env are gone. `Get` is unchanged and
never hydrates.

What this changes for you:

- `LoadBlock`, `LoadBlob`, `ArchiveReader`, `Nodes`, `Cached`,
  `ArtifactNodes` and `MemoryBlocks` are removed. A tree reads through any
  `Env: Provider<archive::Load> + Holds + ConditionalSync` (plus
  `Provider<blob::Load>` for artifact trees, and `Provider<archive::Get>`
  where a difference is computed). A test store is
  `dialog_search_tree::helpers::Blocks`, a `Volatile` under a test
  catalog.
- `dialog_repository::LocalIndex` and `NetworkedIndex` are removed: read
  through the env itself (`tree.get(key, &env)`, `select.execute(&env)`).
  `NetworkedIndex::with_priority(p)` is `tree.ranked(p)`; a tree that
  must read only what the replica holds is `tree.locally()` (or
  `Load::locally()` for a single read). `load_or_hydrate`,
  `load_blob_or_hydrate`, `hydrate` and `RemoteFallback` stay where they
  were exported.
- `PersistentTree::empty()` and `PersistentTree::from_hash(root)` are
  `PersistentTree::empty_in(catalog)` and `PersistentTree::at(catalog,
  root)`; `empty_with_manifest`, `TransientTree::new`,
  `TransientTree::with_manifest`, `TransientTree::empty_with_manifest`,
  `TransientTree::stitch`, `HitchhikerTree::empty`, `TreeHistory::from_root`,
  `TreeHistory::empty` and `distribution::capture` take the catalog, and
  `Accessor::new(env, catalog)` does too. `PersistentTree::catalog()`,
  `with_root`, `reading()`, `ranked(priority)` and `locally()` are new;
  `NodeCache::of`, `NodeCache::hold` and `NodeCache::held_by` find and
  place the cache an env holds.
- `fetch_spilled`, `fetch_spilled_cached`, `fetch_spilled_reference` and
  `merge::observe_revisions` take the archive the value lives in; a
  spilled value is read by `load_spill`.
- `Branch::history`, `Snapshot::history`, `Branch::log`, `Snapshot::log`,
  `Select::perform` and `Select::estimate_perform` ask less of the env
  (the two loads and `Holds`), and the commands that read trees ask
  `Holds` of it. Every peer and session already holds.
- `HeldCaches` holds its node cache where trees find it, so the budget,
  `release` and `clear` govern every tree read through the env, and an
  env given its own `HeldCaches` before anything opens keeps nodes under
  that budget.
- A borrowed environment is an environment: `&T` provides every command
  `T` provides (`dialog_capability::Provider`) and holds what `T` holds
  (`dialog_common::Holds`).
- A resolver premise (`tree/node`, `tree/span`, `tree/key`, `tree/value`)
  loads the block it names against the wildcard subject's archive
  (`dialog_query::resolver::anywhere`), and `Scope` asks for the two
  loads instead of `LoadBlock` and `LoadBlob`. A query over several lines
  answers from each line's catalog in turn.
- Any read of a repository one of whose open branches tracks a remote
  can hydrate a block it lacks, a snapshot's and a transaction's staged
  view included, and a branch can hydrate a block only a sibling's remote
  holds. A miss tries every remote the subject's open branches track, in
  the order they were opened and each in its tracking order, where before
  only the reading branch's first remote was asked; a miss no reachable
  remote answers fails naming the first remote that could not be reached,
  rather than reading as absent. With no remote tracked a read is local,
  as before. An export that does not download reads only what the replica
  holds.
- A push's novelty walk, its subtree forwarding and its spilled-value
  shipping read only the local archive, by `Get`, and never hydrate.

Why: the tree's read was a command with no subject, so every reader
needed a wrapper that knew the catalog, another that knew where to
hydrate from (worked out per read by `Branch::fallback`, which was about
9% of a worker's CPU), and the node cache was plumbed through them. Read
as a capability on the env, there is one way to get a node, which goes
through the cache and the env's hydration, and nothing to wrap.

### A difference reads a leaf both sides share only at the keys its buffered ops touch

[#736](https://github.com/dialog-db/dialog-db/pull/736)

When two trees hold the same leaf and differ only in the ops a buffer
above routes to it, the comparison now resolves just the keys those
ops cover: each side's winning op, or the stored entry where that side
has none. Before, the entry walk streamed every stored entry of the
leaf on both sides and dropped the ones that matched. A commit buffered
at the root routes each op to a whole leaf, so every subscription poll
over the commit decoded every entry of each leaf the commit's ops
landed in, whatever its scope. A scoped difference now resolves only
the covered keys inside its scope, so a poll whose scope the commit
missed reads nothing of those leaves. The buffered fast path also
checks the leftmost separator before decoding any buffer, since a
root's is always empty and the decode could never be used.

What this changes for you:

- A subscription poll after a commit costs in proportion to the ops
  the commit buffered, not to the size of the leaves they land in.
  `subscribe-fanout` and `subscribe-pulled` (201 polls after a one-note
  commit) go from about 203M to about 39M instructions, and
  `subscribe-repoll` from 14.2M to 12.6M.
- A poll's cost no longer depends on where a written value sorts. A
  note body sorting into a leaf that every note subscription's
  rule-discovery scope crosses used to cost each of the 201 polls about
  370k more instructions than one sorting elsewhere.
- `differentiate`, `differentiate_within`, `changes` and
  `changes_within` report the same changes. `TreeDifference::changes`
  on a difference built by `compute_within` no longer yields
  out-of-scope spillover from such a leaf; `changes_within` already
  filtered it.

Why: the stored entries of a shared leaf are the same bytes on both
sides, so only a key an op covers can differ, and decoding the rest
was work every subscriber repeated for every commit.

### A tree reads every node through its store, which keeps what it checked

[#730](https://github.com/dialog-db/dialog-db/pull/730)

A search tree no longer carries a node cache. The store a tree reads
through implements the new `dialog_search_tree::Nodes` trait: `kept()`
names the node cache it holds, if any, and `load_node()` answers a read
from that cache, else from the store's blocks, checking the node once and
keeping it. Every tree read goes there: point lookups, scans, traversals,
differences, edits loading the nodes they rewrite, and persists, which
keep the nodes they seal in the same cache. A new read path therefore
cannot bypass the cache, as the difference, `traverse_available`, push's
subtree forwarding, diagnose and `canonical_divergences` did. In the
repository the cache is the line's: `LocalIndex` holds the handle and
`NetworkedIndex` forwards it.

What this changes for you:

- Every tree method that took `Env: Provider<LoadBlock>` takes
  `Env: Nodes<Key, Value>`. A block store that keeps nothing implements
  it in one line (`impl<K, V> Nodes<K, V> for MyStore {}`), as
  `MemoryBlocks`, `ObservingBlocks` and `JournaledBlocks` do; wrap one in
  `Cached::new(cache, &store)` to keep the nodes read through it.
- `PersistentTree::node_cache`, `PersistentTree::empty_with_cache` and
  `PersistentTree::from_hash_with_cache` are removed: use `empty()` and
  `from_hash(root)`, and give the store the cache.
  `PersistentTree::empty_with_manifest` takes only the manifest.
- `TransientTree::new(root)`, `TransientTree::with_manifest(root,
  manifest)` and `TransientTree::empty_with_manifest(manifest)` drop
  their cache argument.
- `TransientTree::persist(delta, store)`, `HitchhikerTree::persist(delta,
  store)` and `HitchhikerTree::persist_mut(delta, store)` take the store
  whose cache keeps the sealed nodes. `TransientNode::persist`,
  `TransientNode::persist_mut` and `Node::into_link` take
  `Option<&NodeCache>`.
- `Accessor::new(env)` takes only the store.
- A difference's `MissingBlocks::Deny` side reads through the store's
  cache. A `MissingBlocks::Boundary` side reads the store's blocks
  directly, so absence is the store's own word.
- A store that keeps nothing skips the range scan's sibling read-ahead
  and an integration's open-ahead pass, which would only read each node
  twice.
- `dialog_corpus::ArchiveReader` requires `ArtifactNodes`
  (`Nodes<Key, State<Datum>>`). `DeltaOverlay` keeps what its
  environment keeps.
- `TreeHistory::from_root_with_cache` and `TreeHistory::empty_with_cache`
  are removed: use `TreeHistory::from_root(root, store)` and the new
  `TreeHistory::empty(store)`. `rekey_tree` drops its cache argument.
  `distribution::capture` reads through the store.
- `LocalIndex::new(env, catalog, nodes)` and `NetworkedIndex::new(env,
  index, remote, nodes)` take the node cache the index keeps.
- `Select::execute(store)` probes the root and scans through the store,
  so a store passed in should keep a node cache for repeated selects to
  be answered from memory.
- Reads that went around the cache now hit it: a subscription poll's
  difference, a snapshot export's walk and a preload walk read the
  line's cache, and a select's scan no longer reads its root again
  after the probe. The `subscribe-repoll` perf scenario reads no block
  from the archive where it read 6, and runs 10.6% fewer instructions.
- A poll after a commit that adds one note, with a subscription per
  note of 200 (the new `subscribe-fanout` perf scenario), reads 0
  blocks instead of 806 (32 MB). Every subscription diffs the same two
  trees within its own demand, and the first to read a changed block
  keeps it for the rest. In a browser over IndexedDB, which pays a
  transaction per block read, a write from another device showed on a
  200-note page in about 0.4 s instead of 2 to 3 s.

Why: the cache was carried by trees and threaded through `*_with_cache`
constructors, so every read path had to remember to use it, several did
not, and two places copied the cache-then-store read by hand. Putting it
on the store makes the cached read exist in one place and every read go
through it.

### A read finds its remote without deriving every upstream

[#730](https://github.com/dialog-db/dialog-db/pull/730)

Every select, subscription poll and migration check asked the branch
where a read held by reference falls back to, and the branch answered
by building its full upstream list: deriving each upstream's sync
target, a hash, and comparing every pair. It now takes the first route
at a peer among the pulls and then the pushes, through
`Tracking::first_remote_route`, so the answer is the same without that
work. It was about 9% of a Tonk service worker's CPU while switching
spaces.

What this changes for you:

- `Upstreams::fallback` is removed; `Branch::fallback` no longer builds
  `upstreams()`.
- A read's fallback is the one it was: the first upstream at a peer,
  reachable or not, pulls before pushes, else the first synced route at
  a peer.

Why: the fallback is asked on every read, and the answer needs one
route, not a hashed and deduplicated set of them.

### A request the access service never answers is given up on, and a read is sent again

[#727](https://github.com/dialog-db/dialog-db/pull/727)

An invocation `dialog-remote-ucan` sends as an HTTP request (every one
when the service names no socket, a block read or write, or the fallback
when the socket does not connect) waited for its answer with no bound.
One sent on a pooled connection the network had silently dropped, as a
NAT or a mobile network does a quiet one, was never answered and never
failed. It now waits as long as an invocation over the socket waits
(`Liveness::answer`, 30 s by default), plus the time its payload takes
to upload at 16 KiB/s, and is then aborted and answered as unavailable.

What this changes for you:

- A memory operation sent this way that the service does not begin
  answering in time fails with `Rejection::Unavailable`, as one over a
  silent socket already did; a blob read or write fails with a transport
  error. Either may be tried again with an invocation of its own.
- `UcanSite::with_liveness` now bounds these requests too.
- A block or blob read the service has not begun answering after 3 s
  is sent again, up to seven copies (one more than the connections a
  browser keeps to one host), and the first answer is taken. A
  read that met connections the network forgot is answered in seconds
  rather than failing after 30 s each. The copies carry one invocation,
  so a service may see it presented more than once; the refusal of a
  copy as replayed is not taken for the read's answer.

Why: in tonk, a sync whose request was lost this way held the sync of
every space behind it, so after a few minutes idle on a NATed network,
changes from another device stopped arriving until the page reloaded.

### A rule's conclusion under `last` succeeds the claim its body read

Fixes [#724](https://github.com/dialog-db/dialog-db/issues/724)

A deductive rule that reads a cell and derives another value for it,
`x/status(e) := "todo" :- x/status(e) = "triage"`, rewrites the cell,
the way a write does. Its conclusion stood as the newest fact its body
consumed, which for such a rule is the very claim it rewrites, so the
two tied on standing and a `last` read fell to value order: `"triage"`
won over `"todo"`, and the reverse rewrite only appeared to work because
`"triage"` sorts after `"todo"`. Inside its component the rule still
reads the cell as a set, and its conclusion now remembers the values of
the cell it displaced. When the component is derived, each cell's
candidates are ranked by those successions, a rewrite after what it
read and a rewrite of a rewrite one further, and the pick elects over
the ranks: by version, then cause, then rank, then value. A conclusion
of the value it read displaces nothing, so a rule deriving a relation
from itself still settles; one whose body does not read the cell
stands as before, as the newest fact it consumed.

What this changes for you:

- A `last` read of a cell some rule rewrites returns the rule's value,
  whatever the values' order. Two rules rewriting one claim elect by
  which one read newer facts, as concurrent writes do, and a later
  write to the cell still wins. `all` is unchanged and keeps both.
- A chain of rewrites reads its last link.
- Rewrites that read each other back, `"triage"` to `"todo"` beside
  `"todo"` to `"triage"`, form a cycle no rank orders. Its members
  share one rank and are elected among as concurrent candidates, by
  version then value, so a merge that closes such a cycle leaves every
  query answerable. Before, the same pair also fell to value order,
  but so did every rewrite.
- `dialog_corpus::Standing` carries a `succession`, zero for every
  claim, and `Standing::new` builds a claim's standing. Code building a
  `Standing` by its fields names the new one. Nothing stored or synced
  changes: a rank is computed when a component is read.

Why: a succession orders a cell's candidates by what each saw, and a
conclusion that read a claim and replaced its value saw it. Letting
value order decide made the result of a rewrite depend on the spelling
of its values and on unrelated facts its body happened to read. Ranking
at the exit rather than inside the fixpoint keeps the component
positive and finite, so a cycle of rewrites converges instead of
failing, which an open rule set has to guarantee under every merge.

### A subscription learns of the readable change to a single-valued cell, not only the moved row

Following [#710](https://github.com/dialog-db/dialog-db/pull/710) and
[#714](https://github.com/dialog-db/dialog-db/pull/714)

A poll told incremental maintenance which rows moved. What a rule reads
of a single-valued attribute is the one claim its pick elects from the
cell, and a cell can hold several: a session value over a committed one,
or two committed claims after concurrent writers merged. A row moving
then changes which claim is read without that claim moving: a session
value outvoting a committed one reported only the session value arriving,
and retracting a cell's winner reported only the winner going, not the
claim beneath it coming back. A rule reading the cell from the entity it
derives was unaffected, since that entity is re-derived whole; a
recursive rule, or one reading another entity's cell by its value, kept
a stale row. For every touched cell of an attribute some reachable rule
reads as single-valued, when any reachable rule joins across entities or
recurs, the poll now counts every claim the cell holds as asserted and
every claim it held before as retracted, so the delta-join and the
fixpoint learn of the readable change.

What this changes for you:

- A subscription over a recursive rule, a rule two concept layers deep,
  or a rule reading another entity's single-valued attribute no longer
  keeps a stale row when a session write outvotes a committed value or a
  retraction uncovers an older one. Its results match a fresh evaluation.
- Such a poll reads each touched single-valued cell once more. A
  subscription whose rules are all entity-local pays nothing new.
- `dialog_query::concept::query::affected::elected_attributes` and
  `Elected` are new: the attributes a concept's reachable rules read as
  single-valued.

Why: the invariant that a maintained result equals a fresh evaluation
holds only if maintenance learns of every change a reader can see, and
election makes a cell's readable value a function of all its claims.

### A subscription's view of the state before a change is exact over a cleared or committed-over overlay

Following [#710](https://github.com/dialog-db/dialog-db/pull/710)

A subscription maintained incrementally reads the state before a change
from its pinned revision under the session overlay rewound to the pinned
sequence. The rewind undid each instant's readable moves, and a session
line holds a fact in two independent ways, stored in it and hidden
beneath it by a tombstone, so one fact can be both at once: asserted over
its own tombstone. The readable moves do not say which of the two an
instant changed. A commit retracting such a fact takes the stored copy
out and leaves the tombstone, which the rewind undid by lifting the
tombstone instead; clearing the overlay lists the fact as removed and
un-hidden at once, which the rewind took for no move. Either left the
view before the change short of a fact, and a subscription could keep a
row a fresh evaluation no longer returned. An instant now records how
the line held each fact it moved, and the rewind restores exactly that.

What this changes for you:

- A subscription over a non-local rule no longer keeps a stale row after
  an overlay clear, or a commit retracting a fact the overlay had
  re-asserted over its own tombstone. Its results match a fresh
  evaluation again.
- `Instant` carries `found`: how the overlay held each fact the instant
  moved, before it did, as a `Hold` (stored, hidden).

Why: the rewind inverted a log that did not determine what it changed.
Recording what each instant found makes the inverse exact for any move,
instead of assuming one representation per fact.

### A subscription retracts a row whose supporting facts go together

[#710](https://github.com/dialog-db/dialog-db/pull/710), fixing
[#708](https://github.com/dialog-db/dialog-db/issues/708)

A concept subscription maintained incrementally finds the rows a change
can affect by binding each changed fact into the premise it matches and
joining the rule's remaining premises sideways. It joined them against
the state after the change, so a row two retracted facts had supported,
each under a different premise of a non-local rule, was found through
neither: bound into its premise, each was joined against a state the
other had already left. The row stayed in the subscription's results,
with an empty delta, while a fresh evaluation had none. The join now
runs against the state the derivation it looks for holds in: the state
before the change for a fact that stopped being readable, the state
after it for one that became readable, and the other way round under a
negated premise, where the mirror image (two facts asserted together
under two `unless` premises) was missed the same way. The state before
a change is the subscription's pinned revision under the session
overlay rewound to the pinned sequence, so a change through the overlay
is covered too. An optional premise is read again on each side for the
changed fact's subject, since the row through its old binding and the
row through its new one both change; before, a row that came to exist
because an optional value went absent, or stopped existing because one
arrived, was missed the same way.

What this changes for you:

- `dialog_query::concept::query::affected::affected_entities` takes the
  asserted and retracted facts separately, the environment of the state
  after the change, and a function yielding the state before it, in
  place of one list of facts and one environment.

- A poll after a commit or an overlay instant that retracts more than
  one fact supporting the same row, or asserts more than one under
  negated premises, now moves that row, through the maintenance path,
  where it used to keep it with an empty delta. `Subscription::results`
  matches a fresh evaluation again.
- A subscription whose rule has one premise that does not bind the
  head, such as a department's salaries derived from each member's,
  is now maintained per head instead of recomputed on every poll: the
  join keeps the premise the change was bound into, so it reaches the
  head through it. An election the subscription reads under (`max`,
  `last`) is per entity, so re-deriving the entity takes it again.
- A concept subscription first polled before the branch's first commit
  has no revision to read the state before a change from, so a poll of
  it that needs that state (a retraction, or an assertion under a
  negation, through the session overlay) recomputes instead of
  maintaining, until the branch has a revision. Its results are the
  same.

Why: a derivation a change touches holds on exactly one side of the
change, and its other premises hold only there. Joining every change
against the new state is exact for one changed fact, since every other
premise reads the same facts on both sides, and silently incomplete for
two.

### A difference fetches the blocks it reads a batch at a time

[#690](https://github.com/dialog-db/dialog-db/pull/690), part of [#646](https://github.com/dialog-db/dialog-db/issues/646)

`Prefetch::Lazy`, the default for every difference (`compute`,
`compute_with`, `compute_within`, `differentiate_within`), read each block
at the moment its walk reached it, so a difference over a remote store
paid one round trip per block, one after another. Each pass of the walk
now first plans its reads: a dry run of the walk finds the blocks it is
going to read without reading any, they are fetched concurrently, up to 16
at a time, and the dry run goes on from what they turned out to be until
the pass needs nothing more. The walk then makes the decisions it made
before, over blocks already fetched. It reads the same blocks as before,
each as often; a test holds it to that, read for read, over every case of
the differential's oracle. A pass also resumes where the last one stopped
instead of starting over, below any change since.

What this changes for you:

- A difference over a store with latency costs a round trip per batch
  rather than per block: on a large tree with 1 000 changes spread across
  it, 184 round trips instead of 1 053, for the same 1 054 reads. A
  one-change difference reads 8 blocks in 4 round trips instead of 7.
- Up to 16 reads of one difference are in flight at once against its
  store, and fetched blocks wait for the walk: at most 63 in that
  measurement.
- In memory, where there is no latency to save, a difference costs about
  the same: the spread one about a quarter less CPU, since a pass no
  longer rescans what the last one passed, and a small repeated one, as a
  subscription's poll takes, about 1% more instructions.
- Nothing else is observable: the same blocks, the same changes, the same
  errors and the same `MissingBlocks` outcomes. `Prefetch::Eager` is
  unchanged.

Why: the round trips of a difference were most of what a subscribed
replica waited on to catch up (#646), and they were serial only because
the walk read as it went, not because its reads depended on each other.

### A reset moves a branch forward only

[#686](https://github.com/dialog-db/dialog-db/pull/686), fixing [#682](https://github.com/dialog-db/dialog-db/issues/682)

`Branch::reset` now refuses a revision that does not hold the branch's
head, and the head does not move. A reset back to an older head, or onto
another writer's line that lacks this writer's commits, followed by a
commit, broke what every causal context assumes: that one origin's
revisions form one chain. Below the writer's last revision the commit
re-minted a version already used; above it, on a line without that
revision, the commit forked the origin. Either way a context holding one
revision claimed the other, and merges dropped writes as already seen. The
push guard from #681 could not catch the fork, since a context keeps one
watermark per origin: a push from such a head rewound its upstream.

What this changes for you:

- A reset to a revision that does not hold the branch's head fails with
  `PublishError::Rejected(Rejection::Unsupported { reason })`, whose
  reason says a reset only moves a branch forward. Nothing is written. To
  take another line's work, pull it; to go back to an older revision,
  start a new branch from it.
- A reset to a revision that holds the head, to the head's own tree, or
  on a branch with no head, lands as before.
- `Reset::perform` reads the archive to decide: one entry of the
  revision's context, from the memo, from its edition when the head is of
  the same origin, or one point read of its region, through the remote
  that serves the branch's blocks when they are not local. Its bounds grow
  from `Provider<Publish>` to the read bounds `Branch::history` takes plus
  `Provider<Publish>`. An environment that is a bare storage provider
  without those, such as `Volatile` on its own, can no longer reset a
  branch; a session or a peer can.
- A replica of an earlier release can still reset any way and sync with
  this one. The sync-base and push checks of #681 meet a head it moved
  back. A fork it makes (a takeover followed by a commit) is not detected
  by anything on this release: the forked revisions share an origin and
  no context can tell them apart. Upgrading every writer of a branch
  closes it.

Why: reset was documented as fast-forward bookkeeping, not rewind, and
nothing held it to that. A rewind costs other replicas their writes, and
no later check can tell the forked revisions apart.

### A sync uses its base only while both heads hold it

[#681](https://github.com/dialog-db/dialog-db/pull/681), fixing [#673](https://github.com/dialog-db/dialog-db/issues/673)

A pull merges three ways from the sync base it recorded with an upstream:
it takes everything the base holds as already in our head, and anything
the base holds that the upstream lacks as removed upstream. That holds
only while both heads hold the revision whose tree the base is, and three
paths broke it:

- Settling rival merges. When two writers each merge the same two heads
  at once, the greater keeps its head and recorded the lesser's tree as
  its base without holding it. Once the lesser had built on its merge,
  the greater side's next pull from it never imported the lesser merge's
  revision record. Every later head carried the hole: its ancestry walk
  failed, and a replica that never held the lesser merge could not
  vouch for the revisions a pull from it brought in.
- A reset that moves our head behind the base. The next pull returned at
  once when the upstream had not moved, or merged from the base and
  dropped what only the base held, facts included.
- A reset that moves the upstream's head behind the base. The merge read
  what the upstream never held as removed there, and removed it from
  ours.

A push made the same assumption. It took the upstream for a fast-forward
target whenever the upstream's tree was still the sync base, so a push
after a reset behind the base published the older head and moved the
upstream back, dropping what only the base held from every replica that
then pulled it.

A sync now records the revision whose tree the base is, in a
replica-local cell of its own beside the tracking cell, written after it.
A merge runs from the base only when both heads' contexts observe that
revision; otherwise it runs from no base, screened as a first contact is,
or adopts the upstream by root when the upstream has seen our head. When
our tree is the base, the revision is our head. Otherwise a revision that
is not known (no record, as after an upgrade, or a record of another tree,
as an older release moving the tracking cell or a crash between the two
writes leaves it) is not trusted: the pull merges from no base once, and
the sync it records writes the revision. The shortcut for an upstream
that has not moved checks the same of our head. A push lands only when
our head's context observes the upstream's head.

What this changes for you:

- A pull after a reset keeps every fact either side held, and so does a
  pull from an upstream someone else reset.
- A push from a head that does not hold the upstream's head, as after a
  reset behind the sync base, now fails with
  `PushError::NonFastForward { branch, expected, actual }`, the error a
  push to an upstream that moved already gets. `expected` and `actual`
  can name the same tree here, since the upstream has not moved; the
  local head is what is behind. Pull, then push again. A refused push
  writes nothing: no block is uploaded, the upstream's head is not
  published, and neither the head nor the sync records of the pushing
  branch change. Before, the push published the older head and moved the
  upstream back.
- What the released `0.3.0` does, measured with this release's tests run
  against it:
  - A reset followed by a pull loses the upstream's facts that only the
    base held: 40 of 40 runs.
  - A reset followed by a push rewinds the upstream to the older head:
    10 of 10 runs to a local upstream. The remote push has the same
    tree-only check, read from the code.
  - A replica that never resets is exposed too. When another writer
    resets the upstream behind the replica's sync base, the replica's next
    pull removes facts the base held: 10 of 10 runs. When another replica
    resets, pushes, commits and pushes again, a replica that never reset
    loses its own fact on its next pull: 10 of 10 runs. A deployment is
    exposed if any writer of a branch it syncs with has reset that branch,
    or pushed from a reset one.
  - This change restores nothing already lost. Heads minted by a losing
    merge lack those facts, and pulls carry the heads on. The facts may
    still be in the trees of older revisions wherever their blocks are
    kept, but nothing in dialog brings them back from there.
- Pulls that settle rival merges leave complete histories, and
  `PreparedPull::novelty` vouches for every revision such a head brings
  in. This was introduced by #623, which no release contains.
- A pull whose base either head no longer holds merges from no base. It
  reads both sides' differences in full, once, where a merge from the
  base read only what changed since it; the next pull runs from the new
  base again. The first merging pull after an upgrade does the same once
  per upstream, since a base recorded by an older release is not trusted,
  and so does a pull after a crash between a sync's two writes.
- A push that does not land as is checks that our head holds the
  upstream's: from the memo, from our edition when the upstream's head is
  of our origin, or with one point read of our tree's region. One case
  needs the network: our head reached the branch without its region being
  read (adopted by root, or reset to a head another replica made), the
  upstream's head is of another origin, and the memo does not have the
  answer. Then the read goes to the remote that serves our head's blocks,
  even for a push to a local upstream. When that remote cannot be read,
  as offline, the push fails with `PushError::Unreachable`, naming the
  remote, with a reason saying it could not confirm the branch's head
  holds the upstream's head, and writes nothing. Push again once the
  remote answers: the read is kept locally, and the push lands.
- Opening a branch reads one more replica-local cell, and a sync writes
  one more after the tracking cell. No API changes.

Why: the merge's assumption about its base was true on every path that
existed when the base was introduced, and nothing checked it once resets
and rival merges could break it. Checking it against the revision the
base stands for makes it hold however the heads moved.

### A subscription catching up re-derives its touched entities together

[#679](https://github.com/dialog-db/dialog-db/pull/679), for
[#646](https://github.com/dialog-db/dialog-db/issues/646)

A poll that finds facts changed inside a subscription's cover re-derives
each touched entity with a restricted evaluation of the query. Those
evaluations ran one after another, and on a cold replica each one is a
point read of its entity's leaf, so a catch-up cost a round trip per
touched entity. They now run sixteen at a time, and their rows are folded
into the result in entity order once all of them have finished.

What this changes for you:

- A subscription catching up on many changed entities waits on far fewer
  round trips. In the catch-up soak, a poll after a hundred one-fact
  commits goes from a chain of 80 dependent requests to 31, and from
  18.2 s to 7.5 s on the mobile profile (6.9 s to 2.8 s on broadband,
  18.2 s to 8.0 s in a browser). A poll after ten goes from a chain of 11
  to 3. The remaining chain is the cover diff's own walk.
- It reads the same blocks: request and byte counts are unchanged.
- Deltas, their order and the retained results are as before.
- Nothing stored or synced changes.

Why: the restricted evaluations are independent of one another. Each
reads its own entity's facts, and the cover they record is a union, so
nothing ordered them but the loop.

### A subscription poll that fails part-way no longer loses deltas

[#679](https://github.com/dialog-db/dialog-db/pull/679), for
[#646](https://github.com/dialog-db/dialog-db/issues/646)

When a poll's re-derivation of one touched entity failed, the poll
returned the error, but the entities re-derived before it had already
been applied to the subscription's retained results. The next poll found
those entities unchanged and never emitted their rows, so a subscriber
missed them for good. A failed poll now leaves the retained results as
they were, and the next poll re-derives every touched entity and emits
the whole delta.

What this changes for you:

- After a poll returns an error, the next successful poll emits every
  change since the last successful one, including those of the entities
  the failed poll had already re-derived.
- The error a failed poll returns is unchanged.

Why: a delta a subscriber never sees leaves its view wrong with nothing
to correct it.

### dialog-query no longer turns on dialog-common's test helpers

[#675](https://github.com/dialog-db/dialog-db/pull/675)

dialog-query asked for dialog-common's `helpers` feature in its normal
dependencies, though only its tests use anything behind it. Every build
of dialog-query, and of dialog-repository and dialog-peer above it, on
every target, therefore compiled dialog-common's test helpers: the
`helpers` module (service provisioning, `BenchData`), and the `test` and
`provider` macros. dialog-query now takes the feature as a
dev-dependency only, so a build of these crates no longer turns it on.

What this changes for you:

- A dependent that uses `dialog_common::helpers`, `dialog_common::test`
  or `dialog_common::provider` without enabling `dialog-common/helpers`
  itself, and so got them only because dialog-query turned the feature
  on, no longer compiles. Enable the feature where you use them,
  usually in `[dev-dependencies]`:
  `dialog-common = { version = "...", features = ["helpers"] }`.
- Nothing else moves. dialog-query's, dialog-repository's and
  dialog-peer's own APIs are unchanged, and nothing stored or synced
  changes.

Why: test scaffolding has no place in a production build, and a
feature turned on through a normal dependency reaches every build above
it. #670 puts a seeded source of identifiers for the soak harness behind
that same feature, which makes keeping it out of production builds a
requirement rather than a cleanup.

### A query pinned to one value of a single-valued attribute reads a cold replica in parallel

[#658](https://github.com/dialog-db/dialog-db/pull/658), for
[#650](https://github.com/dialog-db/dialog-db/issues/650)

A premise that binds a single-valued attribute to a value without binding
the entity, such as `status = "open"`, scans the value index for
candidates and checks each candidate against its entity's stored values,
so that an entity whose winning value is no longer `"open"` is not
yielded. Each check now goes out as a preload hint while an earlier one
is awaited. The other premises' entity probes were already pipelined this
way. The checks were not, and they were the chain. On a cold replica each
check fetched one entity leaf, and the probes of the next premise landed
in that same leaf, so the query cost one round trip per entity leaf it
touched.

What this changes for you:

- A cold query driven by such a premise takes about a fifth of the round
  trips it did. The soak's `rule` phase, a rule whose body pins
  `status = "open"`, goes from a chain of 98 to 18 dependent requests and
  from 8.4 s to 1.7 s on the broadband profile (22.6 s to 4.7 s on mobile,
  22.4 s to 6.4 s in a browser). The `filtered` phase, a concept query
  pinning a status, goes from 8.8 s to 1.7 s on broadband.
- It fetches no more blocks: the hinted leaves are the ones the checks
  read anyway. Request and byte counts are unchanged.
- Results are unchanged, and so is the plan. A replica whose speculation
  budget is zero (`FetchBudget::ZERO`) behaves as before.
- Nothing stored or synced changes.

Why: #536 made a rule body plan with its pinned values bound, which is
correct and reads fewer blocks. It also made the value-pinned premise the
driver, and that premise's checks were the one per-row read in the
nested-loop join that nothing read ahead.

### A difference prunes a subtree both sides route the same buffered ops to

[#669](https://github.com/dialog-db/dialog-db/pull/669), fixing [#662](https://github.com/dialog-db/dialog-db/issues/662) and [#648](https://github.com/dialog-db/dialog-db/issues/648)

Comparing two trees prunes a subtree both sides hold by the same hash
only when nothing a buffer above it routes down could differ. Before
this, any subtree with buffered ops routed down to it was expanded, even
when both sides routed it the same ops. A root's buffer reaches every
child, and a root cannot settle against the other side's root on its
buffers alone, so a commit that added one fact made the walk read
nearly every leaf of a deep tree. Now a pair with the same hash and the
same winning routed ops prunes unread. A pair with the same hash and
different routed ops is expanded one level, so the ops reach the
children they belong to and the shared children prune there.

What this changes for you:

- A push after a commit on another writer's head no longer uploads the
  whole tree. On a deep tree a replica that took turns with another
  writer could download every leaf during induction's catch-up on its
  second commit, and then upload every one of them on every later push
  (about 350 nodes and 16 MB per push in the soak's alternating-writers
  scenario, in about one run in five). Each push is the new root again.
- A joiner's first push no longer re-uploads index nodes it held of the
  adopted head (#648).
- `TreeDifference`, `differentiate` and `differentiate_within` read and
  report fewer blocks over trees whose buffers hold the same ops on both
  sides. The changes they report are the same.

Why: a wrong prune would silently drop a change in sync, so pruning
stayed conservative wherever buffered ops were pending. Ops routed from
an ancestor are not in a subtree's hash, but a subtree's whole
contribution to the difference is its hash and the winning ops routed
to it, so when both match nothing beneath can differ. The argument is
in `SparseTree::prune`'s documentation, and a property test checks it
against a materialized difference over random trees with divergent
flush histories.

### A transaction's writes to a cell under `last` commit as they would one by one

[#647](https://github.com/dialog-db/dialog-db/pull/647)

A transaction that wrote one cell under `all` and under a choosing pick
(`last`, `max`, `min`, `top`) could commit something other than what the
same writes committed one per transaction leave. The commit squashes a
cell's writes value by value: a repeated assertion of a value is
dropped, and an assertion a later retraction of its value cancels is
dropped. Neither holds once a choosing write stands between them, since
it succeeds whichever claim its pick elects at its place in the order,
and that may be the value an earlier write of the transaction asserted.
Two shapes lost a fact:

- Assert a value under `all`, write the cell under `last`, assert the
  value again: the commit dropped the repeat, and the `last` write
  retired the value. The value was missing.
- Assert a value under `all`, write the cell under `last`, retract the
  value: the commit dropped the assertion against the retraction, so
  the `last` write succeeded the claim the line held instead. That
  claim was missing.

A cell holding a choosing write is now committed as written, every
write in order, and the tree settles it as it settles one commit per
write. A cell with no choosing write is squashed as before.

What this changes for you:

- A transaction of either shape commits what the same writes committed
  one by one leave: a read after the commit returns the value asserted
  again, or the line's claim, beside the `last` value.
- A transaction that asserts and then retracts a value in a cell it
  also writes under a choosing pick now leaves a retraction record for
  that value in the history, where it left none. The facts are the
  same.
- No API moves.

Why: the squash reasons about one value at a time, which is exact only
while writes of different values commute. A choosing write is the one
write that does not commute with the others, so a cell holding one has
to reach the tree in the order it was written.

### A chain canonicalized at a link that changes nothing is canonical

[#649](https://github.com/dialog-db/dialog-db/pull/649)

A staged chain canonicalizes at its last link. When that link was an
amend that changed nothing (a retract of a fact the chain never held, a
re-assert of a value that stands), the commit returned the tip as it
was, before the seal that flushes the buffers, so the canonicalize was
dropped and the chain kept whatever buffered form its earlier links
left. An amend asked to canonicalize now canonicalizes its tip whether
or not the link changed anything, and reports the tip unchanged only
when it was already canonical.

What this changes for you:

- Two replicas holding the same facts no longer disagree on the root
  because one chain ended in a no-op amend. Before, each named a root
  that depended on where its earlier links flushed, so a sync saw
  divergence where there was none and a merge did work that found
  nothing.
- `TransactionCommit::amend` with `canonicalize` on a link that changes
  nothing returns a batch whose tree is canonical; it returned the
  buffered tip before. A commit that is not an amend keeps its rule: a
  batch that changes nothing mints nothing and canonicalizes nothing,
  unless `allow_empty` asks for a revision.
- A commit that changes nothing hands its live spine back to the line,
  so the next commit appends to it instead of opening the root frame
  from bytes again. The bytes written are the same either way.
  `BufferedBatch::release` and `SpineSlot::root` are new.

Why: the canonicalize request was checked after the no-op return. An
amend is the one commit that rewrites a revision's tree in place, so it
has a tree to canonicalize even when the link itself wrote nothing.

#### Compatibility and migration

- Nothing stored or synced changes for the corpus rename: keys, node
  encodings, spilled-value references, revisions and the wire format are
  the same bytes, nothing is rewritten on upgrade, and an older release
  reads everything this one writes. Only Rust names and the crate graph
  move.
- `Download::QueriedRanges` changes nothing stored or synced: the blocks
  it fetches are blocks the replica would have fetched at its next read
  of them, nothing is rewritten on upgrade, and releases on either side
  read what the other writes. Nothing for an embedder to run.
- Bounding the caches changes nothing stored or synced: caches hold only
  what a replica can read again, nothing is rewritten on upgrade, and
  releases on either side read what the other writes. Nothing for an
  embedder to run.
- Nothing stored or synced changes for trees reading through `Load`:
  blocks, nodes, spilled values and their hashes are the same bytes,
  `Get`'s wire shape is unchanged, nothing is rewritten on upgrade, and an
  older release reads everything this one writes. `Load` is a local
  command and never crosses the wire; what changes is which reads may
  hydrate a block the replica lacks (any read of a repository an open
  branch of which tracks a remote), and that a hydrated block is written
  back locally as before. Nothing for an embedder to run.
- A reset that moves a branch forward only changes nothing stored or
  synced, and rewrites nothing. Heads an earlier release forked by a
  takeover and a commit keep their fork; nothing finds or repairs it.
- A branch has a new replica-local cell, `bases`, never synced, that
  records each sync base's revision. A replica upgraded from an older
  release has none, and its bases are not merged from: the first pull
  that has to merge runs from no base and writes the record. That covers
  a bad base an older release left behind, by a reset, a kept rival or
  `upgrade`. An older release ignores the cell, and the tracking cell's
  encoding is unchanged; when an older release moves the tracking cell
  the record no longer matches it and is not used. Nothing is rewritten
  on upgrade, and nothing an embedder runs is needed.
- Nothing repairs what earlier pulls did. Heads already minted with a
  hole by a build of `main` since #623 keep it, and facts a replica lost
  to a pull after a reset are not restored by this change. Nothing synced
  changes shape: an older release pulls everything this one mints.
- Nothing a replica stores or syncs changes shape, and nothing is
  rewritten. A head an older release published from such a chain names
  a buffered tree; it stays valid, reads, pulls and merges as before,
  and converges on the canonical root at its next canonicalizing commit
  that mints a revision (one that changes a fact, or one with
  `allow_empty`). Nothing an embedder has to run.

### A branch's live spine is held only while a handle of it is open

[#637](https://github.com/dialog-db/dialog-db/pull/637)

The environment's caches kept, per repository and branch name, the live
buffered spine a commit leaves for the next commit on that branch, for the
life of the environment. A repository visited by many writers once each
(open, commit, drop the handle) held a spine per writer: about 146 kB each
at 10 000 writers in the crowd scenario of
[#633](https://github.com/dialog-db/dialog-db/issues/633), 1.5 GB in all.
The caches now hold the spine weakly: every open handle of one branch, and
every snapshot or transaction taken from one, shares it, and the last of
them dropped takes it with it.

What this changes for you:

- A branch opened again after its last handle went decodes its root once,
  on its first commit, from the node cache (no block is read). In the
  crowd scenario that first commit costs about 0.3 ms more than one with
  the spine held at 1 000 writers (1.1 ms against 0.8 ms) and is within
  the run-to-run noise at 10 000 (2.7 to 3.0 ms either way); every later
  commit through the handle is as before, and a handle kept open commits
  as before.
- What an environment retains no longer grows with the number of branches
  written through it once. The crowd scenario's peak at 10 000 writers
  goes from 1.75 GB to 0.29 GB live (1.95 GB to 0.42 GB resident).
- `HeldCaches::release` and `clear` behave as before. `SpineSlot` gains
  `downgrade`, `shares` and `is_held`, and `WeakSpineSlot` is new.
- Nothing stored is rewritten and no format changes: the persisted bytes
  are identical with or without the spine.

Why: the spine is an accelerator for the next commit on the branch, and
only a handle can make that commit; keeping it after the handle went
bought nothing and cost a root per writer.

### A persisted node's bytes can be attributed to what they carry

[#634](https://github.com/dialog-db/dialog-db/pull/634), for
[#632](https://github.com/dialog-db/dialog-db/issues/632)

`PersistentNode::attribution` reports where a node frame's bytes go: the
prelude, the link hashes, separators and scales of an index, and its
entries' keys, values and polarity, next to the weight the pacing metered
for the same entries and a share per key layout. The perf matrix gains
`first-query`: a replica that adopted a head by root, two hundred
one-commit origins deep, runs one point query, and the gate holds the
blocks and bytes it reads and the root frame's bytes.

What this changes for you:

- A new method on `PersistentNode` and a new `FrameAttribution` type in
  `dialog-search-tree`. Nothing existing moves.
- Nothing stored or synced changes.

Why: the root a replica's first query reads carries the write buffer the
history's commits left in it, and choosing a policy for what a published
root may cost needs the bytes attributed and the shape in the gate first.

### A head carries no causal context, and its tree carries all of it

[#625](https://github.com/dialog-db/dialog-db/pull/625)

A head no longer carries one watermark per origin its history ever
included. It carries its own, as its edition and a signed count of its
own origin's revisions, and a signed marker saying its tree holds the
rest, in a new context region with one entry per origin. The region is
exact for every origin except the head's own: that entry is where the
writer's chain of commits began, and a reader brings it up to the head
from the head's edition, one edition and one revision per commit, and
refuses a region that does not bring it to the count the head signed. A
commit on a head of its own origin therefore writes nothing to the
region and reads nothing of it. A commit on another origin's head writes
two entries: that origin's exact watermark, which the head carries, and
its own, one past what the head holds of its origin, which one point read
of the region answers: a root-to-leaf path, a subset of the whole region.
A commit never reads the region whole. A merge writes its own entry and every entry its two sides hold
differently. A pull answers whether our head has seen the upstream's
from our head's record when the record names it as a parent or skip
link, so a pull right after a commit on the upstream's head reads
nothing. A pull decides "nothing new" from its own context and the
upstream head's version, and a fast-forward from the two heads' editions
when they share an origin, at zero reads; a fast-forward across origins
reads the upstream's region, its root and a leaf.

What this changes for you:

- `Revision` has fields `region: bool` and `count: Option<u64>`. A
  struct literal must name them. `Revision::publish_region` takes the
  head's own count, and `Revision::own_watermark` gives the head's own
  watermark when it carries its count.
- `Revision::context` is `None` on every head this release mints. A
  head's whole context is in its branch's memo
  (`Branch::contexts().cached(&version)`), in its tree
  (`history::context_in`, then `Revision::context_from_region`), or
  derived by the ancestry walk (`history::context_of`).
  `Revision::inline_context` gives the context of a head minted before
  the region, and `Revision::publish_region` marks a head as carrying
  its context in its tree.
- `Context::get`, `Context::set` and `FromIterator<(Origin, Watermark)>`
  are new, and so is `Watermark::check`, the per-entry bounds
  `Revision::verify` applies. `EDITION_CEILING` moves to
  `dialog_capability::history` and stays exported from `identity`.
- `ContextCache` holds `Arc<Context>`. `cached` and `context_of` return
  one, and `insert` takes anything that converts into one.
  `merge::screen_data` takes anything that converts into an
  `Arc<Context>`.
- The context region is `CONTEXT_KEY_TAG` (6). It comes with
  `context_key`, `context_entry`, `read_context_entry`,
  `merge::context_scope`, `merge::without_context`,
  `history::context_in`, `history::region_writes` and `history::Held`.
- `Revision::verify` refuses a head that claims both an inline context
  and a context region, and a count on a head without a region, a zero
  count, or a count above what the head's edition leaves room for. A
  pull refuses a head whose region holds no entry for the head's own
  origin, holds it above the head, holds an entry outside the watermark
  bounds, or derives a count other than the one the head signed, as
  `DialogCorpusError::InvalidKey`, and keeps its own head.
- A commit no longer remembers the new head's whole context in the
  branch's memo unless it had the parent's there already. The first
  reader that needs the whole map reads the region then.
- `Revision::precedence` compares two heads by their contexts only when
  both carry them inline, and orders two heads of one origin exactly by
  edition. Any other pair is answered from editions without reading
  either tree: a higher edition is `After`, and one at or below the
  other's is `Concurrent`, never `Before`. A cell watcher therefore
  fetches a lower head of another origin (a branch reset to it, or one
  delivered out of order) instead of dropping it as stale.
- A fast-forward between heads of different origins now reads the
  upstream's root and the path to its region's leaf (3 blocks in the
  tests); before, it read nothing.
- `diagnose --head <revision cell>` prints the origin count and recent
  writers of a head that carries its context inline, and
  `ContextProfile::from_region` profiles one from its region's entries.

Why: every head carried every origin that ever wrote, so every commit
signed and every pull and cell watcher moved a head whose size grew with
the number of writers, dead ones included. Atop 10 000 origins a head
was 390 kB, its signed payload 480 kB, and a one-fact commit took
5.4 ms, against 0.25 ms atop 10. Now the head is under 300 bytes and its
signed payload 183 bytes at any origin count, and a one-fact commit atop
10 000 origins costs what one atop a history as deep from one origin
does (2.4 ms against 2.3 ms). A dead origin costs its 48
region bytes once per replica instead of once per operation.

### A differential over a buffer holding several ops for one key compares the newest op on each side

[#630](https://github.com/dialog-db/dialog-db/pull/630)

A hitchhiker link buffer can hold several ops for one key, newest last, and
every reader resolves the key to its newest op. The settled-node path of the
differential (two nodes with identical children that differ only in their
buffers) subtracted one side's ops from the other's op by op, so an op a side
had already superseded could cancel the other side's live value, or surface
as a change neither side could observe. Both sides are now reduced to their
newest op per key before the comparison.

What this changes for you:

- A differential between two trees sharing a settled node, where one side
  buffers several ops for a key below the node's lowest separator, now
  reports what each side resolves the key to. Before, with ours holding
  `k: v1` then `k: v2` and theirs holding `k: v1`, the diff reported ours'
  `v2` and nothing for theirs; it now reports ours' `v2` against theirs'
  `v1`. With theirs holding `k: v2`, the diff reported a spurious change on
  ours' `v1`; it now reports no change at `k`.
- Nothing stored is rewritten and no format changes: frames holding several
  ops for one key read as before, and an older release reads what this one
  writes.

Why: a pull of one head against another where the newer head re-asserted a
key it still buffered could miss the older head's value or carry a change
that was not one. A buffer's older ops are history, not state, and the
differential has to compare state against state, as every other reader of a
buffer does.

### A dialog peer can be reached over iroh

[#521](https://github.com/dialog-db/dialog-db/pull/521)

`dialog-peer-iroh` reaches another dialog process, on the same machine or
another one, over iroh QUIC. A peer is named by the `did:key` of its iroh
endpoint, so its address is `did:key:...?route=...` and the routes are hints:
a peer that moves is still the same peer. An iroh address is one more address
a contact can be reached at, carried in `dialog-network`'s table beside the
other sites, and the same code runs in a browser over a WebRTC data channel.
Each request is a signed invocation; the payload is bound to it, and carried
blocks are checked by blake3 rather than trusted by address. Blobs stream over
a channel of their own. A watch is refused as unsupported until a peer streams
one; the cell is read by resolving it again.

What this changes for you:

- `dialog-peer-iroh` is new: the site (`Iroh`), the responder that performs a
  verified invocation against a local store, and the transport.
- `peer::Hello` (`/use/get/dialog/peer/hello`) asks a peer which peer it is
  and which key signs for it, and `peer::Spaces`
  (`/use/get/dialog/peer/spaces`) which spaces it keeps, under the names it
  recorded them by. `Peer` answers both.
- `MemoryError`, `ArchiveError`, `BlobError` and `memory::Current` are
  `Serialize` and `Deserialize`, so a peer that performs an effect can send
  its refusal back.

Why: every replica holder is a peer, but none of the existing transports
could reach one that is another dialog process, since they all assume a
server already reachable at a URL.

### A snapshot can be exported without the bytes of its assets

[#623](https://github.com/dialog-db/dialog-db/pull/623)

`SnapshotExport::without_assets` exports a revision's tree and its spilled
values, and leaves the bytes of each asset the revision records where they
are. A replica that adopts the head this way holds every fact of it, such as
`dialog.asset/size`, and none of the bytes those facts name; they come on the
first read of the asset, as they do after a pull.

What this changes for you:

- `SnapshotExport::without_assets()` is new. An export that does not call it
  carries the asset bytes, as before.

Why: a member who joins a space should not download every file in it before
they can read its facts. With the bytes left behind, a file they never open
costs them its facts alone.

### A pull settles rival merges of simultaneous mutual pulls on one head

[#623](https://github.com/dialog-db/dialog-db/pull/623)

Two replicas that pull each other at the same moment each mint a merge of the
same history. The two merges hold the same data and differ only by their own
revision records, so a merge of them matched neither tree, and every round of
simultaneous pulls minted two new merges. A pull now recognises the pair, from
the two heads' contexts and revision records, and both sides settle on the
merge with the greater version: that side keeps its head and advances only its
sync base, and the other adopts that head by its root, as a fast-forward does.

What this changes for you:

- Two replicas that keep pulling each other reach the same head and stop
  minting merges, where they used to mint two more every round.
- The side that adopts the greater merge drops its own, which recorded no
  claim of its own.

Why: two replicas that sync with each other continuously, as two devices of
one person do, pulled at the same moment often enough to grow their history
without bound.

### A batch lands when another writer moved the induction watermark

[#623](https://github.com/dialog-db/dialog-db/pull/623)

Publishing a batch advances the branch's induction watermark to the head it
landed. A second writer over the same replica, another process over the same
store, can land a commit on that head and advance the watermark in the window
before this write. The publish then failed with `VersionMismatch`, though its
head had landed. It now reads the watermark again and moves it to its head
only while the watermark still stands at the head the batch built on, or at
none; anywhere else, another writer moved it on.

What this changes for you:

- `TransactionPublish::perform` no longer fails with
  `PublishError::VersionMismatch` from the induction watermark after its head
  has landed. It still fails that way when the head itself moved.

Why: a commit whose head has landed is in the branch. Failing it told the
caller to retry a write that had already happened.

### A peer or a session names the replica it writes as

[#623](https://github.com/dialog-db/dialog-db/pull/623)

`PeerBuilder::profile` names the profile a peer writes as, where it is not the
peer's home. A session of a persona that writes into a space of that persona
names the persona, and so does a device of a persona whose state branch is in
a repository of the device. The profile names the replica, not only the
authority: a branch the peer opens keeps its metadata, its tracking and its
records in the registry under the profile's replica of the repository.

What this changes for you:

- `PeerBuilder::profile(did)` is new. A peer that does not call it writes as
  its home, as before.
- With a profile, each revision names the profile as its authority, and every
  entity the peer writes derives from the profile's replica, not the home's.

Why: a Wasm component that holds a space is given that space as the one
storage it can reach, yet it writes on behalf of a persona. Its revisions have
to say so, and what it records about its replica has to be the persona's.

### A prepared pull names what it merges and what is new in it

[#623](https://github.com/dialog-db/dialog-db/pull/623)

A caller can now decide, before it commits a pull, whether each revision the
pull brings in may enter the branch: checking the authority each one names,
say. `PreparedPull::upstream` is the head the prepare fetched and verified,
the one revision the commit adopts. `PreparedPull::novelty` lists the
revisions the commit brings into the branch, ancestors first, each as a
`NewRevision` with its record and its claims. `history::novelty` in
`dialog-corpus` is the walk underneath it.

What this changes for you:

- `PreparedPull::upstream`, `PreparedPull::novelty`, `NewRevision` and
  `dialog_corpus::history::novelty` are new.
- A revision whose record neither the upstream nor the branch holds fails
  the walk with `DialogCorpusError::IncompleteHistory`, where
  `history::log` skips it.
- A no-op pull names nothing new, and neither does one whose commit keeps the
  branch's head and advances only its sync base.

Why: a pull adopted whatever its peer's head held. A replica that checks what
it accepts needs to see every revision a pull would bring in before it lands.

### A replica can be reached at addresses its embedder keeps

[#623](https://github.com/dialog-db/dialog-db/pull/623)

`ConnectedReplica::reached_at` reaches a replica held at a peer at addresses
the embedder gives it, without recording them as contacts of the host.

What this changes for you:

- `ConnectedReplica::reached_at(host, peer, addresses, subject)` is new. Its
  `addresses` must not be empty.

Why: an embedder that keeps the addresses of its peers itself, as a daemon
that dials its peers does, had to record each one as a contact before it
could reach the replica.

### Dialog builds for WASI, and a key held outside the process can sign

[#594](https://github.com/dialog-db/dialog-db/pull/594)

`dialog-repository`, `dialog-query` and `dialog-peer` build for
`wasm32-wasip2` and `wasm32-wasip3`. The browser paths of `dialog-common`,
`dialog-ucan-core` and `dialog-credentials` are for `wasm32-unknown-unknown`
alone, and a WASI build takes the native ones. `ed25519::external` adds a
signing key that holds only a public key and an `ExternalSign`, which gets
the bytes to sign and gives back the signature.

What this changes for you:

- `Ed25519SigningKey` has a third variant, `External`, and
  `Ed25519KeyError` has a variant of the same name. A match that names every
  variant of either no longer compiles.
- An external key cannot be exported (`Ed25519KeyError::External`), and has
  no agreement key, so `agreement_key` gives `AgreementKeyUnavailable` and
  `Secret::derive` and `Secret::reveal` give
  `SecretError::AgreementKeyUnavailable`.
- `dialog_common::random::fill` is where the workspace gets random bytes.
  On WASI it uses `getrandom` 0.4, and `getrandom` 0.2 everywhere else.
- `Invocation::issued_at` reads the time an invocation was issued.
- On WASI the file system provider compiles and refuses every call.

Why: a Wasm component reaches storage, keys and the clock through the host
that runs it. It cannot use the browser's APIs, and its memory is not where
its host wants a signing key kept.

### A write is safe to send again, and a refused one says what to do next

[#564](https://github.com/dialog-db/dialog-db/pull/564)

A refused request now tells its caller what to do next: try again (and when), rebase onto what the cell holds and write again, or get authority and try again. The access service's writes are made safe to send again to get there. A write that would leave the cell as it already is succeeds, so a publish whose answer was lost, sent again, finds its own write instead of a conflict. A write that does conflict says what the cell holds, so the writer rebases without a read of its own. The cell's state is told only after the write's authority verified; a write without authority is refused for that, and learns nothing of the cell.

What this changes for you:

- `MemoryError::VersionMismatch`, `PublishError::VersionMismatch`, `RetractError::VersionMismatch` and `ResolveError::VersionMismatch` say what the cell holds instead as `current: Current`: `Current::Holds(edition)`, `Current::Empty`, or `Current::Unknown` when whoever refused the write did not say, as S3 does not. Their `actual: Option<Version>` is gone; `Current::version()` reads the version where the cell holds one. Code that builds the variant names `current`; a match with `..` is unaffected. The volatile, filesystem, IndexedDB and loopback stores answer `Holds` or `Empty`, which they used to report as a bare `actual` version.
- A remote branch's `publish` that the remote took, but whose local record was written first through another handle, is refused as `PublishRemoteBranchError::Publish(VersionMismatch)` with `Current::Unknown`: what the local record holds is not the remote's head, and `push` would otherwise rebase onto it.
- The access service answers a publish of the content the cell already holds as done, with the version the cell is at, and a retract of an empty cell as done, whatever version either named.
- It answers any other conflict with `412`, the cell's state as a DAG-CBOR `CellState` in the body (`CELL_STATE_MEDIA_TYPE`) and its version as the `ETag`. Over the socket the `Answer` carries the same. The client decodes it into `current`; an older service's bare `412` decodes as `Current::Unknown`.
- `push` onto a remote that answers with its head reports `PushError::NonFastForward` naming that head and records it, verified, so `pull().assuming_upstream()` rebases onto it without asking the remote again.
- `Rejection::Unavailable` has a `retry_after: Option<u64>` (seconds), left out of the encoding when unsaid, so an older peer reads and writes it as before; `Rejection::unavailable(reason)` builds one without it and `Rejection::retry_after()` reads it. The service sends it as `Retry-After` too (`Response::retry_after`).
- An invocation sent and not answered is `Rejection::Unavailable`, a write included, where a write was `Rejection::Unclassified` before.

Why: an error is useful when the caller can act on it. A bare conflict made every writer read the cell before it could rebase, a lost answer left it unable to tell a write that landed from one that did not, and a retryable refusal gave no hint of when.

### A socket that fails is noticed, and every way it fails has an answer a caller can act on

[#564](https://github.com/dialog-db/dialog-db/pull/564)

A space's resolves, publishes and watches share one long-lived socket, and a connection that went quiet (a laptop that slept, a changed network, a NAT that forgot the mapping) was never closed: the platform reports nothing until something it sends fails, and a watch sends nothing. Every invocation for the space then waited for ever, and replication went on counting the upstream as watched while it no longer followed it. The client now keeps each connection honest, says what became of a frame it sent, and recovers from each way a connection fails: silent, cut, lost in flight, refused, behind, or judged by a clock that differs. The socket module's documentation lists each case and what the client does.

What this changes for you:

- The client pings each connection with a text `ping` every `Liveness::heartbeat` (15 seconds by default) and closes one it heard nothing from for `Liveness::deadline` (45 seconds). A service answers `ping` with `pong`, as text, without reading it as a frame; a hibernating Durable Object answers it through `setWebSocketAutoResponse` with the pair `PING`, `PONG`. A service that does not answer has its connections closed every deadline.
- An invocation waits for its answer, and a watch for its first state, no longer than `Liveness::answer` (30 seconds). A connection nothing waits on is closed after `Liveness::idle` (a minute). Invocations that race to open a socket share one connection. `UcanSite::with_liveness` and `Sockets::with_liveness` set these.
- A socket that cannot be opened holds off every space's socket at the same service, not only its own, for 30 seconds, doubling with each failure in a row up to 15 minutes and starting over once one opens. A service without sockets, such as a deployment whose upgrade answers 404, is tried a few times an hour, not once per space every 30 seconds.
- A frame that never left goes as a request instead, whatever it does. An invocation sent and not answered, over the socket or as a request, is `Rejection::Unavailable`, so a peer's next address is tried with a fresh invocation. That is safe for a write too (see the entry on writes that are safe to send again).
- A push whose publish landed but whose answer was lost no longer leaves every later push refused as `PushError::NonFastForward`: a remote already at the head being pushed is taken as the push having landed, and the sync point is recorded.
- A watch's states are coalesced to the newest while it is not read, instead of queued per change; the last state before it ends is never dropped.
- A watch the service refuses is an error of the `Watch` effect, not the first item of its `EditionSource`.
- `branch.replicate()` begins an ended watch, or one it could not begin, again on its own, after a wait that doubles each time in a row from a second up to a minute and starts over once a watch delivers. Host ticks do not jump the wait.
- An effect at a peer refused as `AuthorizeError::Stale` is sent once more, stamped by the clock the refusal said the service read. `dialog_ucan::clock` keeps the difference per subject, for the process.
- `Access::with_clock` sets the clock a service judges by (the system's by default), and `Access::now` reads it; hand it to `Access::recheck` and `Session::deliver`. `Presented::record` is told the time by it, as a third argument.
- `dialog_common::time::sleep` waits on native and in a browser or worker alike, and `dialog_common::time::Instant` is a clock that only moves forward.
- Test helpers: `UcanSettings::fault` has the loopback service's socket fail on cue (`Fault::Cut`, `Fault::Freeze`, `Fault::LosePublish`), and `UcanSettings::clock_offset` sets its clock ahead or behind.

Why: a socket is worth having only if a caller can rely on it as it relies on a request: an answer or an error it can act on, in bounded time. Each of these was a way it gave neither.

### A peer is reached where its DID document says

[#564](https://github.com/dialog-db/dialog-db/pull/564)

A peer is named by its DID, and where it is reached is what its DID document says. `contact(did).discover()` reads the services the document names and records the access service among them as the peer's address. `Discover` asks for a DID's services beside `Resolve`: a `did:key` names none, and a `did:web` or `did:plc` document is fetched as resolving it is. A document naming a DID other than the one asked for is refused, so a host serving a DID's URL cannot point it at services the DID never named.

What this changes for you:

- `PeerBuilder::discovery` sets how a peer discovers; by default it reads `did:key` locally and `did:web` and `did:plc` over the network, and a peer's sessions discover as it does.
- `UcanAddress::from_services` reads the endpoint from a `UcanAccessService` entry (`ACCESS_SERVICE`) and the socket from a `UcanAccessSocket` entry (`ACCESS_SOCKET`). A service that wants to be discovered adds both to its DID document.
- Discovering a contact again follows its document: an address recorded at the same endpoint that the document no longer names, such as a socket the service stopped announcing, is taken back. Addresses at other services are kept. `DiscoverContact::perform` asks its environment for `Provider<RemoveAddress>` as well, which a `Peer` provides.
- A loopback `did:web` (`localhost`, `*.localhost`, `127.x.x.x`) resolves over plain `http`, so a service on the same machine is discovered as a deployed one is. Every other host stays on `https`.
- `dialog-peer` and `dialog-repository` now depend on `dialog-did-web`.

Why: a peer's address was configured by hand beside its DID, so the two could disagree, and a service could not move or add a socket without every client being told. The DID document already answers who a peer is; it now answers where too.

### Invocations say when they were issued, and a service refuses one that is stale or presented again

[#564](https://github.com/dialog-db/dialog-db/pull/564)

The client stamps every invocation with `iat`. `Access::admit` refuses one issued longer ago than its freshness window (`FRESHNESS`, 30 seconds by default) and one presented before, remembering the invocations it accepted while they are fresh through a `Presented` record. Over a socket an invocation must say when it was issued; over HTTP one that does not is still served, so a client that predates the stamp keeps working.

What this changes for you:

- New refusals: `AuthorizeError::Stale { issued_at, at }`, which carries the service's time, and `AuthorizeError::Replayed { invocation }`. Both answer 401. An exhaustive match on `AuthorizeError` needs the arms.
- The record of accepted invocations (`RecentInvocations` by default) is per `Access`, so per process. An embedder that builds a service per request, or runs several processes, shares one record across them with `Access::with_presented`, or a replay can reach another instance; `Access::with_freshness` sets the window.
- A test that presents one invocation twice to one `Access` now gets `Replayed`.
- A client whose clock is more than the window off from the service's has its first invocation for a subject refused as `Stale`; the refusal says what the service's clock read, and the effect is sent once more stamped by it (see the entry on the socket's failures).

Why: an invocation carried no time of its own and the service never asked how old one was, so a captured invocation held for as long as its delegations did.

### A cell can be watched, and a cell's invocations go over the access service's socket

[#564](https://github.com/dialog-db/dialog-db/pull/564)

`memory::Watch` follows a cell: what it holds when the watch begins, then each state it takes, through an `EditionSource` that may skip intermediate states but never the last one. Its command is `/use/get/memory/cell/watch`, so any grant to read the cell covers it. A UCAN address can name a `socket`: one connection per space, shared by every watch of a cell in that space, which also carries the cell's resolves, publishes and retracts; blocks and blobs stay requests. Every frame on it is an invocation verified on its own, and the service checks a running watch's authority again, as of the time it delivers to it, once it was last found to hold more than an interval before (`Access::recheck`). `Session` is the service's side of one connection and holds no connection or clock, so a loopback server and a service that hibernates between messages serve it alike.

What this changes for you:

- `UcanAddress` has a public `socket` field (`with_socket`, `socket`), so a struct literal of it needs the field; `UcanServiceAddress` (test helper) has one too.
- A site that cannot follow a cell answers a watch with the new `Rejection::Unsupported`, as S3, the filesystem remote and a UCAN address with no socket do; the caller resolves the cell again instead. An exhaustive match on `Rejection` needs the arm.
- `Access::subscribe` accepts a watch and answers a `Subscription`, plain data a service can store, which names the cell it follows (`subject`, `space`, `cell`).
- A frame the client sends names itself (`Request::Invoke { id, .. }`) and the service's answer echoes it, so the answer to an invocation the service could not read reaches whoever sent it. An answer that names neither a frame nor an invocation fails every invocation awaiting an answer on that connection.
- Dropping a site closes its sockets, so the service ends their sessions.
- `dialog-remote-ucan` now depends on tokio and tokio-tungstenite natively and on web-sys's `WebSocket` on wasm.

Why: a host learned that a remote branch moved only by resolving its head again and again, each time a verified invocation the service pays for. A watch tells it when the head moves.

### A branch keeps itself in step with its upstreams, and a head delivered late is not recorded over a newer one

[#564](https://github.com/dialog-db/dialog-db/pull/564)

`Branch::replicate()` keeps a branch in step with its upstreams for as long as its stream is read: an upstream that can be watched is followed and each head it delivers is pulled with no round trip of its own, and one that cannot be watched, a local one, or one whose watch ended is pulled from on the ticks the host passes to `every`, which also begin an ended watch again and push the branch's own commits. A bare replicate pulls and pushes; `pull()` or `push()` does one. `ConnectedBranch::watch` follows a remote branch's head, and `ConnectedBranch::observe` records a delivered head only when it is ahead of the one recorded, read from the local record at that moment, since more than one handle records heads of one remote branch.

What this changes for you:

- `Revision::precedence` orders two heads of one branch (`Precedence::{Same, Before, After, Concurrent}`) by their signed contexts, falling back to editions for heads without one.
- `ConnectedBranch::observe` answers `Observation::{Advanced, Stale, Diverged}`; a diverged head is not recorded, and `replicate` asks the remote where it stands instead.
- `Pull::assuming_upstream` merges the head recorded for a remote upstream without asking the remote.
- `replicate` answers `Replicated::Pulled`, `Replicated::Pushed`, and `Replicated::Unwatched(target)` when a watch ends. An ended watch is begun again on its own after a wait that doubles each time in a row, up to a minute; without `every`, an upstream that cannot be watched at all is pulled from only as the replication begins.
- A watched head that cannot be recorded is answered as an error, and the remote is then asked where it stands, so the head is still pulled.
- `Cell::watch` and `Cell::decode` give any cell a watch.

Why: a delivered head can arrive after a newer one was fetched or delivered. Recorded as it came, it would move the sync point back and make the next push fail or upload what the remote holds.

### A rule's head refuses a number its field may not hold, a difference may be negative, and floats round to integers

[#617](https://github.com/dialog-db/dialog-db/pull/617), after [#615](https://github.com/dialog-db/dialog-db/issues/615) and [#616](https://github.com/dialog-db/dialog-db/pull/616); the design is in `notes/formula-schemes.md`, "Addendum: the number band"

A compiled rule is meant not to fail or go quiet at run time over the type a number happened to take. Three things made that false for numbers since they became one band. `math/difference` shared one type across its operands and result, so a natural minus a larger natural was a non-match and a decrement over a natural counter could not be written with a `-1` literal at all. A head field's declared type was never checked against what the body binds to it, so a rule could write a negative into a `natural:` field, where no natural reader sees it. And a comparison proved nothing to the type system, so no guard could make a difference a natural again. This change fixes all three and adds the rounding formulas a float needs to reach an integer field.

What this changes for you:

- `math/difference` yields an integer for integer operands, naturals included (`3 - 5` is `-2`), and a float for float operands. A rule whose natural head took the difference directly no longer compiles (see the next point); the difference used to be a non-match on underflow.
- Compiling a rule whose head field cannot hold every number the body binds to it fails with `TypeError::HeadField`: an integer that may be negative into a `natural:` field, a float that may have a fraction into an `integer:` field. The error names the field, the variable, both types, why the field does not read it, what to do (`?n >= 0`, `math/floor`, or widening the field) and the premise that gave the variable that shape. A natural into an `integer:` or `float:` field, and an integer into a `float:` field, compile as before.
- A comparison that proves a value non-negative (`>= 0`, `> -1`) makes an integer variable a natural, so a decrement guarded by `>= 0` on its result compiles against a natural head and leaves no row for the negative case instead of writing it.
- A numeric head field narrows a body variable the body left open: a count read through a field with no declared type and stepped by `1` is a natural when the head is, and the scan feeding it reads naturals.
- `math/floor`, `math/round` (halves away from zero) and `math/truncate` take any number to an integer; a float with no finite value yields no row.
- Rust: `Difference<N>::is` is `N::Signed`; the `Number` trait has `Signed`, `subtract_signed`, `floor`, `round` and `truncate`; `Cell::signed_of` and `Cell::is_signed`, `Primitive::SIGNED_NUMERIC`, `Primitive::without`, `Type::proves_non_negative` and `Type::normalized` are new; `TypeEnv::infer_for` infers a body against its head's field types.
- Two values of different integer storage variants (a stored natural and a negative integer) now compute together in `math/sum` and the other arithmetic formulas instead of being a non-match.
- Type inference keeps two variables one variable after a static resolves one of them. Before, a polymorphic literal such as `1` resolving a formula's scheme to the whole numeric set split the formula's cells into separate variables, and a field's type reached only the cell it was written on.

Why: numbers are one band and a rule compiles against types; what the body can bind has to be checked against what the head holds at compile time, with the error saying what to do, rather than refused or dropped at run time.

#### Compatibility and migration

- Nothing stored or synced changes shape, and nothing is rewritten on
  upgrade. A fact can be missing at rest from any replica that wrote it
  under 0.2.0 or 0.3.0 in one transaction of either shape above: the
  write never reached the tree, and the history holds no record of it,
  so it cannot be found or repaired from the data. Asserting the value
  again in a new transaction restores it.
- An embedder that logs what each transaction wrote, in order, can find
  the transactions of 0.2.0 or 0.3.0 the squash bug may have affected.
  Group each transaction's writes by cell, entity and attribute, a
  concept statement's fields each their own cell. A cell is at risk when
  it holds an `all` assertion of a value, then a write under `last`,
  `max`, `min` or `top`, then another assertion of that value or its
  retraction. A cell whose relation a rule derives, or that the session
  overlay held, was settled correctly; counting it only over-reports. To
  see what a flagged cell should hold, replay its writes one per commit
  on a scratch branch and compare. One difference is not the bug: when a
  choosing write lands on a cell holding two claims of its own
  transaction, the two stand equal and the value with the greater hash
  is elected, so one transaction and one commit per write can end
  differently, before this release and after it.
- Nothing a replica stores or syncs is rewritten. A stored rule that no longer compiles under the head check (one that wrote a possibly negative integer into a `natural:` field, or a float into an `integer:` field) is held inert by `Branch::commit`, as any rule that fails to hydrate is: it stops firing and does not fail commits. Re-evaluate such a rule with a guard or a rounding formula, or widen the field, to have it fire again.

### A number binds a typed rule variable as the variable's type reads it

[#616](https://github.com/dialog-db/dialog-db/pull/616), fixes [#615](https://github.com/dialog-db/dialog-db/issues/615)

Since 0.3.0 a non-negative number reads back from storage as a natural, whatever type wrote it, and a field declared `integer` reads it as the integer it is. A scan already did. A value bound into a typed variable by any other route (a commit's induction seeding a rule from a new fact, a formula result, a merge of two rows) was instead held to the variable's exact type, and a natural meeting an `integer` variable failed the whole commit with `Commit-time induction failed: seeded rule body: ... Variable ?n of kind SignedInt cannot bind UnsignedInt(40)`. Binding now reads the value through the variable's type, as a scan does: the natural 40 binds an `integer` variable as the integer 40, and only a value the type does not read at all (a string into a number, a negative into a natural) is a mismatch, which the callers that filter rows treat as a non-match.

The route that showed it needed two replicas. A commit's induction replays every fact that reached the branch since it last induced, so the first commit after a pull seeds its rules with the pulled facts, among them a count the other replica's rule wrote, which reads back as a natural. A replica's own writes advance that watermark, so alone it only ever read the count by scan and never hit the check.

What this changes for you:

- An inductive rule over a field declared `integer` (or `float`) no longer fails the commit when the fact seeding it holds a natural, which is what every non-negative number is once stored. The first increment after a pull commits and syncs instead of being lost.
- A constant premise field agrees with a seeded fact its type reads as the constant (`count: 40` matches the stored natural 40), as a scan for that constant does.
- `Match::bind` on a typed variable stores the value as the variable's type reads it (`SignedInt(40)` for an `i128` variable given `UnsignedInt(40)`), not the value as given.

Why: numbers are one band, and the band is read through a type at every boundary, not only at the scan. A rule that compiles must not fail at run time over the type a number happened to be stored as.

### Repositories opened through one environment decode and plan the same rules once

[#611](https://github.com/dialog-db/dialog-db/pull/611)

The rule cache and the plan cache were held per branch and per repository. What rule resolution decodes and builds from content (rule bodies, keyed by entity and the bytes the reader read; inductive bodies; a rule's head re-spelled onto an attribute; selecting rules) and query plans are now held once per environment and shared by every repository opened through it. Each of those maps is bounded. Assembled rule sets, the rules set aside, rule discovery and the trigger footprint stay per branch.

What this changes for you:

- A peer with many spaces holding the same rules decodes each rule body and plans each rule once, not once per space.
- `HeldCaches::release` keeps the shared rule content and plans, since nothing in them belongs to the released repository; `HeldCaches::clear` drops them.

Why: those entries are pure functions of content and answer nothing about which rules a repository holds, so sharing them is sound. An assembled rule set is keyed by its lines' rules roots and kept one per spelling of a concept: shared across repositories, the sets of every repository querying the same concept would evict each other on every query. Sharing assembled sets between repositories whose rules are equal would need the key to name a line by its rule content rather than its root, and is left until a workload shows it is needed.

### An assembled rule set stands until a rule fact of its layers changes

[#610](https://github.com/dialog-db/dialog-db/pull/610)

A rule set, and the analysis of which rules are set aside, was kept per exact layer state: each line's tree root, each overlay's revision, each staged layer's generation. Any write moved one of those, so a session-overlay fact, a staged fact or a commit of plain facts rebuilt every rule set on the next query, and a transaction's query never shared the set its branch had assembled. Rule resolution reads only `dialog.rule/*` and `dialog.concept/transient`, so an entry is now keyed by those facts alone. A line is named by its rules root: a commit whose batch writes no rule fact carries the rules root of the head it built on to the head it mints, and any head moved another way (a pull, a merge, a raw commit) is its own rules root. An overlay or a staged layer is named by a hash of the rule facts it holds and hides, and is left out of the key when it holds none. Lookup is an exact match; nothing is replayed.

What this changes for you:

- A query or subscription after a session-overlay write, a transaction write or a commit that writes no rule fact reuses the assembled rule set instead of rebuilding it. Results are unchanged. On the `rule_cache` bench (a chain of 10 committed rules over 50 people, native, arm64), such a query takes 0.40 ms after an overlay write (1.78 ms on 0.4.0-rc.2), 0.46 ms after a commit (1.99 ms) and 0.39 ms in a transaction (1.91 ms); with nothing written in between it takes 0.39 ms on either.
- The first query after a commit that installs or retracts a rule, or after a pull or merge, assembles its rule sets once, as before. That includes rules a transaction staged and then committed: the staged layer and the committed line name the same rule facts differently, so the `rule-dispatch` perf scenario assembles 6 sets, as on 0.4.0-rc.2, not the 3 an earlier draft of this change reached by replaying reads.
- Each spelling of a concept (the same relation under different field names) keeps its own assembled rule set, at most four per concept; before, queries alternating between two spellings evicted each other's. The `subscribe-repoll` perf scenario assembles 9 rule sets instead of 18.
- A decoded rule body is cached by its entity and the hash of the bytes it was decoded from, and is used only after the reader's own read returned those bytes. A rule whose stored bytes do not hash to its entity stays inert even when the genuine body under that entity was decoded earlier; before, a cached body stood in for whatever bytes a layer held.
- A subscription now records, as rule demand, the rule reads the branch's head cache answered. Before, a rule set assembled from cached discovery did not demand that range, and a rule installed into it did not re-run the subscription.

Why: tonk writes session facts through the overlay and commits as commands run, and every such write took it off the rule cache. A rule set is a function of the rule facts of its layers, so that is what the cache is keyed by.

### A query's own overlay is held as a line, so every rule set it assembles is cached

[#609](https://github.com/dialog-db/dialog-db/pull/609)

The facts a query folds in beside its lines, the injected `db:session` metadata and whatever `.with(..)` adds, were a `Changes` batch read as a store: filtered and sorted per selector, scanned for retracts and for rules on every read. A rule in that batch turned the assembled rule-set cache off for the whole query, and its bodies were decoded every time. The batch is now held as an `Ephemeral`, the store a branch's session overlay already uses. It is read by range, and its revision joins the key of every rule-set and read-settlement cache, so a query folding in the same facts as an earlier one is served what the earlier one assembled. Every overlay and staged rule is read through one function and hydrated once per content address. `Changes` stays the write batch and the wire form; nothing reads one as a source any more.

What this changes for you:

- `QueryLayer::overlay` returns an `Ephemeral` instead of an `Arc<Changes>`.
- `Changes::select` and `impl Provider<Select> for Changes` are removed from `dialog-corpus`. To read facts held in memory beside a tree, hold them in an `Ephemeral` (`Ephemeral::apply` takes the batch) and read it with `Ephemeral::select`.
- A query or subscription whose `.with(..)` carries a rule no longer rebuilds the rule set and re-decodes each rule body on every evaluation. Results are unchanged.
- Asset changes in a `.with(..)` batch are dropped from the query's overlay, as they were before: a query reads facts only.
- `EntityKey::from_fact`, `AttributeKey::from_fact` and `ValueKey::from_fact` build the key from its parts in one pass instead of parsing and re-encoding it once per field. The bytes are unchanged. `dialog_corpus::fact_index_keys`, which builds all three at once, is now public.

Why: a query fell off the cache whenever its overlay held a rule. A performance report on Sapling, which hands rules to each query in its overlay, measured rule decoding and assembly at about 60% of instructions on that path. Reading the per-query batch as a separate kind of store is what let that path skip the cache, so the batch now goes through the store and the cache key every other overlay goes through.

### The storage rewrite carries blob-index keys over as they are

[#605](https://github.com/dialog-db/dialog-db/pull/605), forward-ported from `release/0.3.x`

The rewrite that moves every number into one band (`rekey`, run by `Branch::migrate`) parses each stored key as entity, attribute and value. A tree written before the blob index was retired can still hold blob-index keys, a tag and a hash with no value slot, and those did not parse, so the whole rewrite aborted. `rekey` now copies a blob-index key unchanged.

What this changes for you:

- A branch an 0.2.x release wrote with blob-index entries migrates instead of failing with `Could not convert bytes into key: a stored key does not parse`. For tonk, that was every pull of such an account's `main`.

Why: 0.3.1 ships this fix from `release/0.3.x`; without it on `main`, the next release would bring the failure back.

### `dialog-network`, `dialog-repository` and `dialog-peer` are built as `rlib` only

[#601](https://github.com/dialog-db/dialog-db/pull/601)

The three crates declared `crate-type = ["cdylib", "rlib"]` since they were created. Nothing in the workspace, its CI or tonk consumes the dynamic library the `cdylib` target produced: the wasm test jobs build `wasm-bindgen-test` binaries, and tonk's worker crates are cdylibs of their own that link these as `rlib`s. They are now `rlib` only.

What this changes for you:

- `cargo build` no longer emits a `libdialog_network`, `libdialog_repository` or `libdialog_peer` dynamic library. A build that needs one declares its own wrapper crate with that crate type.
- A clean release build of `dialog-peer` overlaps the compilation of `dialog-query`, `dialog-repository` and `dialog-peer` instead of running them one after another.

Why: with a `cdylib` target, cargo waits for every upstream crate's full object code before starting a crate, rather than starting on its metadata. A crate type nobody consumed serialized the slowest part of every build that reaches the peer.

### A certificate a remote refuses as revoked is retracted only on a signed revocation that verifies

[#602](https://github.com/dialog-db/dialog-db/pull/602)

A refusal of a revoked certificate now carries the signed revocation,
and a peer retracts the certificate only once that revocation verifies.
Before, a peer retracted whatever certificate a remote named as revoked,
so a remote could make a holder discard grants that still stand just by
naming them. A revocation checker now returns the revocation document
with its match (`RevocationMatch::proof`, a UCAN container's DAG-CBOR
bytes). The UCAN access service hands it to the holder in
`AuthorizeError::Revoked`'s new `revocation` field, in the container's
base64url tagged form. The peer decodes it and checks four things
before it retracts. The document is a `/ucan/revoke` of the very
certificate named. Its issuer stands above that certificate in a chain
the peer retains. Its signature and proofs verify. Its evidence shows
the issuer could withdraw what it names. A refusal with no revocation,
or with one that fails any check, retracts nothing and is reported as it
came.

What this changes for you:

- `RevocationMatch` has a new field, `proof: Option<Vec<u8>>`. A
  checker that builds one needs `proof: None`, or the document when it
  keeps one.
- `ContainerError::Revoked` is now `{ cid, found: Box<RevocationMatch> }`.
  The revoker that was its `revoker` field is `found.principal`.
- `AuthorizeError::Revoked` has a new field, `revocation:
  Option<String>`. A pattern that spells out its fields needs `..` or
  the field; a constructor needs `revocation: None`.
- A fork refused as revoked by a responder that sends no revocation
  (any responder before this release, and a revocation index that does
  not keep documents) retracts nothing and fails with the refusal, where
  it used to retract the named certificate and retry.
- A revocation the peer accepts must be signed by a `did:key`
  principal, the way the proof walk checks the certificates it admits.
- The UCAN test service (`dialog-remote-ucan-s3`'s `helpers`) takes
  `POST /revocations` with a signed revocation container in the body and
  returns it with each refusal. `POST /revocations/<cid>` still refuses
  the certificate, with no document, as an older service would.
  `REVOCATIONS_PATH` moves from the native-only server module to
  `helpers` itself, so a client test can name it on wasm.

Why: a refusal naming a certificate is a claim, and a holder that acts
on it discards authority it may still have. The signed revocation is the
evidence the claim rests on, and only the holder can tell whether its
issuer was entitled to withdraw that link, since only the holder knows
the chains it retains. A lenient fallback that retracted when no
revocation came would let a remote omit it and be believed anyway.

### A certificate a remote refuses as revoked is retracted, and the request is proved again through another grant

[#598](https://github.com/dialog-db/dialog-db/pull/598)

A peer that retains two grants for the same claim, one revoked and one
fresh, presented whichever the proof walk admitted first, and the walk
admits candidates in content hash order: half the time it presented the
revoked one, and the remote refused a request the peer had every right
to make. A device linked to an account again, after the account revoked
its earlier grant, holds exactly that pair until the revocation reaches
it, which is how this surfaced as a flaky sync.

The refusal now names the certificate. `AuthorizeError::Revoked` carries
`certificate`, the CID text of the revoked link, when the responder
knows which it was (the UCAN access service does). A `Peer` performing
a fork that comes back `Revoked` with a certificate retracts that
certificate where it retains grants, as one commit on the branch it
proves from, and authorizes the fork again; the walk no longer sees the
revoked link and admits the other grant. The loop ends when the request
goes through, when the named certificate is not retained here (the
original refusal is reported, since it names the authority that was
withdrawn), or when the remote names one already retracted.
`Delegations::retained(cid)` finds a retained certificate by the CID a
refusal names, and the new `AuthorizationFailure` trait, implemented by
every effect error that wraps an `AuthorizeError`, is how a caller reads
the refusal back out of an effect's error without matching each enum.

What this changes for you:

- `AuthorizeError::Revoked` has a new field, `certificate:
  Option<String>`. A pattern that spells out its fields needs `..` or
  the field; a constructor needs `certificate: None`.
- A fork refused as revoked retracts the named certificate from the
  peer's access branch and is retried once per named certificate,
  through the peer's remaining grants. A caller that retried on
  `Revoked` itself can stop; a caller that counted requests to the
  access service sees one more per revoked link in a presented chain;
  a caller that lists the grants a principal holds no longer sees a
  grant a remote refused as revoked.
- The retraction takes the remote at its word. Only a certificate the
  peer retains can be named to any effect, and a certificate retracted
  this way comes back if a later sync re-retains it, to be retracted
  again on the next refusal.
- `Delegations::retained(cid)` on a branch reads back the retained
  certificate with that CID, or `None`.
- `AuthorizationFailure::authorization_failure(&self) ->
  Option<&AuthorizeError>` on `ArchiveError`, `BlobError`, `MemoryError`,
  `BranchError`, `PeerError`, `ForkError`, `DialogStorageError`,
  `DialogCorpusError`, `ResolveError`, `PublishError`,
  `RetractError`, `S3Error` and `AuthorizeError` itself.
- The UCAN test service (`dialog-remote-ucan-s3`'s `helpers`) accepts
  `POST /revocations/<cid>` and refuses that certificate from then on.

Why: the retaining replica is the one that learns of a revocation, and
until it does the revoked grant is a valid candidate on every other
replica. The remote is the first to know; it says which link it refused,
and the peer that asked is in the best position to drop that grant and
try the others rather than report a refusal that a different grant would
have avoided. Retracting rather than remembering the refusal in memory
makes the answer outlive the process and reach the replicas the branch
syncs to, through the one mechanism a grant already leaves a replica by.

### An entry a read yields is its key and value, and the hash of its key is kept by the segment it is edited in

[#600](https://github.com/dialog-db/dialog-db/pull/600)

[#569](https://github.com/dialog-db/dialog-db/pull/569) gave every `Entry` the cell the blake3 of its key is kept in. Only the boundary rules read that hash, on the edit path, but every entry a scan yields carried the cell too: about 48 bytes more per entry, enough that moving an entry through the scan's stream adaptors became a `memcpy` call. The cell now lives in a `SegmentEntry`, the entry as a `TransientSegment` holds it, and `Entry` is a key and a value again. A stored leaf still keeps its keys' hashes for every opening of it, a node sealed from ranked entries still opens with their hashes, and an edit's key hashed before it lands still brings its hash into the leaf. Trees are shaped byte for byte as before.

What this changes for you:

- `TransientSegment::entries`, `entries_mut`, `into_entries`, `into_parts` and `take_entries` hold `SegmentEntry`s. A `SegmentEntry` derefs to its `Entry`, `into_entry` takes the entry out, and `set_value` replaces the value and keeps the key's hash. `TransientSegment::new` takes `SegmentEntry`s, built `From<Entry>`; `upsert` takes either.
- `Entry::hashed` is gone: `SegmentEntry::hashed` and `SegmentEntry::key_hash` read the kept hash. `Entry::key_hash` computes the hash each time it is called, as in 0.3.0.
- `PersistentSegment::from_entries` and `PersistentNodeBody::segment_from_entries` take `Entry`s or `SegmentEntry`s.
- An `Entry` can be built with a struct literal again.

Why: a scan moves every entry in its range and never asks for a hash; the edit path asks for few. Measured on a cold reopen against 0.3.0 the cell cost `number-range-mixed` 6.0% more instructions and `number-range-wide` 3.4%.

### A revoked delegation's match is boxed in `Invalid::Revoked`

[#599](https://github.com/dialog-db/dialog-db/pull/599)

`Invalid::Revoked` carried the revoked delegation's CID and the matching
`RevocationMatch` (a second CID and its issuer) inline, which made every
result of `Invocation::check` and `Chain::check` at least 208 bytes. The
match is now behind a `Box`.

What this changes for you:

- `Invalid::Revoked { found, .. }` is a `Box<RevocationMatch>`. Reading
  its fields (`found.principal`, `found.revocation`) is unchanged; code
  that builds the variant wraps the match in `Box::new`, and code that
  needs the `RevocationMatch` itself takes `*found`.

Why: Rust 1.99's clippy flags the size as `result_large_err`, and every
verification paid to move it, on success as well as on refusal.

### A flush that fails leaves nothing in the held caches, and environments sharing them share branch writers

[#579](https://github.com/dialog-db/dialog-db/pull/579)

A seal keeps the nodes it produces in the node cache before they reach
the archive, and that cache outlives the handle. A commit, merge or pull
whose flush failed left those nodes held for the repository, so a later
read of them was answered from the cache and never fetched. They are now
forgotten when the flush fails. Separately, a session works through its
peer's archive and caches, so its handles of a branch and its peer's now
share one writer: the writers live in `HeldCaches` beside the caches.

What this changes for you:

- A failed flush no longer leaves the repository's scope holding blocks
  its archive never received.
- A peer and its sessions take turns moving a branch's head. `release`
  and `clear` leave writers alone.
- One `HeldCaches` is for one archive: environments over different
  storages must not share a set.
- `NodeCache::forget` and `Delta::keys` are new.

Why: a scope must hold only what its archive has, and every handle that
writes to one archive must take turns.

### A formula derived `#[formula(cached)]` is computed once per input

[#576](https://github.com/dialog-db/dialog-db/pull/576)

A formula says it wants caching where it is derived, and the engine
keeps its outputs by the formula and its input values in the
`FormulaCache` the environment holds, bounded at 4,096 applications and
evicting by SIEVE. A computation that fails is not remembered. The
revision formulas are marked cached, and the process-wide memo of
verified revision records they used is gone.

What this changes for you:

- `Scope` requires `Holds`: an environment a query runs through has to
  hold things, which a wrapper does by forwarding to its operator.
- `FormulaQuery::evaluate` takes the environment. `expand_with` expands
  through a given cache; `expand` still works without one.
- `Formula` gains `resolve_with` and `formula::emit`.
- `HeldCaches::formulas()` is the cache, and `clear()` clears it.
- An environment that holds nothing remembers nothing across
  evaluations.

Why: verification dominates projecting a record and the fixpoint
evaluator re-applies these formulas every round; any costly formula
needs the same, and each hand-written memo was another static nobody
owned.

### Branch writers and in-flight block reads belong to the environment

[#575](https://github.com/dialog-db/dialog-db/pull/575)

The writers of open branches and the in-flight block GETs of the S3 and
filesystem transports were process-wide statics: they outlived every
environment, and two environments in one process shared them whether
they meant to or not. A branch's writer is now held by the environment,
and each `S3` and `Fs` site owns its in-flight GETs, so readers join one
another's requests only through the same site.

What this changes for you:

- `S3` and `Fs` are no longer `Copy`, and are built with `default()`.
- `Holds` gains `held_or`, which `Holdings` answers under its lock, and
  `held_key::<T>(name)` names a key by its type. An environment that
  forwards `Holds` should forward `held_or` too, or two first opens at
  once may each make their own writer.
- Two environments in one process that hold nothing in common no longer
  share a writer or join each other's block reads. A race between them
  as one issuer is refused by a merging commit rather than minting an
  edition twice.

Why: none of the three registries had an owner, so none could be
scoped, bounded or released with the work it served.

### A branch's caches are held by the environment it is opened through

[#574](https://github.com/dialog-db/dialog-db/pull/574)

Tree nodes, spilled values, verified revision records, causal verdicts,
contexts, plans, rule sets, the live spine and the memo of trees
rewritten under the number band were made with each branch handle and
died with it, so a service that opened the branch per request reused
nothing. The environment now holds them as one `HeldCaches`, each at the
width it is sound to share: tree nodes under one byte budget for every
repository, each reading through its own scope; what is addressed by
content or version per repository; what is tagged by a head per branch.
Opening a branch again through the same environment finds them warm.

What this changes for you:

- `OpenBranch::perform` and `LoadBranch::perform` require the
  environment to implement `Holds`. Peers and sessions do. A bare
  `Volatile` holds nothing, so a branch opened on it directly starts
  cold every time, as before.
- Build a peer `.with(HeldCaches::with_budget(bytes))` to bound its node
  cache. `peer.caches()` gives `release(&repository)`, `clear()` and
  `bytes()`. A session shares its peer's caches unless given its own.
- Code that reopened a branch to get cold caches has to release them
  first: `HeldCaches::of(env).release(&repository)`.
- The process-wide root-manifest memo is gone.
- The per-repository caches accumulate until the repository is released
  or the environment dropped.

Why: whoever owns the environment decides how long its caches live, and
can bound or drop them; a cache nobody owns can be neither.

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
257-byte keys, per thread. Now an entry in a segment keeps the hash of its key, a
separator its own, and a stored node the hashes of its keys and its
piece summary, each computed on first ask and dropped with its owner. A
node a persist seals is kept in the tree's node cache. Trees are shaped
byte for byte as before; nothing stored changes.

What this changes for you:

- The entries a `TransientSegment` holds keep their key's hash, so
  replace one whose key changes rather than assigning the key in place.
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

### Compatibility and migration

- Nothing stored or synced changes for the file store's lock; only the
  lock files beside its cells do. A lock file the previous release left
  behind holds a bare PID, which this release reads as a holder: one
  left by a writer that crashed is recovered like any other, and one
  whose writer is still running is held. Only a writer on the previous
  release that is still running excludes nothing: it and a writer on
  this release do not exclude each other while both write one store
  during an upgrade, since the older one deletes a lock file it cannot
  read as a PID as stale, and this release's is `pid token` (and for a
  cell whose name has an extension, the two releases lock different
  files). Stop writers on the previous release before starting ones on
  this release.
- A registry branch (`meta`) or host state branch an earlier release laid
  out is rewritten under the number band the first time this release
  writes it, as `Branch::migrate_storage` rewrites any branch: one pass
  over its entries, published as a revision that descends from the head.
  0.2.0 and earlier refuse the rewritten tree by name, as they refuse any
  migrated tree. An embedder runs nothing.
- Nothing stored or synced changes for the refusal of a rival head of
  one version (#705): nothing is rewritten on upgrade, and heads, trees
  and the wire format are as before, so an older release reads all this
  one writes. An older release still takes a rival head as seen, so a
  write it races with one of this release under the same key can still
  be dropped there; upgrade every replica that shares a key.
- Positions already stored keep their bytes and their order: nothing is
  rewritten, and both releases read every position the other writes. Only
  newly derived positions differ. An older replica and this one derive
  different positions for the same insert, so the same insert made on both
  no longer converges to one entry; each still lands between the
  neighbours it named.
- Nothing stored or synced changes for the node cache moving to the
  store: trees, nodes and their hashes are the same bytes, nothing is
  rewritten on upgrade, and an older release reads everything this one
  writes. Only how often a node is read from the archive changes.
- Nothing stored changes for the difference that prunes shared
  subtrees: no stored data is rewritten, trees are laid out as before,
  and an older release reads everything this one writes. Only how many
  blocks a comparison reads and reports changes.
- An older replica holds heads that carry their whole context inline,
  or none, and trees with no context region. Nothing is rewritten on
  upgrade: those heads verify under their own payload shapes and pull as
  before. The first head this release mints on top of one writes the
  whole map into its tree's region, once, in the same batch as its
  revision record.
- An older release cannot verify a head this release mints: its signed
  payload ends in a `0x03` marker and the head's own count, which the
  older release does not know, so its pull refuses the head as an
  invalid signature. Upgrade every replica that pulls from one running
  this release. Nothing an embedder runs is needed beyond that.
- A head minted by a build of this release from before heads carried
  their count ends in a lone `0x02` marker. No release or pre-release
  tag carries such a build (none contains #625's commit), so these heads
  exist only where an untagged build of `main` ran, and the `0x02` shape
  can be retired before the release. Until then it still verifies and
  pulls, nothing is rewritten, and a reader derives its count from its
  region.
  A commit on such a head of another origin reads the head's region for
  the count, as those builds did, and the head it mints carries one.
- `UcanAddress` encodes an optional `socket`, left out when absent, so an address without one keeps its bytes. An older replica reads an address that names a socket and drops the socket, reaching the peer over requests.
- Invocations now carry `iat`. A service from this release refuses a stale or replayed invocation (`AuthorizeError::Stale`, `AuthorizeError::Replayed`), and over HTTP still serves one without `iat`, so an older client keeps working against it; over a socket `iat` is required, and an older client never opens one.
- The socket carries text `ping` and `pong` messages beside its frames. A service's socket must answer `ping` with `pong`, or a client closes the connection every `Liveness::deadline`; no released service has a socket, so none predates this.
- A conflict's `412` now has a body, and `Rejection::Unavailable` may carry `retry_after`. An older client ignores both; this client reads an older service's bare `412` as a conflict that does not say what the cell holds.
- Nothing stored is rewritten.
- Nothing new is stored by the rewrite fix: a blob-index entry keeps the
  key and value it had, so the tree reads as the older release wrote it
  apart from the numbers the rewrite moves. A branch whose migration
  failed on such an entry wrote nothing, and migrates on upgrade.
- Nothing new is stored. A certificate a remote refuses as revoked is
  retracted with the ordinary delegation retract, the same facts and
  blob tombstone a `Delegations::retract` by hand writes, so an older
  release reads the branch as before. The `certificate` field is
  additive on the wire: an older access service answers `Revoked`
  without it, and a peer on this release reports that refusal as
  before, without retracting or retrying. An older peer reading this
  release's answer ignores the field.
- Nothing stored changes for a refusal's revocation either. The
  `revocation` field is additive on the wire: an older access service
  answers `Revoked` without it, and a peer on this release reads that
  refusal and retracts nothing. An older peer reading this release's
  answer ignores the field and retracts the named certificate on the
  service's word, as it did before.

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
- A number read back raw (an `Selector` select, a `Value` match)
  comes back as the narrowest type that holds it: a natural when it is
  whole and not negative, an integer when whole and negative, a float
  otherwise. Code matching `Value::UnsignedInt` on counts and timestamps
  still sees them; code matching `Value::Float(5.0)` on a stored `5.0`
  now sees `UnsignedInt(5)`. A typed read (a concept field, a derived
  struct) views the number as the type it declares, unchanged.
- An `Selector` range bound reads each number through the lens
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
  fails with `CommitError::Fact(InvalidValue("NaN is not a value"))`
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
- `dialog_corpus::identity` and `dialog_query::identity` hold the
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

- `dialog_corpus::Attribute` is `dialog_corpus::Relation`.
  `DialogCorpusError::InvalidAttribute` and `ReservedAttribute` are
  `InvalidRelation` and `ReservedRelation`, and `ATTRIBUTE_LENGTH` is
  `RELATION_LENGTH`.
- In `dialog_query`, the validated relation name the `the!` macro
  builds is `Relation` (it was `The`), and what an attribute's `the`
  holds, one relation or a keyed collection, is the enum `The`
  (`The::Relation(..)`, `The::Collection { .. }`; it was `Relation`,
  with an `Attribute` variant). `dialog_query::ArtifactsAttribute` is
  `ArtifactsRelation`, and `Reach::Attribute` is `Reach::Relation`.
- One type, `dialog_corpus::Pick` (`Last`, `All`, `Top(..)`, `Max`,
  `Min`), replaces both `dialog_corpus::Policy` and
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
  `Instruction::Assert(Fact, Pick)` are the one write form, beside
  `Retract`; `Change::Replace`, `Instruction::Replace`, `Change::Succeed`
  and `Instruction::Succeed` are gone. `dialog_corpus::Pick` is the
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
