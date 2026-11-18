# A consumer that can only query cannot draw a branch's search tree: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | *What do we know?* The problem and the approach (top story). Three of the four things to build in dialog are built and tested on the draft branch: the readings over a decoded node, the five resolvers, the entities. That an index node's buffered ops are readable by no resolver. | *Where are we at?* Building. All four things are built on the draft branch, which now sits on top of the stack (`claude/layers-15`) instead of beside it. |
| **Agenda** | *What do we need to know?* The maintainer's word on what building settled without them: a span's and an entry's entity being its place, `fanout-bits`, `of` taking an entity only. Whether buffered ops are to be exposed, and as what. How a row's details are reached by its own entity. | *Where are we going?* Tonk: pin, a query step that is not a concept, the component, and the removal of its reader. In dialog: the points still open in what is built. |

## 2026-11-12, Describing: what was tried and what was seen

**Asked.** The maintainer, after several rounds of fitting the resolvers
to the inspector: stop working with the resolvers we have and ask what
the right way is to expose an interface for exploring a search tree from
the query engine; from a clean slate.

**Learned.** The description in the top story: what the inspector needs,
what the worker's hand-written operations give it, what the resolvers
give instead, and where the two part. Each claim is in the ledger with
what backs it.

Two approaches were put on the table before any problem was stated, and
are parked in the ledger as opinions, not examined:

- the maintainer's: lower-level primitives, reading a block and then
  separate formulas that decode it as an index node or as a segment node;
- the agent's: a resolver yields each span whole, as one record value.

The maintainer asked that the first not bias the thinking. Neither is a
column yet; there is no matrix until there is a problem for it to serve.

**Decided.** Not decided. The draft branch stays as it is: what it
changed (entities for nodes and entries, a resolver that lists) was
decided while fitting the resolvers to the inspector, which is the
question this topic sets aside, so whether any of it is wanted is open
until a direction is chosen.

## 2026-11-12, Diagnosing: a problem statement is offered

**Asked.** When someone opens the tree for a branch, what are they
trying to find out? Not answered. The maintainer gave a problem
statement instead: "the dialog query engine cannot be used to inspect
the underlying tree, which causes the embedder to implement side
channels to do it."

**Learned.** The statement has no remedy in it, which the request it
replaces did. Two things in it do not yet hold up:

- *Those two cannot both be true.* The ledger has as a fact that a
  resolver heads a query and answers when given a node. So "cannot be
  used to inspect" is not true as worded; it is true of something
  narrower. What tonk's side channel does that a query did not, read off
  its code:
  1. starts from the branch with nothing given (the root is the default);
  2. returns each child of a node with its details together in one row;
  3. says whether a child's block is held locally, without fetching it;
  4. returns a key already split into parts, and the fact it encodes as
     entity, attribute, type and value;
  5. merges a blob record's fields into its entry's row.
- *"Side channel"* is used as if its cost were known. Tonk's reads
  blocks through the environment and decodes them with dialog's own
  public `inspect_*` functions. What goes wrong because of that is not
  written anywhere in the topic, and it is what the criteria will come
  from.

**Decided.** Not decided. The statement is recorded in the top story as
offered. The next question is what goes wrong because an embedder has a
side channel.

## 2026-11-12, Delimiting: what goes wrong with the side channel

**Asked.** What goes wrong because an embedder has a side channel? The
maintainer: "What exists today was a hastily put together bandaid that
we're struggling to detangle from the embedder code."

**Learned.** The harm is the braid, not the reading of blocks. What the
reader is woven into in tonk, each checked:

- the worker: `resolve_formula` answers `tree/*` by name;
- the query wire format: `Predicate::Formula(String)` exists for it, and
  the worker's `intent` operation now rides the same arm;
- the page bundle: `<tonk-tree>` is Rust registered by `tonk-guest`;
- display routing: `<tonk-display>` names `tonk-tree` among the elements
  it forwards a space's context to.

So the objective under the first statement is that showing the tree need
not be part of the embedder's host at all, which holds only if a
consumer that can do nothing but query can get what it draws. That gave
the draft in the top story. It keeps three of the five things tonk's
reader does (start from the branch, a child with its details, a key in
parts) and leaves two out pending the maintainer: local-or-remote, and
blob fields on an entry's row. "A number of reads that does not grow
with the tree" is there because a walk of the whole tree to show one
node's children was measured on the draft branch and is what a page
cannot afford on a tree held partly on a remote.

**Decided.** Not decided: the draft awaits the maintainer.

## 2026-11-12, Delimiting to Directing: the statement is accepted, and a constraint surfaces

**Asked.** Is the draft the problem, and what in it is wrong or missing?
The maintainer: yes to "a number of reads that does not grow with the
tree"; yes to consumers beyond the inspector, "any embedding should be
able to inspect the tree where now it's done through custom machinery";
the two things left out are needed. On the first of them: "I'm not sure
there is a clean way to accomplish this without undermining the
resolver's idempotence claim, because the same query may return a
different result depending on whether the node has been downloaded or
not. If you can help me find a way to do it without undermining a
property I'd be delighted. Otherwise I may have to let this property
go."

**Learned.** *What does that word mean?* Two different properties are
going by one name:

- **Same rows.** The same tree query yields the same rows whenever it is
  asked.
- **Permanent rows.** A row, once yielded, is never wrong.

The module that makes the claim argues only the second, and says what it
is for (`rust/dialog-corpus/src/inspect.rs`, "Why this is sound under
differential subscriptions"): rows derived through a `Load` "can become
unnecessary, never wrong, so no invalidation machinery is required for
them". It names a locality probe as the thing that would break it.

The first property does not hold today. A resolver whose block is
neither held nor reachable yields no rows (`let Some(block) = loaded
else { continue }` in `rust/dialog-query/src/resolver.rs`); asked again
once the block can be had, it yields them. Nothing false was said in
between, which is the second property holding while the first does not.

That suggests where a way through might be, entered in the ledger as a
hypothesis and not examined yet: "this block is not held" is a claim
that turns false when the block arrives, and is what breaks permanence;
"here are this block's rows", yielded by a read that does not fetch
(`Load::locally` exists), is permanent, and a consumer that sees no rows
for a child its parent names has learned the child is not held without
the engine having said so. What that leaves unanswered is how a
subscriber is told to look again when a block arrives, which is news of
more rows, not a withdrawal of any.

**Decided.** The problem statement is accepted as it now reads in the
top story. Phase moves to Directing. The constraint is not yet a
criterion: which of the two properties it is has been asked.

## 2026-11-12, Directing: what "idempotent" means here, and three ways to say what is held

**Asked.** Which property is to be kept: the same rows always, or a row
once yielded never wrong? The maintainer: permanence "may suffice,
although that seems far more confusing to me than thinking of
idempotence", and then what they had actually meant:

- A tree read fetches whatever blocks it needs, "meaning behavior will
  be unobservable from having those blocks vs not".
- "The reason we required node as opposed to allowing scan was precisely
  so that you would not download the whole tree by a mistake."

And three ways they see to tell what is held:

1. Capture partiality in what is queried, so the same revision says it
   has no node where the replica lacks one, and a command that fetches a
   block changes that, so asking again answers. "Very complicated and
   I'm not even sure possible at all."
2. Pretend the whole tree is local and be deliberate about not asking
   for what would download. Then nothing can be asked about locality.
3. A resolver that takes a time, and says whether a block was local at
   that time; commands update the time and a rule passes it in. "None of
   this is ideal."

**Learned.** The glossary now has the maintainer's meaning of
*idempotent*; the agent's split into "same rows" and "permanent rows"
was the agent's and is dropped as a framing.

*Those two cannot both be true.* On the draft branch a resolver whose
node is open walks from the root and loads every block, fetching what is
not held. That is the download by mistake that requiring the node was
there to prevent. The maintainer asked for listing ("No it should list")
before saying why the node was required; the two are now both on record
and one has to give.

Ways 1 and 3 are one idea: make what the answer depends on an input, so
the read stays a function of its inputs. They differ in the input. Time
moves when nothing was fetched, and needs commands to move it. What the
answer depends on is what the replica holds, and dialog already treats
one such thing this way: the module's argument says the only mutable
fact in the domain is the current root, "which reaches queries as an
ordinary tracked fact (`dialog.branch/tree`), never through this
command". What made way 1 look impossible is putting it in the
revision, which is shared and synced while holdings differ per replica.
Dialog has a place for what differs per replica: the `Replica` entity,
"this device's view of the repository", whose facts are synthesized when
a query runs and never stored.

That gives a fourth way, entered as an opinion of the agent's and not
examined: what a replica holds is a fact of the replica, read by a scan,
which the resolvers' own admission rule already says anything addressed
by mutable state must be. The resolvers stay as the maintainer means
them. A rule that asks whether a child is held before asking for its
details never fetches, and a scan that lists what is held, by walking
from the root through local reads only, could be the thing that makes a
rule over a resolver answerable with nothing given, without a resolver
ever listing.

Nothing in the fourth way has been checked against the code.

**Decided.** Not decided. The agent's "presence under a local read" from
the last entry is withdrawn: it has a tree read's rows depend on what is
held, which is what the maintainer means the resolvers not to do.

## 2026-11-12, Directing: a matrix for what is held

**Asked.** The maintainer: check what needs checking and give a concrete
proposal. They doubt what is in a block store can be captured exactly,
and offered more ways: something that changes on every query, or turns
moved by hand, so the same thing is never asked twice; block creation
time, to filter out blocks that were not there before; metadata added
whenever a block arrives. Their uncertainty: asked with an old value,
"I cannot tell what blocks were there before".

**Learned.** Checked in the code:

- Every branch and snapshot carries an ephemeral line
  (`rust/dialog-repository/src/repository/ephemeral.rs`): session facts
  in memory, folded into every read of the line, never committed. Each
  change mints an instant with a sequence and a chained hash, kept in a
  ring of 1024, so a reader pinned earlier gets the exact delta since.
- A subscription's poll returns unchanged unless the head moved or that
  line's sequence moved (`Subscription::poll`). Nothing in it looks at
  the archive.
- A block fetched from a remote is written back to the local archive in
  one function (`hydrate_through` in
  `rust/dialog-repository/src/repository/archive/hydrate.rs`).
- The archive's effects are `Get`, `Put`, `Import` and `Load`; none
  removes a block.

What the maintainer is reaching for with time, turns and a value that
changes per query is an input that moves when what is held moves, so the
read stays a function of its inputs. The ephemeral line's revision is
such an input and already exists: a query pins it, caches are keyed by
it, a poll watches it. So nothing about the block store has to be
captured. What is captured is each answer that was asked for, as a
fact, and the arrival of a block corrects the fact. That is the fourth
column of [design-held](design-held.md), and it answers "what was there
before" without asking the store about the past: the past answer is the
fact as it stood.

Its cost is a state the others do not have: a node nobody probed has no
fact.

**Decided.** Not decided. The matrix has three `?` cells in the
proposed column.

## 2026-11-12, Directing: the maintainer simplifies the proposal

**Asked.** Is "not known until probed" acceptable? Not answered
directly. The maintainer generalized the proposal instead: a command
asks whether a block is local, and its handler checks and asserts a fact
saying it is or is not; a command fetches a block, and its handler
fetches and then asserts that it is local. "That way you actually model
it, without complicating things that we were complicating before."

**Learned.** This is the fourth column with the hook in the hydration
path taken out: the fetch command's own handler says the block arrived.
It is entered as a fifth column in [design-held](design-held.md). It
settles two of the fourth's open cells (nothing is built in the
hydration path; a command is issued against a branch, so there is no
question of which line to write) and it accepts "not known" by
construction, since only a command produces a fact.

It opens one cell and worsens one:

- A command with a handler is tonk's mechanism (a transient concept and
  an `impl Provider<C> for CommandEnv` in its worker), not dialog's. As
  given, each embedding would write the two handlers, and "any embedding
  should be able to inspect the tree where now it's done through custom
  machinery" is the maintainer's own addition to the problem statement.
  Whether two small handlers through an embedding's ordinary command
  mechanism count as that machinery is the maintainer's to say.
- A block that lands some other way leaves "not local" standing until
  something asks again.

**Decided.** Not decided.

## 2026-11-12, Directing: what is held is decided; a strategy matrix is written

**Asked.** Should each embedding write the two handlers, or should
dialog provide them? The maintainer: "Dialog has built-in handlers for
the commands, and it asserts and does this job for you." And: "I don't
want it to assert a permanent fact. I want it actually to assert it in
the overlay when you probe for something. So that way it can disappear."

**Learned.** *Is that a fact, a hypothesis, or an opinion?* "Dialog has
built-in handlers" was checked as a fact and is not one yet: dialog
dispatches a command as a transient fact (`Transaction::dispatch`) and
reacts to it at commit with inductive rules, which derive facts and
perform no effect; nothing built in reads a block or writes the overlay
in response to a command. It is entered as the maintainer's decision
about what dialog will have, and what it needs is entered as a
hypothesis.

"In the overlay" is the ephemeral line the fourth and fifth columns
already named. Nothing changes there; it is now said by the maintainer.

Writing the strategy matrix after that decision gave it a sixth column.
If a command with a built-in handler can say whether a block is local by
asserting session facts, the same thing can say what a node holds: its
spans and its entries, each an entity with ordinary facts. Queries then
read facts and never a block, so the questions that occupied the earlier
sittings (how a rule is given its node, why only `this` reaches a body,
how a child's details stay paired) do not arise in that column. It pays
for that in a command before every query and in memory held for the
session.

**Decided.** What is held: the fifth column of
[design-held](design-held.md), handlers in dialog, facts in the overlay.
It rests on an open hypothesis (dialog reacting to a command with an
effect). The strategy is not decided.

## 2026-11-12, Directing: the held decision is revised by the maintainer

**Asked.** Is anything in the use cases wrong or missing? Not answered.
The maintainer improved the held decision instead: "it might even be
better if dialog doesn't even assert anything. It just dispatches
another command that then your system can react to and take a response
if it needs to, or ignore it. That way [a space] can have a rule that
puts it in overlay or puts wherever it wants to put it."

**Learned.** This takes the last policy out of dialog: what the fact is
called and where it is kept. It fits what dialog already does with
commands, which is to dispatch them as transient facts and let inductive
rules react.

*Those two cannot both be true*, as dialog stands: the maintainer wants
the answer not to be a permanent fact, and wants a rule to decide where
it goes. A rule's head is committed, or transient and passed to the next
round (`transaction/induce.rs`); nothing routes a head to the branch's
ephemeral line. So today a rule can keep the answer only by committing
it. A committed fact about what a replica holds is also wrong in a
second way: it syncs to replicas that hold something else. The two wishes
agree once a head can be marked for the overlay, as one can be marked
transient.

A transient is visible only inside the commit that carries it, so a
query never sees the answer unless a rule kept it. A space with no such
rule gets nothing, without an error, which is the "or ignore it" the
maintainer names.

**Decided.** The sixth column of [design-held](design-held.md), by the
maintainer, replacing the fifth. It rests on two open hypotheses.

**Backtracked.** Not a phase: one decision, made and revised in the same
day, both entries kept.

## 2026-11-13, Directing: what the held decision rests on is settled

**Asked.** Do you want overlay heads added to dialog's rules? The
maintainer: the first dependency (a built-in reaction to a command) "we
can address pretty easily and we should do it". The second "would be
nice", and "even without it we can expose responses as commands and
store results in the db itself, although it would be better if we can
come up with a way to assert / retract / deduce / induce into the
overlay."

**Learned.** The held decision no longer waits on the overlay: with
answers as commands, where an answer is kept is a rule's business and
can change later without dialog changing. "Easy" is the maintainer's
estimate and is in the ledger as an opinion; nothing was built or
measured.

The want is wider than a rule head. Assert and retract into the overlay
exist as calls on the branch (`Ephemeral::assert`, `retract`); what does
not exist is reaching it from a rule, by deduction or by induction. That
is a question about rules and the overlay in general, of which this
topic is one user.

Committing the answer in the meantime has a consequence the rule's
author must handle: the fact syncs, so it has to name the replica whose
holding it describes.

**Decided.** By the maintainer: build the built-in reaction; do not
wait for overlay writes. Scope: a way for rules to write to the overlay
is not taken on in this topic; whether it becomes a topic of its own is
on the agenda.

## 2026-11-13, Directing: contents by resolvers; where the difficulty came from

**Asked.** For a node's contents: commands answered with commands, as
for held, or queries? The maintainer: "I expected through resolvers;
what commands help us [with is] to avoid issuing a query if it would
cause a block to be fetched."

**Learned.** Column 6 is struck. Commands have one job: letting a
consumer know what it may ask for without causing a fetch.

Looking at the columns left (2 to 5), all four put a rule and a concept
between the consumer and the resolver. Every difficulty recorded since
the first sitting sits in that layer:

- a resolver's rows had no entity, so a concept over them collapsed;
- a rule could not be given its node, so a concept of nodes had to
  recurse from the root;
- only `this` reaches a rule's body, so a child's details came back
  unpaired or cost a walk.

None is a property of the resolvers. A resolver asked directly, with
its node as a constant, returns whole rows from one block: seen on
2026-11-12 through tonk's evaluate, and it is how dialog's own tests
read them. The concept was assumed because a tonk page sends concept
queries and a tonk view shows a concept, and the assumption was never
written down or questioned. It is now column 7.

*You are not your idea.* Column 7 is where the agent started on
2026-11-12, before this topic existed (let the embedder's query carry
any premise), and left when asked whether a concept and a rule could not
expose the resolvers. Three of the columns and the draft branch's two
changes followed from that turn.

**Decided.** By the maintainer: contents through resolvers; column 6
struck. The strategy among columns 2 to 5 and 7 is not decided; it
turns on whether a node has to be a concept.

## 2026-11-13, Directing: the resolvers themselves are put in question

**Asked.** Does a tree node have to be reachable as a concept? Not
answered. The maintainer opened a second decision: "I also would like us
to design the resolvers we're going to expose, as I think current ones
are no good. I think something that allows us to obtain a block as
record or bytes and have a formula to parse it is probably better than
what we have now."

**Learned.** This is the opinion parked on 2026-11-12, now asked to be
examined. It arrived as a remedy with a judgement ("no good") and no
statement of what is wrong, so the matrix
([design-resolvers](design-resolvers.md)) was started from what the
agent can point at in the status quo:

- *Is that simple, or easy?* Each of the seven resolvers does four
  things in one place: load, check the hash, decode one part, project.
  The decoders already exist apart from them. That is a braid, and the
  maintainer's column separates it.
- A segment is split across three resolvers paired by position, and
  tonk's reader exists partly to put them back together.
- Adding one field to a resolver on the draft branch took six edits.
- The shape the maintainer names has a precedent in dialog: a revision
  is one record-valued fact and two formulas project it.

A third column (fewer, wider resolvers) was added so the choice is not
between the status quo and one idea.

What the maintainer's column costs is a block travelling as a value.

**Decided.** Not decided. This decision is independent of the strategy
one: either shape of step can be asked directly or wrapped in a
concept.

## 2026-11-13, Directing: the maintainer's model, and a recommendation

**Asked.** What is no good about the current resolvers? The maintainer:
"the model I was aiming for was to represent nodes as opaque resources
(in the wasm component model sense) and then have formulas that act as
accessors so you can read the parts you need from a query. I think the
implementation currently implies reading the block each time, but the
block read is cached so that should be cheap, was the expectation." A
bad query getting a whole node as bytes is fine; "what I don't want is
for every formula to have to decode those bytes over again, which
perhaps is what is happening already." They asked for a recommendation.

**Learned.** It is what is happening. Each resolver loads the block's
bytes, copies them, and each `inspect_*` function builds a node from
them again. Whether the byte read is cached does not help: what is
repeated is the decode.

The opaque resource the maintainer describes already exists in dialog,
one layer down: since #755 the environment answers
`nodes::Read<CorpusNode>` with a decoded node, from the node cache it
keeps, and every query scope provides it. The resolvers do not use it;
they ask for bytes (`archive::Load`) as the tree did before that
change.

*What does that word mean?* "Formulas as accessors": in dialog a
formula is a function of its input values with no environment, so it
cannot turn a handle into a node. An accessor that takes a handle has to
ask the environment, which makes it a resolver. For formulas to be the
accessors, the node has to travel as bytes, and then each one decodes
it, which is the thing not wanted. So the model is kept and the word
changes: the accessors are resolvers over a handle.

That is column 4, the agent's recommendation, and it differs from
column 2 in where the braid is cut: not between a read step and decoding
formulas, but between the environment's node layer (read, check, decode,
keep) and steps that only project.

Of the draft branch: a node as an entity is the handle this needs;
listing when no node is given is not part of it.

**Decided.** Not decided; the maintainer has the recommendation.

## 2026-11-13, Designing: accessors over a handle are chosen; the handle's scheme

**Asked.** Do you want column 4? The maintainer: "Ok I like your
proposal." And: "We actually may even have the ability to read a block
via an asset: url but not sure if that is accurate. If it is perhaps we
should be representing node entities as asset:{hash} also."

**Learned.** Checked: it is not accurate for a node. `asset:` names
content in an archive's blob store, read through the blob lane and
recorded by a fact that a branch holds it. A node's block is kept in the
index catalog and read through the node layer. The two are different
stores of one archive, so the scheme is also what tells an accessor
where to look.

The maintainer's instinct has a place it does fit: what a node points at
in the blob store, a spilled value and a blob record, is an asset and
should be yielded as `asset:`, which today's resolvers do not do (a
spill is a base58 string, a blob record's hash is bytes).

**Decided.** By the maintainer: column 4 of
[design-resolvers](design-resolvers.md). The handle's scheme is not
decided; the comparison is in that file.

## 2026-11-13, Designing: the handle is `node:`; an accessor set is drafted

**Asked.** Is `node:` for nodes and `asset:` for blob-store content the
split you want? The maintainer: "ok node: it is".

**Learned.** Drafting the set ([design-accessors](design-accessors.md))
turned up what the earlier decisions already imply and what they leave
open. Implied: a segment's entries come from one accessor; `of` is the
entity; a spill and a blob record's hash are `asset:` entities and
absent when there is none; names are kebab-case. Open: seven points,
listed in the file, of which one (an entity per entry and per span)
waits on the strategy question of whether a node must be a concept.

**Decided.** By the maintainer: the handle is `node:`. The accessor set
is a draft.

## 2026-11-14, Designing to Building: the accessor set is decided

**Asked.** Must a node be reachable as a concept, or is asking the
accessors directly enough? Not answered in those terms. The maintainer:
"I think mostly makes sense. We should expose all info and not hide any;
as I said it's really a way to expose node info to the query engine. I
think folding manifest may make sense, I don't recall why it wasn't
before."

**Learned.** "Expose all" settles four of the seven open points at
once: the raw block is in, the three unexposed manifest fields are in,
a blob record's row holds everything with what does not apply absent,
and the stored fanout parameter is what is yielded. Why the manifest
was a resolver of its own could not be found: the note that introduced
the resolvers gives no reason and this clone's history is too shallow
to reach the commit.

The concept question, asked three times, was not answered directly. The
agent read the maintainer's description of the accessors' purpose as an
answer (they expose a node to the query engine; what is built on them is
the builder's) and wrote the approach on that reading, marked as a
reading, in [design-accessors](design-accessors.md) and on the agenda.
If it is wrong, what changes is one thing: whether a span and an entry
carry an entity.

**Decided.** By the maintainer: the accessor set, with everything
exposed and the manifest folded in. With that the approach is whole and
is written in the top story. Phase moves to Building. It rests on four
open hypotheses, each of which the build confirms or refutes in its
first days; none was measured here.

Nothing is built in this sitting.

## 2026-11-14, Building: what #795 does for the held decision

**Asked.** The maintainer: look at #795 to see whether it addresses
getting a command's response back and storing it in the session.

**Learned.** Read: the pull request's description and changelog entry,
`session.rs` and `session/invoke.rs` on its branch, and the note it
builds (`notes/layers.md`, #721). Not run.

- Storing it in the session: yes. An attribute placed on a
  memory layer is written there by a rule's head and read and
  subscribed to through the session. This replaces what the held
  decision called "a rule head that writes to the overlay", and it is
  declared by the space's schema, so a component can bring its own
  layer and placements as data.
- Getting the response back: no. #795 dispatches commands and runs
  rules; no effect is performed and no answer comes back. The note puts
  a command "answered with a result" under channels, which #795 lists
  as not done.
- It gives the pieces a handler could be made of (a subscription
  through the session, and `invoke` to start the next tick with the
  answer), which is a reading and not a test.

**Decided.** Nothing new. The held decision's second dependency is
met by #795 once it lands; its first is still to be built, and where
(on the session, or in dispatch) is a build decision.

## 2026-11-14, Building back to Designing: concepts must work too

**Asked.** Is the agent's reading right that accessors yield no entity
per span or entry? The maintainer: "It should work through concepts, as
in be complementary, independent of whether tonk-display is changed to
view resolvers directly." And on the name: "maybe fanout-factor is
better".

**Learned.** The reading was wrong and is struck. The accessors serve
both a direct query and a concept, and neither waits on the other.

What that asks of the design was checked by a test on the draft branch
before writing anything down (`it_reads_only_the_nodes_a_fact_names`):

- A rule whose body is a fact naming nodes and `tree/node` over them
  compiles, lists exactly the named nodes (2 block reads in a tree of
  21) and looks one up with 1. So the worry of 2026-11-12, that a rule
  over a step requiring its node cannot install, is answered without
  listing and without changing what a rule is: the rule names its nodes
  by a fact, and "held" facts are such a fact.
- Asked by `this` for a node no fact names: no row, and 1 block read.
  The planner ran the accessor first. Costs: an accessor is 200, a
  concept read with its entity given is more. If the fact is "held",
  that read is a fetch. This breaks what the held decision was for, and
  is new.

`fanout-factor`: a tree's branching factor is its fanout, so the name
would be read as the thing it is meant not to be. Put back to the
maintainer with `fanout-bits`.

**Decided.** By the maintainer: concepts and direct queries both. A
span and an entry carry an entity. Not decided: the three open points
now in [design-accessors](design-accessors.md).

**Backtracked.** From Building to Designing. The approach was declared
whole on the agent's reading of an answer the maintainer had not given;
the reading was wrong. Nothing had been built on it.

## 2026-11-14, Designing: what the layers session says about handlers

**Asked.** The maintainer asked that the use case be relayed to the
session that owns #795 and #721: a session command for a node's
locality answered with a command that rules capture in the session, and
tonk's command handlers expressible the same way, "ideally as a session
subscription that can do the job and then invoke a command back as a
response". Relayed; that session replied. What follows is its account,
not checked here.

**Learned.** Attributed to the layers session:

- A subscription cannot observe a dispatched command, and a request
  should not be made a fact so that one can. What reacts to a command
  with an effect is a handler.
- A handler is registered on the session per command concept, dialog's
  own or an embedder's, one mechanism. A dispatch runs its ticks to a
  fixed point and publishes; the commands the chain dispatched and no
  rule consumed are handed to the handlers
  (`TransactionBatch::induced()` holds them today); each answer is
  dispatched as a new invocation and starts the next chain. Handlers
  run after the publish, never inside the chain, so a lost
  compare-and-swap re-runs no effect.
- An answer is correlated with its request by the command's entity.
- `notes/layers.md` on #721 now lists this under Open.

It also proposed making "is this node held?" a query-time accessor that
never fetches, keeping only the fetch as a command. *Those two cannot
both be true:* that is the third column of
[design-held](design-held.md), which the maintainer set aside because
the same query would answer differently once a block arrived. The relay
had not told that session of the idempotence constraint; a correction
was sent. The proposal is recorded and not taken up.

**Decided.** Nothing. The maintainer's wish for a handler to be a
subscription is answered, by that session, as: not a subscription, a
handler run by the session's invoke loop.

## 2026-11-14, Building: the resolvers are built

**Asked.** The maintainer: continue and complete the implementation.

**Learned.** Built on the draft branch: the readings in `inspect` over
a decoded node; `rust/dialog-query/src/resolver.rs` rewritten as five
resolvers over a handle that ask `nodes::Read`; listing removed, the
handle required again. 1,745 tests in the three crates, the workspace
clippy gate and a wasm check of the two crates pass.

Three hypotheses of the ledger are settled by it: the decoders take a
node and a node keeps its block's size; a resolver binds a slot absent;
costing a resolver above reads of facts orders the plan.

Two things were learned that the design did not have:

- A test of entries found one where it expected a dozen. The tree was
  right: after a small commit most writes sit on the root index as
  buffered ops, and no resolver yields those, only their count. The
  design says nothing a node holds is hidden, and this is.
- Block reads stopped being a measure. Every node of a tree a replica
  wrote is kept by its environment, so reading one reads no block; the
  earlier tests that counted block reads were measuring the old
  resolvers' own loads. What a plan reads is now pinned on the plan.

Not built: the handler for commands (item 4), which is the session
work's; nothing on the tonk side, which waits on a dialog release
carrying this and on #795.

**Decided.** In building, by the agent, for the maintainer to overturn:
the row entities, the cost, `fanout-bits`, `of` as an entity only
([design-accessors](design-accessors.md), "As built").

## 2026-11-14, Building: on top of the session stack; the handler is built

**Asked.** The maintainer: the stack session is doing more work to align
with what was discussed here, and both its views and the maintainer's
have shifted; get its status, and build on top of it, not beside it.

**Learned.** From GitHub, not yet from that session, which was asked
and had not answered when this was written: the stack is #795 ← #797 ←
#800 ← #803 ← #805, with a further step on control layers and channels
in progress. #797 is the piece the held decision was waiting for:
`Session::handle` registers a handler that is offered a dispatch's
commands after it has published and whose answer is dispatched as the
next tick's stimulus. Its own test models a fetch of a node answered
with a command a rule captures on a memory layer.

The stack touches none of the resolver's files. `claude/layers-5` was
merged into the draft branch; the conflicts were the changelog and the
design index. Dialog 0.4.0 was released in between, so this topic's
changelog entries moved under the new Unreleased.

`Holdings` (`rust/dialog-repository/src/session/holdings.rs`) is the
handler for the two node commands, as decided in
[design-held](design-held.md): it answers with commands and asserts
nothing. A handler is offered commands and no environment, so it keeps
the one it was made with. Tested through a session: a probe of a held
node and of an unknown one are captured on a memory layer by a rule,
a fetch that finds the node nowhere leaves it not held, an entity that
is no node is not answered, and nothing is committed.

Not tested: a fetch that brings a node from a remote. Not known: what
shifted in the stack session's plan and the maintainer's.

**Decided.** Nothing new. The draft branch follows the stack from here.

## 2026-11-14, Building: what the stack session says shifted

**Asked.** That session, for its status, which branch to build on, and
what shifted. Its answer, by message, not checked beyond what GitHub
shows:

- Build on `claude/layers-5` as it stands; once a pull request is open
  on a branch it only adds commits. `claude/layers-6` (control layers
  and channels) is still being rewritten and is not to be built on yet.
- The maintainer decided, and `layers-6` implements, that
  `Session::handle`, `Handler` and the command feed go. A command lands
  in a `control:` layer for one tick; what answers it is a provider, a
  subscription over the command concept through a helper on the
  session, which answers by invoking a command naming the one it
  answers by its entity, after the dispatch it answers has published.
  This is the shape the maintainer first asked for here ("ideally a
  session subscription that can do the job and then invoke a command
  back as a response"), which that session had earlier said could not
  be.
- Locality stays a command: nothing shifted there.
- Nothing in the stack touches the resolvers or `dialog.branch/tree`.
  One near contact: #803 writes the tree's hash as base58 text in
  `dialog.layer/revision`, where `dialog.branch/tree` is now a `node:`
  entity.

**Learned.** `Holdings` was built on `Session::handle` an hour before
this arrived. What it answers was separated from how it is wired:
`Holdings::answers(&Changes) -> Changes` is the whole of it, and the
`Handler` impl is one line over it, so when the provider helper lands
the wiring changes and the answers do not.

*Those two cannot both be true*, to raise with the maintainer: the root
of a tree is a `node:` entity in `dialog.branch/tree` and base58 text in
`dialog.layer/revision`. A rule going from a layer's revision to its
tree would have to turn one into the other, and no formula does.

**Decided.** Nothing. The draft branch stays on `layers-5` until
`layers-6` is announced.

## 2026-11-14, Building: on `layers-6`; the node commands are a provider

**Asked.** Nothing new of the maintainer. The stack session announced
`claude/layers-6` (#807) pushed and verified, with the provider API.

**Learned.** As that session said it would: `Session::handle` and
`Handler` are gone; a published dispatch's commands stand in `control:`
for one tick; `Session::arrivals(layer, query)` yields the rows of a
query that newly arrive in a layer, each once; a provider answers by
invoking.

`claude/layers-6` was merged into the draft branch and `Holdings`
re-wired: `Holdings::serve(&session)` holds two readers of `control:`,
one per command, and `poll` answers what arrived in one dispatch. It
no longer keeps an environment of its own, since a provider is handed
one when it is polled. The commands, the answers and the test's
assertions did not change; the test gained two: the provider's own
answers are not commands to it, and nothing stays in `control:`.

`poll` does not wait. A provider that should run on its own needs a
loop around it, or a `next` that waits on either reader; neither is
built.

1,793 tests in the three crates, their doc tests, the workspace clippy
gate and a wasm check pass on the merge.

**Decided.** Nothing.

## 2026-11-15, Building: the fanout parameter's name

**Asked.** The maintainer: was `fanout-factor` a wrong name? Then, of
the three offered: "fanout-exponent sounds good".

**Learned.** Not wrong; open to two readings that are not `n`. The
agent had put `fanout-bits` in the code without the maintainer choosing
it, and said so.

**Decided.** By the maintainer: `fanout-exponent`. Renamed in the
resolver, its tests and the changelog.

## 2026-11-15, Building: on `layers-8`; what an embedder calls

**Asked.** The maintainer: is something missing in the stack's work for
a good integration story; then, raise those points with the stack
session; and tonk-labs/tonk#1318 moves tonk onto the stack and
dissolves its own machinery.

**Learned.** The stack had moved two steps since the last merge:
durable layers and `Session::sync` (#808), and what tonk's integration
found missing (#809): a session's transaction lands its commands, a
session opens through a read-only environment, and
`Session::arrivals_rows` reads everything that arrives in a layer.

tonk#1318 says how an embedder drives providers: it reads what a
dispatch left in `control:` under its own lock, inside the request, and
runs its registry then. It does not run a standing loop, because a loop
outlives the request and cannot tell which client asked. So what an
embedder needs of a provider dialog ships is a function from the
commands that landed to their answers, which `Holdings` had on
2026-11-14 and lost when it was re-wired.

It is back, as `holdings::answers(&branch, &commands, &env)`.
`Holdings` now reads `control:` through one `arrivals_rows` reader
instead of one reader per command, which also gives it a waiting form,
`next`.

Raised with the stack session, at the maintainer's word: nothing runs a
provider, so one dialog ships is not built in; an asker cannot wait for
its answer; there is no convention for a provider that fails; and a
tree's root is text in `dialog.layer/revision` and an entity in
`dialog.branch/tree`. Not raised, since #809 answered it: a provider
waiting on more than one command.

1,807 tests in the three crates, the workspace clippy gate and a wasm
check pass on the merge.

**Decided.** Nothing.

## 2026-11-15, Building: on `layers-15`; the node commands are two providers of a stack

**Asked.** By the stack session, at the maintainer's word: where do
tonk's `tree/*` formulas end up, and does anything have to stay a
formula. Answered from this topic's decisions: in dialog, as the
resolvers; locality as a provider; nothing stays a formula, and tonk's
tree arms are to be deleted, not ported. It also asked what entity a
tree and a revision are here: a tree is `node:` (`Entity::from_node`);
a revision's entity is the one main already has (`Revision::entity()`,
`did:key:z6Mk…`, from origin and edition), which this branch does not
touch. Whether revisions move to a `revision:` scheme is the
maintainer's, and was put to them.

**Learned.** The stack moved seven steps: a `Session` is a `Stack`
(`branch.stack()`, module `stack/`); `Stack::provide(Provider)`
registers a provider per command concept; performing an invocation runs
none, and its report's `effect()` is a step the embedder performs when
it chooses; a command's life is rows under `dialog.command/` (`invoked`,
`answered`, `failed`, `unhandled`, `progress`). Three of the four points
raised on 2026-11-15 are answered by it: a stack holds and runs its own
providers; a provider that fails has a convention; the embedder still
chooses when. Awaiting an answer was not looked for.

`claude/layers-15` was merged into the draft branch. `Holdings` as a
reader of `control:` is gone, as that session said it would be:
`holdings::providers(&stack, env)` is two `Provider`s, one per command
concept, and `holdings::provide` registers them. A provider is handed a
command and nothing to read through, so it keeps the environment it was
made with. An answer is written `.at(control_layer())`, which makes it
a command a rule captures and nothing keeps. A command naming no node
now fails with a reason where it used to go unanswered.

1,843 tests in the three crates, the crate's doc tests, the workspace
clippy gate and a wasm check pass on the merge.

That session also said the stack sits at +2.1 to +2.9% on the read
scenarios against a 3% gate, and asked that resolver cost stay off any
path a plain read takes. Nothing here is on such a path: a resolver runs
only where a query names one. The perf gate was not run on this branch.

**Decided.** Nothing. `claude/layers-16` (stack subscriptions) is in
progress; a further merge will follow.


## 2026-11-16: a revision is named by `revision:`

**Did.** The maintainer chose the `revision:` scheme for a revision's
entity over the `did:key:z6Mk…` it had, and left the compatibility
approach to this session. Read every place a revision's record is found
by entity, then changed the name and made each reader accept both.

**Learned.** The record is a stored fact, so the name is part of what a
replica holds, not only of what a query derives. Three readers find a
record by entity: the history reader
(`rust/dialog-corpus/src/history/query.rs`), the built-in projection
rules (`rust/dialog-repository/src/rules.rs`) and a stack's durable
layers (`rust/dialog-repository/src/stack/durable.rs`). `verify`
compares versions, not entities, so it needed nothing.

A second rule per concept for the older name would have made
`Revision` a two-rule concept, and the single-rule exact path was not
measured against that. One rule suffices: a formula,
`dialog/revision-slot`, yields the entities a revision's record may be
under, and the scan reads at those. Asked by name it is two lookups
where it was one; asked of no revision it is the same attribute scan.
A record at any other entity still joins nothing.

A fact a space wrote about a revision under its older name does not
join `this` any more. That is the cost of the change, and it falls on
tonk.

Tests: a record under the older name is read by the history reader; a
tree committed as an older release did projects by name and unnamed,
and a later commit finds it as a parent; the older name is no revision.
2,097 tests across `dialog-capability`, `dialog-corpus`, `dialog-query`
and `dialog-repository` pass. The perf gate was not run, and what an
older release does on meeting a `revision:` record was not tested.

**Decided.** `revision:<base58(hash)>` over the same hash; written
under the new name, read under either, nothing rewritten.
