# A consumer that can only query cannot draw a branch's search tree: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | *What do we know?* The problem, accepted. What is held is decided: two commands with handlers built into dialog, each answering with a second command that a rule in the space places. That a rule's head is committed or transient today, never the overlay. That dialog dispatches commands as transient facts and reacts with inductive rules at commit, and has no built-in reaction that performs an effect. Use cases are drafted (nine). A strategy matrix exists with six columns, the sixth born from the decision on what is held. | *Where are we at?* Directing. The matrix is written and not read by the maintainer. |
| **Agenda** | *What do we need to know?* Whether the maintainer takes column 4 of [design-resolvers](design-resolvers.md) (asked). If so, the accessor set: which steps, and what a row of each holds. Whether a tree node has to be reachable as a concept, or whether a consumer asking a resolver directly is enough (asked 2026-11-13, not answered): every difficulty in columns 2 to 5 of the strategy comes from the concept. For [design-resolvers](design-resolvers.md): what a decoding formula's row is; whether the block travels as bytes or as a record. Whether writing to the overlay from rules is its own topic. Whether the use cases are right (asked, not answered). For column 6 of the strategy and for the held decision alike: how dialog reacts to a command with an effect and answers with a command. For column 5: whether a record reaches a page whole. For column 4: what a decoding formula's rows would be, which the maintainer has not said. Which rows the maintainer weighs most. | *Where are we going?* The maintainer reads the use cases and the matrix; the open cells are checked; a direction is chosen. |

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
