# dialog-stack

A **stack** is a branch and the layers its schema declares beside it,
read and written as one. This crate is where an embedder imports the
stack from. The code still lives in `dialog-repository`, and every name
here is a re-export of it: `dialog_stack::Stack` and
`dialog_repository::Stack` are the same type. When the code moves into
this crate, an embedder that imports from `dialog_stack` does not change
an import.

This README is written for an embedder who already knows dialog's facts,
rules and branches and wants to drive a stack. Every example is
`no_run`, since a stack needs storage, and every one compiles as a doc
test of this crate.

## What a stack is

A stack reads several layers as one composite, the way an image editor
shows every layer of a picture at once. One layer is the **primary**, the
branch the stack was opened on, whose schema declares the others. Every
stack also has three layers whatever its schema says: `memory:` is the
primary itself, `session:` is the stack's own in-memory store, gone when
the process is, and `control:` is the channel the stack invokes commands
through. A layer is declared by its **type**, a bare-scheme entity:
`session:` for a layer held in memory, `channel:` for a layer whose rows
live one tick, and `branch:` for a durable layer, a tree the primary
points at, named `memory:<name>`.

Where a row lies is a dimension of the row, `in`, next to `the`, `of`
and `is`. A write with no layer named lands in the primary, or in the
layer the stack has selected (`Stack::select`), except a write under
`dialog.`, which never follows the selection. A read with `in` left
unbound reads every layer and binds each row's layer. A read with `in`
bound reads that one layer.

A stack describes itself with facts. It writes the facts of its three
implied layers into `session:` when it opens. The declared layers are
facts in the primary's tree:

```text
; implied, written into session: when the stack opens
control:        dialog.layer/type        channel:
session:        dialog.layer/type        session:
memory:         dialog.layer/type        branch:
memory:         dialog.layer/branch      <the primary's branch entity>

; declared, stored in the primary's tree
session:tab     dialog.layer/type        session:
mail:box        dialog.layer/type        channel:
memory:notes    dialog.layer/type        branch:

; where a durable layer stands, in the primary, keyed by the layer
memory:notes    dialog.branch/revision   <its revision's entity>
memory:notes    dialog.branch/tree       "<its tree, base58>"
```

## Opening a stack

`branch.stack()` builds an [`OpenStack`], and `perform(&env)` opens it.
Opening reads the primary's schema at its head, builds every layer it
declares (a session layer empty, a durable layer at the tree the primary
points it at) and mints `session:`. Opening commits nothing,
so the environment only needs what a read needs: [`OpenStackEnv`] is
storage reads, archive and tree node loads, memory `Resolve`, `Identify`
(the stack derives the primary's branch entity from the operator's
profile) and the remote resolve a read may take. Invoking through the
stack needs [`StackEnv`], which adds what a commit and its publish need.

A layer is declared in the schema with [`LayerDeclaration`], committed
to the primary like any fact, so every replica of the branch opens the
same layers. A layer this process alone wants is declared on the stack
with [`OpenStack::declare`]: such a declaration reaches no layer and no
peer, and it outranks what the primary declares. Declarations are read
from the primary's tree, then the primary's session store, then the
stack's own, each one outranking the one before. A layer declared with
[`Stack::declare`] after the stack opened is not built.

[`Stack::layers`] lists the three implied layers first, then the session
layers and channels the schema declares, then its durable layers, then
the channels created at run time.

```rust,no_run
# use dialog_corpus::Entity;
# use dialog_repository::Branch;
# use dialog_stack::{LayerDeclaration, LayerType, StackEnv};
# use dialog_stack::{control_layer, memory_layer, session_layer};
# async fn example(branch: Branch, env: impl StackEnv) -> anyhow::Result<()> {
let tab: Entity = "session:tab".parse()?;
let notes: Entity = "memory:notes".parse()?;
branch
    .transaction()
    .assert(LayerDeclaration::new(tab.clone(), LayerType::Session))
    .assert(LayerDeclaration::new(notes.clone(), LayerType::Branch))
    .commit()
    .publish()
    .perform(&env)
    .await?;

// A layer this process alone holds, declared on the stack.
let scratch: Entity = "session:scratch".parse()?;
let stack = branch
    .stack()
    .declare(LayerDeclaration::new(scratch.clone(), LayerType::Session))
    .perform(&env)
    .await?;

let layers = stack.layers();
assert_eq!(layers[..3], [memory_layer(), session_layer(), control_layer()]);
assert!(layers.contains(&tab) && layers.contains(&notes) && layers.contains(&scratch));
# Ok(())
# }
```

## Reading

[`Stack::query`] reads the primary and every layer. The `in` of an
attribute query, or of a descriptor in a rule, says which layer it reads:
a variable binds each row's layer and reads them all, and a constant
reads one layer alone. A claim carries no layer: a read that asks where
a row lies names `in` as a variable and reads the binding, as the field
of a concept here (`memory:` for a row of the primary).

```rust,no_run
# use dialog_corpus::{Entity, Value};
# use dialog_query::attribute::Relation;
# use dialog_query::query::Output as _;
# use dialog_query::{AttributeQuery, Claim, ConceptQuery, Term};
# use dialog_stack::{OpenStackEnv, Stack};
# async fn example(stack: Stack, env: impl OpenStackEnv) -> anyhow::Result<()> {
// A variable `in`: every layer, each row's layer bound to `layer`.
let everywhere: ConceptQuery = serde_json::from_value(serde_json::json!({
    "assert": {
        "with": {
            "on": { "the": "ui/selected", "as": "Boolean", "in": { "?": { "name": "layer" } } }
        }
    },
    "where": {
        "this": { "?": { "name": "doc" } },
        "on": { "?": { "name": "on" } },
        "layer": { "?": { "name": "layer" } }
    }
}))?;
for row in stack.query().select(everywhere).perform(&env).try_vec().await? {
    let layer: Entity = row.get("layer")?;
    println!("{} in {layer}", row.entity());
}

// A constant `in`: `session:tab` alone.
let tab: Entity = "session:tab".parse()?;
let in_tab: Vec<Claim> = stack
    .query()
    .select(
        AttributeQuery::from(
            Term::<Relation>::from(Relation::from(
                "ui/selected".parse::<dialog_corpus::Relation>().expect("an attribute"),
            ))
            .of(Term::<Entity>::var("doc"))
            .is(Term::<bool>::var("on")),
        )
        .with_layer(Term::Constant(Value::Entity(tab))),
    )
    .perform(&env)
    .try_vec()
    .await?;
# let _ = in_tab;
# Ok(())
# }
```

A rule names a layer the same way, with `in` in its descriptors. This
rule reads a command's `cmd.fetched/node` from `control:` and keeps the
fact in `session:tab`, which a channel's row would not outlive:

```rust,no_run
# use dialog_query::InductiveRule;
# use dialog_stack::{Stack, StackEnv};
# async fn example(stack: Stack, env: impl StackEnv) -> anyhow::Result<()> {
let keep: InductiveRule = serde_json::from_value(serde_json::json!({
    "assert!": {
        "with": {
            "fetched": { "the": "node/fetched", "as": "Entity", "pick": "all", "in": "session:tab" }
        }
    },
    "when": [{
        "assert": {
            "with": { "node": { "the": "cmd.fetched/node", "as": "Entity", "in": "control:" } }
        },
        "where": {
            "this": { "?": { "name": "fetched" } },
            "node": { "?": { "name": "this" } }
        }
    }]
}))?;
// A rule lives under `dialog.`, so it lands in the primary.
stack.assert(keep).perform(&env).await?;
# Ok(())
# }
```

[`Stack::layer`] hands out one layer by its type, as a [`Layer`]:
[`Layer::Ephemeral`] for `session:` and the declared session layers,
[`Layer::Durable`] for `memory:` and the declared durable layers (its
name and the revision it stands at), and [`Layer::Channel`] for
`control:` and every other channel.

```rust,no_run
# use dialog_corpus::Entity;
# use dialog_stack::{Layer, Stack};
# fn example(stack: Stack) -> anyhow::Result<()> {
let tab: Entity = "session:tab".parse()?;
match stack.layer(&tab) {
    Some(Layer::Ephemeral(memory)) => println!("held in memory, at {:?}", memory.revision()),
    Some(Layer::Durable(durable)) => println!("a tree, at {:?}", durable.revision()),
    Some(Layer::Channel(channel)) => println!("a channel, at tick {}", channel.sequence()),
    None => println!("no such layer"),
}
# Ok(())
# }
```

## Writing: invoke and transact

Every write through a stack is an [`Invocation`], and performing it runs
one **dispatch**, the unit of publication. A dispatch runs the stack's
rules to a fixed point over the primary and every layer, one round of
induction per **tick**. It routes each write to its layer and publishes
the primary's share with one compare-and-swap against the head it began
at. Each layer takes its share only once that publish has won, so no
layer ever shows a commit that was refused. A dispatch that writes only
into memory layers publishes nothing. When the head moved under it, the
whole dispatch runs again from the new head, up to [`MAX_RERUNS`] times.

There are three ways to build one:

- [`Stack::assert`]: writes alone, with nothing read first (an import, a
  seed).
- [`Stack::transact`]: a closure that reads the stack and stages writes
  computed from what it read. It takes a closure, not facts, because a
  lost compare-and-swap runs the closure again from the new head on a
  fresh [`Attempt`], and drops what the lost attempt staged. A write
  computed from a read is only ever published against the head it read.
- [`Stack::invoke`]: a command, sent into `control:` (see the next
  section).

Performing an invocation returns an [`InvocationReport`]: the revision
it left the primary at (the unchanged head when nothing durable was
written), how many ticks it ran, and the commands it landed on
`control:`.

[`Stack::transaction`] is the same commit without the re-run, for a
caller that handles a lost head itself: its publish fails with
`PublishError::VersionMismatch` and lands nothing. A publish that wins
reports as an invocation does. `.commit().publish().perform(&env)`
returns an [`InvocationReport`], and so does a staged chain's
`batch.publish().perform(&env)` ([`StackBatch`]), naming the commands
every link of the chain landed. An embedder that resolves a command
against a head of its choosing, through a [`StackTransaction`], runs
the stack's providers over it as an invoker does, with
`report.effect().perform(&env)`.

```rust,no_run
# use dialog_corpus::{Entity, Statement as _};
# use dialog_stack::{Stack, StackEnv};
# async fn example(stack: Stack, env: impl StackEnv + Clone + dialog_common::ConditionalSend) -> anyhow::Result<()> {
let doc: Entity = "doc:1".parse()?;
let tab: Entity = "session:tab".parse()?;
// No layer named: the primary.
let report = stack
    .assert(dialog_query::the!("doc/title").of(doc.clone()).is("Draft".to_string()))
    .perform(&env)
    .await?;
println!("published {:?} in {} ticks", report.revision(), report.ticks());

// `.at(..)` names the layer.
stack
    .assert(dialog_query::the!("ui/selected").of(doc.clone()).is(true).at(tab))
    .perform(&env)
    .await?;

// A transaction built by hand, with a command written into `control:`.
let report = stack
    .transaction()
    .assert(dialog_query::the!("doc/title").of(doc.clone()).is("Final".to_string()))
    .assert(dialog_query::the!("doc/index").of(doc).is(true).at(dialog_stack::control_layer()))
    .commit()
    .publish()
    .perform(&env)
    .await?;
println!("landed {} commands", report.commands().len());
report.effect().perform(&env).await?;
# Ok(())
# }
```

A transact body reads through its attempt and stages what it decided:

```rust,no_run
# use dialog_common::ConditionalSend;
# use dialog_corpus::{Entity, Value};
# use dialog_query::attribute::Relation;
# use dialog_query::query::Output as _;
# use dialog_query::{AttributeQuery, Term};
# use dialog_stack::{Stack, StackEnv};
# async fn example<Env: StackEnv + Clone + ConditionalSend>(stack: Stack, env: Env) -> anyhow::Result<()> {
let counter: Entity = "counter:1".parse()?;
let reader = env.clone();
let report = stack
    .transact(move |tx| {
        let (env, counter) = (reader.clone(), counter.clone());
        async move {
            let value = AttributeQuery::from(
                Term::<Relation>::from(Relation::from(
                    "counter/value".parse::<dialog_corpus::Relation>().expect("an attribute"),
                ))
                .of(Term::<Entity>::from(counter.clone()))
                .is(Term::<u128>::var("value")),
            );
            let rows = tx.query().select(value).perform(&env).try_vec().await?;
            let n = match rows.first().map(|row| &row.is) {
                Some(Value::UnsignedInt(n)) => *n,
                _ => 0,
            };
            tx.assert(dialog_query::the!("counter/value").of(counter).is(n + 1));
            Ok(())
        }
    })
    .perform(&env)
    .await?;
# let _ = report;
# Ok(())
# }
```

## Commands, providers and effects

A **command** is a row of `control:` that lives for one tick: the tick
it is the stimulus of. [`Stack::invoke`] sends one into `control:` and
runs a dispatch, whose first tick reads it. A command can also be
written `.at(control_layer())`, or be the head of a rule whose
descriptor names `in: control:` (or whose concept carries the
`dialog.concept/transient` marker); such a command is the stimulus of
the tick after the one that wrote it. After its tick it is in no layer.
A rule premised on it copies what it wants into a layer that keeps it.
Once the dispatch has published, and only then, its commands land on the
outside end of `control:` as one tick, and the report names them
([`InvocationReport::commands`]). A dispatch re-run from a moved head
therefore runs no effect twice.

Every change in a command's life is a row of the command's entity in
`control:` for one tick:

```text
<cmd>  dialog.command/invoked    <tick>          ; the tick of its dispatch it is stimulus of, from 1
<cmd>  dialog.command/answered   <concept>       ; a provider answered it, one row per provider
<cmd>  dialog.command/failed     "<reason>"      ; a provider failed, one row per provider
<cmd>  dialog.command/unhandled  true            ; no provider claims it
<cmd>  dialog.command/progress   <value>         ; a provider's own intermediate answer
```

A **provider** performs the commands of its concepts: a function from
the rows of one landed command to the statements that answer it.
[`Stack::provide`] registers one, and the first registration of a
concept writes `session: dialog.provider/command <concept>`, one row per
concept, so what this process can perform is a row any query can read.

A provider claims a command when the command's rows satisfy its
concept, the same test decoding the concept from those rows would pass:
every required field has a row of the field's attribute and, when the
field declares a type, a value of that type (as is, or through a number
view). Optional fields are not needed, and rows of attributes outside the
concept do not disqualify. Sharing one attribute with a concept is not
satisfying it, so two concepts that both carry a time stamp never claim
each other's commands. When the concepts of several providers are each
satisfied by one command, every one of them runs, in the order they were
registered, and each writes its own `answered` (or `failed`) row.

One provider can answer several concepts, a command's current shape and
its legacy one: `Provider::new(&current, handler).also(&legacy)`. It
claims a command that satisfies any of them, runs once for it however
many it satisfies, and its `answered` row names the first in its own
order.

The rows of the `control:` entity itself are no command: they are the
**tick**'s own facts, who asked and whatever else an embedder stamps on
the loopback beside the commands, such as `control: tonk.session/client
client:<id>` written `.at(control_layer())`. The report holds them apart
([`InvocationReport::tick`]), no provider claims them, no effect marks
them `unhandled`, and every command of the tick carries them
([`Command::tick`]).

Performing an invocation runs no provider. [`InvocationReport::effect`]
is a separate step, which the embedder performs when it chooses: after
it has answered a request (a worker's `waitUntil`), or never.
[`Effect::perform`] runs the provider of each command, one after
another, then writes every answer, with each command's lifecycle row, in
one more dispatch. The [`EffectReport`] says which commands were
answered, which failed and why, which no provider claimed, and whether
the answer dispatch landed commands of its own
([`EffectReport::remaining`]). An answer placed `.at(control_layer())` is
such a command, and the next effect runs it. The embedder drives that
chain and decides when to stop. A provider that fails answers with the
[`Failure`] and does not stop the others. A command no provider claims
is answered `unhandled`, which is not an error.

```rust,no_run
# use dialog_corpus::{Changes, Entity, Statement as _, Value};
# use dialog_query::{Attribute, Concept, ConceptDescriptor, Descriptor};
# use dialog_stack::{Failure, Provider, Stack, StackEnv};
/// The node a fetch command asks for (`cmd.fetch/node`).
#[derive(Attribute, Clone, PartialEq)]
#[domain("cmd.fetch")]
pub struct Node(pub Entity);

/// A command to fetch a node.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Fetch {
    /// The command.
    pub this: Entity,
    /// The node it fetches.
    pub node: Node,
}

# async fn example(stack: Stack, env: impl StackEnv + Clone + dialog_common::ConditionalSend) -> anyhow::Result<()> {
let tab: Entity = "session:tab".parse()?;
let fetch = <Fetch as Descriptor<ConceptDescriptor>>::descriptor();
stack.provide(Provider::new(fetch, move |command| {
    let tab = tab.clone();
    async move {
        let Some(Value::Entity(node)) = command.rows().first().map(|row| row.is.clone()) else {
            return Err(Failure::new("a fetch names its node"));
        };
        // ... fetch the node ...
        let mut answer = Changes::new();
        dialog_query::the!("node/fetched")
            .of(node)
            .is(command.entity().clone())
            .at(tab)
            .assert(&mut answer);
        Ok(answer)
    }
}))?;

let command: Entity = "cmd:1".parse()?;
let node: Entity = "node:1".parse()?;
let report = stack
    .invoke(dialog_query::the!("cmd.fetch/node").of(command).is(node))
    .perform(&env)
    .await?;
// The request can be answered here; the effect runs after.
let mut effect = report.effect().perform(&env).await?;
for (command, reason) in effect.failed() {
    eprintln!("{command} failed: {reason}");
}
while effect.remaining() {
    effect = effect.effect().perform(&env).await?;
}

// A command no provider claims is answered `unhandled`.
let report = stack
    .invoke(dialog_query::the!("cmd.print/page").of("cmd:2".parse::<Entity>()?).is(1u128))
    .perform(&env)
    .await?;
let effect = report.effect().perform(&env).await?;
assert_eq!(effect.unhandled(), ["cmd:2".parse::<Entity>()?]);
# Ok(())
# }
```

A provider that wants to say how far it got calls
[`Command::progress`], an invocation that writes the command's
`dialog.command/progress` row into `control:` for one tick.

## Channels

A **channel** ([`Channel`]) is a layer whose rows live one tick, with
two ends. What the outside end sends ([`Channel::send`]) is read by the
next dispatch through the stack, at its first tick, as rows of the
channel, and by no tick after it. A send made while a dispatch runs is
taken by the next dispatch. What a dispatch writes into the channel
(`.at(channel)`, or a rule head naming it) is the next tick's stimulus
inside the dispatch. Once the dispatch has published, it lands on the
outside end as one tick of the channel's log, which an [`Arrivals`]
reader reads from its own position, each tick once, oldest first. A
publish that loses its compare-and-swap lands nothing.

[`Stack::channel`] returns the channel of a name, creating it if the
stack holds none, and records `<name> dialog.layer/type channel:` in
`session:`. Any entity outside `memory:` and `session:` names a channel.
A channel can also be declared in the schema with [`LayerType::Channel`].

`control:` is the built-in channel, the loopback: [`Stack::invoke`]
sends into it, and what lands on its outside end is what providers
answer. It is the only channel that hands back what it was sent: a row
sent into any other channel is read inside alone.

```rust,no_run
# use dialog_corpus::{Changes, Entity, Statement as _};
# use dialog_stack::{Stack, StackEnv, control_layer};
# async fn example(stack: Stack, env: impl StackEnv) -> anyhow::Result<()> {
let mailbox: Entity = "mail:box".parse()?;
let channel = stack.channel(&mailbox)?;
let mut outbox = stack.arrivals_rows(&mailbox).expect("the stack holds the channel");

// Outside to inside: the next dispatch reads the message as a row of `mail:box`.
channel.send(
    dialog_query::the!("mail.message/subject")
        .of("message:1".parse::<Entity>()?)
        .is("Hello".to_string()),
);
stack.assert(Changes::new()).perform(&env).await?;

// Inside to outside: a write into the channel lands on its outside end.
stack
    .assert(
        dialog_query::the!("mail.message/subject")
            .of("message:2".parse::<Entity>()?)
            .is("Re: Hello".to_string())
            .at(mailbox),
    )
    .perform(&env)
    .await?;
for row in outbox.rows() {
    println!("sent out: {} {} {:?}", row.of, row.the, row.is);
}

// The loopback reads the same way: this waits for the next dispatch
// through the stack to land commands.
let mut commands = stack.arrivals_rows(&control_layer()).expect("every stack has control:");
let landed = commands.next_rows().await;
# let _ = landed;
# Ok(())
# }
```

A consumer that performs one concept reads through a query instead,
with [`Stack::arrivals`]: `Arrivals::poll` reads what has arrived, and
`Arrivals::next` waits for the next dispatch to publish.

## Subscriptions

[`Stack::subscribe`] registers the stack's subscription to a query, or
attaches to the one it holds, and returns a [`Subscriber`]: every
reader of one query reads one evaluation, keyed by the query's
[`QueryHash`] and its type. The evaluation is
dialog's demand-gated [`Subscription`], the one [`Stack::subscription`]
hands out unshared.

A dispatch through the stack that published, or that wrote into a
memory layer (`session:`, a session layer), schedules a poll; a write
into a channel does not. A write the stack does not see, into the
primary's session overlay among them, is scheduled with
[`Stack::schedule_poll`]. [`Stack::settle`] polls every subscription of
the stack once when a poll is scheduled, delivers what moved to each
reader, and clears the mark, so a commit and a write into a memory
layer in one request cost one evaluation, and a second `settle` does
nothing. [`Effect::perform`] settles after its answer dispatch: an
embedder that drives effects need not call `settle` itself.

A reader's first delivery is the whole result set, as `asserted`, and
each later one is the [`Delta`] a poll made; [`Subscriber::results`]
reads the whole current set. The stack holds its subscriptions weakly:
one goes when its last reader is dropped or closed.

```rust,no_run
# use dialog_corpus::Entity;
# use dialog_query::attribute::Relation;
# use dialog_query::{AttributeQuery, Term};
# use dialog_stack::{Stack, StackEnv};
# async fn example(stack: Stack, env: impl StackEnv + Clone + dialog_common::ConditionalSend) -> anyhow::Result<()> {
/// `ui/selected` of every entity.
fn selected() -> AttributeQuery {
    AttributeQuery::from(
        Term::<Relation>::from(Relation::from(
            "ui/selected".parse::<dialog_corpus::Relation>().expect("an attribute"),
        ))
        .of(Term::<Entity>::var("doc"))
        .is(Term::<bool>::var("on")),
    )
}

// Two readers of one query share one subscription.
let mut tab = stack.subscribe(selected());
let mut panel = stack.subscribe(selected());
assert_eq!(tab.hash(), panel.hash());

// A dispatch schedules a poll; one settle evaluates once and delivers to both.
stack
    .assert(dialog_query::the!("ui/selected").of("doc:1".parse::<Entity>()?).is(true))
    .perform(&env)
    .await?;
stack.settle(&env).await?;
for delta in tab.poll() {
    println!("+{:?} -{:?}", delta.asserted, delta.retracted);
}
let everything = panel.results();

// A reader that waits is woken by the next settle that delivers.
let next = tab.next().await;
# let _ = (everything, next);
# Ok(())
# }
```

## Durable layers

A layer declared with [`LayerType::Branch`], named `memory:<name>`, is
durable. It has no head of its own: it is the tree the primary points
at. The primary holds two facts of the layer's entity, written together
under `last`: `dialog.branch/revision`, naming the entity of the revision
the layer stands at, and `dialog.branch/tree`, that revision's tree hash
in base58. While the primary names nothing, the layer is empty.

A write into the layer is minted as the layer's next tree, built on the
tree the primary pointed at when the dispatch began. The dispatch's
commit of the primary carries the new pointer facts, so there is one
commit and one compare-and-swap, the primary's. A lost one leaves the
minted tree an orphan that nothing names, and the dispatch runs again,
reading the layer where the new head points it. The primary's history
therefore says which tree of each durable layer goes with each of its
own commits.

[`Stack::sync`] pulls and pushes the primary and carries the pointed
trees with it. The pull downloads, as its [`Download`] says, the trees
the new head points at before the primary's head moves, so a replica
that goes offline right after it can still read every layer. The push
uploads the pointed trees before the primary's head, so a replica that
pulls the head finds every tree upstream. When two replicas each moved
a durable layer, the pull leaves the primary's pointer cells holding a
claim of each. Rather than let `last` pick one, the sync replays this
replica's changes to the layer since the two met onto the other's tree,
as a branch pull rebases local commits: our writes win on the cells they
touch, and theirs stand elsewhere. It then points the primary at the
merged tree, as a dispatch. Memory layers are not synced.

```rust,no_run
# use dialog_corpus::{Entity, Statement as _};
# use dialog_stack::{Download, Stack, SyncEnv};
# async fn example(stack: Stack, env: impl SyncEnv) -> anyhow::Result<()> {
let notes: Entity = "memory:notes".parse()?;
stack
    .assert(
        dialog_query::the!("doc/note")
            .of("doc:1".parse::<Entity>()?)
            .is("Remember the milk".to_string())
            .at(notes.clone()),
    )
    .perform(&env)
    .await?;
let at = stack
    .layer(&notes)
    .and_then(|layer| layer.durable().and_then(|durable| durable.revision().cloned()));
println!("memory:notes stands at {at:?}");

stack.sync(Download::QueriedRanges).perform(&env).await?;
# Ok(())
# }
```

## Backup and restore

`session:` and the declared session layers live in memory and are gone
with the process. [`Stack::backup`] takes them as a [`Backup`], which
serializes with any serde format. `branch.stack().restore(backup)`
([`OpenStack::restore`]) opens a stack on the same branch holding them
again, in this process or in another one. A backup taken of a stack on
another branch is refused with `CommitError::ForeignBackup` before any
layer is built.

A backup holds:

- every row of `session:` and of each declared session layer, tombstones
  among them, with the revision each layer stood at;
- the primary's branch entity, and the revision the stack last recorded
  the primary at.

It does not hold the channels, whose rows live one tick, or the durable
layers, which a restored stack reads where the primary points them then.
It does not hold the primary's session overlay (`Branch::overlay`)
either: the overlay belongs to the embedder, and the stack never writes
into it.

Restoring replays each layer's rows, then recomputes the rows that
describe the process rather than the session. A channel created at run
time and a provider's discovery row are not restored: create the channel
and register the provider again. A layer the schema no longer declares
is dropped with its rows. A layer that stood where its revision row said
when the backup was taken is recorded where its replayed rows leave it.
One that had moved without being recorded is the first dispatch's
stimulus, and so is the primary when it moved while the backup was away
(a commit or a pull through another handle). A rule premised on such a
layer's revision fires once.

```rust,no_run
# use dialog_repository::Branch;
# use dialog_stack::{Backup, Stack, StackEnv};
# async fn example(stack: Stack, branch: Branch, env: impl StackEnv) -> anyhow::Result<()> {
let bytes = serde_json::to_vec(&stack.backup())?;
// ... carried to another worker, which opens its own handle on the branch ...
let backup: Backup = serde_json::from_slice(&bytes)?;
let stack = branch.stack().restore(backup).perform(&env).await?;
# let _ = stack;
# Ok(())
# }
```

## Facts a stack writes, by layer

| Attribute | Of | Lives in | Written by | When |
|---|---|---|---|---|
| `dialog.layer/type` | `memory:`, `session:`, `control:` | `session:` | the stack | when it opens |
| `dialog.layer/type` | a declared layer | `memory:` (the primary's tree) | the embedder, with [`LayerDeclaration`] | when it commits the declaration |
| `dialog.layer/type` | a channel created at run time | `session:` | [`Stack::channel`] | when it creates the channel |
| `dialog.layer/branch` | `memory:` | `session:` | the stack | when it opens; names the primary's branch entity |
| `dialog.layer/revision` | `session:`, `control:`, each declared session layer and channel | `session:` | the stack | when it opens, then after a dispatch or a sync after which the layer stands somewhere else |
| `dialog.branch/revision` | the primary's branch entity | `session:` | nobody: it is the branch metadata, read as a row of `session:` | always current |
| `dialog.branch/revision` | `memory:<name>` | `memory:` | the dispatch that writes into the layer, or the sync that merges it | in the primary commit that moves the layer |
| `dialog.branch/tree` | `memory:<name>` | `memory:` | beside `dialog.branch/revision` | in the same commit |
| `dialog.session/layer` | `session:` | `session:` | [`Stack::select`] | the dispatch it runs; it takes effect from the next one |
| `dialog.command/invoked` | a command | `control:`, one tick | the dispatch's own commit | in the tick the command is stimulus of |
| `dialog.command/answered` | a command | `control:`, one tick | the effect's answer dispatch | a provider answered |
| `dialog.command/failed` | a command | `control:`, one tick | the effect's answer dispatch | the provider failed |
| `dialog.command/unhandled` | a command | `control:`, one tick | the effect's answer dispatch | no provider claims it |
| `dialog.command/progress` | a command | `control:`, one tick | [`Command::progress`] | when the provider reports |
| `dialog.provider/command` | `session:` | `session:` | [`Stack::provide`] | the first registration of each concept |

The attributes are exported as functions, so a query or a rule names
them without spelling them out: [`layer_type_attr`],
[`layer_branch_attr`], [`revision_attr`], [`selection_attr`],
[`invoked_attr`], [`answered_attr`], [`failed_attr`],
[`unhandled_attr`], [`progress_attr`] and [`provided_attr`]. Rows in
`session:` and `control:` are process-local: none of them is stored or
synced.
