# The tree and the repository name their environment

Phase: **Describing.** See the [design index](README.md) for what the
phases mean, and the [journal](effect-handlers/journal.md) for where the
topic stands.

## Description

A tree operation in `dialog-search-tree`, and the repository code above
it, perform commands on an environment they are given by reference. Each
operation's signature names what it needs of that environment as trait
bounds (`Env: Nodes<Key, Value>`, `Env: Provider<Load> + Provider<Hydrate>
+ Holds`, and so on), and the compiler binds each `perform` to the impl
those bounds pick. Since dialog-db/dialog-db#755, `Provider::execute`
answers with a `Resume`: the output at once, or a future that produces
it. Before that it was an `async fn` under `async_trait`, and the
maintainer measured one boxed future per command performed, hit or miss,
at 2 to 4% of the point-read scenarios in `dialog-perf`; the first fix was
a second trait on the request (`nodes::Ready`), which the maintainer
called "not exactly using capability model" and "a layering issue or flaw
in capability system design".

The maintainer says (2026-11-08) the intent for the capability system
was "something like [effing-mad](https://github.com/rosefromthedead/effing-mad)
or my [actor](https://github.com/gozala/actor), where sync / async would
not matter", with Unison's abilities as another inspiration, and that
"the whole perform thing meant to be stand-in for yield". After #755 the
maintainer says `Resume` "offers a compromise": it makes a provider's
answer sync or async without the caller caring, but the tree still names
its environment's type, and nothing suspends the tree for a handler to
resume. What the maintainer wants is "proper effects and indeed
decoupling of the tree/repo logic from the environment", on a separate
branch, and only "if it's plausible to do a proper inversion that
performs as good or better".

What exists today on the data side: `dialog_capability::Effect` is a
serializable, attenuable request routed by subject, `Capability<Fx>` is
its chain, `Fork<Site, Fx>` sends one to a remote, and `Command` is the
local-only kind the node requests are. On the dispatch side there is one
mechanism, `Provider<C>::execute` on a concrete environment type, with
composition by `derive(Provider)` field forwarding and by wrapper types
(`Metered`, `Counting`, `DeltaOverlay`, `QueryEnv`) that implement the
same trait.

Two things in the tree are written by hand that an inversion might
subsume, as observed in #755: the integration's read-ahead (`open_pending`)
gathers the nodes a window of changes will read and issues them together
so they cross the wire in one round trip; and every test environment in
the tree, repository, peer and query crates implements the provider
traits again to count, fault or record what the code under test performs.

## Problem

## Approach
