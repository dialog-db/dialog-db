# One notation for queries, rules and transactions

Status: Exploring, on [dialog-db/dialog-db#728](https://github.com/dialog-db/dialog-db/pull/728). Its [journal](notation/journal.md), [evaluation](notation/evaluation.md), [current design](notation/design.md) and [results](notation/results) live in [`notation/`](notation). See the [research index](README.md) for what the statuses mean.

## Idea

Dialog has a formal notation for schemas ([notes/notation.md](../notes/notation.md)) and no notation for what people and agents actually write against a replica: questions, writes, rules and commands. Tonk spells those in YAML today, with workarounds for what the YAML cannot say: anchor-less assertions for derived identity, transient flags for commands, and rules that restate a body for every head.

The idea is one notation for all four, small enough that a reader who has never seen it reads a document right the first time. A question is a call, a write is the same call with `!`, a rule is heads and a `when`, and a definition is installed with the same `!` a write has. Lifetimes go on attributes, as layers, and commands are ordinary concepts in the layer that lasts one tick.

What "reads right" means is not a matter of taste, so it is measured: the least capable model we can get is given a sketch and a task, and what it writes and predicts is scored. If the cheapest reader gets it, people and stronger agents will too. If it does not, the spelling is hiding something.

## The question

Which spelling of entity derivation, rules, lifetimes and commands does a reader who has never seen the notation get right from the text alone?

## Hypothesis

A form is read right when what it does is written where it happens. A reader cannot see a derivation in a line that does not say "hash" or "derive", or a lifetime in a form that never names one. So every special behavior needs a visible word at the definition or at the call, and a short rule in the spec does more than a clever spelling.

## Measured against

The spec as the PR first had it: a `(default ?x is: {…})` line for derived entities, `(assert head premises…)` with `!` on the head's call for timing, and `transient: true` on a command's fields. R1 variant A and R4 variant V0 are that baseline, read by the same kind of reader as every variant.

## Bars

These were written after rounds 1 to 15 had run, from the scoring each round used at the time (see the journal's first entry). They are not moved again.

- **Success:** at least four of five readers (or three of three) do the task right and say why, with the spec's rule text in front of them: a write that creates leaves the entity out, two devices' writes converge on one entity, an update names the entity, a query finds by field.
- **Failure:** one in three or fewer: the spelling is dropped, whatever its other merits.
- **The middle:** a spelling that passes but the maintainer rejects on design grounds (it overloads a form, it reads as a call that passes nothing, it puts construction in a concept). The journal records both the numbers and the reason.

Readers are `claude-haiku` subagents. Three per variant is the least; a difference of one reader in three is noise.

## The second question: how to write it

Added on 2026-11-07, before R16 ran. With the notation held fixed, which way of writing its spec lets the same reader write and predict correctly: the spec as it is, a reference card, a walkthrough, or a card with the mistakes earlier readers made? The hypothesis is that the spec's length, rationale and evidence get in a cheap reader's way, and that a short document with examples does better. The bars, the rubric and the four documents are in [`results/r16-plan.md`](notation/results/r16-plan.md), committed before any reader ran: a style wins with a median of at least 13 of 15 points and at least 2 above the spec's, and loses if none beats the spec's median by more than 1.

## Prior art

- [Dedalus](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2009/EECS-2009-173.html): now, next and async as rule timings.
- [Bloom](http://bloom-lang.net) and Bloom^L: collections, merge operators, channels, lattices. The [design](notation/design.md#prior-art) has what it changed.
- [Datomic](https://docs.datomic.com): temporary ids, transaction functions, a query over several sources.
- [Unison](https://www.unison-lang.org)'s remote abilities: sending a computation by hash.
- [conduct](https://github.com/Gozala/conduct): labeled arguments and one primary.
