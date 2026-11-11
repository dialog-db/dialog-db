# Design

How we get from a complaint, a bug or a feature request to a problem worth
solving and an approach worth building, with the reasons written down.
Each topic is one file, `<topic>.md`: the top story, with its title,
description, problem and approach, and the phase it is in. Its working
material sits beside it in `<topic>/`. The
[`design` skill](../.claude/skills/design/SKILL.md) is how a topic is run;
the practice is Rich Hickey's
[Design in Practice](https://github.com/matthiasn/talk-transcripts/blob/master/Hickey_Rich/DesignInPractice.md).

A design topic decides what is worth building. A
[research](../research/README.md) topic measures a question a design topic
could not settle by reasoning; the two link to each other.

## Layout

```
design/
  README.md                 this index: every topic, with its phase
  <topic>.md                the top story: title, description, problem,
                            approach, and the phase it is in
  <topic>/
    journal.md              reflective inquiry, kept current, then an entry
                            per sitting, newest last
    claims.md               the claims ledger: what the topic rests on, each
                            a fact with evidence, a hypothesis with its test,
                            or an opinion with its owner
    glossary.md             terms the topic needs, defined once
    use-cases.md            what the user could accomplish were the problem
                            solved; How filled in the Design phase
    direction.md            the strategy decision matrix
    design-<decision>.md    one implementation decision matrix per decision
    diagrams/               of the problem first, then of the solution
```

## Phases

Every topic states one, at the top of its file:

- **Describing:** writing down the situation, without saying what the
  problem is.
- **Diagnosing:** from symptoms, or from a feature, to the cause.
- **Delimiting:** stating the problem succinctly, and its scope.
- **Directing:** use cases, approaches, a strategy decision matrix, a
  chosen direction.
- **Designing:** implementation decisions, each with its own matrix.
- **Building:** decided and being built; design continues as
  implementation decisions come up.
- **Landed:** merged. Terms that outlive the topic move to
  `notes/glossary.md`.
- **Dropped:** decided not to do, with why.

Backtracking moves a topic to an earlier phase and says so in its journal.

## Topics

- [A read follows the head it should have pinned](query-isolation.md):
  **Building.** Every unit of isolation (a query, a select, a poll, a
  transaction) reads one revision, and the types make a live read
  unwritable; the [journal](query-isolation/journal.md) records what the
  harness found on main.
- [The search tree's concerns are not expressed as capabilities](tree-node-reads.md):
  **Building.** The tree talks in nodes through capabilities it performs on
  the environment; caching, decoding and storage form belong to providers.
  The [plan](tree-node-reads/plan.md) is for whoever builds it.
- [A parameter holds one variable or one constant](patterns.md):
  **Designing.** A pattern in place of a term, over a composite value kept
  as a versioned record. The [plan](patterns/plan.md) is for whoever builds
  it.
