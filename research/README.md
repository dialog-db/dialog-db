# Research

What we explore before we commit to it, and the measurements that decide
it. Each topic is one file, `<topic>.md`: the idea, its status and, once
the research is done, its report. While the research runs, its working
material sits beside it in `<topic>/`. The
[`research` skill](../.claude/skills/research/SKILL.md) is how a topic is
run. A research topic usually answers a question a
[design topic](../design/README.md) could not settle by reasoning.

## Layout

```
research/
  README.md               this index: every topic, with its status
  <topic>.md              the topic: the idea (question, hypothesis, what
                          success and failure look like), its status, and
                          the report once the research is done
  <topic>/                the working material, while the research runs
    journal.md            what was tried, measured and decided, newest last
    evaluation.md         the axes a decision is measured on, and where each stands
    design.md             the current decision, once there is one
    spike.md              a first round of measurements in full, if there was one
    results/              reports of runs, with their tables and charts
```

A topic has the working files it needs; a proposed one has none, and one
in progress always has a journal.

## Statuses

Every topic states one, at the top of its file:

- **Proposed:** written down, not yet picked up.
- **Exploring:** measuring whether the idea is worth building.
- **Building:** decided; being built, with the measurements that settle the
  open choices.
- **Evaluating:** built; being measured against what it would replace.
- **Landed:** merged. The topic's file ends with its report; what is worth
  keeping as reference moves to `notes/`.
- **Shelved:** not worth it. The topic's file ends with its report, saying
  why.

The status is given here, at the top of the topic's file and, while it
runs, in the journal's "Where things stand"; they change together.

## Topics

- [Graph queries](graph-queries.md): **Proposed.** A concept's entity field
  names the concept it points to, so a concept describes a graph, a query
  returns it and a subscription follows changes anywhere in it.
- [Record values](record-values.md): **Exploring**, on
  [dialog-db/dialog-db#748](https://github.com/dialog-db/dialog-db/pull/748).
  `Value::Record` as a typed, canonical value whose fields can be
  destructured in queries, sharing the replica's tokens with its claims.
- [Single-file format](single-file-format.md): **Building**, on
  [dialog-db/dialog-db#733](https://github.com/dialog-db/dialog-db/pull/733).
  A replica's blocks in one append-only file, every token stored once.
