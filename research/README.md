# Research

What we explore before we commit to it, and the measurements that decide
it. An idea starts in [`ideas/`](ideas/); once there is something to
measure it becomes a topic of its own here, with its results, the design
it led to and a journal of what was tried, measured and decided. The
[`research` skill](../.claude/skills/research/SKILL.md) is how a topic is
run.

## Layout

```
research/
  README.md               this index: every idea and topic, with its status
  ideas/<name>.md         an idea: the question, the goal, what success looks like
  <topic>/
    idea.md               the idea, moved here from ideas/ when it is picked up
    journal.md            what was tried, measured and decided, newest last
    evaluation.md         the axes a decision is measured on, and where each stands
    design.md             the current decision, once there is one
    spike.md              a first round of measurements in full, if there was one
    results/              reports of runs, with their tables and charts
    charts/               charts a report draws as SVG
```

A topic has the files it needs; only `idea.md` and `journal.md` are
always there.

## Statuses

Every idea and topic states one:

- **Proposed:** written down, not yet picked up.
- **Exploring:** measuring whether the idea is worth building.
- **Building:** decided; being built, with the measurements that settle the
  open choices.
- **Evaluating:** built; being measured against what it would replace.
- **Landed:** merged; what is worth keeping moves to `notes/`.
- **Shelved:** not worth it, for the reasons the journal gives.

The status is given here, at the top of the idea and in the journal's
"Where things stand"; they change together.

## Ideas

- [Graph queries](ideas/graph-queries.md): **Proposed.** A concept's
  entity field names the concept it points to, so a concept describes a
  graph, a query returns it and a subscription follows changes anywhere
  in it.
- [Single-file format](ideas/single-file-format.md): **Building**, on
  [dialog-db/dialog-db#733](https://github.com/dialog-db/dialog-db/pull/733).
  A replica's blocks in one append-only file, every token stored once.

## Topics

None has landed here yet: a topic moves in with the pull request that
carries its research.
