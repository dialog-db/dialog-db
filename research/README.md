# Research

Ideas from [`ideas/`](../ideas/README.md) that grew into longer research.
Each topic has a folder holding the idea, its results, the design it led
to, and a journal of what was tried, measured and decided, newest last.

Every topic states its status, one of:

- **Exploring:** measuring whether the idea is worth building.
- **Building:** decided; being built, with the measurements that settle the
  open choices.
- **Evaluating:** built; being measured against what it would replace.
- **Landed:** merged; the topic's notes move to `notes/`.
- **Shelved:** not worth it, for the reasons the journal gives.

## Topics

### [Single-file format](single-file-format/journal.md)

**Status: Building.** A replica's blocks in one append-only file of tokens
and frames, read in place through a mapping of the file. Built and measured
against main's archive: 66× smaller on tonk-team, cold reads level, cached
nodes a third of the memory, commits about 2.3 ms slower. Being decided:
where the block index lives (a hitchhiker tree leads on size); the
[evaluation](single-file-format/evaluation.md) that decides it, on queries,
writes, sync, memory and storage, is planned and not yet run.

[Idea](single-file-format/idea.md) ·
[spike](single-file-format/spike.md) ·
[design](single-file-format/design.md) ·
[evaluation](single-file-format/evaluation.md) ·
[journal](single-file-format/journal.md) ·
PR [#733](https://github.com/dialog-db/dialog-db/pull/733)
