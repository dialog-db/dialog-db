# Collaborative text editing

Phase: **Directing**

## Description

Developers want to build applications with collaborative text editing,
come to Dialog, and we cannot tell them how to do it.

- Most of them evaluated Dialog, saw no answer for text, and picked a
  text CRDT library instead. They never built on Dialog, so we have no
  usage from them, only the gap they saw.
- Three builders did build on Dialog and hit it: a Tonk document editor
  (racing collaborators drop changes; single editor accepted), an
  Obsidian-style notebook (no known way to edit a note across replicas;
  builder considering a text CRDT library alongside), and the
  maintainer's Observable-style playground (concurrent editors override
  each other).
- Earlier spikes (some in notes, some in unmerged PRs) never landed;
  the maintainer says the tradeoffs stacked up against them.

"Support" is open: compose with a text CRDT library, use one from within
Dialog, or something built in.

## Problem

Developers want apps where several people edit the same text, peer to
peer and offline, and every replica converges to the same text without
losing anyone's edits. Dialog resolves concurrent writes to a text field
by last writer wins, so one edit is lost. Apps can shrink the loss by
splitting text into smaller fields, not remove it. So developers pick a
text CRDT library instead of Dialog.

## Approach

?
