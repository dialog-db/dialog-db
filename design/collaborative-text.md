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
- Those who passed were building collaborative editors: text was the
  whole product, so the missing answer was the dealbreaker.
- Two earlier spikes never landed: a CRDT document stored as a value,
  shelved for cost and for how much it changed the core; and a key with
  the document beside Dialog, never built.

"Support" is open: compose with a text CRDT library, use one from within
Dialog, or something built in.

## Problem

Developers want apps where several people edit the same text, peer to
peer and offline, and every replica converges to the same text without
losing anyone's edits. Dialog resolves concurrent writes to a text field
by last writer wins, so one edit is lost. Apps can shrink the loss by
splitting text into smaller fields, not remove it. So developers pick a
text CRDT library instead of Dialog.

Scope: Dialog replicates partially, so a database is always partial;
a peer may hold a whole document before editing it.

## Approach

?
