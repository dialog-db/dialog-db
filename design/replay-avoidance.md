# A commit that lost its branch's race replays even when nothing it read changed

**Phase: Describing.**

## Description

When two writers commit to one branch at once, the one that publishes
second finds the head moved and replays its operation over the latest head
([file-writers](file-writers.md), decided 2026-11-10). A replay re-runs the
operation's rules and builds its tree again, though often nothing the
operation read was touched by the other commit. Irakli wants a system that
captures what a transaction considered, so that a commit whose reads do not
overlap the other commit's writes skips the replay and publishes its writes
over the new head as they are.

## Problem

Not stated yet.

## Approach

Not chosen yet.


A commit that finds its branch's head moved replays only if what it read
changed. What it records as "what it read" decides how often it replays,
and whether it can miss a change.

| criterion \ approach | Key ranges: each scan's range of keys | Matched triples: the facts that ended up bound to variables (Irakli) | Ranges as a filter, patterns as the test: each scan's range and the pattern it matched with; the two trees are diffed over the ranges, and each changed fact is tested against the pattern |
|---|---|---|---|
| what a commit holds | two keys a scan | 🟡 every fact matched: a scan of 300 issues holds 300 | two keys and a pattern a scan |
| a change to a value it read | caught | caught | caught |
| a fact asserted into a range it scanned, which would now match (a phantom) | caught | 🔴 missed: no triple it read changed, yet the result would have; a rule picking a field's latest value (`Pick::Last`), or matching on absence, reads exactly this | caught: the new fact is in the diff, and matches the pattern |
| replays for a change it did not depend on | 🟡 any change in a wide range forces one | few | few: a changed fact in the range that does not match the pattern is passed over |
| what validating costs | the difference over the ranges | a lookup of each fact read in the new tree | the difference over the ranges (`TreeDifference` reads only the paths that differ), then one pattern test a changed fact |

In every case, the result of the check:

- nothing it read changed: its writes go onto the latest tree as they are,
  and it publishes with the new parent;
- something changed: it replays its operation over the latest head, and
  tries again.
