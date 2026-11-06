# Journal: collaborative text editing

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | Three apps hit lost or overridden concurrent edits; one never got as far as concurrent edits. Two builders gave up on concurrent editing; one is looking at Automerge. Ordered relations excludes text by design. | Describing. |
| **Agenda** | How each app stores its text, and what Dialog does to concurrent writes of it. Whether "dropped" and "overridden" are one mechanism. What unit people edit concurrently: a character, a line, a cell, a note. | Finish describing; then diagnose. |

## 2026-11-06, Describing: opening the topic

**Asked.** What is the situation? The maintainer: many users ask for
collaborative text editing and Dialog has no good solution for it at the
moment.

**Learned.** The request arrives as a feature. The ordered relations note
already rules collaborative text out of its own scope.

**Decided.** Not decided. Next: concrete requests.

## 2026-11-06, Describing: three concrete cases

**Asked.** One concrete request: who, building what, doing what instead?
Answer: three cases. A Tonk document editor where racing collaborators
drop changes; builder and maintainer accepted single editor. An
Obsidian-style notebook whose builder does not know how to allow editing
across replicas and is considering Automerge. The maintainer's
Observable-style playground, where concurrent editors override each other.

**Learned.** "Many users" is three apps, all close to the project. The
symptoms are worded three ways: dropped, overridden, and "don't know how".
The third is not a failure observed but a missing answer. The playground
edits cells, not one long text; the unit of concurrent editing may differ
per app.

**Decided.** Not decided. No real names in this topic.
