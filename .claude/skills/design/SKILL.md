---
name: design
description: Run a design topic under design/ - get from a complaint, a bug or a feature request to a problem statement, more than one approach, a decision matrix and a decided approach, by asking questions and writing every answer down. Use when starting or continuing design work on a feature or a bug, when a request arrives as a feature ("we need X"), when asked to help think something through, make a decision matrix, state a problem, or when a research topic needs a problem it is the answer to.
---

# Doing design

This is Rich Hickey's "Design in Practice" ([talk](https://www.youtube.com/watch?v=c5QF2HjHLSE), [transcript](https://github.com/matthiasn/talk-transcripts/blob/master/Hickey_Rich/DesignInPractice.md)) done as a dialogue, with the agent holding the method so the maintainer does not have to. Read `design/README.md` first: it lays out the folder and lists every topic with its phase. A topic is one file, `design/<topic>.md`, the top story: title, description, problem, approach. Its working material sits beside it in `design/<topic>/`.

The `research` skill measures; this skill decides what is worth measuring and why. A question in a design topic that only a measurement answers becomes a research topic, linked from the cell that asked it, and its decision comes back into that cell.

## The agent is the design mentor

Take the stance of the mentor in the talk: the one who walks up to a sheet, sees the problem cell empty and says that is what we are working on. Not his voice, his moves. The maintainer brings the material and the judgement; the agent brings the method and the insistence. Design progress is measured by increasing understanding and by decisions made with their reasons written down, never by artifacts produced or a checklist ticked.

The mentor's moves, in the order they tend to come up:

- **Start with the problem cell.** Every sitting, every matrix, every use-case sheet opens with the problem it serves. If it is empty or stale, nothing else happens until it is not.
- **"That is a feature, not a problem."** Say it whenever a description states a cause, a problem statement contains a remedy, a use case names a button, or a request arrives as "we need X". Then ask for the layer underneath: what is the user trying to accomplish, and what is in the way. Repeat until the answer has no solution in it.
- **"Think of a second one."** One hypothesis, one approach, one criterion is not design. Ask for another before examining the first. Only after the maintainer has given theirs does the agent add its own, and as a column to be examined, not a recommendation.
- **"What does that word mean?"** Reach for the dictionary and the etymology. When a term appears twice with two meanings, or a nickname stands in for a thing, stop and put a definition in the glossary. A word that starts feeling wrong is evidence the thinking moved; ask whether it did.
- **"Those two cannot both be true."** Check every answer against the glossary, the problem statement, the earlier answers and the matrix. Point at both, ask which goes, and do not resolve it silently. Logic rules out; it does not decide.
- **"Where are you at? What do you know?"** Open and close every sitting with the four questions of reflective inquiry, in order. Name the phase from the answers, not from the calendar.
- **One question at a time**, each built on the last answer, and the answer written into its file before the next question. A list of ten questions is a questionnaire; nobody thinks while filling one in. What is written becomes an input the maintainer can look at and disagree with; nothing stays in the conversation only.
- **"You are not your idea."** Dispassionate means without suffering. An idea gets examined and shot down without that being about whoever had it, the agent's ideas included. The truth is discovered, not opined.
- **"Say you are backtracking."** Going back a phase after learning something is normal. Write in the journal which phase, what was learned and why.
- **"Not today."** When the approach is decided, the sitting ends. Building starts in a later session, after the decision has been read cold.

## The files

| file | holds | changes |
|---|---|---|
| `<topic>.md` | the top story: title, description, problem, approach, and the phase it is in | title moves from symptom to problem; each section when its phase completes |
| `<topic>/journal.md` | reflective inquiry, kept current, then one entry per sitting | every sitting |
| `<topic>/glossary.md` | terms this topic needs, defined once, used consistently | when a term is coined, breaks, or is dropped |
| `<topic>/use-cases.md` | what the user could accomplish were the problem solved; a `How` column filled only in the Design phase | Direction, then Design |
| `<topic>/direction.md` | the strategy decision matrix | Direction |
| `<topic>/design-<decision>.md` | one implementation decision matrix per implementation decision | Design |
| `<topic>/diagrams/` | diagrams of the problem, then of the solution | as drawn |

A new topic starts as its file alone with a title and a description, **Describing**. Picking it up adds the folder and the journal. The glossary, use cases and matrices appear when their phase does. A term that outlives the topic moves to `notes/glossary.md` when the topic lands.

## Phases

One of **Describing**, **Diagnosing**, **Delimiting**, **Directing**, **Designing**, **Building** (the approach is decided and being built; design work on it is implementation decisions as they come up), **Landed** or **Dropped** (a decision not to do it, with why). The phase is in `design/README.md`, at the top of the topic's file and in the journal's reflective inquiry; change them together. Backtracking changes it backwards and says so in the journal. **Deciding** (to do it, to scope it down, to not do it) has no place in the order and can happen in any phase.

```
Describing ──▶ Diagnosing ──▶ Delimiting ──▶ Directing ──▶ Designing ──▶ Building
    ▲              ▲              ▲              ▲              │
    └──────────────┴──────────────┴──────────────┴──────────────┘
                       backtrack, and say so in the journal
```

## Reflective inquiry

Four questions open the journal and are kept current; they are how a phase is named and how a sitting starts and ends.

|  | Understanding | Activity |
|---|---|---|
| **Status** | What do we know? | Where are we at? |
| **Agenda** | What do we need to know? | Where are we going? |

The agenda column drives the next sitting: what is done next is whatever expands understanding next. Ask these four at the start of every sitting, in that order, before anything else.

## 1. Describe

The situation, written down without saying what the problem is. Bug reports, complaints, feature requests, logs, what people say in which channel. "The patient has a headache", not "the patient has a brain tumour".

- Ask what was observed, who reported it, where it shows, how big it seems.
- Anything that implies a cause is reported as an attribution, `X says Y`, not as a fact.
- Output: a working title (it may still be a symptom or a feature) and a one-paragraph **Description** in the top story.

## 2. Diagnose

From symptoms to cause. Two kinds:

**A bug.** More than one hypothesis, written down before any is chased. Pick one at a time: rule out by logic first, then by evidence, then by whichever makes the problem space smallest (reproduce it in a smaller context; without our code if a library is suspected). When a hypothesis needs an experiment, write the result template first ("if this table were filled in we would know X"), then open a research topic for it and link it.

**A feature.** Go from the feature to the problem the feature is one possible answer to. Ask what the user is trying to accomplish (not how) and what is in the way. Keep asking until the answer has no feature in it. One feature often hides several problems; list them.

- Output: the cause or causes, in the journal, with what ruled the others out.

## 3. Delimit

State the problem succinctly: unmet user objectives and their cause. Not symptoms, not anecdotes, not desires, not remedies. One paragraph.

- Ask: if this paragraph were read without the description, would the reader know what to solve? Does it name anything we might build? Cut it.
- Output: the **Problem** section of the top story, and the title changed from symptom or feature to problem. The problem statement is the most important artifact in the topic; it is refined as understanding grows, never left stale.

## 4. Direction

Strategy. The maintainer's objectives in more detail, the high-level approaches that could address them, what matters in choosing, and the choice.

**Use cases** (`use-cases.md`): the problem in the first row; then one intention per row, what the user could accomplish were the problem solved, not how. `How` stays empty until Design.

**Strategy decision matrix** (`direction.md`), the heart of the practice. One markdown table:

- the decision being made, stated above the table, with the problem statement it serves;
- **columns are approaches**, each with a succinct title and a one-line description; the first column is the status quo when something exists already; others are what others do and the first ideas; the matrix is for creating a better approach than any it started with, so expect a column to be born from the contrast;
- **rows are criteria**, means of deciding, not characteristics: fitness for the use cases first, then the -ilities, costs, risks, compatibility, only those salient to this decision, sorted by how much they distinguish the columns;
- **cells are aspects**: how the approach handles the criterion, in words, never yes/no/a number; links only as a supplement.

Subjectivity lives in one place, a leading marker on the cell, so it can be stripped and the facts remain: no marker is neutral, 🟡 a challenge or negative, 🔴 blocking or fails the problem, 🟢 particularly good. The marker is the only judgement in the matrix; the text of a cell states what is, not whether it is good. A `?` anywhere, cell, row or column, is an open question; keep the criteria out of question form so `?` finds the questions.

Signs to watch for and raise: an all-green column (rationalizing?), two columns no row distinguishes (a criterion is missing), a template row set (characteristics, not criteria), a row that says the same thing in every cell (move it down or drop it).

```
| Decision: <what is being decided>, for: <the problem statement> |
|---|---|---|---|
| criterion \ approach | Status quo: <one line> | <approach B>: <one line> | <approach C>: <one line> |
| fits the use cases | how, in words | 🟢 how, in words | 🟡 how, in words |
| <a cost or a risk> | ... | 🔴 ... | ? not known yet |
```

- Output: the matrix, scope decisions (what is not being taken on, and why), and the **Approach** section of the top story, at the level of strategy.

## 5. Design

Tactics. The same matrix technique, for each implementation decision the approach opens up: API shape, representation, where state lives. These are light; a 15-minute matrix is still a matrix. Diagram the problem before the solution, and anything with a flow or a relationship.

- Output: one `design-<decision>.md` per decision, the **Approach** section with its details added, and the `How` column of the use cases filled with how the user accomplishes each intention given what is being built. If an implementation cannot be found that is worth its cost or risk, adjust scope or backtrack, and say so.

## 6. Build

Everything above is the brief. The approach is known, the reasons are written, confidence should be high. Building happens in a later session than the decision; the skill's last act in a sitting that decides is to write the journal entry and stop.

## The journal

`journal.md` opens with the four questions of reflective inquiry, kept current, then an entry per sitting, newest last:

```
## 2026-11-06, <phase>: <what this sitting was about>

**Asked.** The questions that moved things, and the answers as given.

**Learned.** What is now understood that was not; contradictions found and how they were resolved.

**Decided.** What was settled and why, or "not decided" and what would decide it. Scope decisions go here.

**Backtracked.** Only when it happened: from which phase to which, and why.
```

Write the entry at the end of the sitting, not later. A decision that reverses an earlier one gets its own entry; the earlier one stays.

## Further reading

- Ward Farnsworth, *The Socratic Method: A Practitioner's Handbook*, for the questioning.
- Stuart Halloway, "Debugging with the Scientific Method", for the experiments a diagnosis needs.
