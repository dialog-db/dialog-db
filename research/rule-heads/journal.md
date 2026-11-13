# Rule heads: journal

## Where things stand

**Exploring.** Three sittings are in.

- At 500 commands a commit ([results](results/2026-11-12.md)) giving the condition a name costs 22% to 39% more instructions than one rule with a wide head, which stands in for a rule of several heads, and writing the body again a head costs 61% to 148% more.
- At one command a commit ([results](results/2026-11-12-one-command.md)) the order is the same and the three are closer: `named` 17% to 28% over `wide`, `repeat` 27% to 87% over, of a commit that costs 2.9 to 4.0 million instructions with no rule.
- With three prototype changes to induction on ([results](results/2026-11-12-induce-cost.md), branch `spike/induce-cost`), `repeat` is 1% to 7% over `wide` in the corner cells and every shape is 13% to 69% cheaper. `named` stays 21% to 43% over: its second round is untouched.

Both of the first two are between the topic's bars, so it is the maintainer's to weigh; the third says most of what `repeat` costs is recoverable without a rule of several heads. Next, if he wants it: the cells between the corners, a sound version of evaluating a shared body once, and the design topic the measurements turned up (a seed that leaves its subject free, dialog-db/dialog-db#789).

## Datasets

None on disk. Each scenario seeds its own to-dos, lists, owners and teams from an index.

## 2026-11-12, what three ways of writing on one condition cost

**Question.** Does a commit that writes several facts on one condition cost enough with rules of one head for dialog to need a rule of several?

**Method.** `dialog-perf`, 500 `move` commands in one transaction, the rules as `wide`, `repeat` and `named`; a body of one read and of four; two heads and four; a command of four fields and of two; a commit with no rule; and a real move that retracts. Three sweeps under callgrind. The three shapes are checked to write the same facts.

**Results.** [results/2026-11-12.md](results/2026-11-12.md). `named` is 21.5% to 38.7% over `wide` and `repeat` 61.0% to 148.2% over. For the move that retracts, `named` is 29.9% under `repeat`. Runs of one scenario differ by 0.43% at most.

Two of the agent's guesses were wrong. `named` is not within a tenth of `wide` where the body is heavy: the gap is about 120 million instructions a head whatever the body. And `repeat` is not the cheaper one where the body is one read: `named` is cheaper in every cell. A narrower command changes nothing: the same ratios with two fields as with four.

**Decision.** Not decided. Neither bar is met. What can be said without the maintainer: the notation compiles a rule of several heads to `repeat` today, and `named` does the same work for less in every cell.

Commits: `4138dc9` and the one this entry is in.

## 2026-11-12, one command a commit: what is expected

**Question.** A person issues one command a commit. Do the differences between `wide`, `named` and `repeat`, measured at 500 commands a commit, hold there?

**Method.** Scenarios `heads-each-…`: 500 to-dos seeded and the rules installed as before, one commit of one command off the record so the rules are loaded, then 100 commits of one command each, a different to-do each time. The instructions divided by 100 are one commit. Two floors beside the three shapes: `none`, the command dispatched with no rule to read it, which writes nothing, and `plain`, a transaction that asserts the same facts itself with no rule installed, which is what committing those facts costs before any rule. Each shape is reported gross and net of `plain`.

Checks that the input does not flatter the result: the same at 10 and 300 commits (a commit should not cost more for the commits before it), the same over 5000 to-dos, and the first commit of a process, with none before it.

**Expected.** Guesses, written before any of these ran. A commit of two facts with no rule costs about 2 million instructions (`commit-rows` in `perf/baseline/`: 197 million for 100). At 500 a commit a command costs 1.8 million as `wide`, 2.3 as `named` and 2.9 as `repeat`, one read and two heads. So:

- the order holds: `wide`, `named`, `repeat`;
- gross, the three are closer than at 500, since each carries the same commit;
- net of `plain`, they are as far apart as at 500 or further. What a round does once (probing the triggers, building the round's view, loading and planning a rule) was shared by 500 commands and is now paid by one, `named` has a round more, and `repeat` a rule more a head.

**Bars.** Proposed by the agent before any run. On the gross number, which is what a person pays, in the four cells (one read or four, two heads or four):

- the difference **holds** if `named` is 10% or more over `wide` in every cell;
- it **does not hold** if the cheaper of `named` and `repeat` is within 10% of `wide` in every cell: then at one command a commit a rule of several heads buys nothing worth having;
- between, the cells are reported one by one.

Net of `plain` has no bar. It is reported to say what the rules themselves cost. Three runs, interleaved; a difference inside the spread between runs is noise.

## 2026-11-12, what the engine could recover: what is expected

**Question.** How much of what `named` and `repeat` cost over `wide` could the engine recover, with no rule of several heads?

**Where the instructions go.** Read from the profiles of the two sittings above, before any prototype ran.

- **Range estimates.** The harness's command premise leaves its subject unnamed, as a rule that reads a command has no use for the command's entity. A seed then binds the command's fields and leaves the entity free, so each field is read by value. The conjunction of the fields asks the tree how wide each read is, to choose a merge or a fold: four estimates an evaluation, about 150 thousand instructions each. In `wide`, one read and two heads, that is 309 of 901 million at 500 a commit and 0.66 of 4.5 million a commit at one. `repeat` pays it a head.
- **Planning.** Every seed plans its rule again: 52 thousand instructions a seed for the body of one read.
- **The second round of `named`.** A rule that reads the transient command costs 116 thousand instructions an evaluation: 81 the layered read, of which 52 settling the cell.

**Prototypes.** On `spike/induce-cost`, each behind a switch of one build (`DIALOG_SPIKE`):

```
 plan      a rule is planned once for each set of names a seed binds, and the plan kept
 share     rules of a round that state one body have it evaluated once
 subject   a seed binds the entity its rows are about, where the premise leaves it unnamed
```

**Expected.** Guesses, written while the first sweep ran and before any of its numbers were read.

- `subject` takes the estimates off every shape, less what four reads at one entity cost in their place: about a quarter off `wide` at 500, the same amount off `named`, and that amount a head off `repeat`. It does not bring `named` nearer `wide`: the gap stays and the base shrinks, so the percentage grows.
- `share` brings `repeat` to `wide` and what a head costs to write.
- `plan` takes 3% to 8% off each at 500 and less at one command a commit.
- Nothing here touches the second round of `named`, so its gap of about 120 million a head at 500 stays.

**Bars.** Proposed by the agent.

- With every prototype on, `repeat` **needs no rule of several heads for its cost** if it is within 10% of `wide` in every cell, at 500 a commit and at one.
- The prototypes **are not worth more work for this question** if `repeat` stays 30% or more over `wide`.
- `named` has no bar: nothing here aims at it. It is reported, and what is left of its gap is the second round's.
- No gated scenario grows past the gate's allowance with every prototype on, and every counter holds (`perf compare` against `perf/baseline/` and against the same scenarios swept from the build without the prototypes).

Three runs, interleaved: off, `plan`, `subject`, `share`, all, then again.

## 2026-11-12, one command a commit: the difference holds

**Question.** Do the differences between `wide`, `named` and `repeat` hold at one command a commit?

**Method.** As set out in the entry of that name above: `heads-each-…`, 100 commits of one command each over 500 to-dos after one commit off the record, three sweeps under callgrind. `it_writes_the_same_facts_a_commit_a_command` checks that the three shapes and `plain` write the same facts.

**Results.** [results/2026-11-12-one-command.md](results/2026-11-12-one-command.md). Millions of instructions a commit; runs of one scenario differ by 0.32% at most.

| body | heads | `plain` | `wide` | `named` | `repeat` | `named` over `wide` | `repeat` over `wide` |
|---|---|---|---|---|---|---|---|
| one read | 2 | 2.91 | 4.52 | 5.30 | 5.73 | +17.4% | +26.9% |
| one read | 4 | 4.02 | 5.83 | 7.43 | 10.30 | +27.5% | +76.7% |
| four reads | 2 | 2.91 | 4.94 | 5.81 | 6.72 | +17.8% | +36.1% |
| four reads | 4 | 4.02 | 6.25 | 7.90 | 11.66 | +26.3% | +86.5% |

Net of `plain`, `named` is 43% to 89% over `wide` and `repeat` 76% to 247% over.

The three guesses held: the order is the same, gross the three are closer than at 500, and net they are further apart. One thing was not guessed: a commit with no rule does not cost a fixed amount. It is 1.97 million averaged over the first 10 commits, 3.18 over 300, and 4.48 over a store of 5000 to-dos, so the gross percentages move with how much has been committed, between +13% and +47% for `named` over the checks. What the rules add moves much less.

Where a commit's instructions go, `wide` with one read and two heads: 2.15 million the commit itself, 0.50 of it signing, and 1.84 induction, of which 0.66 is four range estimates of the command premise's fields. The entry above this one is about that.

The check called "the first commit of a process" is the first commit after the rules were installed by the same process, which has already decoded them. A process that starts with the rules stored was not run.

**Decision.** The bar is met: `named` is 10% or more over `wide` in every cell, so the difference holds at one command a commit. Whether it is large enough to want a rule of several heads is the maintainer's, as before.

Commits: `d311797` and the one this entry is in.

## 2026-11-13, what the engine could recover: `repeat` comes to `wide`

**Question.** How much of what `named` and `repeat` cost over `wide` could the engine recover, with no rule of several heads?

**Method.** The branch `spike/induce-cost` at e13e838, one build, each prototype a switch: `plan`, `share`, `subject`, and `entity`, added after the others were set out, for the question of how often a rule is analyzed. The corner cells (one read and two heads, four reads and four heads) at 500 commands a commit and at one. Three sweeps a variant, interleaved. The 30 gated scenarios swept with every prototype on and with none, and compared. `dialog-perf`'s tests under every switch.

**Results.** [results/2026-11-12-induce-cost.md](results/2026-11-12-induce-cost.md). Millions of instructions, runs of one scenario differ by 0.35% at most.

| | body, heads | `wide` | `named` | `repeat` | `named` over `wide` | `repeat` over `wide` |
|---|---|---|---|---|---|---|
| 500 a commit, as it is | 1 read, 2 | 901.7 | 1142.2 | 1452.8 | +26.7% | +61.1% |
| 500 a commit, `plan`, `share` and `subject` | 1 read, 2 | 541.9 | 762.3 | 548.5 | +40.7% | +1.2% |
| 500 a commit, as it is | 4 reads, 4 | 1562.5 | 2077.3 | 3882.8 | +33.0% | +148.5% |
| 500 a commit, `plan`, `share` and `subject` | 4 reads, 4 | 1142.3 | 1631.6 | 1191.7 | +42.8% | +4.3% |
| one a commit, as it is | 1 read, 2 | 4.51 | 5.31 | 5.74 | +17.6% | +27.3% |
| one a commit, `plan`, `share` and `subject` | 1 read, 2 | 3.74 | 4.54 | 3.84 | +21.2% | +2.6% |
| one a commit, as it is | 4 reads, 4 | 6.25 | 7.90 | 11.67 | +26.4% | +86.6% |
| one a commit, `plan`, `share` and `subject` | 4 reads, 4 | 5.28 | 6.91 | 5.67 | +30.7% | +7.4% |

Each alone, against the build as it is: `subject` takes 22% to 38% off `wide` at 500 and 13% to 16% at one; `share` takes 37% to 59% off `repeat` at 500 and 20% to 43% at one, and nothing off the others; `plan` takes 2% to 9% off at 500 and 1% to 6% at one; `entity` takes 1.3% to 3.7% off at one.

Against what was guessed: `share` did what was expected. `subject` took more off `wide` than the quarter guessed where the body is one read. `named` did end further from `wide`, and the 220 million it is over at 500 with two heads is its second round, as it was.

How often a rule is compiled: an inductive rule is decoded and analyzed once a process, kept by its entity and its source's hash, and never in the commits measured. It is planned once a seed, 500 times in a commit of 500, which is what `plan` keeps. A commit reads each rule's source from the tree and hashes it to find the rule it has, 55 thousand instructions a rule a round, which is what `entity` skips. A process that starts with its rules stored, and so decodes them once, was not run.

The gated scenarios, every prototype on against none in one binary: no regression, every counter unchanged, instructions from -3.4% (`rule-dispatch`) to +1.2%. Against `perf/baseline/` the comparison fails on this machine with the prototypes on and off alike, 26 of 30 scenarios 4% to 16% over their files, so it says nothing here.

One thing found is not a cost alone. A rule that leaves a premise's subject unnamed is fired for entities the commit did not touch, wherever one field of the premise is written and another entity holds the same value. `subject` changes that. It is written up as a design topic of its own, dialog-db/dialog-db#789.

**Decision.** The bar is met in the cells measured: with the prototypes on, `repeat` is within 10% of `wide` (1.2% to 7.4%). So what `repeat` costs over a rule of several heads is the engine's to recover, by evaluating once a body that several rules state, and the question of the topic turns on whether that is built, not on `named`. Not decided by the agent: whether to build it. The prototypes are not sound enough to land; the results file says what each leaves out. The cells between the corners were not run.

Commits: `e13e838` on `spike/induce-cost`, and the one this entry is in.

## How to reproduce

```
cargo build -p dialog-perf --release
for run in 1 2 3; do
  target/release/perf sweep --only "$(target/release/perf list | grep -o 'heads-[a-z0-9-]*' | paste -sd,)" --out-dir target/heads-$run
done
```

`heads-each-…` are among them. Their checks, each a sweep of its own:

```
target/release/perf sweep --only … --size 10 --out-dir …        # and 300, and 1
DIALOG_PERF_HEADS_TODOS=5000 target/release/perf sweep --only … --out-dir …
DIALOG_PERF_HEADS_COLD=1 target/release/perf sweep --only … --size 1 --out-dir …
```

The prototypes, from a build of `spike/induce-cost`:

```
DIALOG_SPIKE=subject target/release/perf sweep --only … --out-dir …   # or plan, share, entity, all, or nothing
target/release/perf compare --base <the gated scenarios, none on> <the same, all on>
```
