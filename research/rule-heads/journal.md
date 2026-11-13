# Rule heads: journal

## Where things stand

**Exploring.** The harness is on the branch and one sitting is in: [results of 2026-11-12](results/2026-11-12.md). Giving the condition a name costs 22% to 39% more instructions than one rule with a wide head, which stands in for a rule of several heads; writing the body again a head costs 61% to 148% more. That is between the bars, so it is the maintainer's to weigh. Next, if he wants it: the same at one command a commit, and what the 128 thousand instructions to evaluate a rule of one read are.

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

## How to reproduce

```
cargo build -p dialog-perf --release
for run in 1 2 3; do
  target/release/perf sweep --only "$(target/release/perf list | grep -o 'heads-[a-z0-9-]*' | paste -sd,)" --out-dir target/heads-$run
done
```
