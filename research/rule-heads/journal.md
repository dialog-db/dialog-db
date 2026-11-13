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

## How to reproduce

```
cargo build -p dialog-perf --release
for run in 1 2 3; do
  target/release/perf sweep --only "$(target/release/perf list | grep -o 'heads-[a-z0-9-]*' | paste -sd,)" --out-dir target/heads-$run
done
```
