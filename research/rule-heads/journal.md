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

## How to reproduce

```
cargo build -p dialog-perf --release
for run in 1 2 3; do
  target/release/perf sweep --only "$(target/release/perf list | grep -o 'heads-[a-z0-9-]*' | paste -sd,)" --out-dir target/heads-$run
done
```
