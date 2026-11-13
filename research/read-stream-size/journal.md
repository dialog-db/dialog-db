# Read stream size: journal

## Where things stand

**Evaluating.** The change is three edits in
`rust/dialog-query/src/concept/query.rs` and one test. A concept read's
stream is 3,136 bytes where it was 12,432. The point scenarios count 14% to
21% fewer instructions and no scenario counts more past what one binary
moves between runs. The wall clock does not move by anything these runs can
see: what callgrind counted was mostly `rep movsb`, one instruction a
byte, which hardware does not pay for that way. Next: the maintainer
decides whether to land it, and whether to re-record baselines.

## Datasets

None of its own. Every number is a `dialog-perf` scenario at its catalog
size, seeded by the harness.

## 2026-11-12 — Which fields make up the stream

**Question.** What the bytes of the stream `ConceptQuery::evaluate_erased`
returns are.

**Method.** `-Zprint-type-sizes` on a release build of `dialog-perf`, the
stream instantiated for `QueryEnv`, on main at `f65891b`, rustc 1.99.1.

**Results.** 12,432 bytes on this toolchain (the issue's 16,896 was measured
on another build and was not reproduced here).

| bytes | what | why it is there |
|---|---|---|
| 5,728 | the `evaluate_unsorted` stream, sorted arm | `for await` pins what it iterates in a slot of its own |
| 5,728 | the `evaluate_unsorted` stream, plain arm | a second `for await`, a second slot |
| about 960 | `this`, `app`, `first`, `rules`, the rules future | every read needs them |

Inside the 5,712 bytes of `evaluate_unsorted`'s body:

| bytes | what | a plain read runs it |
|---|---|---|
| 1,992 | the future of `Continuation::rows` | no: recursive concepts only |
| 1,392 | the future of `fixpoint::evaluate` | no: recursive concepts only |
| 1,696 | the `relation` stream | no: an attribute a rule derives |
| 232, 144 | the futures of `reduce_rows`, `stored_absent` | no |
| the rest | locals of 88 to 96 bytes each | some |

The compiler gives each of these its own room, although no two are alive
at once. So the size is the sum over the paths, not the deepest path as the
issue supposed.

The sibling streams, the issue's third open question: an attribute read
(`AttributeQueryOnly::evaluate`) is 3,216 bytes, `ResolverQuery::evaluate`
and the plan's enum 3,984, a formula 752. Each is boxed where it is made
(`Plan::evaluate`), so each is moved once. Not changed here.

**Decision.** Make the unsorted stream once, and box the three things only
a recursive or derived read runs.

## 2026-11-12 — What a move costs, and what counts it

**Question.** Whether the instructions the gate counts for the moves are
time.

**Method.** `read-point` under callgrind with `--dump-instr=yes`, costs
summed by address inside `__memcpy_avx_unaligned_erms`.

**Results.** Of 22,520,617 instructions, 6,865,828 are in `memcpy`, and
4,994,682 of those sit on two `rep movsb` instructions. Valgrind counts
one instruction for every byte a `rep movsb` copies. A CPU with fast
string copy (this one reports `erms` and `fsrm`) moves 12 KB in a few
hundred cycles.

**Decision.** "Close to a third of a point read" is true of the count and
not shown of the time. The fix is still worth making for the count, since
the gate holds every change to 3% of it, but it is not a 20% faster read.

## 2026-11-12 — The change against main

**Question.** What the change does to every scenario.

**Method.** Two binaries built by the container's rustc 1.99.1 from
`f65891b` and from the change, `perf sweep` on each, three rounds
interleaved (main, change, main, change, ...), on an Intel Xeon at 2.10 GHz.
The nix toolchain CI uses was not run, so these are not the gate's numbers.
Wall clock is the measured phase `perf run` prints, each a fresh process,
main and change alternating.

**Results.** Instructions, lowest and highest of three rounds; the delta is
between medians. Outcomes and step counters are equal in every scenario.

| scenario | main min | main max | change min | change max | delta |
|---|---|---|---|---|---|
| `commit-batch` | 316,241,381 | 317,084,607 | 316,915,609 | 317,069,761 | +0.2% |
| `commit-derived` | 117,806,642 | 118,092,046 | 101,084,622 | 101,415,195 | -14.2% |
| `commit-hot-key` | 195,084,856 | 195,181,160 | 195,087,929 | 195,235,187 | +0.0% |
| `commit-rows` | 197,580,503 | 197,635,788 | 197,572,833 | 197,749,304 | -0.0% |
| `commit-succeed` | 1,235,598,726 | 1,476,900,251 | 1,187,470,207 | 1,459,252,989 | -7.7% |
| `export-walk` | 28,024,333 | 28,105,346 | 28,005,181 | 28,066,857 | -0.0% |
| `first-query` | 1,852,916 | 1,927,164 | 1,814,847 | 1,888,986 | -0.6% |
| `number-commit` | 841,943,335 | 843,196,583 | 841,485,784 | 843,067,458 | -0.0% |
| `number-point` | 33,228,553 | 33,436,545 | 28,459,641 | 28,743,038 | -13.9% |
| `number-range-mixed` | 7,689,672 | 7,713,857 | 7,687,401 | 7,694,098 | -0.2% |
| `number-range-narrow` | 5,445,083 | 5,546,402 | 5,484,637 | 5,517,881 | +0.1% |
| `number-range-wide` | 24,196,260 | 24,224,713 | 24,202,966 | 24,223,303 | +0.1% |
| `number-scan` | 28,191,442 | 28,197,103 | 28,129,176 | 28,137,287 | -0.2% |
| `query-join` | 76,721,425 | 76,806,057 | 76,533,867 | 76,602,390 | -0.2% |
| `read-point` | 22,394,570 | 22,658,637 | 17,651,237 | 17,849,657 | -20.8% |
| `read-scan` | 30,328,463 | 30,386,207 | 30,299,031 | 30,367,984 | -0.1% |
| `read-sibling` | 29,984,828 | 30,208,163 | 25,026,698 | 25,171,304 | -16.9% |
| `rule-dispatch` | 594,708,991 | 595,126,090 | 558,128,222 | 559,071,761 | -6.1% |
| `rule-install` | 163,621,459 | 163,963,182 | 163,438,048 | 163,934,737 | -0.1% |
| `rule-overlay` | 263,304,880 | 263,865,544 | 253,414,658 | 254,358,096 | -3.5% |
| `rule-point` | 50,833,669 | 51,440,569 | 41,237,194 | 42,200,545 | -18.8% |
| `rule-ranked` | 59,726,682 | 59,891,563 | 59,537,640 | 59,583,891 | -0.5% |
| `rule-recursive` | 798,596,622 | 802,133,800 | 653,349,591 | 654,687,873 | -18.2% |
| `rule-scan` | 87,809,423 | 87,919,206 | 87,644,361 | 87,865,708 | -0.2% |
| `rule-session` | 68,389,289 | 68,656,009 | 58,718,849 | 58,742,127 | -14.2% |
| `subscribe-fanout` | 39,184,826 | 39,472,972 | 39,150,947 | 39,417,625 | -0.5% |
| `subscribe-pulled` | 39,458,380 | 39,511,768 | 39,195,063 | 39,493,855 | -0.4% |
| `subscribe-repoll` | 12,644,068 | 12,658,029 | 11,458,299 | 11,465,174 | -9.4% |
| `transaction-reads` | 300,370,085 | 300,730,618 | 293,819,041 | 294,112,907 | -2.2% |

`commit-succeed` is marked volatile in the catalog; its spread on either
binary is wider than its delta.

Wall clock of the measured phase, microseconds:

| scenario | runs each | main min | main median | change min | change median |
|---|---|---|---|---|---|
| `read-point` | 25 | 3,829 | 4,693 | 3,524 | 4,687 |
| `rule-point` | 25 | 8,718 | 10,391 | 8,325 | 10,175 |
| `rule-session` | 12 | 11,104 | 11,689 | 10,952 | 12,154 |
| `rule-recursive` | 12 | 132,979 | 142,850 | 127,803 | 142,339 |

The medians differ by less than the runs of one binary do. The minimums
lean 1% to 8% towards the change, which a single cold process per run
cannot tell from noise.

**Decision.** Not decided: the maintainer's. The count's bar is met; the
time's is not shown either way.

## How to reproduce

```sh
# the sizes
RUSTC_BOOTSTRAP=1 RUSTFLAGS=-Zprint-type-sizes CARGO_TARGET_DIR=target/sizes \
  cargo build -p dialog-perf --release > sizes.txt
grep -n "async block@dialog_query::ConceptQuery::evaluate_erased<'_, repository" sizes.txt

# the instructions, one sweep per binary and round
target/release/perf sweep --out-dir target/perf-current

# where memcpy's count sits
valgrind --tool=callgrind --collect-atstart=no --toggle-collect=dialog_perf_measured \
  --cache-sim=no --dump-instr=yes target/release/perf run read-point

# the wall clock
target/release/perf run read-point > /dev/null
```
