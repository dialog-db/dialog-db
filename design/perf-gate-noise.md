# Two runs of one binary differ by more than 1% under the Perf gate's counter

**Phase: Describing.** Nobody has picked this up. What follows is the situation, not yet a problem statement.

## Description

The Perf gate counts the instructions a scenario retires under callgrind and fails a pull request that grows a scenario by more than 3%, or by more than a million instructions where 3% is less (`Tolerance`, `rust/dialog-perf/src/compare.rs`). It runs each sweep once.

One binary, built from the base of #778, run five times on one machine (Intel Xeon 2.10GHz, not a CI runner), by the agent building patterns. The totals below are read from the five callgrind files it kept:

| scenario | least | most | spread |
|---|---|---|---|
| `rule-point` | 55,261,690 | 56,276,396 | 1,014,706, 1.84% |
| `read-point` | 24,593,901 | 24,930,342 | 336,441, 1.37% |
| `read-sibling` | 32,009,971 | 32,399,290 | 389,319, 1.22% |
| `number-point` | 35,229,292 | 35,390,656 | 161,364, 0.46% |
| `subscribe-repoll` | 13,513,052 | 13,547,132 | 34,080, 0.25% |

What the agent says of where the spread is, not checked for this note:

- The allocator is called the same number of times every run. What its calls cost inside is what moves: on `number-point`, 7.30 to 7.46 million.
- The instructions outside the allocator and outside `memcpy` are steady: on `number-point`, 18.324 to 18.332 million, a spread of 0.04%.

```
 one scenario's count
   ├─ outside the allocator and memcpy    steady to a few thousand
   ├─ memcpy                              follows the sizes moved
   └─ inside the allocator                same calls, a different cost each run
```

How it showed: after the fix on #778 some read scenarios counted fewer instructions than the base, with no work removed from a read, and what the fix left behind (0.10% to 0.15% of a scenario) could not be read from the gate at all. It was read from the first row of the diagram.

Not known:

- Whether a CI runner shows the same spread. The gate compares a pull request's sweep with a sweep of its base made in the same job, and no one has run one binary twice there.
- Why the allocator costs a different number of instructions for the same calls. Three guesses, none tested:
  - where the heap lands differs each run (address space layout), and the allocator's paths depend on it. Five runs with the layout fixed (`setarch -R`) and the same environment would show a spread near none.
  - the sizes or the order of what is asked for differ each run, from a seed (a hash map's, an identifier's). A count of allocations by size, run by run, would differ.
  - more than one thread allocates, and which one frees what differs. A count of threads in a scenario would say.
- Whether the commit and rule scenarios outside these five move as much.
- How often the gate has failed, or passed, on this alone.

The tolerance, the baselines and how the gate runs are the maintainer's to move (`.claude/rules/perf-baselines.md`). This note describes the count; it proposes no change to what is gated.
