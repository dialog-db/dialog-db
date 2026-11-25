# A counter kept as its increments, measured in tonk

Run 2026-11-25 in tonk (`tonk-labs/tonk` at `c0ec36e`, dialog pinned at
`875cc7f`) with the experiment build of tonk's
`research/concurrent-cli-writes/harness/eval-policies.patch`, and a copy of
its `counter.sh` that takes the schema below, gives each writer's
`increment` a distinct `time`, and reads `tally` at the end. The harness
lives on tonk's branch `worker/busy-thompson-7gnvos`.

Variants, as tonk's results describe them: **queue**, every writer on main
with `TONK_EVAL_POLICY=queue`; **branch**, a branch per writer, pulling
main before writing and queued to a reconciler that merges into main;
**branch+rules**, the same with rules run after every pull.

## Final count, N writers each incrementing once

| model | N | queue | branch | branch+rules |
|---|---|---|---|---|
| ticks (below), rounds 1, 2, 3 | 4 | 4, 4, 4 | 4, 4, 4 | 4, 4, 4 |
| ticks (below), rounds 1, 2, 3 | 8 | 8, 8, 8 | 8, 8, 8 | 8, 8, 8 |
| tonk's guide (one `count`, read and written + 1), same build | 4 | - | 1 | - |
| tonk's guide, same build | 8 | - | 1 | - |

No writer failed. Tonk's own three rounds of the guide's model under
branch ended at 1, 1, 1 for N = 4 and 2, 2, 1 for N = 8.

## Other checks

| check | result |
|---|---|
| a counter with no increments | count 0 |
| two increments with the same body (same `time`) | count 1: content-addressed, they are one entity |
| reading the count over 16 increments, one replica | 735 ms |
| over 32 | 10 032 ms |
| over 64 | not finished after several minutes; stopped |

## The schema

Tonk's notation has `last`, `all`, `top`, `max` and `min` picks and no
fold, so the count is the rank of the last increment along a chain:
`<` orders the ticks, `unless:` finds the one just before each, recursion
with `math/sum` ranks them. The `tally` rule derives on query (`assert:`),
so it is computed over whatever the branch holds after a merge.

```yaml
concept!: &counter
  description: A tally that can be incremented
  with:
    label:
      description: What the counter counts
      the: xyz.tonk.counter/label
      as: text

command!: &increment
  description: A request to increase a counter by one
  with:
    subject:
      description: The counter to increment
      the: xyz.tonk.counter.increment/subject
      as: entity
    time:
      description: When the increment happened, which makes each one distinct
      the: xyz.tonk.counter.increment/time
      as: float

concept!: &tick
  description: One increment of a counter, kept
  with:
    counter:
      description: The counter it increments
      the: xyz.tonk.counter.tick/counter
      as: entity

rule!:
  description: Keeps each increment as a tick
  assert!: tick
  when:
    - assert: increment
      where: { this: ?this, subject: ?counter }

concept!: &tick/before
  description: The ticks of the same counter that sort before a tick
  with:
    earlier:
      description: A tick that sorts before this one
      the: xyz.tonk.counter.tick/earlier
      as: entity
      pick: all

rule!:
  description: A tick of the same counter sorts before another
  assert: tick/before
  when:
    - assert: tick
      where: { this: ?earlier, counter: ?counter }
    - assert: tick
      where: { this: ?this, counter: ?counter }
    - assert: "<"
      where: { of: ?earlier, with: ?this }

concept!: &tick/gap
  description: Earlier ticks with another tick between them and this one
  with:
    over:
      description: An earlier tick that is not the one just before
      the: xyz.tonk.counter.tick/over
      as: entity
      pick: all

rule!:
  description: An earlier tick is not the previous one when a tick sits between
  assert: tick/gap
  when:
    - assert: tick/before
      where: { this: ?this, earlier: ?middle }
    - assert: tick/before
      where: { this: ?middle, earlier: ?over }

concept!: &tick/previous
  description: The tick just before a tick
  with:
    previous:
      description: The tick just before this one
      the: xyz.tonk.counter.tick/previous
      as: entity

rule!:
  description: The previous tick is the earlier one with nothing between
  assert: tick/previous
  when:
    - assert: tick/before
      where: { this: ?this, earlier: ?previous }
  unless:
    - assert: tick/gap
      where: { this: ?this, over: ?previous }

concept!: &tick/rank
  description: A tick's position among its counter's ticks, from one
  with:
    rank:
      description: How many ticks up to and including this one
      the: xyz.tonk.counter.tick/rank
      as: natural

rule!:
  description: The first tick ranks one
  assert: tick/rank
  when:
    - assert: tick
      where: { this: ?this, counter: ?counter }
    - assert: "=="
      where: { this: ?rank, is: 1 }
  unless:
    - assert: tick/before
      where: { this: ?this }

rule!:
  description: A tick ranks one more than the one before it
  assert: tick/rank
  when:
    - assert: tick/previous
      where: { this: ?this, previous: ?previous }
    - assert: tick/rank
      where: { this: ?previous, rank: ?before }
    - assert: math/sum
      where: { of: ?before, with: 1, is: ?rank }

concept!: &tally
  description: How many times a counter has been incremented
  with:
    count:
      description: The number of increments
      the: xyz.tonk.counter/count
      as: natural

rule!:
  description: The count is the rank of the last tick
  assert: tally
  when:
    - assert: tick
      where: { this: ?last, counter: ?this }
    - assert: tick/rank
      where: { this: ?last, rank: ?count }
  unless:
    - assert: tick/before
      where: { earlier: ?last }

rule!:
  description: A counter with no ticks counts zero
  assert: tally
  when:
    - assert: counter
      where: { this: ?this }
    - assert: "=="
      where: { this: ?count, is: 0 }
  unless:
    - assert: tick
      where: { counter: ?this }

counter!: &the-counter
  label: "clicks"
```

The harness is [`counter-ticks.sh`](counter-ticks.sh), run from a tonk
checkout as `TONK=<experiment build> COUNTER_SCHEMA=<the schema above, saved as a file>
counter-ticks.sh <variant> <N> <label> <out>`.
