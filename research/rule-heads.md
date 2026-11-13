# What a rule of several heads would save

**Status: Exploring.** Asked by the design of the notation ([macros](../design/macros.md), #773), which would drop rules of several heads from its kernel if writing on one condition is as cheap without them.

## The question

A command moves a to-do: on one condition, several facts are written. Does dialog need a rule of several heads to do that efficiently, or is one of the two ways it can be written with rules of one head close enough?

```
 wide      one rule, one head of H attributes, the whole body          the body once, H facts, one round
 repeat    H rules, one head each, each with the whole body            the body H times, one round
 named     one rule issues a command carrying what the body bound,     the body once, a round more,
           and H rules of one head read it                             a transient fact a match
```

`wide` stands in for a rule of several heads, which dialog does not have: one rule that evaluates its body once and writes H facts. It can only assert, and only to one entity, so it is a floor for what several heads would cost and not the thing itself. `repeat` is what the notation compiles a rule of several heads to today.

## Hypothesis

`named` costs what `wide` does and a round: within a tenth of it where the body is heavy. `repeat` costs the body once a head, so it falls behind as the body and the heads grow. Where the body is one read, a round costs more than the body, and `repeat` is the cheaper of the two.

## Measured against

`main` at dd19d6c, through `dialog-perf`: instructions under callgrind and the engine's own counters, for one commit that dispatches 500 commands. Each of the three is run with a body of one read and of four, and with two heads and four. A pair of runs with a head that retracts checks that retracting is not what decides it.

## Bars

Proposed by the agent before any run; the maintainer has not set them. The measure is instructions, with archive bytes written beside it. "The best without" is the cheaper of `repeat` and `named` in a cell.

- **No rule of several heads is needed** if the best without is within 10% of `wide` in every cell.
- **Several heads earn their place** if `wide` is 30% or more under the best without, in the cells with a body of four reads.
- **Between**, the maintainer weighs it against what several heads cost to build: a stored rule that holds them (#779) and an engine with more than one conclusion a rule (#776).

A difference inside the spread between runs of one build is noise. Three runs, interleaved.
