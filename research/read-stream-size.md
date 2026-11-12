# Read stream size

**Status: Evaluating.** Built and measured against main; not landed. The
brief is [dialog-db/dialog-db#781](https://github.com/dialog-db/dialog-db/issues/781),
the design topic `design/read-stream-size.md` on
[dialog-db/dialog-db#780](https://github.com/dialog-db/dialog-db/pull/780).
The record of the runs is the [journal](read-stream-size/journal.md).

## The question

A concept read is one stream, returned by value and moved into the box it
is polled from. Is it large because of something every read needs, and
does making it small make a read cheaper?

## The hypothesis

The stream is large because it has room for paths a plain read never
takes (the fixpoint, a derived attribute's relation, the sorted buffer),
so that room can be boxed away at no cost to a plain read, and a point
read then counts fewer instructions by about what the moves cost.

The bars below were written after the size breakdown and the first
`read-point` count were in, not before. They are the issue's draft of
"done", made into numbers.

## What it is measured against

Main at `f65891b`, on every `dialog-perf` scenario, both binaries built by
one toolchain on one machine.

- **Success:** a plain read's stream under 4 KB; `read-point` and the
  other point scenarios at least 10% fewer instructions; no scenario more
  than its own run-to-run spread above main; outcomes and step counters
  equal.
- **Failure:** the bytes are in something every read holds, or a scenario
  counts more than its spread above main, or the plain read needs an
  allocation it did not make before.
- **The middle:** a smaller count with no change in time. The maintainer
  judges whether a count the gate holds is worth the change by itself.

## Where it stands

The count's bar is met: 12,432 bytes to 3,136, the point scenarios 14% to
21% fewer instructions, nothing above its spread, no allocation added to a
plain read. The result is in the middle all the same: the wall clock did
not move by anything the runs could tell from noise, because most of what
callgrind counted for the moves is `rep movsb`, which it counts one
instruction a byte.

Left open:

- Whether the gate should count a `rep movsb` as its bytes. A count that
  charges a 12 KB copy as 12 thousand instructions overstates it against
  time, and a change that adds such a copy fails the gate for little.
- The numbers under the nix toolchain, which are the gate's.
- The commit path's 11 to 12 KB futures in `dialog-corpus/src/buffered.rs`.
  `commit-batch`, `commit-rows` and `number-commit` did not move here, and
  nothing has measured what those futures cost.
- The stream is still 3 KB, most of it a dozen locals of about 90 bytes
  the compiler does not overlap. Holding fewer of them across a stop would
  take it further.
