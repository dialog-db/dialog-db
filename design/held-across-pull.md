# A replica cannot read or write what it held once a pull moves its head

Phase: **Building.** See the [design index](README.md) for what the
phases mean, and the [journal](held-across-pull/journal.md) for where the
topic stands.

## Description

Tonk reported that loading a space under emulated 3G takes a long time
although the space had been loaded before, and that offline it is the same
or worse. The maintainer expected a load to need no network once a space's
data was on the device. Tonk also sees slow reactions to edits at times,
and a pulsing dot in its interface that seems to appear at random
(tonk-team BUG-213).

The maintainer says the expectation rested on this: a loaded space has
live subscriptions, their queries fetch any block they miss, so everything
a page needs becomes local as it is needed.

Reproduced in tonk's browser build with two devices over a local access
service: a device that had rendered a page from its own store took 13.8 s
to render it again at 600 ms round trips, and did not render it with the
network down, after another device changed the space and this one pulled
while showing another page.

## Problem

A partial replica reads and writes the keys it has read or written from
its own blocks, with no network. It loses that for every such key whose
leaf another replica changed, each time it adopts that replica's head by
root: the changed leaves are held by reference until a read or a write
asks for them, and the read or write that asks waits on the remote, or
fails without one. Nothing fetches them first unless a query over those
keys happens to be running when the head moves.

So a replica's ability to work without the network is undone by sync, the
more often the more its peers write, and the application cannot tell
which of its reads and writes will wait.

## Approach

Decided 2026-11-11, column E of the [matrix](held-across-pull/direction.md):
how much a pull fetches is the caller's decision, and there is an extent
that fetches what the replica holds and nothing else.

- `Branch::pull` takes a `Download`, with no default, so every pull says
  how much it fetches. `Download::QueriedRanges` brings local only what
  the replica holds of its head, as the head about to be adopted has it.
  The walk is `PersistentTree::follow`: the held tree and the adopted
  tree in step, a node of the adopted one read only where its keys meet a
  leaf the replica holds. `MinimalRanges`, `Operational`, `Ranges` and
  `Everything` are what a bare pull and the old pull download did.
- Whatever is named is fetched before the head moves, so the replica
  reads and writes on the head it holds for as long as the fetch takes.
- `Pull::retry(Limit)` makes a pull that a commit overtook again, on the
  head the commit left.

Not covered, and open: a replica's first write to a key it only read; a
spilled value or blob a changed leaf newly references; a replica already
standing on a head it holds little of.
