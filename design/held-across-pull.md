# A replica cannot read or write what it held once a pull moves its head

Phase: **Directing.** See the [design index](README.md) for what the
phases mean, and the [journal](held-across-pull/journal.md) for where the
topic stands. One approach of the [matrix](held-across-pull/direction.md)
is built on the topic's branch, ahead of the decision; the journal says
why.

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

Not decided. The [matrix](held-across-pull/direction.md) compares what
exists, what tonk does for its own branches, and three ways of bringing a
replica's held keys along. The topic's branch builds one of them
(a pull follows the head it adopts, bounded by what the replica holds) so
that its costs could be measured for the matrix.
