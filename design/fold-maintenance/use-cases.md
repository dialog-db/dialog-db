# What a subscriber could do: use cases

**Problem served:** the one proposed in [the topic](../fold-maintenance.md).

Each is something tonk does by other means today; the evidence is in [tonk-as-built.md](tonk-as-built.md). `How` is filled in the Design phase.

| | intention | today | How |
|---|---|---|---|
| 1 | Show how many of something there are, and have it stay right | an element's script counts rows it subscribed to, or fetches and counts (§4) | |
| 2 | Show a thing with all of some field of it, none included, as one value | the page folds every row of the answer again on each change (§2.5) | |
| 3 | Rename one thing on a page of a thousand and pay for one | a renamed notebook of N blocks retracts N rows and asserts N; the page folds, serializes and renders every binding again (§2) | |
| 4 | Show a list whose items each have a view of their own without a subscription for each | N streams and N subscriptions, each checked on every write (§6) | |
| 5 | See two collections of one view change together | they arrive as separate frames that can disagree, and the element holds its projection until they agree (§2, §9) | |
| 6 | Keep a row when a field of it is missing | optional values are split into queries of their own (§5) | |
| 7 | Keep a list in an order its author chose, without moving what stayed | elements sort rows they read back out of the page (§3) | |
| 8 | Tell "nothing matched" from "not loaded yet" | control frames beside the data (§9) | |

1, 2 and 3 are this topic. 4 and 5 bear on what one subscription is, and so on how a fold's state is held. 6 is decided on #773 (a read that collects never gates). 7 and 8 are other topics' and are here so they are not designed against.
