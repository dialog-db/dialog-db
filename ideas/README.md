# Ideas

A pool of things we want to explore but have not committed to. Each entry
states the idea, the goal of the exploration, and what success looks like,
so that someone (or several agents in parallel) can pick one up and spike
it out without asking.

An idea that gets spiked should link its results here. An idea that lands
becomes a note in `notes/`.

## Spiked

- [Single-file representation](single-file-format.md):
  [results](single-file-format-spike.md). Columnar frames written
  against their predecessor are 87× smaller than file-per-block on Stack
  Exchange and 22× on tonk-team's pushed history, and grow with the live
  tree. Decoding a node's rows costs about a file read; rebuilding the
  block costs 10× more, and blocks an older encoder wrote do not rebuild.
  [Design](single-file-format-design.md): one append-only file of frames
  that serves nodes to local reads and blocks to sync, with one writer at
  a time and lock-free readers.
