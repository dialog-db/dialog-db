# A replica that holds every block still pays to warm it: use cases

| | intention | How |
|---|---|---|
| **problem** | a walk over a tree whose every block is local can fetch nothing, and is run for every hint, because nothing records that a tree is wholly local | |
| 1 | a query on a branch whose tree was made here, by its own commits or by a branch it adopted from in this process, costs only its reads | the commits record each tree they make; the walk finds its root recorded and returns |
| 2 | a query after a full download costs only its reads, on that head and on every commit made on it | the download records the tree; each commit on it records its own |
| 3 | a partial replica gets every hinted range warmed, as today | its head was adopted by reference, so its tree is not recorded and the walk runs |
| 4 | a replica that adopted a head by reference keeps the prefetch the catchup and join scenarios measure | the same as 3 |
| 5 | a replica that holds every block, restarted, costs only its reads | not served by the decided step (P1); P2 would serve it |
| 6 | a replica that holds most of its head, after a pull, walks only where the upstream changed | not served by D; column E would serve it |
