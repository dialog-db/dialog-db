# A replica cannot read or write what it held once a pull moves its head: direction

Decision: what brings a replica's held keys along when its head moves by
root, and who asks for it. For: a partial replica loses the ability to
read and write, from its own blocks, each key whose leaf another replica
changed, every time it adopts that replica's head by root.

Decided 2026-11-11: column E, by the maintainer (journal). On 2026-11-12 its form became `pull(Download::QueriedRanges)`, an argument every pull must give. Column D was built first and is replaced by E on the topic's branch; its cells stay as measured.

| criterion \ approach | A. Status quo: adopt by root, fetch on demand | B. Subscriptions first: the embedder evaluates its live queries against the prepared head, then commits | C. Download the difference: fetch every node by which the two heads differ before the head moves | D. Follow what is held, in every pull: walk both trees in step, fetch an adopted node only where its keys meet a held leaf | E. Follow what is held, asked for: the same walk as a `Download` scope the embedder runs between `prepare` and `commit` |
|---|---|---|---|---|---|
| reads again, with no network, a key read before | 🔴 fetches when the key's leaf changed upstream: 1 hydration where 0 are wanted | 🟡 for keys under a live query when the head moves; a key read earlier and not subscribed now fetches, which is the reported case | 🟢 no fetch | 🟢 no fetch: `it_reads_what_it_held_after_adopting_a_head_another_device_moved` | 🟢 no fetch where the embedder asked; as A where it did not |
| writes again, with no network, a key written before | 🔴 fetches after every pull that changed its leaves: 2 hydrations | 🔴 a query reads through one order; the write's other leaves are not fetched | 🟢 no fetch | 🟢 no fetch: `it_writes_where_it_wrote_after_adopting_a_head_another_device_moved` | 🟢 where asked |
| first write to a key only read | 🟡 fetches the leaves of the orders never read, once | 🟡 the same | 🟢 no fetch for keys the upstream changed; the same as A for the rest | 🟡 the same as A | 🟡 the same as A |
| what a pull fetches | 🟢 nothing | the blocks the live queries read | 🔴 every changed node in every order and history: 44 blocks where the query needed 7 | the changed nodes on the paths to held leaves; nothing for a replica that holds no leaf | the same as D when asked |
| what a pull costs a replica that holds everything | nothing fetched, everything changed fetched at its next read | as A beyond the live queries | every changed node | every changed node: the same as C | the same as D when asked |
| reads and writes while the pull fetches | never waits on a pull; waits afterwards, one read at a time | on the held head for as long as the queries take | on the held head for as long as the fetch takes | on the held head for as long as the fetch takes; a commit meanwhile makes the pull lose its publish and be made again | the same as D |
| a pull that cannot fetch | 🔴 succeeds; the next read of the missing node fails | ? the embedder decides | fails, head stays | fails with `PullError::Tree`, head stays | the download fails, the embedder does not commit |
| can be forgotten by a caller | nothing to forget | 🔴 each embedder builds it | 🟡 if it is an option of pull | 🟢 cannot: every pull does it | 🟡 has been forgotten once already for a branch that needed it (the account branch in tonk) |
| an embedder that wants a pull to fetch nothing | 🟢 has it | has it by not doing it | ? needs an opt-out | ? needs an opt-out: none is built | 🟢 has it by not asking |
| where the rule lives | nowhere | in each embedder, tied to its subscription machinery | in dialog | in dialog: `PersistentTree::follow`, called by `prepare_upstream` | in dialog, and in each embedder's pull |
| fetches under the branch's write lock | none | none | ? | ? `land` prepares a second upstream again under the lock, and that prepare now follows | none in dialog; the embedder's own ordering |
| a replica already on a head it holds little of | fetches on demand | fetches on demand | no help: there is no held tree to compare | no help: the same | no help: the same |

Open questions the matrix does not settle:

- `?` Whether D needs a way to pull without following, and for whom.
  Nothing in dialog or tonk asks for one today.
- `?` Whether D and E are one approach: D for every pull, with E's
  `Download` scope exposed as well for a caller that holds a revision and
  wants it brought local outside a pull. No caller is known.
- `?` Whether the first write to a key only read should fetch at all. Two
  ideas, neither examined: a view preloads the write leaves of what it
  shows (`nodes::Preload`), or a commit is made that does not read the
  leaves it writes to.
