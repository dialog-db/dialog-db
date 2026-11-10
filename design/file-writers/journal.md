# Journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | Two processes advancing two branches of one space share no lock on main and take turns on one file lock with the file archive. Cells live outside the file, a file each. No contention between branches has been observed or measured. Two wishes arrived together: writers that never wait, and cells kept in the file. | Describing. |
| **Agenda** | What the second process loses by waiting its turn, and how much. What keeping cells in the file would let someone do that they cannot now. Whether the two wishes are one problem or two. | Ask what was observed; then ask what cells in the file would buy. |

## 2026-11-10, Describing: the topic opens

**Asked.** Irakli: design branch storage in the same file archive, or
memory cells more generally; branches contending on one file does not feel
right; perhaps each session or branch writes a file of its own and an
asynchronous job integrates it, readers considering both; perhaps main
holds another tree for memory cells.

**Learned.** The integrator is already an option the single-file design
names and leaves unstarted. Cells are a file each today, so two branches'
heads do not contend now; their blocks do, on the file's writer lock. No
run has measured two branches of one replica writing at once.

**Decided.** Not decided. Two wishes are braided in the request: writers
that never wait on one another, and cells kept in the file. Each arrived as
a remedy; the problems they answer are not stated yet.

**Asked (continued).** Who writes at once? Irakli: two processes advancing
two branches independently and concurrently. On main there is no
coordination among them at all; with the single-file archive they have to
contend for writes.

**Learned.** Checked against the code and it holds: main's `fs` archive
writes each block as its own content-addressed file with no shared lock,
and each head is its own cell. The file archive is what introduces the
shared turn. What the waiting costs a process is not stated yet.
