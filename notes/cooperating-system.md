# The Cooperating System

2026-10-08

Two ideas came out of a conversation between Chris and Irakli, and a round of
written exploration and feedback followed. This synopsis records where they
seem to be converging, for Tonk, Sapling, and Dialog DB alike. It is a basis
for discussion, not a design. Corrections are welcome.

1. **Tickets.** A member of a space invites a principal by writing a delegation
   into the space. The invitee claims it from a peer that holds a replica, with
   no invitation passed through a side channel.
2. **The Cooperating System.** A space carries the runtime it needs, as Wasm
   Components. The software a person installs shrinks to a slow-changing host
   that boots each space's runtime from the space itself.

## Tickets

### Converging

**A ticket is an outbox in the target space.** A member with the authority to
invite writes a delegation for Ben into the cell `/ticket/{Ben's DID}` of the
space that Ben is invited to. The invitee does not need to know who invited
them: every invitation for Ben in that space is in the same place. For a
service peer, writing a ticket is a write to a provisioned space, billable like
any other.

**Ben claims a ticket with an invocation on his own subject:**

```text
{
  iss: "did:key:zBen",
  sub: "did:key:zBen",
  args: { sub: "did:key:zSpace" }
}
```

The executor:

1. checks that the invocation is valid and not revoked
2. resolves its local replica of `zSpace`, and answers `None` if it has none
3. resolves the cell `/ticket/did:key:zBen` in that replica, and answers `None`
   if the cell is absent or empty
4. answers with the contents of the cell

**The ticket's address is public.** A UCAN can be published openly, because
only the holder of the audience key can use it. Only readers of the space and
the audience can read the ticket. An address derived from a secret shared by
the two parties (an earlier proposal) adds nothing to access control, and it
would force Ben to know who invited him.

**Ben learns nothing about spaces he was not invited to.** The executor answers
`None` both when it holds no replica and when it holds one with no ticket for
Ben. So the only thing Ben learns is whether a peer holds a space that he was
invited to, and a peer that holds such a space should let him have it anyway.
The two `None` cases must be indistinguishable, including in timing.

**A ticket lets Ben enumerate his invitations.** Once Ben holds a ticket, it can
be used to enumerate the spaces he has been invited to, so he does not need a
separate pointer for each one.

**The authority to invite is a cell grant.** Writing `ticket/*` needs a
delegation for those cells, in the same way as any other write (see
[Grants Scoped to Branches](#grants-scoped-to-branches)).

### Open

- **Replication of tickets.** Memory cells do not replicate today, so a ticket
  is available only from the peers where it was written. Claiming "from any
  replica" needs cells that replicate, or tickets kept somewhere that does, such
  as a branch.
- **Authenticating a replicated cell.** If cells replicate, a peer that serves
  one must prove that its value was published by a principal allowed to publish
  that cell. Otherwise one peer could forge another member's ticket. That
  suggests a signed record for a cell: the value, plus the proof of the publish.
- **The first contact.** How Ben finds the first ticket, or the first peer to
  ask, before enumeration helps.
- **The shape of enumeration.** What the invocation that enumerates looks like,
  and what a peer reveals in answer to it.

## The Cooperating System

### Converging

**The host is network and storage.** The installed host (the daemon in Sapling,
a service worker in Tonk) provides keys, network, storage, and the supervision
of sandboxes. Everything else is runtime: the Dialog DB implementation, API
routes as `wasi:http` components, and the other parts of the runtime. They run
as Wasm Components in a sandboxed process for each persona and space, with host
capabilities provided as imports.

**A space embodies its own runtime.** Different spaces may run different
runtimes. A space runs one runtime at a time, so its members never run skewed
versions of it.

**A space boots from cells.** Each space has well-known cells, for example
`/boot/loader` (the component that implements Dialog DB) and `/boot/wasip3` (a
polyfill, if one is needed). Each cell holds a hash. The host reads the hash and
fetches the component by `blob::Get`, from the space's archive or from anywhere
else, such as a cache or an instance already live. The host then instantiates
the loader, which can read branches, revisions, and trees. To boot, the host
needs only `memory::Resolve` and `blob::Get`. Nothing about revisions or search
trees is frozen into the host.

**Authority has two levels.** The host exposes network and storage through
something like `open(did)`: a peer DID for the network, a space DID for storage.
Opening needs a delegation from the system principal, and it returns a channel.
Within a channel, effects such as `memory::Resolve` need no authority from the
system, but each needs a UCAN delegation from the subject. The system validates
`open` against its own subject. Each session validates effects against its
target subject.

<a id="grants-scoped-to-branches"></a>

**Grants scoped to branches.** Grants are positive. A writer that may publish
only to `main` gets:

```text
/use/get
/use/put/archive
/use/put/memory   cell=branch/main
```

Exclusion and negation are possible, but they get complicated, so they are
avoided. If the lists grow too long, branch names get namespaces. Changing a
space's runtime is then a grant on the `/boot/*` cells, held by owners by
default.

**A runtime update applies to the whole space.** Once the space updates, every
load uses the new runtime. A member who rejects the update can fork the space.
In practice a member's host likely also holds back (it does not run a new
runtime until the person accepts it, or until it comes signed by a publisher
the person trusts) rather than choosing another version. How that feels depends
on the UX of the full software.

**The host ships a default runtime.** A new persona is preloaded with it. When
the host updates, it offers to update the persona's runtime.

**The interfaces are shared.** The host imports, the boot cells, the protocol
between peers, and the shape of grants should be specified jointly by Tonk and
Sapling, and owned by the Dialog DB project, unless a reason appears not to.
One component binary can then run in either host.

### Open

- **The validity of pulled data.** Grants scoped to cells constrain what a
  principal may publish to its own replica. In a mesh, a peer also adopts heads
  that it pulls from other peers, and something must check that each revision
  was authorized by its claimed author. That check reads revisions and trees, so
  it is presumably the runtime's job, not the host's. To confirm.
- **Authenticating boot cells.** The same signed record for a cell as for
  tickets, so that a joining peer can trust the `/boot/*` values it pulls. Also
  how a host refuses a rollback to an older loader, and how it stages a new
  value before adopting it.
- **Background lifetime.** With Dialog DB in the sandbox, something must keep a
  space current when nothing is open. One candidate is a process for each
  persona and space with a lifetime driven by events, like a service worker.
- **The frozen surface.** The list of interfaces that must stay stable: the host
  imports (the WIT world), the boot cells, signed cell records, the protocol
  between peers, and the protocol between a view and the trusted frame around
  it.
- **The same default runtime in Tonk and Sapling.** Desirable, but too far out
  to say.

## References

- Dialog DB memory cells and effects:
  https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-effects/src/memory.rs
- Dialog DB memory layout:
  https://github.com/dialog-db/dialog-db/blob/main/notes/memory-layout.md
- UCAN: https://github.com/ucan-wg/spec
- The Wasm Component Model: https://component-model.bytecodealliance.org/

