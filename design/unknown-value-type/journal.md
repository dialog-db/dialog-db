# Journal

|  | Understanding | Activity |
|---|---|---|
| **Status** | The key layout per index, how each value type ends, what a release does today with an unknown type byte (scans fail, a covering history record fails the pull, #884's cardinality-one write passes it over). The maintainer's lean survives the layout for bytes not yet assigned, not for the assigned ones, which interleave three framing classes. | Directing: four approaches in a matrix, written by the agent from the code; no sitting with the maintainer yet. |
| **Agenda** | Whether a future type needs fixed-width framing at all, or terminated serves every type the roadmap foresees (decides B against C). Whether the order of an attribute's bands across types is something any reader relies on (decides how much D's renumbering costs). How a writer learns that every replica of a space carries the rule before it writes a new tag. The two open hypotheses in the ledger. | The maintainer's choice of direction; then a design matrix for the fence. |

## 2026-11-26, Directing: from the lean to a matrix

**Asked.** The maintainer, through the coordinator: close the gap,
bundle it with the coming stored-format migration, design note first.
His lean: the type tag carries a framing class by range. Asked to state
the real key layout per index, give at least two approaches with a
matrix, check the lean against the layout, the number band and index
order, say what an old engine does today and what migration existing
repos need, and relate it to #857.

**Learned.**

- The value is the last component in EAV, AEV, history and coverage and
  the first in VAE; in every ordering its end must be known, in VAE to
  read the attribute and entity after it, elsewhere to tell a spill tail
  from value bytes.
- Today's type bytes mix three framing classes (fixed-width,
  escaped-terminated, self-delimiting) with no range naming one class,
  so the lean applies to bytes not yet assigned (C) unless every key is
  rewritten (D).
- The number band's self-delimiting codec cannot be a class an unknown
  reader steps over; a class must be a width or a terminator.
- The composite band (22) already follows rule B: a byte past the
  number band's is delimited as a byte string. B is that rule extended
  to every unassigned byte.
- A covering history record with an unknown type byte fails a pull,
  through `coverage_range`; data entries likely do not (open).
- #857 rewrites rules and concepts, not fact keys, so D is not a cost
  the migration already pays.

**Decided.** Nothing; the direction is the maintainer's. The agent's
reading, an opinion: B and C cost no byte and move no stored key, and
differ only in whether a future type may be fixed-width without
escaping; D buys one uniform table at the price of a full key rewrite
and a new band order.
