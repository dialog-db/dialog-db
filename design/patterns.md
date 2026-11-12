# A parameter holds one variable or one constant

**Phase: Building.** The direction is the maintainer's, decided 2026-11-11. The implementation decisions are for whoever builds it, each in its own file beside [the plan](patterns/plan.md); the [journal](patterns/journal.md) says how far the build is.

## Description

A premise is applied to parameters, a map from a name to a `Term`, and a `Term` is a variable or a constant (`rust/dialog-query/src/parameters.rs`, `term.rs`). A constant is a `Value`, and a `Value` is a scalar or `Record`, bytes dialog does not look into (`rust/dialog-corpus/src/model/value.rs`). The one record dialog writes is a revision's, dag-cbor that formulas of fixed cells take apart (`history/revision_record.rs`, `rust/dialog-query/src/formula/revision.rs`).

So a rule cannot hold a value made of other values, cannot take one apart by its shape, and cannot make one from what it has bound.

Where it shows:

- The notation being designed in [macros](macros.md) writes a struct where a value stands, `{text: ?x}`, and its proof of concept (#773) refuses it.
- A rule that issues a command, or creates something, names an entity from what it bound. `entity/derive` there takes two values and is chained a link a value, because it cannot be given one record.
- A rule about syntax takes a form apart and builds another. The proof of concept does that in memory, outside dialog, since a form is not a value dialog can hold.
- The record values research (#748) has what a record is and how its bytes might be laid out, and nothing built.

## Problem

A rule cannot say a value by its shape: dialog has no value made of values, and a parameter has no way to hold more than one variable.

## Approach

```
 today      Term    = Variable | Constant

 decided    Pattern = Variable | Constant         the leaves: what Term is
                    | Map   { key: Pattern … , rest }
                    | List  [ Pattern … , run ]
                    | Tuple [ Pattern … ]
```

- **A parameter holds a pattern.** Where the value is bound, the pattern takes it apart and binds what is inside it. Where everything inside it is bound, the pattern is the value. One thing, both ways.
- **A composite value first.** A map, a list or a tuple of values, kept whole as a `Value::Record`: dag-cbor under a version byte, so a later layout is a later version. Named by a merkle reference of its parts, so no layout moves a name. A revision's record is a bare dag-cbor map and never starts with the version byte, so the two tell apart and nothing stored is rewritten.
- **The planner and inference look inside.** A pattern is feasible by the variables in it, not as one variable. Its type follows from its shape.
- **No stored format and no wire form changes for it now.** Rules of tonk's notation never hold a pattern. What installing a rule that holds one does is to be said, not left to happen.

Out of scope: the layout of a record's bytes beyond the first version (#748's), and rules of several heads ([rule-heads](rule-heads.md), #776).
