# A parameter holds one variable or one constant

**Phase: Implementation.** Patterns use the canonical composite value from [#843](https://github.com/dialog-db/dialog-db/pull/843), with Chronicle sharing its scalar atoms through tokens in [#849](https://github.com/dialog-db/dialog-db/pull/849). The implementation work is in [the plan](patterns/plan.md).

## Description

A premise is applied to parameters, a map from a name to a `Term`, and a `Term` is a variable or a constant (`rust/dialog-query/src/parameters.rs`, `term.rs`). A constant is a `Value`: a scalar or a `Composite`, a map or list that can be read by shape (`rust/dialog-corpus/src/model/composite.rs`). Its canonical header table holds each scalar atom used by it and its nested composites once. Chronicle interns those atoms in the same token dictionary it uses for standalone values; see [the composite design](../composite-value.md).

Patterns let a rule hold a value made of other values, take one apart by its shape, and make one from what it has bound.

Where it shows:

- The notation being designed in [macros](macros.md) writes a struct where a value stands, `{text: ?x}`, and its proof of concept (#773) refuses it.
- A rule that issues a command, or creates something, names an entity from what it bound. `entity/derive` there takes two values and is chained a link a value, because it cannot be given one record.
- A rule about syntax takes a form apart and builds another. The proof of concept does that in memory, outside dialog, since a form is not a value dialog can hold.
- The earlier record values research (#748) has been superseded by the composite value implementation and layout in #843 and #849.

## Problem

A rule cannot say a value by its shape: dialog has no value made of values, and a parameter has no way to hold more than one variable.

## Approach

```
 today      Term    = Variable | Constant

 decided    Pattern = Variable | Constant         the leaves: what Term is
                    | Map   { key: Pattern … , rest }
                    | List  [ Pattern … , run ]
```

- **A parameter holds a pattern.** Where the value is bound, the pattern takes it apart and binds what is inside it. Where everything inside it is bound, the pattern is the value. One thing, both ways.
- **Use the composite value.** A map or a list of values, as the IPLD data model has them and no tuple. Patterns build and take apart `Value::Composite` maps and lists. The scalar atoms in a composite and all its nested composites share one sorted table; Chronicle maps those atoms to the same tokens used by standalone values. Composite identity comes from its parts, independent of its canonical bytes and token IDs. A revision record remains its own bare dag-cbor map.
- **The planner and inference look inside.** A pattern is feasible by the variables in it, not as one variable. Its type follows from its shape.
- **Use the canonical composite value.** Patterns build and match `Value::Composite`; they add no separate record encoding or token table. Composite canonical bytes and token sharing are defined by #843 and #849.
- **No stored format and no wire form changes for patterns.** A rule that holds a structural pattern has no stored body; its identity and runtime overlay behavior are handled by the query/rule implementation.

Out of scope: designing another composite encoding or token table, and rules of several heads ([rule-heads](rule-heads.md), #776).
