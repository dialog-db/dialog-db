# #900: a premise query needs no entity, for `feat/supplied-formulas`

On top of #900's head `35862362c`, two commits:

- `a960d4db0`, the tests:
  - `notation/read` of a constant text, alone, on a branch holding nothing, answers a row per form;
  - `notation/analyze` of a constant `forms`, alone, on an empty branch answers its rows, and a subscription over it re-evaluates when a definition the stub read changes;
  - a selected variable a row leaves unbound (an optional field with no claim) is absent from the row, not an error;
  - `.this(variable)` of a variable nothing binds is refused with `UnselectedVariable`.

  Written against the new API, so this commit does not build alone.
- `d67c0f37d`, the change: `PremiseQuery::new(premises, select)`, and `.this(variable)` for rows that also carry an entity. `PremiseRow::this` is `Option<Entity>`.

To take them:

    git fetch scratch/engine-patches/supplied-formulas.bundle feat/supplied-formulas
    git checkout feat/supplied-formulas && git merge --ff-only FETCH_HEAD

Gate, on `d67c0f37d`: fmt, workspace clippy `-D warnings`, supplied_tests 16, dialog-query lib 1159, dialog-repository lib 970, and dialog-common's convention test all pass.
