# #900: a premise row's key built where an error can carry it

On top of #900's head `d67c0f37d`, one commit, `ad9db6b98`:

- `premises:`, the entity a row with no entity of its own is kept under, is parsed in `PremiseQuery::realize`, failing as `EvaluationError::Store`, the way `fold.rs:228-232` parses `fold:`. It is no longer an `expect` in a `LazyLock`.
- `PremiseRow`'s fields are private (`clippy::partial_pub_fields` refuses a mix), read through `entity()`, `values()` and `get(variable)`.
- No behaviour changes, so there is no failing test first; the 16 supplied tests pin what it does.

To take it:

    git fetch scratch/engine-patches/supplied-formulas.bundle feat/supplied-formulas
    git checkout feat/supplied-formulas && git merge --ff-only FETCH_HEAD

Gate, on `ad9db6b98`: fmt, workspace clippy `-D warnings`, supplied_tests 16, dialog-query lib 1159, dialog-repository lib 970, dialog-common's convention test.
