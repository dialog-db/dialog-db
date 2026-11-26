# #900 review changes, for `feat/supplied-formulas`

On top of #900's head `0b783dc47`, three commits:

- `c4ed3e34b`, a merge of main `5a399332a`. CHANGELOG conflicted; both entries are kept.
- `9b79125d4`, the tests: a stub given two forms, the first defined, answers one row with `at` 1, and a constant `forms` that is not a list is refused with `TypeError::KindMismatch`. Written against the new signature, so this commit does not build alone.
- `3dfee59c2`, the change: the cell is `forms` (a list), `at` is a new output, `NotationAnalyze::analyze` takes `&[Value]`, and `supply_read`/`supply_analyze` are `#[doc(hidden)]` with who may call them.

To take them, with the merge:

    git fetch scratch/engine-patches/supplied-formulas.bundle feat/supplied-formulas
    git checkout feat/supplied-formulas && git merge --ff-only FETCH_HEAD

The two `.patch` files are the non-merge commits alone. They apply after a merge of current main.

Gate, on `3dfee59c2`: fmt, `clippy --all --all-targets --all-features -D warnings`, the `supplied_tests` (13), the dialog-query lib tests (1153) and dialog-repository lib tests (967), and dialog-common's convention test all pass.
