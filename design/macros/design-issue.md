# How a head says it is issued

**Decision:** what the kernel writes for a head that holds one round, now that `assert` is the write that stays.

**For:** no form carries `!` (#728, R45). Until 2026-11-16 the kernel told three heads apart by word: `assert!` stays, `assert` holds a round, no word derives. Take the mark away and `assert` has two meanings.

Nobody writes the held head. It is what the prelude answers for `(add (todo …))` in a head's place:

```
 written                          kernel, before                       kernel, now
 (assert! (xyz.todo …))           (assert! (xyz.todo …))               (assert (xyz.todo …))        kept
 (add (todo …))    in a head      (assert (command.add.todo …))        (command.add.todo …)         one round
 (xyz.pending …)   in a head      (xyz.pending …)                      (xyz.pending …)              while the premises hold
 (add (todo …))    in a premise   (command.add.todo _ …)               (command.add.todo _ …)       read
```

| criterion \ approach | A word: `(issue A)` | A keyword: `(:issue A)` | No word: the relation says |
|---|---|---|---|
| a name an author may want | 🟡 takes one more. `issue`, `hold`, `dispatch`, `do` are each a likely command: `(issue (invoice ?i))` | none taken: a keyword is never a command's name | 🟢 none taken, and none added |
| a stored concept marked to hold one round by mistake | 🔴 `(issue (todo ?t))` marks `xyz.todo` transient and says nothing. A rule with the head `(assert (todo ?t))` did the same until now, one character from the write (read off `kernel.rs` and `compile.rs` as they were; not run) | 🟡 the same, though not by a name's accident | 🟢 cannot be said: only a command's relation is issued |
| where the lifetime is said | on the call, and again on the relation, which the branch is told is transient | the same | on the relation alone. #728: "Lifetime belongs to what is called, not to the call" |
| a command kept by mistake | `(assert (command.add.todo …))` compiles | the same | refused: a command holds one round |
| what the kernel has to know | a fourth head word | a head that is a formula's shape, which no other head is | which relations are a command's: those under `command.`, where the prelude already puts every one |
| a spelling for issuing, should #728 take `(do add …)` | a rule in the prelude | a rule in the prelude | a rule in the prelude |
| deriving a command, held while premises hold and never dispatched | sayable, with no word | sayable | 🟡 not sayable. Nothing asks for it: a rule that concludes a command issues it |

**Decided, 2026-11-16, by the agent, the maintainer away:** no word. It is the one column where the lifetime is said once, and the one where the mistake found on the way cannot be written. The cost is that `command.` is a domain the kernel knows, which it was already in all but name: a premise on a command reads `command.add.todo`, so a fact asserted there by hand was already a forged command.

Rests on one opinion, the maintainer's on #728: no form carries `!`. He decided the opposite here on 2026-11-11 ("without `!` the new count does not persist"), when a plain `assert` head was not kept. It is kept now, so what he wanted then holds; the mark is what went.

Pinned by `it_issues_what_a_head_with_no_word_says_of_a_command` (kernel) and `it_refuses_a_write_under_the_name_it_had` (expander). The second was checked against the build without the refusal: `(hold! (todo todo:1 text: "x"))` expands to `(rule (command.hold!.todo …))` with the warning "no rule reads this command", and nothing is written.
