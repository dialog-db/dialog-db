# Claims: a rule that leaves a premise's subject unnamed is seeded by value alone

The tests named are in `rust/dialog-repository/src/repository/branch/transaction/induce/subject_tests.rs` on #791. Each runs one list of commits seeded as #791 seeds and as main does, and asserts both outcomes. "Recorded on main" is the same scenarios run on main at dd19d6c with that file added and nothing else changed, 2026-11-13.

| # | claim | kind | backing | status |
|---|---|---|---|---|
| 1 | On main a rule with an unnamed subject fires for an entity the commit did not touch, where one field is written and another entity holds the value | fact | recorded on main: `cmd.seen/kind` for `user:alice` and `user:bob`; `it_fires_for_the_entity_a_commit_wrote_and_no_other` | holds |
| 2 | The same rule with its subject named, in a variable nothing reads, fires for the entity written alone, on main | fact | recorded on main: `user:bob` alone; the same test | holds |
| 3 | The firing for an untouched entity writes what was not there: a command is issued again and acted on, and a durable value under `last` is put back over a later write | fact | recorded on main: counts 2 and 2, `alice`'s record back to `click`; `it_issues_no_command_for_an_untouched_entity`, `it_leaves_what_an_untouched_entity_holds` | holds |
| 4 | On main, which entities are fired for depends on whether a commit writes one field of an entity or two; the crate's check of grouped seeding against seeding row by row fails on a commit of two | fact | recorded on main: `it_fires_alike_for_one_field_written_and_for_two` panics with "grouped seeding must write the transients seeding row by row writes" | holds |
| 5 | On main a rule premised on an attribute a deductive rule derives from itself misses the entities whose derived facts changed, when its subject is named; with its subject unnamed it finds those that hold the value written | fact | recorded on main: through a mirror, `alice` alone with the subject named, `alice` and `carol` with it unnamed | holds |
| 6 | With the seed bound and a written attribute counted as derived when a deductive rule concludes it from something touched, both spellings fire in full: `alice`, `bob`, `carol` | fact | `it_fires_in_full_when_a_written_attribute_is_derived_from_itself` | holds |
| 7 | A rule fired in full fires for every match, on main and on #791 | fact | `it_still_fires_in_full_for_every_match` | holds |
| 8 | With the seed bound, seeding rows together and seeding them one by one agree for an unnamed subject | fact | the crate's check runs in every test that commits through a rule; `dialog-repository`'s 712 library tests pass on #791 | holds |
| 9 | No rule in use relies on being fired for every holder of a value | hypothesis | confirmed if tonk's rules that leave a subject unnamed write the same over a replayed replica on main and on #791; refuted by one that does not | open; not run |
| 10 | A rule should fire only for the entity a commit wrote | opinion | the agent, 2026-11-13 | - |
