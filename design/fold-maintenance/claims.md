# A folded value is recomputed for its subscriber: claims

| claim | kind | backing | status |
|---|---|---|---|
| a deductive rule can carry a `reduce` clause, evaluated by grouping on the head's other fields, with `count`, `count-distinct`, `sum`, `min`, `max`, `avg` | fact | `notes/aggregation.md`; `rust/dialog-query/src/reduce.rs` `Aggregator` | holds |
| an authored rule with a `reduce` is refused | fact | `rust/dialog-query/src/rule.rs` `open_rule_error`, `TypeError::ReduceInOpenRule` | holds on main at `8ce5f37` |
| keeping a fold current under a subscription was left for later, recomputing on each poll first | fact | `notes/aggregation.md`, Milestones, A5 | holds; whether "recompute" is of one group or of all is not read from the code |
| a subscription of a plain concept answers one changed fact in under a millisecond among 10 rows and among 1000 | fact | dialog-db/dialog-db#773, `rust/dialog-notation/examples/cost.rs`, `cargo run -p dialog-notation --example cost -- 5`, dev profile, medians of 5 | holds on that branch |
| what a reducing rule under a subscription costs as its groups grow | hypothesis | that it grows with the group or with every group. Confirmed or refuted by the same harness with the fold gate lifted, at 10, 100 and 1000 | open; being measured on #773 |
| if a view cannot be lightning fast it will not fly; grouping rows after the query is the last resort | opinion | Irakli, 2026-11-18 | open; no number is set for "lightning fast" |
| the engine may allow a deductive rule to fold, so long as the notation does not let a rule read one | opinion | Irakli, 2026-11-18 | decides the direction of dialog-db/dialog-db#830 |
| folding the changes, with a group's state held in the subscription, keeps a fold current at the cost of the change and with no change of format | hypothesis | the agent's, after DBSP and the counting algorithm. Refuted if a subscription cannot report what stopped holding without reading the group again | open |
| DBSP treats a change as a weighted row and derives the incremental form of a query; CouchDB keeps reductions in the inner nodes of its view B-tree; Noria keeps state only for keys that are read | fact | Budiu and others, "DBSP: Automatic Incremental View Maintenance for Rich Query Languages", VLDB 2023; CouchDB's `couch_btree`; Gjengset and others, "Noria", OSDI 2018 | holds; cited from the papers' abstracts and the agent's recall, not re-read in full for this note |
