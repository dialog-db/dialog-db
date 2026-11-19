# How tonk keeps a view current today

A reading of tonk-labs/tonk on `main` at `c006b50`, made on 2026-11-19 by a research thread for this topic. Paths are relative to that repository unless marked as dialog; dialog was read at the tag tonk pins, `v0.5.0-rc.6`. Nothing was built or run. Every statement is tagged READ, TESTED (a named test pins it) or INFERRED.

### 1. From a view to what runs

**The plan.** The planner splits a template into `chrome`, which renders once against the first conclusion, and one `repeat` element, which is cloned once per conclusion (READ `rust/tonk-template/src/lib.rs:163-190`). Inside a repeat, any field that is not scalar becomes a `PlanNode::Iteration` over that field's values (READ `lib.rs:206-215`).
- **Choosing the repeat root.** `this_repeat_root` takes the shallowest element that refers to the subject. If a bare `{this}` attribute sits on that element (`binds_this_marker`), that element repeats. Otherwise the fragment-root element repeats (READ `lib.rs:303-308`, `357-403`).
- **Scalar fields are never iterated.** Fields of cardinality one, and `/key` fields, are excluded from iteration (READ `lib.rs:484-492`). The set of scalar fields comes from the model's descriptor (READ `rust/tonk-template/src/resolve.rs:286-302`; TESTED `it_does_not_make_a_cardinality_one_field_an_iteration_root`, lib.rs:864).
- **One doc is stale.** `rust/tonk-core/docs/templates.md:59-75` says the plan "does *not* consult the descriptor". That is no longer true: the code passes the scalar set in (READ).

**What one `<tonk-display>` opens** (READ `rust/tonk-display/src/element.rs:20-22`, `1304-1383`, `1392-1532`):
- **One-shot queries:**
- a name lookup, when `model` is a name (`element.rs:1615-1634`);
- after each view frame, `view_bindings_query` (`resolve.rs:401-415`), plus up to two more per event declaration the compiled bindings do not cover (`element.rs:2261-2350`);
- the embed, style and font reads (`embed.rs:236`, `290`, `503`).
- **Live subscriptions, up to four, each tagged:**
- `name`: `id:<name>` → `db.name/referent` (`resolve.rs:575-588`). Only when the model is a name.
- `model`: the concept-of-concepts row with `this` pinned to the model, projecting `source` (the descriptor JSON) (`resolve.rs:601-625`).
- `view`: the model entity's whole `show` dictionary, one row per facet (`resolve.rs:38-44`). Skipped when a model renders inside itself.
- `entity`: the model's fields, either with `this` pinned (`entity_query`, `resolve.rs:259-272`) or, in directory mode, with `this` unbound (`instances_query`, `resolve.rs:451-464`).

**The notebook page** has four nested displays:
- `tonk:site` → the route model (READ `core.yaml:1774-1783`);
- `tonk:notebook/route` → `<tonk-display entity={entity} model=notebook>` (READ `notebook.yaml:458-464`);
- the notebook's `ui` view (READ `notebook.yaml:308-330`);
- inside it, a directory `<tonk-display model=notebook/block>` (READ `notebook.yaml:327`, `359-369`).

That comes to about 14 subscriptions for the notebook part of the page: four displays times model, view and entity, plus `name` for the two named models (INFERRED from the list above). Per subscription:
- **The notebook's entity subscription** returns one flat row per block entry, and every row repeats `title` (`block`, `block/key` and `title` are all bound: READ `resolve.rs:243-249`; TESTED `it_binds_the_key_operand_of_a_collection_field`, resolve.rs:853). The `block` field iterates as one hidden `<span data-block data-key>` per entry.
- **The block directory subscription** returns every `notebook/block` in the space, not only this notebook's. `<tonk-notebook>` filters by `data-notebook` in the DOM (READ `rust/tonk-inspector/src/notebook.rs:918-953`).
- **The list is scoped per notebook, not per item.** There are no per-block subscriptions.

**A list whose items are nested displays.** The default directory view `tonk:_` renders one nested `<tonk-display entity={this} model={dom.host/model}>` per instance (READ `core.yaml:32-50`; `table.yaml:543-548` does the same). See question 6 for what that costs.

### 2. On a change

1. **A write.** A notebook writes by evaluating notation text (`notebook.rs:1303-1325`). Element events post commands. After a request's writes, `run_scheduled_polls` settles each branch once: "the writes of a turn cost one evaluation per subscription" (READ `rust/dialog-reactor/src/lib.rs:261-276`; called from `router/evaluate.rs:206,352`, `command.rs:857`, `sync.rs:117,157,...`).
2. **Dialog decides who wakes, and what changes.** Each subscription compares the tree diff with its demand cover:
 - outside the cover: no evaluation at all;
 - inside a fact range: per-entity DRed maintenance, which over-deletes that entity's rows, re-derives the query restricted to that entity, and emits a `Delta`;
 - inside a rule range, or with recursion: full recompute.

 READ dialog `rust/dialog-repository/src/repository/branch/subscription.rs` at rc.6, lines 1-66.
3. **Which plans can be maintained.** tonk's `QueryPlan` maintains only `Standard` concept queries. The concept-of-concepts, command and rule enumerations return `Restriction::Unsupported` and recompute every time (READ `rust/tonk-schema/src/concept.rs:1278-1290`). The `model` subscription is therefore recomputed whenever it wakes. The comment at `element.rs:1706-1724` says it "re-pushes on every branch revision, including plain entity-data writes". The worker also re-serializes the descriptor with "non-deterministic key ordering" (`element.rs:131-137`, `1718-1724`), so a recompute that finds nothing new still yields a non-empty delta and a frame (READ the comments; INFERRED that this is why the frame is sent). The display compares the parsed descriptor and does nothing if it is unchanged (TESTED `it_does_not_remount_on_a_repeat_model_frame`, element.rs:6693).
4. **On the wire.** The first frame is a `Snapshot` with every row. Later frames are `Delta {asserted, retracted}`, and an empty delta is not sent (READ `rust/dialog-reactor/src/subscription/connection.rs:76-107`). Frames are delivered outermost display first, by nesting level (READ `branch/state.rs:280-326`).
5. **On the page, every frame is rebuilt in full.** `on_update` applies the delta to a stored copy of the full row set (`apply_delta`, which matches rows by value and repairs drift; READ `element.rs:843-883`, `921-972`; TESTED `it_applies_a_delta_equivalently_to_a_snapshot`, `it_preserves_other_rows_of_a_multi_valued_entity_through_a_delta`, element.rs:4954, 5040). It then sends the whole merged set down the same path a snapshot takes: `select_rows` folds all rows (`fold.rs:84-145`), the whole frame is serialized to JS and deserialized again in `<tonk-view>.draw` (READ `element.rs:2760-2767`, `view.rs:153-175`), and `Renderer::apply` runs.
6. **The renderer.** `update_repeat` walks every incoming subject and re-renders every binding as a string. It writes to the DOM only where a cached value differs (READ `render.rs:391-546`, `752-777`; TESTED `it_rewrites_only_changed_bindings_on_subsequent_apply` and `it_preserves_node_identity_for_unchanged_bindings_across_applies`, view.rs:1056, 1030). So one changed fact costs O(N rows) of folding, serde and string rendering on the page, plus one DOM write (INFERRED cost).

**The four notebook cases** (all INFERRED from the paths above, unless tagged):
- **Editing a block's source.**
- The prose editor debounces for 400 ms (READ `tonk-prose/src-js/index.ts:77`), then writes.
- The block directory subscription maintains that one block and sends a delta of one row retracted and one asserted.
- The page re-folds every block in the space, and one `data-source` attribute changes.
- A `MutationObserver` in `<tonk-notebook>` defers with `setTimeout 0`. `project_blocks` then queries every row in the DOM again, sorts them, rebuilds the whole markdown document and assigns it to `prose.value`, which diffs characters (READ `notebook.rs:762-890`, `1021-1070`).
- **Inserting a block in the middle.** Two subscriptions change: the notebook's sequence entry and the new block row. They arrive as separate frames. `notebook.yaml:876-878` says: "the entries and the block rows are separate renders that do not update together" (READ). The element holds its projection until its pending inserts appear (`adopt_inserts`, `notebook.rs:1078`). In the DOM, one span and one row are cloned and inserted at their sorted place (TESTED `it_iterates_a_keyed_collection_in_key_order`, render.rs:1451).
- **Renaming a notebook.** Every row of the notebook's projection carries `title`, so the delta retracts N rows and asserts N rows for a notebook with N blocks. The DOM ends up with one changed `title` attribute.
- **Removing a block.** Rules retract the entry and the block's facts (READ `notebook.yaml:935-965`). Rows are retracted and the renderer removes the row and the span (READ `render.rs:489-499`, `849-858`).

### 3. Repetition and identity

**Keys.**
- A repeat row is keyed by the conclusion's `this` (READ `render.rs:86-93`).
- An iteration row is keyed by the value's string form for a list, and by the entry key for a keyed collection. A scalar is keyed by `this` (READ `render.rs:948-956`, `1049-1059`).
- Duplicate values collapse into one row (READ `render.rs:829-832`, `fold.rs:117-127`).

**Order.**
- The first mount inserts rows in frame order. Later updates insert new rows at their sorted key position, comparing subject URIs or value strings (TESTED `it_mounts_rows_in_frame_order_not_sorted`, render.rs:1501, and `it_inserts_a_run_of_new_rows_between_existing_ones`, render.rs:1532). The two orders disagree.
- Rows are never moved. A changed key removes the old row and clones a new one. The one exception is the single-row rename heuristic (READ `render.rs:472-486`, `838-847`).
- A template cannot order rows by a field. Elements do it themselves: the notebook's `sort_by_key` (`notebook.rs:1028`) and the table's `data-order` (`table.yaml:485`) (READ).

### 4. Counting and collecting today

Neither Dialog nor the planner aggregates anything. Collection happens in two places:
- **The fold.** `select_rows` folds rows that share a `this`. A field becomes a list ("distinct per-field values in first-seen order"), or a map for keyed collections (READ `fold.rs:3-14`, `103-145`).
- **Element code.** Three examples, all READ:
- The FAB member roster subscribes to all members and then writes `count.set_text_content(Some(&graph.nodes.len().to_string()))` (`rust/tonk-fab/src/member_roster.rs:484-496`).
- `profile.yaml` counts from worker API fetches, not subscriptions, for example `const count = group.targets.filter(target => target.acknowledged).length;` (`profile.yaml:5778`) and `fetch(`${origin}/api/agents`)` (`profile.yaml:2598`).
- The notebook and table elements assemble their collections by reading hidden DOM rows (`notebook.rs:918-1002`).

The library uses `pick: :all` and `:last`, but no count, sum, min or max picks (READ, by grepping the `pick:` values). The notebook avoids aggregates on purpose: "an aggregate inside that recursion is unstratified: inserts derived nothing at all" (READ `notebook.yaml:105-107`, `756-758`).

### 5. Optional (`maybe:`) fields

- **Storage.** A stored descriptor has no top-level `maybe` block. Optional fields sit in `with` with `"optional": true` (READ dialog `rust/dialog-query/src/concept/descriptor.rs:57-69`; the reconstruction is at `tonk-schema/src/concept.rs:279-320`).
- **Reading.** The entity and directory queries bind every field in `with`, optional ones included (`resolve.rs:259-272`). Dialog returns a fallback row with the slot `Absent`, and `project` leaves that field out (READ `rust/tonk-core/src/conclusion.rs:31-47`).
- **Rendering.** A missing field renders as an empty string (READ `lib.rs:711-749`). Optional scalar fields are not iteration roots, so the host element survives when the value is absent (TESTED `it_keeps_a_scalar_field_host_when_the_value_is_absent`, view.rs:481). An attribute mixing literal text with a missing field is not written until the field lands (READ `render.rs:977-993`).
- **Under subscription** they behave like any other field of the entity: an absent-to-present change is an ordinary per-entity delta (INFERRED).
- **Workarounds.** Where an optional value would otherwise be pinned, tonk splits it into a separate query so a missing value does not drop the whole row: `event_flags_query` (`resolve.rs:182-227`) and `view_bindings_query` / `view_embeds_query` (`resolve.rs:390-443`) (READ).

### 6. Nested views

- **Each nested `<tonk-display>` runs its own full pipeline.** The renderer builds rows detached so a nested display mounts once (READ `render.rs:33-35`).
- **For a list of N nested displays:**
- `model`, `name` and `view` are identical across the N. In the page they share one upstream stream, keyed by URL plus the dag-json body (READ `rust/tonk-host/src/ops.rs:776-830`; TESTED `it_shares_one_upstream_between_identical_subscriptions`, ops.rs:1708). The URL carries `?level=N` (READ `tonk-host/src/url.rs:48`), so the same query at a different depth gets its own upstream.
- A late joiner can share a stream only within the first 64 frames after a snapshot (READ `stream.rs:27`). After that it opens its own upstream.
- On the worker, identical queries share one evaluation, by `QueryHash` (READ `subscription/plan.rs:26-31`).
- `entity` is distinct per item: N SSE streams and N Dialog subscriptions, each checked against its cover on every write (INFERRED).
- Each item also runs its own one-shot reads after its view frame (bindings and embeds). I found no dedup for one-shot queries (INFERRED).

### 7. Cost as written

**Measurements.**
- "a 200-row list opens the same three queries 200 times ... Measured on a 200-note space, two in three subscriptions a warm reload opened were exact duplicates" (READ `tonk-host/src/stream.rs:4-9`).
- `plan/perf-gates.md` says "CPU is not tonk's suspected problem". The gates count guest boots, content removals and empty frames, not render cost. Its stability table records rows wiped and rows re-inserted (READ `perf-gates.md:12-16`, `120-131`; ratchet in `bench/stability/ratchets.json`).
- `bench/perf/run.sh` and `bench/perf/why-blocks.sh` measure join and sync cost in blocks and requests, not rendering (READ).
- A settle that takes 500 ms or more is logged with the number of subscriptions held (READ `branch/state.rs:335-355`).
- I found no benchmark of view render or subscription latency per change.

**Batching:**
- one poll per request (`lib.rs:261-276`);
- empty deltas dropped (`connection.rs:89-91`);
- insertions batched into document fragments (`render.rs:337-351`, `501-545`);
- the 400 ms prose debounce;
- the notebook observer deferred with `setTimeout 0` (`notebook.rs:790-805`);
- 25 projection retries 120 ms apart (`notebook.rs:358-359`);
- a 5 s hold when a reconnect delivers an empty frame (`element.rs:2665-2693`, `3452-3454`; TESTED `it_holds_content_on_a_reconnect_empty_frame`).

No timer-based throttling of polls exists (INFERRED from searching). `reactor-spec.md:24-35` describes the original "re-run every subscription" design, which Dialog's stack subscriptions have since replaced (READ `dialog-reactor/README.md`).

### 8. Where definitions live

Definitions are ordinary facts on the space branch. The standard library (`/library/core.yaml`, `profile.yaml` and components) is evaluated into claims and committed, together with records of the install and its version (READ `router/repository.rs:4029-4040`, `4988-5019`, `4187-4196`). Where each kind lives:
- **Concepts:** a `db.meta/concept` marker, `db.concept.with/<field>` → an attribute entity, and boolean optional markers. The descriptor is rebuilt from these facts and synthesized as `source`, together with built-in concepts from `concept_registry` (READ `tonk-schema/src/concept.rs:837-859`, `250-320`, `958-1030`).
- **Names:** `id:<name>` `db.name/referent` (READ `resolve.rs:559-588`).
- **Views:** `<model> xyz.tonk.view/<facet> <template>` facts on the model entity, plus the `xyz.tonk.view/bindings`, `xyz.tonk.view/embeds`, style and font dictionaries (READ `resolve.rs:315-443`).
- **Commands and rules:** read through the command-of-commands and rule-of-rules enumerations (READ `concept.rs:1175-1224`).

Second replicas receive these the way they receive any other fact, through branch sync (READ `ARCHITECTURE.md:40-55`; INFERRED that no separate channel exists). An older stored shape is migrated when the branch first opens (READ `router/query.rs:119-169`). Rendering a view reads them back through the `name`, `model` and `view` subscriptions described in question 1.

### 9. What tonk needs from Dialog's subscription API

**Things tonk works around, with evidence:**
- **The page cannot use deltas incrementally.** It rebuilds the full set and re-folds it (`element.rs:835-842`). The comment at `element.rs:797-798` is stale: it still says the worker always sends `reset` and that deltas will come "with DBSP integration".
- **Stored rows drift from deltas,** which needs the drift repair (`element.rs:903-913`).
- **Rows come back as a cross product.** A query with two keyed collections "would join entry against entry, handing back the cross product" (`resolve.rs:99-106`). Every many-valued field multiplies the rows, and the fold removes duplicates.
- **Optional values had to be split out** into separate queries, so a missing one does not drop the row (question 5).
- **Metadata queries cannot be maintained,** and the descriptor JSON is not deterministic (question 2.3).
- **Two collections in one view cannot be read together.** There is no consistent snapshot across subscriptions, so the notebook needs pending overlays, a stray-entry cleanup and the rule that "retracts every other entry" (`notebook.yaml:874-901`; `notebook.rs:682-703`).
- **No ordering of directory rows, and no aggregates** (questions 3 and 4).
- **The `pending` and `upgrading` control frames** exist so an absent branch is not read as "nothing matched" (`router/query.rs:171-181`).
- **Identical subscriptions open separately per depth** (question 6).

**What Dialog already provides and tonk relies on:** a shared evaluation per `QueryHash`, cover gating, per-entity maintenance (`subscription/plan.rs:75-81`), and delivery ordered by level.

### Requirements a new design must meet, as tonk's code shows them

1. **Cost of a one-fact change must not grow with the list on the page.** Today it is O(N): re-fold, two serde passes and a render of every binding (question 2.5).
2. **Rows of one list must update together.** Data that one view needs should arrive as one consistent frame, not several subscriptions that can disagree (`notebook.yaml:876-878`).
3. **Lists must be keyed and ordered.** Ordered by a key or position the author chooses, rows kept stable across reorders, insertion without moving survivors (question 3; keyed collections in `plan/keyed-collections.md:1-12`).
4. **Counts and collected values must come from the engine.** Today elements compute them (question 4), and aggregates inside the notebook's recursive rules are refused as unstratified.
5. **Optional fields must not drop rows** under maintenance (question 5).
6. **Lists of nested views must cost about one subscription per list,** not N (question 6; the 200-row measurement).
7. **Absence must be told apart from "not loaded".** Keep `pending`, the hold on an empty reconnect, and empty-then-refill counted as a defect (`perf-gates.md:52`).
8. **Changing a definition must recover live.** A concept or view edited or seeded late must re-resolve without a reload, and must compare definitions independently of their serialization (questions 2.3 and 8).
9. **Notation definitions must be stored as synced facts on the branch,** read back by live queries and migrated when a branch opens (question 8).
10. **The page must not be told about writes that cannot change it.** Today a data write still re-pushes the `model` frame (`element.rs:1706`).

### What I could not determine

- The actual extent of the demand cover for the concept-of-concepts and name subscriptions, which would show whether a plain data write really wakes them. I relied on the comment only.
- Real latency or CPU numbers for a render or a delta. None are in the repository.
- Whether one-shot `tonk-query` requests are deduplicated anywhere. I found no deduplication.
- The exact number of subscriptions on a live notebook page, including outer chrome such as the FAB and the site shell. My figure of about 14 covers the notebook chain only.
- Whether the descriptor's non-deterministic key order still happens at rc.6, or is only a historical comment.
