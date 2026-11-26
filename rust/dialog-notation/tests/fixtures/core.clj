; core: the concepts of tonk's core that the notebook reads and does not
; declare, from tonk's rust/tonk-core/assets/library/core.yaml at
; c006b50, in the notation as research/notation/design.md has it. A
; component is read joined with core, so the notebook sketch is read
; after this one. Only the four concepts the sketch uses are here.
;
; A line starting `; GAP:` says what the design has no spelling for, and
; the YAML lines that need it, as in notebook.clj. Every line here is
; `cardinality: :one`: core's fields give no pick, and tonk reads such a
; field's newest claim.

; A tab's location and the route concept it renders.
; GAP: a concept pinned to an entity, `this: tonk:site`. The design gives
; a concept the identity of its attributes. YAML 1700.
; GAP: an optional field, `maybe: selection`: the text selected in the
; tab's page, absent when nothing is. The design lists optional fields
; open, so the field is left out. YAML 1744-1751.
(concept (site ?s path: ?path anchor: ?anchor space: ?space branch: ?branch
               replica: ?replica branch-entity: ?branch-entity
               profile-branch: ?profile-branch route: ?route concept: ?concept)
  ; Currently active path on this site.
  (xyz.tonk.site/path of: ?s is: ?path as: :text cardinality: :one)
  ; Currently active anchor (URL hash) on this site.
  (xyz.tonk.site/anchor of: ?s is: ?anchor as: :text cardinality: :one)
  ; The space (repository name) on this site.
  (xyz.tonk.site/space of: ?s is: ?space as: :text cardinality: :one)
  ; The active branch name on this site (defaults to "main").
  (xyz.tonk.site/branch of: ?s is: ?branch as: :text cardinality: :one)
  ; Currently active replica on this site.
  (xyz.tonk.site/replica of: ?s is: ?replica as: :entity cardinality: :one)
  ; The same branch as an entity, so a view can walk from where the tab is
  ; to what that branch follows. The name beside it is a location token;
  ; this is the graph node.
  (xyz.tonk.site/branch-entity of: ?s is: ?branch-entity as: :entity cardinality: :one)
  ; The branch the profile is on, whichever repository this tab is on:
  ; what reaches the profile as `{profile-branch}@profile:tonk`.
  (xyz.tonk.site/profile-branch of: ?s is: ?profile-branch as: :text cardinality: :one)
  ; Matched route of this site.
  (xyz.tonk.site/route of: ?s is: ?route as: :entity cardinality: :one)
  ; Matched route's concept (the model the shell mounts).
  (xyz.tonk.site/concept of: ?s is: ?concept as: :entity cardinality: :one))

; A route: maps a path pattern to the model the SW mounts.
; GAP: a concept pinned to an entity, `this: tonk:route`. YAML 1804.
(concept (route ?r path: ?path concept: ?concept)
  ; The axum/matchit path pattern.
  (xyz.tonk.route/path of: ?r is: ?path as: :text cardinality: :one)
  ; The route model to render when this path matches.
  (xyz.tonk.route/concept of: ?r is: ?concept as: :entity cardinality: :one))

; One opening of the command palette, as interpreted so far. Session facts.
; GAP: an optional field, `maybe: selection`, as on `site`. YAML 2750-2754.
; GAP: a layer named by the YAML's prose, "Session facts": the design's
; `in:` names a layer, and core says it in a description only.
(concept (intent/expression ?e input: ?input site: ?site time: ?time)
  ; What is typed.
  (tonk.dialog.intent.expression/input of: ?e is: ?input as: :text cardinality: :one)
  ; The tab's site, where the palette was opened.
  (tonk.dialog.intent.expression/site of: ?e is: ?site as: :entity cardinality: :one)
  ; When the line was last interpreted.
  (tonk.dialog.intent.expression/time of: ?e is: ?time as: :float cardinality: :one))

; What an expression could mean as one command. Rules derive the command's
; field values onto it.
(concept (intent ?i expression: ?e command: ?c)
  ; The expression it was read from.
  (tonk.dialog.intent/expression of: ?i is: ?e as: :entity cardinality: :one)
  ; The command it would run.
  (tonk.dialog.intent/command of: ?i is: ?c as: :entity cardinality: :one))

; Invocations: what became of a command someone ran. An invoker that wants
; to follow a command (the command palette) stamps it, as it lands in
; `control:` for one tick, with when it was run and which command it is;
; rules copy that command's life into the stack's `session:` layer.
; Ported from tonk's core.yaml 3005-3036.

; A followed command as it lands, in `control:` for one tick.
; GAP: a concept pinned to an entity, `this: tonk:command/invoked`: the
; notation names no entity for a concept (gaps row 6). YAML 3026.
; GAP: a sized type, `as: float:64`: the notation's types are unsized, so
; `at` is `:float`. YAML 3031.
(concept (command/invoked ?c at: ?at concept: ?concept)
  ; When the invoker ran it, in milliseconds since the epoch, by its own clock.
  (xyz.tonk.command/invoked-at of: ?c is: ?at as: :float cardinality: :one)
  ; The concept of the command that was run.
  (xyz.tonk.command/concept of: ?c is: ?concept as: :entity cardinality: :one))
