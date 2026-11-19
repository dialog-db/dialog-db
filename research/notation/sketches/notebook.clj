;; The notebook library, redone in this notation. It starts from tonk's
;; rust/tonk-core/assets/library/notebook.yaml (origin/main) but is not a
;; line-by-line translation: workarounds the notation no longer needs are gone.
;;
;; Assumed here, beyond what design.md agrees:
;; - An attribute line is a call on the stored name:
;;   `(xyz.tonk.notebook/title ?of is: ?is as: :text pick: :last)`.
;; - `layer: :now` puts a command's fields in the layer that lasts one tick:
;;   written to trigger rules, never stored.
;; - A write that leaves the entity out lands on the digest of every
;;   attribute and value it writes, so a command needs no line saying how its
;;   entity is made.
;; - `(option ?x)` marks a parameter a match may leave unbound.
;; - A retract through a concept that omits parameters retracts all their values.
;; - `event!` and `view!` name what they define by their first argument, the
;;   way `concept!` does. How a name binds to an entity is the naming thread.
;; - `"""…"""` is a string that may hold `"`.

;;; Attributes

(concept! (notebook/title ?of is: ?is)
  (xyz.tonk.notebook/title ?of
      is: ?is
      as: :text
      pick: :last))

;; The notebook's blocks in document order, keyed by fractional position.
(concept! (notebook/entry ?of is: ?is)
  (xyz.tonk.notebook ?of
      is: ?is
      as: (:sequence :entity)
      pick: :all))

(concept! (block/notebook ?of is: ?is)
  (xyz.tonk.notebook.block/notebook ?of
      is: ?is
      as: :entity
      pick: :last))

(concept! (block/source ?of is: ?is)
  (xyz.tonk.notebook.block/source ?of
      is: ?is
      as: :text
      pick: :last))

;; Where a block sits. Derived (see below), except at the document edge.
(concept! (block/at ?of is: ?is)
  (xyz.tonk.notebook.block/at ?of
      is: ?is
      as: :text
      pick: :last))

;;; Concepts

;; A notebook with no blocks yet is still a notebook, so its entries are optional.
(concept! (notebook ?n title: ?title entry: (option ?entry))
  (notebook/title ?n is: ?title)
  (notebook/entry ?n is: ?entry))

(concept! (notebook/block ?b notebook: ?n source: ?s)
  (block/notebook ?b is: ?n)
  (block/source ?b is: ?s))

;;; Commands

;; Insert a block between two others. The element names no entity: the
;; insert's entity is the digest of its fields, and it becomes the block's.
;; `before:` is the block it precedes, or tonk:notebook/edge at the end.
;; `after:` is the existing block the run of inserts follows, or
;; tonk:notebook/edge at the start; every insert of one run carries the same
;; `after:`.
(concept! (block/insert ?b notebook: ?n source: ?s after: ?lo before: ?hi)
  (xyz.tonk.notebook.insert/notebook ?b is: ?n as: :entity pick: :last layer: :now)
  (xyz.tonk.notebook.insert/source ?b is: ?s as: :text pick: :last layer: :now)
  (xyz.tonk.notebook.insert/after ?b is: ?lo as: :entity pick: :last layer: :now)
  (xyz.tonk.notebook.insert/before ?b is: ?hi as: :entity pick: :last layer: :now))

;; The element's own edits. Each event fills a command, so each is one.
(concept! (block/edit ?e block: ?b source: ?s notebook: ?n)
  (xyz.tonk.block.edit/subject ?e is: ?b as: :entity pick: :last layer: :now)
  (xyz.tonk.block.edit/source ?e is: ?s as: :text pick: :last layer: :now)
  (xyz.tonk.block.edit/notebook ?e is: ?n as: :entity pick: :last layer: :now))

(concept! (block/place ?p block: ?b notebook: ?n key: ?key)
  (xyz.tonk.block.place/subject ?p is: ?b as: :entity pick: :last layer: :now)
  (xyz.tonk.block.place/notebook ?p is: ?n as: :entity pick: :last layer: :now)
  (xyz.tonk.block.place/key ?p is: ?key as: :text pick: :last layer: :now))

(concept! (block/move ?m block: ?b to: ?key)
  (xyz.tonk.block.move/subject ?m is: ?b as: :entity pick: :last layer: :now)
  (xyz.tonk.block.move/to ?m is: ?key as: :text pick: :last layer: :now))

(concept! (block/remove ?r block: ?b)
  (xyz.tonk.block.remove/subject ?r is: ?b as: :entity pick: :last layer: :now))

;; Its own attribute concept, so the palette rule below can conclude it.
(concept! (retitle/notebook ?of is: ?is)
  (xyz.tonk.notebook.retitle/subject ?of
      is: ?is
      as: :entity
      pick: :last
      layer: :now))

(concept! (notebook/retitle ?r notebook: ?n title: ?title)
  (retitle/notebook ?r is: ?n)
  (xyz.tonk.notebook.retitle/title ?r is: ?title as: :text pick: :last layer: :now))

;;; Positions

;; A block in the sequence is at its entry's key.
(rule! (assert (block/at ?b is: ?key))
  (when (notebook/entry ?n is: {?key ?b})))

;; The edge is at the empty position, which dialog/position reads as an open
;; bound. It anchors both ends, so one rule positions every inserted block.
(block/at! tonk:notebook/edge is: "")

;; An inserted block sits between the run's anchor and its successor, whose
;; position is stored, derived the same way, or the edge's. The recursion
;; ends at the edge.
(rule! (assert (block/at ?b is: ?at))
  (when (block/insert ?b after: ?lo before: ?hi)
        (block/at ?lo is: ?lo-at)
        (block/at ?hi is: ?hi-at)
        (dialog/position ?b after: ?lo-at before: ?hi-at is: ?at)))

;;; Effects

;; An insert creates its block and enters it at its derived position.
(rule! (assert! (notebook/block ?b notebook: ?n source: ?s))
  (when (block/insert ?b notebook: ?n source: ?s)))

(rule! (assert! (notebook/entry ?n is: {?at ?b}))
  (when (block/insert ?b notebook: ?n)
        (block/at ?b is: ?at)))

(rule! (assert! (notebook/block ?b notebook: ?n source: ?s))
  (when (block/edit block: ?b source: ?s notebook: ?n)))

(rule! (assert! (notebook/entry ?n is: {?key ?b}))
  (when (block/place block: ?b notebook: ?n key: ?key)))

;; A move enters the block under its new key and drops every other key it held.
(rule! (assert! (notebook/entry ?n is: {?key ?b}))
  (when (block/move block: ?b to: ?key)
        (block/notebook ?b is: ?n)))

(rule! (retract! (notebook/entry ?n is: {?old ?b}))
  (when (block/move block: ?b to: ?key)
        (notebook/entry ?n is: {?old ?b})
        (!= ?old ?key)))

;; A remove drops the block's entries and the block itself.
(rule! (retract! (notebook/entry ?n is: {?key ?b}))
  (when (block/remove block: ?b)
        (notebook/entry ?n is: {?key ?b})))

(rule! (retract! (notebook/block ?b))
  (when (block/remove block: ?b)))

(rule! (assert! (notebook/title ?n is: ?title))
  (when (notebook/retitle notebook: ?n title: ?title)))

;; Said from the palette on a notebook's page, a retitle renames that notebook.
(rule! (assert (retitle/notebook ?i is: ?n))
  (when (intent ?i command: notebook/retitle expression: ?e)
        (intent/expression ?e site: ?site)
        (site ?site concept: notebook/route)
        (notebook/route ?site entity: ?n)))

;;; Events: how <tonk-notebook>'s CustomEvents fill the commands. A `where`
;;; key names the command parameter the path fills.

(event! on/notebook-retitle
  type: "titlechange"
  where: {:notebook ".detail.notebook"
          :title ".detail.title"})

(event! on/block-edit
  type: "blockedit"
  where: {:block ".currentTarget.dataset.subject"
          :source ".detail.source"
          :notebook ".detail.notebook"})

(event! on/block-place
  type: "place"
  where: {:block ".currentTarget.dataset.subject"
          :notebook ".detail.notebook"
          :key ".detail.key"})

(event! on/block-remove
  type: "remove"
  where: {:block ".currentTarget.dataset.subject"})

;;; Views

;; One notebook. The element reads two hidden row sets: the notebook's
;; sequence, one item per entry in position order, and the block rows.
(view! notebook
  show: {:ui """
    <tonk-notebook
      data-subject={this}
      data-notebook={this}
      title={title}
      on:block-edit=block/edit
      on:block-place=block/place
      on:block-remove=block/remove
      on:notebook-retitle=notebook/retitle
      style="display: block;"
    >
      <div class="notebook-data" hidden>
        <div class="notebook-order">
          <span class="notebook-order__item" data-block={entry} data-key={entry/key}></span>
        </div>
        <tonk-display model=notebook/block></tonk-display>
      </div>
    </tonk-notebook>
    """})

;; Every notebook: the rows the heading switcher matches against, and a
;; working list of links when its script never loads.
(view! notebook
  show: {:directory """
    <div class="notebook-index">
      <a class="notebook-index__item" href="/notebook/{this}">
        <span class="notebook-index__title">{title}</span>
      </a>
    </div>
    """})

;; The block rows the element reads: one per block, its source on the row.
(view! notebook/block
  show: {:directory """
    <div class="notebook-block-rows">
      <div class="notebook-block-row"
        subject={this}
        data-block={this}
        data-notebook={notebook}
        data-source={source}></div>
    </div>
    """})

;;; Routes

;; The index, `/space/{repo}/notebook`: a draft notebook whose heading is the
;; switcher.
(concept! (notebook/index-route ?site repo: ?repo branch: ?branch)
  (xyz.tonk.site/repo ?site is: ?repo as: :text pick: :last)
  (xyz.tonk.site/branch ?site is: ?branch as: :text pick: :last))

(view! notebook/index-route
  show: {:ui """
    <tonk-notebook-index class="notebook-index">
      <tonk-notebook
        draft
        with="{branch}@{repo}"
        on:block-edit=block/edit
        on:block-place=block/place
        on:block-remove=block/remove
        on:notebook-retitle=notebook/retitle
        style="display: block;"
      ></tonk-notebook>
      <tonk-display with="{branch}@{repo}" model=notebook></tonk-display>
    </tonk-notebook-index>
    """})

;; One notebook, `/space/{repo}/notebook/{entity}`.
(concept! (notebook/route ?site entity: ?n repo: ?repo branch: ?branch)
  (xyz.tonk.site/entity ?site is: ?n as: :entity pick: :last)
  (xyz.tonk.site/repo ?site is: ?repo as: :text pick: :last)
  (xyz.tonk.site/branch ?site is: ?branch as: :text pick: :last))

(view! notebook/route
  show: {:ui """
    <div class="notebook-space">
      <tonk-display with="{branch}@{repo}" entity={entity} model=notebook></tonk-display>
    </div>
    """})

;; Each route's entity is hashed from its path and concept (see core.clj).
(route! path: "/notebook" concept: notebook/index-route)
(route! path: "/notebook/{*entity}" concept: notebook/route)

;;; Seed

(notebook! id:notebook/scratch title: "Scratch")

(notebook/entry! id:notebook/scratch is: {"N1" id:notebook/scratch/1})
(notebook/entry! id:notebook/scratch is: {"N5" id:notebook/scratch/2})
(notebook/entry! id:notebook/scratch is: {"N9" id:notebook/scratch/3})

(notebook/block! id:notebook/scratch/1
  notebook: id:notebook/scratch
  source: "# Scratch\n\nA `dialog-yaml` fence is a live cell: it evaluates against this branch and renders its results underneath.")

(notebook/block! id:notebook/scratch/2
  notebook: id:notebook/scratch
  source: "```dialog-yaml\nconcept:\n```")

(notebook/block! id:notebook/scratch/3
  notebook: id:notebook/scratch
  source: "Query cells run on their own, because a query has no effect. Cells that assert are recognized and held: they need a checkpoint to land in, which is not built yet.")
