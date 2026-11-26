; notebook: a <tonk-notebook> document whose ```dialog-yaml fences are live
; cells, ported from tonk's rust/tonk-core/assets/library/notebook.yaml at
; c006b50, form by form and in its order, in the notation as
; research/notation/design.md has it, with a struct's keys as keywords.
;
; A notebook is a prose document. Its fenced `dialog` blocks are evaluated
; against the branch and their results rendered beneath them, which is the
; element's business, not the model's. Blocks are the stored unit: the
; document the editor shows is the blocks joined with a blank line, and an
; edit writes back only the blocks that changed. The notebook's blocks are a
; sequence, one fact per block under `xyz.tonk.notebook/<position>`.
;
; What this document reads and does not declare comes from core.yaml, which
; is installed before it: `route`, `site`, `intent`, `intent/expression`,
; the element's attributes and the name a tag resolves through.
;
; A line starting `; GAP:` says what the design has no spelling for, and the
; YAML lines that need it. The form under it is the nearest the design can
; say.

; The notebook's title.
; (attribute! xyz.tonk.notebook/title, as text: no form. An attribute needs
; no declaration; its type is on each line that reads it, below.)

; A block's markdown source.
; (attribute! xyz.tonk.notebook.block/source, as text: no form, as above.)

; A notebook: prose with live dialog query cells. Its content is the block
; sequence, every block keyed by a fractional position, in document order.
(concept (notebook ?n title: ?title block: {?k ?b})
  ; The notebook's title.
  (xyz.tonk.notebook/title of: ?n is: ?title as: :text cardinality: :one)
  ; The notebook's blocks, in document order. `pick: :all` per entry in the
  ; YAML (line 55), written so the renderer iterates the entries.
  (xyz.tonk.notebook/?k of: ?n is: ?b as: :entity cardinality: :many by: :position))

; The title alone, so a create or a rename writes one field rather than the
; notebook's every field. Asserting `notebook` would need a block too, and a
; new notebook has none.

; A notebook's title, on its own.
(concept (notebook/named ?n title: ?title)
  ; The notebook's title.
  (xyz.tonk.notebook/title of: ?n is: ?title as: :text cardinality: :one))

; The sequence alone, so a placement writes one entry.

; A notebook's block sequence, as standalone entries.
(concept (notebook/blocks ?n block: {?k ?b})
  ; The notebook's blocks, in document order.
  (xyz.tonk.notebook/?k of: ?n is: ?b as: :entity cardinality: :one by: :position))

; Where a block sits, whatever the source of that position: a block already
; in the sequence, a block whose position this commit derives, and the
; document edge. The derivation reads its neighbours through it, so an
; inserted block can take its place from a neighbour just inserted.

; A block's position, derived or stored.
(concept (block/position ?b at: ?at)
  ; The position itself.
  (xyz.tonk.notebook.block/at of: ?b is: ?at as: :text cardinality: :one))

; An entry of a block that is not the block's first: some entry of the same
; block sorts before it. A sequence written before the placement rules can
; hold a block twice, and the page shows it at its first entry, so the
; position rules must derive from that one. A concept of its own rather
; than a `min` on `block/position`, because the position rules recurse
; through `block/position` and a fold inside that recursion is
; unstratified.

; A position a block holds besides its first.
(concept (block/stray-entry ?b at: ?at)
  ; The later position.
  (xyz.tonk.notebook.block/stray-at of: ?b is: ?at as: :text cardinality: :many))

; A block's entry is stray when another of its entries sorts first.
; GAP: a comparison. The design lists "less than, greater than" among
; constraints and gives no spelling; this is the build's, `(< ?p : 10)`.
; YAML 129-132.
(rule (block/stray-entry ?b at: ?at)
  (notebook/blocks ?n block: {?first ?b})
  (notebook/blocks ?n block: {?at ?b})
  (< ?first : ?at))

; One block of a notebook. It points back at its notebook so the block rows
; the element reads can be scoped, and so a block written before its
; placement lands still belongs somewhere.

; One markdown block of a notebook.
(concept (notebook/block ?b notebook: ?n source: ?s)
  ; The notebook this block belongs to.
  (xyz.tonk.notebook.block/notebook of: ?b is: ?n as: :entity cardinality: :one)
  ; The block's markdown source.
  (xyz.tonk.notebook.block/source of: ?b is: ?s as: :text cardinality: :one))

; A block and the block that follows it, or `case:none` at the document
; tail. A block's position derives from its successor's and the tail anchors
; the chain at the document edge, so a run inserted in one commit resolves
; in one pass. Its own concept: a block whose successor is retracted is
; still a block.

; A block and the block that follows it.
(concept (block/chain ?b next: ?x)
  ; The following block, or `case:none` at the tail.
  (xyz.tonk.notebook.block/next of: ?b is: ?x as: :entity cardinality: :one))

; Not the space shell. This library once re-pointed `id:tonk/space` at a
; notebook model, so installing it replaced the space home. A library that
; adds a feature reaches it through its own routes, below.

; Saving a notebook as a Markdown file. Cmd+S on macOS, Ctrl+S elsewhere,
; saves what the editor shows. The element wraps <tonk-notebook>, so a press
; elsewhere never reaches it. It listens in the bubble phase and steps aside
; for a press something inside already handled. The text is the
; <tonk-prose> editor's live `value`, read at the press. The file name is the
; leading heading, made safe for common file systems. It writes through
; `showSaveFilePicker` where the realm has it, and through an
; `<a download>` over a Blob URL otherwise; a cancelled picker downloads
; nothing.

; Saves the notebook it wraps as a Markdown file on Cmd+S / Ctrl+S.
; GAP: naming an entity. The YAML anchors the element as `&notebook-save`
; (line 211): its entity is derived from its body and `id:notebook-save`
; names it, which is how the tag <notebook-save> finds it. The design
; leaves how an entity is named open, so the entity here is a constant and
; the name is written as the attribute tonk reads, `db.name/referent`.
; GAP: an element is four collections keyed `by: :name` over one entity,
; and a concept holds one collection, so core's `element` cannot be asserted
; through; each entry is written by its attribute. YAML 211-301.
; GAP: text spanning lines and holding `"` and `\`. Every method is a
; JavaScript text over several lines; the escapes are Clojure's, which the
; design does not adopt. YAML 214-301.
(assert (db.name/referent of: id:notebook-save is: tonk:element/notebook-save as: :entity cardinality: :one))
(assert (xyz.tonk.element/description of: tonk:element/notebook-save
          is: "Saves the notebook it wraps as a Markdown file on Cmd+S / Ctrl+S."
          as: :text cardinality: :one))
(assert (xyz.tonk.element.method/connected of: tonk:element/notebook-save
          is: "(self) => {
  self.__keydown ??= (event) => self.keydown(event);
  self.addEventListener('keydown', self.__keydown);
}"
          as: :text cardinality: :one by: :name))
(assert (xyz.tonk.element.method/disconnected of: tonk:element/notebook-save
          is: "(self) => {
  if (self.__keydown) self.removeEventListener('keydown', self.__keydown);
}"
          as: :text cardinality: :one by: :name))
(assert (xyz.tonk.element.method/released of: tonk:element/notebook-save
          is: "(self) => {
  if (self.__keydown) self.removeEventListener('keydown', self.__keydown);
  self.__keydown = null;
}"
          as: :text cardinality: :one by: :name))
(assert (xyz.tonk.element.method/keydown of: tonk:element/notebook-save
          is: "(self, event) => {
  if (event.defaultPrevented || event.isComposing) return;
  if (!self.isSaveChord(event)) return;
  event.preventDefault();
  self.save();
}"
          as: :text cardinality: :one by: :name))
(assert (xyz.tonk.element.method/is-save-chord of: tonk:element/notebook-save
          is: "(self, event) => {
  if ((event.key || '').toLowerCase() !== 's') return false;
  if (event.altKey || event.shiftKey) return false;
  const platform = navigator.userAgentData?.platform || navigator.platform || '';
  const mac = /mac|iphone|ipad|ipod/i.test(platform);
  return mac ? event.metaKey && !event.ctrlKey : event.ctrlKey && !event.metaKey;
}"
          as: :text cardinality: :one by: :name))
(assert (xyz.tonk.element.method/markdown of: tonk:element/notebook-save
          is: "(self) => {
  const prose = self.querySelector('tonk-notebook tonk-prose');
  const value = prose?.value;
  return typeof value === 'string' ? value : '';
}"
          as: :text cardinality: :one by: :name))
(assert (xyz.tonk.element.method/heading of: tonk:element/notebook-save
          is: "(self, markdown) => {
  const first = String(markdown ?? '').trimStart().split('\\n')[0] ?? '';
  const line = first.replace(/^ +/, '');
  const hashes = line.match(/^#*/)[0].length;
  if (hashes < 1 || hashes > 6) return '';
  return line.slice(hashes).trim().replace(/#+$/, '').trim();
}"
          as: :text cardinality: :one by: :name))
(assert (xyz.tonk.element.method/file-name of: tonk:element/notebook-save
          is: "(self, markdown) => {
  let name = self.heading(markdown)
    .replace(/[\\u0000-\\u001f\\u007f<>:\"/\\\\|?*]/g, ' ')
    .replace(/\\s+/g, ' ')
    .replace(/^[\\s.]+|[\\s.]+$/g, '');
  if (name.length > 120) name = name.slice(0, 120).replace(/[\\s.]+$/, '');
  if (/^(con|prn|aux|nul|com[0-9]|lpt[0-9])$/i.test(name)) name = `${name}_`;
  return `${name || 'notebook'}.md`;
}"
          as: :text cardinality: :one by: :name))
(assert (xyz.tonk.element.method/save of: tonk:element/notebook-save
          is: "async (self) => {
  const markdown = self.markdown();
  const name = self.fileName(markdown);
  const type = 'text/markdown';
  if (typeof window.showSaveFilePicker === 'function') {
    try {
      const handle = await window.showSaveFilePicker({
        suggestedName: name,
        types: [{ description: 'Markdown', accept: { [type]: ['.md'] } }],
      });
      const writable = await handle.createWritable();
      await writable.write(new Blob([markdown], { type }));
      await writable.close();
      self.dispatchEvent(new CustomEvent('notebook-saved',
        { bubbles: true, detail: { name: handle.name, via: 'picker' } }));
      return;
    } catch (error) {
      if (error?.name === 'AbortError') return;
    }
  }
  // The link is never attached. A guest catches every click on an
  // `<a href>` at its document to relay navigation to the host, and a
  // `blob:` URL reads to it as an external link to open, so a link in
  // the document would never download. Clicked detached, the click
  // reaches no document and the browser downloads it.
  const url = URL.createObjectURL(new Blob([markdown], { type: `${type};charset=utf-8` }));
  const link = document.createElement('a');
  link.href = url;
  link.download = name;
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
  self.dispatchEvent(new CustomEvent('notebook-saved',
    { bubbles: true, detail: { name, via: 'download' } }));
}"
          as: :text cardinality: :one by: :name))

; One notebook. The element reads two hidden row sets: the notebook's own
; sequence, one `.notebook-order__item` per entry in position order carrying
; the entry's key, and the block rows carrying each block's source. Wrapped
; in <notebook-save> so Cmd/Ctrl+S saves it.
; Its view, `(view notebook :ui …)` as it was, is a query in notebook-views.clj.

; Every notebook: the directory view, what core's `/{*model}` route renders
; at `/space/{repo}/notebook`, one link per notebook. These rows are also the
; heading switcher's candidates, so an index whose script never loads is
; still a working set of links.
; GAP: a view with no entity. A `directory` view renders every instance,
; its markup outside the per-entity element once; the design binds the
; view's entity when it renders. YAML 343-356.
; Its view, `(view notebook :directory …)` as it was, is a query in notebook-views.clj.

; The block rows the element reads: one per block, its source on the row.
; GAP: a view with no entity, as above. YAML 359-369.
; Its view, `(view notebook/block :directory …)` as it was, is a query in notebook-views.clj.

; The index, `/space/{repo}/notebook`, with no entity: its own route and its
; own concept, not the entity route with an optional field. A literal path
; wins over `/{*model}` under the router's specificity rules.
; The entity is derived from the route's fields, as the YAML derives a
; claim's entity from its body, though not by the same hash.
; GAP: a concept named by an entity. The route names its concept by the
; entity the YAML pins it to, `tonk:notebook/index-route`; the design gives
; a concept the identity of its attributes and no way to pin one. YAML 392.
(assert (route ?r path: "/notebook" concept: tonk:notebook/index-route)
  (id ?r path: "/notebook" concept: tonk:notebook/index-route))

; Both notebook route models are pinned, as every route model should be. A
; concept's identity comes from its fields alone, and a route model's fields
; are the few site fields every page reads, so an unpinned one would be the
; very concept a space's own `{repo, branch}` page declares, share its `ui`
; view, and one page would replace the other.

; The notebook index (`/space/{repo}/notebook`).
; GAP: a concept pinned to an entity, `this: tonk:notebook/index-route`.
; Without it this concept is the same set of attributes as every other route
; concept over `repo` and `branch`, and its views collide with theirs. YAML 395.
(concept (notebook/index-route ?site repo: ?repo branch: ?branch)
  ; The space repository, picked off the site entity.
  (xyz.tonk.site/repo of: ?site is: ?repo as: :text cardinality: :one)
  ; The space branch, picked off the site entity.
  (xyz.tonk.site/branch of: ?site is: ?branch as: :text cardinality: :one))

; The index is a notebook, a draft one: the same <tonk-notebook> opened on
; nothing. Its heading is the switcher, and the body below it is the
; notebook's real body, so nothing changes shape when the draft becomes a
; notebook. The nested display renders the notebook rows the switcher
; matches against.
; Its view, `(view notebook/index-route :ui …)` as it was, is a query in notebook-views.clj.

; One notebook, `/space/{repo}/notebook/{entity}`.
; GAP: a concept named by an entity, `tonk:notebook/route`, as above. YAML 439.
(assert (route ?r path: "/notebook/{*entity}" concept: tonk:notebook/route)
  (id ?r path: "/notebook/{*entity}" concept: tonk:notebook/route))

; A single notebook page (`/space/{repo}/notebook/{entity}`).
; GAP: a concept pinned to an entity, `this: tonk:notebook/route`. YAML 442.
(concept (notebook/route ?site entity: ?entity repo: ?repo branch: ?branch)
  ; The notebook entity from the path, picked off the site.
  (xyz.tonk.site/entity of: ?site is: ?entity as: :entity cardinality: :one)
  ; The space repository, picked off the site entity.
  (xyz.tonk.site/repo of: ?site is: ?repo as: :text cardinality: :one)
  ; The space branch, picked off the site entity.
  (xyz.tonk.site/branch of: ?site is: ?branch as: :text cardinality: :one))

; Its view, `(view notebook/route :ui …)` as it was, is a query in notebook-views.clj.

; Insert one block. The element names no entity: a block's identity derives
; from this command's body. It used to be minted from the document's block
; count, a position wearing an identity's clothes. `next` is the block this
; one precedes, by entity, or `case:none` at the tail: positions derive by
; recursion down the chain from the tail, the end with a fixed anchor. The
; command's own attributes, apart from `notebook/block`'s: a command that
; wrote the block's attributes would be the block.

; Insert one block into a notebook, before another.
(concept (block/insert notebook: ?n source: ?s next: ?x prev: ?p)
  ; The notebook the block belongs to.
  (xyz.tonk.notebook.insert/notebook of: ?c is: ?n as: :entity in: :control cardinality: :one)
  ; The block's markdown source.
  (xyz.tonk.notebook.insert/source of: ?c is: ?s as: :text in: :control cardinality: :one)
  ; The block this one precedes, or `case:none` at the tail.
  (xyz.tonk.notebook.insert/next of: ?c is: ?x as: :entity in: :control cardinality: :one)
  ; The existing block this run of inserts follows, or `tonk:notebook/edge`
  ; at the document head. Only the chain's last block consults it; the
  ; others take their bound from their successor.
  (xyz.tonk.notebook.insert/prev of: ?c is: ?p as: :entity in: :control cardinality: :one))

; Create a notebook from the index's heading switcher: the switcher's create
; row fires it, with only a title. (No form follows in the YAML: the
; command it describes is not in this file.)

; Events: how <tonk-notebook>'s CustomEvents fill the commands. Each is
; inline in the views that bind it, as `:on/<type>` with paths, so the four
; `event!` forms have no form of their own here. Detail keys are spelled
; the way the element spells them.

; on/notebook-retitle: `titlechange`, subject from `.detail.notebook`, title
; from `.detail.title`. Inline in the two `:ui` views above.

; on/block-edit: `blockedit`, subject from `.currentTarget.dataset.subject`,
; source from `.detail.source`, notebook from `.detail.notebook`. Inline above.

; on/block-place: `place`, subject from `.currentTarget.dataset.subject`,
; notebook from `.detail.notebook`, key from `.detail.key`. Inline above.

; on/block-remove: `remove`, subject from `.currentTarget.dataset.subject`,
; notebook from `.detail.notebook`, key from `.detail.removed`. Inline above.

; Rename a notebook from its document's own heading. The title is the first
; heading of the document: the element watches it and emits `titlechange`
; when it changes. The subject is a named attribute rather than an inline
; field so the palette's rule below can conclude it.

; The notebook entity.
; (attribute! xyz.tonk.notebook.retitle/subject, as entity: no form; it is
; declared on its line in `notebook/retitle` below.)
; GAP: metadata on an attribute, `role: :object`, which the palette reads to
; fill the command. YAML 571.

; Set a notebook's title from its leading heading.
; GAP: metadata on a command, `action: ["rename", "retitle"]`, the names the
; palette answers to, and `role: :goal` on its title. YAML 575, 582.
(concept (notebook/retitle subject: ?n title: ?title)
  ; The notebook entity.
  (xyz.tonk.notebook.retitle/subject of: ?c is: ?n as: :entity in: :control cardinality: :one)
  ; The heading text, without its `#` markers.
  (xyz.tonk.notebook.retitle/title of: ?c is: ?title as: :text in: :control cardinality: :one))

; Said from the command palette on a notebook's page, "rename to Plans"
; renames that notebook: the notebook the page shows is derived onto the
; retitle's intent as its subject.

; The notebook a page shows is the one a retitle from its palette renames.
; GAP: a concept as a value, `command: notebook/retitle`: the intent names the
; command it would run by the command concept's entity, and the design names
; no entity for a concept. YAML 595.
; GAP: a concept named by an entity, `concept: tonk:notebook/route`. YAML 604.
(rule (xyz.tonk.notebook.retitle/subject ?i ?n)
  (intent ?i command: notebook/retitle expression: ?e)
  (intent/expression ?e site: ?site ...)
  (site ?site concept: tonk:notebook/route ...)
  (notebook/route ?site entity: ?n ...))

; Write one notebook block's source.
(concept (block/edit subject: ?b source: ?s notebook: ?n)
  ; The block entity, stamped on the element's data-subject.
  (xyz.tonk.block.edit/subject of: ?c is: ?b as: :entity in: :control cardinality: :one)
  ; The block's new markdown source.
  (xyz.tonk.block.edit/source of: ?c is: ?s as: :text in: :control cardinality: :one)
  ; The notebook the block belongs to. Carried on every edit: a block written
  ; with only its source does not match `notebook/block`, so it renders no
  ; row. Re-asserting it on an existing block changes nothing.
  (xyz.tonk.block.edit/notebook of: ?c is: ?n as: :entity in: :control cardinality: :one))

; Placement is per block: put this block into its notebook's sequence under a
; position. The element derives the position between the block's neighbours,
; so a move re-keys only what moved.

; Place one block in its notebook's sequence.
(concept (block/place subject: ?b notebook: ?n key: ?k)
  ; The block entity.
  (xyz.tonk.block.place/subject of: ?c is: ?b as: :entity in: :control cardinality: :one)
  ; The notebook whose sequence the block joins.
  (xyz.tonk.block.place/notebook of: ?c is: ?n as: :entity in: :control cardinality: :one)
  ; The block's position in the sequence.
  (xyz.tonk.block.place/key of: ?c is: ?k as: :text in: :control cardinality: :one))

; A move re-keys a block that already has an entry: its entries go and one
; under `key` takes their place, in the same commit. `from` names the entry
; the element read; the rules retract every entry the block holds, so it
; stays only because the element writes it.

; Move one block to another position in its notebook's sequence.
(concept (block/move subject: ?b notebook: ?n from: ?from key: ?k)
  ; The block entity.
  (xyz.tonk.block.move/subject of: ?c is: ?b as: :entity in: :control cardinality: :one)
  ; The notebook whose sequence the block moves in.
  (xyz.tonk.block.move/notebook of: ?c is: ?n as: :entity in: :control cardinality: :one)
  ; The position the block held.
  (xyz.tonk.block.move/from of: ?c is: ?from as: :text in: :control cardinality: :one)
  ; The position the block moves to.
  (xyz.tonk.block.move/key of: ?c is: ?k as: :text in: :control cardinality: :one))

; Take one block out of its notebook's sequence.
(concept (block/remove subject: ?b notebook: ?n key: ?k)
  ; The block entity.
  (xyz.tonk.block.remove/subject of: ?c is: ?b as: :entity in: :control cardinality: :one)
  ; The notebook whose sequence the block leaves.
  (xyz.tonk.block.remove/notebook of: ?c is: ?n as: :entity in: :control cardinality: :one)
  ; The position the block held. Its own attribute: two commands of one
  ; shape would both decode from one event.
  (xyz.tonk.block.remove/key of: ?c is: ?k as: :text in: :control cardinality: :one))

; An inserted block exists.

; Persist an inserted block.
; GAP: a command's own entity. The YAML reads the insert's entity, `this:
; ?this`, and makes it the block's; the design derives a command's entity
; when it is asserted and gives a rule no way to read it. `of: ?b` on the
; command premise reads it here. That spelling is the managing agent's
; opinion, not the design's. YAML 704.
(rule (assert (notebook/block ?b notebook: ?n source: ?s))
  (block/insert of: ?b notebook: ?n source: ?s next: ?x prev: ?p))

; A block already in the sequence is at the position it is stored under, the
; first when it is stored under several (see `block/stray-entry`). This gives
; the derivation its neighbours. The YAML binds the position with `==`; a
; constraint binds nothing, so the key is the position here.

; A placed block's position.
(rule (block/position ?b at: ?k)
  (notebook/blocks ?n block: {?k ?b})
  (unless (block/stray-entry ?b at: ?k)))

; An inserted block's successor, so the position rules can recurse over it.
; `case:none` at the tail matches no block, which ends the recursion.

; Record an inserted block's successor.
; GAP: a command's own entity, read with `of:` as above, the managing
; agent's opinion and not the design's. YAML 738.
(rule (block/chain ?b next: ?x)
  (block/insert of: ?b notebook: ?n source: ?s next: ?x prev: ?p))

; The document's edge has a position: the empty one, which `dialog/position`
; reads as an open bound. That keeps the tail rule to one rule.
(assert (block/position tonk:notebook/edge at: ""))

; The chain's tail anchors against the block it follows. That block's
; position is stored, so the anchor is a lookup, not a fold, which would sit
; in the recursion's cycle. An absent `prev` gives the start of the range,
; correct only when there is nothing to come after.

; Position the last new block after the block it follows.
; GAP: a command's own entity, read with `of:` as above, the managing
; agent's opinion and not the design's. YAML 773.
(rule (block/position ?b at: ?at)
  (block/chain ?b next: case:none)
  (block/insert of: ?b notebook: ?n source: ?s next: _ prev: ?p)
  (block/position ?p at: ?lo)
  (dialog/position ?b after: ?lo before: "" is: ?at))

; ...and every other block sits before its successor. The rule reads its own
; conclusion for the successor; it ends at `case:none`, which the rule above
; positions. `before:` the successor, since a block precedes it, and both
; bounds, so a run appended after a block lands after it.

; Position a block before its successor.
; GAP: a command's own entity, read with `of:` as above, the managing
; agent's opinion and not the design's. YAML 811.
(rule (block/position ?b at: ?at)
  (block/chain ?b next: ?x)
  (block/insert of: ?b notebook: ?n source: ?s next: _ prev: ?p)
  (block/position ?p at: ?lo)
  (block/position ?x at: ?hi)
  (dialog/position ?b after: ?lo before: ?hi is: ?at))

; The derived position becomes the block's entry in the sequence.

; Place an inserted block.
; GAP: a command's own entity, read with `of:` as above, the managing
; agent's opinion and not the design's. YAML 835.
(rule (assert (notebook/blocks ?n block: {?k ?b}))
  (block/insert of: ?b notebook: ?n source: ?s next: ?x prev: ?p)
  (block/position ?b at: ?k))

; Retitle a notebook from its heading.
(rule (assert (notebook/named ?n title: ?title))
  (notebook/retitle subject: ?n title: ?title))

; Persist a block edit onto its block.
(rule (assert (notebook/block ?b notebook: ?n source: ?s))
  (block/edit subject: ?b source: ?s notebook: ?n))

; The entry lands under `xyz.tonk.notebook/<key>`.

; Persist a placement into the notebook's sequence.
(rule (assert (notebook/blocks ?n block: {?k ?b}))
  (block/place subject: ?b notebook: ?n key: ?k))

; A placement leaves the block with one entry, the placed one. The element can
; read a block as unplaced while an entry exists, so the rule retracts every
; other entry the block has, read from the sequence. The entry under the
; placed key is left alone.

; A placed block leaves every other entry it held.
; GAP: a retract of one entry of a collection: the design's retract through
; a concept takes every field on the entity, and what it takes for a
; collection is open. YAML 885.
(rule (retract (notebook/blocks ?n block: {?old ?b}))
  (block/place subject: ?b notebook: ?n key: ?k)
  (notebook/blocks ?n block: {?old ?b})
  (!= ?old is: ?k))

; Enter a moved block under its new position.
(rule (assert (notebook/blocks ?n block: {?k ?b}))
  (block/move subject: ?b notebook: ?n key: ?k ...))

; Every entry the block held goes, not just `from`: a block entered twice is
; repaired by its next move.

; A moved block leaves every other entry it held.
; GAP: a retract of one entry of a collection, as above. YAML 916.
(rule (retract (notebook/blocks ?n block: {?old ?b}))
  (block/move subject: ?b notebook: ?n key: ?k ...)
  (notebook/blocks ?n block: {?old ?b})
  (!= ?old is: ?k))

; Every entry the block holds, read from the sequence rather than the key the
; element names: a stray entry would bring the block back.

; Retract a removed block's every entry from the notebook's sequence.
; GAP: a retract of one entry of a collection, as above. YAML 937.
(rule (retract (notebook/blocks ?n block: {?k ?b}))
  (block/remove subject: ?b notebook: ?n ...)
  (notebook/blocks ?n block: {?k ?b}))

; The block's own facts go too. A block with a source and no entry is what
; the element treats as a placement that never landed, so a deleted block
; would come back.

; Retract a removed block's own facts.
(rule (retract (notebook/block ?b notebook: ?n source: ?s))
  (block/remove subject: ?b notebook: ?n ...)
  (notebook/block ?b notebook: ?n source: ?s))

; Seed.

(assert (notebook id:notebook/scratch title: "Scratch" block: {"N1" id:notebook/scratch/1}))

; The YAML asserts `notebook` without its title; a write of some fields goes
; through a concept that holds only them.
(assert (notebook/blocks id:notebook/scratch block: {"N5" id:notebook/scratch/2}))

(assert (notebook/blocks id:notebook/scratch block: {"N9" id:notebook/scratch/3}))

; The seeded blocks are exactly what `split` produces from the projected
; document, or the first edit re-partitions the whole notebook: a heading is
; grouped with the paragraph it introduces.
; GAP: text spanning lines. YAML 990-993.
(assert (notebook/block id:notebook/scratch/1
          notebook: id:notebook/scratch
          source: "# Scratch

A `dialog-yaml` fence is a live cell: it evaluates against this branch and renders its results underneath."))

; GAP: text spanning lines. YAML 998-1001.
(assert (notebook/block id:notebook/scratch/2
          notebook: id:notebook/scratch
          source: "```dialog-yaml
concept:
```"))

(assert (notebook/block id:notebook/scratch/3
          notebook: id:notebook/scratch
          source: "Query cells run on their own, because a query has no effect. Cells that assert are recognized and held: they need a checkpoint to land in, which is not built yet."))
