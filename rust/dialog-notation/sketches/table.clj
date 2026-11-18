; table: a <tonk-table> spreadsheet over individuated claims, ported from
; tonk's rust/tonk-core/assets/library/table.yaml at bfb2d72, form by form
; and in its order, in the notation as research/notation/design.md has it,
; with a struct's keys as keywords. YAML 1-19.
;
; The workbook is not a blob: every sheet and every non-empty cell is its
; own claim instance, so cells are addressable, concurrent editors merge at
; cell grain, and rules can react to individual cells. Only the raw input
; is stored (`"=B2*C2"`, `"3.5"`); computed values are derived, and every
; replica's IronCalc engine recomputes them.
;
; Wiring follows the board pattern: directory views materialize each claim
; as a hidden data-row div inside the element's light DOM (claims in, via
; the element's MutationObserver); the element emits one typed CustomEvent
; per user mutation (claims out), each bound below to a command whose rule
; asserts or retracts the one claim. Facet concepts (`table/cell-content`,
; `table/sheet-name`) give rules a partial-upsert head, and clearing a cell
; retracts its content, the keystone fact whose absence unmatches the cell
; from the directory.
;
; The table reads nothing core declares: `dom.host/model`, in the directory
; view below, is a value the host element gives, not a concept. It is read
; after core, as tonk reads a component.
;
; A line starting `; GAP:` says what the design has no spelling for, and the
; YAML lines that need it. The form under it is the nearest the design can
; say. A spelling marked "the managing agent's opinion" is one the build
; settled and the design does not.

; Attributes. An attribute needs no declaration; its type is on each line
; that reads it, below, and its description on the line. So the thirteen
; `attribute!` forms have no form of their own here. YAML 21-91.

; The workbook a sheet belongs to.
; (attribute! xyz.tonk.table.sheet/table, as entity: no form. YAML 25.)

; The sheet's display name.
; (attribute! xyz.tonk.table.sheet/name, as text: no form. YAML 30.)

; Lexicographic key ordering sheets in the tab strip.
; (attribute! xyz.tonk.table.sheet/order, as text: no form. YAML 35.)

; The sheet a cell belongs to.
; (attribute! xyz.tonk.table.cell/sheet, as entity: no form. YAML 40.)

; The cell's A1-style address ("B2").
; (attribute! xyz.tonk.table.cell/at, as text: no form. YAML 45.)

; The cell's raw user input, a formula ("=B2*C2") or a literal ("3.5",
; "Widget"). Never the computed value: values are derived by the engine on
; every replica.
; (attribute! xyz.tonk.table.cell/content, as text: no form. YAML 50.)

; Optional style JSON (font/fill/format), absent when plain.
; (attribute! xyz.tonk.table.cell/style, as text: no form. YAML 58.)

; The sheet a sized column belongs to.
; (attribute! xyz.tonk.table.column/sheet, as entity: no form. YAML 63.)

; The column's letters ("B").
; (attribute! xyz.tonk.table.column/at, as text: no form. YAML 68.)

; The column's width in px, as decimal text.
; (attribute! xyz.tonk.table.column/width, as text: no form. YAML 73.)

; The sheet a sized row belongs to.
; (attribute! xyz.tonk.table.row/sheet, as entity: no form. YAML 78.)

; The row's 1-based number, as text ("2").
; (attribute! xyz.tonk.table.row/at, as text: no form. YAML 83.)

; The row's height in px, as decimal text.
; (attribute! xyz.tonk.table.row/height, as text: no form. YAML 88.)

; Concepts. Every line is `cardinality: :one`: the YAML's fields give no
; pick, and tonk reads such a field's newest claim.

; A spreadsheet workbook edited with <tonk-table>.
; GAP: a concept pinned to an entity, `this: tonk:table`. The design gives
; a concept the identity of its attributes. YAML 98.
(concept (table ?t name: ?name)
  ; The workbook's display name.
  (xyz.tonk.table/name of: ?t is: ?name as: :text cardinality: :one))

; One sheet of a table workbook.
; GAP: a concept pinned to an entity, `this: tonk:table/sheet`. YAML 107.
(concept (table/sheet ?s table: ?table name: ?name order: ?order)
  ; The workbook a sheet belongs to.
  (xyz.tonk.table.sheet/table of: ?s is: ?table as: :entity cardinality: :one)
  ; The sheet's display name.
  (xyz.tonk.table.sheet/name of: ?s is: ?name as: :text cardinality: :one)
  ; Lexicographic key ordering sheets in the tab strip.
  (xyz.tonk.table.sheet/order of: ?s is: ?order as: :text cardinality: :one))

; A sheet's name as a standalone fact, so a rename upserts one field.

; A sheet's name, as a standalone fact.
(concept (table/sheet-name ?s name: ?name)
  ; The sheet's display name.
  (xyz.tonk.table.sheet/name of: ?s is: ?name as: :text cardinality: :one))

; One non-empty cell of a sheet.
; GAP: a concept pinned to an entity, `this: tonk:table/cell`. YAML 121.
; GAP: an optional field, `maybe: style`: the cell's style, absent when
; plain. The design lists optional fields open, so the field is left out.
; YAML 127-128.
(concept (table/cell ?c sheet: ?sheet at: ?at content: ?content)
  ; The sheet a cell belongs to.
  (xyz.tonk.table.cell/sheet of: ?c is: ?sheet as: :entity cardinality: :one)
  ; The cell's A1-style address ("B2").
  (xyz.tonk.table.cell/at of: ?c is: ?at as: :text cardinality: :one)
  ; The cell's raw user input.
  (xyz.tonk.table.cell/content of: ?c is: ?content as: :text cardinality: :one))

; A cell's content as a standalone fact: the edit rule's upsert head, and
; the keystone the clear rule retracts. A cell without content no longer
; matches `table/cell`, so its row (and the cell) disappear.

; A cell's raw input, as a standalone fact.
(concept (table/cell-content ?c content: ?content)
  ; The cell's raw user input.
  (xyz.tonk.table.cell/content of: ?c is: ?content as: :text cardinality: :one))

; A cell's style as a standalone fact (formatting edits never contend with
; typing).

; A cell's style JSON, as a standalone fact.
(concept (table/cell-style ?c style: ?style)
  ; Optional style JSON (font/fill/format), absent when plain.
  (xyz.tonk.table.cell/style of: ?c is: ?style as: :text cardinality: :one))

; Sizing claims: only explicitly sized columns and rows have one. Every
; other line uses the engine default, so a fresh sheet mints zero sizing
; claims.

; An explicitly-sized column of a sheet.
; GAP: a concept pinned to an entity, `this: tonk:table/column`. YAML 149.
(concept (table/column ?k sheet: ?sheet at: ?at width: ?width)
  ; The sheet a sized column belongs to.
  (xyz.tonk.table.column/sheet of: ?k is: ?sheet as: :entity cardinality: :one)
  ; The column's letters ("B").
  (xyz.tonk.table.column/at of: ?k is: ?at as: :text cardinality: :one)
  ; The column's width in px, as decimal text.
  (xyz.tonk.table.column/width of: ?k is: ?width as: :text cardinality: :one))

; A column's width, as a standalone fact.
(concept (table/column-width ?k width: ?width)
  ; The column's width in px, as decimal text.
  (xyz.tonk.table.column/width of: ?k is: ?width as: :text cardinality: :one))

; An explicitly-sized row of a sheet.
; GAP: a concept pinned to an entity, `this: tonk:table/row`. YAML 162.
(concept (table/row ?r sheet: ?sheet at: ?at height: ?height)
  ; The sheet a sized row belongs to.
  (xyz.tonk.table.row/sheet of: ?r is: ?sheet as: :entity cardinality: :one)
  ; The row's 1-based number, as text ("2").
  (xyz.tonk.table.row/at of: ?r is: ?at as: :text cardinality: :one)
  ; The row's height in px, as decimal text.
  (xyz.tonk.table.row/height of: ?r is: ?height as: :text cardinality: :one))

; A row's height, as a standalone fact.
(concept (table/row-height ?r height: ?height)
  ; The row's height in px, as decimal text.
  (xyz.tonk.table.row/height of: ?r is: ?height as: :text cardinality: :one))

; Commands: domain-shaped, each in its own namespace so one can never read
; as another. `time` is a component-minted monotonic nonce that keeps
; otherwise-identical creates distinct. YAML 174-179.

; Events: how <tonk-table>'s CustomEvents fill the commands. Each is inline
; in the view that binds it, `table :ui` below, as `:on/<type>` with paths,
; so the nine `event!` forms have no form of their own here. Detail keys are
; spelled the way the element spells them. YAML 181-194.

; on/table-create-sheet: `createsheet`, table from `.detail.sheetTable`,
; name from `.detail.sheetName`, order from `.detail.sheetOrder`, time from
; `.detail.time`. Inline below. YAML 196.

; on/table-rename-sheet: `renamesheet`, sheet from `.detail.renameSheet`,
; name from `.detail.renameName`. Inline below. YAML 204.

; on/table-create-cell: `createcell`, sheet from `.detail.cellSheet`, at
; from `.detail.cellAt`, content from `.detail.cellContent`, time from
; `.detail.time`. Inline below. YAML 210.

; on/table-edit-cell: `editcell`, cell from `.detail.editCell`, content from
; `.detail.editContent`. Inline below. YAML 218.

; on/table-clear-cell: `clearcell`, cell from `.detail.clearCell`. Inline
; below. YAML 224.

; on/table-create-column: `createcolumn`, sheet from `.detail.columnSheet`,
; at from `.detail.columnAt`, width from `.detail.columnWidth`, time from
; `.detail.time`. Inline below. YAML 229.

; on/table-resize-column: `resizecolumn`, column from `.detail.resizeColumn`,
; width from `.detail.resizeWidth`. Inline below. YAML 237.

; on/table-create-row: `createrow`, sheet from `.detail.rowSheet`, at from
; `.detail.rowAt`, height from `.detail.rowHeight`, time from
; `.detail.time`. Inline below. YAML 243.

; on/table-resize-row: `resizerow`, row from `.detail.resizeRow`, height
; from `.detail.resizeHeight`. Inline below. YAML 251.

; Create a sheet in workbook `table`.
(concept (table/create-sheet table: ?table name: ?name order: ?order time: ?time)
  ; The workbook gaining the sheet.
  (xyz.tonk.table.create-sheet/table of: ?c is: ?table as: :entity in: :control)
  ; The new sheet's name.
  (xyz.tonk.table.create-sheet/name of: ?c is: ?name as: :text in: :control)
  ; Tab-strip order key, computed by the element.
  (xyz.tonk.table.create-sheet/order of: ?c is: ?order as: :text in: :control)
  ; A monotonic integer nonce from the element.
  (xyz.tonk.table.create-sheet/time of: ?c is: ?time as: :natural in: :control))

; Rename `sheet` to `name`.
(concept (table/rename-sheet sheet: ?sheet name: ?name)
  ; The sheet being renamed.
  (xyz.tonk.table.rename-sheet/sheet of: ?c is: ?sheet as: :entity in: :control)
  ; The new name.
  (xyz.tonk.table.rename-sheet/name of: ?c is: ?name as: :text in: :control))

; Set a previously-empty cell (mints its claim).
(concept (table/create-cell sheet: ?sheet at: ?at content: ?content time: ?time)
  ; The sheet the cell is on.
  (xyz.tonk.table.create-cell/sheet of: ?c is: ?sheet as: :entity in: :control)
  ; A1-style address.
  (xyz.tonk.table.create-cell/at of: ?c is: ?at as: :text in: :control)
  ; The raw input.
  (xyz.tonk.table.create-cell/content of: ?c is: ?content as: :text in: :control)
  ; A monotonic integer nonce from the element.
  (xyz.tonk.table.create-cell/time of: ?c is: ?time as: :natural in: :control))

; Replace `cell`'s raw input.
(concept (table/edit-cell cell: ?cell content: ?content)
  ; The edited cell.
  (xyz.tonk.table.edit-cell/cell of: ?c is: ?cell as: :entity in: :control)
  ; The new raw input.
  (xyz.tonk.table.edit-cell/content of: ?c is: ?content as: :text in: :control))

; Empty `cell` (retracts it from the sheet).
(concept (table/clear-cell cell: ?cell)
  ; The cleared cell.
  (xyz.tonk.table.clear-cell/cell of: ?c is: ?cell as: :entity in: :control))

; Size a previously-default column (mints its claim).
(concept (table/create-column sheet: ?sheet at: ?at width: ?width time: ?time)
  ; The sheet the column is on.
  (xyz.tonk.table.create-column/sheet of: ?c is: ?sheet as: :entity in: :control)
  ; Column letters.
  (xyz.tonk.table.create-column/at of: ?c is: ?at as: :text in: :control)
  ; Width px, as decimal text.
  (xyz.tonk.table.create-column/width of: ?c is: ?width as: :text in: :control)
  ; A monotonic integer nonce from the element.
  (xyz.tonk.table.create-column/time of: ?c is: ?time as: :natural in: :control))

; Change `column`'s width.
(concept (table/resize-column column: ?column width: ?width)
  ; The resized column.
  (xyz.tonk.table.resize-column/column of: ?c is: ?column as: :entity in: :control)
  ; The new width px, as decimal text.
  (xyz.tonk.table.resize-column/width of: ?c is: ?width as: :text in: :control))

; Size a previously-default row (mints its claim).
(concept (table/create-row sheet: ?sheet at: ?at height: ?height time: ?time)
  ; The sheet the row is on.
  (xyz.tonk.table.create-row/sheet of: ?c is: ?sheet as: :entity in: :control)
  ; The row number, as text.
  (xyz.tonk.table.create-row/at of: ?c is: ?at as: :text in: :control)
  ; Height px, as decimal text.
  (xyz.tonk.table.create-row/height of: ?c is: ?height as: :text in: :control)
  ; A monotonic integer nonce from the element.
  (xyz.tonk.table.create-row/time of: ?c is: ?time as: :natural in: :control))

; Change `row`'s height.
(concept (table/resize-row row: ?row height: ?height)
  ; The resized row.
  (xyz.tonk.table.resize-row/row of: ?c is: ?row as: :entity in: :control)
  ; The new height px, as decimal text.
  (xyz.tonk.table.resize-row/height of: ?c is: ?height as: :text in: :control))

; Rules.

; Sheet lifecycle. The created sheet's entity is the command entity (`?this`
; flows from the command match into the head).

; Creates a sheet.
; GAP: a command's own entity, read with `of:` on the command premise, as
; the YAML reads it with `this: ?this`. That spelling is the managing
; agent's opinion, not the design's. YAML 399-404.
(rule (assert (table/sheet ?this table: ?table name: ?name order: ?order))
  (table/create-sheet of: ?this table: ?table name: ?name order: ?order))

; Renames a sheet.
(rule (assert (table/sheet-name ?this name: ?name))
  (table/rename-sheet sheet: ?this name: ?name))

; Cell lifecycle. Created cells are the command entity; edits upsert the
; content facet in place.

; Sets a previously-empty cell.
; GAP: a command's own entity, read with `of:` as above, the managing
; agent's opinion and not the design's. YAML 415-420.
(rule (assert (table/cell ?this sheet: ?sheet at: ?at content: ?content))
  (table/create-cell of: ?this sheet: ?sheet at: ?at content: ?content))

; Replaces an edited cell's input.
(rule (assert (table/cell-content ?this content: ?content))
  (table/edit-cell cell: ?this content: ?content))

; Clearing retracts the content fact, the keystone whose absence unmatches
; the cell from `table/cell` (its style may linger on the entity, harmlessly,
; until the address is written again).

; Empties a cleared cell.
(rule (retract (table/cell-content ?this content: ?content))
  (table/clear-cell cell: ?this)
  (table/cell-content ?this content: ?content))

; Sizing lifecycle, same shape as cells: creates flow the command entity
; into the head, resizes upsert the single-fact facet.

; Sizes a previously-default column.
; GAP: a command's own entity, read with `of:` as above, the managing
; agent's opinion and not the design's. YAML 443-448.
(rule (assert (table/column ?this sheet: ?sheet at: ?at width: ?width))
  (table/create-column of: ?this sheet: ?sheet at: ?at width: ?width))

; Resizes a column.
(rule (assert (table/column-width ?this width: ?width))
  (table/resize-column column: ?this width: ?width))

; Sizes a previously-default row.
; GAP: a command's own entity, read with `of:` as above, the managing
; agent's opinion and not the design's. YAML 457-462.
(rule (assert (table/row ?this sheet: ?sheet at: ?at height: ?height))
  (table/create-row of: ?this sheet: ?sheet at: ?at height: ?height))

; Resizes a row.
(rule (assert (table/row-height ?this height: ?height))
  (table/resize-row row: ?this height: ?height))

; Data-row views (invisible; feed the element). Each directory view renders
; one bare data div per claim. The element reads and observes these rows
; inside its light DOM; the `hidden` wrapper in the main view keeps them
; unpainted. `subject` carries each claim's entity id. YAML 471-478.

; GAP: a view with no entity. A `directory` view renders every instance,
; its markup outside the per-entity element once; the design binds the
; view's entity when it renders. YAML 480-486.
; GAP: a view of a concept named by an entity, `this: tonk:table/sheet`;
; the design's view names its concept. YAML 481.
(view table/sheet :directory
  [:div {:class "table-sheet-rows"}
    [:div {:class "table-sheet-row"
           :subject ?s
           :data-table ?table
           :data-name ?name
           :data-order ?order}]]
  (table/sheet ?s table: ?table name: ?name order: ?order))

; GAP: a view with no entity, as above. YAML 488-494.
; GAP: an optional field, `data-style={style}`: the cell's style is
; `maybe:`, which the design leaves open, so `table/cell` has no style and
; the attribute is left out. YAML 493.
(view table/cell :directory
  [:div {:class "table-cell-rows"}
    [:div {:class "table-cell-row"
           :subject ?c
           :data-sheet ?sheet
           :data-at ?at
           :data-content ?content}]]
  (table/cell ?c sheet: ?sheet at: ?at content: ?content))

; GAP: a view with no entity, as above. YAML 496-502.
(view table/column :directory
  [:div {:class "table-column-rows"}
    [:div {:class "table-column-row"
           :subject ?k
           :data-sheet ?sheet
           :data-at ?at
           :data-width ?width}]]
  (table/column ?k sheet: ?sheet at: ?at width: ?width))

; GAP: a view with no entity, as above. YAML 504-510.
(view table/row :directory
  [:div {:class "table-rowsize-rows"}
    [:div {:class "table-rowsize-row"
           :subject ?r
           :data-sheet ?sheet
           :data-at ?at
           :data-height ?height}]]
  (table/row ?r sheet: ?sheet at: ?at height: ?height))

; Views: the live grid. `subject` selects the element's claims mode; the
; nested hidden displays materialize this branch's sheet and cell claims
; where the element's observer can see them; each `:on/<type>` binds one
; element event to its command. YAML 512-517.

; GAP: a valueless HTML attribute, `hidden`. `true` for an attribute present
; with no value is the managing agent's opinion, not the design's. YAML 534.
; GAP: a nested view with no entity, `<tonk-display model=tonk:table/sheet>`,
; four times. The design's reference names an entity, `{:view :row :of ?t}`;
; `:model` naming the concept whose view it is, by its name and not its
; pinned entity, is the managing agent's opinion. YAML 535-538.
(view table :ui
  [:tonk-table {:subject ?t
                :on/createsheet (table/create-sheet table: .detail.sheetTable
                                                    name: .detail.sheetName
                                                    order: .detail.sheetOrder
                                                    time: .detail.time)
                :on/renamesheet (table/rename-sheet sheet: .detail.renameSheet
                                                    name: .detail.renameName)
                :on/createcell (table/create-cell sheet: .detail.cellSheet
                                                  at: .detail.cellAt
                                                  content: .detail.cellContent
                                                  time: .detail.time)
                :on/editcell (table/edit-cell cell: .detail.editCell
                                              content: .detail.editContent)
                :on/clearcell (table/clear-cell cell: .detail.clearCell)
                :on/createcolumn (table/create-column sheet: .detail.columnSheet
                                                      at: .detail.columnAt
                                                      width: .detail.columnWidth
                                                      time: .detail.time)
                :on/resizecolumn (table/resize-column column: .detail.resizeColumn
                                                      width: .detail.resizeWidth)
                :on/createrow (table/create-row sheet: .detail.rowSheet
                                                at: .detail.rowAt
                                                height: .detail.rowHeight
                                                time: .detail.time)
                :on/resizerow (table/resize-row row: .detail.resizeRow
                                                height: .detail.resizeHeight)}
    [:div {:hidden true}
      {:view :directory :model table/sheet}
      {:view :directory :model table/cell}
      {:view :directory :model table/column}
      {:view :directory :model table/row}]]
  (table ?t name: ?name))

; A directory view so `tonk render table` (list of workbooks) resolves.
; GAP: a view with no entity, as above. YAML 543-549.
; GAP: a value the host element gives, `model={dom.host/model}`: the nested
; display renders the entity under the model its host was rendering, which
; is not among the view's conditions. The design's template holds only the
; conditions' variables and, in an event binding, paths of the event. The
; nearest is the model written out, which is this view's own. Here it is
; the same model: a `<tonk-display>` with no entity renders its model's own
; `directory` (core.yaml 32-36), so the host of this view renders `table`.
; Where the host's model is not known, in core's view of any model
; (core.yaml 48, 966), the model cannot be written out. YAML 548.
; GAP: a nested view with no view named, `<tonk-display entity={this}>`:
; the display picks the entity's view. The design's reference names one,
; `{:view :row :of ?t}`; `:ui` is the nearest. YAML 548.
(view table :directory
  [:div {:class "table-directory"}
    {:view :ui :of ?t :model table}]
  (table ?t name: ?name))

; Starter content: one workbook, one sheet, and the demo cells, each cell
; its own claim (`tonk query table/cell` lists them; agents assert more).
; Stable `id:` URIs keep re-seeding idempotent. Only raw input is seeded;
; D-column values compute on load. YAML 551-556.

(assert (table id:table/book name: "Demo"))

(assert (table/sheet id:table/book/sheet1 table: id:table/book name: "Sheet1" order: "m"))

; The YAML's `---` between the cells separates documents of one file; the
; notation's document is a set of forms, so it has no form here.

(assert (table/cell id:table/book/s1!A1 sheet: id:table/book/sheet1 at: "A1" content: "Item"))

(assert (table/cell id:table/book/s1!B1 sheet: id:table/book/sheet1 at: "B1" content: "Qty"))

(assert (table/cell id:table/book/s1!C1 sheet: id:table/book/sheet1 at: "C1" content: "Price"))

(assert (table/cell id:table/book/s1!D1 sheet: id:table/book/sheet1 at: "D1" content: "Total"))

(assert (table/cell id:table/book/s1!A2 sheet: id:table/book/sheet1 at: "A2" content: "Widget"))

(assert (table/cell id:table/book/s1!B2 sheet: id:table/book/sheet1 at: "B2" content: "2"))

(assert (table/cell id:table/book/s1!C2 sheet: id:table/book/sheet1 at: "C2" content: "3.5"))

(assert (table/cell id:table/book/s1!D2 sheet: id:table/book/sheet1 at: "D2" content: "=B2*C2"))

(assert (table/cell id:table/book/s1!A3 sheet: id:table/book/sheet1 at: "A3" content: "Gadget"))

(assert (table/cell id:table/book/s1!B3 sheet: id:table/book/sheet1 at: "B3" content: "1"))

(assert (table/cell id:table/book/s1!C3 sheet: id:table/book/sheet1 at: "C3" content: "12"))

(assert (table/cell id:table/book/s1!D3 sheet: id:table/book/sheet1 at: "D3" content: "=B3*C3"))

(assert (table/cell id:table/book/s1!C4 sheet: id:table/book/sheet1 at: "C4" content: "Sum"))

(assert (table/cell id:table/book/s1!D4 sheet: id:table/book/sheet1 at: "D4" content: "=SUM(D2:D3)"))
