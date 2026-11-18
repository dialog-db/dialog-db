; The table's views, restated after the maintainer's choice of 2026-11-18:
; a view repeats as tonk's templates do, and a nested view is a call. Read
; after core.clj and the forms of table.clj that are not views. Each
; `subject={this}` of the YAML is an attribute holding the entity's
; variable alone, the marker.

; Each directory: the row holds the entity alone in `:subject` and
; repeats; the rows' div stands. YAML 480-510.
(view table/sheet :directory
  [:div {:class "table-sheet-rows"}
    [:div {:class "table-sheet-row"
           :subject ?s
           :data-table ?table
           :data-name ?name
           :data-order ?order}]]
  (table/sheet ?s table: ?table name: ?name order: ?order))

; GAP: an optional field, `data-style={style}`, as in table.clj. YAML 493.
(view table/cell :directory
  [:div {:class "table-cell-rows"}
    [:div {:class "table-cell-row"
           :subject ?c
           :data-sheet ?sheet
           :data-at ?at
           :data-content ?content}]]
  (table/cell ?c sheet: ?sheet at: ?at content: ?content))

(view table/column :directory
  [:div {:class "table-column-rows"}
    [:div {:class "table-column-row"
           :subject ?k
           :data-sheet ?sheet
           :data-at ?at
           :data-width ?width}]]
  (table/column ?k sheet: ?sheet at: ?at width: ?width))

(view table/row :directory
  [:div {:class "table-rowsize-rows"}
    [:div {:class "table-rowsize-row"
           :subject ?r
           :data-sheet ?sheet
           :data-at ?at
           :data-height ?height}]]
  (table/row ?r sheet: ?sheet at: ?at height: ?height))

; The live grid: tonk-table holds `?t` alone in `:subject`, the root; each
; hidden directory a call, its concept named, so no model is read off the
; host. YAML 512-540.
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
      (view table/sheet :directory)
      (view table/cell :directory)
      (view table/column :directory)
      (view table/row :directory)]]
  (table ?t name: ?name))

; Every workbook, each its `:ui`. The call names the concept, so the
; YAML's `model={dom.host/model}` is gone.
; GAP: the repeating element. The YAML's `<tonk-display entity={this}>`
; carries the marker, so in tonk the display repeats and the div stands; a
; call has no attribute to carry one, so here the whole template repeats,
; a div a table. YAML 543-549.
(view table :directory
  [:div {:class "table-directory"}
    (view table :ui of: ?t)]
  (table ?t name: ?name))
