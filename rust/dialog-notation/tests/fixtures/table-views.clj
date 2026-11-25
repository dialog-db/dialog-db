; The table's views, restated as queries whose answer is markup, as
; design/macros/design-queries.md has them (stage 2 of 2026-11-18). Read
; after core.clj and table.clj. Four are a row each; the page collects
; them; the directory is a query of no entity, which stands once.

; One sheet's row. YAML 480-491.
(query (table/sheet-row ?s is: ?row)
  (table/sheet ?s table: ?table name: ?name order: ?order)
  (view ?row [:div {:class "table-sheet-row"
                    :subject ?s
                    :data-table ?table
                    :data-name ?name
                    :data-order ?order}]))

; One cell's row.
; GAP: an optional field, `data-style={style}`, as in table.clj. YAML 493.
(query (table/cell-row ?c is: ?row)
  (table/cell ?c sheet: ?sheet at: ?at content: ?content)
  (view ?row [:div {:class "table-cell-row"
                    :subject ?c
                    :data-sheet ?sheet
                    :data-at ?at
                    :data-content ?content}]))

; One sized column's row.
(query (table/column-row ?k is: ?row)
  (table/column ?k sheet: ?sheet at: ?at width: ?width)
  (view ?row [:div {:class "table-column-row"
                    :subject ?k
                    :data-sheet ?sheet
                    :data-at ?at
                    :data-width ?width}]))

; One sized row's row.
(query (table/rowsize-row ?r is: ?row)
  (table/row ?r sheet: ?sheet at: ?at height: ?height)
  (view ?row [:div {:class "table-rowsize-row"
                    :subject ?r
                    :data-sheet ?sheet
                    :data-at ?at
                    :data-height ?height}]))

; The live grid, and under it the four sets of rows it reads. Each set is
; every row of its kind, collected with `_` for the entity, as the YAML's
; displays are. YAML 512-540.
(query (table/ui ?t is: ?ui)
  (table ?t name: ?name)
  (table/sheet-row _ is: (distinct ?sheets))
  (table/cell-row _ is: (distinct ?cells))
  (table/column-row _ is: (distinct ?columns))
  (table/rowsize-row _ is: (distinct ?sizes))
  (view ?ui
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
        [:div {:class "table-sheet-rows"} ?sheets]
        [:div {:class "table-cell-rows"} ?cells]
        [:div {:class "table-column-rows"} ?columns]
        [:div {:class "table-rowsize-rows"} ?sizes]]]))

; Every workbook, each its page, in one directory. No entity: it answers
; once, and stands empty when there is no table, as tonk's YAML keeps the
; outer div and repeats the display inside it. YAML 543-549.
(query (table/directory is: ?directory)
  (table/ui _ is: (distinct ?tables))
  (view ?directory [:div {:class "table-directory"} ?tables]))
