; The notebook's views, restated as queries whose answer is markup, as
; design/macros/design-queries.md has them (stage 2 of 2026-11-18). Read
; after core.clj and notebook.clj. Three queries make the page; a notebook
; with no block shows its whole page.

; One entry of the order: the block at a position. Its answer is an
; entry, so the page's order stands in the order of the positions.
(query (notebook/order-item ?n is: {?k ?item})
  (notebook/blocks ?n block: {?k ?b})
  (view ?item [:span {:class "notebook-order__item" :data-block ?b :data-key ?k}]))

; One block's row, its source on it. YAML 359-369.
(query (notebook/block-row ?b notebook: ?n is: ?row)
  (notebook/block ?b notebook: ?n source: ?s)
  (view ?row [:div {:class "notebook-block-row"
                    :subject ?b
                    :data-block ?b
                    :data-notebook ?n
                    :data-source ?s}]))

; One notebook: the element reads two hidden row sets, the order and the
; block rows. Wrapped in <notebook-save> so Cmd/Ctrl+S saves it. YAML
; 308-331.
(query (notebook/ui ?n is: ?ui)
  (notebook/named ?n title: ?title)
  (notebook/order-item ?n is: (collect ?order))
  (notebook/block-row _ notebook: ?n is: (collect ?rows))
  (view ?ui
    [:notebook-save {:style "display: block;"}
      [:tonk-notebook {:data-subject ?n
                       :data-notebook ?n
                       :title ?title
                       :on/blockedit (block/edit subject: .currentTarget.dataset.subject
                                                 source: .detail.source
                                                 notebook: .detail.notebook)
                       :on/place (block/place subject: .currentTarget.dataset.subject
                                              notebook: .detail.notebook
                                              key: .detail.key)
                       :on/remove (block/remove subject: .currentTarget.dataset.subject
                                                notebook: .detail.notebook
                                                key: .detail.removed)
                       :on/titlechange (notebook/retitle subject: .detail.notebook
                                                         title: .detail.title)
                       :style "display: block;"}
        [:div {:class "notebook-data" :hidden true}
          [:div {:class "notebook-order"} ?order]
          [:div {:class "notebook-block-rows"} ?rows]]]]))

; One notebook's link in the index. YAML 343-356.
(query (notebook/index-item ?n is: ?item)
  (notebook/named ?n title: ?title)
  (view ?item
    [:a {:class "notebook-index__item" :href ["/notebook/" ?n]}
      [:span {:class "notebook-index__title"} ?title]]))

; Every notebook: one index, which stands empty when there is none.
(query (notebook/directory is: ?directory)
  (notebook/index-item _ is: (collect ?items))
  (view ?directory [:div {:class "notebook-index"} ?items]))

; The index route: the draft notebook and the directory.
; GAP: read from another branch and space, `with="{branch}@{repo}"` on the
; nested display. A query collected reads this branch. YAML 434.
(query (notebook/index-page ?site is: ?page)
  (notebook/index-route ?site repo: ?repo branch: ?branch)
  (notebook/directory is: (collect ?directory))
  (view ?page
    [:tonk-notebook-index {:class "notebook-index"}
      [:notebook-save {:style "display: block;"}
        [:tonk-notebook {:draft true
                         :with [?branch "@" ?repo]
                         :on/blockedit (block/edit subject: .currentTarget.dataset.subject
                                                   source: .detail.source
                                                   notebook: .detail.notebook)
                         :on/place (block/place subject: .currentTarget.dataset.subject
                                                notebook: .detail.notebook
                                                key: .detail.key)
                         :on/remove (block/remove subject: .currentTarget.dataset.subject
                                                  notebook: .detail.notebook
                                                  key: .detail.removed)
                         :on/titlechange (notebook/retitle subject: .detail.notebook
                                                           title: .detail.title)
                         :style "display: block;"}]]
      ?directory]))

; One notebook's page: the notebook's own markup, for the entity the
; route names.
; GAP: read from another branch and space, as above. YAML 463.
(query (notebook/page ?site is: ?page)
  (notebook/route ?site entity: ?entity repo: ?repo branch: ?branch)
  (notebook/ui ?entity is: (collect ?ui))
  (view ?page [:div {:class "notebook-space"} ?ui]))
