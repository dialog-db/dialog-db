; The notebook's views, restated after the maintainer's choice of
; 2026-11-18: a view repeats as tonk's templates do, and a nested view is a
; call, `(view concept :name …)`. Read after core.clj and the forms of
; notebook.clj that are not views; nothing else of notebook.clj changes.
; Where tonk's YAML marks the repeating element with `subject={this}` or
; the like, the element here holds the entity's variable alone in an
; attribute, which is the marker.

; One notebook. tonk-notebook holds `?n` alone in `:data-subject`, and it
; is the outermost element that shows anything of the notebook: it repeats,
; and notebook-save stands. A block is a field of many values, an entry of
; the notebook's collection: the span that shows it is cloned once a
; block. YAML 308-331.
; GAP: a notebook with no block is no `notebook`: the concept requires
; every field. Its view shows notebook-save, empty.
(view notebook :ui
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
        [:div {:class "notebook-order"}
          [:span {:class "notebook-order__item" :data-block ?b :data-key ?k}]]
        (view notebook/block :directory)]]]
  (notebook ?n title: ?title key: ?k block: ?b))

; Every notebook. The YAML marks nothing: `href="/notebook/{this}"` is a
; value of several parts, no marker, so the whole template repeats, one
; `div.notebook-index` a notebook, as tonk renders it. YAML 343-356.
(view notebook :directory
  [:div {:class "notebook-index"}
    [:a {:class "notebook-index__item" :href ["/notebook/" ?n]}
      [:span {:class "notebook-index__title"} ?title]]]
  (notebook ?n title: ?title))

; The block rows: the row holds `?b` alone in `:subject` and repeats; the
; rows' div stands. YAML 359-369.
(view notebook/block :directory
  [:div {:class "notebook-block-rows"}
    [:div {:class "notebook-block-row"
           :subject ?b
           :data-block ?b
           :data-notebook ?n
           :data-source ?s}]]
  (notebook/block ?b notebook: ?n source: ?s))

; The index: the draft notebook and every notebook's directory, a call.
; GAP: read from another branch and space, `with="{branch}@{repo}"` on the
; nested display. A call's labels are fields of the concept called, and
; where to read is none: the call reads this branch. YAML 434.
(view notebook/index-route :ui
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
    (view notebook :directory)]
  (notebook/index-route ?site repo: ?repo branch: ?branch))

; One notebook's page: the notebook's own view, called for the entity the
; route names.
; GAP: read from another branch and space, as above. YAML 463.
(view notebook/route :ui
  [:div {:class "notebook-space"}
    (view notebook :ui of: ?entity)]
  (notebook/route ?site entity: ?entity repo: ?repo branch: ?branch))
