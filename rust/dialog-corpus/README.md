# dialog-corpus

The corpus: the body of claims a branch holds, in the search tree, keyed so
that entity-, attribute- and value-ordered scans are all range reads; and the data model those claims are written in
(`Fact`, `Entity`, `Relation`, `Value`), with the importer and exporter
traits other formats implement.

The crate holds no store. It asks the environment it is given for what it
reads: a tree's nodes through `dialog_search_tree::nodes`, and a value
spilled out of its key through `dialog_effects::blob::Load`. What a store
is, where it is and what it caches belong to whoever answers.
