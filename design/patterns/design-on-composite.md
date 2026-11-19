# Design decision: patterns over the composite of #843

**Problem it serves:** the patterns stood on a value the maintainer has replaced: dag-cbor maps with text keys and lists, kept in a `Value::Record` after a version byte. Its replacement is `Value::Composite` ([design/composite-value.md](../composite-value.md)): a map whose keys are any scalars and whose key may hold several values as a set, a record as the map whose keys are all attributes, and a list, read in place through a view. Each place the patterns met the old value has to say what it means on the new one.

**Decided, each the building agent's, to be overruled:**

| question | decided | why |
|---|---|---|
| what is left of the old value | nothing | no release tag contains the commits that brought it (09378ab9, 87e7005a, 033cac15), and neither does `main`, so no replica holds a record in its layout |
| what a map pattern matches | a composite map, a record among them; a list pattern a composite list | a record is a map told by its keys, so one pattern kind serves both, as the composite has one kind for both |
| what a pattern's key is | any scalar, named as the composite holds it: text for a text key, an attribute for a field. Text and an attribute are two keys | the composite keeps them apart (they sort under different type bytes and have different references); a pattern that read `"note/title"` as `note/title` would match values it does not build |
| how a key is written in code | `Key`, implemented by `&str`, `String`, an attribute (the query layer's `Relation`, as `the!` makes one, and the corpus's) and `Value` | keeps `Pattern::map([("title", ...)])` of #778 compiling, and adds attributes and other scalars |
| how a key is written in a pattern's text | text quoted, as #778 wrote it; any other key as its value's debug form | #778's writing is unchanged for every pattern it could make; an attribute and the text that spells it are written apart |
| a key that holds several values | an entry matches a key that holds exactly one; a map whose named key holds several matches no pattern that names it; a rest takes such a key with all its values; a rest that holds a named key builds nothing | a pattern built holds one value under each key it names, so taken apart it must have read one, or a rule holds or not by which premise ran first. Reading a key's several values one per row (Datomic's way for a card-many attribute) is the other choice; it makes `take_apart` yield rows, not a row, and breaks building and taking apart agreeing. Left for when a need shows |
| a key named twice in a pattern | refused, `PatternError::RepeatedKey` | follows from the row above: the second entry could match nothing. The natural 3 and the float 3.0 are one key, as in a composite |
| closed or open | as #778: no rest is exactly the named entries, a rest takes the others | the maintainer's question in the journal (2026-11-13) is still open; this port changes nothing about it |
| reserved keys `...` and `?` | nothing refused | #778 refuses no key. A text key is written quoted, so `"..."` cannot be read as a rest; whether the notation's reader refuses them is the notation's to decide |
| `EvaluationError::RecordVersion` | removed, and `Pattern::unify` returns `bool` | a composite has no version, and its bytes are accepted only as its one canonical encoding when the value is read, so a pattern meets no composite it cannot read. An opaque `Value::Record` is no shape and drops the row |
| typing | a map pattern and a rest are `composite:map`, a list pattern and a run `composite:list` | the query layer's `Primitive` has a bit for each, and puts a record on the map's |
| a composite constant in a rule's identity | its `Composite::reference` | a constant's reference is meant to agree with `Value::reference`; the wire form of a composite has no arm in the rule reference's reader |

**Reading in place.** A named key is read by `Composite::get` and an item by `Composite::item`: each decodes only what it returns, and a nested composite comes back as a view of the same buffer. A pattern three maps deep, each map holding sixteen siblings of 64 bytes, binds the one field it names with one scalar decoded (`it_decodes_only_the_field_it_binds`, with the composite's decode counter). Only a rest or a run that binds a variable decodes what it takes, to build the map or the list.
