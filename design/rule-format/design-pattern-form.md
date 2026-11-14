# How a pattern is written where a parameter stands

**Problem it serves:** a rule that holds a map or a list of terms cannot be kept ([rule-format](../rule-format.md)). Format 4 has to write one, and the patterns plan left "the words of the wire form" undecided.

What is there to fit beside, in a stored body:

```
 a variable    { "?": { "name": "title" } }
 a constant    { "text:": "Buy milk" }        the key is the name of its type, which ends in `:`
```

| criterion \ approach | A: under the name of a type, `map:` | B: under a word, `map` | C: under one key, `{ "pattern": { "map": … } }` |
|---|---|---|---|
| cannot be read as a constant or a variable | 🟡 it can today. `map:` is a key shaped as a type's name is, so a type of that name, were one added, would be read as a pattern | 🟢 a type's name ends in `:` and a variable is under `?`; a word is neither | 🟢 the same |
| a map's key is any value | written as pairs in all three: a dag-cbor map takes text for a key | the same | the same |
| one pattern, one byte string | the entries in the order a map keeps its keys in, in all three | the same | the same |
| a rule that holds no pattern | untouched in all three: a term is written as it was | the same | the same |
| what is read when printed as JSON | `{"map:": [...]}` | `{"map": [...], "rest": ...}` | one level more for every pattern, at every depth |

**Decided: none of the three.** B was the agent's on 2026-11-14 and stood for a morning. The maintainer, the same day, asked why a map is not written as a map and a list as a list, and settled this:

```
 {author: ?who pinned: true ... ?others}        [?first "math" ... ?more]

 { "match": {                                    { "match": [
     "author": { "?": { "name": "who" } },           { "?": { "name": "first" } },
     "pinned": { "boolean:": true },                 { "text:": "math" },
     "...":    { "?": { "name": "others" } }         { "...": { "?": { "name": "more" } } }
 } }                                             ] }
```

- One key, `match`, where a parameter stands. His word, over `pattern` and over `#`.
- Inside it a map is a map pattern and an array is a list pattern, at any depth, with no wrapper again. A variable and a constant are written as they are everywhere in a body.
- `...` is a key, in both: among a map's entries, and as the one entry of a list's last item. Its value takes the rest. He refused `"..."` as a bare string among a list's items.
- `... ` with nothing after it is a blank: `{ "...": { "?": {} } }`.

It is C of the matrix with the shape words taken out of it: the shape is read off whether what is under `match` is a map or an array.

## Open: what a map's key is

The form writes a key as the text of a dag-cbor map's key. What is built does not fit that yet:

```
 a map as built (#778)       a key is a value of any type; `author` and "author" are two keys
 the notation                {author: ?who} keys by the symbol `author`; {"a" ?x} and {1 ?x} are read too
 the form above              a key is text, so it says one kind of key and no other
```

So the form says less than a map pattern can hold, and something has to give. The row "a map's key is any value" of the matrix is why A, B and C all wrote pairs.

| criterion \ approach | names only: a map's key is a symbol | text only: a map's key is text, as in IPLD | any value, as built |
|---|---|---|---|
| the form above | 🟢 fits with nothing refused: a symbol is lowercase letters, digits, `-` and `.`, so no key is `?`, `...` or ends in `:`, which is how a nested map is told from a variable, the rest and a constant | 🟡 fits, and three keys are refused or escaped: `"?"`, `"..."`, and one that ends in `:` alone in a map | 🔴 does not fit: entries go back to pairs, or a key is spelled with its type |
| `{author: ?who}` in the notation | the key it writes today | 🟡 the key becomes text; what is kept under #773 today is keyed by symbols | as today |
| `{"a" ?x}`, `{1 ?x}` | refused | `{"a" ?x}` stays, `{1 ?x}` refused | as today |
| a kept syntax tree | 🟡 a struct node keeps its entries in a map keyed by nodes (`syntax.rs`); they become a list of pairs | 🟡 the same | as today |
| what else uses a key that is not a name | nothing found: 17 of the notation's 18 maps are records of names, the other is the row above | the same | ? |

Not decided. The maintainer's: it is what a map is in the data model (#778), not only how a rule is written. The agent's hypothesis: names only, since nothing built needs another key and it is the one under which his form has no special case. Refuted by a use for a key that is not a name.
