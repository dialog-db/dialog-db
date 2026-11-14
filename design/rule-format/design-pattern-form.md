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

**Decided: B**, by the agent, 2026-11-14, open to the maintainer saying otherwise before a release writes it.

```
 { "map":   [[key, pattern], …], "rest": term }     rest only where the pattern has one
 { "list":  [pattern, …],        "run":  term }     run only where it has one
```

There were three, with `tuple`, until the same day: the data model has maps and lists and no tuple (Irakli, 2026-11-14), so the patterns build lost its tuple and this lost the word for it.

A leaf is the term it is. The words are the ones a pattern is named by (`pattern_reference`), without the `:`.
