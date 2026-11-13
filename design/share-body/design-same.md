# What makes two bodies one

Decision: when may one rule write its head from another rule's matches. For: a commit pays for a condition once for every rule that states it.

A wrong answer here is a lost or an invented match, so the criteria that decide are the ones about what the evaluation reads.

| criterion \ approach | Status quo: every rule alone | The stored encoding, compared under a renaming (the prototype on `spike/induce-cost`) | One identity for the body: the rule's own canonical spelling with the head left out | The premises and the types, as the evaluation holds them, under a labeling of the body alone |
|---|---|---|---|---|
| a type the head gives a variable | each rule is evaluated under its own types | 🔴 the body is evaluated under a rule made of every member's head, so each head's narrowing applies to all; two heads that narrow one variable differently lose matches | 🔴 the types are not part of a spelling; a head that narrows a number is not seen | the type of every variable is part of what is compared; rules whose heads narrow differently are two bodies |
| what is compared | nothing | the rule's JSON encoding, searched for a pairing of premises by backtracking | the encoded premises | the premises by `PartialEq`, after bucketing by their encoding; an encoding that dropped something the evaluation reads would not make two bodies one |
| a head that writes a collection | written | 🟡 left out | written | written: the key is a variable of the body and is read through the renaming like any other |
| a variable the body does not bind (an optional head field) | unbound | 🟡 looked up under its own name in another rule's matches | depends on the lookup | unbound: a name with no place in the body is not looked up |
| rounds with more than one body | every rule alone | 🟡 not shared at all | partitioned | partitioned |
| what it costs a round | nothing | a search a set of candidates, kept for the thread and never dropped | a hash lookup a rule | a comparison of premises a rule a round; the spelling is worked out once a rule and kept with the decoded rule, in a cache the environment bounds |
| where two spellings of one body are told apart | - | where the encodings differ beyond variable names | past the bound of the labeling's search (1024 leaves) | the same bound; then the two are evaluated alone, as before |

Chosen: the last column. The agent's choice, not yet the maintainer's.

Why the head reaches the body at all: `TypeEnv::infer_for` narrows a variable the body left open to the numeric type of the head field that reads it, so that the scan feeding a natural field reads naturals. Nothing else of a head is read by a seeded evaluation (claims 1 and 2).
