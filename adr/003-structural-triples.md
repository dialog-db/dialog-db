# Structural datalog

## Status

This is a design space exploration document that may grow into a concrete
proposal.

## Context

### Nominal datalog

Conventional datalog notation reminiscent [nominal types] where rule names do the heavy lifting. Here is an example of `ancestor` rule

```prolog
ancestor(Of, Is) :-
  parent(Of, Is).
ancestor(Of, Is) :-
  parent(Of, Parent),
  ancestor(Parent, Is).
```

<details>
<summary>You could even express it directly in SQL notation</summary>

```SQL
CREATE TABLE IF NOT EXISTS parent (
    Of TEXT NOT NULL,
    Is TEXT NOT NULL,
    PRIMARY KEY (Of, Is)
);

CREATE TABLE IF NOT EXISTS ancestor (
    Of TEXT NOT NULL,
    Is TEXT NOT NULL,
    PRIMARY KEY (Of, Is)
);

-- Create a trigger to initialize ancestor table from parent relationships
CREATE TRIGGER IF NOT EXISTS insert_parent_to_ancestor
AFTER INSERT ON parent
FOR EACH ROW
BEGIN
    INSERT OR IGNORE INTO ancestor (Of, Is)
    VALUES (NEW.Of, NEW.Is);
END;

-- Create a trigger for transitive relationships
CREATE TRIGGER IF NOT EXISTS update_ancestor_transitive
AFTER INSERT ON ancestor
FOR EACH ROW
BEGIN
    INSERT OR IGNORE INTO ancestor (Of, Is)
    SELECT parent.Of, NEW.Is
    FROM parent
    WHERE parent.Is = NEW.Of;
END;
```
</details>

Nominal nature makes it prone to [schema migration problems], which is a lot harder in decentralized settings and pretty much impossible in open ended one.

#### Data Modeling Concerns

Say we want to model a playlist tracks and start out with a data model like one below

```ts
type Track = {
  artist: string
  title: string
  duration: number
}
```

It would seem natural to record data in the following form

```prolog
track("Queen", "Bohemian Rhapsody", 355).
track("Depeche Mode", "Enjoy the Silence", 373).
```

Later on if we wanted to also include information about an `album` we would would face a challenge.

> ℹ️ One could borrow conventional wisdom from SQL and just add `id` to all our records

With this convention we end up with set of ground facts and a rule to derive tracks

```prolog
artist(11, "Queen").
artist(12, "Depeche Mode").
song(21, 11, "Bohemian Rhapsody", 355).
song(22, 12, "Enjoy the Silence", 373).
track(Artist, Title, Duration) :-
  artist(ArtistID, Artist),
  song(SongID, ArtistID, Title, Duration).
```


Now adding an `album` information to our tracks would be more viable

```prolog
album(31, "Queen").
album(32, "Violator").

song_in_album(21, 31).
song_in_album(22, 32).

track_v2(Artist, Title, Album, Duration) :-
  artist(ArtistID, Artist),
  song(SongID, ArtistID, Title, Duration),
  song_in_album(SongID, AlbumID).
```

This does not solved a schema migration problem, but a best practice at least provides means to iterate over data model. In a decentralized settings however two conflicting meanings can be assigned a names like `album` and there is no clear path to reconsiling them.

### Semantic datalog

[Datomic] and [RDFOX] break away from the norm and embrace semantic triples for modeling data. Here is an example of the same ancestor rule in datomic notation

```clj
[(ancestor ?person ?ancestor)
 [?person :person/parent ?ancestor]]
[(ancestor ?person ?ancestor)
 [?person :person/parent ?parent]
 (ancestor ?parent ?ancestor)]
```

And here is the equivalent in RDFOX syntax which is almost identical to one used by datomic.

```sparql
[?person, :ancestor, ?ancestor] :-
  [?person, :parent, ?ancestor] .
[?person, :ancestor, ?ancestor] :-
  [?person, :parent, ?parent],
  [?parent, :ancestor, ?acestor] .
```

#### Embedded wisdom

It may not be immediately obvious but they do something very interesting, they lifted conventional wisdom from a best practice to an only possibly practice.

- Facts are modeled as semantic triples in form of `[entity, attribute, value]` (a.k.a `[subject, predicate, object]`) ingraining best pracite about `ID`s.

- Facts as semantic triples take composibility to an extreme, because all records end up breaking down to smallest possible units from which everything else is composed.

Previously we end up defining `song_in_album` facts to capture relation as a semantic triple `[?song :song/album ?album]` which enabled us to express extension for `Track` data model and in turn allowed us to define a `track_v2` as composition of two. In type notation it could corresponds to rougly following

```ts
type Track = {
  artist: Artist
  title: string
  duration: number
}

type Artist = {
  name: string
}

type Album = {
  name: string
}

type TrackAlbum = {
  album: Album
}

type TrackV2 = Track & TrackAlbum
```

🤔 However it is good to ponder how things would have turned out if we have started track that accounted for `album`  from the begining, most likely all tracks would have had `artist`, `title`, `album`, and `duration`, but than how would we have stored information about songs that we did not know an `album` or a `duration` for ?

#### Subetyping

In type systems you could define a supertype containing subset of the subtype in fact that is what we end up defining in our `song` rule in datalog example. In other words we broke down model into smaller parts and then composed `track` from them.

That is effectively what triples do they just take it to a logical extreme. This effectively reduces need for schema migration because our tables end up just maps of `entity -> value` where name of the table serves as an `attribute`.

Everything else is just composition (or a query over those tables), therefor instead of migartion we can simply define a new query.


#### Optional fields

Alterantive way to handle our delema would be to make all fields optional _(explains why that is a default in SQL)_ that way you can always have a partial information. In some way this in almost what semantic triples are with a slight difference that record with no fields is possible in a design with all optional fields and impossible in semantic triples, but then again what is the point of entity if it has no relation to anything else ?

In this approach however we either make all schema evolutions a second class extensions or we end up having to deal with schema migrations where we keep adding more and more optional fields to one very generic data model _(which also does not jazz with variant types)_.

### Structural datalog

✨ Semantic datalog removes an ability to make a bad design choices in data modeling how to group units of data by embracing triples. This choice also reduces need for data migrations because all facts are atomic and anything more complicated is derived through relations between those atoms.

Datomic fully embraces namespacing which addresses schema migration problem even in decentralized setup as long as they are evolved in an additive fashion within a namespace which is the only way to do it, something that [Rich Hickey very well puts in his talk][namespacing], if your change is not additive just fork the namespace. However who has an authority over specific namespace in an open ended system or how to coordinate updates to it is not very obvious.

💡 I think we could do better by embracing [the big idea] from union language what if namespaces were simply derived from the data model itself. Here is how we could go about this

```clj
(track :artist artist :title string :duration duration)
```

Here we define the data model for the track entities, we could derive the hash for it's members by enumerating their names and types lets assume it to be `#ba4jcao2nbln4ui7v2ff`, in which case our `track` will be just a local reference to `#ba4jcao2nbln4ui7v2ff` which desugaring to:

```clj
(#ba4jcao2nbln4ui7v2ff
  :artist ?artist :title ?title :duration ?duration
  ;; Type predicates
  (artist ?artist)
  (string ?title)
  (duration ?duration)
  ;; Relation predicates
  (#ba4jcao2nbln4ui7v2ff/artist ?this : ?artist)
  (#ba4jcao2nbln4ui7v2ff/title ?this : ?title)
  (#ba4jcao2nbln4ui7v2ff/duration ?this : ?duration))

(#ba4jcao2nbln4ui7v2ff/artist
  :of ?entity :is ?artist
  (assert
    :the #ba4jcao2nbln4ui7v2ff/artist
    :of ?entity
    :is ?artist))

(#ba4jcao2nbln4ui7v2ff/title : ?title
  (assert
    :the #ba4jcao2nbln4ui7v2ff/title
    :of ?this
    :is ?title))

(#ba4jcao2nbln4ui7v2ff/duration : ?duration
  (assert
    :the #ba4jcao2nbln4ui7v2ff/duration
    :of ?this
    :is ?duration))
```

Where last three relation assertions correspond to following expressions in datomic notation

```clj
[?track :ba4jcao2nbln4ui7v2ff/artist ?artist]
[?track :ba4jcao2nbln4ui7v2ff/title ?title]
[?track :ba4jcao2nbln4ui7v2ff/duration ?duration]
```

#### Structurally identical but semantically different

Structural namespacing removes the need for linear schema evolution as names are no longer have implications for schema evolution, better yet structurally identical schemas defined by different authors would converge onto same namespace e.g. following schema is just a different way to reference `#ba4jcao2nbln4ui7v2ff`

```clj
(song :title string :artist artist :duration duration)
```

However [differentiating between identical structures] may be semantically important, which could be accomplished through [literal types], here is the demontstration

```clj
(track
  :in :gozala.io/music/track
  :artist artist
  :title string
  :duration duration)
```

In fact this is how we can define nominal types like `duration` via semantically distinct data type from the `uint64`.

```clj
(duration unit32 :unit :seconds)
```

#### Explicit rules

Ok lets go back and define body for the `track` rule for which we have defined a data model earlier. Here we pick subset of relations from the more complete `song` data model. This effectively defines unidirectional mapping from `song` to a `track` where `song` really acts as a vocabulary for various song related terms.

```clj
(track
  :artist artist :title string :duration duration
  (song/title ?song : ?title)
  (song/artist ?song : ?artist)
  (song/duration ?song : ?duration))

(song
  :artist artist
  :composer artist
  :name string
  :duration duration
  :album album
  :rating rate
  :genre string
  :year uint8)
```


> ℹ️ This may not have being obvious but I think this provides an interesting framework that can be used to derive track's from the data stored in the database (kind of like triggers in SQL). This effectively means that we can derive records denotedy by `#ba4jcao2nbln4ui7v2ff` when `track` rule conditions are met then we can query them when `(track :title "Bohemian Rhapsody")` encountered.
>
> Implication here is that on query we can query first run rule body on new data to derive `#ba4jcao2nbln4ui7v2ff` facts and then select all the matches from al lthe `#ba4jcao2nbln4ui7v2ff` facts (new or pre-existing). We could also do two things concurrently where we start querying the derived facts in the cache while deriving new ones from novel data. This is also where [DBSP] neatly fits into the picture.
>
> 🧩 Final piece of the puzzle is how do we invalidate derived records when original ones got updated. This is where ideas from [dedalus] fit neatly. I imagine we could define inductive rules with `!` suffix e.g `(track! ...)` that could define deriviation logic like before and use negation in the body to define when they expire get superceded.

## Decision

What is the change that we're proposing and/or doing?

## Consequences

What becomes easier or more difficult to do because of this change?

[nominal types]:https://en.wikipedia.org/wiki/Nominal_type_system
[schema migration problems]:https://www.inkandswitch.com/cambria/
[datomic]:https://www.datomic.com/benefits.html
[rdfox]:https://docs.oxfordsemantic.tech/reasoning.html#rule-languages
[namespacing]:https://youtu.be/oyLBGkS5ICk
[the big idea]:https://www.unison-lang.org/docs/the-big-idea/
[differentiating between identical structures]:https://www.unison-lang.org/docs/fundamentals/data-types/unique-and-structural-types/#what-happens-if-we-create-identical-structural-types
[literal types]:https://www.typescriptlang.org/docs/handbook/literal-types.html
[DBSP]:https://arxiv.org/abs/2203.16684
[dedalus]:https://www.neilconway.org/docs/dedalus_dl2.pdf
