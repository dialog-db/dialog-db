# Data Source Interface

## Status

Currently this is a proposal for the data source interface

## Context

We would like to define an interface that can be used across separate query engine implementations. Specifically we would like to use [datalogia](https://github.com/gozala/datalogia)  able to use implementation implementing this intreface as data store. At the same time we would like to create another query engine implementation in Rust that would be able to use this interface for the data store.

By considering two languages we can make an interface that is not too coupled with the query engine.

## Decision

> Interfaces are typed in [typescript] for compactness.

```ts
interface DataSource extends PullSource, PushTarget {
}
```

### Query Interface

Here is the proposed interface for querying the database.

```ts
interface PullSource {
    pull(query: PullQuery): Pull
}

// Phantom type represents `T` in binary representation.
interface Bytes<T, size extends number=number> extends Uint8Array {
    length: size
    valueOf(): Bytes<Of>
}

// Just a type alias denoting cryptogrphic hash. Assume 32 bytes but
// in practice it's probably just some fixed size which can be more or
// less than 32
type Digest<T, Size extends Number = 32> = Bytes<T, Size>

type Namsepace = string
type Path = string
type The = `${Namespace}/${Path}`

// Attribute / predicate is represented as concatenation
// of two hashes
type Attribute = [...Digest<Namespace>, ...Digest<Path>]

// Entity is the hash of arbitrary URI representing a subject in the
// fact
type Of = Digest<URI>

type Min<T extends { length: number }, U extends { length: number }> =
    // If T.length <= U.length ? T : U
    T | U

// Value reference is either byte represenation of value o
type ValueReference<T> = Min<Bytes<T>, Digest<T>>

// Perhaps we should look more into terminus as they make sure keys
// get sorted https://terminusdb.com/blog/terminusdb-internals-3/
// https://github.com/terminusdb-labs/tdb-succinct
type Is = 
    | [type: 0]                                            // Null
    | [type: 1, ...ValueReference<Uint8Array>]             // Raw bytes
    | [type: 2, ...Of]                                     // Entity
    | [type: 3, value: 1|0]                                // Boolean
    | [type: 4, ...ValueReference<string>]                 // UTF8
    | [type: 5, ...ValueReference<UnsignedInt>]            // ULeb128
    | [type: 6, ...ValueReference<SignedInt>]              // Leb128
    | [type: 7, ...ValueReference<Float>]                  // FP64
    | [type: 8, ...ValueReference<{[key:string]:}>]        // CBOR


type ByAttribute = { the: Attribute, of?: Of, is?: Is }
type ByEntity = { the?: Attribute, of: Of, is?: Is }
type ByValue = { the?: Attribute, of?: Of, is: Is }

// Pull query requires at least on of the 3 components 
type PullQuery =
    | ByAttribute
    | ByEntity
    | ByValue

type Pull {
    next(): Await<Result<FactReader[], Error>>
}

// @cdata - I remember considering not to capture all three
// components in the key but just 2 out of three as third could
// be the value, however having a hash of the value is interesting
// way to avoid having to load value, although I'm not sure it's
// going to be a win in practice. It's only useful when value is
// another entity in all other instances you probably will need
// value itself. Furthermore we may want to consider encryption in
// which case value would be encrypted and will require decryption
// before it could be used.
type FactAddress =
    | [index:0, ...Entity, ...Attribute, ...Is]
    | [index:1, ...Attribute, ...Entity, ...Is]
    | [index:2, ...Is, ...Value, ...Entity]
    
type FactReader = {
    address: FactAddress
    // Just a lazy getters with slices into FactAddress
    the: The
    of: Of
    is: Is
    // Loads and decodes actual fact
    read(): Fact
}

type Fact = {
    the: `${Namespace}/${Path}`,
    of: URI,
    is: Value,
    cause: Digest<Fact>
}
```

### Transaction Interface

```ts
interface PushTarget {
    push(changes: Changes): Result<Digest<Root>, Error>
}

// Hierarchical structure provides some of the columnar encoding
// style savings as it naturally deduplicates. Actual on wire
// representation may be even more compact binary one
type Chages = {
    // base64 encoded namespace digest
    [namespace: string]: {
        // base64 encoded path digest
        [path: string]: {
            // base64 encoded entity digest
            [of: string]: {
                // base64 encoded causal reference
                [cause: string]: {
                    // {} for retraction
                    // {is:value} for assertion
                    is?: Value 
                }
            }
        }
    }
}
```

## Consequences

What becomes easier or more difficult to do because of this change?

[typescript]:https://www.typescriptlang.org/docs/handbook/type-inference.html
