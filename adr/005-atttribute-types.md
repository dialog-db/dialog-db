# Multiple Attribute Types

## Status

Propesed.

## Context

Currently, attributes are expected to be UTF-8 strings with at least `/` delimiter to provide a namespace so that facts in the namespace will end up segmented close to each other.

From the experience of building on top of this design it becomes apparent that named attributes work well for cardinality 1 type relations, but not so well for cardinality >1.

> ℹ️ When relations have set semantics it's actually not bad up until you start tracking causality, but then two goals start to entangle and tradeoffs either favor cardinality 1 or many.

We can optimize layout by cardinality and access pattern and I suggest to capture desired access pattern in the attribute type. Specifically I propose that we define attribute as a variant of three distinct types

```wit
variant attribute {
  name(utf8),
  reference(digest),
  position(fractional-index),
}

// UTF-8 strings (normal attributes like "user/name")
type utf8 = list<u8>;
// 0xFF (255) followed by the bytes of the content hash
type digest = list<u8>;
// 0xFE (254) followed by the fractional index
type fractional-index = list<u8>;
```

⚠️ Some LLM provided insight (have not verified accuracy)

> You could use the non-UTF8 high bit range `(128-255)` for your indexed attributes. Here's why this would work:
>
> 1. Valid UTF8 has specific patterns for bytes in the `128-255` range:
>    - Single-byte characters are `0-127`
>    - Multi-byte character sequences start with bytes in `192-247` range
>    - Continuation bytes are in `128-191` range
>   2. The byte `255 (0xFF)` would be perfect for your index position markers since:
>      - It's never a valid UTF8 byte (outside both lead byte and continuation byte ranges)
>      - Regular string attributes like `"user/name"` would continue using standard UTF8 encoding
>
>   For implementation, you could:
>
>  1. Encode index numbers by using `0xFF` as a prefix, followed by the index value
>  2. When decoding attributes, check if the first byte is `0xFF` to determine if it's an index
>
> This approach avoids adding a type tag byte to common attributes while still allowing you to represent both string and index-based attributes.

## Membership semantics

Set membership can be expressed by using `value` hash formatted as `reference` attribute of the relation. By prefixing hash with `0xFF` byte we will be able to distinguish it from other relations and leverage that during queries.

Additionally this would allow us to lookup if value is a member of an entity by simple range scan without having to decode any values.

Counter argument could be to just prefix attributes with some character like `#` which would provide similar functionality but in user space.

However by using distinct `reference` would allow us to also optimize the way values are stored and merged inside a tree, which would be impossible to do purely in the userland without introducing incompatibilities across users.

## Oredered semantics

Ordered relations could expressed thorugh a [deterministically biased fractional indexing] by representing relation as `position` attribute.

This would offer similar benefits to the `reference` attribute, allowing for efficient sorted enumartion via range scans and optimized storage and merging of values within a tree.

## Decision

What is the change that we're proposing and/or doing?

I propose that we [revise `ValueDataType`](https://github.com/dialog-db/staging/blob/dee2c5440e074b2ba8dab3effe09950b7c2d2db1/rust/dialog-artifacts/src/artifacts/value.rs#L261-L283) definition to account for three distinct attribute types.

## Consequences

What becomes easier or more difficult to do because of this change?

[Deterministically biased Fractional Indexing]:https://observablehq.com/@gozala/deterministically-biased-fractional-indexing
