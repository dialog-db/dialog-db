# Notation

Dialog uses two notations for describing domain models:

- **Formal notation** is the explicit representation. It can be expressed in either JSON or YAML; both forms correspond one to one. Every field is explicit and every reference is structural. The JSON schema defines the formal notation.

- **Abbreviated notation** is a YAML-only shorthand for human authoring. It introduces an addressing scheme, implicit field inference from document structure, and punning. The abbreviated notation is an intermediate representation that expands into the formal notation.

## Formal notation

### Structural identity

Both attributes and concepts are structurally identified. Their identity is derived from their components, not from a name. However, attributes contain a nominal component (`the`) that captures semantic intent, identifying the relation in `domain/name` format and distinguishing attributes that would otherwise be structurally identical.

An attribute's identity is the tuple `(the, type, cardinality)`. A concept's identity is derived from the sorted set of its constituent attribute identities. Two definitions with the same structure are the same thing, regardless of how they are referred to.

The `the` component within an attribute is nominal: it carries meaning beyond structure. `diy.cook/quantity` and `diy.cook/price` may both be `(*, Integer, one)` structurally, but they are distinct attributes because `the` denotes the kind of relation they form, which is what makes it part of the identity in the first place.

### Selector

An attribute selector is the combined `domain/name` string. The total length must not exceed **64 bytes**, which is the storage-layer encoding budget.

### Domain

A domain groups related attributes. Domains may use dot-separated segments for hierarchical organization, following a reversed domain name convention to avoid collisions between independently developed schemas.

**Rules:**

- Lowercase ASCII letters, digits, hyphens, and dots
- Must start with a letter
- Must not end with a dot or hyphen
- At least one character

```
person
diy.cook
io.gozala.person
org.example.hr
```

**Regexp:**

```
^[a-z][a-z0-9.-]*[a-z0-9]$|^[a-z]$
```

### Name

The name component of an attribute takes one of two forms, distinguished by
the case of its first byte. The two are disjoint and exhaustive, which is what
lets a single scan of one domain serve both and lets a query narrow to either
half as a contiguous key range.

#### Symbol names

A named predicate, in lowercase kebab-case. This is the ordinary case: the
named fields of an entity.

**Rules:**

- Lowercase ASCII letters, digits, and hyphens (no dots)
- Must start with a letter
- Must not end with a hyphen
- At least one character

```
quantity
ingredient-name
recipe-step
```

**Regexp:**

```
^[a-z][a-z0-9-]*[a-z0-9]$|^[a-z]$
```

#### Position names

A fractional position, beginning with an uppercase letter. A position name
marks its fact as a member of an ordered relation rather than a named field,
and positions sort lexicographically — so members of one collection come back
from a single range scan already in order, with no per-element join and no
order state held outside the facts themselves.

```
N
N5
Zk3
```

Because the two forms differ in the case of the first byte, one domain can
carry both at once — an entity's named fields and its ordered members
together:

```
todo.list/title     a symbol name: the list's title
todo.list/owner     a symbol name
todo.list/N         a position name: a member
todo.list/N5        a position name: a later member
```

A query selects one half with `name.case` (see [Variables](#variables)), or
omits the constraint to take both and split them downstream.

### References

In the formal notation, all references are structural: attributes are described inline by their full definition `{ the, as, cardinality }` and concepts by their full set of constituent attributes. There are no names to look up; everything is self-describing.

A relation is referenced by its qualified form `domain/name` with `/` as separator. The combined selector must not exceed 64 bytes:

```
person/name
diy.cook/quantity
diy.cook/ingredient-name
io.gozala.person/name
```

### Attribute

An attribute is a relation elevated with domain-specific invariants. It extends a relation's `domain/name` identifier with type and cardinality constraints, specifying what kind of values the association admits and how many. An attribute's identity is structural: `(the, type, cardinality)`. The `description` field is part of the attribute definition but not part of its identity; two attributes with the same structure but different descriptions are the same attribute.

```json
{
  "description": "Name of the person",
  "the": "io.gozala.person/name",
  "cardinality": "one",
  "as": "Text"
}
```

```yaml
description: Name of the person
the: io.gozala.person/name
cardinality: one
as: Text
```

<details>
<summary>Attribute</summary>
<pre>
{
  "Attribute": {
    "type": "object",
    "description": "A relation elevated with domain-specific invariants. An attribute's identity is structural: (the, type, cardinality).",
    "properties": {
      "description": {
        "type": "string",
        "description": "Human-readable description of the attribute."
      },
      "the": {
        "type": "string",
        "description": "The relation in domain/name format (e.g. 'diy.cook/quantity')."
      },
      "cardinality": {
        "type": "string",
        "enum": ["one", "many"],
        "description": "Cardinality of the attribute. Defaults to 'one' when omitted.",
        "default": "one"
      },
      "optional": {
        "type": "boolean",
        "default": false,
        "description": "Whether the field is set-widened: a missing claim yields a row with the field bound to absent rather than dropping the row."
      },
      "conforms": {
        "type": "string",
        "description": "A concept the field's target entity must satisfy, as its 'concept:{hash}' URI. Entity-valued attributes only; mutually exclusive with 'optional'."
      },
      "as": {
        "description": "Value type of the attribute. If omitted, any type is allowed.",
        "type": "string",
        "enum": ["Bytes", "Entity", "Boolean", "Text", "UnsignedInteger", "SignedInteger", "Float", "Symbol"]
      }
    },
    "required": ["the"]
  }
}
</pre>
</details>

#### Value Types

The `as` field declares what kind of value the attribute admits. Scalar types from the `dialog` domain can be referenced without qualification:

| Type              | Description                 |
|-------------------|-----------------------------|
| `Bytes`           | Raw byte sequence           |
| `Entity`          | Reference to another entity |
| `Boolean`         | `true` or `false`           |
| `Text`            | UTF-8 string                |
| `UnsignedInteger` | Unsigned integer            |
| `SignedInteger`   | Signed integer              |
| `Float`           | IEEE 754 floating point     |
| `Record`          | Structured data, opaque to the query layer |
| `Symbol`          | Symbolic identifier         |

A concept can additionally require an entity-valued field's target to satisfy
a concept — see [Concept-typed fields](#concept-typed-fields). That constraint
lives on the field inside a concept's `with`, not on the attribute itself: an
attribute is reusable across concepts, and conformance is a demand one concept
makes of its own field.

#### Future Attribute Extensions

**Not yet supported.** An attribute will be able to constrain values to a
fixed set of symbols:

```json
{
  "description": "Unit of measurement",
  "the": "diy.cook/unit",
  "as": ["diy.cook/tsp", "diy.cook/mls"]
}
```

#### Cardinality

Cardinality governs what happens when a new claim is asserted for an attribute an entity already has a value for.

- `one` (default): asserting a new value retracts the prior claim so at most one value exists at a time.
- `many`: new claims are added alongside existing ones.

The associative layer beneath is indifferent to cardinality; it is the semantic layer that decides what to do with prior claims before asserting new ones.

### Concept

A concept is a named composition of attributes sharing an entity. It describes the shape of a thing in terms of its relations, the primary unit of domain modeling in dialog. An entity matches a concept if and only if it has claims satisfying all the attributes the concept requires.

The name is not part of the concept's identity; two concepts with the same attributes but different names are the same concept. Identity is structural, derived from the sorted set of constituent attributes. However, when a concept is realized into a conclusion, the attribute values can be referenced by the names the concept gave them.

In the formal notation all attributes are inlined with their full form:

```json
{
  "description": "Description of the person",
  "with": {
    "name": {
      "description": "Name of the person",
      "the": "io.gozala.person/name",
      "cardinality": "one",
      "as": "Text"
    },
    "address": {
      "description": "Address of the person",
      "the": "io.gozala.person/address",
      "cardinality": "one",
      "as": "Text"
    }
  }
}
```

```yaml
description: Description of the person
with:
  name:
    description: Name of the person
    the: io.gozala.person/name
    cardinality: one
    as: Text
  address:
    description: Address of the person
    the: io.gozala.person/address
    cardinality: one
    as: Text
```

<details>
<summary>Concept</summary>
<pre>
{
  "Concept": {
    "type": "object",
    "description": "A composition of attributes sharing an entity. An entity matches a concept if and only if it has claims satisfying all required attributes.",
    "properties": {
      "description": {
        "type": "string",
        "description": "Human-readable description of the concept."
      },
      "with": {
        "type": "object",
        "description": "Required fields. An entity must have claims satisfying all these attributes to match.",
        "additionalProperties": {
          "$ref": "#/$defs/Attribute"
        },
        "minProperties": 1
      },
    },
    "required": ["with"]
  }
}
</pre>
</details>

Fields under `with` are required; an entity must have claims satisfying all those attributes to match the concept. The `with` field must include at least one attribute to be considered a valid concept. The name `this` is reserved for referencing the shared entity and must not appear as a field in `with`.

#### Optional attributes

An attribute marked `"optional": true` may or may not have a claim. The entity
still matches the concept as long as every *required* attribute is satisfied,
and the optional value is included in the conclusion when present.

Optionality is carried **per attribute, inside `with`** — there is no separate
block. A concept must still declare at least one required attribute.

```json
{
  "description": "A cooking step",
  "with": {
    "instruction": {
      "description": "What to do in this step",
      "the": "diy.cook.recipe-step/instruction",
      "as": "Text"
    },
    "after": {
      "description": "Step that must be completed before this one",
      "the": "diy.cook.recipe-step/after",
      "as": "Entity",
      "optional": true
    },
    "duration": {
      "description": "Time in minutes this step takes",
      "the": "diy.cook.recipe-step/duration",
      "as": "UnsignedInteger",
      "optional": true
    }
  }
}
```

An entity matches this concept if it has a claim for
`diy.cook.recipe-step/instruction`. Claims for `diy.cook.recipe-step/after` and
`diy.cook.recipe-step/duration` are included when present but are not required
for the entity to match.

An optional attribute is **set-widened** rather than filtered: a missing claim
yields a row with the field bound to *absent*, instead of dropping the row. So
the field's type admits absence — the `option` member of a variable's type set
(see [Constraining a variable](#constraining-a-variable)) — and a consuming
rule sees that an absent binding can arrive through this boundary.

Marking an attribute optional changes the concept's identity: a required
attribute hashes as an empty map, an optional one as `{"optional": true}`.

#### Keyed collections

A field may select *every entry of a domain* rather than one attribute. Its
`the` names a domain and which half of it (see [Name](#name)): the
symbol-named half is a **dictionary**, the position-named half a
**sequence**. `as` is the type of each entry's value.

```json
{
  "with": {
    "title": { "the": "todo.list/title", "as": "Text" },
    "member": {
      "description": "The list's members, in order",
      "the": { "domain": "todo.list", "keyed": "sequence" },
      "cardinality": "many",
      "as": "Text"
    }
  }
}
```

The facts behind such a field are ordinary claims whose attribute's name half
is the entry's key: `todo.list/N` and `todo.list/N5` are two members. A
collection field is therefore *many facts* by construction; `cardinality` is
per entry (whether one key may hold two values), not about the collection.

A collection field cannot be optional: it is zero-or-more already, and an
entity with no entries simply yields no rows.

Because a dictionary takes every symbol-named fact in its domain, a dictionary
field's domain must be its own — a scalar attribute declared in the same
domain would read as one more entry. A sequence shares a domain with scalars
safely, since positions and symbols are disjoint by first byte.

**Querying.** Each matched entry is one row binding the field to the entry's
value and the field's **key** to the entry's key, the name half as text (`N5`,
`title`). In a `where`, a collection field is bound as an **entry**, a mini
fact in the slots an attribute query uses: `the` for the key, `is` for the
value.

```json
{ "member": { "the": { "?": { "name": "key" } }, "is": { "?": { "name": "member" } } } }
```

A constant `the` selects one entry (`{"the": "N5", "is": …}`); a bare term
under the field (`"member": {"?": {"name": "m"}}`) binds every entry with the
key unconstrained. The key joins, filters, and feeds formulas like any other
term; for a sequence it is what `dialog/position` reads to derive a neighbour's
position. In a conclusion the pair is the field's operand and its key operand,
`<field>/key`, which the entry form spells on the wire.

Internally the concept's rule scans the domain with the attribute slot refined
by `domain` and `name.case`, and projects the key with `dialog/attribute-parts`
(`of` → `domain`, `name`).

### Deductive Rules

An advanced form of composition that goes beyond stitching attributes together. Rules can impose additional constraints, compute derived values using formulas, and follow transitive paths across relations. A rule's body is a set of premises; its conclusion is a concept instance. Rules are resolved at query time by the semantic layer.

<details>
<summary>Rule</summary>
<pre>
{
  "Rule": {
    "type": "object",
    "description": "An advanced composition: premises are matched against claims, and when all are satisfied the conclusion (a concept instance) is derived.",
    "properties": {
      "description": {
        "type": "string",
        "description": "Human-readable description of the rule."
      },
      "deduce": {
        "$ref": "#/$defs/Concept",
        "description": "The conclusion: a concept instance the rule derives when its body is satisfied."
      },
      "when": {
        "type": "array",
        "description": "Conjunction of premises. All must be satisfied by the same variable bindings.",
        "items": { "$ref": "#/$defs/Premise" },
        "minItems": 1
      },
      "unless": {
        "type": "array",
        "description": "Exclusion patterns. If any can be satisfied, the result is filtered out (negation as failure).",
        "items": { "$ref": "#/$defs/Premise" }
      }
    },
    "required": ["deduce", "when"]
  },
  "Premise": {
    "type": "object",
    "description": "A single premise in a rule body. Combines an assertion (what to match) with named term bindings (how to bind variables).",
    "properties": {
      "assert": {
        "description": "What to match: a concept (inline definition), a formula reference, or a constraint reference.",
        "oneOf": [
          { "$ref": "#/$defs/Concept" },
          { "$ref": "#/$defs/FormulaRef" },
          { "$ref": "#/$defs/ConstraintRef" }
        ]
      },
      "where": {
        "type": "object",
        "description": "Named terms mapping field names to variables or constants. For concepts, names correspond to the concept's attribute names. For formulas and constraints, names correspond to their parameter names.",
        "additionalProperties": { "$ref": "#/$defs/Term" }
      }
    },
    "required": ["assert", "where"]
  },
  "Term": {
    "description": "A term is either a variable or a constant value.",
    "oneOf": [
      { "$ref": "#/$defs/Variable" },
      { "$ref": "#/$defs/Constant" }
    ]
  },
  "Variable": {
    "type": "object",
    "description": "A query variable. Variables are bound by the query engine. The same variable in multiple positions requires unification.",
    "properties": {
      "?": {
        "type": "object",
        "properties": {
          "name": {
            "type": "string",
            "description": "Variable name. When omitted, acts as a blank (wildcard) that matches any value without binding."
          }
        }
      }
    },
    "required": ["?"]
  },
  "Constant": {
    "description": "A concrete value: string, number, or boolean.",
    "oneOf": [
      { "type": "string" },
      { "type": "number" },
      { "type": "integer" },
      { "type": "boolean" }
    ]
  }
}
</pre>
</details>

#### Variables

A variable represents a value to be bound by the query engine. A variable appearing in multiple positions within the same rule requires those positions to have equal values (unification).

In the formal notation, a named variable is `{ "?": { "name": "x" } }` and a blank (wildcard) that matches any value without binding it `{ "?": {} }`:

```json
{ "?": { "name": "person" } }
{ "?": {} }
```

In the abbreviated notation, `?person` is shorthand for `{ "?": { "name": "person" } }` and `_` is shorthand for `{ "?": {} }`.

##### Constraining a variable

A variable may carry a `where` record narrowing what it admits. Every slot is
optional and all present slots are conjoined; a variable with no `where` is
unconstrained.

```json
{ "?": { "name": "count", "where": { "type": { "uint": {} }, ">=": 1 } } }
```

Two container conventions run through the format, and they are not
interchangeable:

- An **object is a union** where its entries are alternatives of one kind. The
  `type` set is the case: a value matching any present key is admitted, and
  intersecting two sets keeps the keys they share.
- An **array is an intersection** where its entries are independent
  obligations. Conformance (`as`) is the case: every listed concept must be
  satisfied.
- A **record of fixed named slots** is neither. `where` itself is one: each
  slot appears at most once, and two constraints on one slot merge rather than
  accumulate.

**`type`** — the set of admissible types. Ten members, each mapping to a value
type except `option`, which is the row-level absence atom: present alongside
others it marks the variable optional.

```json
{ "type": { "text": {} } }
{ "type": { "text": {}, "symbol": {} } }
{ "type": { "text": {}, "option": {} } }
```

| key | admits |
| --- | --- |
| `bytes` | a byte buffer |
| `entity` | an entity reference |
| `boolean` | a boolean |
| `text` | a UTF-8 string |
| `uint` | an unsigned integer |
| `int` | a signed integer |
| `float` | a floating-point number |
| `record` | structured data, opaque to the query layer |
| `symbol` | a symbol — the type attributes carry |
| `option` | absence (an optional value) |

Each value is a per-variant parameter record, empty today; it is where a
future parameter such as an integer width would go.

**`domain`** and **`name`** — constrain the two halves of an attribute
structurally. Symbol-typed values only.

```json
{ "domain": { "is": "todo.list" }, "name": { "case": "position" } }
```

The domain is written **without** a trailing slash; the separator is an
encoding detail supplied on the way in. `name.case` is `position` or `symbol`
(see [Name](#name)); omitting it admits either half.

**`starts-with`** — a lexical prefix. Applies to types with a lexical form
(text, symbol, entity). For attributes prefer `domain`/`name`, which say the
same thing structurally and cannot be written in a way that silently degrades
the scan.

**`as`** — concepts the value's entity must conform to. An array, and so an
intersection: all listed concepts must be satisfied. Entity-typed values only.
Enforced structurally — the target concept's premises are conjoined into the
query — rather than as a per-row test.

```json
{ "as": [{ "concept:bafy...": {} }] }
```

**`>=`, `>`, `<=`, `<`** — order bounds. Comparable types only.

```json
{ "type": { "uint": {} }, ">=": 1, "<": 100 }
```

Every slot other than `type` narrows the admissible type set as a side effect:
a prefix implies a textual type, conformance implies `entity`, an order bound
implies a comparable type. A constraint whose narrowing would empty the set is
rejected rather than silently yielding a variable that matches nothing.

The variable `this` (`?this` in abbreviated notation) is implicit in every rule and refers to the entity of the asserted concept. It must not be declared in the concept's `with` (because it is not an attribute); it must be used in the `when` premises to bind the entity of the conclusion.

#### Conjunction

A concept definition is effectively a rule with an implied conjunction. Every pattern in the `when` body must be satisfied by the same variable bindings for the rule to produce a result.

```json
{
  "deduce": {
    "description": "An ingredient",
    "with": {
      "name": {
        "description": "Ingredient name",
        "the": "diy.cook/ingredient-name",
        "as": "Text"
      },
      "quantity": {
        "description": "Amount needed",
        "the": "diy.cook/quantity",
        "as": "UnsignedInteger"
      },
      "unit": {
        "description": "Unit of measurement",
        "the": "diy.cook/unit",
        "as": "Text"
      }
    }
  },
  "when": [
    {
      "assert": {
        "with": {
          "name": { "the": "diy.cook/ingredient-name", "as": "Text" }
        }
      },
      "where": {
        "this": { "?": { "name": "this" } },
        "name": { "?": { "name": "name" } }
      }
    },
    {
      "assert": {
        "with": {
          "quantity": { "the": "diy.cook/quantity", "as": "UnsignedInteger" }
        }
      },
      "where": {
        "this": { "?": { "name": "this" } },
        "quantity": { "?": { "name": "quantity" } }
      }
    },
    {
      "assert": {
        "with": {
          "unit": { "the": "diy.cook/unit", "as": "Text" }
        }
      },
      "where": {
        "this": { "?": { "name": "this" } },
        "unit": { "?": { "name": "unit" } }
      }
    }
  ]
}
```

#### Disjunction

Disjunction is expressed by defining multiple rules that deduce the same concept. Any rule can produce a match independently.

```json
{
  "deduce": {
    "description": "An employee",
    "with": {
      "name": {
        "description": "Employee name",
        "the": "org.employee/name",
        "as": "Text"
      },
      "role": {
        "description": "Employee role",
        "the": "org.employee/role",
        "as": "Text"
      }
    }
  },
  "when": [
    {
      "assert": {
        "with": {
          "name": { "the": "org/name", "as": "Text" },
          "title": { "the": "org/title", "as": "Text" }
        }
      },
      "where": {
        "name": { "?": { "name": "name" } },
        "title": { "?": { "name": "role" } }
      }
    }
  ]
}
```

```json
{
  "deduce": {
    "description": "An employee",
    "with": {
      "name": {
        "description": "Employee name",
        "the": "org.employee/name",
        "as": "Text"
      },
      "role": {
        "description": "Employee role",
        "the": "org.employee/role",
        "as": "Text"
      }
    }
  },
  "when": [
    {
      "assert": {
        "with": {
          "name": { "the": "org/name", "as": "Text" },
          "position": { "the": "org/position", "as": "Text" }
        }
      },
      "where": {
        "name": { "?": { "name": "name" } },
        "position": { "?": { "name": "role" } }
      }
    }
  ]
}
```

Because disjunction is expressed by separate rules, a new rule deriving an existing concept can be added from a different domain without touching the original definitions.

#### Negation

`unless` filters out matches where a given pattern holds:

```json
{
  "deduce": {
    "description": "A safe meal",
    "with": {
      "attendee": {
        "description": "Person attending the meal",
        "the": "diy.planner.safe-meal/attendee",
        "as": "Entity"
      },
      "recipe": {
        "description": "Recipe for the meal",
        "the": "diy.planner.safe-meal/recipe",
        "as": "Entity"
      },
      "occasion": {
        "description": "The occasion",
        "the": "diy.planner.safe-meal/occasion",
        "as": "Entity"
      }
    }
  },
  "when": [
    {
      "assert": {
        "with": {
          "attendee": { "the": "diy.planner/attendee", "as": "Entity" },
          "recipe": { "the": "diy.planner/recipe", "as": "Entity" },
          "occasion": { "the": "diy.planner/occasion", "as": "Entity" }
        }
      },
      "where": {
        "attendee": { "?": { "name": "person" } },
        "recipe": { "?": { "name": "recipe" } },
        "occasion": { "?": { "name": "occasion" } }
      }
    }
  ],
  "unless": [
    {
      "assert": {
        "with": {
          "person": { "the": "diy.planner/person", "as": "Entity" },
          "recipe": { "the": "diy.planner/recipe", "as": "Entity" }
        }
      },
      "where": {
        "person": { "?": { "name": "person" } },
        "recipe": { "?": { "name": "recipe" } }
      }
    }
  ]
}
```

If the `unless` pattern can be satisfied, the result is excluded. This reflects the closed-world assumption: if something cannot be derived from what is known, it is treated as absent.

#### Aggregation

A deductive rule may carry a `reduce` clause beside `when` and `unless`. Each
entry names a head field and the fold that defines it; the head stays an
ordinary concept, so a reducing rule's conclusion composes like any other.

```json
{
  "deduce": {
    "with": {
      "dept":  { "the": "org.employee/dept",  "as": "Entity" },
      "total": { "the": "org.employee/total", "as": "UnsignedInteger" }
    }
  },
  "when": [
    {
      "assert": {
        "with": {
          "dept":   { "the": "org.employee/dept",   "as": "Entity" },
          "salary": { "the": "org.employee/salary", "as": "UnsignedInteger" }
        }
      },
      "where": {
        "dept":   { "?": { "name": "dept" } },
        "salary": { "?": { "name": "salary" } }
      }
    }
  ],
  "reduce": {
    "total": { "apply": "sum", "of": { "?": { "name": "salary" } } }
  }
}
```

Evaluation is a pipeline: evaluate `when`/`unless` as usual, group the
resulting rows by the head fields that are **not** reduced, fold each group,
and emit one row per group.

The grouping set is **derived**, never declared — it is exactly the head fields
absent from `reduce`. That is what makes the classic aggregation hazard
unwriteable: a field cannot be both grouped and reduced, because a key is
either present in `reduce` or it isn't. In this example `dept` groups because
`reduce` does not mention it, and `total` is folded because it does.

**Aggregators**

| `apply` | folds to |
| --- | --- |
| `count` | number of present bindings |
| `count-distinct` | number of distinct present values |
| `sum` | sum of the group's values; identity `0` |
| `min` | least present value |
| `max` | greatest present value |
| `avg` | mean of the present numeric values |

**Absent inputs are skipped**, SQL-NULL style: a fold consumes only present
bindings, and `count` counts present bindings. Coalesce first if you want
other behaviour.

Whether a reduced field may be required depends on its fold having an
identity. `count` and `sum` always produce a value, so their head field can be
required. `min`, `max`, and `avg` have no identity, so over an input that
admits absence they may produce nothing — and their head field must then be
declared optional.

`min` and `max` require comparable values; `sum` and `avg` require numeric
ones. Mixing the integer band with floats inside one group is an error rather
than a silent promotion.

**Stratification.** A fold reads a complete relation, so every concept premise
of a reducing rule contributes an aggregating edge to the dependency graph,
treated like negation: aggregation through recursion is rejected, exactly as
negation through recursion is.

#### Constraints

Constraints restrict variable bindings within a rule body.

##### Equality

An equality constraint asserts that two terms must hold equal values. It can filter (both bound), infer (one bound, one free), or fail (neither bound).

```json
{
  "when": [
    {
      "assert": {
        "with": {
          "name": { "the": "org.employee/name", "as": "Text" }
        }
      },
      "where": {
        "this": { "?": { "name": "person" } },
        "name": { "?": { "name": "name" } }
      }
    },
    {
      "assert": "==",
      "where": {
        "this": { "?": { "name": "name" } },
        "is": "Alice"
      }
    }
  ]
}
```

<details>
<summary>EqualityConstraint</summary>
<pre>
{
  "==": {
    "type": "object",
    "description": "Asserts two terms must hold equal values. Can filter (both bound), infer (one bound, one free), or fail (neither bound).",
    "properties": {
      "this": { "$ref": "#/$defs/Term", "description": "Left-hand term." },
      "is":   { "$ref": "#/$defs/Term", "description": "Right-hand term." }
    },
    "required": ["this", "is"]
  }
}
</pre>
</details>

##### Range

Range constraints — `<`, `<=`, `>`, `>=` — assert that the `of` term stands in the relation to the `with` term. They order values of the comparable types: numbers, text, symbols, entities, and bytes. Both sides must hold values (the planner orders the constraint after the premises binding them); a row whose sides cannot be ordered — a non-comparable value, a differently-typed pair, a NaN — is a non-match, never an error.

Within numbers, a *constant* side is a polymorphic literal that adapts losslessly to the data's type per row: `1` compares against float data as `1.0`, while `1.5` against integer data matches nothing (no integer is `1.5`). Data is never adapted, and non-numeric literals never adapt — a text bound orders only against text values.

Like Datomic's range predicates, these take direct advantage of the value index: when a variable's type narrows to a single comparable type, the bound becomes an index range and the scan never reads values outside it.

```json
{
  "when": [
    {
      "assert": {
        "with": {
          "name": { "the": "org.employee/name", "as": "Text" }
        }
      },
      "where": {
        "this": { "?": { "name": "person" } },
        "name": { "?": { "name": "name" } }
      }
    },
    {
      "assert": ">=",
      "where": {
        "of": { "?": { "name": "name" } },
        "with": "Q"
      }
    }
  ]
}
```

<details>
<summary>RangeConstraint</summary>
<pre>
{
  "<": {
    "type": "object",
    "description": "Asserts `of` is strictly less than `with`, over the comparable types.",
    "properties": {
      "of":   { "$ref": "#/$defs/Term", "description": "Left-hand term." },
      "with": { "$ref": "#/$defs/Term", "description": "Right-hand term." }
    },
    "required": ["of", "with"]
  },
  "<=": {
    "type": "object",
    "description": "Asserts `of` is less than or equal to `with`, over the comparable types.",
    "properties": {
      "of":   { "$ref": "#/$defs/Term", "description": "Left-hand term." },
      "with": { "$ref": "#/$defs/Term", "description": "Right-hand term." }
    },
    "required": ["of", "with"]
  },
  ">": {
    "type": "object",
    "description": "Asserts `of` is strictly greater than `with`, over the comparable types.",
    "properties": {
      "of":   { "$ref": "#/$defs/Term", "description": "Left-hand term." },
      "with": { "$ref": "#/$defs/Term", "description": "Right-hand term." }
    },
    "required": ["of", "with"]
  },
  ">=": {
    "type": "object",
    "description": "Asserts `of` is greater than or equal to `with`, over the comparable types.",
    "properties": {
      "of":   { "$ref": "#/$defs/Term", "description": "Left-hand term." },
      "with": { "$ref": "#/$defs/Term", "description": "Right-hand term." }
    },
    "required": ["of", "with"]
  }
}
</pre>
</details>

#### Formulas

A pure computation, similar to formulas in a spreadsheet. Given bound input fields, a formula derives output fields. Formulas can be used within rules and queries to compute values, filter matches, or transform data without leaving the query engine.

```json
{
  "when": [
    {
      "assert": {
        "with": {
          "quantity": { "the": "diy.cook/quantity", "as": "UnsignedInteger" }
        }
      },
      "where": {
        "this": { "?": { "name": "entity" } },
        "quantity": { "?": { "name": "int" } }
      }
    },
    {
      "assert": "math/sum",
      "where": {
        "of": { "?": { "name": "int" } },
        "with": 10,
        "is": { "?": { "name": "total" } }
      }
    }
  ]
}
```

##### Math Formulas

**Sum**: Adds two integer values.

```json
{ 
  "assert": "math/sum", 
  "where": { 
    "of": { "?": { "name": "a" } },
    "with": { "?": { "name": "b" } }, 
    "is": { "?": { "name": "result" }
    } 
  } 
}
```

**Difference**: Subtracts the second value from the first (saturating at 0).

```json
{ 
  "assert": "math/difference", 
  "where": { 
    "of": { "?": { "name": "a" } }, 
    "subtract": { "?": { "name": "b" } }, 
    "is": { "?": { "name": "result" } } 
    } 
  } 
}
```

**Product**: Multiplies two integer values.

```json
{ 
  "assert": "math/product", 
  "where": { 
    "of": { "?": { "name": "a" } }, 
    "times": { "?": { "name": "b" } }, 
    "is": { "?": { "name": "result" } } 
    } 
  } 
}
```

**Quotient**: Divides the first value by the second. Produces no result when the divisor is zero.

```json
{ 
  "assert": "math/quotient", 
  "where": { 
    "of": { "?": { "name": "a" } }, 
    "by": { "?": { "name": "b" } }, 
    "is": { "?": { "name": "result" } } 
    } 
  } 
}
```

**Modulo**: Computes the remainder of division. Produces no result when the divisor is zero.

```json
{ 
  "assert": "math/modulo", 
  "where": { 
    "of": { "?": { "name": "a" } }, 
    "by": { "?": { "name": "b" } }, 
    "is": { "?": { "name": "result" } } 
    } 
  } 
}
```

<details>
<summary>MathFormula</summary>
<pre>
{
  "math/sum": {
    "type": "object",
    "description": "Adds two integers: is = of + with.",
    "properties": {
      "of":   { "$ref": "#/$defs/Term", "description": "First operand." },
      "with": { "$ref": "#/$defs/Term", "description": "Second operand." },
      "is":   { "$ref": "#/$defs/Term", "description": "Derived: sum of the two operands." }
    },
    "required": ["of", "with", "is"]
  },
  "math/difference": {
    "type": "object",
    "description": "Subtracts second from first (saturating at 0): is = of - subtract.",
    "properties": {
      "of":       { "$ref": "#/$defs/Term", "description": "Minuend." },
      "subtract": { "$ref": "#/$defs/Term", "description": "Subtrahend." },
      "is":       { "$ref": "#/$defs/Term", "description": "Derived: difference (saturating)." }
    },
    "required": ["of", "subtract", "is"]
  },
  "math/product": {
    "type": "object",
    "description": "Multiplies two integers: is = of * times.",
    "properties": {
      "of":    { "$ref": "#/$defs/Term", "description": "Multiplicand." },
      "times": { "$ref": "#/$defs/Term", "description": "Multiplier." },
      "is":    { "$ref": "#/$defs/Term", "description": "Derived: product." }
    },
    "required": ["of", "times", "is"]
  },
  "math/quotient": {
    "type": "object",
    "description": "Divides first by second: is = of / by. Produces no result when divisor is zero.",
    "properties": {
      "of": { "$ref": "#/$defs/Term", "description": "Dividend." },
      "by": { "$ref": "#/$defs/Term", "description": "Divisor." },
      "is": { "$ref": "#/$defs/Term", "description": "Derived: quotient (empty if by = 0)." }
    },
    "required": ["of", "by", "is"]
  },
  "math/modulo": {
    "type": "object",
    "description": "Remainder of division: is = of % by. Produces no result when divisor is zero.",
    "properties": {
      "of": { "$ref": "#/$defs/Term", "description": "Dividend." },
      "by": { "$ref": "#/$defs/Term", "description": "Divisor." },
      "is": { "$ref": "#/$defs/Term", "description": "Derived: remainder (empty if by = 0)." }
    },
    "required": ["of", "by", "is"]
  }
}
</pre>
</details>

##### Text Formulas

**Concatenate**: Joins two strings.

```json
{ 
  "assert": "text/concatenate", 
  "where": { 
    "first": { "?": { "name": "a" } }, 
    "second": { "?": { "name": "b" } }, 
    "is": { "?": { "name": "result" } } 
  }
}
```

**Length**: Computes the byte length of a string.

```json
{ 
  "assert": "text/length", 
  "where": { 
    "of": { "?": { "name": "text" } }, 
    "is": { "?": { "name": "result" } } 
  }
}
```

**Uppercase**: Converts a string to uppercase.

```json
{ 
  "assert": "text/upper-case",
  "where": { 
    "of": { "?": { "name": "text" } },
    "is": { "?": { "name": "result" } } 
  } 
}
```

**Lowercase**: Converts a string to lowercase.

```json
{ 
  "assert": "text/lower-case",
  "where": { 
    "of": { "?": { "name": "text" } },
    "is": { "?": { "name": "result" } } 
  } 
}
```

**Like**: Matches a string against a glob pattern. Produces a result only when the pattern matches.

- `*` matches any sequence of characters
- `?` matches any single character
- `\` escapes special characters

```json
{ 
  "assert": "text/like",
  "where": { 
    "text": { "?": { "name": "input" } },
    "pattern": "*@*.*",
    "is": { "?": { "name": "matched" } } 
  } 
}
```

<details>
<summary>TextFormula</summary>
<pre>
{
  "text/concatenate": {
    "type": "object",
    "description": "Joins two strings: is = first ++ second.",
    "properties": {
      "first":  { "$ref": "#/$defs/Term", "description": "First string." },
      "second": { "$ref": "#/$defs/Term", "description": "Second string." },
      "is":     { "$ref": "#/$defs/Term", "description": "Derived: concatenation." }
    },
    "required": ["first", "second", "is"]
  },
  "text/length": {
    "type": "object",
    "description": "Byte length of a string.",
    "properties": {
      "of": { "$ref": "#/$defs/Term", "description": "String to measure." },
      "is": { "$ref": "#/$defs/Term", "description": "Derived: byte length as integer." }
    },
    "required": ["of", "is"]
  },
  "text/upper-case": {
    "type": "object",
    "description": "Converts a string to uppercase.",
    "properties": {
      "of": { "$ref": "#/$defs/Term", "description": "String to convert." },
      "is": { "$ref": "#/$defs/Term", "description": "Derived: uppercased string." }
    },
    "required": ["of", "is"]
  },
  "text/lower-case": {
    "type": "object",
    "description": "Converts a string to lowercase.",
    "properties": {
      "of": { "$ref": "#/$defs/Term", "description": "String to convert." },
      "is": { "$ref": "#/$defs/Term", "description": "Derived: lowercased string." }
    },
    "required": ["of", "is"]
  },
  "text/like": {
    "type": "object",
    "description": "Glob pattern match. '*' matches any sequence, '?' matches a single character.",
    "properties": {
      "text":    { "$ref": "#/$defs/Term", "description": "Text to match." },
      "pattern": { "$ref": "#/$defs/Term", "description": "Glob pattern." },
      "is":      { "$ref": "#/$defs/Term", "description": "Derived: the matched text (empty if no match)." }
    },
    "required": ["text", "pattern", "is"]
  }
}
</pre>
</details>


##### Logic Formulas

**And**: Logical AND of two booleans.

```json
{ 
  "assert": "boolean/and", 
  "where": { "left": { "?": { "name": "a" } }, "right": { "?": { "name": "b" } }, "is": { "?": { "name": "result" } } } }
```

**Or**: Logical OR of two booleans.

```json
{ "assert": "boolean/or", 
  "where": { 
    "left": { "?": { "name": "a" } }, 
    "right": { "?": { "name": "b" } },
    "is": { "?": { "name": "result" } } 
  } 
}
```

**Not**: Logical NOT of a boolean.

```json
{ 
  "assert": "boolean/not", 
  "where": { 
    "value": { "?": { "name": "a" } },
    "is": { "?": { "name": "result" } } 
  } 
}
```

<details>
<summary>LogicFormula</summary>
<pre>
{
  "boolean/and": {
    "type": "object",
    "description": "Logical AND of two booleans.",
    "properties": {
      "left":  { "$ref": "#/$defs/Term", "description": "First boolean." },
      "right": { "$ref": "#/$defs/Term", "description": "Second boolean." },
      "is":    { "$ref": "#/$defs/Term", "description": "Derived: left AND right." }
    },
    "required": ["left", "right", "is"]
  },
  "boolean/or": {
    "type": "object",
    "description": "Logical OR of two booleans.",
    "properties": {
      "left":  { "$ref": "#/$defs/Term", "description": "First boolean." },
      "right": { "$ref": "#/$defs/Term", "description": "Second boolean." },
      "is":    { "$ref": "#/$defs/Term", "description": "Derived: left OR right." }
    },
    "required": ["left", "right", "is"]
  },
  "boolean/not": {
    "type": "object",
    "description": "Logical NOT of a boolean.",
    "properties": {
      "value": { "$ref": "#/$defs/Term", "description": "Boolean to negate." },
      "is":    { "$ref": "#/$defs/Term", "description": "Derived: NOT value." }
    },
    "required": ["value", "is"]
  }
}
</pre>
</details>

### Assertions and Claims

Tools interact with the associative layer by submitting **assertions** and **retractions**. An assertion proposes that a relation holds; a retraction proposes that it no longer does. Once the transactor incorporates an assertion, it becomes a **claim**, the fundamental unit of information stored in the associative layer.

An assertion specifies a relation (`the`), an entity (`of`), and a value (`is`):

```yaml
assert!:
  the: diy.cook/quantity
  of:  did:key:zCarrot
  is:  2
```

Assertions can be made without defining attributes in advance. The associative layer simply accretes; it does not validate, enforce, or interpret.

An assertion may carry an optional `cause` field: a causal reference to the provenance of a prior claim this assertion intends to succeed. When `cause` is absent, no succession is intended and the assertion is additive. When present, the transactor resolves succession based on the existing claims for the same entity-attribute pair.

```yaml
assert!:
  the:   issue/assignee
  of:    did:key:zIssue42
  is:    did:key:zDana
  cause:
    by: did:key:zHome,
    period: 3,
    moment: 9
```

Once incorporated by the transactor, a claim records the full provenance of its production:

```yaml
the: issue/assignee
of:  did:key:zIssue42
is:  did:key:zDana
cause:
  by: did:key:zWork
  period: 4
  moment: 1
```

The `cause` on a claim captures when and where it was produced: `by` identifies the producing authority, `period` reflects the last synchronization cycle, and `moment` captures local ordering within that period. Together they establish a partial order across the distributed system.

<details>
<summary>Claim, Provenance</summary>
<pre>
{
  "Claim": {
    "type": "object",
    "description": "A claim in the associative layer. An assertion that has been incorporated by the transactor. Composed of a relation (the), an entity (of), a value (is), and provenance (cause).",
    "properties": {
      "the": {
        "type": "string",
        "description": "The relation, in domain/name format (e.g. 'diy.cook/quantity')."
      },
      "of": {
        "type": "string",
        "description": "The entity this claim is about."
      },
      "is": {
        "description": "The value being linked to the entity through this relation.",
        "oneOf": [
          { "type": "string" },
          { "type": "number" },
          { "type": "integer" },
          { "type": "boolean" }
        ]
      },
      "cause": {
        "$ref": "#/$defs/Provenance",
        "description": "Provenance describing who produced this claim and when."
      }
    },
    "required": ["the", "of", "is", "cause"]
  },
  "Provenance": {
    "type": "object",
    "description": "Provenance of a claim, capturing when and where it was produced. Establishes partial order across a distributed system.",
    "properties": {
      "by": {
        "type": "string",
        "description": "DID of the operator or session authority that produced the claim."
      },
      "period": {
        "type": "integer",
        "minimum": 0,
        "description": "Coordinated time component: last synchronization cycle."
      },
      "moment": {
        "type": "integer",
        "minimum": 0,
        "description": "Uncoordinated local time component: moment within a period."
      }
    },
    "required": ["by", "period", "moment"]
  }
}
</pre>
</details>

## Abbreviated notation

The abbreviated notation is a YAML-only shorthand that expands into the formal notation. It infers details from the enclosing context and introduces an addressing scheme for referencing attributes and concepts without inlining their full definitions.

### Addressing

Since `domain/name` is usually unique enough to identify an attribute in a single application context it serves as a practical shorthand.

> ℹ️ It is highly unlikely to have several attributes for same relation, but with different types or cardinality.

All abbreviated addresses expand to structural reference in the formal notation.

#### Implicit addressing

The **label** under which an attribute is defined implies its name; the **enclosing key** implies its domain:

```yaml
diy.cook:
  quantity:
    description: Amount needed
    as: UnsignedInteger
```

Expands to:

```yaml
description: Amount needed
the: diy.cook/quantity
cardinality: one
as: UnsignedInteger
```

The label `quantity` becomes the name, the enclosing key `diy.cook` becomes the domain, and `cardinality` defaults to `one`.

#### Relative addressing

Relative addressing reduces repetition by making references relative to the context they appear in.

**`.`** same name, same domain. When used as a concept field value, inherits both from the label and enclosing concept domain:

```yaml
diy.cook:
  Ingredient:
    description: An ingredient
    with:
      quantity: .
```

Expands to:

```json
{
  "description": "An ingredient",
  "with": {
    "quantity": {
      "the": "diy.cook.ingredient/quantity"
    }
  }
}
```

The concept label `Ingredient` under `diy.cook` produces the attribute domain `diy.cook.ingredient`. The field name `quantity` becomes the attribute name, giving `diy.cook.ingredient/quantity`.

**`.name`** explicit name, inferred domain:

```yaml
diy.cook:
  Ingredient:
    description: An ingredient
    with:
      name: .ingredient-name
```

Expands to:

```json
{
  "description": "An ingredient",
  "with": {
    "name": {
      "the": "diy.cook.ingredient/ingredient-name"
    }
  }
}
```

The field label is `name` but the attribute name is overridden to `ingredient-name` via `.ingredient-name`.

#### Fully qualified addressing

**`domain/name`** crosses domain boundaries explicitly:

```yaml
diy.cook:
  Ingredient:
    description: An ingredient
    with:
      name: io.gozala.person/name
```

Expands to:

```json
{
  "description": "An ingredient",
  "with": {
    "name": {
      "the": "io.gozala.person/name"
    }
  }
}
```

The field `name` in this concept is backed by an attribute from a completely different domain.

### Attribute

The abbreviated notation infers `the` and `cardinality` from document structure. An immediate name implies attribute name, and enclosing key implies attribute domain. Cardinality when omitted defaults to `one`.

#### Overriding name

Use `the: ./name` to override the inferred attribute name while keeping the domain from context:

```yaml
diy.cook:
  quantity-int:
    the: ./quantity
    description: Quantity as a whole number
    as: UnsignedInteger
```

Expands to:

```yaml
description: Quantity as a whole number
the: diy.cook/quantity
cardinality: one
as: UnsignedInteger
```

The label `quantity-int` is the key used for referencing this definition, but `the` overrides the actual attribute name to `quantity`. This attribute is referenceable as `diy.cook/quantity-int` in the abbreviated notation.

#### Overriding domain

Use `the: domain/.` to override the inferred domain while keeping the name from the label:

```yaml
diy.cook:
  quantity:
    the: io.gozala.person/.
    description: Quantity as a person attribute
    as: UnsignedInteger
```

Expands to:

```yaml
description: Quantity as a person attribute
the: io.gozala.person/quantity
cardinality: one
as: UnsignedInteger
```

The name `quantity` comes from the label, but the domain is overridden to `io.gozala.person`.

#### Concept-typed fields

An entity-valued field can require its target to satisfy a concept, written
with dot-prefix notation:

```yaml
diy.cook:
  ingredient:
    description: An ingredient in a recipe
    as: .Ingredient
```

`.Ingredient` resolves to `diy.cook/Ingredient` within the current domain. The
constraint is enforced structurally: the target concept's premises are
conjoined onto the field, so only entities that actually satisfy it match.

Only entity-valued attributes can conform, and a conforming field cannot also
be optional — the left join over "edge exists AND target conforms" is absence
over a derived predicate, which stratification has to own first.

#### Future attribute extensions

**Not yet supported.** Symbol enumerations use array syntax:

```yaml
diy.cook:
  unit:
    description: The unit of measurement
    as: [:tsp, :mls]
```

`[:tsp, :mls]` means the value must be one of the symbols `diy.cook/tsp` or
`diy.cook/mls`.

### Concept

#### Attribute references

A concept can reference pre-defined attributes by address instead of inlining them:

```yaml
io.gozala.person:
  name:
    description: Name of the person
    as: Text
  address:
    description: Address of the person
    as: Text

io.gozala:
  Person:
    description: Description of the person
    with:
      name: io.gozala.person/name
      address: io.gozala.person/address
```

#### Punning

The same can be expressed more concisely through punning, where `.` references the same-named attribute under the current domain:

```yaml
io.gozala.person:
  name:
    description: Name of the person
    as: Text
  address:
    description: Address of the person
    as: Text

io.gozala:
  Person:
    description: Description of the person
    with:
      name: .
      address: .
```

Expands to:

```json
{
  "description": "Description of the person",
  "with": {
    "name": {
      "description": "Name of the person",
      "the": "io.gozala.person/name",
      "as": "Text"
    },
    "address": {
      "description": "Address of the person",
      "the": "io.gozala.person/address",
      "as": "Text"
    }
  }
}
```

`name: .` expands to `io.gozala.person/name` by inheriting the field name and the concept's domain (`io.gozala/Person` normalizes to `io.gozala.person`).

#### Inline attributes

Attribute definitions can be inlined inside a concept in abbreviated form. The domain is derived by lowercasing the concept label and appending it as an additional segment:

```
diy.cook/RecipeStep  ->  diy.cook.recipe-step/
```

```yaml
io.gozala:
  Person:
    description: Description of the person
    with:
      name:
        description: Name of the person
        as: Text
      address:
        description: Address of the person
        as: Text
```

Expands to:

```json
{
  "description": "Description of the person",
  "with": {
    "name": {
      "description": "Name of the person",
      "the": "io.gozala.person/name",
      "cardinality": "one",
      "as": "Text"
    },
    "address": {
      "description": "Address of the person",
      "the": "io.gozala.person/address",
      "cardinality": "one",
      "as": "Text"
    }
  }
}
```

`name` defined inline inside `io.gozala/Person` lives at `io.gozala.person/name` and can be referenced from anywhere by that path.

#### Optional fields

An optional field carries `optional: true` alongside its other keys, inside
`with`:

```yaml
diy.cook:
  RecipeStep:
    description: A cooking step
    with:
      instruction: .
      after:
        description: Step to perform this after
        as: .RecipeStep
        optional: true
```

### Deductive Rules

In abbreviated notation, rules use the enclosing key structure for naming and domain scoping. Premises in `when` and `unless` use a compact syntax that expands into the formal concept-based premise form.

#### Concept matching

A concept reference in a premise matches entities that satisfy that concept:

```yaml
diy.cook:
  Ingredient:
    deduce:
      Ingredient:
        name: ?name
        quantity: ?quantity
        unit: ?unit
    when:
      - diy.cook/ingredient-name:
          this: ?this
          is: ?name
      - diy.cook/quantity:
          this: ?this
          is: ?quantity
      - diy.cook/unit:
          this: ?this
          is: ?unit
```

Expands to:

```json
{
  "deduce": {
    "description": "An ingredient",
    "with": {
      "name": { "the": "diy.cook/ingredient-name", "as": "Text" },
      "quantity": { "the": "diy.cook/quantity", "as": "UnsignedInteger" },
      "unit": { "the": "diy.cook/unit", "as": "Text" }
    }
  },
  "when": [
    {
      "assert": {
        "with": {
          "is": { "the": "diy.cook/ingredient-name" }
        }
      },
      "where": {
        "this": { "?": { "name": "this" } },
        "is": { "?": { "name": "name" } }
      }
    },
    {
      "assert": {
        "with": {
          "is": { "the": "diy.cook/quantity" }
        }
      },
      "where": {
        "this": { "?": { "name": "this" } },
        "is": { "?": { "name": "quantity" } }
      }
    },
    {
      "assert": {
        "with": {
          "is": { "the": "diy.cook/unit" }
        }
      },
      "where": {
        "this": { "?": { "name": "this" } },
        "is": { "?": { "name": "unit" } }
      }
    }
  ]
}
```

When a premise references a named concept, the concept's fields map to `where` bindings:

```yaml
org.example:
  employee-from-person:
    deduce:
      Employee:
        name: ?name
        role: ?role
    when:
      - org.example/Person:
          name: ?name
          title: ?role
```

Expands to:

```json
{
  "deduce": {
    "with": {
      "name": { "the": "org.example.employee/name" },
      "role": { "the": "org.example.employee/role" }
    }
  },
  "when": [
    {
      "assert": {
        "with": {
          "name": { "the": "org.example.person/name" },
          "title": { "the": "org.example.person/title" }
        }
      },
      "where": {
        "name": { "?": { "name": "name" } },
        "title": { "?": { "name": "role" } }
      }
    }
  ]
}
```

#### Constraints

Constraints restrict variable bindings. The equality constraint `==` asserts that two terms must hold equal values:

```yaml
org.example:
  alice:
    deduce:
      Employee:
        name: ?name
        role: ?role
    when:
      - org.example/Person:
          name: ?name
          title: ?role
      - ==:
          this: ?name
          is: Alice
```

Expands to:

```json
{
  "deduce": {
    "with": {
      "name": { "the": "org.example.employee/name" },
      "role": { "the": "org.example.employee/role" }
    }
  },
  "when": [
    {
      "assert": {
        "with": {
          "name": { "the": "org.example.person/name" },
          "title": { "the": "org.example.person/title" }
        }
      },
      "where": {
        "name": { "?": { "name": "name" } },
        "title": { "?": { "name": "role" } }
      }
    },
    {
      "assert": "==",
      "where": {
        "this": { "?": { "name": "name" } },
        "is": "Alice"
      }
    }
  ]
}
```

The range constraints `<`, `<=`, `>`, `>=` order the `of` term against the `with` term, over the comparable types (numbers, text, symbols, entities, bytes):

```yaml
org.example:
  senior:
    deduce:
      Senior:
        name: ?name
        age: ?age
    when:
      - org.example/Person:
          name: ?name
          age: ?age
      - '>=':
          of: ?age
          with: 65
```

Expands to:

```json
{
  "deduce": {
    "with": {
      "name": { "the": "org.example.senior/name" },
      "age": { "the": "org.example.senior/age" }
    }
  },
  "when": [
    {
      "assert": {
        "with": {
          "name": { "the": "org.example.person/name" },
          "age": { "the": "org.example.person/age" }
        }
      },
      "where": {
        "name": { "?": { "name": "name" } },
        "age": { "?": { "name": "age" } }
      }
    },
    {
      "assert": ">=",
      "where": {
        "of": { "?": { "name": "age" } },
        "with": 65
      }
    }
  ]
}
```

#### Formulas

Formulas compute derived values. They are referenced by name with their parameters as bindings:

```yaml
diy.cook:
  doubled-quantity:
    deduce:
      DoubledQuantity:
        quantity: ?doubled
    when:
      - diy.cook/quantity:
          this: ?this
          is: ?qty
      - math/sum:
          of: ?qty
          with: ?qty
          is: ?doubled
```

Expands to:

```json
{
  "deduce": {
    "with": {
      "quantity": { "the": "diy.cook.doubled-quantity/quantity" }
    }
  },
  "when": [
    {
      "assert": {
        "with": {
          "is": { "the": "diy.cook/quantity" }
        }
      },
      "where": {
        "this": { "?": { "name": "this" } },
        "is": { "?": { "name": "qty" } }
      }
    },
    {
      "assert": "math/sum",
      "where": {
        "of": { "?": { "name": "qty" } },
        "with": { "?": { "name": "qty" } },
        "is": { "?": { "name": "doubled" } }
      }
    }
  ]
}
```

#### Negation

`unless` filters out results where a given pattern can be satisfied:

```yaml
diy.planner:
  safe-meal:
    deduce:
      SafeMeal:
        attendee: ?person
        recipe: ?recipe
        occasion: ?occasion
    when:
      - diy.planner/PlannedMeal:
          attendee: ?person
          recipe: ?recipe
          occasion: ?occasion
    unless:
      - diy.planner/AllergyConflict:
          person: ?person
          recipe: ?recipe
```

Expands to:

```json
{
  "deduce": {
    "with": {
      "attendee": { "the": "diy.planner.safe-meal/attendee" },
      "recipe": { "the": "diy.planner.safe-meal/recipe" },
      "occasion": { "the": "diy.planner.safe-meal/occasion" }
    }
  },
  "when": [
    {
      "assert": {
        "with": {
          "attendee": { "the": "diy.planner.planned-meal/attendee" },
          "recipe": { "the": "diy.planner.planned-meal/recipe" },
          "occasion": { "the": "diy.planner.planned-meal/occasion" }
        }
      },
      "where": {
        "attendee": { "?": { "name": "person" } },
        "recipe": { "?": { "name": "recipe" } },
        "occasion": { "?": { "name": "occasion" } }
      }
    }
  ],
  "unless": [
    {
      "assert": {
        "with": {
          "person": { "the": "diy.planner.allergy-conflict/person" },
          "recipe": { "the": "diy.planner.allergy-conflict/recipe" }
        }
      },
      "where": {
        "person": { "?": { "name": "person" } },
        "recipe": { "?": { "name": "recipe" } }
      }
    }
  ]
}
```

If any attendee has an allergy conflict with a recipe, that meal is excluded from the results.
