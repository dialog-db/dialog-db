# Notation

Dialog uses two notations for describing domain models:

- **Formal notation** is the explicit representation. It can be expressed in either JSON or YAML; both forms correspond one to one. Every field is explicit and every reference is structural. The JSON schema defines the formal notation.

- **Abbreviated notation** is a YAML-only shorthand for human authoring. It introduces an addressing scheme, implicit field inference from document structure, and punning. The abbreviated notation is an intermediate representation that expands into the formal notation.

## Formal notation

### Structural identity

Both attributes and concepts are structurally identified. Their identity is derived from their components, not from a name. However, attributes contain a nominal component (`the`) that captures semantic intent, identifying the relation in `domain/name` format and distinguishing attributes that would otherwise be structurally identical.

An attribute's identity is the tuple `(the, type, cardinality)`. A concept's identity is derived from the sorted set of its constituent attribute identities. Two definitions with the same structure are the same thing, regardless of how they are referred to.

The `the` component within an attribute is nominal: it carries meaning beyond structure. `diy.cook/quantity` and `diy.cook/price` may both be `(*, Integer, one)` structurally, but they are distinct attributes because `the` denotes the kind of relation they form, which is what makes it part of the identity in the first place.

### Domain

A domain is a dot-separated reversed domain name, always normalized to lowercase. Domains group related attributes and colocate them in the index, which complements dialog's query-driven replication: all attributes sharing a domain prefix can be fetched together efficiently, making it practical to replicate a coherent slice of information without pulling unrelated data. The reversed domain notation avoids collisions between independently developed schemas.

**Rules:**

- Dot-separated segments in reverse domain order
- All segments lowercase
- Each segment: 1–63 ASCII alphanumeric characters or hyphens
- Hyphens not allowed at start or end of a segment
- Top-level domain must not start with a digit
- At least two segments required
- Maximum 253 characters total (including dots)

```
diy.cook
io.gozala.person
org.example.hr
```

**Regexp:**

```
^[a-z]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+$
```

### Name

The name component of an attribute uses lowercase kebab-case.

**Rules:**

- Lowercase ASCII letters, digits, and hyphens
- Must start with a letter
- Hyphens not allowed at start or end
- 1–63 characters

```
quantity
ingredient-name
recipe-step
```

**Regexp:**

```
^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$
```

### References

In the formal notation, all references are structural: attributes are described inline by their full definition `{ the, type, cardinality }` and concepts by their full set of constituent attributes. There are no names to look up; everything is self-describing.

A relation is referenced by its qualified form `domain/name` with `/` as separator:

```
diy.cook/quantity
diy.cook/ingredient-name
io.gozala.person/name
```

**Regexp:**

```
^[a-z]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+/[a-z]([a-z0-9-]{0,61}[a-z0-9])?$
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
      "as": {
        "description": "Value type of the attribute. If omitted, any type is allowed.",
        "type": "string",
        "enum": ["Bytes", "Entity", "Boolean", "Text", "Integer", "Float", "Symbol"]
      }
    },
    "required": ["the"]
  }
}
</pre>
</details>

#### Value Types

The `as` field declares what kind of value the attribute admits. Scalar types from the `dialog` domain can be referenced without qualification:

| Type      | Description                 |
|-----------|-----------------------------|
| `Bytes`   | Raw byte sequence           |
| `Entity`  | Reference to another entity |
| `Boolean` | `true` or `false`           |
| `Text`    | UTF-8 string                |
| `Integer` | Signed integer              |
| `Float`   | IEEE 754 floating point     |
| `Symbol`  | Symbolic identifier         |

#### Future Attribute Extensions

An attribute will also be able to reference a concept as its value type, or constrain values to a fixed set of symbols (neither is yet supported):

```json
{
  "description": "Ingredient used in the recipe",
  "the": "diy.cook/ingredient",
  "as": {
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
        "as": "Integer"
      }
    }
  }
}
```

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
      "maybe": {
        "description": "[Future extension, not yet supported] Optional fields. The entity may or may not have these attributes."
      }
    },
    "required": ["with"]
  }
}
</pre>
</details>

Fields under `with` are required; an entity must have claims satisfying all those attributes to match the concept. The `with` field must include at least one attribute to be considered a valid concept. The name `this` is reserved for referencing the shared entity and must not appear as a field in `with`.

#### Optional attributes

> ⚠️ Optional attributes are not currently supported. For now `maybe` field can be used as metadata which is ignored by the query engine.

Fields under `maybe` define attributes that the entity may or may not have related claims for. The entity will still match the concept as long as all required attributes (defined in `with`) are satisfied. Optional attribute values will be included in the conclusion when present.

```json
{
  "description": "A cooking step",
  "with": {
    "instruction": {
      "description": "What to do in this step",
      "the": "diy.cook.recipe-step/instruction",
      "as": "Text"
    }
  },
  "maybe": {
    "after": {
      "description": "Step that must be completed before this one",
      "the": "diy.cook.recipe-step/after",
      "as": "Entity"
    },
    "duration": {
      "description": "Time in minutes this step takes",
      "the": "diy.cook.recipe-step/duration",
      "as": "Integer"
    }
  }
}
```

An entity matches this concept if it has a claim for `diy.cook.recipe-step/instruction`. Claims for `diy.cook.recipe-step/after` and `diy.cook.recipe-step/duration` are included in the conclusion when present but are not required for the entity to match.

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
        "as": "Integer"
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
          "quantity": { "the": "diy.cook/quantity", "as": "Integer" }
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

#### Formulas

A pure computation, similar to formulas in a spreadsheet. Given bound input fields, a formula derives output fields. Formulas can be used within rules and queries to compute values, filter matches, or transform data without leaving the query engine.

```json
{
  "when": [
    {
      "assert": {
        "with": {
          "quantity": { "the": "diy.cook/quantity", "as": "Integer" }
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
    as: Integer
```

Expands to:

```yaml
description: Amount needed
the: diy.cook/quantity
cardinality: one
as: Integer
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
    as: Integer
```

Expands to:

```yaml
description: Quantity as a whole number
the: diy.cook/quantity
cardinality: one
as: Integer
```

The label `quantity-int` is the key used for referencing this definition, but `the` overrides the actual attribute name to `quantity`. This attribute is referenceable as `diy.cook/quantity-int` in the abbreviated notation.

#### Overriding domain

Use `the: domain/.` to override the inferred domain while keeping the name from the label:

```yaml
diy.cook:
  quantity:
    the: io.gozala.person/.
    description: Quantity as a person attribute
    as: Integer
```

Expands to:

```yaml
description: Quantity as a person attribute
the: io.gozala.person/quantity
cardinality: one
as: Integer
```

The name `quantity` comes from the label, but the domain is overridden to `io.gozala.person`.

#### Future attribute extensions

**Not yet supported.** Concept references use dot-prefix notation, and symbol enumerations use array syntax:

```yaml
diy.cook:
  ingredient:
    description: An ingredient in a recipe
    as: .Ingredient
  unit:
    description: The unit of measurement
    as: [:tsp, :mls]
```

`.Ingredient` resolves to `diy.cook/Ingredient` within the current domain. `[:tsp, :mls]` means the value must be one of the symbols `diy.cook/tsp` or `diy.cook/mls`.

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

#### Future concept extensions

**Not yet supported.** Optional fields use the `maybe` key:

```yaml
diy.cook:
  RecipeStep:
    description: A cooking step
    with:
      instruction: .
    maybe:
      after:
        description: Step to perform this after
        as: .RecipeStep
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
      "quantity": { "the": "diy.cook/quantity", "as": "Integer" },
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
