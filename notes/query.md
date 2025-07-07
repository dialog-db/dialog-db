# DialogDB Query Syntax Schema

DialogDB is a database that uses **facts** and **rules** to deduce conclusions. It can be queried using Abstract Syntax Trees (ASTs) in the [IPLD data model](https://ipld.io/docs/data-model/kinds/) and encoded as JSON using [DAG-JSON codec](https://ipld.io/docs/codecs/known/dag-json/).

All knowledge in DialogDB is represented as facts in form of `{the, of, is, cause}` relations:
- **the**: Attribute/predicate
- **of**: Entity/subject
- **is**: Value/object
- **cause**: Causal relationship (timestamp)

## Syntax Forms

### Rule

Rules define how to deduce new relations from known ones. A rule consists of:
- **match**: Variables that the rule exposes (its "head" or interface)
- **when**: Named branches (disjuncts) containing sequences of predicates (conjuncts)

```json
{
  "match": {
    "person": { "?": { "id": 1 } }
  },
  "when": {
    "adult": [
      {
        "match": {
          "the": "cafe.familiar.person/age",
          "of": { "?": { "id": 1 } },
          "is": { "?": { "id": 2 } }
        }
      },
      {
        "operator": ">=",
        "match": {
          "of": { "?": { "id": 2 } },
          "is": 18
        }
      }
    ]
  }
}
```

#### Disjuncts

Within the `when` clause, each named branch (disjunct) contains a sequence of conjuncts that must ALL be satisfied (AND semantics). Each conjunct is one of:

- **Predicate**: Pattern matching or computation
- **Negation**: Exclusion of matches
- **Recursion**: Recursive rule invocation

**Named Branches vs Datalog:**
Unlike traditional Datalog which uses multiple rules with the same head, DialogDB uses **named branches** within a single rule:

**Traditional Datalog:**
```prolog
accessible(Doc, User) :- public(Doc).
accessible(Doc, User) :- owner(Doc, User).
```

**DialogDB:**
```json
{
  "match": {
    "document": { "?": { "id": 1 } },
    "user": { "?": { "id": 2 } }
  },
  "when": {
    "public": [
      {
        "match": {
          "the": "cafe.familiar.document/visibility",
          "of": { "?": { "id": 1 } },
          "is": "public"
        }
      }
    ],
    "owner": [
      {
        "match": {
          "the": "cafe.familiar.document/owner",
          "of": { "?": { "id": 1 } },
          "is": { "?": { "id": 2 } }
        }
      }
    ]
  }
}
```

Each named branch represents a different way to satisfy the rule. The rule succeeds if ANY branch succeeds (OR semantics).

#### Predicate

A Predicate is the basic unit of pattern matching or computation. It can be:

- **Selection**: Pattern matching against facts in the database
- **RuleApplication**: Applying another rule with variable bindings
- **FormulaApplication**: Built-in operators for computation and comparison

#### Negation

Negation excludes matches that satisfy a predicate using the `not` operator. **Negation cannot produce new matches - it can only filter out existing matches.**

```json
{
  "not": {
    "match": {
      "the": "cafe.familiar.user/status",
      "of": { "?": { "id": 1 } },
      "is": "blocked"
    }
  }
}
```

**Key Characteristics:**

1. **Filtering Only**: Negation acts as a filter that removes matches from the result set. It cannot bind variables or generate new solutions.

2. **Execution Order**: Negation predicates are evaluated **after** all positive predicates in the same conjunct. This ensures that all variables are bound before negation is applied.

3. **Variable Binding Requirement**: **All variables inside a negated predicate must be bound by other (positive) predicates in the same disjunct.** If any variable in the negated predicate is unbound, the rule is considered invalid.

**Valid Example:**
```json
{
  "when": {
    "active": [
      // First: bind ?1 with positive predicate
      {
        "match": {
          "the": "cafe.familiar.user/name",
          "of": { "?": { "id": 1 } },
          "is": { "?": { "id": 2 } }
        }
      },
      // Then: use bound ?1 in negation
      {
        "not": {
          "match": {
            "the": "cafe.familiar.user/status",
            "of": { "?": { "id": 1 } },  // ?1 is bound above
            "is": "blocked"
          }
        }
      }
    ]
  }
}
```

**Invalid Example:**
```json
{
  "when": {
    "invalid_rule": [
      {
        "not": {
          "match": {
            "the": "cafe.familiar.user/status",
            "of": { "?": { "id": 1 } },  // ERROR: ?1 is not bound by any positive predicate
            "is": "blocked"
          }
        }
      }
    ]
  }
}
```

**Schema:**
```json
{
  "type": "object",
  "properties": {
    "not": {
      "oneOf": [
        { "$ref": "#/definitions/Selection" },
        { "$ref": "#/definitions/RuleApplication" },
        { "$ref": "#/definitions/FormulaApplication" }
      ]
    }
  },
  "required": ["not"]
}
```

#### Recursion

Recursion enables recursive rule application for transitive relationships:

```json
{
  "recur": {
    "ancestor": { "?": { "id": 1 } },
    "descendant": { "?": { "id": 2 } }
  }
}
```

**Schema:**
```json
{
  "type": "object",
  "properties": {
    "recur": {
      "type": "object",
      "description": "Variable bindings for recursive invocation",
      "additionalProperties": {
        "$ref": "#/definitions/Term"
      }
    }
  },
  "required": ["recur"]
}
```

### Selection

Selection matches relations denoted by facts in the database using pattern matching against the `{the, of, is}` structure:

```json
{
  "match": {
    "of": "person:alice",
    "the": "cafe.familiar.person/age",
    "is": { "?": { "id": 1 } }
  }
}
```

**Schema:**
```json
{
  "type": "object",
  "properties": {
    "match": {
      "type": "object",
      "properties": {
        "of": { "$ref": "#/definitions/Term", "description": "The entity (subject)" },
        "the": { "$ref": "#/definitions/Term", "description": "The attribute (predicate)" },
        "is": { "$ref": "#/definitions/Term", "description": "The value (object)" }
      },
      "anyOf": [
        { "required": ["of"] },
        { "required": ["the"] },
        { "required": ["is"] }
      ]
    }
  },
  "required": ["match"]
}
```

**Examples:**
```json
// Match all facts about a specific entity
{
  "match": {
    "of": { "?": { "id": 1 } },
    "the": { "?": { "id": 2 } },
    "is": { "?": { "id": 3 } }
  }
}

// Match all facts with a specific attribute
{
  "match": {
    "the": "cafe.familiar.person/name",
    "of": { "?": { "id": 1 } },
    "is": { "?": { "id": 2 } }
  }
}

// Match facts with a specific value
{
  "match": {
    "is": "Alice",
    "the": { "?": { "id": 1 } },
    "of": { "?": { "id": 2 } }
  }
}
```

### RuleApplication

RuleApplication applies a rule with specific variable bindings. This is the way to execute queries:

```json
{
  "match": {
    "person": { "?": { "id": 1 } }
  },
  "rule": {
    "match": {
      "person": { "?": { "id": 10 } }
    },
    "when": {
      "adult": [
        {
          "match": {
            "the": "cafe.familiar.person/age",
            "of": { "?": { "id": 10 } },
            "is": { "?": { "id": 11 } }
          }
        },
        {
          "operator": ">=",
          "match": {
            "of": { "?": { "id": 11 } },
            "is": 18
          }
        }
      ]
    }
  }
}
```

**Schema:**
```json
{
  "type": "object",
  "properties": {
    "match": {
      "type": "object",
      "description": "Bindings for rule variables",
      "additionalProperties": { "$ref": "#/definitions/Term" }
    },
    "rule": { "$ref": "#/definitions/Rule" }
  },
  "required": ["match", "rule"]
}
```

**Variable Scoping:**
- Variables are unified within each rule's scope
- Each rule application creates its own scope
- Variables in outer rules are NOT automatically unified with nested rules
- Values pass between scopes through explicit `match` bindings

### FormulaApplication

FormulaApplication applies built-in operators for computation and comparison. Think of formulas as **built-in rules** that come with DialogDB - they're pre-defined rules that don't need to be written by the user.

#### Spreadsheet Analogy

DialogDB variables work like cells in a spreadsheet:

- **Bound variable**: Like a spreadsheet cell that contains a value (e.g., `A1 = "Alice"`)
- **Unbound variable**: Like an empty spreadsheet cell waiting for a value

Just as a spreadsheet formula `=LEN(A1)` requires cell A1 to have a value before it can calculate the length, DialogDB formulas need their input variables to be bound (have values) before they can execute.

| Spreadsheet | DialogDB |
|-------------|----------|
| `A1 = "Alice"` | Variable `?1` bound to `"Alice"` |
| `B1 = LEN(A1)` | Formula with input `?1`, output `?2` |
| Can't calculate `LEN()` of empty cell | Can't execute formula with unbound input |
| Cell coordinates (A1, B1) | Named parameters (`of`, `is`, `with`) |

#### Built-in Rules

Formulas are essentially built-in rules that:
- Take inputs through required parameters (must be bound)
- Produce outputs through optional parameters (can be unbound)
- Execute deterministic computations or comparisons
- Are provided by the system rather than user-defined

#### Variable Binding Requirements

- **Input parameters**: MUST BE BOUND - formulas need actual values to operate on
- **Output parameters**: CAN BE UNBOUND - these capture the formula's results
- **No variable can be both input and output** in the same formula

#### Bidirectional vs Unidirectional Operations

**Bidirectional (Reversible) Operations:**
```json
// Equality: can check if two values match
{
  "operator": "==",
  "match": {
    "of": { "?": { "id": 1 } },    // INPUT: must be bound
    "is": "Alice"                  // INPUT: expected value
  }
}
```

**Unidirectional (Transform) Operations:**
```json
// Text length: text → length works, but length ≠> text
{
  "operator": "text/length",
  "match": {
    "of": "Alice",                 // INPUT: must be bound
    "is": { "?": { "id": 1 } }     // OUTPUT: captures result (5)
  }
}
```

#### Schema

```json
{
  "oneOf": [
    { "$ref": "#/definitions/EqualityFormula" },
    { "$ref": "#/definitions/ComparisonFormula" },
    { "$ref": "#/definitions/TextLengthFormula" },
    { "$ref": "#/definitions/DataTypeFormula" },
    { "$ref": "#/definitions/ArithmeticFormula" },
    { "$ref": "#/definitions/TextConcatFormula" },
    { "$ref": "#/definitions/TextWordsFormula" },
    { "$ref": "#/definitions/TextLikeFormula" }
  ]
}
```

#### EqualityFormula

Compares two values for equality.

**Example:**
```json
{
  "operator": "==",
  "match": {
    "of": { "?": { "id": 1 } },
    "is": "Alice"
  }
}
```

**Schema:**
```json
{
  "type": "object",
  "properties": {
    "operator": { "const": "==" },
    "match": {
      "type": "object",
      "properties": {
        "of": { "$ref": "#/definitions/Term", "description": "INPUT: First value" },
        "is": { "$ref": "#/definitions/Term", "description": "INPUT: Second value" }
      },
      "required": ["of", "is"]
    }
  },
  "required": ["operator", "match"]
}
```

#### ComparisonFormula

Compares two values using relational operators (`>`, `<`, `>=`, `<=`).

**Example:**
```json
{
  "operator": ">",
  "match": {
    "of": { "?": { "id": 1 } },
    "is": 18
  }
}
```

**Schema:**
```json
{
  "type": "object",
  "properties": {
    "operator": { "enum": [">", "<", ">=", "<="] },
    "match": {
      "type": "object",
      "properties": {
        "of": { "$ref": "#/definitions/Term", "description": "INPUT: Value to compare" },
        "is": { "$ref": "#/definitions/Term", "description": "INPUT: Comparison threshold" }
      },
      "required": ["of", "is"]
    }
  },
  "required": ["operator", "match"]
}
```

#### TextLengthFormula

Calculates the length of a text string.

**Example:**
```json
{
  "operator": "text/length",
  "match": {
    "of": "Alice",
    "is": { "?": { "id": 1 } }
  }
}
```

**Schema:**
```json
{
  "type": "object",
  "properties": {
    "operator": { "const": "text/length" },
    "match": {
      "type": "object",
      "properties": {
        "of": { "$ref": "#/definitions/Term", "description": "INPUT: Text to measure" },
        "is": { "$ref": "#/definitions/Term", "description": "OUTPUT: Length result" }
      },
      "required": ["of"]
    }
  },
  "required": ["operator", "match"]
}
```

#### DataTypeFormula

Determines the data type of a value.

**Example:**
```json
{
  "operator": "data/type",
  "match": {
    "of": { "?": { "id": 1 } },
    "is": { "?": { "id": 2 } }
  }
}
```

**Schema:**
```json
{
  "type": "object",
  "properties": {
    "operator": { "const": "data/type" },
    "match": {
      "type": "object",
      "properties": {
        "of": { "$ref": "#/definitions/Term", "description": "INPUT: Value to check" },
        "is": { "$ref": "#/definitions/Term", "description": "OUTPUT: Type name" }
      },
      "required": ["of"]
    }
  },
  "required": ["operator", "match"]
}
```

#### ArithmeticFormula

Performs arithmetic operations (`+`, `-`, `*`, `/`, `%`, `**`).

**Example:**
```json
{
  "operator": "+",
  "match": {
    "of": { "?": { "id": 1 } },
    "with": { "?": { "id": 2 } },
    "is": { "?": { "id": 3 } }
  }
}
```

**Schema:**
```json
{
  "type": "object",
  "properties": {
    "operator": { "enum": ["+", "-", "*", "/", "%", "**"] },
    "match": {
      "type": "object",
      "properties": {
        "of": { "$ref": "#/definitions/Term", "description": "INPUT: First number" },
        "with": { "$ref": "#/definitions/Term", "description": "INPUT: Second number" },
        "is": { "$ref": "#/definitions/Term", "description": "OUTPUT: Result" }
      },
      "required": ["of", "with"]
    }
  },
  "required": ["operator", "match"]
}
```

#### TextConcatFormula

Concatenates two text strings.

**Example:**
```json
{
  "operator": "text/concat",
  "match": {
    "of": "Hello",
    "with": " World",
    "is": { "?": { "id": 1 } }
  }
}
```

**Schema:**
```json
{
  "type": "object",
  "properties": {
    "operator": { "const": "text/concat" },
    "match": {
      "type": "object",
      "properties": {
        "of": { "$ref": "#/definitions/Term", "description": "INPUT: First string" },
        "with": { "$ref": "#/definitions/Term", "description": "INPUT: Second string" },
        "is": { "$ref": "#/definitions/Term", "description": "OUTPUT: Concatenated result" }
      },
      "required": ["of", "with"]
    }
  },
  "required": ["operator", "match"]
}
```

#### TextWordsFormula

Splits text into words.

**Example:**
```json
{
  "operator": "text/words",
  "match": {
    "of": "Hello World",
    "is": { "?": { "id": 1 } }
  }
}
```

**Schema:**
```json
{
  "type": "object",
  "properties": {
    "operator": { "const": "text/words" },
    "match": {
      "type": "object",
      "properties": {
        "of": { "$ref": "#/definitions/Term", "description": "INPUT: Text to split" },
        "is": { "$ref": "#/definitions/Term", "description": "OUTPUT: Array of words" }
      },
      "required": ["of"]
    }
  },
  "required": ["operator", "match"]
}
```

#### TextLikeFormula

Matches text against a pattern.

**Example:**
```json
{
  "operator": "text/like",
  "match": {
    "text": { "?": { "id": 1 } },
    "pattern": "*.txt",
    "is": { "?": { "id": 2 } }
  }
}
```

**Schema:**
```json
{
  "type": "object",
  "properties": {
    "operator": { "const": "text/like" },
    "match": {
      "type": "object",
      "properties": {
        "text": { "$ref": "#/definitions/Term", "description": "INPUT: Text to match" },
        "pattern": { "$ref": "#/definitions/Term", "description": "INPUT: Pattern to match against" },
        "is": { "$ref": "#/definitions/Term", "description": "OUTPUT: Matched text" }
      },
      "required": ["text", "pattern"]
    }
  },
  "required": ["operator", "match"]
}
```

**Available Operators:**
- **Comparison**: `==`, `>`, `<`, `>=`, `<=`
- **Arithmetic**: `+`, `-`, `*`, `/`, `%`, `**`
- **Text**: `text/length`, `text/concat`, `text/words`, `text/like`, `text/case/upper`, `text/case/lower`
- **Data**: `data/type`, `data/refer`
- **UTF-8**: `text/to/utf8`, `utf8/to/text`

## Common Type Definitions

### Type

```json
{
  "description": "Type constraint for variables",
  "oneOf": [
    { "type": "object", "properties": { "Null": { "type": "object", "additionalProperties": false } }, "additionalProperties": false },
    { "type": "object", "properties": { "Boolean": { "type": "object", "additionalProperties": false } }, "additionalProperties": false },
    { "type": "object", "properties": { "Integer": { "type": "object", "additionalProperties": false } }, "additionalProperties": false },
    { "type": "object", "properties": { "Float": { "type": "object", "additionalProperties": false } }, "additionalProperties": false },
    { "type": "object", "properties": { "String": { "type": "object", "additionalProperties": false } }, "additionalProperties": false },
    { "type": "object", "properties": { "Bytes": { "type": "object", "additionalProperties": false } }, "additionalProperties": false },
    { "type": "object", "properties": { "Entity": { "type": "object", "additionalProperties": false } }, "additionalProperties": false },
    { "type": "object", "properties": { "Name": { "type": "object", "additionalProperties": false } }, "additionalProperties": false },
    { "type": "object", "properties": { "Position": { "type": "object", "additionalProperties": false } }, "additionalProperties": false },
    { "type": "object", "properties": { "Reference": { "type": "object", "additionalProperties": false } }, "additionalProperties": false },
  ]
}
```

### Scalar

```json
{
  "oneOf": [
    { "type": "null" },
    { "type": "boolean" },
    { "type": "string" },
    { "type": "number" },
    { "type": "integer" },
    {
      "type": "object",
      "description": "Bytes representation as per DAG-JSON spec",
      "properties": {
        "/": {
          "type": "object",
          "properties": {
            "bytes": {
              "type": "string",
              "description": "Base64 encoded bytes without padding (RFC 4648, section 4)"
            }
          },
          "required": ["bytes"]
        }
      },
      "required": ["/"]
    },
    {
      "type": "object",
      "description": "IPLD Link representation",
      "properties": {
        "/": {
          "type": "string",
          "description": "IPLD Link as CID string"
        }
      },
      "required": ["/"]
    }
  ]
}
```


### Variable

```json
{
  "type": "object",
  "properties": {
    "?": {
      "type": "object",
      "properties": {
        "id": { "type": "integer", "description": "Unique variable identifier" },
        "name": { "type": "string", "description": "Optional human-readable name" },
        "type": { "$ref": "#/definitions/Type", "description": "Optional type constraint" }
      },
      "required": ["id"]
    }
  },
  "required": ["?"]
}
```

### Term
```json
{
  "oneOf": [
    { "$ref": "#/definitions/Variable" },
    { "$ref": "#/definitions/Scalar" }
  ]
}
```


### Conjunct
```json
{
  "oneOf": [
    { "$ref": "#/definitions/Selection" },
    { "$ref": "#/definitions/RuleApplication" },
    { "$ref": "#/definitions/FormulaApplication" },
    { "$ref": "#/definitions/Negation" },
    { "$ref": "#/definitions/Recursion" }
  ]
}
```
