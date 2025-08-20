# DialogDB Query Syntax Schema

DialogDB is a database that consists of persisted and indexed relations known as **facts**, and uses **rules** to deduce new relations from known ones. Queries are expressed as Abstract Syntax Trees (ASTs) in the [IPLD data model](https://ipld.io/docs/data-model/kinds/), encoded as JSON using the [DAG-JSON codec](https://ipld.io/docs/codecs/known/dag-json/).

**Key Points:**
- Facts follow uniform `{the, of, is, cause}` structure
- Queries are rule applications with named arguments
- Attributes use reverse domain notation (e.g., `cafe.familiar.person/age`)
- Entities are URIs (e.g., `person:alice`)

**Hierarchy:**
- **Rule**: Top-level concept defining how to deduce conclusions
  - Contains **Disjuncts**: Named branches (OR logic)
    - Each disjunct contains **Conjuncts**: Predicates that must all succeed (AND logic)
      - **Predicate**: Basic operations (Selection, Formula Application, Rule Application)
      - **Negation**: Excludes matches (wraps other predicate)
      - **Recursion**: Enables recursive rule application

## Syntax Forms

## Rule

Rule is the foundational concept in DialogDB. Rules define how to deduce conclusions from facts and other rules. Unlike traditional Datalog which uses multiple separate rules for different logical paths, DialogDB uses **named disjuncts** within a single rule.

### Schema

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "match": {
      "type": "object",
      "description": "Variables exposed by this rule (the rule's interface)",
      "additionalProperties": {
        "$ref": "#/definitions/Variable"
      }
    },
    "when": {
      "type": "object",
      "description": "Named disjuncts - at least one branch must succeed",
      "additionalProperties": {
        "type": "array",
        "description": "Conjuncts - all predicates must be satisfied for this branch",
        "items": {
          "$ref": "#/definitions/Conjunct"
        }
      }
    }
  },
  "required": ["match"],
  "additionalProperties": false
}
```

### Named Disjuncts vs Multiple Rules

**Traditional Datalog (multiple rules):**
```prolog
% Multiple rules with same head "accessible(Doc, User)"
accessible(Doc, User) :- public(Doc).
accessible(Doc, User) :- owner(Doc, User).
```

**DialogDB (named disjuncts):**
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

The rule succeeds if **ANY** of the named branches succeeds (OR semantics). Branch names (`"public"`, `"owner"`) are for human convenience and debugging.

### Variables

Variables are placeholders with unique IDs that unify within rule scopes. They represent values that can be bound through database queries, formula operations, or explicit bindings.

#### Schema

```json
{
  "type": "object",
  "properties": {
    "?": {
      "type": "object",
      "properties": {
        "id": {
          "type": "number",
          "description": "Unique identifier for this variable"
        },
        "name": {
          "type": "string",
          "description": "Optional human-readable name for this variable"
        },
        "type": {
          "$ref": "#/definitions/Type",
          "description": "Optional type predicate"
        }
      },
      "required": ["id"]
    }
  },
  "required": ["?"]
}
```

#### Examples

```json
// Simple variable with just an ID
{ "?": { "id": 1 } }

// Variable with optional name for clarity
{ "?": { "id": 2, "name": "person" } }

// Variable with type constraint
{ "?": { "id": 3, "type": "string" } }
```

### Terms

Terms are the basic building blocks that can appear in patterns. They are either scalar values (constants) or variables (placeholders).

#### Schema

```json
{
  "oneOf": [
    { "$ref": "#/definitions/Scalar" },
    { "$ref": "#/definitions/Variable" }
  ]
}
```

#### Examples

```json
// Scalar values (constants)
"Alice"                    // String
42                        // Number
true                      // Boolean
{ "/": { "bytes": "SGVsbG8gV29ybGQ" } }  // Bytes (DAG-JSON)

// Variables (placeholders)
{ "?": { "id": 1 } }      // Variable reference
```

### Predicate

Predicates are the basic operations that can appear in rule bodies. They either match facts (Selection) or apply built-in operators (Formula Application).

#### Schema

```json
{
  "oneOf": [
    { "$ref": "#/definitions/SelectionForm" },
    { "$ref": "#/definitions/FormulaApplication" }
  ]
}
```

### Negation

Negation wraps other conjuncts to exclude matches that satisfy the inner predicate.

#### Schema

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "not": {
      "$ref": "#/definitions/Conjunct"
    }
  },
  "required": ["not"],
  "additionalProperties": false
}
```

#### Examples

```json
// Exclude entities with a specific attribute value
{
  "not": {
    "match": {
      "the": "cafe.familiar.user/status",
      "of": { "?": { "id": 1 } },
      "is": "blocked"
    }
  }
}

// Exclude based on formula result
{
  "not": {
    "operator": ">",
    "match": {
      "of": { "?": { "id": 1 } },
      "is": 100
    }
  }
}
```

### Recursion

Recursion enables recursive rule evaluation for transitive relationships.

#### Schema

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
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
  "required": ["recur"],
  "additionalProperties": false
}
```

#### Examples

```json
// Recursive call with variable bindings
{
  "recur": {
    "ancestor": { "?": { "id": 1 } },
    "descendant": { "?": { "id": 2 } }
  }
}
```

## Selection

Selection matches facts in the database by pattern matching against the uniform `(The, Of, Is, Cause)` structure.

### Schema

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "match": {
      "type": "object",
      "properties": {
        "of": {
          "$ref": "#/definitions/Term",
          "description": "The entity (subject) to match"
        },
        "the": {
          "$ref": "#/definitions/Term",
          "description": "The attribute (predicate) to match"
        },
        "is": {
          "$ref": "#/definitions/Term",
          "description": "The value (object) to match"
        }
      },
      "anyOf": [
        { "required": ["of"] },
        { "required": ["the"] },
        { "required": ["is"] }
      ]
    },
    "fact": {
      "type": "object",
      "description": "Optional fact configuration (reserved for future extensions)",
      "additionalProperties": false
    }
  },
  "required": ["match"],
  "additionalProperties": false
}
```

### Examples

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

## Rule Application

Rule Application is how you execute queries. It applies a rule with specific variable bindings.

### Schema

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "match": {
      "type": "object",
      "description": "Bindings for rule variables - maps rule variables to terms",
      "additionalProperties": {
        "$ref": "#/definitions/Term"
      }
    },
    "rule": {
      "$ref": "#/definitions/Rule"
    }
  },
  "required": ["match", "rule"],
  "additionalProperties": false
}
```

### Examples

```json
// Simple query using a rule
{
  "match": {
    "person": { "?": { "id": 1 } },
    "name": { "?": { "id": 2 } }
  },
  "rule": {
    "match": {
      "person": { "?": { "id": 10 } },
      "name": { "?": { "id": 11 } }
    },
    "when": {
      "where": [
        {
          "match": {
            "the": "cafe.familiar.person/name",
            "of": { "?": { "id": 10 } },
            "is": { "?": { "id": 11 } }
          }
        }
      ]
    }
  }
}

// Complex rule showing the full hierarchy
{
  "match": {
    "user": { "?": { "id": 1 } },
    "access": { "?": { "id": 2 } }
  },
  "rule": {
    "match": {
      "user": { "?": { "id": 10 } },
      "access": { "?": { "id": 11 } }
    },
    "when": {
      "admin": [
        // Predicate (Selection): Check if user is admin
        {
          "match": {
            "the": "cafe.familiar.user/role",
            "of": { "?": { "id": 10 } },
            "is": "admin"
          }
        }
      ],
      "owner": [
        // Predicate (Selection): Get document owner
        {
          "match": {
            "the": "cafe.familiar.document/owner",
            "of": { "?": { "id": 12 } },
            "is": { "?": { "id": 10 } }
          }
        },
        // Negation: Exclude archived documents
        {
          "not": {
            "match": {
              "the": "cafe.familiar.document/status",
              "of": { "?": { "id": 12 } },
              "is": "archived"
            }
          }
        },
        // Predicate (Formula): Check name length
        {
          "operator": "text/length",
          "match": {
            "of": { "?": { "id": 11 } },
            "is": { "?": { "id": 13 } }
          }
        }
      ]
    }
  }
}
```

## Formula Application

Formula Application applies built-in operators. **Critical**: Input parameters must be bound (have values) before execution.

### Variable Binding Requirements

- **Bound variable**: Has a concrete value from a previous operation
- **Unbound variable**: Placeholder waiting to be filled
- **Input parameters**: MUST BE BOUND (formulas need actual values)
- **Output parameters**: CAN BE UNBOUND (capture results)

### Schema

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "oneOf": [
    {
      "description": "Equality comparison",
      "type": "object",
      "properties": {
        "operator": { "const": "==" },
        "match": {
          "type": "object",
          "properties": {
            "of": { "$ref": "#/definitions/Term", "description": "INPUT: Value to compare (must be bound)" },
            "is": { "$ref": "#/definitions/Term", "description": "INPUT: Value to compare against OR expected result" }
          },
          "required": ["of", "is"],
          "additionalProperties": false
        }
      },
      "required": ["operator", "match"],
      "additionalProperties": false
    },
    {
      "description": "Text length calculation",
      "type": "object",
      "properties": {
        "operator": { "const": "text/length" },
        "match": {
          "type": "object",
          "properties": {
            "of": { "$ref": "#/definitions/Term", "description": "INPUT: Text to measure (must be bound)" },
            "is": { "$ref": "#/definitions/Term", "description": "OUTPUT: Length result (can be unbound)" }
          },
          "required": ["of"],
          "additionalProperties": false
        }
      },
      "required": ["operator", "match"],
      "additionalProperties": false
    },
    {
      "description": "Addition operation",
      "type": "object",
      "properties": {
        "operator": { "const": "+" },
        "match": {
          "type": "object",
          "properties": {
            "of": { "$ref": "#/definitions/Term", "description": "INPUT: First number (must be bound)" },
            "with": { "$ref": "#/definitions/Term", "description": "INPUT: Second number (must be bound)" },
            "is": { "$ref": "#/definitions/Term", "description": "OUTPUT: Sum result (can be unbound)" }
          },
          "required": ["of", "with"],
          "additionalProperties": false
        }
      },
      "required": ["operator", "match"],
      "additionalProperties": false
    }
  ]
}
```

**Note**: Complete schema includes all operators (`>`, `>=`, `<`, `<=`, `data/type`, `text/*`, `math/*`, etc.) with their specific input/output patterns.

### Examples

```json
// Equality comparison - both inputs must be bound
{
  "operator": "==",
  "match": {
    "of": { "?": { "id": 1 } },    // INPUT: must be bound
    "is": "Alice"                  // INPUT: expected value to match
  }
}

// Text length calculation - input bound, output captures result
{
  "operator": "text/length",
  "match": {
    "of": { "?": { "id": 1 } },    // INPUT: must be bound (the text)
    "is": { "?": { "id": 2 } }     // OUTPUT: captures length (can be unbound)
  }
}
```

## Common Type Definitions

### Term

```json
{
  "oneOf": [
    { "$ref": "#/definitions/Scalar" },
    { "$ref": "#/definitions/Variable" }
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
        "id": {
          "type": "number",
          "description": "Unique identifier for this variable"
        },
        "name": {
          "type": "string",
          "description": "Optional human-readable name for this variable"
        },
        "type": {
          "$ref": "#/definitions/Type",
          "description": "Optional type predicate"
        }
      },
      "required": ["id"]
    }
  },
  "required": ["?"]
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
    {
      "type": "object",
      "description": "BigInt representation"
    },
    {
      "type": "object",
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
      "required": ["/"],
      "description": "Bytes representation as per DAG-JSON spec"
    },
    {
      "type": "object",
      "properties": {
        "/": {
          "type": "string",
          "description": "IPLD Link representation"
        }
      },
      "required": ["/"]
    }
  ]
}
```

### Conjunct

```json
{
  "oneOf": [
    { "$ref": "#/definitions/Predicate" },
    { "$ref": "#/definitions/Negation" },
    { "$ref": "#/definitions/Recursion" },
    { "$ref": "#/definitions/RuleApplication" }
  ]
}
```

### Predicate

```json
{
  "oneOf": [
    { "$ref": "#/definitions/SelectionForm" },
    { "$ref": "#/definitions/FormulaApplication" }
  ]
}
```

---

## Background & Concepts

*This section provides conceptual background for those interested in understanding DialogDB's design. It can be skipped if you only need the schema reference above.*

### Facts and Uniform Structure

DialogDB uses a uniform fact representation similar to Datomic, where all facts follow the pattern `(The, Of, Is, Cause)`:

**Traditional Datalog allows arbitrary predicates:**
```prolog
parent(person:alice, person:bob).
likes(person:john, food:pizza).
age(person:alice, 30).
```

**DialogDB uses uniform structure:**
```js
{ "the": "cafe.familiar.family/parent", "of": "person:alice", "is": "person:bob", "cause": cause1 }
{ "the": "cafe.familiar.preference/likes", "of": "person:john", "is": "food:pizza", "cause": cause2 }
{ "the": "cafe.familiar.person/age", "of": "person:alice", "is": 30, "cause": cause3 }
```

### Named Arguments vs Positional Arguments

Unlike traditional Datalog which uses positional arguments, DialogDB uses named arguments for clarity and flexibility.

### Variable Binding and Query Planning

Think of DialogDB variables like spreadsheet cells:
- **Bound variable**: Like `A1 = "Alice"` (has a value)
- **Unbound variable**: Like an empty cell waiting for a value

Just as `=LEN(A1)` requires A1 to have a value, DialogDB formulas need their inputs bound before execution.

The query planner automatically:
1. Orders operations so variables are bound before use
2. Escalates unbound requirements to parent rules
3. Requires applications to provide top-level inputs

### Bidirectional vs Unidirectional Operations

Some operations can work in multiple directions while others are one-way:
- **Bidirectional**: `x + y = z` (could solve for any unknown)
- **Unidirectional**: `text/length` (text→length works, length≠>text is lossy)
