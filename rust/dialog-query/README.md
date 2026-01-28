# Dialog Query Engine

A Rust implementation of the Datalog-inspired query engine for Dialog-DB.

## Overview

This crate provides declarative pattern matching and rule-based deduction over facts, designed to be equivalent to the TypeScript query engine in `@query/`. The engine supports both constant-only queries and variable-based queries with proper unification.

## Architecture

The query engine is organized into several modules:

- **`variable`** - Variable definitions and scoping (`Variable`, `VariableName`, `VariableScope`)
- **`term`** - Query terms (`Term::Variable`, `Term::Constant`)
- **`fact`** - Fact operations (`Assertion`, `Retraction`, `Fact`)
- **`fact_selector`** - Fact pattern matching (`FactSelector`, `FactSelectorPlan`)
- **`selection`** - Variable bindings and unification (`Match`, `Selection`) 
- **`plan`** - Query planning and execution (`EvaluationPlan`, `EvaluationContext`)
- **`query`** - Query trait for polymorphic querying
- **`syntax`** - Query syntax forms and planning
- **`stream`** - Stream processing utilities
- **`error`** - Error types and result handling

## Current Implementation Status

### ✅ Phase 1: Core Query Engine
- [x] Basic Cargo package setup with all dependencies
- [x] Module organization and structure
- [x] Core type definitions (`Variable`, `Term`, `FactSelector`)
- [x] Fact selector with pattern matching
- [x] Variable scoping and binding system
- [x] Query planning infrastructure with `EvaluationPlan` trait
- [x] Stream-based execution with `Selection` trait
- [x] Unification logic for variable resolution
- [x] Error handling with proper error types
- [x] **Query trait implementation** - Supports constant-only queries
- [x] **Evaluation plan system** - Handles queries with variables
- [x] **Comprehensive test coverage** - All 36 tests passing

### ✅ API Design

The query engine supports two main usage patterns:

#### 1. Direct Queries (Constants Only)
```rust
use dialog_query::prelude::*;

// Create a fact selector with only constants
let selector = FactSelector::new()
    .the("person/name")
    .of("user123")
    .is("Alice");

// Query directly against a store
let results = selector.query(&store)?;
```

#### 2. Variable Queries (Plan → Evaluate)
```rust
use dialog_query::prelude::*;

// Create a fact selector with variables
let name_var = Variable::new("name", ValueDataType::String);
let selector = FactSelector::new()
    .the("person/name")
    .of("user123")
    .is(Term::Variable(name_var));

// Create execution plan
let scope = VariableScope::new();
let plan = selector.plan(&scope)?;

// Execute with evaluation context
let context = EvaluationContext::new(store, initial_selection);
let results = plan.evaluate(context);
```

### 🚧 Next Steps (Phase 2)
- [ ] Extended syntax forms (`Select`, `DeductiveRule`, `Conjunct`)
- [ ] JSON serialization/deserialization matching TypeScript AST
- [ ] Multi-conjunct queries (AND logic)
- [ ] Named disjuncts (OR logic)
- [ ] Rule-based deduction
- [ ] Advanced query optimization

## Key Features

### Variable Unification
The engine properly unifies variables in query patterns with actual data:
- **Pattern**: `{the: "person/name", of: ?entity, is: ?name}`
- **Unification**: Binds `?entity` and `?name` from matching facts
- **Result**: Stream of `Match` objects with variable bindings

### Streaming Architecture
- **Selection Trait**: Async streams of variable bindings
- **EvaluationPlan**: Transforms selections through query operations  
- **Lazy Evaluation**: Results computed on-demand

### Type Safety
- **Constrained Selectors**: Compile-time guarantee of valid queries
- **Variable Scoping**: Prevents unbound variable errors
- **Error Propagation**: Comprehensive error handling with context

## Testing

The crate includes comprehensive tests covering:

```bash
cargo test  # Runs all 36 tests
```

- Fact selector creation and validation
- Variable scoping and binding
- Query planning and execution
- Unification logic
- Error handling
- Stream processing

## Design Principles

1. **Incremental Development** - Build functionality step by step with tests
2. **TypeScript Compatibility** - Match the behavior of the existing TypeScript engine
3. **Stream Integration** - Leverage existing streaming infrastructure
4. **Type Safety** - Compile-time guarantees where possible
5. **Clear API Separation** - Distinct patterns for constants vs. variables

## Integration

This crate integrates with the broader Dialog-DB ecosystem:

- **dialog-artifacts**: Provides `ArtifactStore` trait and data types
- **dialog-storage**: Content-addressed storage backend
- **dialog-common**: Shared utilities and conditional Send bounds

The query engine serves as the foundation for higher-level query languages and rule systems in Dialog-DB.