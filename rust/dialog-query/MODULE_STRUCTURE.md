# Dialog Query Module Structure

## Overview
The dialog-query crate implements a Datalog-inspired query engine with a clean, focused module structure that separates concerns and follows Rust conventions.

## Module Organization

### Core Types
- **`variable.rs`** - Variable definitions, scoping, and data types (`Variable`, `VariableName`, `VariableScope`)
- **`term.rs`** - Term enum for pattern matching (`Term::Variable`, `Term::Constant`)  
- **`fact.rs`** - Fact operations (`Assertion`, `Retraction`, `Fact`) with conversion to/from `Instruction`
- **`selection.rs`** - Variable bindings and unification (`Match`, `Selection`)

### Query Components
- **`fact_selector.rs`** - Fact pattern matching (`FactSelector`, `FactSelectorPlan`) with evaluation logic
- **`query.rs`** - Query trait for polymorphic querying over stores
- **`selector.rs`** - Base selector abstractions
- **`syntax.rs`** - Query syntax forms and planning interface

### Execution
- **`plan.rs`** - Query planning and execution (`EvaluationPlan`, `EvaluationContext`)
- **`stream.rs`** - Stream processing utilities

### Utilities  
- **`error.rs`** - Comprehensive error types (`QueryError`, `InconsistencyError`)

## Key Design Principles

### ✅ Clean API Separation
- **Query trait**: Constants-only queries for direct store access
- **Plan → Evaluate**: Variable queries with proper unification

### ✅ Stream-Based Architecture
- **Selection**: Async streams of variable bindings
- **EvaluationPlan**: Transforms selections through query operations
- **Lazy Evaluation**: Results computed on-demand

### ✅ Type Safety
- **Constrained Selectors**: Compile-time guarantee of valid queries  
- **Variable Scoping**: Prevents unbound variable errors
- **Error Propagation**: Comprehensive error handling with context

### ✅ Test Co-location
- All modules include `#[cfg(test)]` blocks with comprehensive tests
- 36 tests covering all major functionality
- Tests live alongside the code they verify

## Current API

### Basic Fact Operations
```rust
use dialog_query::prelude::*;

// Create facts
let assertion = Fact::assert("user/name", entity, Value::String("Alice".to_string()));
let retraction = Fact::retract("user/name", entity, Value::String("Alice".to_string()));

// Convert to instructions for storage
let instruction: Instruction = assertion.into();
```

### Constants-Only Queries (Query Trait)
```rust
use dialog_query::prelude::*;

// Direct query with constants
let selector = FactSelector::new()
    .the("user/name") 
    .of(alice_entity)
    .is(Value::String("Alice".to_string()));

let results = selector.query(&store)?;
```

### Variable Queries (Plan → Evaluate)
```rust
use dialog_query::prelude::*;

// Create pattern with variables
let name_var = Variable::new("name", ValueDataType::String);
let selector = FactSelector::new()
    .the("user/name")
    .of(alice_entity)
    .is(Term::Variable(name_var));

// Plan and execute
let scope = VariableScope::new();
let plan = selector.plan(&scope)?;
let context = EvaluationContext::new(store, initial_selection);
let results = plan.evaluate(context);
```

### Variable Unification
```rust
// Pattern: {the: "user/name", of: ?entity, is: ?name}
// Unifies variables from matching facts
// Returns: Stream<Match> with variable bindings
for await match_frame in results {
    let entity = match_frame.get(&entity_var)?;
    let name = match_frame.get(&name_var)?;
    // Use bound variables...
}
```

## Integration Points

### Dialog-DB Ecosystem
- **dialog-artifacts**: Provides `ArtifactStore` trait and data types
- **dialog-storage**: Content-addressed storage backend  
- **dialog-common**: Shared utilities and conditional Send bounds

### Future Extensions
- Extended syntax forms (`Select`, `DeductiveRule`, `Conjunct`)
- JSON serialization for cross-language compatibility
- Multi-conjunct queries (AND/OR logic)
- Advanced query optimization

This architecture provides a solid foundation for building higher-level query languages and rule systems while maintaining type safety and performance.