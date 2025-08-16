# Dialog Query Module Structure

## Overview
The codebase has been restructured into logical, focused modules that group related functionality together.

## Module Organization

### Core Types
- **`variable.rs`** - Variable definitions, type constraints, and macros (`var!`, `typed_var!`, `v!`)
- **`term.rs`** - Term enum (Constant/Variable) for pattern matching
- **`selector.rs`** - Selector patterns for database queries
- **`fact.rs`** - Fact representation for database storage

### Query Forms
- **`select.rs`** - Select form for querying facts by pattern matching
- **`syntax.rs`** - Minimal Syntax trait for query forms (only `plan` method)

### Execution
- **`plan.rs`** - Evaluation traits and context for query execution

### Utilities
- **`error.rs`** - Error types and Result aliases

## Key Changes from Previous Structure

### ✅ Removed Bloated Files
- ❌ `types.rs` - Split into focused modules
- ❌ `traits.rs` - Split into focused modules

### ✅ Simplified Traits
- **`Syntax`** trait now only has `plan()` method
- Removed TypeScript-inspired methods like `to_json()` and `to_debug_string()`

### ✅ Logical Grouping
- Related types and implementations are co-located
- Each module has a clear, single responsibility
- Better discoverability and maintainability

### ✅ Clean Exports
All commonly used types are available from the crate root:
```rust
use dialog_query::{
    Variable, Term, Selector, Select, Syntax, EvaluationPlan,
    var, typed_var, v  // Macros
};
```

## Usage Examples

### Creating Variables
```rust
use dialog_query::{var, typed_var, v, ValueDataType};

let person = var!(person);                    // Untyped
let name = typed_var!(name, String);          // Typed with macro
let email = v!(?email<String>);               // ?name syntax
let manual = Variable::typed("id", ValueDataType::Entity); // Manual
```

### Creating Selects
```rust
use dialog_query::{Select, Term, v};

let select = Select::by_attribute(Term::Constant(Value::String("person/name".to_string())))
    .with_entity(Term::Variable(v!(?person)))
    .with_value(Term::Variable(v!(?name<String>)));
```

### Planning and Execution
```rust
use dialog_query::{Syntax, EvaluationPlan, VariableScope};

let scope = VariableScope::empty();
let plan = select.plan(&scope)?;
let cost = plan.cost();
```

This structure is much cleaner, more maintainable, and follows Rust conventions better than the previous TypeScript-inspired approach.