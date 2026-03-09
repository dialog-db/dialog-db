#![warn(missing_docs)]

//! Procedural macros for the dialog project.
//!
//! This crate provides derive macros for the query engine (`Attribute`, `Concept`,
//! `Formula`) and procedural macros for testing and service provisioning.
//! Query macros are re-exported through `dialog_query`, while test/provider macros
//! are re-exported through `dialog_common` (with the `helpers` feature).
//!
//! Procedural macros must be defined in their own crate, which is why these live
//! here rather than in the crates that use them.

use proc_macro::TokenStream;
mod provider;
mod query;
mod test;

/// A cross-platform test macro with automatic service provisioning.
///
/// This macro is re-exported as [`dialog_common::test`] (with the `helpers` feature).
/// See that documentation for usage examples.
///
/// # CI Test Matrix
///
/// The macro generates code that supports these CI configurations:
///
/// 1. `cargo test` - Unit tests run natively
/// 2. `cargo test --target wasm32-unknown-unknown` - Unit tests run in wasm
/// 3. `cargo test --features integration-tests` - Unit tests + integration tests run natively
/// 4. `cargo test --features web-integration-tests` - Integration tests run in wasm
///    (unit tests skipped, native provider spawns wasm inner tests)
///
/// # Generated Code
///
/// For **unit tests** (no parameters): Uses `#[test]` on native, `#[wasm_bindgen_test]` on wasm.
/// Gated with `#[cfg(not(feature = "web-integration-tests"))]` to skip during wasm integration runs.
///
/// For **integration tests** (with address parameter):
/// - **Native** (`integration-tests` feature): Starts service, runs test, stops service
/// - **Web** (`web-integration-tests` feature): Starts service, spawns wasm subprocess, stops service
/// - **Wasm inner** (`dialog_test_wasm_integration` cfg): Deserializes address from env var, runs test
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    test::generate(attr, item)
}

/// Mark a function as a service provider for integration tests.
///
/// This macro is re-exported as [`dialog_common::provider`] (with the `helpers` feature).
/// See that documentation for usage examples.
///
/// Transforms an async function returning `Service<Address, Provider>` into a
/// `Provisionable` implementation that works with the `#[dialog_common::test]` macro.
///
/// # Generated Code
///
/// The macro generates:
/// 1. The original provider function (native-only via `#[cfg(not(target_arch = "wasm32"))]`)
/// 2. A `Provisionable` trait implementation on the address type
///
/// This allows the address type to be used with `#[dialog_common::test]`.
#[proc_macro_attribute]
pub fn provider(attr: TokenStream, item: TokenStream) -> TokenStream {
    provider::generate(attr, item)
}

/// Derive macro to generate Concept implementation from a struct definition.
///
/// Generates all necessary boilerplate for implementing a concept,
/// including Query, Conclusion, Statement, and Term types.
///
/// The struct must have a `this: Entity` field. All other fields must implement
/// the `dialog_query::attribute::Attribute` trait.
///
/// # Example
///
/// Given attribute types and a concept struct:
///
/// ```no_run
/// # mod employee {
/// #     #[derive(Debug, Clone, PartialEq)]
/// #     pub struct Name(pub String);
/// #     #[derive(Debug, Clone, PartialEq)]
/// #     pub struct Role(pub String);
/// # }
/// # #[derive(Debug, Clone, PartialEq)]
/// # struct Entity;
/// // #[derive(Concept, Debug, Clone, PartialEq)]
/// pub struct Employee {
///     this: Entity,
///     name: employee::Name,
///     role: employee::Role,
/// }
/// ```
///
/// The macro expands to (simplified):
///
/// ```no_run
/// # #[derive(Clone)] struct Entity;
/// # #[derive(Clone)] struct Term<T>(T);
/// # impl<T> Term<T> { fn var(_: &str) -> Self { todo!() } }
/// # struct Employee { this: Entity }
/// // Query pattern with Term-wrapped fields
/// pub struct EmployeeQuery {
///     pub this: Term<Entity>,
///     pub name: Term<String>,  // <Name as Attribute>::Type
///     pub role: Term<String>,  // <Role as Attribute>::Type
/// }
///
/// // Typed term accessors
/// pub struct EmployeeTerms;
/// impl EmployeeTerms {
///     pub fn this() -> Term<Entity> { Term::var("this") }
///     pub fn name() -> Term<String> { Term::var("name") }
///     pub fn role() -> Term<String> { Term::var("role") }
/// }
///
/// // Concept trait — ties everything together
/// # trait Concept { type Conclusion; type Query; type Term; }
/// impl Concept for Employee {
///     type Conclusion = Employee;
///     type Query = EmployeeQuery;
///     type Term = EmployeeTerms;
/// }
///
/// // Application trait — reconstructs Employee from query match
/// # trait Application { type Conclusion; }
/// impl Application for EmployeeQuery {
///     type Conclusion = Employee;
/// }
///
/// // Conclusion trait — extracts the entity
/// # trait Conclusion { fn this(&self) -> &Entity; }
/// impl Conclusion for Employee {
///     fn this(&self) -> &Entity { todo!() }
/// }
///
/// // Statement trait — assert/retract into transactions
/// # trait Statement { fn assert(self, transaction: &mut ()); fn retract(self, transaction: &mut ()); }
/// impl Statement for Employee {
///     fn assert(self, transaction: &mut ()) { todo!() }
///     fn retract(self, transaction: &mut ()) { todo!() }
/// }
///
/// // Not operator — enables `!employee` for retraction
/// impl std::ops::Not for Employee {
///     type Output = ();
///     fn not(self) -> Self::Output { todo!() }
/// }
///
/// // IntoIterator — converts to Associations for storage
/// impl IntoIterator for Employee {
///     type Item = ();
///     type IntoIter = std::vec::IntoIter<()>;
///     fn into_iter(self) -> Self::IntoIter { todo!() }
/// }
/// ```
///
/// # Usage
///
/// ```no_run
/// # struct Employee; struct Term<T>(T);
/// # impl<T> Term<T> { fn var(_: &str) -> Self { todo!() } fn from(_: T) -> Self { todo!() } }
/// # struct EmployeeQuery { this: Term<()>, name: Term<String>, role: Term<()> }
/// # impl Employee { fn query<S>(_: S) -> std::vec::IntoIter<Employee> { todo!() } }
/// # impl EmployeeQuery { fn query<S>(self, _: S) -> std::vec::IntoIter<Employee> { todo!() } }
/// # let session = ();
/// // Query with a pattern
/// let query = EmployeeQuery {
///     this: Term::var("this"),
///     name: Term::from("Alice".to_string()),
///     role: Term::var("role"),
/// };
/// ```
#[proc_macro_derive(Concept)]
pub fn derive_concept(input: TokenStream) -> TokenStream {
    query::concept::derive(input)
}

/// Derive macro to generate Formula implementation from a struct definition.
///
/// Formulas are pure computations: given bound input fields, they compute output
/// fields. The query planner uses cost annotations to decide execution order.
///
/// # Attributes
///
/// - `#[output]` or `#[output(cost = N)]` - Mark fields as output/computed
///   - If cost is omitted, defaults to 1
///   - Total formula cost is the sum of all output field costs
///
/// # Example
///
/// ```no_run
/// # struct Input { first: String, second: String }
/// // #[derive(Debug, Clone, Formula)]
/// #[derive(Debug, Clone)]
/// pub struct Concatenate {
///     /// First string
///     pub first: String,
///     /// Second string
///     pub second: String,
///     /// Concatenated result
///     // #[output(cost = 2)]
///     pub is: String,
/// }
///
/// impl Concatenate {
///     pub fn compute(input: Input) -> Vec<Self> {
///         vec![Concatenate {
///             first: input.first.clone(),
///             second: input.second.clone(),
///             is: format!("{}{}", input.first, input.second),
///         }]
///     }
/// }
/// ```
///
/// The macro expands to (simplified):
///
/// ```no_run
/// # struct Concatenate { first: String, second: String, is: String }
/// # struct Term<T>(T);
/// // Input struct — only non-output fields
/// pub struct ConcatenateInput {
///     pub first: String,
///     pub second: String,
/// }
///
/// // Query struct — all fields as Terms for query patterns
/// pub struct ConcatenateQuery {
///     pub first: Term<String>,
///     pub second: Term<String>,
///     pub is: Term<String>,
/// }
///
/// // Formula trait — describes the computation and writes output fields
/// # struct Bindings;
/// # struct EvaluationError;
/// # trait Formula { type Input; fn cells() -> (); fn cost() -> usize; fn compute(input: ConcatenateInput) -> Vec<Concatenate>; fn write(&self, bindings: &mut Bindings) -> Result<(), EvaluationError>; }
/// impl Formula for Concatenate {
///     type Input = ConcatenateInput;
///
///     fn cells() -> () { /* lazily built from cell definitions */ }
///     fn cost() -> usize { 2 }  // sum of output field costs
///     fn compute(input: ConcatenateInput) -> Vec<Concatenate> {
///         todo!() // delegates to user's Concatenate::compute
///     }
///     fn write(&self, bindings: &mut Bindings) -> Result<(), EvaluationError> {
///         // bindings.write("is", &self.is.clone().into())?;
///         todo!()
///     }
/// }
/// ```
///
/// # Usage
///
/// ```no_run
/// # struct Term<T>(T);
/// # impl<T> Term<T> { fn var(_: &str) -> Self { todo!() } }
/// # struct ConcatenateQuery { first: Term<String>, second: Term<String>, is: Term<String> }
/// // Use in a query to concatenate first + last name
/// let pattern = ConcatenateQuery {
///     first: Term::var("first"),
///     second: Term::var("last"),
///     is: Term::var("full_name"),
/// };
/// ```
#[proc_macro_derive(Formula, attributes(output))]
pub fn derive_formula(input: TokenStream) -> TokenStream {
    query::formula::derive(input)
}

/// Derive macro for the Attribute trait on tuple structs.
///
/// Generates an implementation of `dialog_query::attribute::Attribute` for
/// tuple structs that wrap a single value type.
///
/// # Attributes
///
/// - `#[cardinality(many)]` - Marks the attribute as having many values (defaults to One)
/// - `#[domain(custom)]` or `#[domain("io.gozala")]` - Override the default domain
///   (`#[namespace(...)]` is accepted as a legacy alias)
///
/// The default domain is derived from the module path (last segment, with
/// underscores converted to hyphens). The attribute name is derived from the
/// struct name converted to kebab-case.
///
/// # Example
///
/// ```no_run
/// mod employee {
///     /// A person's given name
///     #[derive(Clone, PartialEq)]
///     // #[derive(Attribute, Clone, PartialEq)]
///     pub struct Name(pub String);
///
///     /// Tags associated with an employee
///     #[derive(Clone, PartialEq)]
///     // #[derive(Attribute, Clone, PartialEq)]
///     // #[cardinality(many)]
///     pub struct Tag(pub String);
/// }
/// ```
///
/// The macro expands to (simplified):
///
/// ```no_run
/// # struct Name(String);
/// # struct AttributeQuery<T>(T); struct AttributeStatement<T>(T);
/// # enum Cardinality { One }
/// # struct AttributeDescriptor;
/// # impl AttributeDescriptor { fn new() -> Self { Self } }
/// // Attribute trait — maps the newtype to its inner value type
/// # trait Attribute { type Type;
/// #   fn descriptor() -> AttributeDescriptor;
/// #   fn value(&self) -> &Self::Type; fn new(value: Self::Type) -> Self; }
/// # trait Predicate { type Conclusion; type Application; type Descriptor; }
/// impl Attribute for Name {
///     type Type = String;
///
///     fn descriptor() -> AttributeDescriptor {
///         // Domain derived from module path: "employee"
///         // Name derived from struct name: "Name" -> "name"
///         AttributeDescriptor::new(/* ... */)
///     }
///
///     fn value(&self) -> &String { &self.0 }
///     fn new(value: String) -> Self { Self(value) }
/// }
///
/// // Debug — shows domain, name, and value
/// impl std::fmt::Debug for Name {
///     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
///         f.debug_struct("Name")
///             .field("domain", &"employee")
///             .field("name", &"name")
///             .field("value", &self.0)
///             .finish()
///     }
/// }
///
/// // Display — shows "domain/name: value"
/// impl std::fmt::Display for Name {
///     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
///         write!(f, "employee/name: {:?}", self.0)
///     }
/// }
///
/// // Generic From — any type convertible to the inner type
/// # trait NewName { fn new(v: String) -> Self; }
/// # impl NewName for Name { fn new(v: String) -> Self { Self(v) } }
/// impl<U: Into<String>> From<U> for Name {
///     fn from(value: U) -> Self { <Self as NewName>::new(value.into()) }
/// }
/// ```
///
/// # Usage
///
/// ```no_run
/// # mod employee { pub struct Name(pub String); }
/// // Create attribute values
/// let name = employee::Name("Alice".to_string());
/// ```
#[proc_macro_derive(Attribute, attributes(cardinality, domain, namespace))]
pub fn derive_attribute(input: TokenStream) -> TokenStream {
    query::attribute::derive(input)
}
