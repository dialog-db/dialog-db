/// Concept descriptors for entity-centric queries.
pub mod descriptor;
/// Concept application for querying entities that match a concept pattern.
pub mod query;

pub use descriptor::{ConceptDescriptor, ConceptFieldDescriptor};
pub use query::ConceptQuery;

use crate::artifact::Type as ValueType;
use crate::attribute::{Attribute, AttributeDescriptor, AttributeStatement};
use crate::descriptor::Descriptor;
use crate::error::EvaluationError;
pub use crate::predicate::Predicate;
use crate::selection::Binding;
use crate::term::Term;
use crate::type_system::{Primitive, Type as Kind};
use crate::types::{Any, TypeDescriptor, Typed};
use crate::{Entity, Parameters, Value};
use dialog_common::ConditionalSend;
use std::fmt::Debug;

/// Concept is a set of attributes associated with entity representing an
/// abstract idea. It is a tool for the domain modeling and in some regard
/// similar to a table in relational database or a collection in the document
/// database, but unlike them it is disconnected from how information is
/// organized, in that sense it is more like view into which you can also insert.
///
/// Concepts are used to describe conclusions of the rules, providing a mapping
/// between conclusions and facts. In that sense you concepts are on-demand
/// cache of all the conclusions from the associated rules.
///
/// Note: IntoIterator is not a bound on this trait to allow attributes to
/// implement Concept by delegating to their instance types (e.g., Title
/// delegates to its AttributeStatement type). Conclusion types still implement IntoIterator.
pub trait Concept: Predicate + Clone + Debug
where
    Self::Conclusion: Conclusion,
{
    /// Typed term accessors for building queries (e.g. `PersonTerms::name()`).
    type Term;

    /// Returns a description of this concept.
    fn description() -> &'static str {
        ""
    }

    /// Content-addressed identity for this concept.
    fn this(&self) -> Entity;
}

/// Field-level abstraction used by `#[derive(Concept)]` to dispatch
/// between required (`N: Attribute`) and optional (`Option<N>`)
/// concept fields without doing syntactic type matching in the
/// proc macro.
///
/// Two blanket impls cover the two cases:
///
/// - `impl<N: Attribute> ConceptField for N` — required path.
/// - `impl<N: Attribute> ConceptField for Option<N>` — optional path.
///
/// These don't overlap because `Option` is `#[fundamental]`: Rust's
/// trait coherence treats `Option<T>` as structurally distinct from
/// a bare type parameter `T`. The Concept derive refers to
/// `<F as ConceptField>::TermType` etc. without inspecting `F`'s
/// syntactic shape — Rust resolves the right impl at type-check
/// time. Aliases, prelude paths, and renamed imports all work.
pub trait ConceptField: Sized + Clone {
    /// The underlying attribute newtype. For both required `N` and
    /// optional `Option<N>`, this is `N`.
    type Attribute: Attribute
        + Descriptor<AttributeDescriptor>
        + From<<Self::Attribute as Attribute>::Type>;

    /// The type wrapping the attribute's scalar at the term layer.
    /// - Required (`N`): `<N as Attribute>::Type`.
    /// - Optional (`Option<N>`): `Option<<N as Attribute>::Type>`.
    type TermType: Typed + Clone + Debug + ConditionalSend + 'static;

    /// `true` if this field is set-widened (the `Option<N>` impl),
    /// `false` for the bare-attribute (`N`) impl. Drives
    /// [`field_descriptor`](Self::field_descriptor) and the
    /// `#[derive(Concept)]` compile-time "at least one required
    /// field" assertion.
    const OPTIONAL: bool;

    /// The [`ConceptFieldDescriptor`] for this field: the underlying
    /// attribute's descriptor wrapped with this field's optionality.
    /// Used by `#[derive(Concept)]` to build the concept's attribute
    /// map without branching on [`OPTIONAL`](Self::OPTIONAL) in the
    /// generated code.
    fn field_descriptor() -> ConceptFieldDescriptor {
        let descriptor = <Self::Attribute as Descriptor<AttributeDescriptor>>::descriptor().clone();
        if Self::OPTIONAL {
            ConceptFieldDescriptor::optional(descriptor)
        } else {
            ConceptFieldDescriptor::required(descriptor)
        }
    }

    /// Build the `is` slot term for this field's attribute query.
    /// Required fields pass the user's `value_param` through
    /// unchanged. Optional fields return an optional-typed term
    /// so the `AttributeQuery` evaluates with Absent-fallback
    /// semantics (resolution is derived from the term's kind).
    fn term(value_param: Term<Any>) -> Term<Any>;

    /// Realize a value of `Self` from the row binding for this slot.
    ///
    /// - Required: the binding must be `Present`; the inner scalar
    ///   is unwrapped via `TryFrom<Value>` and wrapped in `N`.
    /// - Optional: `Present(v)` becomes `Some(N(v))`, `Absent`
    ///   becomes `None`.
    fn realize(binding: Binding) -> Result<Self, EvaluationError>;

    /// Push the attribute statement(s) for this field into `buf`.
    ///
    /// - Required: always one statement.
    /// - Optional: one if `Some`, none if `None`.
    fn push_statements(&self, this: Entity, buf: &mut Vec<AttributeStatement>);
}

// Required path: any `N` that satisfies the attribute bounds gets
// the `ConceptField` impl with `OPTIONAL = false` and
// `TermType = N::Type`. This is the "bare attribute" case in a
// concept field — `pub name: GivenName` and similar.
//
// Coherence note: this blanket and the next one (`for Option<N>`)
// are non-overlapping because `Option` is `#[fundamental]`. The
// `#[fundamental]` annotation on std's `Option` tells the trait
// checker to treat `Option<T>` as structurally distinct from a
// bare type parameter `T`, so `impl<N> Trait for N` and
// `impl<N> Trait for Option<N>` are allowed to coexist.
//
// Other `#[fundamental]` types where this same pattern would work
// include `Box<T>`, `&T`, `&mut T`, and `Pin<T>`.
impl<N> ConceptField for N
where
    N: Attribute + Descriptor<AttributeDescriptor> + Clone + From<<N as Attribute>::Type>,
    <N as Attribute>::Type: Into<Value>,
{
    type Attribute = N;
    type TermType = <N as Attribute>::Type;
    const OPTIONAL: bool = false;

    fn term(value_param: Term<Any>) -> Term<Any> {
        // Required: pass the user's term through as-is; its kind
        // (if any) is not optional, so the query stays required.
        value_param
    }

    fn realize(binding: Binding) -> Result<Self, EvaluationError> {
        let value = binding.content()?;
        let inner = <<N as Attribute>::Type>::try_from(value).map_err(|_| {
            EvaluationError::TypeMismatch {
                expected: <<<N as Attribute>::Type as Typed>::Descriptor as TypeDescriptor>::TYPE
                    .unwrap_or(ValueType::Symbol),
                actual: ValueType::Symbol,
            }
        })?;
        Ok(N::from(inner))
    }

    fn push_statements(&self, this: Entity, buf: &mut Vec<AttributeStatement>) {
        let descriptor = <Self as Descriptor<AttributeDescriptor>>::descriptor();
        let expr = AttributeStatement {
            the: descriptor.the().clone(),
            of: this,
            is: <<N as Attribute>::Type as Into<Value>>::into(
                <Self as Attribute>::value(self).clone(),
            ),
            cause: None,
            cardinality: Some(descriptor.cardinality()),
        };
        buf.push(expr);
    }
}

// Optional path: `Option<N>` (where `N: Attribute`) gets the
// `ConceptField` impl with `OPTIONAL = true` and
// `TermType = Option<N::Type>`. This is the "set-widened" case in
// a concept field — `pub nickname: Option<Nickname>` and similar.
// `realize` maps `Binding::Absent` to `None`; `push_statements`
// emits zero records when the value is `None` (absence is never
// persisted).
//
// Optionality at the call site is purely type-system driven —
// `<Option<N> as ConceptField>` resolves to *this* impl by virtue
// of the structural `Option<N>` shape. Aliased imports
// (`use core::option::Option as Maybe`, etc.) resolve identically
// at the type level even though the surface syntax differs, which
// is why no proc-macro syntactic detection is needed.
impl<N> ConceptField for Option<N>
where
    N: Attribute + Descriptor<AttributeDescriptor> + Clone + From<<N as Attribute>::Type>,
    <N as Attribute>::Type: Into<Value>,
{
    type Attribute = N;
    type TermType = Option<<N as Attribute>::Type>;
    const OPTIONAL: bool = true;

    fn term(value_param: Term<Any>) -> Term<Any> {
        // Optional: return an optional-typed term. The underlying
        // kind (if any) is wrapped via `Type::optional`; an untyped
        // term becomes "any primitive, optional." The
        // `AttributeQuery` reads `is.is_optional()` and switches to
        // the Absent-fallback evaluation path.
        let name = match value_param.name() {
            Some(n) => n.to_string(),
            None => return value_param,
        };
        let kind = match value_param.kind() {
            Some(k) => k.optional(),
            None => Kind::primitive_set(Primitive::ALL).optional(),
        };
        Term::<Any>::typed_var(name, kind)
    }

    fn realize(binding: Binding) -> Result<Self, EvaluationError> {
        match binding {
            Binding::Present(value) => {
                let inner = <<N as Attribute>::Type>::try_from(value).map_err(|_| {
                    EvaluationError::TypeMismatch {
                        expected:
                            <<<N as Attribute>::Type as Typed>::Descriptor as TypeDescriptor>::TYPE
                                .unwrap_or(ValueType::Symbol),
                        actual: ValueType::Symbol,
                    }
                })?;
                Ok(Some(N::from(inner)))
            }
            Binding::Absent => Ok(None),
        }
    }

    fn push_statements(&self, this: Entity, buf: &mut Vec<AttributeStatement>) {
        if let Some(inner) = self.as_ref() {
            let descriptor = <N as Descriptor<AttributeDescriptor>>::descriptor();
            let expr = AttributeStatement {
                the: descriptor.the().clone(),
                of: this,
                is: <<N as Attribute>::Type as Into<Value>>::into(
                    <N as Attribute>::value(inner).clone(),
                ),
                cause: None,
                cardinality: Some(descriptor.cardinality()),
            };
            buf.push(expr);
        }
    }
}

// Blanket impl for &T -> Parameters that uses the generated From<T> impl
impl<T> From<&T> for Parameters
where
    T: Clone + Into<Parameters>,
{
    fn from(source: &T) -> Self {
        source.clone().into()
    }
}

/// A materialized concept — a concrete record whose fields have been
/// resolved from a query [`Match`].
///
/// Every concept struct carries a `this: Entity` field that identifies the
/// entity it describes. This trait surfaces that field, serving two purposes:
///
/// 1. **Compile-time enforcement** — the `#[derive(Concept)]` macro generates
///    a `Conclusion` impl whose return type is `&Entity`. If the `this` field
///    is missing the macro emits an error; if it has the wrong type the
///    generated impl produces a type mismatch.
/// 2. **Uniform entity access** — any code generic over `Conclusion` can
///    retrieve the underlying entity without knowing the concrete concept
///    type.
///
/// ```compile_fail
/// use dialog_query::{Concept, Entity};
/// use dialog_macros::Attribute;
///
/// mod attrs {
///     #[derive(dialog_macros::Attribute, Clone, PartialEq)]
///     pub struct Name(pub String);
/// }
///
/// /// Concept without a `this` field — should fail.
/// #[derive(Concept, Debug, Clone)]
/// pub struct BadConcept {
///     pub name: attrs::Name,
/// }
/// ```
///
/// ```compile_fail
/// use dialog_query::{Concept, Entity};
/// use dialog_macros::Attribute;
///
/// mod attrs {
///     #[derive(dialog_macros::Attribute, Clone, PartialEq)]
///     pub struct Name(pub String);
/// }
///
/// /// Concept with wrong type for `this` — should fail.
/// #[derive(Concept, Debug, Clone)]
/// pub struct BadConcept {
///     pub this: String,
///     pub name: attrs::Name,
/// }
/// ```
///
/// A concept must declare at least one *required* attribute. A
/// struct whose only attribute fields are `Option<_>` constrains
/// nothing (every entity matches), so the derive rejects it at
/// compile time via a const assertion over
/// [`ConceptField::OPTIONAL`].
///
/// ```compile_fail
/// use dialog_query::{Concept, Entity};
///
/// mod attrs {
///     #[derive(dialog_macros::Attribute, Clone, PartialEq)]
///     pub struct Nickname(pub String);
/// }
///
/// /// Only an optional attribute — should fail to compile.
/// #[derive(Concept, Debug, Clone)]
/// pub struct AllOptional {
///     pub this: Entity,
///     pub nickname: Option<attrs::Nickname>,
/// }
/// ```
///
/// A concept with no attribute fields at all (only `this`) is
/// likewise rejected — it would match every entity.
///
/// ```compile_fail
/// use dialog_query::{Concept, Entity};
///
/// /// No attributes — should fail to compile.
/// #[derive(Concept, Debug, Clone)]
/// pub struct NoAttributes {
///     pub this: Entity,
/// }
/// ```
pub trait Conclusion: ConditionalSend {
    /// Each instance has a corresponding entity and this method
    /// returns a reference to it.
    fn this(&self) -> &Entity;
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::AttributeStatement;
    use crate::Query;
    use crate::artifact::{ArtifactSelector, ArtifactsAttribute, Value};
    use crate::query::Output;

    use crate::Concept;
    use crate::attribute::query::AttributeQuery;
    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use crate::term::Term;
    use crate::the;
    use anyhow::Result;
    use dialog_repository::helpers::{test_operator_with_profile, test_repo};

    // Define a Person concept for testing via `#[derive(Concept)]`.
    // The newtypes live in a module named `person` so the attribute
    // domain defaults to `person`, yielding `person/name` and
    // `person/age` to match the stored facts the tests assert against.
    mod person {
        use crate::Attribute;

        /// Name of the person.
        #[derive(Attribute, Clone, PartialEq)]
        pub struct Name(pub String);

        /// Age of the person.
        #[derive(Attribute, Clone, PartialEq)]
        pub struct Age(pub u32);
    }

    /// A person concept used across the scaffold tests below.
    #[derive(Concept, Debug, Clone)]
    pub struct Person {
        pub this: Entity,
        /// Person's name (`person/name`).
        pub name: person::Name,
        /// Person's age (`person/age`).
        pub age: person::Age,
    }

    #[dialog_common::test]
    fn it_creates_person_concept() {
        // Test that the Person concept has the expected properties
        let concept = Person::descriptor().clone();
        // Operator is now a URI based on the hash of the concept's attributes
        assert!(
            concept.this().to_string().starts_with("concept:"),
            "Operator should be a concept URI"
        );

        // Test Person has 2 attributes (name and age)
        assert_eq!(concept.with().iter().count(), 2);

        // Verify attribute names
        let attr_names: Vec<&str> = concept.with().iter().map(|(name, _)| name).collect();
        assert!(attr_names.contains(&"name"));
        assert!(attr_names.contains(&"age"));
    }

    #[dialog_common::test]
    fn it_creates_person_match() {
        // Test creating a PersonQuery for querying
        let entity_var = Term::var("person_entity");
        let name_var: Term<String> = Term::var("person_name");
        let age_var: Term<u32> = Term::var("person_age");

        let person_match = Query::<Person> {
            this: entity_var.clone(),
            name: name_var.clone(),
            age: age_var.clone(),
        };

        // Test that fields are accessible
        assert_eq!(person_match.this, entity_var);
        assert_eq!(person_match.name, name_var);
        assert_eq!(person_match.age, age_var);
    }

    #[dialog_common::test]
    fn it_creates_match_with_constant_values() {
        // Test querying for a specific person with constant values
        let entity_var = Term::var("alice_entity");
        let name_const = Term::from("Alice".to_string());
        let age_const = Term::from(30u32);

        let person_match = Query::<Person> {
            this: entity_var.clone(),
            name: name_const.clone(),
            age: age_const.clone(),
        };

        // Verify the constants are preserved
        assert_eq!(person_match.name, name_const);
        assert_eq!(person_match.age, age_const);

        // Test that constants are properly identified
        assert!(person_match.name.is_constant());
        assert!(person_match.age.is_constant());
    }

    #[dialog_common::test]
    fn it_creates_match_with_mixed_terms() {
        // Test mixing variables and constants in a match pattern
        let entity_var = Term::var("person_entity");
        let name_const = Term::from("Bob".to_string());
        let age_var: Term<u32> = Term::var("any_age");

        let person_match = Query::<Person> {
            this: entity_var.clone(),
            name: name_const.clone(),
            age: age_var.clone(),
        };

        // Name should be constant, age should be variable
        assert!(person_match.name.is_constant());
        assert!(person_match.age.is_variable());
        assert_eq!(person_match.age.name(), Some("any_age"));
    }

    #[dialog_common::test]
    fn it_creates_person_instance() {
        // Test creating a Person instance
        let entity = Entity::new().unwrap();
        let person = Person {
            this: entity.clone(),
            name: person::Name("Charlie".to_string()),
            age: person::Age(25),
        };

        // Test Instance trait - should return the same entity
        assert_eq!(Conclusion::this(&person), &entity);
    }

    #[dialog_common::test]
    fn it_maintains_concept_name_consistency() {
        // Test that concept identifier is consistent across different access patterns
        let concept = Person::descriptor().clone();
        // Operator is now a URI based on the hash of the concept's attributes
        assert!(
            concept.this().to_string().starts_with("concept:"),
            "Operator should be a concept URI"
        );

        // The concept should have consistent naming
        let _person = Person {
            this: Entity::new().unwrap(),
            name: person::Name("Test".to_string()),
            age: person::Age(1),
        };

        // Instance should have the same concept identifier
        // (though our current Instance impl doesn't store concept info)
        // Verify the identifier is still consistent
        assert!(
            concept.this().to_string().starts_with("concept:"),
            "Operator should be a concept URI"
        );
    }

    #[dialog_common::test]
    fn it_exposes_match_fields() {
        // Test that PersonQuery has the expected fields
        let entity_var = Term::var("entity");
        let name_var: Term<String> = Term::var("name");
        let age_var: Term<u32> = Term::var("age");

        let person_match = Query::<Person> {
            this: entity_var.clone(),
            name: name_var.clone(),
            age: age_var.clone(),
        };

        // Should have this, name, and age fields
        assert_eq!(person_match.this, entity_var);
        assert_eq!(person_match.name, name_var);
        assert_eq!(person_match.age, age_var);
    }

    #[dialog_common::test]
    fn it_formats_debug_output() {
        // Test that our derived Debug implementations work
        let person = Person {
            this: Entity::new().unwrap(),
            name: person::Name("Debug Test".to_string()),
            age: person::Age(42),
        };

        let debug_output = format!("{:?}", person);
        assert!(debug_output.contains("Person"));
        assert!(debug_output.contains("Debug Test"));
        assert!(debug_output.contains("42"));
    }

    #[dialog_common::test]
    fn it_clones_concept() {
        // Test that our derived Clone implementations work
        let entity = Entity::new().unwrap();
        let person1 = Person {
            this: entity.clone(),
            name: person::Name("Original".to_string()),
            age: person::Age(35),
        };

        let person2 = person1.clone();
        assert_eq!(person1.this, person2.this);
        assert_eq!(person1.name, person2.name);
        assert_eq!(person1.age, person2.age);

        // Test PersonQuery clone
        let entity_var = Term::var("entity");
        let match1 = Query::<Person> {
            this: entity_var.clone(),
            name: Term::var("name"),
            age: Term::var("age"),
        };

        let match2 = match1.clone();
        assert_eq!(match1.this, match2.this);
        assert_eq!(match1.name, match2.name);
        assert_eq!(match1.age, match2.age);
    }

    #[dialog_common::test]
    async fn it_matches_concept_structure() -> Result<()> {
        // Test that PersonQuery correctly implements the Match trait
        // This doesn't require actual querying, just tests the structure

        let alice = Entity::new()?;

        // Test 1: Create a PersonQuery with mixed terms
        let person_match = Query::<Person> {
            this: Term::from(alice.clone()),
            name: Term::from("Alice".to_string()),
            age: Term::var("age"),
        };

        // Test that we can convert to Parameters
        let params: Parameters = person_match.clone().into();
        assert!(params.get("this").is_some());
        assert!(params.get("name").is_some());
        assert!(params.get("age").is_some());

        // Test 2: Verify concept attributes are accessible
        let concept = Person::descriptor().clone();
        assert_eq!(concept.with().iter().count(), 2); // name and age

        // Verify we can find specific attributes
        let name_attr = concept.with().iter().find(|(name, _)| *name == "name");
        assert!(name_attr.is_some());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_empty_for_no_matches() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        let missing_query = AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::constant("NonExistent".to_string()),
            Term::blank(),
            None,
        );

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let no_results = missing_query.perform(&source).try_vec().await?;
        assert_eq!(no_results.len(), 0, "Should find no non-existent people");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_queries_with_concept_dsl() -> Result<()> {
        use crate::Query;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        mod employee {
            use crate::Attribute;

            #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            pub struct Name(pub String);

            #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            pub struct Role(pub String);
        }

        #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub struct Employee {
            this: Entity,
            name: employee::Name,
            role: employee::Role,
        }

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let mallory = Entity::new()?;

        branch
            .transaction()
            .assert(Employee {
                this: alice.clone(),
                name: employee::Name("Alice".to_string()),
                role: employee::Role("cryptographer".to_string()),
            })
            .assert(Employee {
                this: bob.clone(),
                name: employee::Name("Bob".to_string()),
                role: employee::Role("janitor".to_string()),
            })
            .assert(
                the!("employee/name")
                    .of(mallory.clone())
                    .is("Mallory".to_string()),
            )
            .assert(
                the!("employee/role")
                    .of(mallory.clone())
                    .is("Hacker".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let employee = Query::<Employee> {
            this: Term::var("this"),
            name: Term::var("name"),
            role: Term::var("role"),
        };

        let mut employees = employee.perform(&source).try_vec().await?;
        employees.sort();
        let mut expected = vec![
            Employee {
                this: bob.clone(),
                name: employee::Name("Bob".to_string()),
                role: employee::Role("janitor".to_string()),
            },
            Employee {
                this: alice.clone(),
                name: employee::Name("Alice".to_string()),
                role: employee::Role("cryptographer".to_string()),
            },
            Employee {
                this: mallory.clone(),
                name: employee::Name("Mallory".to_string()),
                role: employee::Role("Hacker".to_string()),
            },
        ];
        expected.sort();
        assert_eq!(employees, expected);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_negates_concept_with_not_operator() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        mod person {
            use crate::Attribute;

            #[derive(Attribute, Clone, PartialEq)]
            pub struct Name(pub String);

            #[derive(Attribute, Clone, PartialEq)]
            pub struct Age(pub usize);
        }

        #[derive(Concept, Debug, Clone, PartialEq)]
        pub struct Person {
            this: Entity,
            name: person::Name,
            age: person::Age,
        }

        let alice = Entity::new()?;

        let alice_person = Person {
            this: alice.clone(),
            name: person::Name("Alice".to_string()),
            age: person::Age(25),
        };

        branch
            .transaction()
            .assert(alice_person.clone())
            .commit()
            .perform(&operator)
            .await?;

        // Verify Alice exists
        use futures_util::TryStreamExt;

        let name_attr: ArtifactsAttribute = "person/name".parse()?;
        let age_attr: ArtifactsAttribute = "person/age".parse()?;

        let name_facts: Vec<_> = branch
            .claims()
            .select(
                ArtifactSelector::new()
                    .the(name_attr.clone())
                    .of(alice.clone()),
            )
            .perform(&operator)
            .await?
            .try_collect()
            .await?;
        assert_eq!(name_facts.len(), 1, "Should have Alice's name");
        assert_eq!(
            name_facts[0].is,
            Value::String("Alice".to_string()),
            "Name should be Alice"
        );

        let age_facts: Vec<_> = branch
            .claims()
            .select(
                ArtifactSelector::new()
                    .the(age_attr.clone())
                    .of(alice.clone()),
            )
            .perform(&operator)
            .await?
            .try_collect()
            .await?;
        assert_eq!(age_facts.len(), 1, "Should have Alice's age");
        assert_eq!(age_facts[0].is, Value::UnsignedInt(25), "Age should be 25");

        // Now retract using !operator
        branch
            .transaction()
            .retract(alice_person)
            .commit()
            .perform(&operator)
            .await?;

        // Verify Alice has been retracted
        let name_facts_after: Vec<_> = branch
            .claims()
            .select(
                ArtifactSelector::new()
                    .the(name_attr.clone())
                    .of(alice.clone()),
            )
            .perform(&operator)
            .await?
            .try_collect()
            .await?;
        assert_eq!(
            name_facts_after.len(),
            0,
            "Should not have Alice's name after retraction"
        );

        let age_facts_after: Vec<_> = branch
            .claims()
            .select(
                ArtifactSelector::new()
                    .the(age_attr.clone())
                    .of(alice.clone()),
            )
            .perform(&operator)
            .await?
            .try_collect()
            .await?;
        assert_eq!(
            age_facts_after.len(),
            0,
            "Should not have Alice's age after retraction"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_negates_relation_with_not_operator() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let name_attr = the!("user/name");

        let name_relation: AttributeStatement = name_attr
            .clone()
            .of(alice.clone())
            .is("Alice".to_string())
            .into();

        branch
            .transaction()
            .assert(name_relation.clone())
            .commit()
            .perform(&operator)
            .await?;

        // Verify relation exists
        use futures_util::TryStreamExt;

        let facts: Vec<_> = branch
            .claims()
            .select(
                ArtifactSelector::new()
                    .the(name_attr.clone().into())
                    .of(alice.clone()),
            )
            .perform(&operator)
            .await?
            .try_collect()
            .await?;
        assert_eq!(facts.len(), 1, "Should have name relation");

        // Retract using .revert()
        branch
            .transaction()
            .retract(name_relation)
            .commit()
            .perform(&operator)
            .await?;

        // Verify relation has been retracted
        let facts_after: Vec<_> = branch
            .claims()
            .select(
                ArtifactSelector::new()
                    .the(name_attr.clone().into())
                    .of(alice.clone()),
            )
            .perform(&operator)
            .await?
            .try_collect()
            .await?;
        assert_eq!(
            facts_after.len(),
            0,
            "Should not have name relation after retraction"
        );

        Ok(())
    }

    // Tests migrated from tests/attribute_concept_test.rs
    mod person_attr_concept {
        use crate::Attribute;

        #[derive(Attribute, Clone, PartialEq)]
        pub struct Name(pub String);

        #[derive(Attribute, Clone, PartialEq)]
        pub struct Birthday(pub u32);

        #[derive(Attribute, Clone, PartialEq)]
        pub struct Email(pub String);
    }

    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct DerivedPerson {
        pub this: Entity,
        pub name: person_attr_concept::Name,
        pub birthday: person_attr_concept::Birthday,
    }

    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct PersonWithEmail {
        pub this: Entity,
        pub name: person_attr_concept::Name,
        pub email: person_attr_concept::Email,
    }

    #[dialog_common::test]
    async fn it_asserts_concept_with_attribute_fields() -> Result<()> {
        use futures_util::TryStreamExt;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice_id = Entity::new()?;

        let alice = DerivedPerson {
            this: alice_id.clone(),
            name: person_attr_concept::Name("Alice".to_string()),
            birthday: person_attr_concept::Birthday(19830703),
        };

        branch
            .transaction()
            .assert(alice.clone())
            .commit()
            .perform(&operator)
            .await?;

        let name_query = AttributeQuery::new(
            Term::from(the!("person-attr-concept/name")),
            Term::from(alice_id.clone()),
            Term::blank(),
            Term::blank(),
            None,
        );

        let birthday_query = AttributeQuery::new(
            Term::from(the!("person-attr-concept/birthday")),
            Term::from(alice_id.clone()),
            Term::blank(),
            Term::blank(),
            None,
        );

        let name_facts: Vec<_> = name_query
            .perform(&TestEnv::new(&branch, &operator, RuleRegistry::new()))
            .try_collect()
            .await?;

        let birthday_facts: Vec<_> = birthday_query
            .perform(&TestEnv::new(&branch, &operator, RuleRegistry::new()))
            .try_collect()
            .await?;

        assert_eq!(name_facts.len(), 1);
        assert_eq!(birthday_facts.len(), 1);

        assert_eq!(name_facts[0].is, Value::String("Alice".to_string()));
        assert_eq!(birthday_facts[0].is, Value::UnsignedInt(19830703));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_queries_concept_with_attribute_fields() -> Result<()> {
        use futures_util::TryStreamExt;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice_id = Entity::new()?;
        let bob_id = Entity::new()?;

        let alice = DerivedPerson {
            this: alice_id.clone(),
            name: person_attr_concept::Name("Alice".to_string()),
            birthday: person_attr_concept::Birthday(19830703),
        };

        let bob = DerivedPerson {
            this: bob_id.clone(),
            name: person_attr_concept::Name("Bob".to_string()),
            birthday: person_attr_concept::Birthday(19900515),
        };

        branch
            .transaction()
            .assert(alice)
            .assert(bob)
            .commit()
            .perform(&operator)
            .await?;

        let query = Query::<DerivedPerson> {
            this: Term::var("person"),
            name: Term::var("name"),
            birthday: Term::var("birthday"),
        };

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results: Vec<DerivedPerson> = query.perform(&source).try_collect().await?;

        assert_eq!(results.len(), 2);

        let alice_result = results.iter().find(|p| p.name.value() == "Alice");
        let bob_result = results.iter().find(|p| p.name.value() == "Bob");

        assert!(alice_result.is_some());
        assert!(bob_result.is_some());

        assert_eq!(alice_result.unwrap().birthday.value(), &19830703u32);
        assert_eq!(bob_result.unwrap().birthday.value(), &19900515u32);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_queries_concept_with_constant_term() -> Result<()> {
        use futures_util::TryStreamExt;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice_id = Entity::new()?;
        let bob_id = Entity::new()?;

        let alice = DerivedPerson {
            this: alice_id.clone(),
            name: person_attr_concept::Name("Alice".to_string()),
            birthday: person_attr_concept::Birthday(19830703),
        };

        let bob = DerivedPerson {
            this: bob_id.clone(),
            name: person_attr_concept::Name("Bob".to_string()),
            birthday: person_attr_concept::Birthday(19900515),
        };

        branch
            .transaction()
            .assert(alice)
            .assert(bob)
            .commit()
            .perform(&operator)
            .await?;

        let query = Query::<DerivedPerson> {
            this: Term::var("person"),
            name: Term::from("Alice"),
            birthday: Term::var("birthday"),
        };

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results: Vec<DerivedPerson> = query.perform(&source).try_collect().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.value(), "Alice");
        assert_eq!(results[0].birthday.value(), &19830703u32);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_reuses_attributes_across_concepts() -> Result<()> {
        use futures_util::TryStreamExt;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice_id = Entity::new()?;

        let alice_with_email = PersonWithEmail {
            this: alice_id.clone(),
            name: person_attr_concept::Name("Alice".to_string()),
            email: person_attr_concept::Email("alice@example.com".to_string()),
        };

        branch
            .transaction()
            .assert(alice_with_email)
            .commit()
            .perform(&operator)
            .await?;

        let alice_with_birthday = DerivedPerson {
            this: alice_id.clone(),
            name: person_attr_concept::Name("Alice".to_string()),
            birthday: person_attr_concept::Birthday(19830703),
        };

        branch
            .transaction()
            .assert(alice_with_birthday)
            .commit()
            .perform(&operator)
            .await?;

        let name_query = AttributeQuery::new(
            Term::from(the!("person-attr-concept/name")),
            Term::from(alice_id.clone()),
            Term::blank(),
            Term::blank(),
            None,
        );

        let email_query = AttributeQuery::new(
            Term::from(the!("person-attr-concept/email")),
            Term::from(alice_id.clone()),
            Term::blank(),
            Term::blank(),
            None,
        );

        let birthday_query = AttributeQuery::new(
            Term::from(the!("person-attr-concept/birthday")),
            Term::from(alice_id.clone()),
            Term::blank(),
            Term::blank(),
            None,
        );

        let name_facts: Vec<_> = name_query
            .perform(&TestEnv::new(&branch, &operator, RuleRegistry::new()))
            .try_collect()
            .await?;

        let email_facts: Vec<_> = email_query
            .perform(&TestEnv::new(&branch, &operator, RuleRegistry::new()))
            .try_collect()
            .await?;

        let birthday_facts: Vec<_> = birthday_query
            .perform(&TestEnv::new(&branch, &operator, RuleRegistry::new()))
            .try_collect()
            .await?;

        assert_eq!(name_facts.len(), 1);
        assert_eq!(email_facts.len(), 1);
        assert_eq!(birthday_facts.len(), 1);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_retracts_concept_with_attributes() -> Result<()> {
        use futures_util::TryStreamExt;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice_id = Entity::new()?;

        let alice = DerivedPerson {
            this: alice_id.clone(),
            name: person_attr_concept::Name("Alice".to_string()),
            birthday: person_attr_concept::Birthday(19830703),
        };

        branch
            .transaction()
            .assert(alice.clone())
            .commit()
            .perform(&operator)
            .await?;

        branch
            .transaction()
            .retract(alice)
            .commit()
            .perform(&operator)
            .await?;

        let name_query = AttributeQuery::new(
            Term::from(the!("person-attr-concept/name")),
            Term::from(alice_id.clone()),
            Term::blank(),
            Term::blank(),
            None,
        );

        let birthday_query = AttributeQuery::new(
            Term::from(the!("person-attr-concept/birthday")),
            Term::from(alice_id),
            Term::blank(),
            Term::blank(),
            None,
        );

        let name_facts: Vec<_> = name_query
            .perform(&TestEnv::new(&branch, &operator, RuleRegistry::new()))
            .try_collect()
            .await?;

        let birthday_facts: Vec<_> = birthday_query
            .perform(&TestEnv::new(&branch, &operator, RuleRegistry::new()))
            .try_collect()
            .await?;

        assert_eq!(name_facts.len(), 0);
        assert_eq!(birthday_facts.len(), 0);

        Ok(())
    }

    // Tests migrated from tests/concept_query_shortcut_test.rs
    mod shortcut_employee {
        use crate::Attribute;

        #[derive(Attribute, Clone)]
        pub struct Name(pub String);

        #[derive(Attribute, Clone)]
        pub struct Job(pub String);
    }

    #[derive(Concept, Debug, Clone)]
    pub struct ShortcutEmployee {
        pub this: Entity,
        pub name: shortcut_employee::Name,
        pub job: shortcut_employee::Job,
    }

    #[dialog_common::test]
    async fn it_queries_concept_via_shortcut() -> Result<()> {
        use futures_util::TryStreamExt;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;
        let bob = Entity::new()?;

        branch
            .transaction()
            .assert(ShortcutEmployee {
                this: alice.clone(),
                name: shortcut_employee::Name("Alice".into()),
                job: shortcut_employee::Job("Engineer".into()),
            })
            .assert(ShortcutEmployee {
                this: bob.clone(),
                name: shortcut_employee::Name("Bob".into()),
                job: shortcut_employee::Job("Designer".into()),
            })
            .commit()
            .perform(&operator)
            .await?;

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let employees_shortcut: Vec<ShortcutEmployee> = Query::<ShortcutEmployee>::default()
            .perform(&source)
            .try_collect()
            .await?;

        let employees_explicit: Vec<ShortcutEmployee> = Query::<ShortcutEmployee>::default()
            .perform(&source)
            .try_collect()
            .await?;

        assert_eq!(employees_shortcut.len(), 2);
        assert_eq!(employees_explicit.len(), 2);

        let mut found_alice = false;
        let mut found_bob = false;

        for emp in &employees_shortcut {
            if emp.name.value() == "Alice" {
                assert_eq!(emp.job.value(), "Engineer");
                found_alice = true;
            } else if emp.name.value() == "Bob" {
                assert_eq!(emp.job.value(), "Designer");
                found_bob = true;
            }
        }

        assert!(found_alice, "Should find Alice");
        assert!(found_bob, "Should find Bob");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_filters_concept_query_via_shortcut() -> Result<()> {
        use futures_util::TryStreamExt;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;

        branch
            .transaction()
            .assert(ShortcutEmployee {
                this: alice.clone(),
                name: shortcut_employee::Name("Alice".into()),
                job: shortcut_employee::Job("Engineer".into()),
            })
            .commit()
            .perform(&operator)
            .await?;

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let result1: Vec<ShortcutEmployee> = Query::<ShortcutEmployee>::default()
            .perform(&source)
            .try_collect()
            .await?;

        let result2: Vec<ShortcutEmployee> = Query::<ShortcutEmployee> {
            this: Term::var("this"),
            name: Term::var("name"),
            job: Term::var("job"),
        }
        .perform(&source)
        .try_collect()
        .await?;

        let result3: Vec<ShortcutEmployee> = Query::<ShortcutEmployee>::default()
            .perform(&source)
            .try_collect()
            .await?;

        assert_eq!(result1.len(), 1);
        assert_eq!(result2.len(), 1);
        assert_eq!(result3.len(), 1);

        assert_eq!(result1[0].name.value(), result2[0].name.value());
        assert_eq!(result2[0].name.value(), result3[0].name.value());

        Ok(())
    }

    // Tests migrated from tests/query_helper_comprehensive_test.rs
    mod helper_person {
        use crate::Attribute;

        #[derive(Attribute, Clone, PartialEq)]
        pub struct Name(pub String);
    }

    mod helper_employee {
        use crate::Attribute;

        #[derive(Attribute, Clone, PartialEq)]
        pub struct Name(pub String);

        #[derive(Attribute, Clone, PartialEq)]
        pub struct Department(pub String);
    }

    #[derive(Concept, Debug, Clone)]
    pub struct HelperPerson {
        pub this: Entity,
        pub name: helper_person::Name,
    }

    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct HelperEmployee {
        pub this: Entity,
        pub name: helper_employee::Name,
        pub department: helper_employee::Department,
    }

    #[dialog_common::test]
    async fn it_queries_single_attribute() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        branch
            .transaction()
            .assert(helper_person::Name::of(alice).is("Alice"))
            .assert(helper_person::Name::of(bob).is("Bob"))
            .commit()
            .perform(&operator)
            .await?;

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let alice_query = Query::<HelperPerson> {
            this: Term::var("person"),
            name: Term::from("Alice".to_string()),
        };

        let results = alice_query.perform(&source).try_vec().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.value(), "Alice");

        let all_people_query = Query::<HelperPerson> {
            this: Term::var("person"),
            name: Term::var("name"),
        };

        let all_results = all_people_query.perform(&source).try_vec().await?;
        assert_eq!(all_results.len(), 2);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_queries_multi_attribute_with_constants() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        branch
            .transaction()
            .assert(helper_employee::Name::of(alice.clone()).is("Alice"))
            .assert(helper_employee::Department::of(alice.clone()).is("Engineering"))
            .assert(helper_employee::Name::of(bob.clone()).is("Bob"))
            .assert(helper_employee::Department::of(bob).is("Sales"))
            .commit()
            .perform(&operator)
            .await?;

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let alice_engineering_query = Query::<HelperEmployee> {
            this: Term::var("employee"),
            name: Term::from("Alice".to_string()),
            department: Term::from("Engineering".to_string()),
        };

        let results = alice_engineering_query.perform(&source).try_vec().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.value(), "Alice");
        assert_eq!(results[0].department.value(), "Engineering");
        assert_eq!(results[0].this, alice);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_multi_attribute_variable_limitation() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        branch
            .transaction()
            .assert(helper_employee::Name::of(alice.clone()).is("Alice"))
            .assert(helper_employee::Department::of(alice.clone()).is("Engineering"))
            .assert(helper_employee::Name::of(bob.clone()).is("Bob"))
            .assert(helper_employee::Department::of(bob.clone()).is("Sales"))
            .commit()
            .perform(&operator)
            .await?;

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let engineering_query = Query::<HelperEmployee> {
            this: Term::var("employee"),
            name: Term::var("name"),
            department: Term::from("Engineering".to_string()),
        };

        let results = engineering_query.perform(&source).try_vec().await?;
        assert_eq!(
            results,
            vec![HelperEmployee {
                this: alice.clone(),
                name: helper_employee::Name("Alice".into()),
                department: helper_employee::Department("Engineering".into())
            }]
        );

        Ok(())
    }
}
