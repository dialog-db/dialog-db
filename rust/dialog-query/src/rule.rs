//! Rule-based deduction and induction system.
//!
//! Two rule kinds share the same analysis pipeline and differ only at
//! evaluation time:
//!
//! - [`DeductiveRule`] derives new facts on demand when its body is
//!   queried. Standard Datalog semantics.
//! - [`InductiveRule`] (a.k.a. *effect*) asserts its head facts when
//!   its body matches during commit-time evaluation. Fires the
//!   reactor's fixpoint loop; trigger facts on `effect:system` are
//!   ephemeral.
//!
//! The [`Rule`] enum carries either variant and is what compile-time
//! analysis errors (in [`TypeError`](crate::TypeError) and
//! [`AnalyzerError`](crate::AnalyzerError)) reference, so error
//! reporting is uniform across both kinds.

use crate::concept::descriptor::ConceptDescriptor;
use crate::error::TypeError;
use crate::planner::{Conjunction, Planner};
use crate::premise::Premise;
use crate::{Environment, Type};
use std::fmt::{Display, Formatter, Result as FmtResult};

/// Rule analysis — inference and dependency graph over premises.
pub mod analyzer;
/// Deductive rule definitions for deriving new facts.
pub mod deductive;
/// Inductive rule definitions (a.k.a. effects).
pub mod inductive;
/// Premises collection type.
pub mod premises;
/// Type inference over a rule's premises.
pub mod types;
/// When trait and tuple implementations.
pub mod when;

pub use analyzer::{AnalyzedRule, analyze};
pub use deductive::DeductiveRule;
pub use deductive::descriptor::DeductiveRuleDescriptor;
pub use inductive::InductiveRule;
pub use inductive::descriptor::InductiveRuleDescriptor;
pub use premises::*;
pub use types::TypeEnv;
pub use when::*;

/// A compiled rule — either deductive or inductive.
///
/// Both variants share the same head ([`ConceptDescriptor`]) and
/// body ([`Conjunction`]); they differ in what the evaluator does
/// with a matching body. Errors raised during compile-time analysis
/// reference the in-progress rule via this enum so the same error
/// type works for both kinds.
#[derive(Debug, Clone, PartialEq)]
pub enum Rule {
    /// A deductive rule — yields tuples on query.
    Deductive(DeductiveRule),
    /// An inductive rule — asserts head facts on commit.
    Inductive(InductiveRule),
}

impl Rule {
    /// The conclusion (head) of this rule.
    pub fn conclusion(&self) -> &ConceptDescriptor {
        match self {
            Rule::Deductive(r) => r.conclusion(),
            Rule::Inductive(r) => r.conclusion(),
        }
    }
}

impl Display for Rule {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Rule::Deductive(r) => Display::fmt(r, f),
            Rule::Inductive(r) => Display::fmt(r, f),
        }
    }
}

impl From<DeductiveRule> for Rule {
    fn from(r: DeductiveRule) -> Self {
        Rule::Deductive(r)
    }
}

impl From<InductiveRule> for Rule {
    fn from(r: InductiveRule) -> Self {
        Rule::Inductive(r)
    }
}

/// Common construction surface for both rule kinds.
///
/// Implementors are constructible from a head + planned body via
/// [`from_parts`](Compile::from_parts), and convertible into a
/// [`Rule`] so analysis errors can refer to the in-progress rule
/// uniformly. The default [`compile`](Compile::compile) runs the
/// shared analysis pipeline (planner + unbound-variable check).
pub trait Compile: Sized + Into<Rule> {
    /// Build the rule from its compiled parts. Called by
    /// [`compile`](Self::compile) once analysis passes.
    fn from_parts(conclusion: ConceptDescriptor, join: Conjunction) -> Self;

    /// Plan the premises, run rule-level type analysis, and verify
    /// every head variable is bound. Default impl is identical for
    /// deductive and inductive rules; the only difference between
    /// the kinds lives at evaluation time, not compile time.
    fn compile(conclusion: ConceptDescriptor, premises: Vec<Premise>) -> Result<Self, TypeError> {
        // A concept with no required (`with`) attributes is
        // unconstructable (see `ConceptDescriptor`'s `TryFrom` /
        // `Deserialize` and the `#[derive(Concept)]` compile-time
        // assertion), so `conclusion` is guaranteed non-degenerate
        // here — no explicit emptiness check is needed.

        // Plan the order of premises in a scope where none of the
        // rule parameters are bound to find the optimal execution
        // order, or to discover unsatisfiable premises (e.g. a
        // formula whose required cell is never derived by another
        // premise).
        let join = Planner::from(premises).plan(&Environment::new())?;

        // Run rule-level type analysis: inference + required-head
        // check + Coalesce contract validation. Failures wrap into
        // the corresponding `TypeError::*` variants so the user
        // sees the in-progress rule embedded in the error.
        if let Err(err) = analyzer::analyze(conclusion.clone(), &join.steps) {
            let in_progress = Self::from_parts(conclusion, join);
            return Err(match err {
                analyzer::AnalysisError::Inference { reason } => {
                    TypeError::TypeInference { reason }
                }
                analyzer::AnalysisError::RequiredHeadFromOptional { variable } => {
                    TypeError::RequiredHeadFromOptional {
                        rule: Box::new(in_progress.into()),
                        variable,
                    }
                }
                analyzer::AnalysisError::CoalesceTypeMismatch { reason } => {
                    TypeError::CoalesceTypeMismatch {
                        rule: Box::new(in_progress.into()),
                        reason,
                    }
                }
            });
        }

        // Verify that every conclusion parameter is derived by one
        // of the premises; otherwise the rule could never fully
        // bind its output. Build `Self` only when needed for the
        // error path so the happy path doesn't allocate twice.
        let unbound = conclusion
            .operands()
            .find(|name| !join.binds.contains(name))
            .map(String::from);
        if let Some(variable) = unbound {
            let in_progress = Self::from_parts(conclusion, join);
            return Err(TypeError::UnboundVariable {
                rule: Box::new(in_progress.into()),
                variable,
            });
        }

        Ok(Self::from_parts(conclusion, join))
    }
}

/// Helper for the [`Display`] impls on [`DeductiveRule`] and
/// [`InductiveRule`] — both render their schema the same way.
pub(crate) fn fmt_rule_schema(conclusion: &ConceptDescriptor, f: &mut Formatter<'_>) -> FmtResult {
    write!(f, "{} {{", conclusion.this())?;
    write!(f, "this: {},", Type::Entity)?;
    for (name, attribute) in conclusion.with().iter() {
        match attribute.content_type() {
            Some(ty) => write!(f, "{}: {},", name, ty)?,
            None => write!(f, "{}: Any,", name)?,
        }
    }
    write!(f, "}}")
}

/// Macro for creating When collections with clean array-like syntax
///
/// This macro provides the most concise way to create rule conditions:
///
/// ```rust
/// use dialog_query::{when, When, Term, Any, artifact::Value, the};
/// use dialog_query::AttributeQuery;
///
/// fn example() -> impl When {
///     let r1 = AttributeQuery::new(
///         Term::from(the!("ns/attr1")),
///         Term::var("entity"),
///         Term::<Any>::constant("value1".to_string()),
///         Term::blank(),
///         None,
///     );
///     let r2 = AttributeQuery::new(
///         Term::from(the!("ns/attr2")),
///         Term::var("entity"),
///         Term::<Any>::var("value2"),
///         Term::blank(),
///         None,
///     );
///
///     when![r1, r2]
/// }
/// ```
#[macro_export]
macro_rules! when {
    [$($item:expr),* $(,)?] => {
        $crate::rule::Premises::from(vec![$($item),*])
    };
}

#[cfg(test)]
mod tests {
    extern crate self as dialog_query;

    use super::*;
    use crate::artifact::{Entity, Type};
    use crate::concept::descriptor::ConceptDescriptor;
    use crate::term::Term;
    use crate::{AttributeStatement, Query};

    // Define a Person concept for testing via `#[derive(Concept)]`.
    // The newtypes live in a module named `person` so the attribute
    // domain defaults to `person`, yielding `person/name` and
    // `person/age` to match the facts the scaffold previously asserted.
    mod person {
        use crate::Attribute;

        /// Name of the person
        #[derive(Attribute, Clone, PartialEq)]
        pub struct Name(pub String);

        /// Age of the person
        #[derive(Attribute, Clone, PartialEq)]
        pub struct Age(pub u32);
    }

    /// A person concept used by the rule scaffold tests below.
    #[derive(crate::Concept, Debug, Clone)]
    pub struct Person {
        pub this: Entity,
        /// Name of the person (`person/name`)
        pub name: person::Name,
        /// Age of the person (`person/age`)
        pub age: person::Age,
    }

    #[dialog_common::test]
    async fn it_installs_rule() {
        // Define a rule function using the clean API
        fn person_rule(person: Query<Person>) -> impl When {
            (Query::<Person> {
                this: person.this,
                name: person.name,
                age: person.age,
            },)
        }

        // Verify rule installs into registry
        use crate::rule::deductive::DeductiveRule;
        use crate::session::RuleRegistry;
        let mut rules = RuleRegistry::new();
        let concept = Person::descriptor().clone();
        let premises = person_rule(Query::<Person>::default())
            .into_premises()
            .into_vec();
        let rule = DeductiveRule::new(concept, premises).unwrap();
        let result = rules.register(rule);
        assert!(result.is_ok(), "register should work");
    }

    mod macro_person {
        use crate::Attribute;

        /// Name of the person
        #[derive(Attribute, Clone, PartialEq)]
        pub struct Name(pub String);

        /// Birthday of the person
        #[derive(Attribute, Clone, PartialEq)]
        pub struct Birthday(pub u32);
    }

    #[derive(crate::Concept, Debug, Clone)]
    pub struct MacroPerson {
        /// Person entity
        pub this: Entity,

        /// Name of the person
        pub name: macro_person::Name,

        /// Birthday of the person
        pub birthday: macro_person::Birthday,
    }

    #[dialog_common::test]
    fn it_generates_derived_rule_types() {
        // Test that the generated module and types exist
        let entity = Term::var("person_entity");

        // Test the generated Query struct
        let _person_match = Query::<MacroPerson> {
            this: entity.clone(),
            name: Term::var("person_name"),
            birthday: Term::var("person_birthday"),
        };

        // Test that MacroPerson implements Concept
        let concept: ConceptDescriptor = MacroPerson::descriptor().clone();
        // Operator is now a computed URI
        assert!(
            concept.this().to_string().starts_with("concept:"),
            "Operator should be a concept URI"
        );

        // Test the attributes() method
        let attrs = concept.with().iter().collect::<Vec<_>>();

        assert_eq!(attrs.len(), 2);
        assert_eq!(attrs[0].0, "name");
        assert_eq!(attrs[0].1.domain(), "macro-person");
        assert_eq!(attrs[0].1.name(), "name");
        assert_eq!(attrs[0].1.description(), "Name of the person");
        assert_eq!(attrs[0].1.content_type(), Some(Type::String));
        assert_eq!(attrs[1].0, "birthday");
        assert_eq!(attrs[1].1.domain(), "macro-person");
        assert_eq!(attrs[1].1.name(), "birthday");
        assert_eq!(attrs[1].1.description(), "Birthday of the person");
        assert_eq!(attrs[1].1.content_type(), Some(Type::UnsignedInt));

        // Test that MacroPerson implements Rule
        let test_match = Query::<MacroPerson> {
            this: Term::var("person"),
            name: Term::var("name"),
            birthday: Term::var("birthday"),
        };

        let when_result = MacroPerson::when(test_match);
        assert_eq!(when_result.len(), 2); // Should have 2 field statements
    }

    #[dialog_common::test]
    fn it_exposes_attribute_descriptors() {
        // Test that attribute descriptors are accessible via inherent methods
        let name_desc = macro_person::Name::descriptor();
        let birthday_desc = macro_person::Birthday::descriptor();
        assert_eq!(name_desc.domain(), "macro-person");
        assert_eq!(name_desc.name(), "name");
        assert_eq!(birthday_desc.domain(), "macro-person");
        assert_eq!(birthday_desc.name(), "birthday");
    }

    mod macro_employee {
        use crate::Attribute;

        /// Person's first name
        #[derive(Attribute, Clone, PartialEq)]
        pub struct GivenName(pub String);

        /// Person's preferred nickname
        #[derive(Attribute, Clone, PartialEq)]
        pub struct Nickname(pub String);

        /// Person's age in years
        #[derive(Attribute, Clone, PartialEq)]
        pub struct Age(pub u32);
    }

    /// Concept with both required and optional fields. Exercises the
    /// `Option<T>` branch of the `#[derive(Concept)]` macro: typed
    /// `Term<Option<U>>` query field, optional descriptor pair into
    /// `with_maybe`, and optional realize via `Binding`.
    #[derive(crate::Concept, Debug, Clone)]
    pub struct MacroEmployee {
        /// Employee entity
        pub this: Entity,
        /// Required given name
        pub given_name: macro_employee::GivenName,
        /// Optional preferred nickname
        pub nickname: Option<macro_employee::Nickname>,
        /// Optional age
        pub age: Option<macro_employee::Age>,
    }

    #[dialog_common::test]
    fn it_emits_typed_optional_terms_in_macro() {
        // The query struct must accept `Term<Option<String>>` and
        // `Term<Option<u32>>` for the optional fields. Constructing the
        // query with these types is itself a compile-time test.
        let _query = Query::<MacroEmployee> {
            this: Term::var("emp"),
            given_name: Term::<String>::var("emp_given"),
            nickname: Term::<Option<String>>::var("emp_nickname"),
            age: Term::<Option<u32>>::var("emp_age"),
        };

        // Default-constructed query: every field is a named variable
        // including the optional ones.
        let default = Query::<MacroEmployee>::default();
        assert!(matches!(default.this, Term::Variable { .. }));
        assert!(matches!(default.given_name, Term::Variable { .. }));
        assert!(matches!(default.nickname, Term::Variable { .. }));
        assert!(matches!(default.age, Term::Variable { .. }));

        // Concept descriptor: required field flows into `with`,
        // optional fields flow into `maybe`.
        let concept: ConceptDescriptor = MacroEmployee::descriptor().clone();
        let with: Vec<_> = concept.with().iter().collect();
        assert_eq!(with.len(), 1);
        assert_eq!(with[0].0, "given-name");

        let maybe = concept.maybe().expect("concept has maybe attributes");
        let maybe: Vec<_> = maybe.iter().collect();
        assert_eq!(maybe.len(), 2);
        assert_eq!(maybe[0].0, "nickname");
        assert_eq!(maybe[1].0, "age");

        // Rule body emits one attribute query per field (required
        // *and* optional), with the resolution chosen by each
        // field's `<F as ConceptField>::RESOLUTION` const. For
        // MacroEmployee (1 required + 2 optional fields), `when()`
        // returns 3 attribute queries.
        let when_result = MacroEmployee::when(Query::<MacroEmployee>::default());
        assert_eq!(when_result.len(), 3);
    }

    #[dialog_common::test]
    fn it_persists_only_some_optional_values() {
        // IntoIterator emits a relation per field: required always,
        // optional only when `Some(_)`. With one Some and one None,
        // we should see exactly two statements (1 required + 1 Some).
        let entity = Entity::new().unwrap();
        let employee = MacroEmployee {
            this: entity,
            given_name: macro_employee::GivenName("Ada".into()),
            nickname: Some(macro_employee::Nickname("AL".into())),
            age: None,
        };

        let statements: Vec<AttributeStatement> = employee.into_iter().collect();
        assert_eq!(statements.len(), 2);
    }

    /// Aliasing `Option` to another name still routes through the
    /// `ConceptField` impl for `Option<N>`. Proves the macro does
    /// not depend on syntactic detection of the literal `Option`
    /// identifier — Rust's type system resolves the alias to the
    /// underlying `Option<N>` shape at type-check time, picking
    /// up the right blanket impl.
    #[dialog_common::test]
    fn it_routes_optional_through_alias_via_concept_field() {
        use core::option::Option as Maybe;

        #[derive(crate::Concept, Debug, Clone)]
        #[allow(dead_code)]
        pub struct AliasedConcept {
            /// Entity
            pub this: Entity,
            /// Required name
            pub given_name: macro_employee::GivenName,
            /// Optional nickname, spelled via an aliased Option
            pub nickname: Maybe<macro_employee::Nickname>,
        }

        // Concept descriptor must route `nickname` into `maybe`,
        // not `with`. If the macro were doing syntactic Option
        // detection by ident name, `Maybe` would not match and
        // `nickname` would land in `with` — wrong.
        let concept: ConceptDescriptor = AliasedConcept::descriptor().clone();
        let with: Vec<_> = concept.with().iter().collect();
        assert_eq!(with.len(), 1);
        assert_eq!(with[0].0, "given-name");

        let maybe = concept
            .maybe()
            .expect("AliasedConcept must have a `maybe` slot");
        let maybe_entries: Vec<_> = maybe.iter().collect();
        assert_eq!(maybe_entries.len(), 1);
        assert_eq!(maybe_entries[0].0, "nickname");
    }
}
