use dialog_query::{
    Attribute, Cardinality, Entity, Term, ValueDataType,
};
use tokio::io::Empty;

// #[relation]
// struct Employee {
//     name: String,
//     job: String,
// }

#[allow(non_snake_case)]
pub mod Employee {
    use dialog_query::{
        error::QueryResult,
        syntax::VariableScope,
        Query, Syntax,
    };
    use dialog_artifacts::{Artifact, ArtifactStore, DialogArtifactsError};
    use futures_util::Stream;
    pub use dialog_query::{Entity, Term};
    // Defines Employee model that will be returned when queries are matched
    struct Model {
        this: Entity,
        name: String,
        job: String,
    }

    // Defines predicate that can be used in queries
    pub struct Match {
        pub this: Term<Entity>,
        pub name: Term<String>,
        pub job: Term<String>,
    }
    impl Match {
        pub fn not(&self) -> Not {
            Not {
                this: self.this.clone(),
                name: self.name.clone(),
                job: self.job.clone(),
            }
        }
    }
    impl Default for Match {
        fn default() -> Self {
            Self {
                this: Term::var("self"),
                name: Term::var("name"),
                job: Term::var("job"),
            }
        }
    }

    pub struct MatchPlan;
    impl Syntax for Match {
        type Plan = MatchPlan;
        fn plan(&self, scope: &VariableScope) -> QueryResult<Self::Plan> {
            Ok(MatchPlan)
        }
    }
    
    impl Query for MatchPlan {
        fn query<S>(
            &self,
            _store: &S,
        ) -> QueryResult<impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 'static>
        where
            S: ArtifactStore,
        {
            Ok(futures_util::stream::empty())
        }
    }

    pub struct Claim {
        pub this: Entity,
        pub name: String,
        pub job: String,
    }

    // Define negation predicate
    pub struct Not {
        pub this: Term<Entity>,
        pub name: Term<String>,
        pub job: Term<String>,
    }

    pub mod name_api {
        pub use dialog_query::{Entity, Term};
        pub struct PredicateBuilder;
        impl PredicateBuilder {
            pub fn of(&self, term: impl Into<Term<Entity>>) -> Of {
                Of(term.into())
            }
        }
        pub struct Match {
            pub of: Term<Entity>,
            pub is: Term<String>,
        }
        impl Default for Match {
            fn default() -> Self {
                Self {
                    of: Term::var("of"),
                    is: Term::var("is"),
                }
            }
        }
        pub struct Not {
            pub of: Term<Entity>,
            pub is: Term<String>,
        }

        pub struct Of(pub Term<Entity>);
        impl Of {
            pub fn is(self, term: impl Into<Term<String>>) -> Match {
                Match {
                    of: self.0,
                    is: term.into(),
                }
            }
            pub fn not(self, term: impl Into<Term<String>>) -> Not {
                Not {
                    of: self.0,
                    is: term.into(),
                }
            }
        }
    }

    pub mod job_api {
        pub use dialog_query::{Entity, Term};
        pub struct PredicateBuilder;
        impl PredicateBuilder {
            pub fn of(&self, term: impl Into<Term<Entity>>) -> Of {
                Of(term.into())
            }
        }
        pub struct Match {
            pub of: Term<Entity>,
            pub is: Term<String>,
        }
        impl Default for Match {
            fn default() -> Self {
                Self {
                    of: Term::var("of"),
                    is: Term::var("is"),
                }
            }
        }
        pub struct Not {
            pub of: Term<Entity>,
            pub is: Term<String>,
        }

        pub struct Of(pub Term<Entity>);
        impl Of {
            pub fn is(self, term: impl Into<Term<String>>) -> Match {
                Match {
                    of: self.0,
                    is: term.into(),
                }
            }
            pub fn not(self, term: impl Into<Term<String>>) -> Not {
                Not {
                    of: self.0,
                    is: term.into(),
                }
            }
        }
    }

    #[allow(non_snake_case)]
    pub const name: name_api::PredicateBuilder = name_api::PredicateBuilder {};
    #[allow(non_snake_case)]
    pub const job: job_api::PredicateBuilder = job_api::PredicateBuilder {};
}

fn main() {
    let entity = Term::<Entity>::var("self");

    // Predicate using owned variable
    let employee = Employee::Match {
        this: entity.clone().into(),
        name: "John Doe".into(),
        job: "Software Engineer".into(),
    };

    let find = Employee::Match::default();

    // This now works! We can pass variable references thanks to From<&TypedVariable<T>>
    let constraint = Employee::name.of(&entity).is("John Doe");
    let exclude = Employee::name.of(&entity).not("Jane Doe");
    //
    //
    //

    let other = Term::var("other");
    let engineer = Employee::job.of(other).is(Term::Any);

    // We can also use owned variables
    let entity_owned = Term::<Entity>::var("owned_entity");
    let constraint_owned = Employee::name.of(entity_owned).is("Jane Doe");

    println!("✅ Variable references work: Employee::name.of(&entity).is(...)");
    println!("✅ Owned variables work: Employee::name.of(entity).is(...)");
    println!("✅ Both create Match structs for pattern matching!");

    // We can still use the entity variable after passing a reference
    let another_constraint = Employee::name.of(&entity).is("Bob Smith");

    println!("✅ Variable can be reused after passing by reference!");
}
