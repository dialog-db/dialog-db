# Rules

I like to take inspiration from [Bevy]( https://bevy.org/learn/quick-start/getting-started/ecs/) as it has nice and intuitive APIs. Here is the quote that is precisely the mindset of dialog

> Add this struct to your main.rs file:
> ```rs
> #[derive(Component)]
> struct Person;
> ```
> But what if we want our people to have a name? In a more traditional design, we might just tack on a `name: String` field to Person. But other entities might have names too! For example, dogs should probably also have a name. It often makes sense to break up datatypes into small pieces to encourage code reuse. So let's make `Name` its own component:
>
> ```rs
> #[derive(Component)]
> struct Name(String);
> ```

## Derived Rules

In dialog we have attributes that act very similar, however dialog needs to worry about on-wire serialization and interaction across tools in different languages, which comes with extra design considerations:

1. All attributes should have namespace to avoid unintential name collisions.
2. Members need to also have names.

For this reason we take slightly different approach illustrated in by the following example:

```rs
#[derive(Rule)]
pub struct Person {
    /// Name of the person
    pub name: String,
    /// Name of the person
    pub birthday: u32,
}
```

This should correspond roughly to the following datomic schema:

```clj
In datomic this will translate to following schema

```clj
{:db/ident :person/name
 :db/valueType :db.type/string
 :db/cardinality :db.cardinality/one
 :db/doc "Name of the person"}

{:db/ident :person/birthday
 :db/valueType :db.type/long
 :db/cardinality :db.cardinality/one
 :db/doc "Birthday of the person"}
```

Derived `Rule` should also expand to the following form:

```rs
use dialog_query::attribute::{Attribute, MatchAttribute};
use dialog_query::Entity;
use dialog_query::fact::Scalar;
use std::marker::PhantomData;

#[allow(non_snake_case)]
/// Creates a match for MicroshaftEmployee by entity
pub fn Person<Of: Into<Term<Entity>>>(term: Of) -> person::Attributes {
    person::r#match(term)
}

impl Person {
    pub fn r#match<Of: Into<Term<Entity>>>(term: Of) -> person::Attributes {
        let of: Term<Entity> = term.into();
        person::Attributes {
            name: MatchAttribute::new(person::THE, "name", of.clone()),
            birthday: MatchAttribute::new(person::THE, "birthday", of.clone()),

        }
    }

    /// Builder for the `name` attribute predicate. Takes entity /// for which `name` attribute predicate is being created.
    pub fn name<T: Into<Term<Entity>>>(of: T) -> MatchAttribute<String> {
        MatchAttribute::new(person::THE, "name", of.into())
    }

    /// Builder for the `birthday` attribute predicate.
    /// Takes entity for which `birthday` attribute predicate is // being created.
    pub fn birthday<T: Into<Term<Entity>>>(of: T) -> MatchAttribute<u32> {
        MatchAttribute::new(person::THE, "birthday", of.into())
    }
}

pub mod person {
    use dialog_query::{Entity, Scalar};
    use dialog_query::attribute::{MatchAttribute};
    pub use dialog_query::{FactSelector, Term};

    pub const THE: &'static str = "person";

    /// Attributes of the person relation.
    pub struct Attributes {
        pub name: MatchAttribute<String>,
        pub birthday: MatchAttribute<u32>,
    }

    /// Pattern for matching person relations.
    pub struct Match {
        pub this: Term<Entity>,
        pub name: Term<String>,
        pub birthday: Term<u32>,
    }

    impl Predicate for Match {
        pub fn plan(&self, scope: &VariableScope) -> PlanResult<Plan<Person>> {
            todo!("implement match planner")
        }
    }

    pub struct Not {
        pub this: Term<Entity>,
        pub name: Term<String>,
        pub birthday: Term<u32>,
    }

    impl Predicate for Not {
        pub fn plan(&self, scope: &VariableScope) -> PlanResult<Plan<Person>> {
            todo!("implement negation planner")
        }
    }

    /// Pattern for claiming person relation from rules.
    pub struct Claim {
        pub name: Term<String>,
        pub birthday: Term<u32>,
    }

    /// Term for the entity can be derived from the Match
    impl From<Match> for Term<Entity> {
        fn from(source: Match) -> Self {
            source.this
        }
    }

    /// Default implementation for the Match
    impl Default for Match {
        fn default() -> Self {
            Self {
                this: Term::var("this"),
                name: Term::var("name"),
                birthday: Term::var("birthday"),
            }
        }
    }
  }
}
```


In datomic this will translate to following schema

```clj
{:db/ident :microshaft.employee/name
 :db/valueType :db.type/string
 :db/cardinality :db.cardinality/one}

{:db/ident :microshaft.employee/job
 :db/valueType :db.type/string
 :db/cardinality :db.cardinality/one}

{:db/ident :microshaft.employee/salary
 :db/valueType :db.type/long
 :db/cardinality :db.cardinality/one}

{:db/ident :microshaft.employee/address
 :db/valueType :db.type/string
 :db/cardinality :db.cardinality/many}
```

### Querying using derived rules

Structs that derive rules can be used to query the database for the matching facts

```rs
fn main() {
    // Define a variable for the person entity
    let person = Term::var("person");

    // Constraint that can be used match facts in which entity
    // has `person/name` attribute with value `John`
    let _named_john = Person(&person).name.is("John");

    let _birthday = Person(&person).birthday.is(1983_07_03 as u32);

    // Predicate that can be used to query for `Person` with given
    // name `John` and birthday `1983_07_03`
    let john = Match<Person> {
        this: person.clone(),
        name: Term::from("John"),
        birthday: Term::from(1983_07_03u32),
    };

    let store = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(store).await?;
    let dialog = Dialog::open(artifacts);

    // Find for all `Person` that have name `John` and birthday `1983_07_03`
    let results = dialog.query(john).await?;
}
```

## Defined Rules

So far we have looked at basic rules that are derived from _premise_ - set of related attributes on an entity. Such rules
simply deduce premise by joining all attributes by an entity - They basically look for persisted facts in the database.

In datalog you can define rules that deduce premise by a more complicated logic that span across number of entities. With extensions like [dedalus](https://dsf.berkeley.edu/papers/datalog2011-dedalus.pdf) you can even define behaviors of distributed systems.

Here is an example of such a rule in a notation inspired by dedalus.

> In our notation we will have recursive call with `!` suffix in inductive rules as opposed to `@next` suffix on the rule premise. We will also use `!` prefix on predicates to describe a negation.

```prolog
% rule creates a new counter if non exists yet.
counter(entity: Entity, count: Int, title: String) :-
  % when no counter exists at this time
  not counter(_, _, _),
  % we claim (create) a counter next time with count 0 and title "init"
  counter!(entity, 0, "init").

% rule increments counter when increment (event) fact exists.
counter(entity: Entity, count: Int, title: String) :-
  % when there is a counter with count of last_count
  counter(entity, last_count, title),
  % and there is an increment event on this counter
  increment(entity),
  % we claim (update) counter with incremented count
  counter!(entity, count + 1, title).
```

Also note that disjunctions (logical or) is expressed through rule that share the premise.

We take a inspiration from datalog notation and allow defining additional disjunctions (from the default one that simply finds stored facts) using `#[rule]` attribute macro.

```rs
#[derive(Rule)]
pub struct Counter {
    pub count: i32,
    pub title: String,
}

#[derive(Rule)]
pub struct Increment;

// Rule that will match when there is no counter.
#[rule(Counter)]
fn new(counter: Match<Counter>) -> When {
    [
        // No counter exists at this time
        Not<Counter> {
            this: Term::blank(),
            count: Term::blank(),
            title: Term::blank(),
        },
        // claim (create) new counter next time
        Claim<Counter> {
            this: counter.this,
            count: 0,
            title: "basic counter".to_string(),
        }
    ]
}

// Rule that will match when we have a counter and an
// increment action associated with same entity.
#[rule(Counter)]
fn inc(terms: Match<Counter>) -> When {
    // We have want to find counter and capture it's count so we define a var.
    let last_count = Term::var("last_count");
    [
        // We have a counter with last_count for it's current
        // count value.
        Match<Counter> {
            this: counter.this,
            count: last_count,
            title: counter.title
        },
        // We also have `Incerment` fact asserted on the same
        // entity, signalling increment action taking place.
        Match<Increment> { this: counter.this },
        // Built in oprator that derives an incremented term
        // from provided term.
        Math::inc(last_count).is(terms.count),
        // Going forward we will
        Claim<Counter> {
            this: counter.this,
            count: counter.count,
            title: counter.title
        }
    ]
}
```

### Installing Rules as Behavior

Rules can be installed into the Dialog as reactive behaviors.

```rs
async fn main() -> Result<()> {
    let store = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(store).await?;

    let session = Dialog::open(artifacts)
        .with(Counter::new)
        .with(Counter::inc);

    // Finds either existing counter or creates and persists one
    // with `Counter::new` rule.
    let counters = session.query(Counter).await?;
}
```

### Execution Model

It is worth elaborating on rule execution model. Time of the execution is arbitrary, that is when above example runs the query it may find facts for the counter in which cases non of the rules will be executed. However if the counter is not found `Counter::new` rule will be executed which will claim a `Counter` that engine will decompose into set of facts and commit them to the database - effectively caching rule execution for next runs.

This execution model allows multiple concurrent applications to share same database while maintaining desired invariants, for example one application may not have `Counter::new` rule or `Counter::inc` rule, however if it stores a `Counter` and an `Increment` fact, another application that has those rules will react next time `Counter` is being queried and update it according to their rules. Furthermore, updates will propagate to other applications as it does not matter if update logic was imperative (using .commit) or declarative through rules.
