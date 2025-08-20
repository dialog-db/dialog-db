# Schema

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

In dialog we have attributes that act basically the same, however we also want them to have namespace not just name, to do so I think we could use enums instead

```rs
#[relation]
enum MicroshaftEmployee {
    Name(String),
    Job(String),
    Salary(u32),
    #[many]
    Address(String),
}
```

This expands to following form

```rs
// namespace corresponding MicroshaftEmployee
pub mod MicroshaftEmployee {
    use dialog_query::*;

    pub struct Name(String);
    impl Name {
        pub fn name() -> &'static str {
            "microshaft.employee/name"
        }
        pub fn value_type() -> ValueDataType {
            ValueDataType::String
        }
        pub fn cardinality() -> Cardinality {
            Cardinality::One
        }

        pub fn of(entity: Term<Entity>) -> NameOf {
            NameOf(entity)
        }
    }

    pub struct NameOf(Term<Entity>);
    impl NameOf {
        pub fn is(value: Term<String>) -> Match {
            Match { of: self.0, is: value }
        }
    }


    pub struct Match {
     of: Term<Entity>,
     is: Term<String>
    };

}
#[derive(Attribute::one("microshaft.employee"))]
struct Name(String);

#[derive(Attribute::one("microshaft.employee"))]
struct Job(String);

#[derive(Attribute::one("microshaft.employee"))]
struct Salary(u32);

#[derive(Attribute::many("microshaft.employee"))]
struct Address(String);

impl MicroshaftEmployee {
    type Name: &'static  = Name;
    const Job: &'static = Job;
    const Salary: &'static = Salary;
    const Address: &'static = Address;
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

## Premises and Rules

The `#[schema]` macro is now renamed to `#[premise]` to better reflect its purpose - defining what we want to materialize:

```rs
#[premise]
pub struct Employee {
    name: Name,
    job: Job,
}

#[premise]
pub struct Counter {
    count: Count,
    title: Title,
}
```

The `#[premise]` macro generates pattern matching structs for queries:

```rs
impl Employee {
    pub struct Match {
        pub entity: Var<Entity>,
        pub name: NameConstraint,
        pub job: JobConstraint,
    }

    pub struct Not {
        pub entity: Var<Entity>,
        pub name: NameConstraint,
        pub job: JobConstraint,
    }

    pub struct Claim {
        pub name: Name,
        pub job: Job,
    }
}
```

## Rule Definitions

Rules define how to derive premises from base facts. Each rule gets its own named state:

```prolog
counter(entity: Entity, count: Int, title: String) :-
  !counter(entity, _, _),
  counter(entity, 0, "init").

counter(entity: Entity, count: Int, title: String) :-
  counter(entity, last_count, title),
  increment(entity),
  counter!(entity, count + 1, title).
```

```rs

// When gets set of terms corresponding to the rule's premises, and
// returns set of predicates that when true will produce conclusions
// in the form of the premise.
#[rule(Counter)]
fn new(terms: Terms<Counter>) -> When {
    [
        // We have no counter at all because there is no entity that matches
        // the counter relation.
        Counter::Not { entity: terms.entity, count: Term::Any, title: Term::Any },
        // If had no conture will claim (create) new counter with this values
        Counter::Claim { count: 0, title: "basic counter" }
    ]
}

// Rule that will run when we have a counter that has increment action asserted
// on the same entity
#[rule(Counter)]
fn inc(terms: Terms<Counter>) -> When {
    // We have want to find counter and capture it's count so we define a var.
    let last_count = Term::var("last_count");
    [
        // Rule runs if we have counter(s) matching our entity and count
        // corresponding to the last count.
        Counter::Match { entity: terms.entity, count: last_count, title: _ },
        // We also have `Incerment` fact asserted on the same entity, signalling
        // that count action takes place.
        Increment::Match { entity: terms.entity },
        // Built in oprator that derives value by incrementing value in the term
        Math::inc(last_count).is(terms.count),
        // From now on our counter will have new incremented count, but same title
        Counter::Claim { count: terms.count, title: terms.title }
    ]
}
```

The `#[rule]` macro generates:
- A zero-sized type for each rule (e.g., `NewCounter`, `IncrementCounter`)
- `impl Rule<Counter>` for that type
- A const on the premise: `Counter::New`, `Counter::Increment`

## Query Execution

The application layer defines queries using the `Query` trait:

```rs
impl Query for Employee {
    type Input = Name;
    type Output = Vec<Employee>;

    fn query(input: Self::Input) -> QueryDef {
        Employee::find(Name::is(input), Job::any())
    }

    fn handle(results: Vec<Employee>) -> Self::Output {
        results
    }
}
```

```rs

fn my_rule(terms: Terms<Counter>) -> When {

}

impl RuleTrait for F where F: Fn(Terms<Counter>) -> When {}

trait RuleTrait {}

impl Counter {
    fn rules() -> Vec<When> {
        let terms: Terms<Counter> = todo!("statically know via macro generation");
        let mut whens = vec![];

        whens.push(new(terms));
        whens.push(inc(terms));

        whens
    }
}
```

Rules are explicitly registered with the Dialog engine:

```rs
async fn main() -> Result<()> {
    let store = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(store).await?;


    //let app = Bevy::app().add_systems(my_other_system).add_systems((||{}, my_system.after(my_other_system)))

    fn universal_behavior(whens: Vec<When>, artifacts: &mut Artifacts) -> Result<(), QueryError> {
        todo!();
    }

    trait Behavior {
        fn exhibit(&self, &mut artifacts: &Artifacts) -> Result<()>;
    }

    let session = Dialog::open(artifacts)
        .enable(my_rule)
        // .enable(Counter::new)
        // .enable(Counter::inc)
        // .enable(Counter::dec)
        // .enable(Counter::reset)
        .enable(Counter)
        .connect()
        // .enable(Employee::Active)
        //
        //
    // let loop = session.run().fork();

    session.commit([
      Counter::Assert { count: 10, title: "External counter" }
    ])
]);
}
```

## Terms API

The `Terms<T>` type provides access to variables for a premise:

```rs
pub struct Terms<Counter> {
    pub entity: Var<Entity>,
    pub count: FieldAccessor<Count>,
    pub title: FieldAccessor<Title>,
}
```

Field accessors support constraints:
- `.any()` - matches any value
- `.is(value)` - matches specific value
- `.value()` - gets the variable for use in claims

## Pattern Matching Syntax

All patterns use consistent named field syntax:

```rs
// Finding entities
Counter::Match { entity: terms.entity, count: last_count, title: _ }

// Ensuring non-existence
Counter::Not { entity: terms.entity, count: _, title: _ }

// Claiming new facts
Counter::Claim { count: 0, title: "basic counter" }
```

The `_` serves as a wildcard meaning "any value" for that field.

## Unknowns

### Optionals

Not sure if this can be made work, but if so it would probably a most natural way in rust

```rs
#[premise]
struct Employee {
    name: Name,
    address: Option<Address>,
}
```

### Implicits

Not sure how to go about implicits, which are different from optionals because query engine needs to be able to resolve it during execution.

Also note that we can not mark relation implicit because it's how you compose relations in some cases it's required, in others it's optional and in others yet it is implied.

```rs
#[implicit("unknown")]
type Role = Job;
```

Would be even better if we could annotate it inside the composition, but not sure how could this be manifested.

```rs
#[premise]
struct Employee {
    name: Name,
    #[implicit("Earth")]
    address: Address,
}
```
