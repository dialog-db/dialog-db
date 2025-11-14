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


## Attributes

[Datomic schema] and [clojure spec] have very similar model to Entity Component System (ECS) system, however since all data is persisted and is shared across applications namespacing of attributes is critical for avoiding unintentional conflicts. For this reason we leverage rust native `mod` names and derive namespace component from it, here is an example of defining some attributes

```rs
mod employee {
    /// Name of the employee
    #[derive(Attribute)]
    pub struct Name(String);

    /// Job title of the employee
    #[derive(Attribute)]
    pub struct Job(String);

    /// Salary of the employee
    #[derive(Attribute)]
    pub struct Salary(u32);

    /// Employees managed by this entity. May have multiple
    /// subordinates.
    #[derive(Attribute)]
    #[cardinality(many)]
    pub struct Manages(Entity);
}
```

Which roughly translates to a following schema in datomic:

```edn
{:db/ident :employee/name
 :db/valueType :db.type/string
 :db/cardinality :db.cardinality/one
 :db/doc "Name of the employee"}

{:db/ident :employee/job
 :db/valueType :db.type/string
 :db/cardinality :db.cardinality/one
 :db/doc "Job title of the employee"}

{:db/ident :employee/salary
 :db/valueType :db.type/long
 :db/cardinality :db.cardinality/one
 :db/doc "Salary of the employee"}

{:db/ident :employee/manages
 :db/valueType :db.type/ref
 :db/cardinality :db.cardinality/many
 :db/doc "Employees managed by this entity. May have multiple subordinates."}
```

> ℹ️ We infer type from the definition of the struct and cardinality is assumed one unless specified otherwise using `cardinality` attribute. `mod` name is used as a namespace component of the attribute. Also note that doc comments are captured as documentation for the attribute.

### Supported value types

Attributes could be derived only on structs that wrap closed set of data types that satisfy `Scalar` constraint.

### Support for Atomic Records

Attributes could also be derived for composite structs in which case they need to satisfy `ValueType` trait in which case they will be represented as bytes and serialized / deserialized on demand.

Generally it is recommended to use this only in cases where only atomic updates should be possible. Geolocation and time are good example of values where updating individual components should not be possible.

## Concepts

Dialog has notion of concepts which is similar to [entity maps]  in clojure spec. It is also equivalent to queries in bevy. Concepts can be made up on the fly with a group of attributes.

```rs
pub mod person {
    /// Name of the person
    #[derive(Attribute, Debug, Clone)]
    pub struct Name(String);

    /// Address of the person
    #[derive(Attribute, Debug, Clone)]
    pub struct Birthday(String);
}

/// Person entity a name and an address
type Person = (Entity, person::Name, person::Birthday);

let alice = Match<Person>(
    Term::var("person"),
    "Alice".into(),
    birthday: 1983_07_03u32.into(),
);

let store = MemoryStorageBackend::default();
let artifacts = Artifacts::anonymous(store).await?;
let dialog = Dialog::open(artifacts);

// Find for all `Person` that have name `Alice` and birthday `1983_07_03`
let results = alice.query(dialog).await?;
```

It is also possible to define more reusable concepts with named members as follows


```rs
#[derive(Concept, Debug, Clone)]
pub struct Person {
    this: Entity,
    name: person::Name,
    birthday: person::Birthday,
}

// Predicate that can be used to query for `Person` with given
// name `John` and birthday `1983_07_03`
let john = Match::<Person> {
    this: Term::var("person"),
    name: "John".into(),
    birthday: 1983_07_03u32.into(),
};

let store = MemoryStorageBackend::default();
let artifacts = Artifacts::anonymous(store).await?;
let session = Dialog::open(artifacts);

// Find for all `Person` that have name `John` and birthday `1983_07_03`
let results = john.query(session).await?;
```

## Updates

Updating individual attributes on entity should be pretty straightforward.

```rs
let mut transaction = session.edit();

// retracts name "Alice"
transaction.retract(
    (
        alice,
        person::Name("Alice"),
    )
);
// and assert name "Alison" instead
transaction.assert(
    (
        alice,
        person::Name("Alison"),
        employee::Job("Neuropsychologist"),
    )
);

transaction.commit().await?;
```

[datomic schema]:https://docs.datomic.com/schema/schema-reference.html
[entity maps]:https://clojure.org/guides/spec#_entity_maps
[clojure spec]:https://clojure.org/guides/spec
