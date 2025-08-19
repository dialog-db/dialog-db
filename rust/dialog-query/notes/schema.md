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
struct Name(String);
impl Attribute for Name {
    domain() -> &'static str {
        "microshaft.employee"
    }
    cardinality() -> Cardinality {
        Cardinality::One
    }
}
impl From<&Name> for dialog_artifacts::ValueDataType {
    fn from(value: &Name) -> Self {
        dialog_artifacts::ValueDataType::String
    }
}

struct Job(String);
impl Attribute for Job {
    domain() -> &'static str {
        "microshaft.employee"
    }
    cardinality() -> Cardinality {
        Cardinality::One
    }
}
impl From<&Job> for dialog_artifacts::ValueDataType {
    fn from(value: &Job) -> Self {
        dialog_artifacts::ValueDataType::String
    }
}

struct Salary(u32);
impl Attribute for Salary {
    domain() -> &'static str {
        "microshaft.employee"
    }
    cardinality() -> Cardinality {
        Cardinality::One
    }
}
impl From<&Salary> for dialog_artifacts::ValueDataType {
    fn from(value: &Salary) -> Self {
        dialog_artifacts::ValueDataType::UnsignedInt
    }
}

struct Address(String);
impl Attribute for Address {
    domain() -> &'static str {
        "microshaft.employee"
    }
    cardinality() -> Cardinality {
        Cardinality::Many
    }
}
impl From<&Address> for dialog_artifacts::ValueDataType {
    fn from(value: &Address) -> Self {
        dialog_artifacts::ValueDataType::String
    }
}

impl MicroshaftEmployee {
    const Name: &'static = Name;
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

I would expect to define models using such constructs which would look like this:

```rs
#[schema]
struct Employee(Name, Salary, Job, Address);
```

You would be able to also define models with subset of fields like this:

```rs
#[schema]
struct Employee(Name, Job);
```

You should be able to query using schema by writing something like this:

```rs
async fn demo() -> Result<()> {
    let store = MemoryStorageBackend::default();
    let artifacts = Artifacts::anonymous(store).await?;

    let employees = Employee::find(Name::is("Alice"), Job::any())
        .query(&artifacts)?.collect().await;
    println!("{:?}", employees);

    Ok(())
}
```


## Unknowns

### Optionals

Not sure if this can be made work, but if so it would probably a most natural way in rust

```rs
#[schema]
struct Employee(Name, Option<Address>);
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
#[schema]
struct Employee(
  Name,
  #[implicit("Earth")]
  Address
);
```


## Alternative Approach

```rs
#[component]
struct Employee {
  name: String,
  salary: i32,
  #[many]
  address: String,
}
impl Rule for Employee {

}
```
