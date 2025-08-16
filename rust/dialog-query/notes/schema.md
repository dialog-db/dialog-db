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
#[schema]
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
#[derive(Attribute::one("microshaft.employee"))]
struct Name(String);

#[derive(Attribute::one("microshaft.employee"))]
struct Job(String);

#[derive(Attribute::one("microshaft.employee"))]
struct Salary(u32);

#[derive(Attribute::many("microshaft.employee"))]
struct Address(String);

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
