# Modelig Relations

Lookup with known attribute should be pretty straight forward

```rs
#[derive(Attribute)]
pub struct Name(String);

#[derive(Attribute)]
pub struct Job(String);

pub fn test(terms) => {
  vec! [
    Match::<Name> { of: terms.person, is: terms.name },
    Match::<Job> { of: terms.person, is: terms.job }
  ]
}
```

# General Relations

```rs
pub fn traverse(terms) => {
    vec! [
        For { this: terms.person, the: term.name, is: terms.value }
    ]
}
```
