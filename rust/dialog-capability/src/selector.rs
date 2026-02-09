//! Type-level selector for extracting values from capability chains.
//!
//! This module provides the `Selector` trait and type-level indices (`Here`, `There`)
//! for extracting specific types from a chain of `Constrained` values.

use std::marker::PhantomData;

use crate::Subject;

/// Using an empty enum makes it impossible to construct, clearly indicating
/// values that can not exist.
pub enum Never {}

// Type-level Indices

/// Index pointing to the head of the chain.
pub struct Here;

/// Index pointing somewhere in the tail of the chain.
pub struct There<Index>(PhantomData<Index>);

/// Trait for extracting a value by type from a capability chain.
///
/// The `Index` type parameter is inferred by the compiler to locate
/// the correct position in the chain.
pub trait Selector<T, Index> {
    /// Get a reference to the value.
    fn select(&self) -> &T;
}

/// Subject can be selected from itself (base case for chain traversal).
impl Selector<Subject, Here> for Subject {
    fn select(&self) -> &Subject {
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use crate::{Attenuation, Effect};
    use serde::{Deserialize, Serialize};

    // Test types

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct Level1;

    impl Attenuation for Level1 {
        type Of = Subject;
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct Level2 {
        value: i32,
    }

    impl Attenuation for Level2 {
        type Of = Level1;
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct Level3 {
        name: String,
    }

    impl Effect for Level3 {
        type Of = Level2;
        type Output = ();
    }

    #[test]
    fn it_selects_subject_from_itself() {
        let subject = Subject::from(did!("key:test"));
        let selected: &Subject = subject.select();
        assert_eq!(selected.0, did!("key:test"));
    }

    #[test]
    fn it_selects_head_from_constrained() {
        let cap = Subject::from(did!("key:test")).attenuate(Level1);

        // Select the head (Level1)
        let level1: &Level1 = cap.policy();
        assert_eq!(level1, &Level1);
    }

    #[test]
    fn it_selects_subject_from_chain() {
        let cap = Subject::from(did!("key:test")).attenuate(Level1);

        // Subject is in the tail, accessed via There<Here>
        let subject: &Subject = cap.policy();
        assert_eq!(subject.0, did!("key:test"));
    }

    #[test]
    fn it_selects_from_deep_chain() {
        let cap = Subject::from(did!("key:test"))
            .attenuate(Level1)
            .attenuate(Level2 { value: 42 })
            .invoke(Level3 {
                name: "test".into(),
            });

        // Select Level3 (head)
        let level3: &Level3 = cap.policy();
        assert_eq!(level3.name, "test");

        // Select Level2 (one step back)
        let level2: &Level2 = cap.policy();
        assert_eq!(level2.value, 42);

        // Select Level1 (two steps back)
        let level1: &Level1 = cap.policy();
        assert_eq!(level1, &Level1);

        // Select Subject (at the root)
        let subject: &Subject = cap.policy();
        assert_eq!(subject.0, did!("key:test"));
    }
}
