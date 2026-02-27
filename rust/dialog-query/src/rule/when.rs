use super::Premises;
use crate::premise::Premise;

/// Trait for types that can be converted into a When collection
///
/// This trait enables ergonomic rule definitions by allowing various types
/// to be used as rule premises:
/// - Single items: `Into<Premise>` types
/// - Tuples: `(Query<A>, Query<B>, ...)`
/// - Arrays: `[Query<A>; N]`
/// - Vectors: `Vec<Query<A>>`
///
/// # Examples
///
/// ```rs
/// // Return a tuple of different Query types
/// fn my_rule(emp: Query<Employee>) -> impl When {
///     (
///         Query::<Stuff> { this: emp.this, name: emp.name },
///         Query::<OtherStuff> { this: emp.this, value: emp.value },
///     )
/// }
/// ```
pub trait When {
    /// Convert this collection into a set of premises
    fn into_premises(self) -> Premises;
}

// Implement IntoWhen for When itself
impl When for Premises {
    fn into_premises(self) -> Premises {
        self
    }
}

// Implement IntoWhen for arrays
impl<T: Into<Premise>, const N: usize> When for [T; N] {
    fn into_premises(self) -> Premises {
        self.into()
    }
}

// Implement IntoWhen for Vec
impl<T: Into<Premise>> When for Vec<T> {
    fn into_premises(self) -> Premises {
        self.into()
    }
}

// Implement IntoWhen for tuples of different sizes
// This allows heterogeneous premise types in a single rule

impl<T1> When for (T1,)
where
    T1: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![self.0.into()])
    }
}

impl<T1, T2> When for (T1, T2)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![self.0.into(), self.1.into()])
    }
}

impl<T1, T2, T3> When for (T1, T2, T3)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![self.0.into(), self.1.into(), self.2.into()])
    }
}

impl<T1, T2, T3, T4> When for (T1, T2, T3, T4)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
    T4: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
        ])
    }
}

impl<T1, T2, T3, T4, T5> When for (T1, T2, T3, T4, T5)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
    T4: Into<Premise>,
    T5: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
            self.4.into(),
        ])
    }
}

impl<T1, T2, T3, T4, T5, T6> When for (T1, T2, T3, T4, T5, T6)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
    T4: Into<Premise>,
    T5: Into<Premise>,
    T6: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
            self.4.into(),
            self.5.into(),
        ])
    }
}

impl<T1, T2, T3, T4, T5, T6, T7> When for (T1, T2, T3, T4, T5, T6, T7)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
    T4: Into<Premise>,
    T5: Into<Premise>,
    T6: Into<Premise>,
    T7: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
            self.4.into(),
            self.5.into(),
            self.6.into(),
        ])
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8> When for (T1, T2, T3, T4, T5, T6, T7, T8)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
    T4: Into<Premise>,
    T5: Into<Premise>,
    T6: Into<Premise>,
    T7: Into<Premise>,
    T8: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
            self.4.into(),
            self.5.into(),
            self.6.into(),
            self.7.into(),
        ])
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9> When for (T1, T2, T3, T4, T5, T6, T7, T8, T9)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
    T4: Into<Premise>,
    T5: Into<Premise>,
    T6: Into<Premise>,
    T7: Into<Premise>,
    T8: Into<Premise>,
    T9: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
            self.4.into(),
            self.5.into(),
            self.6.into(),
            self.7.into(),
            self.8.into(),
        ])
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> When for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
    T4: Into<Premise>,
    T5: Into<Premise>,
    T6: Into<Premise>,
    T7: Into<Premise>,
    T8: Into<Premise>,
    T9: Into<Premise>,
    T10: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
            self.4.into(),
            self.5.into(),
            self.6.into(),
            self.7.into(),
            self.8.into(),
            self.9.into(),
        ])
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> When
    for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
    T4: Into<Premise>,
    T5: Into<Premise>,
    T6: Into<Premise>,
    T7: Into<Premise>,
    T8: Into<Premise>,
    T9: Into<Premise>,
    T10: Into<Premise>,
    T11: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
            self.4.into(),
            self.5.into(),
            self.6.into(),
            self.7.into(),
            self.8.into(),
            self.9.into(),
            self.10.into(),
        ])
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> When
    for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
    T4: Into<Premise>,
    T5: Into<Premise>,
    T6: Into<Premise>,
    T7: Into<Premise>,
    T8: Into<Premise>,
    T9: Into<Premise>,
    T10: Into<Premise>,
    T11: Into<Premise>,
    T12: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
            self.4.into(),
            self.5.into(),
            self.6.into(),
            self.7.into(),
            self.8.into(),
            self.9.into(),
            self.10.into(),
            self.11.into(),
        ])
    }
}
