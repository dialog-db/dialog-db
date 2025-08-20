
pub trait Predicate {
    type Value;
    fn evaluate(&self, value: &Self::Value) -> bool;
}
