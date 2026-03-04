use crate::Transaction;
use crate::artifact::{Entity, Value};
use crate::attribute::The;
use crate::schema::Cardinality;
use crate::statement::Statement;

/// A type-erased, attribute statement.
///
/// Holds all the information needed to assert or retract a single
/// attribute without any generic type parameters. Both
/// [`StaticAttributeExpression`](super::expression::typed::StaticAttributeExpression)
/// and [`DynamicAttributeExpression`](super::expression::dynamic::DynamicAttributeExpression)
/// can convert into this type via `.into()`, enabling heterogeneous
/// collections (e.g. `Vec<AttributeStatement>`) for concept instances
/// that contain attributes of different types.
#[derive(Clone, Debug)]
pub struct AttributeStatement {
    /// The attribute (predicate).
    pub the: The,
    /// The entity this attribute belongs to.
    pub of: Entity,
    /// The concrete value.
    pub is: Value,
    /// Whether this attribute allows one or many values per entity.
    pub cardinality: Cardinality,
}

impl Statement for AttributeStatement {
    fn assert(self, transaction: &mut Transaction) {
        if self.cardinality == Cardinality::One {
            transaction.associate_unique(self.the, self.of, self.is);
        } else {
            transaction.associate(self.the, self.of, self.is);
        }
    }

    fn retract(self, transaction: &mut Transaction) {
        transaction.dissociate(self.the, self.of, self.is);
    }
}
