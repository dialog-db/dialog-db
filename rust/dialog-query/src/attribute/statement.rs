use crate::Transaction;
use crate::artifact::{Entity, Value};
use crate::attribute::The;
use crate::attribute::expression::dynamic::DynamicAttributeExpression;
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
pub type AttributeStatement = DynamicAttributeExpression<The, Entity, Value>;

impl Statement for AttributeStatement {
    fn assert(self, transaction: &mut Transaction) {
        let the = self.the;
        let value = self.is;
        match self.cardinality {
            Some(Cardinality::One) => {
                transaction.associate_unique(the, self.of, value);
            }
            _ => {
                transaction.associate(the, self.of, value);
            }
        }
    }

    fn retract(self, transaction: &mut Transaction) {
        transaction.dissociate(self.the, self.of, self.is);
    }
}
