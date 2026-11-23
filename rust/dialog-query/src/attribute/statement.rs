use crate::attribute::Relation;
use crate::attribute::expression::dynamic::DynamicAttributeExpression;
use crate::corpus::{Entity, Value};
use crate::schema::Cardinality;
use crate::statement::Statement;
use dialog_corpus::Update;

/// A type-erased, attribute statement.
///
/// Holds all the information needed to assert or retract a single
/// attribute without any generic type parameters. Both
/// [`StaticAttributeExpression`](super::expression::typed::StaticAttributeExpression)
/// and [`DynamicAttributeExpression`](super::expression::dynamic::DynamicAttributeExpression)
/// can convert into this type via `.into()`, enabling heterogeneous
/// collections (e.g. `Vec<AttributeStatement>`) for concept instances
/// that contain attributes of different types.
pub type AttributeStatement = DynamicAttributeExpression<Relation, Entity, Value>;

impl Statement for AttributeStatement {
    fn assert(self, update: &mut impl Update) {
        let the = self.the;
        let value = self.is;
        let cardinality = self.cardinality.unwrap_or(Cardinality::Many);
        update.associate(the.into(), self.of, value, cardinality);
    }

    fn retract(self, update: &mut impl Update) {
        update.dissociate(self.the.into(), self.of, self.is);
    }
}
