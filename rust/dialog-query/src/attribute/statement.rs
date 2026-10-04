use crate::artifact::{Entity, Value};
use crate::attribute::The;
use crate::attribute::expression::dynamic::DynamicAttributeExpression;
use crate::schema::Cardinality;
use crate::statement::Statement;
use dialog_artifacts::{Succession, Update};

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
    fn assert(self, update: &mut impl Update) {
        let the = self.the;
        let value = self.is;
        match (self.succession, self.cardinality) {
            (Some(succession), _) => {
                update.succeed(the.into(), self.of, value, succession);
            }
            // A cardinality-one write with no policy spelled is a
            // `last` write.
            (None, Some(Cardinality::One)) => {
                update.succeed(the.into(), self.of, value, Succession::Last);
            }
            (None, _) => {
                update.associate(the.into(), self.of, value);
            }
        }
    }

    fn retract(self, update: &mut impl Update) {
        update.dissociate(self.the.into(), self.of, self.is);
    }
}
