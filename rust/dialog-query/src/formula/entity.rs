//! A formula that makes an entity from what a rule has bound.
//!
//! A rule that concludes a fact of an entity it did not read has to name
//! that entity: the row of a command it issues, a record it creates. The
//! name has to be the same wherever and whenever the rule fires on the
//! same bindings, so it is derived from them and nothing else.

use merkle_reference::{Reference, Type};

use crate::corpus::Entity;
use crate::{Formula, Value, formula::Input, identity};

/// The type of derived entities, `id:`.
pub const ID: Type = Type::from_reference(Reference::from_hex(
    "8464fd649119523a5fd882eed8ce38f5fb024e9e2fd5a7da674c39840b446c52",
));

/// The entity derived from `of` with `with`: `id:` followed by the
/// base58 of the two values' references, as a list under [`ID`].
///
/// It is a function of the two values, so equal values name one entity
/// on every replica. Deriving again from the result takes in one more
/// value, and the order values are taken in is part of the name.
pub fn derive(of: &Value, with: &Value) -> Entity {
    identity::entity("id:", ID.list([of.reference(), with.reference()]))
}

/// Derive an entity from a value and one more.
#[derive(Debug, Clone, Formula)]
pub struct Derive {
    /// What the entity is derived from.
    pub of: Value,
    /// What it is derived with.
    pub with: Value,
    /// The entity.
    #[output]
    pub is: Entity,
}

impl Derive {
    /// The entity the two values name.
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        let is = derive(&input.of, &input.with);
        vec![Derive {
            of: input.of,
            with: input.with,
            is,
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    fn it_names_the_type_of_derived_entities() {
        assert_eq!(Type::named("id:"), Ok(ID));
    }

    #[dialog_common::test]
    fn it_derives_one_entity_from_equal_values() {
        let of = Value::String("command.add.todo".into());
        let with = Value::UnsignedInt(1);
        let entity = derive(&of, &with);
        assert!(entity.as_str().starts_with("id:"));
        assert_eq!(entity, derive(&of.clone(), &with.clone()));
    }

    #[dialog_common::test]
    fn it_derives_another_entity_from_other_values_or_another_order() {
        let a = Value::String("a".into());
        let b = Value::String("b".into());
        assert_ne!(derive(&a, &b), derive(&b, &a));
        assert_ne!(derive(&a, &b), derive(&a, &Value::UnsignedInt(1)));
        // A value's type is part of it: the text and the entity that
        // read alike are not one value.
        let text = Value::String("todo:1".into());
        let entity = Value::Entity("todo:1".parse().expect("an entity"));
        assert_ne!(derive(&a, &text), derive(&a, &entity));
    }

    #[dialog_common::test]
    fn it_computes_what_derive_does() {
        let of = Value::String("seed".into());
        let with = Value::Boolean(true);
        let computed = Derive::compute(DeriveInput {
            of: of.clone(),
            with: with.clone(),
        });
        assert_eq!(computed.len(), 1);
        assert_eq!(computed[0].is, derive(&of, &with));
    }
}
