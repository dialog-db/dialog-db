mod attribute;
pub use attribute::*;

mod entity;
pub use entity::*;

mod value;
pub use value::*;

mod part;
pub use part::*;

macro_rules! mutable_slice {
    ( $array:expr, $index:expr, $run:expr ) => {{
        const START: usize = $index;
        const END: usize = $index + $run;
        &mut $array[START..END]
    }};
}

pub(crate) use mutable_slice;

pub(crate) const ENTITY_LENGTH: usize = 32;
pub(crate) const ATTRIBUTE_LENGTH: usize = 64;
pub(crate) const VALUE_DATA_TYPE_LENGTH: usize = 1;
pub(crate) const VALUE_REFERENCE_LENGTH: usize = 32;

pub(crate) const ENTITY_KEY_LENGTH: usize =
    ENTITY_LENGTH + ATTRIBUTE_LENGTH + VALUE_DATA_TYPE_LENGTH;

pub(crate) const ATTRIBUTE_KEY_LENGTH: usize =
    ATTRIBUTE_LENGTH + ENTITY_LENGTH + VALUE_DATA_TYPE_LENGTH;

pub(crate) const VALUE_KEY_LENGTH: usize =
    VALUE_DATA_TYPE_LENGTH + VALUE_REFERENCE_LENGTH + ATTRIBUTE_LENGTH + ENTITY_LENGTH;

pub(crate) const MINIMUM_ENTITY: [u8; ENTITY_LENGTH] = [u8::MIN; ENTITY_LENGTH];
pub(crate) const MAXIMUM_ENTITY: [u8; ENTITY_LENGTH] = [u8::MAX; ENTITY_LENGTH];
pub(crate) const MINIMUM_ATTRIBUTE: [u8; ATTRIBUTE_LENGTH] = [u8::MIN; ATTRIBUTE_LENGTH];
pub(crate) const MAXIMUM_ATTRIBUTE: [u8; ATTRIBUTE_LENGTH] = [u8::MAX; ATTRIBUTE_LENGTH];
pub(crate) const MINIMUM_VALUE_REFERENCE: [u8; VALUE_REFERENCE_LENGTH] =
    [u8::MIN; VALUE_REFERENCE_LENGTH];
pub(crate) const MAXIMUM_VALUE_REFERENCE: [u8; VALUE_REFERENCE_LENGTH] =
    [u8::MAX; VALUE_REFERENCE_LENGTH];
