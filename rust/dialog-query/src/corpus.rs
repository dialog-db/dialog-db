pub use dialog_corpus::selector::Constrained;
pub use dialog_corpus::{
    COMPOSITE_VERSION, Cause, Composite, CompositeError, CompositeItems, CompositeMap,
    DialogCorpusError, Entity, Fact, Instruction, NameShape, Relation as CorpusRelation, Select,
    Selector, TypeError as CorpusTypeError, Value, ValueDataType as Type, decode_value,
    encode_value_owned,
};
pub use futures_util::stream::Stream;

pub use dialog_common::{ConditionalSend, ConditionalSync};

// Alternative 1: Try to make it work with associated type
/// Trait for types that can produce a send-safe iterator of instructions
pub trait Instructions:
    IntoIterator<Item = Instruction, IntoIter: ConditionalSend> + ConditionalSend
{
}

impl<T> Instructions for T
where
    T: IntoIterator<Item = Instruction> + ConditionalSend,
    T::IntoIter: ConditionalSend,
{
}
