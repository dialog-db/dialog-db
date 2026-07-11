pub use dialog_artifacts::selector::Constrained;
// `dialog_artifacts::Record` (the untyped value) is deliberately not
// re-exported here: `crate::types::Record` is the type *descriptor* of the
// same name, and both modules glob into the crate root. Use the
// `dialog_artifacts` path when the raw record value is needed.
pub use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStore, ArtifactStoreMut, ArtifactStoreMutExt, Artifacts,
    Attribute as ArtifactsAttribute, Cause, DialogArtifactsError, Entity, Instruction, RecordError,
    RecordFormat, Recorded, Select, TypeError as ArtifactTypeError, Value, ValueDataType as Type,
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
