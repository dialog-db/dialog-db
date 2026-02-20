pub use dialog_artifacts::selector::Constrained;
pub use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStore, ArtifactStoreMut, ArtifactStoreMutExt, Artifacts,
    Attribute, Cause, DialogArtifactsError, Entity, Instruction, TypeError, Value,
    ValueDataType as Type,
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
