pub use dialog_artifacts::selector::Constrained;
pub use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStore, ArtifactStoreMut, ArtifactStoreMutExt, Artifacts,
    Attribute, Cause, DialogArtifactsError, Entity, Instruction, TypeError, Value,
    ValueDataType as Type,
};
pub use futures_util::stream::Stream;

pub use dialog_common::{ConditionalSend, ConditionalSync};
// For testing, we can access MemoryStorageBackend through dialog-artifacts
// to ensure version consistency
pub trait Store: ArtifactStore + Clone + Send + 'static {}

// Alternative 1: Try to make it work with associated type
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
