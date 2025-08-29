pub use dialog_artifacts::selector::Constrained;
pub use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStore, ArtifactStoreMut, Artifacts, Attribute, Cause,
    DialogArtifactsError, Entity, Instruction, TypeError, Value, ValueDataType,
};

// For testing, we can access MemoryStorageBackend through dialog-artifacts
// to ensure version consistency
pub trait Store: ArtifactStore + Clone + Send + 'static {}
