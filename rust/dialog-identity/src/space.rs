use dialog_varsig::Did;

/// Handle to a named space a peer holds.
///
/// Knows the peer DID and the space name. Use `.open()`, `.load()`, or
/// `.create()` to build a command, then `.perform(&session)` to execute
/// it.
pub struct SpaceHandle {
    /// The DID of the peer that holds this space.
    pub peer: Did,
    /// The space name.
    pub name: String,
}
