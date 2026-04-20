use dialog_varsig::Did;

/// Handle to a named space under a profile.
///
/// Knows the profile DID and space name. Use `.open()`, `.load()`,
/// or `.create()` to build a command, then `.perform(&operator)` to
/// execute it.
pub struct SpaceHandle {
    /// The profile DID that owns this space.
    pub profile_did: Did,
    /// The space name.
    pub name: String,
}
