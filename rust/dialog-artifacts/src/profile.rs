//! Profile configuration for opening an environment.
//!
//! A [`Profile`] describes which identity to open — profile name and operator
//! strategy. Use [`Profile::default()`] for the common case (named "default",
//! unique operator per session).

/// How to create the operator key for a session.
pub enum Operator {
    /// Generate a random ephemeral keypair each time.
    Unique,
    /// Derive deterministically from the profile key + context.
    Derived(Vec<u8>),
}

impl Operator {
    /// Shorthand for `Operator::Unique`.
    pub fn unique() -> Self {
        Self::Unique
    }

    /// Shorthand for `Operator::Derived(context.into())`.
    pub fn derived(context: impl Into<Vec<u8>>) -> Self {
        Self::Derived(context.into())
    }
}

/// Describes which profile to open and how to create the operator.
///
/// This is a configuration type — pass it to `Environment::open` to
/// materialize the actual credentials.
///
/// # Examples
///
/// ```no_run
/// use dialog_artifacts::Profile;
///
/// // Default profile with unique operator
/// let profile = Profile::default();
///
/// // Named profile with derived operator
/// let profile = Profile::named("work")
///     .operated_by(dialog_artifacts::Operator::derived(b"alice"));
/// ```
pub struct Profile {
    /// The profile name (e.g. "default", "work", "personal").
    pub name: String,
    /// How to create the operator key.
    pub operator: Operator,
}

impl Profile {
    /// Create a profile descriptor with the given name.
    ///
    /// Defaults to `Operator::Unique`. Use `.operated_by()` to change.
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            operator: Operator::Unique,
        }
    }

    /// Set the operator strategy.
    pub fn operated_by(mut self, operator: Operator) -> Self {
        self.operator = operator;
        self
    }
}

impl Default for Profile {
    fn default() -> Self {
        Self::named("default")
    }
}
