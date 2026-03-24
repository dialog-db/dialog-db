//! Test environment — in-memory volatile storage, no credentials or remote.

use dialog_storage::provider::Volatile;

use super::Environment;

/// Test environment: in-memory local storage, unit credentials and remote.
pub type TestEnvironment = Environment<(), Volatile, ()>;
