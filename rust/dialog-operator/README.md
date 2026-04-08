# dialog-operator

Profiles, operators, and the runtime capability environment for Dialog.

A **Profile** is a named identity on a device, backed by a signing credential. An **Operator** is a session-scoped environment derived from a profile that routes all capability effects (storage, archive, memory, access control) through DID-based dispatch with privilege narrowing.

## Usage

```rust
use dialog_operator::profile::Profile;
use dialog_capability::Subject;
use dialog_storage::provider::environment::Storage;

// Create the environment (platform-specific storage)
let storage = Storage::default();

// Open or create a profile
let profile = Profile::open("alice")
    .perform(&storage)
    .await?;

// Derive an operator (narrows access, scoped to profile)
let operator = profile
    .derive(b"my-app")
    .allow(Subject::any())
    .build(storage)
    .await?;

// Open a repository through the profile
let contacts = profile.repository("contacts")
    .open()
    .perform(&operator)
    .await?;
```
