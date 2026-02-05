//! Capability-based authorization system.
//!
//! This crate provides a hierarchical capability system for authorization and
//! access control. Capabilities form chains from a root [`Subject`]
//! _(represented by [did:key](https://w3c-ccg.github.io/did-method-key/))_
//! through any number of constraints down to [`Effect`]s that perform actual
//! operations.
//!
//! # Quick Example
//!
//! ```rust
//! # mod example {
//! use dialog_capability::{Subject, Ability, Attenuation, Policy, Effect};
//! use serde::{Serialize, Deserialize};
//!
//! // Attenuation: narrows ability (adds "/storage" to path) and adds parameters
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! struct Storage;
//! impl Attenuation for Storage {
//!     type Of = Subject;
//! }
//!
//! // Policy: constrains parameters only (no path change)
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! struct Store { name: String }
//! impl Policy for Store {
//!     type Of = Storage;
//! }
//!
//! // Effect: narrows ability (adds "/get"), and is invocable
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! struct Get { key: Vec<u8> }
//! impl Effect for Get {
//!     type Of = Store;
//!     type Output = Result<Option<Vec<u8>>, std::io::Error>;
//! }
//!
//! pub fn example() {
//!     // Build a capability chain
//!     let capability = Subject::from("did:key:z6MkhaXgBZD...")
//!         .attenuate(Storage)                        // ability: /storage
//!         .attenuate(Store { name: "index".into() }) // ability: /storage (unchanged)
//!         .invoke(Get { key: b"my-key".to_vec() });  // ability: /storage/get
//!
//!     // The ability is expressed as a path
//!     assert_eq!(capability.ability(), "/storage/get");
//!
//!     // Extract constraint values from the chain
//!     assert_eq!(Store::of(&capability).name, "index");
//!     assert_eq!(Get::of(&capability).key, b"my-key");
//! }
//! # }
//! ```
//!
//! # Core Concepts
//!
//! ## Subject
//!
//! A [`Subject`] is the root of every capability chain - it identifies the
//! resource (via a DID) and represents full authority: ability `/` with no
//! policy constraints. All capabilities are derived by attenuating a Subject.
//!
//! ## Abilities and Policies
//!
//! A capability represents a set of invocable operations (effects). This set
//! is defined by two things:
//!
//! - **Ability**: A path like `/storage` or `/storage/get` that determines
//!   which effects are included. An ability includes all effects whose path
//!   starts with it - so `/storage` includes `/storage/get`, `/storage/set`,
//!   `/storage/delete`, etc. The root ability `/` encompasses all possible
//!   effects.
//!
//! - **Policies**: Parameters that constrain how effects can be invoked,
//!   without changing which effects are permitted. For example, a
//!   `Store { name: "index" }` policy doesn't change what operations are
//!   allowed (get, set, delete all remain possible), but constrains them
//!   to only apply to the "index" store.
//!
//! ## Capability Hierarchy
//!
//! Capabilities are built as chains using [`Constrained`]:
//!
//! ```text
//! Subject ("did:key:z6Mk...")            → ability: /
//!   └── Attenuation (e.g., Storage)      → ability: /storage
//!         └── Policy (e.g., Store)       → ability: /storage (unchanged)
//!               └── Effect (e.g., Get)   → ability: /storage/get
//! ```
//!
//! Each level narrows access further. Chains can have any number of layers -
//! the structure is defined by the `Policy::Of` / `Attenuation::Of` type
//! associations.
//!
//! ## Policy, Attenuation, and Effect
//!
//! These three traits form a hierarchy:
//!
//! - **[`Policy`]**: Constrains **parameters only**. Policies narrow what
//!   the capability can access without changing the ability path.
//!
//!   Example: `Store { name: "index" }` restricts to the "index" store.
//!
//! - **[`Attenuation`]**: Constrains **ability and parameters**. Attenuations
//!   add a segment to the ability path, narrowing what operations are permitted.
//!
//!   Example: `Storage` adds `/storage` to the ability.
//!
//! - **[`Effect`]**: Constrains **ability and parameters**, and is **invocable**.
//!   Effects can be executed by a [`Provider`] to produce an output.
//!
//!   Example: `Get { key }` adds `/get` to ability and can be performed to
//!   retrieve a value.
//!
//! ## Invocable vs Non-Invocable Capabilities
//!
//! Not all capabilities can be executed. A capability like
//! `Subject.attenuate(Storage).attenuate(Store { name: "index" })` represents
//! permission to access the "index" store, but there's no concrete operation
//! to perform yet.
//!
//! Only capabilities ending in an [`Effect`] are **invocable** - they can be
//! passed to a [`Provider`] to produce an output. Effects define an associated
//! `Output` type that specifies what the operation returns.
//!
//! Use `.invoke(effect)` to create an invocable capability, then
//! `.perform(provider)` to execute it.
//!
//! # Key Types
//!
//! **Building capabilities:**
//!
//! | Type | Role | Example |
//! |------|------|---------|
//! | [`Subject`] | Root of chains (a DID) | `Subject::from("did:key:z6Mk...")` |
//! | [`Capability<T>`] | Complete capability chain | `Capability<Get>` |
//! | [`Constrained<P, Of>`] | Internal chain element | (used internally) |
//!
//! **Defining constraints:**
//!
//! | Trait | Constrains | Example Types |
//! |-------|------------|---------------|
//! | [`Policy`] | Parameters only | `Store`, `Catalog`, `Cell` |
//! | [`Attenuation`] | Ability + parameters | `Storage`, `Memory`, `Archive` |
//! | [`Effect`] | Ability + parameters, invocable | `Get`, `Set`, `Resolve` |
//!
//! **Authorization:**
//!
//! | Type | Role |
//! |------|------|
//! | [`Ability`] | Trait providing `subject()` and `ability()` |
//! | [`Provider<I>`] | Executes capabilities |
//! | [`Authorization`] | Proof of delegated authority |
//! | [`Delegation<C, A>`] | Grants capability to another principal |
//! | [`Access`] | Looks up authorization proofs |

mod error;
pub use error::*;

mod selector;
pub use selector::*;

mod settings;
pub use settings::*;

#[cfg(feature = "ucan")]
pub mod ucan;

mod ability;
pub use ability::*;

mod constraint;
pub use constraint::*;

mod policy;
pub use policy::*;

mod attenuation;
pub use attenuation::*;

mod effect;
pub use effect::*;

mod subject;
pub use subject::*;

mod constrained;
pub use constrained::*;

mod capability;
pub use capability::*;

mod provider;
pub use provider::*;

mod authority;
pub use authority::*;

mod authorization;
pub use authorization::*;

mod access;
pub use access::*;

mod claim;
pub use claim::*;

mod authorized;
pub use authorized::*;

mod invocation;
pub use invocation::*;

mod delegation;
pub use delegation::*;
