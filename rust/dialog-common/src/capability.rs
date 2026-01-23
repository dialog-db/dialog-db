//! Capability-based authorization system.
//!
//! This module provides a hierarchical capability system for authorization and
//! access control. Capabilities form chains from a root [`Subject`] (typically a DID)
//! through any number of [`Policy`] constraints down to [`Effect`]s that perform
//! actual operations.
//!
//! # Core Concepts
//!
//! ## Capability Hierarchy
//!
//! Capabilities are built as chains using [`Constrained`]:
//!
//! ```text
//! Subject (repository DID)
//!   └── Policy₁ (e.g., Archive, Storage, Memory)
//!         └── Policy₂ (e.g., Catalog, Store, Space)
//!               └── Policy₃ (e.g., Cell - optional, can have more layers)
//!                     └── Effect (e.g., Get, Put, Resolve)
//! ```
//!
//! Each level attenuates (restricts) access further. Chains can have any number
//! of policy layers - the structure is defined by the `Policy::Of` / `Attenuation::Of`
//! associations.
//!
//! ## Policy, Attenuation, and Effect
//!
//! These three traits form a hierarchy where each builds on the previous:
//!
//! - **[`Policy`]**: The base constraint type. Contributes only to **parameters**.
//!   Policies scope and filter access without changing the command identity. They
//!   define *where* or *on what* the operation applies (e.g., `Catalog { catalog: "index" }`
//!   restricts to a specific catalog). A policy does NOT add to the command path.
//!
//! - **[`Attenuation`]**: Every Attenuation is also a Policy. In addition to parameters,
//!   attenuations contribute to the **command path**. They define the operation
//!   namespace and appear in the path (e.g., `Archive` → `/archive`). The command
//!   path identifies *what* operation is being performed.
//!
//! - **[`Effect`]**: Every Effect is also an Attenuation. Effects add to the command
//!   path like attenuations, but unlike attenuations, effects are **invocable** - they
//!   can be executed by a [`Provider`] and produce an output (defined by
//!   `Effect::Output`).
//!
//! # Key Types
//!
//! | Type | Description |
//! |------|-------------|
//! | [`Subject`] | Root of capability chains (a DID) |
//! | [`Constrained<P, Of>`] | Capability chain element |
//! | [`Capability<T>`] | Full capability chain for constraint T |
//! | [`Ability`] | Abstract interface for capability chains (subject + command) |
//! | [`Attenuation`] | Types that contribute to command path |
//! | [`Policy`] | Scoping constraint trait |
//! | [`Effect`] | Executable operation trait |
//! | [`Invocation`] | Connects types to Provider I/O |
//! | [`Provider<I>`] | Executes invocations |
//! | [`Authorization`] | Proof of authority trait |
//! | [`Delegation<C, A>`] | Grants access to another party |
//! | [`Access`] | Finds delegation chains |

// Core modules
mod ability;
pub mod access;
mod attenuation;
mod authority;
mod authorization;
mod authorized;
mod claim;
mod constrained;
mod constraint;
mod delegation;
mod effect;
mod interface;
mod invocation;
mod policy;
pub mod provider;
mod selector;
mod settings;
mod subject;

// Re-exports
pub use ability::Ability;
pub use access::Access;
pub use attenuation::Attenuation;
pub use authority::{Authority, Principal};
pub use authorization::{Authorization, AuthorizationError};
pub use authorized::{Authorized, PerformError};
pub use claim::Claim;
pub use constrained::Constrained;
pub use constraint::Constraint;
pub use delegation::Delegation;
pub use effect::Effect;
pub use interface::Capability;
pub use invocation::Invocation;
pub use policy::Policy;
pub use provider::Provider;
pub use selector::{Here, Never, Selector, There};
#[cfg(feature = "ucan")]
pub use settings::Parameters;
pub use settings::Settings;
pub use subject::{Did, Subject};
