//! Site trait for declaring authorization requirements.
//!
//! A [`Site`] represents pure configuration for a target location — no
//! credential material. It declares what intermediate and final authorization
//! types it needs via associated types.
//!
//! [`RemoteSite`] marks sites that require the full Authorize → Redeem
//! pipeline. [`Local`] represents direct local access.

use dialog_common::ConditionalSend;

/// Pure site configuration — no credential material.
///
/// Implemented by types that describe where an operation should be directed.
/// The associated types track the authorization lifecycle:
///
/// - `Permit`: intermediate proof produced by the Authorize step
/// - `Access`: final access token produced by the Redeem step
pub trait Site: Clone + ConditionalSend + 'static {
    /// Intermediate permit produced by the Authorize step.
    type Permit: ConditionalSend;
    /// Final access token produced by the Redeem step.
    type Access: ConditionalSend;
}

/// Marker for sites requiring remote authorization (Authorize → Redeem pipeline).
pub trait RemoteSite: Site {}

/// Local site — no remote backend needed.
///
/// Used for operations that execute directly without remote authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Local;

impl Site for Local {
    type Permit = Local;
    type Access = Local;
}
