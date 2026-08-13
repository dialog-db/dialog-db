//! The evaluation scope: the environment bundle every premise
//! executes against.
//!
//! Evaluation reaches the world exclusively through effects the
//! environment provides: range scans ([`Select`]) with demand
//! recording, rule discovery ([`SelectRules`]), and idempotent
//! content-addressed block loads ([`Load`]) for procedure premises.
//! `Scope` names that bundle once so premise evaluation signatures
//! stay stable as effects are added.

use dialog_artifacts::Select;
use dialog_artifacts::inspect::Load;
use dialog_capability::Provider;
use dialog_common::ConditionalSync;

use crate::source::SelectRules;

/// The full provider bundle premise evaluation requires. Blanket
/// implemented: any environment providing the three effects is a
/// `Scope`.
pub trait Scope<'a>:
    Provider<Select<'a>> + Provider<SelectRules> + Provider<Load> + ConditionalSync
{
}

impl<'a, T> Scope<'a> for T where
    T: Provider<Select<'a>> + Provider<SelectRules> + Provider<Load> + ConditionalSync
{
}
