//! The evaluation scope: the environment bundle every premise
//! executes against.
//!
//! Evaluation reaches the world exclusively through effects the
//! environment provides: range scans ([`Select`]) with demand
//! recording, rule discovery ([`SelectRules`]), idempotent
//! content-addressed loads for resolver premises (tree nodes through
//! [`LoadBlock`], spilled values through [`LoadBlob`]), and
//! advisory replication hints ([`Preload`]) for ranges evaluation
//! expects to need. It also holds what evaluation keeps between
//! queries ([`Holds`]): the outputs of formulas that ask to be cached.
//! `Scope` names that bundle once so premise evaluation signatures stay
//! stable as effects are added.

use dialog_artifacts::{Estimate, LoadBlob, LoadBlock, Preload, Select};
use dialog_capability::Provider;
use dialog_common::{ConditionalSync, Holds};

use crate::source::SelectRules;

/// The full provider bundle premise evaluation requires. Blanket
/// implemented: any environment providing the effects is a `Scope`.
pub trait Scope<'a>:
    Provider<Select<'a>>
    + Provider<SelectRules>
    + Provider<LoadBlock>
    + Provider<LoadBlob>
    + Provider<Preload>
    + Provider<Estimate>
    + Holds
    + ConditionalSync
{
}

impl<'a, T> Scope<'a> for T where
    T: Provider<Select<'a>>
        + Provider<SelectRules>
        + Provider<LoadBlock>
        + Provider<LoadBlob>
        + Provider<Preload>
        + Provider<Estimate>
        + Holds
        + ConditionalSync
{
}
