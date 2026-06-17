//! Shared test helpers for the FS-remote integration tests.
//!
//! Drives a capability through the full FS-remote [`ForkInvocation`] pipeline
//! (attest a unit authorization → resolve the registered directory → delegate
//! to `dialog_storage`), bypassing operator-level `Network` dispatch so the
//! tests exercise the [`Fs`] site directly.

#![allow(dead_code)]

use dialog_capability::{Capability, Constraint, Effect, Fork, ForkInvocation, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_remote_fs::{Fs, FsAddress};

/// Execute a capability against the [`Fs`] site for the given handle id,
/// driving it through `Fork::attest` → `ForkInvocation::perform`.
pub async fn execute<Fx>(handle_id: &str, capability: Capability<Fx>) -> Fx::Output
where
    Fx: Effect + ConditionalSend + ConditionalSync + 'static,
    Fx::Of: Constraint<Capability: ConditionalSend + ConditionalSync>,
    Fs: Provider<ForkInvocation<Fs, Fx>>,
    ForkInvocation<Fs, Fx>: ConditionalSend,
{
    Fork::<Fs, Fx>::new(capability, FsAddress::new(handle_id))
        .attest(Default::default())
        .perform(&Fs)
        .await
}
