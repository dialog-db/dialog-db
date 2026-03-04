//! In-memory emulated network connection.
//!
//! When a [`Network`] is wrapped in [`Emulator`], all remote invocations are
//! redirected to in-memory [`Volatile`] storage regardless of the address type.
//!
//! The emulator provides [`Route<Address>`] — a generic route that implements
//! [`ProviderRoute`] and [`Provider<RemoteInvocation<Fx, Address>>`] by delegating
//! to [`Volatile`] storage keyed by address.

use async_trait::async_trait;
use dialog_capability::{Capability, Constraint, Effect, Provider, ProviderRoute};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::remote::RemoteInvocation;
use std::collections::HashMap;
use std::hash::Hash;

pub use crate::Emulator;
use crate::provider::volatile::Volatile;

/// An emulated route that maps addresses to [`Volatile`] storage.
///
/// Each address gets its own independent `Volatile` instance, so data
/// stored via one address is isolated from other addresses.
///
/// Implements [`ProviderRoute`] so it can be used as a field in a
/// `#[derive(Router)]` struct.
pub struct Route<Address> {
    connections: HashMap<Address, Volatile>,
}

impl<Address> Route<Address> {
    /// Create a new emulated route with no cached connections.
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
        }
    }
}

impl<Address> Default for Route<Address> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Address> ProviderRoute for Route<Address> {
    type Address = Address;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Address, Fx> Provider<RemoteInvocation<Fx, Address>> for Route<Address>
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: ConditionalSend,
    Address: Eq + Hash + Clone + ConditionalSend + ConditionalSync + 'static,
    Volatile: Provider<Fx>,
{
    async fn execute(&mut self, input: RemoteInvocation<Fx, Address>) -> Fx::Output {
        let (capability, address) = input.into_parts();
        if !self.connections.contains_key(&address) {
            self.connections.insert(address.clone(), Volatile::new());
        }
        let volatile = self.connections.get_mut(&address).unwrap();
        <Volatile as Provider<Fx>>::execute(volatile, capability).await
    }
}
