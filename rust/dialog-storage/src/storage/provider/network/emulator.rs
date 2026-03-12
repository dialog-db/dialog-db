//! In-memory emulated network connection.
//!
//! When a [`Network`] is wrapped in [`Emulator`], all remote invocations are
//! redirected to in-memory [`Volatile`] storage regardless of the address type.
//!
//! Primary designed for testing allowing us to exercise the full push / pull
//! pipeline (invocation, dispatch, provider execution) without standing up real
//! S3 buckets or UCAN services.
//!
//! The emulator provides [`Route<Address>`] — a generic route that implements
//! [`ProviderRoute`] and [`Provider<RemoteInvocation<Fx, Address>>`] by delegating
//! to [`Volatile`] storage keyed by address. Each unique address gets its own
//! isolated `Volatile` instance.

use async_trait::async_trait;
use dialog_capability::{Capability, Constraint, Effect, Provider, ProviderRoute};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::remote::RemoteInvocation;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

pub use crate::Emulator;
use crate::provider::volatile::Volatile;

/// An emulated route that maps addresses to [`Volatile`] storage.
///
/// Each address gets its own independent `Volatile` instance, so data
/// stored via one address is isolated from other addresses.
///
/// Uses `parking_lot::RwLock` for interior mutability so
/// `Provider::execute` can take `&self`. Connections are wrapped in
/// `Arc` so they can be cloned out of the lock before any `.await`
/// points.
///
/// Implements [`ProviderRoute`] so it can be used as a field in a
/// `#[derive(Router)]` struct.
pub struct Route<Address> {
    connections: RwLock<HashMap<Address, Arc<Volatile>>>,
}

impl<Address> Route<Address> {
    /// Create a new emulated route with no cached connections.
    pub fn new() -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
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
    async fn execute(&self, input: RemoteInvocation<Fx, Address>) -> Fx::Output {
        let (capability, address) = input.into_parts();

        // Check the cache (read lock, dropped immediately).
        let volatile = {
            let cache = self.connections.read();
            cache.get(&address).cloned()
        };

        let volatile = match volatile {
            Some(v) => v,
            None => {
                let new_volatile = Arc::new(Volatile::new());
                // Insert into cache (write lock, dropped immediately).
                self.connections
                    .write()
                    .insert(address.clone(), new_volatile.clone());
                new_volatile
            }
        };

        // Execute on the Arc'd volatile — no lock held.
        <Volatile as Provider<Fx>>::execute(&volatile, capability).await
    }
}
