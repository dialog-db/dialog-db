//! In-memory emulated network connection.

use async_trait::async_trait;
use dialog_capability::{Capability, Constraint, Effect, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::remote::RemoteInvocation;
use std::convert::Infallible;

use super::connector::Connector;
use super::{Address, Network};
pub use crate::Emulator;
use crate::provider::volatile::Volatile;

/// Emulated connection backed by volatile in-memory storage.
///
/// Carries the `T` type parameter so `emulator::Connection<T>` is symmetric
/// with `s3::Connection<Issuer>` and `ucan::Connection<Issuer>`.
#[derive(Debug)]
pub struct Connection<T> {
    address: Address,
    inner: Volatile,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Connection<T> {
    /// Create a new emulated connection for the given address.
    pub fn new(address: Address) -> Self {
        Self {
            address,
            inner: Volatile::new(),
            _marker: std::marker::PhantomData,
        }
    }

    /// The address this connection was opened for.
    pub fn address(&self) -> &Address {
        &self.address
    }

    /// Returns a mutable reference to the underlying volatile storage.
    pub fn as_volatile_mut(&mut self) -> &mut Volatile {
        &mut self.inner
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer> Connector<Address> for Emulator<Network<Issuer>>
where
    Issuer: Clone + ConditionalSend + ConditionalSync,
{
    type Connection = super::Connection<Issuer>;
    type Error = Infallible;

    // `Address` is a feature-gated enum; with no backends enabled it is
    // uninhabited, making this body unreachable.
    #[allow(unreachable_code)]
    async fn open(
        &mut self,
        address: &Address,
    ) -> Result<&mut super::Connection<Issuer>, Infallible> {
        if self.0.router.get_mut(address).is_none() {
            self.0.router.insert(
                address.clone(),
                super::Connection::Emulator(Connection::new(address.clone())),
            );
        }
        Ok(self.0.router.get_mut(address).unwrap())
    }
}

/// Blanket [`Provider`] impl: any effect that [`Volatile`] can handle, the
/// emulated connection can also by forwarding it.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer, Fx> Provider<Fx> for Connection<Issuer>
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: ConditionalSend,
    Issuer: ConditionalSend + ConditionalSync,
    Volatile: Provider<Fx>,
{
    async fn execute(&mut self, input: Capability<Fx>) -> Fx::Output {
        <Volatile as Provider<Fx>>::execute(self.as_volatile_mut(), input).await
    }
}

/// Blanket [`Provider`] for remote invocations on [`Emulator<Network>`].
///
/// Opens (or reuses) an emulated connection for the target address, then
/// delegates to the [`Connection`](emulator::Connection) provider above.
/// The `S3` match arm is unreachable because the emulator only creates
/// `Emulator` connections.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer, Fx> Provider<RemoteInvocation<Fx, Address>> for Emulator<Network<Issuer>>
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: ConditionalSend,
    Issuer: Clone + ConditionalSend + ConditionalSync + 'static,
    Volatile: Provider<Fx>,
{
    // `Address` is a feature-gated enum; with no backends enabled it is
    // uninhabited, making this body unreachable.
    #[allow(unreachable_code)]
    async fn execute(&mut self, input: RemoteInvocation<Fx, Address>) -> Fx::Output {
        let (capability, address) = input.into_parts();
        let connection = match self.open(&address).await {
            Ok(c) => c,
            Err(infallible) => match infallible {},
        };
        match connection {
            super::Connection::Emulator(emulated) => {
                <Connection<Issuer> as Provider<Fx>>::execute(emulated, capability).await
            }
            #[cfg(feature = "s3")]
            _ => unreachable!("emulator mode only produces Emulator connections"),
        }
    }
}
