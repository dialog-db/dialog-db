//! Remote invocation envelope for targeting effects at specific remote sites.
//!
//! [`RemoteInvocation`] pairs a [`Capability`] with an address, allowing
//! providers to route effects to remote site at a given address. It implements
//! [`Invocation`] following the same pattern as [`Authorized`] and allows
//! performing capabilities on addressed sites. Effectively it's a routing
//! mechanism for dispatching to a provider that correspondns to a specific
//! address.

use dialog_capability::{Capability, Constraint, Effect, Invocation, Provider};
use dialog_common::ConditionalSend;
use std::fmt::{self, Debug, Formatter};

/// A routing envelope that pairs a capability with an address that
/// identifies remote site that should handle the effect. This allows the
/// same subject to be targeted at different remote endpoints.
pub struct RemoteInvocation<Fx: Effect, Address> {
    capability: Capability<Fx>,
    address: Address,
}

/// Manual impl for the same reason as `Debug` — `Capability<Fx>` requires
/// `<Fx::Of as Constraint>::Capability: Clone`, not just `Fx: Clone`.
impl<Fx: Effect + Clone, Address: Clone> Clone for RemoteInvocation<Fx, Address>
where
    Fx::Of: Constraint,
    <Fx::Of as Constraint>::Capability: Clone,
{
    fn clone(&self) -> Self {
        Self {
            capability: self.capability.clone(),
            address: self.address.clone(),
        }
    }
}

// Manual impl because `#[derive(Debug)]` would only bound the type
// parameters (`Fx`, `Address`) but `Capability<Fx>` also needs
// `<Fx::Of as Constraint>::Capability: Debug` to be formattable —
// a bound on an associated type that the derive macro can't infer.
impl<Fx: Effect + Debug, Address: Debug> Debug for RemoteInvocation<Fx, Address>
where
    Fx::Of: Constraint,
    <Fx::Of as Constraint>::Capability: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteInvocation")
            .field("capability", &self.capability)
            .field("address", &self.address)
            .finish()
    }
}

impl<Fx, Address> RemoteInvocation<Fx, Address>
where
    Fx: Effect,
    Fx::Of: Constraint,
{
    /// Create a new remote invocation targeting the given address.
    pub fn new(capability: Capability<Fx>, address: Address) -> Self {
        Self {
            capability,
            address,
        }
    }

    /// Invoked capability.
    pub fn capability(&self) -> &Capability<Fx> {
        &self.capability
    }

    /// Address of the remote.
    pub fn address(&self) -> &Address {
        &self.address
    }

    /// Consume and return the inner capability.
    pub fn into_capability(self) -> Capability<Fx> {
        self.capability
    }

    /// Consume and return the inner address.
    pub fn into_address(self) -> Address {
        self.address
    }

    /// Consume and return both parts.
    pub fn into_parts(self) -> (Capability<Fx>, Address) {
        (self.capability, self.address)
    }
}

impl<Fx, Address> RemoteInvocation<Fx, Address>
where
    Fx: Effect,
    Fx::Of: Constraint,
    Address: ConditionalSend,
{
    /// Perform the remote invocation against a provider.
    pub async fn perform<Env>(self, env: &mut Env) -> Fx::Output
    where
        Env: Provider<RemoteInvocation<Fx, Address>>,
    {
        env.execute(self).await
    }
}

impl<Fx, Address> Invocation for RemoteInvocation<Fx, Address>
where
    Fx: Effect,
    Fx::Of: Constraint,
    Address: ConditionalSend,
{
    type Input = RemoteInvocation<Fx, Address>;
    type Output = Fx::Output;
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::{Attenuation, Did, Subject};
    use serde::{Deserialize, Serialize};

    const TEST_DID: &str = "did:key:z6Mk...";

    fn test_did() -> Did {
        TEST_DID.parse().unwrap()
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Archive;
    impl Attenuation for Archive {
        type Of = Subject;
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Get {
        digest: Vec<u8>,
    }
    impl Effect for Get {
        type Of = Archive;
        type Output = Option<Vec<u8>>;
    }

    #[derive(Debug, Clone, PartialEq)]
    struct TestAddress(String);

    #[test]
    fn it_constructs_remote_invocation() {
        let capability = Subject::from(test_did()).attenuate(Archive).invoke(Get {
            digest: vec![1, 2, 3],
        });

        let remote = RemoteInvocation::new(capability, TestAddress("https://example.com".into()));

        assert_eq!(remote.address(), &TestAddress("https://example.com".into()));
        assert_eq!(remote.capability().subject(), &test_did());
    }

    #[test]
    fn it_decomposes_into_parts() {
        let get = Subject::from(test_did()).attenuate(Archive).invoke(Get {
            digest: vec![1, 2, 3],
        });

        let remote = RemoteInvocation::new(get, TestAddress("addr".into()));
        let (capability, address) = remote.into_parts();

        assert_eq!(capability.subject(), &test_did());
        assert_eq!(address, TestAddress("addr".into()));
    }
}
