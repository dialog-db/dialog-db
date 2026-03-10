/// Marker trait for types that can route remote invocations to an address.
///
/// Types implementing this trait declare which `Address` type they handle.
/// The `#[derive(Router)]` macro uses this to generate forwarding
/// `Provider<RemoteInvocation<Fx, Address>>` impls on composite structs.
///
/// Fields whose types don't implement `ProviderRoute` are silently skipped
/// by the generated impls (the where clause is unsatisfied).
pub trait ProviderRoute {
    /// The address type this route handles.
    type Address;
}
