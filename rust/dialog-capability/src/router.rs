use crate::ProviderRoute;

/// Derive macro that generates [`Router`] impl and
/// `Provider<RemoteInvocation<Fx, Address>>` impls for composite structs whose
/// fields each route to a different address type.
pub use dialog_macros::Provider;
pub use dialog_macros::Router;

/// Trait for composite types that route remote invocations to
/// per-address-type fields via a unified address enum.
///
/// Implemented automatically by the [`Router`](derive@Router) derive macro,
/// which generates a `{StructName}Address` enum from the routable fields
/// and implements dispatch logic.
///
/// Each field must implement [`ProviderRoute`](crate::ProviderRoute) so the macro
/// can determine the per-field address type. The generated unified address
/// enum combines all field addresses into a single type.
///
/// Fields annotated with `#[route(skip)]` are excluded from routing.
///
/// # Example
///
/// ```rust,ignore
/// #[derive(dialog_capability::Router)]
/// pub struct Network {
///     #[cfg(feature = "s3")]
///     s3: route::Route<s3::Credentials, s3::Connection>,
///     #[cfg(feature = "ucan")]
///     ucan: route::Route<ucan::Credentials, ucan::Connection>,
/// }
/// // Generates: NetworkAddress enum, Router impl, ProviderRoute blanket,
/// // and Provider<RemoteInvocation<Fx, NetworkAddress>> dispatch.
/// ```
pub trait Router {
    /// The unified address type for this router, typically a generated enum.
    type Address;
}

/// Blanket impl: any `Router` automatically satisfies `ProviderRoute`.
impl<T: Router> ProviderRoute for T {
    type Address = <T as Router>::Address;
}
