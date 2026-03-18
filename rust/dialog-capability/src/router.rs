/// Marker trait for composite types that route remote invocations to
/// per-address-type fields.
///
/// Implemented automatically by the [`Router`](derive@Router) derive macro.
/// Each field must implement [`ProviderRoute`](crate::ProviderRoute) so the macro
/// can determine the address type via the associated `Address` type.
///
/// Fields annotated with `#[route(skip)]` are excluded from routing.
///
/// # Example
///
/// ```rust,ignore
/// #[derive(dialog_capability::Router)]
/// pub struct Network<Issuer> {
///     #[cfg(feature = "s3")]
///     s3: s3::Route<Issuer>,
///     #[cfg(feature = "ucan")]
///     ucan: ucan::Route<Issuer>,
/// }
/// ```
pub trait Router {}

/// Derive macro that generates [`Router`] impl and
/// `Provider<RemoteInvocation<Fx, Address>>` impls for composite structs whose
/// fields each route to a different address type.
pub use dialog_macros::Router;
