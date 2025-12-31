//! Connection trait for opening resources from addresses.

/// Trait for types that can open/create resources from addresses.
///
/// This is used to establish connections to storage backends on demand.
/// Connectors are typically stateless configuration objects that produce
/// new backend instances on each call to `open`.
///
/// # Type Parameters
///
/// - `Resource`: The type of resource this connection produces (e.g., a storage backend)
///
/// # Example
///
/// ```ignore
/// use dialog_artifacts::fx::Connection;
///
/// struct RestConnector {
///     // configuration
/// }
///
/// impl Connection<RestStorageBackend> for RestConnector {
///     type Address = RemoteAddress;
///     type Error = ConnectionError;
///
///     async fn open(&self, address: &Self::Address) -> Result<RestStorageBackend, Self::Error> {
///         // Create connection based on address
///     }
/// }
/// ```
pub trait Connection<Resource> {
    /// The address type used to identify where to connect.
    type Address;

    /// Error type for connection failures.
    type Error;

    /// Open a connection to the resource at the given address.
    ///
    /// Creates a new resource instance. The produced resources typically
    /// share underlying connections internally (e.g., via Arc).
    fn open(
        &self,
        address: &Self::Address,
    ) -> impl std::future::Future<Output = Result<Resource, Self::Error>> + Send;
}
