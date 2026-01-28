use crate::Invocation;

/// Trait for environments that can execute invocations.
///
/// The type parameter `I` implements `Invocation`, which determines:
/// - `I::Input` - what the provider receives
/// - `I::Output` - what the provider returns
///
/// # Example
///
/// ```
/// use dialog_capability::{Provider, Invocation};
/// use async_trait::async_trait;
///
/// // Define an invocation type
/// struct MyInvocation;
///
/// impl Invocation for MyInvocation {
///     type Input = String;
///     type Output = usize;
/// }
///
/// // Implement Provider for your environment
/// struct MyEnv;
///
/// #[async_trait]
/// impl Provider<MyInvocation> for MyEnv {
///     async fn execute(&mut self, input: String) -> usize {
///         input.len()
///     }
/// }
/// ```
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Provider<I: Invocation> {
    /// Execute an invocation and return the output.
    async fn execute(&mut self, input: I::Input) -> I::Output;
}
