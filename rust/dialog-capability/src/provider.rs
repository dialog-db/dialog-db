use crate::Command;

/// Trait for environments that can execute commands.
///
/// The type parameter `C` implements `Command`, which determines:
/// - `C::Input` - what the provider receives
/// - `C::Output` - what the provider returns
///
/// # Example
///
/// ```
/// use dialog_capability::{Provider, Command};
/// use async_trait::async_trait;
///
/// // Define a command type
/// struct MyCommand;
///
/// impl Command for MyCommand {
///     type Input = String;
///     type Output = usize;
/// }
///
/// // Implement Provider for your environment
/// struct MyEnv;
///
/// #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
/// #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
/// impl Provider<MyCommand> for MyEnv {
///     async fn execute(&self, input: String) -> usize {
///         input.len()
///     }
/// }
/// ```
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Provider<C: Command> {
    /// Execute a command and return the output.
    async fn execute(&self, input: C::Input) -> C::Output;
}
