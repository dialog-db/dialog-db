use core::future::Future;
use core::pin::Pin;

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

/// A borrowed environment provides what the environment provides.
///
/// Code that takes its environment by value (a stream that keeps it, a
/// reader cloned into each of its parts) is handed a reference to the
/// environment, never a wrapper around it: the reference performs every
/// command on the environment itself, so nothing stands between the
/// reader and what the environment does with the command.
///
/// ```no_run
/// # use dialog_capability::{Command, Provider};
/// # struct Count;
/// # impl Command for Count { type Input = (); type Output = usize; }
/// # struct Env;
/// # #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
/// # #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
/// # impl Provider<Count> for Env {
/// #     async fn execute(&self, _: ()) -> usize { 1 }
/// # }
/// async fn count<P: Provider<Count>>(env: P) -> usize {
///     env.execute(()).await
/// }
/// # async fn example() {
/// let env = Env;
/// assert_eq!(count(&env).await, 1);
/// # }
/// ```
// Written out rather than through `async_trait`: the borrowed
// environment hands back the environment's own future, so it asks
// nothing of the command's input that the environment does not.
impl<C, T> Provider<C> for &T
where
    C: Command,
    T: Provider<C> + ?Sized,
{
    #[cfg(not(target_arch = "wasm32"))]
    fn execute<'life0, 'async_trait>(
        &'life0 self,
        input: C::Input,
    ) -> Pin<Box<dyn Future<Output = C::Output> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        (**self).execute(input)
    }

    #[cfg(target_arch = "wasm32")]
    fn execute<'life0, 'async_trait>(
        &'life0 self,
        input: C::Input,
    ) -> Pin<Box<dyn Future<Output = C::Output> + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        (**self).execute(input)
    }
}
