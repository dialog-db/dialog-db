use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

use dialog_common::ConditionalSend;

use crate::Command;

/// A future a provider answers with later, boxed so a provider can be
/// named without naming its future.
#[cfg(not(target_arch = "wasm32"))]
pub type Pending<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A future a provider answers with later, boxed so a provider can be
/// named without naming its future (single-threaded wasm form).
#[cfg(target_arch = "wasm32")]
pub type Pending<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// How a provider answers a command: with the output at once, or with a
/// future that produces it.
///
/// A caller awaits either one the same way, so it cannot tell, and does
/// not have to, whether the provider had the answer at hand. A provider
/// that does (a cache that holds the value, a check that fails before any
/// I/O) answers [`Resume::now`] and allocates nothing; one that has to
/// wait answers [`Resume::later`] with the future that waits. A provider
/// standing in front of another hands back what it was handed, so an
/// answer at hand stays at hand through every layer; one that has to look
/// at the answer first takes it apart with [`into_now`](Self::into_now).
///
/// ```
/// use dialog_capability::Resume;
///
/// # async fn example() {
/// let now: Resume<'_, usize> = Resume::now(1);
/// let later: Resume<'_, usize> = Resume::later(async { 2 });
/// assert!(now.is_now() && !later.is_now());
/// assert_eq!(now.await + later.await, 3);
/// # }
/// ```
#[must_use = "a provider's answer does nothing until it is awaited"]
pub struct Resume<'a, T>(Answer<'a, T>);

/// The answer at hand (taken when it is polled), or the future that waits
/// for it.
enum Answer<'a, T> {
    Now(Option<T>),
    Later(Pending<'a, T>),
}

impl<'a, T> Resume<'a, T> {
    /// Answers with `value` at once.
    pub fn now(value: T) -> Self {
        Resume(Answer::Now(Some(value)))
    }

    /// Answers with what `future` produces.
    pub fn later(future: impl Future<Output = T> + ConditionalSend + 'a) -> Self {
        Resume(Answer::Later(Box::pin(future)))
    }

    /// Whether the answer is at hand, so awaiting it cannot suspend.
    pub fn is_now(&self) -> bool {
        matches!(self.0, Answer::Now(Some(_)))
    }

    /// The answer when it is at hand, else the future that produces it:
    /// for a provider that answers at once when what it asked beneath it
    /// did, and waits only when it has to.
    pub fn into_now(self) -> Result<T, Pending<'a, T>> {
        match self.0 {
            Answer::Now(value) => Ok(value.expect("an answer at hand is taken once")),
            Answer::Later(future) => Err(future),
        }
    }
}

impl<'a, T: 'a> Resume<'a, T> {
    /// The answer with `f` applied: at once when it is at hand, else
    /// once the future produces it.
    pub fn map<U: 'a>(self, f: impl FnOnce(T) -> U + ConditionalSend + 'a) -> Resume<'a, U>
    where
        T: ConditionalSend,
    {
        match self.into_now() {
            Ok(value) => Resume::now(f(value)),
            Err(future) => Resume::later(async move { f(future.await) }),
        }
    }
}

impl<T> Unpin for Resume<'_, T> {}

impl<T> Future for Resume<'_, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        match &mut self.get_mut().0 {
            Answer::Now(value) => {
                Poll::Ready(value.take().expect("an answer at hand is polled once"))
            }
            Answer::Later(future) => future.as_mut().poll(cx),
        }
    }
}

impl<T> fmt::Debug for Resume<'_, T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Answer::Now(value) => f.debug_tuple("Now").field(value).finish(),
            Answer::Later(_) => f.debug_tuple("Later").finish_non_exhaustive(),
        }
    }
}

/// Trait for environments that can execute commands.
///
/// The type parameter `C` implements `Command`, which determines:
/// - `C::Input` - what the provider receives
/// - `C::Output` - what the provider returns
///
/// A provider answers with a [`Resume`]: the output at once, or a future
/// that produces it. The [`provider`](macro@crate::provider) attribute
/// writes the common case, an `async fn execute` whose body is the
/// future; a provider that sometimes has the answer at hand writes
/// `execute` itself and returns [`Resume::now`] on that path.
///
/// # Example
///
/// ```
/// use dialog_capability::{Provider, Command, Resume};
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
/// #[dialog_capability::provider]
/// impl Provider<MyCommand> for MyEnv {
///     async fn execute(&self, input: String) -> usize {
///         input.len()
///     }
/// }
///
/// // Or answer at once when the answer is at hand
/// struct Counting { cached: usize }
///
/// impl Provider<MyCommand> for Counting {
///     fn execute(&self, input: String) -> Resume<'_, usize> {
///         if input.is_empty() {
///             return Resume::now(self.cached);
///         }
///         Resume::later(async move { input.len() })
///     }
/// }
/// ```
pub trait Provider<C: Command> {
    /// Execute a command and return the output.
    fn execute(&self, input: C::Input) -> Resume<'_, C::Output>;
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
/// # #[dialog_capability::provider]
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
impl<C, T> Provider<C> for &T
where
    C: Command,
    T: Provider<C> + ?Sized,
{
    fn execute(&self, input: C::Input) -> Resume<'_, C::Output> {
        (**self).execute(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Len;

    impl Command for Len {
        type Input = String;
        type Output = usize;
    }

    struct Env;

    impl Provider<Len> for Env {
        fn execute(&self, input: String) -> Resume<'_, usize> {
            if input.is_empty() {
                return Resume::now(0);
            }
            Resume::later(async move { input.len() })
        }
    }

    #[dialog_common::test]
    async fn it_answers_now_or_later() {
        let env = Env;
        let now = env.execute(String::new());
        assert!(now.is_now());
        assert_eq!(now.await, 0);
        let later = env.execute("abc".to_string());
        assert!(!later.is_now());
        assert_eq!(later.await, 3);
        assert_eq!(Resume::now(1).map(|n| n + 1).await, 2);
        assert_eq!(Resume::later(async { 1 }).map(|n| n + 1).await, 2);
        assert!(Resume::now(1).into_now().is_ok());
        assert!(Resume::<usize>::later(async { 1 }).into_now().is_err());
    }
}
