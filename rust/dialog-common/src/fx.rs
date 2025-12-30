#![allow(async_fn_in_trait)]
//! Algebraic effects system for capability-based programming.
//!
//! This module provides a lightweight algebraic effects implementation that enables
//! writing effectful code in a composable, testable, and type-safe manner. Effects
//! are represented as data that can be interpreted by different handlers (providers),
//! allowing the same code to run against real implementations, mocks, or test doubles.
//!
//! # Overview
//!
//! The effect system is built around these core concepts:
//!
//! | Concept | Description |
//! |---------|-------------|
//! | [`Capability`] | An abstract description of what can be done, with its output type |
//! | [`Provider`] | Something that can fulfill capability requests |
//! | [`Effect`] | A computation that produces a result when performed |
//! | [`Task`] | A composed computation yielding multiple effects |
//!
//! # Quick Start
//!
//! ## 1. Define a capability using `#[effect]`
//!
//! ```no_run
//! use dialog_common::fx::effect;
//!
//! #[effect]
//! pub trait BlockStore {
//!     async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String>;
//!     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String>;
//! }
//! ```
//!
//! This generates:
//! - A `BlockStore` module containing the trait and effect types
//! - `BlockStore::get(key)` and `BlockStore::set(key, value)` functions returning effect structs
//! - `BlockStore::Capability` enum representing all operations
//! - `BlockStore::Output` enum representing all results
//! - `BlockStore::dispatch()` function for implementing providers
//!
//! ## 2. Write effectful functions using `#[effectful]`
//!
//! ```no_run
//! # use dialog_common::fx::{effect, effectful, perform, Effect};
//! # #[effect]
//! # pub trait BlockStore {
//! #     async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String>;
//! #     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String>;
//! # }
//! #[effectful(BlockStore)]
//! fn copy_value(from: Vec<u8>, to: Vec<u8>) -> Result<(), String> {
//!     let value = perform!(BlockStore::get(from))?;
//!     perform!(BlockStore::set(to, value.unwrap_or_default()))
//! }
//! ```
//!
//! The `#[effectful]` macro transforms the function to return a [`Task`] that can
//! be performed with any compatible provider.
//!
//! ## 3. Implement the trait
//!
//! The `#[effect]` macro generates a module with a trait of the same name inside:
//!
//! ```no_run
//! # use dialog_common::fx::effect;
//! # use std::collections::HashMap;
//! # #[effect]
//! # pub trait BlockStore {
//! #     async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String>;
//! #     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String>;
//! # }
//! struct MemoryStore {
//!     data: HashMap<Vec<u8>, Vec<u8>>,
//! }
//!
//! // The trait is at BlockStore::BlockStore
//! impl BlockStore::BlockStore for MemoryStore {
//!     async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String> {
//!         Ok(self.data.get(&key).cloned())
//!     }
//!
//!     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
//!         self.data.insert(key, value);
//!         Ok(())
//!     }
//! }
//! ```
//!
//! ## 4. Create a provider using `#[provider]`
//!
//! Use the `#[provider]` macro to generate a `Provider` implementation:
//!
//! ```no_run
//! # use dialog_common::fx::{effect, effectful, perform, provider, Effect};
//! # use std::collections::HashMap;
//! # #[effect]
//! # pub trait BlockStore {
//! #     async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String>;
//! #     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String>;
//! # }
//! #[provider(BlockStore)]
//! struct MemoryStore {
//!     data: HashMap<Vec<u8>, Vec<u8>>,
//! }
//!
//! impl BlockStore::BlockStore for MemoryStore {
//!     async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String> {
//!         Ok(self.data.get(&key).cloned())
//!     }
//!     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
//!         self.data.insert(key, value);
//!         Ok(())
//!     }
//! }
//!
//! #[effectful(BlockStore)]
//! fn copy_value(from: Vec<u8>, to: Vec<u8>) -> Result<(), String> {
//!     let value = perform!(BlockStore::get(from))?;
//!     perform!(BlockStore::set(to, value.unwrap_or_default()))
//! }
//!
//! # async fn example() -> Result<(), String> {
//! let mut store = MemoryStore { data: HashMap::new() };
//!
//! copy_value(b"src".into(), b"dst".into())
//!     .perform(&mut store)
//!     .await
//! # }
//! ```
//!
//! # Capability Composition
//!
//! Multiple capabilities can be composed together using trait inheritance:
//!
//! ```no_run
//! use dialog_common::fx::effect;
//!
//! #[effect]
//! pub trait BlockStore {
//!     async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String>;
//!     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String>;
//! }
//!
//! #[effect]
//! pub trait Logger {
//!     async fn log(&self, message: String) -> Result<(), String>;
//! }
//!
//! // Compose capabilities using trait inheritance:
//! // #[effect]
//! // pub trait Env: BlockStore + Logger {}
//! // This creates an Env::Capability that includes both BlockStore and Logger operations.
//! ```
//!
//! Then use multiple capabilities in effectful functions:
//!
//! ```no_run
//! # use dialog_common::fx::{effect, effectful, perform, Effect};
//! # #[effect]
//! # pub trait BlockStore {
//! #     async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String>;
//! #     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String>;
//! # }
//! # #[effect]
//! # pub trait Logger {
//! #     async fn log(&self, message: String) -> Result<(), String>;
//! # }
//! #[effectful(BlockStore, Logger)]
//! fn logged_copy(from: Vec<u8>, to: Vec<u8>) -> Result<(), String> {
//!     perform!(Logger::log(format!("Copying {:?} to {:?}", from, to)))?;
//!     let value = perform!(BlockStore::get(from))?;
//!     perform!(BlockStore::set(to, value.unwrap_or_default()))
//! }
//! ```
//!
//! # How It Works
//!
//! Under the hood, the effect system uses coroutines (via `genawaiter`) to suspend
//! execution when an effect is performed and resume it when the provider returns
//! a result:
//!
//! ```text
//!     ┌─────────────────┐
//!     │  Effectful Code │
//!     └────────┬────────┘
//!              │ yields Capability
//!              ▼
//!     ┌─────────────────┐
//!     │    Provider     │
//!     └────────┬────────┘
//!              │ returns Output
//!              ▼
//!     ┌─────────────────┐
//!     │  Effectful Code │
//!     │   (resumed)     │
//!     └─────────────────┘
//! ```
//!
//! # Benefits
//!
//! - **Testability**: Swap providers to test effectful code without real I/O
//! - **Composability**: Combine multiple capabilities seamlessly
//! - **Type Safety**: The compiler ensures all required capabilities are provided
//! - **Separation of Concerns**: Business logic is separate from effect interpretation

use std::future::Future;
use std::pin::Pin;

use genawaiter::GeneratorState;
use genawaiter::sync::{Co, Gen};

// Re-export macros for convenient access
pub use dialog_macros::{effect, effectful, provider};

/// A capability represents an abstract operation with its output type.
///
/// This trait is automatically implemented by capability types generated
/// by the [`effect`] macro. Each capability represents a cohesive set of
/// operations.
///
/// # Example
///
/// The `#[effect]` macro on a trait like:
///
/// ```no_run
/// # use dialog_common::fx::effect;
/// #[effect]
/// pub trait BlockStore {
///     async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String>;
///     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String>;
/// }
/// ```
///
/// Generates a `BlockStore::Capability` enum that implements this trait.
pub trait Capability {
    /// The output type returned when capability requests are fulfilled.
    ///
    /// This is typically an enum with variants for each operation's return type.
    type Output: Default;
}

/// A provider supplies implementations for capability requests.
///
/// Providers bridge the gap between abstract effects and concrete implementations.
/// When an effect is performed, the provider receives the request and returns
/// the appropriate output.
///
/// The `#[effect]` macro generates `IntoProvider` implementations that allow
/// `&mut T` to be used directly with `.perform()`.
pub trait Provider {
    /// The capability this provider can fulfill.
    type Capability: Capability;

    /// Fulfill a capability request and return its output.
    fn provide(
        &mut self,
        request: Self::Capability,
    ) -> impl Future<Output = <Self::Capability as Capability>::Output>;
}

/// An effectful computation that produces `Output` when performed.
///
/// Types implementing this trait represent suspended computations that require
/// a capability to complete. They can be performed using `.perform(&mut backend).await`.
///
/// # Type Parameters
///
/// - `Output`: The result type produced when the effect is performed
/// - `Cap`: The capability required to perform this effect
///
/// # Example
///
/// ```no_run
/// # use dialog_common::fx::effect;
/// # use dialog_common::fx::{Effect, Capability, provider};
/// # use std::collections::HashMap;
/// # #[effect]
/// # pub trait BlockStore {
/// #     async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String>;
/// #     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String>;
/// # }
/// #[provider(BlockStore)]
/// struct MemoryStore { data: HashMap<Vec<u8>, Vec<u8>> }
///
/// impl BlockStore::BlockStore for MemoryStore {
///     async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String> {
///         Ok(self.data.get(&key).cloned())
///     }
///     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
///         self.data.insert(key, value);
///         Ok(())
///     }
/// }
///
/// # async fn example() -> Result<(), String> {
/// let mut store = MemoryStore { data: HashMap::new() };
/// let result: Result<Option<Vec<u8>>, String> = BlockStore::get(b"key".into())
///     .perform(&mut store)
///     .await;
/// # Ok(())
/// # }
/// ```
///
/// Effects can work with any capability that includes their operations via
/// `From`/`TryInto` conversions, enabling capability composition.
pub trait Effect<Output, Cap: Capability>: Sized {
    /// Perform this effect using a provider that supplies the required capability.
    ///
    /// The provider must implement [`Provider`] for the required capability.
    /// Use the `#[provider(EffectModule)]` macro to easily implement `Provider`
    /// for your types.
    fn perform<P>(self, provider: &mut P) -> impl Future<Output = Output>
    where
        P: Provider<Capability = Cap>;
}

/// A composed effect computation that yields capability requests.
///
/// `Task` wraps a coroutine that can yield multiple effects and ultimately
/// produce a final result. It implements [`Effect`], so tasks can be performed
/// just like individual effects.
///
/// Tasks are created by the [`effectful`] macro when transforming functions
/// that use `perform!`.
///
/// # Type Parameters
///
/// - `Cap`: The capability required by this task
/// - `F`: The future type produced by the internal coroutine
///
/// # Example
///
/// ```no_run
/// # use dialog_common::fx::effect;
/// # use dialog_common::fx::{Effect, Task, Capability, Provider};
/// # #[effect]
/// # pub trait BlockStore {
/// #     async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String>;
/// #     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String>;
/// # }
/// // Create a task manually
/// let task: Task<BlockStore::Capability, _> = Task::new(|co| async move {
///     let value = BlockStore::get(b"key".into()).perform(&mut &co).await?;
///     BlockStore::set(b"other".into(), value.unwrap_or_default())
///         .perform(&mut &co)
///         .await
/// });
/// ```
///
/// More commonly, tasks are created via the `#[effectful]` macro:
///
/// ```no_run
/// # use dialog_common::fx::{effect, effectful, perform, Effect};
/// # #[effect]
/// # pub trait BlockStore {
/// #     async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String>;
/// #     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String>;
/// # }
/// #[effectful(BlockStore)]
/// fn copy(from: Vec<u8>, to: Vec<u8>) -> Result<(), String> {
///     let value = perform!(BlockStore::get(from))?;
///     perform!(BlockStore::set(to, value.unwrap_or_default()))
/// }
/// ```
pub struct Task<Cap: Capability, F: Future> {
    generator: Gen<Cap, Cap::Output, F>,
}

impl<Cap: Capability, F: Future> Task<Cap, F> {
    /// Create a new task from a producer function.
    ///
    /// The producer receives a coroutine handle ([`Co`]) that can be used
    /// to perform effects within the task using `.perform(&mut &co).await`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use dialog_common::fx::effect;
    /// # use dialog_common::fx::{Effect, Task, Capability};
    /// # #[effect]
    /// # pub trait BlockStore {
    /// #     async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String>;
    /// #     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String>;
    /// # }
    /// let task: Task<BlockStore::Capability, _> = Task::new(|co| async move {
    ///     let value = BlockStore::get(b"key".into()).perform(&mut &co).await?;
    ///     Ok::<_, String>(value.unwrap_or_default())
    /// });
    /// ```
    pub fn new<P>(producer: P) -> Self
    where
        P: FnOnce(Co<Cap, Cap::Output>) -> F,
    {
        Self {
            generator: Gen::new(producer),
        }
    }
}

impl<Cap: Capability, F: Future> Effect<F::Output, Cap> for Task<Cap, F> {
    async fn perform<P>(self, provider: &mut P) -> F::Output
    where
        P: Provider<Capability = Cap>,
    {
        let mut generator = self.generator;
        let mut output = Cap::Output::default();

        loop {
            match Pin::new(&mut generator).resume_with(output) {
                GeneratorState::Yielded(capability) => {
                    output = provider.provide(capability).await;
                }
                GeneratorState::Complete(result) => {
                    return result;
                }
            }
        }
    }
}

/// Implement Provider for mutable references to providers.
///
/// This allows `.perform(&mut provider)` to work naturally.
impl<P: Provider> Provider for &mut P {
    type Capability = P::Capability;

    async fn provide(
        &mut self,
        request: Self::Capability,
    ) -> <Self::Capability as Capability>::Output {
        P::provide(*self, request).await
    }
}

/// Implement Provider directly for coroutine handle references.
///
/// This allows `Co` to be used directly with `.perform(&mut &co)` in effectful functions.
impl<Cap: Capability> Provider for &Co<Cap, Cap::Output> {
    type Capability = Cap;

    async fn provide(&mut self, request: Cap) -> Cap::Output {
        self.yield_(request).await
    }
}

/// Performs an effect inside an `#[effectful]` function.
///
/// This macro is a placeholder that gets transformed by the [`effectful`] macro
/// into `.perform(&__co).await` calls. Using it outside an `#[effectful]` function
/// results in a compile error.
///
/// # Example
///
/// ```no_run
/// # use dialog_common::fx::{effect, effectful, perform, Effect};
/// # #[effect]
/// # pub trait BlockStore {
/// #     async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String>;
/// #     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String>;
/// # }
/// #[effectful(BlockStore)]
/// fn get_or_default(key: Vec<u8>, default: Vec<u8>) -> Result<Vec<u8>, String> {
///     Ok(perform!(BlockStore::get(key))?.unwrap_or(default))
/// }
/// ```
///
/// # Errors
///
/// If used outside an `#[effectful]` function, you'll get:
///
/// ```text
/// error: perform! should only be used inside #[effectful] functions.
///        Did you forget to add #[effectful(...)] to your function?
/// ```
#[macro_export]
macro_rules! perform {
    ($e:expr) => {
        compile_error!(
            "perform! should only be used inside #[effectful] functions. \
             Did you forget to add #[effectful(...)] to your function?"
        )
    };
}

// Re-export perform at module level
pub use perform;
