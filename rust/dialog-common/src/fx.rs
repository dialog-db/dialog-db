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
//! | [`Effect`] | A computation that can be performed with a provider |
//! | Provider traits | Traits that define what operations a provider supports |
//!
//! # Quick Start
//!
//! ## 1. Define an effect using `#[effect]`
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
//! - A `block_store` module (snake_case) containing the `Provider` trait and effect types
//! - Re-export: `pub use block_store::Provider as BlockStore` (so you can `impl BlockStore`)
//! - Const: `pub const BlockStore: block_store::Consumer` (for `BlockStore.get(key)` syntax)
//! - `Get`, `Set` effect structs that implement `Effect`
//! - Blanket `Effect` implementations for any type implementing `Provider`
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
//!     let value = perform!(BlockStore.get(from))?;
//!     perform!(BlockStore.set(to, value.unwrap_or_default()))
//! }
//! ```
//!
//! The `#[effectful]` macro transforms the function to return an effect that can
//! be performed with any compatible provider.
//!
//! ## 3. Implement the trait and use it
//!
//! ```no_run
//! # use dialog_common::fx::{effect, effectful, perform, Effect};
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
//! // Just implement the trait - no #[provider] needed!
//! impl BlockStore for MemoryStore {
//!     async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String> {
//!         Ok(self.data.get(&key).cloned())
//!     }
//!
//!     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
//!         self.data.insert(key, value);
//!         Ok(())
//!     }
//! }
//!
//! # #[effectful(BlockStore)]
//! # fn copy_value(from: Vec<u8>, to: Vec<u8>) -> Result<(), String> {
//! #     let value = perform!(BlockStore.get(from))?;
//! #     perform!(BlockStore.set(to, value.unwrap_or_default()))
//! # }
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
//! trait Env: BlockStore + Logger {}
//! impl<T: BlockStore + Logger> Env for T {}
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
//! #[effectful(BlockStore + Logger)]
//! fn logged_copy(from: Vec<u8>, to: Vec<u8>) -> Result<(), String> {
//!     perform!(Logger.log(format!("Copying {:?} to {:?}", from, to)))?;
//!     let value = perform!(BlockStore.get(from))?;
//!     perform!(BlockStore.set(to, value.unwrap_or_default()))
//! }
//! ```
//!
//! # How It Works
//!
//! The effect system uses a simple trait-based approach:
//!
//! 1. Each effect operation (like `Get`, `Set`) is a struct implementing `Effect<Output, Provider>`
//! 2. The `#[effect]` macro generates blanket `Effect` impls for any type implementing the Provider trait
//! 3. The `#[effectful]` macro generates an inner struct implementing `Effect` that captures all arguments
//! 4. Effects are composed using trait bounds: `P: Store + Logger`
//!
//! ```text
//!     ┌─────────────────┐
//!     │  Effectful Code │
//!     └────────┬────────┘
//!              │ returns Effect<Output, P>
//!              ▼
//!     ┌─────────────────┐
//!     │   .perform()    │
//!     └────────┬────────┘
//!              │ calls provider methods
//!              ▼
//!     ┌─────────────────┐
//!     │    Provider     │
//!     │   (impl Trait)  │
//!     └─────────────────┘
//! ```
//!
//! # Benefits
//!
//! - **Testability**: Swap providers to test effectful code without real I/O
//! - **Composability**: Combine multiple capabilities seamlessly via trait bounds
//! - **Type Safety**: The compiler ensures all required capabilities are provided
//! - **Separation of Concerns**: Business logic is separate from effect interpretation
//! - **No genawaiter**: Pure async/await, stable Rust features only
//! - **No `#[provider]` macro**: Blanket impls handle everything

use std::future::Future;

// Re-export macros for convenient access
pub use dialog_macros::{effect, effectful};

/// An effectful computation that produces `Output` when performed with a `Provider`.
///
/// Types implementing this trait represent suspended computations that require
/// a provider to complete. They can be performed using `.perform(&mut provider).await`.
///
/// # Type Parameters
///
/// - `Output`: The result type produced when the effect is performed
/// - `Provider`: The type that can execute this effect (typically a trait bound)
///
/// # Example
///
/// ```no_run
/// # use dialog_common::fx::effect;
/// # use dialog_common::fx::Effect;
/// # use std::collections::HashMap;
/// # #[effect]
/// # pub trait BlockStore {
/// #     async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String>;
/// #     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String>;
/// # }
/// struct MemoryStore { data: HashMap<Vec<u8>, Vec<u8>> }
///
/// impl BlockStore for MemoryStore {
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
/// let result: Result<Option<Vec<u8>>, String> = BlockStore.get(b"key".into())
///     .perform(&mut store)
///     .await;
/// # Ok(())
/// # }
/// ```
pub trait Effect<Output, Provider> {
    /// Perform this effect using the given provider.
    fn perform(self, provider: &mut Provider) -> impl Future<Output = Output>;
}

/// Performs an effect inside an `#[effectful]` function.
///
/// This macro is a placeholder that gets transformed by the [`effectful`] macro
/// into `.perform(provider).await` calls. Using it outside an `#[effectful]` function
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
///     Ok(perform!(BlockStore.get(key))?.unwrap_or(default))
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

/// A wrapper that implements [`Effect`] for async closures.
///
/// This type is used internally by the `#[effectful]` macro to wrap function bodies
/// as effects. It enables capturing values from the enclosing scope (including `self`
/// for methods) while still implementing the `Effect` trait.
///
/// # Type Parameters
///
/// - `F`: The async closure type, typically `impl AsyncFnOnce(&mut Provider) -> Output`
///
/// # Example
///
/// ```no_run
/// # use dialog_common::fx::{Effect, Task};
/// fn create_effect<P>() -> impl Effect<i32, P> {
///     Task(async move |_provider: &mut P| { 42 })
/// }
/// ```
pub struct Task<F>(pub F);

impl<F, Output, Provider> Effect<Output, Provider> for Task<F>
where
    F: AsyncFnOnce(&mut Provider) -> Output,
{
    fn perform(self, provider: &mut Provider) -> impl Future<Output = Output> {
        (self.0)(provider)
    }
}

#[cfg(test)]
mod tests {
    use super::{effect, effectful, Effect};
    use std::collections::HashMap;

    // Basic effect with simple HashMap implementation
    #[effect]
    pub trait Store {
        async fn get(&self, key: String) -> Option<String>;
        async fn set(&mut self, key: String, value: String);
    }

    struct MemoryStore {
        data: HashMap<String, String>,
    }

    // No #[provider] needed - just implement the trait!
    impl Store for MemoryStore {
        async fn get(&self, key: String) -> Option<String> {
            self.data.get(&key).cloned()
        }
        async fn set(&mut self, key: String, value: String) {
            self.data.insert(key, value);
        }
    }

    #[tokio::test]
    async fn it_performs_effect_directly_on_provider() {
        let mut store = MemoryStore {
            data: HashMap::new(),
        };

        Store
            .set("key".into(), "value".into())
            .perform(&mut store)
            .await;

        let result = Store.get("key".into()).perform(&mut store).await;
        assert_eq!(result, Some("value".into()));
    }

    #[effectful(Store)]
    fn copy_value(from: String, to: String) {
        if let Some(value) = perform!(Store.get(from)) {
            perform!(Store.set(to, value));
        }
    }

    #[tokio::test]
    async fn it_transforms_perform_macro_in_effectful_fn() {
        let mut store = MemoryStore {
            data: HashMap::new(),
        };

        Store
            .set("source".into(), "hello".into())
            .perform(&mut store)
            .await;

        copy_value("source".into(), "dest".into())
            .perform(&mut store)
            .await;

        let result = Store.get("dest".into()).perform(&mut store).await;
        assert_eq!(result, Some("hello".into()));
    }

    #[effect]
    pub trait Logger {
        async fn log(&self, msg: String);
    }

    // Compose capabilities using trait inheritance
    trait Env: Store + Logger {}
    impl<T: Store + Logger> Env for T {}

    struct TestEnv {
        store: HashMap<String, String>,
    }

    impl Store for TestEnv {
        async fn get(&self, key: String) -> Option<String> {
            self.store.get(&key).cloned()
        }
        async fn set(&mut self, key: String, value: String) {
            self.store.insert(key, value);
        }
    }

    impl Logger for TestEnv {
        async fn log(&self, _msg: String) {}
    }

    #[effectful(Store + Logger)]
    fn logged_copy(from: String, to: String) {
        perform!(Logger.log("Copying".to_string()));
        if let Some(value) = perform!(Store.get(from)) {
            perform!(Store.set(to, value));
        }
    }

    #[effectful(Store)]
    fn setup_data(key: String, value: String) {
        perform!(Store.set(key, value));
    }

    #[effectful(Store)]
    fn get_data(key: String) -> Option<String> {
        perform!(Store.get(key))
    }

    #[tokio::test]
    async fn it_composes_multiple_effects() {
        let mut env = TestEnv {
            store: HashMap::new(),
        };

        // Use effectful functions
        setup_data("src".into(), "data".into())
            .perform(&mut env)
            .await;

        // Multi-capability effectful function
        logged_copy("src".into(), "dst".into())
            .perform(&mut env)
            .await;

        let result = get_data("dst".into()).perform(&mut env).await;
        assert_eq!(result, Some("data".into()));
    }

    struct Cache {
        prefix: String,
    }

    impl Cache {
        #[effectful(Store)]
        fn get(&self, key: String) -> Option<String> {
            perform!(Store.get(format!("{}{}", self.prefix, key)))
        }

        #[effectful(Store)]
        fn set(&self, key: String, value: String) {
            perform!(Store.set(format!("{}{}", self.prefix, key), value))
        }

        #[effectful(Store)]
        fn copy(&self, from: String, to: String) {
            if let Some(value) = perform!(self.get(from)) {
                perform!(self.set(to, value));
            }
        }
    }

    #[tokio::test]
    async fn it_supports_effectful_on_struct_methods() {
        let mut store = MemoryStore {
            data: HashMap::new(),
        };

        let cache = Cache {
            prefix: "cache:".into(),
        };

        cache
            .set("key".into(), "value".into())
            .perform(&mut store)
            .await;

        let result = cache.get("key".into()).perform(&mut store).await;
        assert_eq!(result, Some("value".into()));

        // Verify prefix was applied
        let raw = Store.get("cache:key".into()).perform(&mut store).await;
        assert_eq!(raw, Some("value".into()));
    }

    #[tokio::test]
    async fn it_chains_effectful_method_calls() {
        let mut store = MemoryStore {
            data: HashMap::new(),
        };

        let cache = Cache {
            prefix: "cache:".into(),
        };

        cache
            .set("src".into(), "data".into())
            .perform(&mut store)
            .await;

        cache
            .copy("src".into(), "dst".into())
            .perform(&mut store)
            .await;

        let result = cache.get("dst".into()).perform(&mut store).await;
        assert_eq!(result, Some("data".into()));
    }

    trait Storage {
        #[effectful(Store)]
        fn load(&self, key: String) -> Option<String>;

        #[effectful(Store)]
        fn save(&self, key: String, value: String);
    }

    struct PrefixedStorage {
        prefix: String,
    }

    impl Storage for PrefixedStorage {
        #[effectful(Store)]
        fn load(&self, key: String) -> Option<String> {
            perform!(Store.get(format!("{}{}", self.prefix, key)))
        }

        #[effectful(Store)]
        fn save(&self, key: String, value: String) {
            perform!(Store.set(format!("{}{}", self.prefix, key), value))
        }
    }

    #[tokio::test]
    async fn it_supports_effectful_on_trait_definitions_and_impls() {
        let mut store = MemoryStore {
            data: HashMap::new(),
        };

        let storage = PrefixedStorage {
            prefix: "storage:".into(),
        };

        storage
            .save("mykey".into(), "myvalue".into())
            .perform(&mut store)
            .await;

        let result = storage.load("mykey".into()).perform(&mut store).await;
        assert_eq!(result, Some("myvalue".into()));
    }

    // Test effect with Result return type
    #[effect]
    pub trait Fallible {
        async fn may_fail(&self, succeed: bool) -> Result<String, String>;
    }

    struct FallibleProvider;

    impl Fallible for FallibleProvider {
        async fn may_fail(&self, succeed: bool) -> Result<String, String> {
            if succeed {
                Ok("success".into())
            } else {
                Err("failure".into())
            }
        }
    }

    #[effectful(Fallible)]
    fn try_operation(succeed: bool) -> Result<String, String> {
        perform!(Fallible.may_fail(succeed))
    }

    #[tokio::test]
    async fn it_propagates_results_through_effectful_fns() {
        let mut provider = FallibleProvider;

        let ok_result = try_operation(true).perform(&mut provider).await;
        assert_eq!(ok_result, Ok("success".into()));

        let err_result = try_operation(false).perform(&mut provider).await;
        assert_eq!(err_result, Err("failure".into()));
    }

    #[effectful(Fallible)]
    fn chain_fallible() -> Result<String, String> {
        let first = perform!(Fallible.may_fail(true))?;
        let second = perform!(Fallible.may_fail(true))?;
        Ok(format!("{} and {}", first, second))
    }

    #[tokio::test]
    async fn it_supports_question_mark_operator_in_effectful() {
        let mut provider = FallibleProvider;

        let result = chain_fallible().perform(&mut provider).await;
        assert_eq!(result, Ok("success and success".into()));
    }
}
