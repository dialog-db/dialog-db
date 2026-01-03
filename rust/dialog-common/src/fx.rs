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
//! # Generic Effect Traits
//!
//! Effect traits can have generic type parameters. For generic traits, the macro
//! generates a function instead of a const, requiring turbofish syntax:
//!
//! ```no_run
//! use dialog_common::fx::{effect, Effect};
//!
//! #[effect]
//! pub trait State<T: Clone + Send + Sync + 'static> {
//!     async fn get(&self) -> T;
//!     async fn set(&mut self, value: T);
//! }
//!
//! // Provider implements State for a specific type
//! struct Counter(i32);
//!
//! impl State<i32> for Counter {
//!     async fn get(&self) -> i32 {
//!         self.0
//!     }
//!     async fn set(&mut self, value: i32) {
//!         self.0 = value;
//!     }
//! }
//!
//! # async fn example() {
//! let mut counter = Counter(0);
//!
//! // Use turbofish syntax: State::<T>().method()
//! State::<i32>().set(42).perform(&mut counter).await;
//! let value = State::<i32>().get().perform(&mut counter).await;
//! assert_eq!(value, 42);
//! # }
//! ```
//!
//! # Associated Types in Effect Traits
//!
//! Effect traits can have associated types. The associated type is determined
//! by the provider implementation, not at the effect creation site:
//!
//! ```no_run
//! use dialog_common::fx::{effect, Effect};
//!
//! #[effect]
//! pub trait Producer {
//!     type Item: Clone + Send + Sync;
//!     async fn produce(&self) -> Self::Item;
//! }
//!
//! // Provider determines what Item is
//! struct NumberProducer(i32);
//!
//! impl Producer for NumberProducer {
//!     type Item = i32;
//!
//!     async fn produce(&self) -> Self::Item {
//!         self.0
//!     }
//! }
//!
//! # async fn example() {
//! let mut producer = NumberProducer(42);
//!
//! // No turbofish needed - Item type comes from the provider
//! let value = Producer.produce().perform(&mut producer).await;
//! assert_eq!(value, 42);
//! # }
//! ```
//!
//! The key difference:
//! - **Generic params** (`State<T>`): Type specified at call site → `State::<i32>().get()`
//! - **Associated types** (`type Item`): Type determined by provider → `Producer.produce()`
//!
//! # How It Works
//!
//! The effect system uses a simple trait-based approach:
//!
//! 1. Each effect operation (like `Get`, `Set`) is a struct implementing `Effect<Provider>`
//! 2. The `#[effect]` macro generates blanket `Effect` impls for any type implementing the Provider trait
//! 3. The `#[effectful]` macro generates an inner struct implementing `Effect` that captures all arguments
//! 4. Effects are composed using trait bounds: `P: Store + Logger`
//!
//! ```text
//!     ┌─────────────────┐
//!     │  Effectful Code │
//!     └────────┬────────┘
//!              │ returns impl Effect<P, Output = T>
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

/// An effectful computation that produces an output when performed with a `Provider`.
///
/// Types implementing this trait represent suspended computations that require
/// a provider to complete. They can be performed using `.perform(&mut provider).await`.
///
/// # Type Parameters
///
/// - `Provider`: The type that can execute this effect (typically a trait bound)
///
/// # Associated Types
///
/// - `Output`: The result type produced when the effect is performed
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
pub trait Effect<Provider> {
    /// The output type produced when the effect is performed.
    type Output;
    /// Perform this effect using the given provider.
    fn perform(self, provider: &mut Provider) -> impl Future<Output = Self::Output>;
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
/// fn create_effect<P>() -> impl Effect<P, Output = i32> {
///     Task::new(async move |_provider: &mut P| { 42 })
/// }
/// ```
pub struct Task<F, Output>(pub F, pub std::marker::PhantomData<Output>);

impl<F, Output> Task<F, Output> {
    /// Create a new Task wrapping an async closure.
    pub fn new(f: F) -> Self {
        Task(f, std::marker::PhantomData)
    }
}

impl<F, Output, Provider> Effect<Provider> for Task<F, Output>
where
    F: AsyncFnOnce(&mut Provider) -> Output,
{
    type Output = Output;

    fn perform(self, provider: &mut Provider) -> impl Future<Output = Self::Output> {
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

    // ==========================================================================
    // Tests for generic effect traits
    // ==========================================================================

    /// A generic effect trait that works with any item type T
    #[effect]
    pub trait Container<T: Clone + Send + Sync + 'static> {
        async fn put(&mut self, item: T);
        async fn take(&self) -> Option<T>;
    }

    /// Provider that stores items of type T
    struct ItemHolder<T> {
        item: Option<T>,
    }

    impl<T: Clone + Send + Sync + 'static> Container<T> for ItemHolder<T> {
        async fn put(&mut self, item: T) {
            self.item = Some(item);
        }

        async fn take(&self) -> Option<T> {
            self.item.clone()
        }
    }

    #[tokio::test]
    async fn it_supports_generic_effect_traits_with_strings() {
        let mut holder: ItemHolder<String> = ItemHolder { item: None };

        // Use the Consumer function for generic traits
        Container::<String>()
            .put("hello".to_string())
            .perform(&mut holder)
            .await;

        let result = Container::<String>().take().perform(&mut holder).await;
        assert_eq!(result, Some("hello".to_string()));
    }

    #[tokio::test]
    async fn it_supports_generic_effect_traits_with_integers() {
        let mut holder: ItemHolder<i32> = ItemHolder { item: None };

        Container::<i32>().put(42).perform(&mut holder).await;

        let result = Container::<i32>().take().perform(&mut holder).await;
        assert_eq!(result, Some(42));
    }

    /// A generic effect with multiple type parameters
    #[effect]
    pub trait Converter<From: Clone + Send + Sync + 'static, To: Clone + Send + Sync + 'static> {
        async fn convert(&self, input: From) -> To;
    }

    struct StringToInt;

    impl Converter<String, i32> for StringToInt {
        async fn convert(&self, input: String) -> i32 {
            input.parse().unwrap_or(0)
        }
    }

    #[tokio::test]
    async fn it_supports_generic_effects_with_multiple_type_params() {
        let mut converter = StringToInt;

        let result = Converter::<String, i32>()
            .convert("123".to_string())
            .perform(&mut converter)
            .await;

        assert_eq!(result, 123);
    }

    /// Test effectful functions that use generic effects
    #[effectful(Container<String>)]
    fn store_greeting(name: String) {
        perform!(Container::<String>().put(format!("Hello, {}!", name)));
    }

    #[effectful(Container<String>)]
    fn get_greeting() -> Option<String> {
        perform!(Container::<String>().take())
    }

    #[tokio::test]
    async fn it_supports_effectful_functions_with_generic_effects() {
        let mut holder: ItemHolder<String> = ItemHolder { item: None };

        store_greeting("World".to_string())
            .perform(&mut holder)
            .await;

        let result = get_greeting().perform(&mut holder).await;
        assert_eq!(result, Some("Hello, World!".to_string()));
    }

    // ==========================================================================
    // Tests for associated types in effect traits
    // ==========================================================================
    // With Effect<Provider> having associated type Output, the cycle issue is resolved.
    // The macro now generates proper Effect impls that use P::Item as Output.

    #[effect]
    pub trait Producer {
        type Item: Clone + Send + Sync;
        async fn produce(&self) -> Self::Item;
    }

    struct NumberProducer {
        value: i32,
    }

    impl Producer for NumberProducer {
        type Item = i32;

        async fn produce(&self) -> Self::Item {
            self.value
        }
    }

    #[tokio::test]
    async fn it_supports_associated_types_in_effect_traits() {
        let mut producer = NumberProducer { value: 42 };

        let result = Producer.produce().perform(&mut producer).await;
        assert_eq!(result, 42);
    }
}
