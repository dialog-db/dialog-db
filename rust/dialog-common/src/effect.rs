//! Effect and Provider traits for capability-based operations.
//!
//! This module defines the core traits for effect-based programming:
//!
//! - [`Effect`] - Trait for types that represent operations with outputs
//! - [`Provider`] - Trait for types that can execute effects

use crate::{ConditionalSend, ConditionalSync};
use async_trait::async_trait;

/// Trait for effect types that can be performed.
///
/// Effects are command types that define their output type.
/// This provides a clear contract for what each command returns.
#[async_trait]
pub trait Effect: Sized {
    /// The output type produced when this effect is performed.
    type Output;

    /// Perform this effect using the given provider.
    ///
    /// This is a convenience method that allows calling `effect.perform(&provider)`
    /// instead of `provider.execute(effect)`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = Put { catalog: "index".into(), content: data }
    ///     .perform(&credentials)
    ///     .await?;
    /// ```
    async fn perform<Env>(self, env: &Env) -> Self::Output
    where
        Self: ConditionalSend,
        Env: Provider<Self>,
    {
        env.execute(self).await
    }
}

/// Provider trait for executing effects.
///
/// Implementations execute specific effect types and return their output.
/// This allows requiring specific capabilities via trait bounds:
///
/// ```ignore
/// async fn reader<P>(provider: &P) -> ...
/// where
///     P: Provider<GetEffect> + Provider<ListEffect>
/// { ... }
/// ```
#[async_trait]
pub trait Provider<Fx: Effect + ConditionalSend>: ConditionalSend + ConditionalSync {
    /// Execute the effect and return its output.
    async fn execute(&self, effect: Fx) -> Fx::Output;
}
