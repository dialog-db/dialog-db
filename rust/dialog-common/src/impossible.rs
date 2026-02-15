use std::convert::Infallible;
use std::marker::PhantomData;

/// Generic version of [`Infallible`] — an uninhabited type with an optional
/// type parameter.
///
/// Like [`Infallible`], `Impossible` can never be constructed. Any trait
/// whose methods receive `self`, `&self`, or `&mut self` can be trivially
/// implemented by matching on the uninhabited field:
///
/// ```rust,ignore
/// impl<T> MyTrait for Impossible<T> {
///     fn do_thing(&mut self) -> Whatever {
///         match self.0 {}
///     }
/// }
/// ```
///
/// Because the body is unreachable, the compiler accepts any return type
/// without requiring a real implementation. This makes `Impossible` useful
/// as a stand-in for feature-gated types — you can satisfy trait bounds
/// at compile time without the real type existing.
///
/// # Type aliases
///
/// The type parameter `T` (defaults to `()`) lets generic type aliases
/// consume all their parameters:
///
/// ```rust,ignore
/// #[cfg(feature = "foo")]
/// type Backend<C> = RealBackend<C>;
/// #[cfg(not(feature = "foo"))]
/// type Backend<C> = Impossible<C>;
/// ```
///
/// # Feature-gated enums
///
/// Combined with conditional type aliases, `Impossible` lets you write a
/// single blanket impl over a feature-gated enum without duplicating code
/// per feature combination:
///
/// ```rust,ignore
/// enum Connection<I> {
///     #[cfg(feature = "s3")]
///     S3(s3::Connection<I>),
///     Emulator(emulator::Connection<I>),
/// }
///
/// // s3::Connection<I> is Impossible<I> when `s3` is off, so the bound
/// // is trivially satisfied and the S3 match arm is compiled out.
/// impl<I, Fx> Provider<Fx> for Connection<I>
/// where
///     s3::Connection<I>: Provider<Fx>,
///     emulator::Connection<I>: Provider<Fx>,
/// { ... }
/// ```
pub struct Impossible<T = ()>(
    /// The uninhabited field — accessible so downstream crates can write
    /// `match self.0 {}` in their own trait impls.
    pub Infallible,
    pub PhantomData<T>,
);
