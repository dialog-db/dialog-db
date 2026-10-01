//! The socket: one connection to an access service that carries
//! invocations both ways for as long as it stays open, which is what lets
//! the service answer a watch each time the cell it follows changes.
//!
//! A frame a client sends is what a request carries: the invocation's
//! container, and the bytes a write stores. Every frame is an invocation
//! verified on its own, so the connection carries no authority of its
//! own, and every invocation must say when it was issued (see
//! [`Issuance::Required`](crate::Issuance::Required)). An invocation is
//! named by its content identifier, which is how the service's frames say
//! which invocation they answer: an invocation carries a nonce, so no two
//! share one.
//!
//! The service answers an invocation as it would a request, and answers
//! a watch with the state of the cell it follows when the watch begins,
//! then each state the cell takes, until the watch is cancelled, its
//! authority ends, or the connection closes.

mod frame;
pub use frame::*;

mod session;
pub use session::*;

mod client;
pub use client::Sockets;
