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
//!
//! # How a connection fails, and what a client does
//!
//! A socket is one long-lived connection that a space's resolves,
//! publishes and watches all share, so each way it can fail has an answer
//! a caller can act on, and a test that pins it (the loopback service
//! plays each failure out: see `helpers::Fault`).
//!
//! - **It cannot be opened.** The cell's invocation goes as a request, and
//!   no socket at that service is tried again for a while, longer after
//!   each failure in a row (30 s, doubling up to 15 minutes), so a service
//!   with no socket, or one that is down, costs no connection attempt per
//!   call or per space.
//! - **It goes silent** (a laptop slept, a network changed, a NAT forgot
//!   the mapping, a service wedged). Nothing reports this: the platform
//!   learns only when something it sends fails, and a watch sends
//!   nothing. The client pings ([`PING`], answered with [`PONG`]) every
//!   [`Liveness::heartbeat`], and closes a connection it heard nothing
//!   from for [`Liveness::deadline`]. Its waits end: a watch ends, a read
//!   is answered as unavailable, a write as not known to have failed.
//! - **It is cut** (a deploy, an evicted Durable Object). The same as
//!   above, at once. `Branch::replicate` begins an ended watch again after
//!   a wait that doubles each time in a row, up to a minute, and the new
//!   watch's first state is where the cell stands, so nothing the cell
//!   took meanwhile is missed.
//! - **A frame never leaves** (the connection closed as it was sent). The
//!   invocation goes as a request: it was not carried out.
//! - **A frame leaves and no answer comes** (the connection failed after,
//!   or the service did not answer within [`Liveness::answer`]). The same
//!   invocation cannot be sent again: the service would refuse it as
//!   presented before. It is answered as unavailable, so the caller sends
//!   it again, with an invocation of its own, here or at the peer's next
//!   address. That holds for a write too, since the service's writes are
//!   safe to send again: one that would leave the cell as it already is
//!   is answered as done, and one that conflicts with what the cell holds
//!   is answered with that state. A write that landed before its answer
//!   was lost finds itself, and one that did not lands or learns where
//!   the cell went. A request sent and not answered is answered the same.
//! - **The service cannot read a frame.** Its answer names the frame by
//!   the `id` the client gave it, so it reaches the one invocation it
//!   answers. An answer naming no frame and no invocation (to bytes that
//!   were not a frame at all) fails every invocation awaiting an answer
//!   on the connection, since any of them may have been the one.
//! - **The service refuses a watch.** The refusal is the answer to
//!   beginning it, an error of the `Watch` effect, not the first thing the
//!   watch delivers, so whoever begins it can tell a watch that runs from
//!   one that never will.
//! - **A watch's reader falls behind** (a host paused replication, a tab
//!   is hidden). Its states are coalesced to the newest, never queued per
//!   change; the last state before the watch ends is never dropped.
//! - **The service's clock differs from the client's.** An invocation
//!   issued further from the service's clock than its freshness window is
//!   refused as stale, with what the clock read. The client keeps the
//!   difference for that subject and sends the effect once more, stamped
//!   by the service's clock (see `dialog_ucan::clock`).
//! - **Nothing waits on it.** It is closed once idle for
//!   [`Liveness::idle`], so a site that touched many spaces does not keep
//!   a socket open to each.
//! - **Invocations race to open it.** They share one connection.

mod frame;
pub use frame::*;

mod session;
pub use session::*;

mod client;
pub(crate) use client::Failed;
pub use client::{Liveness, Sockets};
