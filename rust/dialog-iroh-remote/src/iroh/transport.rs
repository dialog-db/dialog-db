//! Native transport: endpoint runtime, framing, client, host, and swarm.
//!
//! Everything that touches a live iroh endpoint lives below this module,
//! which only compiles on non-wasm targets. The rest of the crate —
//! addresses, authorization, the wire protocol types — is target
//! independent.

mod convert;
pub(crate) mod framing;
mod host;
mod node;
pub(crate) mod request;
mod swarm;

pub use host::*;
pub use node::*;
pub use swarm::*;

#[cfg(test)]
mod tests;
