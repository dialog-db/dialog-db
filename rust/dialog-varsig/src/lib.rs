//! [Varsig] implementation.
//!
//! This includes signature metadata and helpers for signing, verifying,
//! and encoding payloads per a given [Varsig] configuration.
//!
//! [Varsig]: https://github.com/ChainAgnostic/varsig

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod algorithm;
pub mod codec;
pub mod did;
pub mod principal;
pub mod resolver;
pub mod signature;

pub use algorithm::*;
pub use codec::*;
pub use did::*;
pub use principal::*;
pub use resolver::*;
pub use signature::*;
