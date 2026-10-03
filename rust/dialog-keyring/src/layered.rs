//! Layered sealing: a tree whose nodes open in nested levels.
//!
//! [`SealedBlocks`](crate::SealedBlocks) seals a node whole, so a party either
//! reads it or holds an opaque blob. Layered sealing splits each node into
//! three regions, so access comes in levels, each strictly inside the next
//! (`notes/privacy.md`'s L1–L3):
//!
//! | Level | Holds | Can |
//! | --- | --- | --- |
//! | Structure | a root's [`StructureKey`] | walk the tree, verify and copy every block, read nothing |
//! | Range | plus the range secret | route a key to the leaf that would hold it, read no entry |
//! | Content | plus the content secret | read the tree |
//!
//! A node's envelope ([`Envelope`]) carries, under its own structure key, the
//! address and structure key of each child, so holding a root's structure key
//! opens the whole tree's shape. The separators that route a key sit in the
//! range region, and the node itself in the content region. How the keys
//! derive from each other, and why content access cannot be held without
//! range access, is in [`keys`].
//!
//! # Addresses are the envelopes' own hashes
//!
//! A node's address is `blake3` of its envelope, and a parent records its
//! children by those addresses. So anyone, holding any level or none, can
//! check that a block is the one its address names, and a replicator can
//! refuse corrupt data it cannot read. The plaintext identity the tree links
//! by is never stored: it lives only inside content regions. That removes the
//! need for the blinding key flat sealing has, whose job was to keep the
//! store from being addressed by plaintext identity.
//!
//! # Convergence
//!
//! Every key is derived from the node's plaintext and the level secrets, the
//! range and content regions are encrypted with a fixed nonce (each of their
//! keys encrypts exactly one message; see [`keys`]), and the structure
//! region's nonce is derived from what it seals. So two replicas sealing the same tree under the
//! same generations produce byte-identical envelopes at identical addresses,
//! and a diff between them still prunes.
//!
//! # Writing is bottom-up
//!
//! A parent records its children's addresses, which are hashes of their
//! envelopes, so children are sealed first. [`LayeredBlocks::write`] seals
//! what a persist staged, from the root down to the nodes already stored.
//!
//! # Kept in memory or in an archive
//!
//! [`LayeredBlocks`] keeps envelopes in memory. [`LayeredArchive`] keeps
//! them in an archive catalog: because an envelope's address is its hash,
//! envelopes are ordinary content-addressed blocks there, imported with
//! `Import` and read with `Get`, and every path that moves blocks between
//! archives moves them unchanged.

mod archive;
pub use archive::LayeredArchive;

pub mod asset;
pub use asset::{AssetOpener, AssetSealer};

mod envelope;
pub use envelope::Envelope;

pub mod keys;
pub use keys::{Access, Level, LevelSecret, StructureKey, Writer};

mod party;
pub use party::Sealing;

mod projection;
pub use projection::{Attach, NoAttachments};

mod space;
pub use space::Space;

mod store;
pub use store::{LayeredBlocks, LayeredRoot};

mod value;
