//! The pure rules of Sapling: the rules that its formulas call in Dialog DB,
//! and that Sapling itself uses, so that each rule has one body. The crate
//! builds for the host and for `wasm32-wasip3`. It reads nothing and writes
//! nothing.
//!
//! - `name`: persona names and space names.
//! - `selector`: the grammar of a selector of a persona or a space.
//! - `suffix`: the suffix that tells apart the things of one name.
//! - `cause`: the cause key of a create.
//! - `did`: the `did:key` of a key.
//! - `did_url`: a persona DID with a suggested name.
//! - `invite`: the invite that a share makes, and that a join takes.

pub mod cause;
pub mod did;
pub mod did_url;
pub mod invite;
pub mod name;
pub mod selector;
pub mod suffix;
