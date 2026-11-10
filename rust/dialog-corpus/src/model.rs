pub use crate::Entity;

mod fact;
pub use fact::*;

mod data;
pub use data::*;

mod instruction;
pub use instruction::*;

pub mod selector;
pub use selector::{Selector, ValueBound};

mod estimate;
pub use estimate::Estimate;

mod query;
pub use query::{
    FactStream, FetchBudget, Likelihood, Preload, PreloadQueue, PreloadRequest, Select, Speculation,
};

mod asset;
pub use asset::*;

mod update;
pub use update::{
    AssetChange, Change, ChangeStream, Changes, Contender, Pick, SortKey, Standing, Statement,
    Update, sort_key,
};

mod relation;
pub use relation::*;

mod symbol;
pub use symbol::*;

mod value;
pub use value::*;

mod ordkey;
pub use ordkey::*;

mod ordvalue;
pub use ordvalue::*;

mod cause;
pub use cause::*;

mod r#match;
pub use r#match::*;

/// A BLAKE3 digest as the model carries it: the bytes of a value's
/// reference, a blob's hash, a spilled value's address.
pub type Blake3Hash = [u8; 32];

use crate::tree::Corpus;

/// The search tree each fact index is: keys are raw key bytes and values
/// are [`State`](crate::State) payloads (see [`crate::tree`]).
pub type Index = Corpus;
