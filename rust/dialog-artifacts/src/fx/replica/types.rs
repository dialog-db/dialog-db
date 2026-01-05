//! Core types for the effectful replica system.
//!
//! This module re-exports types from the main replica module that don't require
//! modification for the effectful version.

// Re-export all types from the main replica module
pub use crate::replica::{
    BranchId, BranchState, Edition, NodeReference, Occurence, Principal, Revision, Site,
    EMPT_TREE_HASH,
};
