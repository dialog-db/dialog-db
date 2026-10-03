//! Walking a committed tree's stored nodes, for the censuses that inspect
//! the tree's shape rather than its facts.

use anyhow::Result;
use dialog_artifacts::{Datum, Key, State};
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, ConditionalSync};
use dialog_search_tree::{LoadBlock, PersistentNode};

/// A node of an artifact tree, as stored.
pub type TreeNode = PersistentNode<Key, State<Datum>>;

/// One stored node, as [`walk`] visits it.
pub struct Visit {
    /// The node's address.
    pub hash: Blake3Hash,
    /// The node's encoded size in bytes.
    pub size: usize,
    /// The separator its parent links it under (empty for the root).
    pub separator: Vec<u8>,
    /// The node.
    pub node: TreeNode,
}

/// The node stored under `hash`, loaded through `env`, and its encoded
/// size in bytes.
pub async fn node<Env>(env: &Env, hash: &Blake3Hash) -> Result<(TreeNode, usize)>
where
    Env: Provider<LoadBlock> + ConditionalSync,
{
    let block = LoadBlock::new(hash.clone())
        .perform(env)
        .await?
        .ok_or_else(|| anyhow::anyhow!("reachable node {hash} missing"))?;
    let size = block.as_ref().len();
    Ok((TreeNode::try_from(block)?, size))
}

/// Visits every node reachable from `root` depth first, children left to
/// right, so leaves arrive in key order.
pub async fn walk<Env>(
    env: &Env,
    root: Blake3Hash,
    mut visit: impl FnMut(Visit) -> Result<()>,
) -> Result<()>
where
    Env: Provider<LoadBlock> + ConditionalSync,
{
    let mut stack = vec![(root, Vec::new())];
    while let Some((hash, separator)) = stack.pop() {
        let (node, size) = node(env, &hash).await?;
        if let dialog_search_tree::NodeBody::Index(index) = node.body() {
            for at in (0..index.len()).rev() {
                stack.push((index.hash_at(at)?.clone(), index.separator(at)?));
            }
        }
        visit(Visit {
            hash,
            size,
            separator,
            node,
        })?;
    }
    Ok(())
}
