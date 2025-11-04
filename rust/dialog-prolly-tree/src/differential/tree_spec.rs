// Tree specification macro and distribution simulator for testing
//
// This module provides tools to create prolly trees with deterministic structure
// for testing the differential algorithm.

use crate::Distribution;
use dialog_storage::HashType;
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
};

/// A distribution that reads ranks directly from keys.
/// Keys are encoded as: [actual_key_bytes, 0x00, rank_byte]
/// This makes the distribution trivial - just read the last byte!
#[derive(Clone)]
pub struct DistributionSimulator;

impl<Hash>
    Distribution<Vec<u8>, Hash> for DistributionSimulator
where
    Hash: HashType,
{
    const BRANCH_FACTOR: u32 = 4;

    fn rank(key: &Vec<u8>) -> u32 {
        // Keys are encoded as [key_bytes, 0x00, rank_byte]
        // Just read the last byte as the rank
        if key.len() >= 2 && key[key.len() - 2] == 0x00 {
            key[key.len() - 1] as u32
        } else {
            1 // Default rank for keys without encoding
        }
    }
}

/// Get the next alphabetic key (a -> b -> c -> ... -> z -> aa -> ab -> ...)
fn next_alpha_key(key: &[u8]) -> Vec<u8> {
    let mut result = key.to_vec();
    let mut i = result.len();

    loop {
        if i == 0 {
            // Overflow: we need to add a new character
            result.insert(0, b'a');
            break;
        }
        i -= 1;

        if result[i] < b'z' {
            result[i] += 1;
            break;
        } else {
            result[i] = b'a';
            // Continue to carry
        }
    }

    result
}

/// Build a rank map from the tree spec
/// For branching factor BF=4, we use generous rank spacing to ensure boundaries form
/// Height 0 boundaries get rank 2, height 1 get rank 4, height 2 get rank 6, etc.
/// If a boundary appears at multiple heights, it gets the HIGHEST rank.
pub fn build_rank_map(levels: &[Vec<Vec<u8>>]) -> HashMap<Vec<u8>, u32> {
    let mut rank_map = HashMap::new();

    // Process from bottom to top, so higher levels overwrite lower levels
    // This ensures keys appearing at multiple heights get the HIGHEST rank
    for (level_idx, boundaries) in levels.iter().enumerate().rev() {
        let height = levels.len() - level_idx - 1;
        let rank = (height + 2) as u32;

        for boundary in boundaries {
            // Insert or overwrite - higher heights (processed later in reverse) will overwrite
            rank_map.insert(boundary.clone(), rank);
        }
    }

    rank_map
}

/// Expected operation on a node during differentiation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expect {
    /// Node should be read during differentiation
    Read,
    /// Node is in memory and doesn't need to be read (e.g., root nodes)
    Skip,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeDescriptor {
    // corresponds to a..b
    Range(String, String),
    // corresponds to ..c
    OpenRange(String),
    // corresponds to (a..d)
    SkipRange(String, String),
    // corresponds to (..k)
    SkipOpenRange(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeDescriptor(pub Vec<Vec<NodeDescriptor>>);

impl TreeDescriptor {
    /// Validate the tree structure
    fn validate(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.0.is_empty() {
            return Err("TreeDescriptor must have at least one level".into());
        }

        // Extract upper bounds from each level
        let mut levels_bounds: Vec<Vec<&str>> = Vec::new();

        for level_descriptors in &self.0 {
            let mut bounds = Vec::new();
            for descriptor in level_descriptors {
                let upper_bound = match descriptor {
                    NodeDescriptor::Range(_, last) => last.as_str(),
                    NodeDescriptor::OpenRange(last) => last.as_str(),
                    NodeDescriptor::SkipRange(_, last) => last.as_str(),
                    NodeDescriptor::SkipOpenRange(last) => last.as_str(),
                };
                bounds.push(upper_bound);
            }
            levels_bounds.push(bounds);
        }

        // Validate each level
        for (level_idx, bounds) in levels_bounds.iter().enumerate() {
            let height = self.0.len() - 1 - level_idx;

            // Check boundaries are in ascending order
            for i in 1..bounds.len() {
                if bounds[i] <= bounds[i - 1] {
                    return Err(format!(
                        "Boundaries at height {} must be in strictly ascending order: {:?} <= {:?}",
                        height,
                        bounds[i - 1],
                        bounds[i]
                    )
                    .into());
                }
            }

            // If not the bottom level, verify parent boundaries exist in children
            if level_idx + 1 < levels_bounds.len() {
                let child_bounds = &levels_bounds[level_idx + 1];
                for parent_bound in bounds {
                    if !child_bounds.contains(parent_bound) {
                        return Err(format!(
                            "Boundary '{}' at height {} must exist at height {}",
                            parent_bound,
                            height,
                            height - 1
                        )
                        .into());
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn build(
        self,
        storage: dialog_storage::Storage<
            dialog_storage::CborEncoder,
            dialog_storage::JournaledStorage<
                dialog_storage::MemoryStorageBackend<[u8; 32], Vec<u8>>,
            >,
        >,
    ) -> Result<TreeSpec, Box<dyn std::error::Error + Send + Sync>> {
        use std::collections::BTreeMap;

        // Validate the tree structure first
        self.validate()?;

        // Disable journaling during tree building to avoid polluting with build reads
        storage.backend.disable_journal();

        // First, collect metadata to build the tree
        let mut all_segments = Vec::new();
        let mut boundaries_per_level = Vec::new();
        // Track expected operations for each boundary
        let mut expected_ops: HashMap<(Vec<u8>, usize), Expect> = HashMap::new();

        for (level_idx, level_descriptors) in self.0.iter().enumerate() {
            let mut level_segment_specs = Vec::new();
            let mut level_boundaries = Vec::new();

            let height = self.0.len() - 1 - level_idx;

            for descriptor in level_descriptors {
                let (first_key, upper_bound, is_skipped) = match descriptor {
                    NodeDescriptor::Range(first, last) => {
                        (Some(first.as_str()), last.as_str(), false)
                    }
                    NodeDescriptor::OpenRange(last) => (None, last.as_str(), false),
                    NodeDescriptor::SkipRange(first, last) => {
                        (Some(first.as_str()), last.as_str(), true)
                    }
                    NodeDescriptor::SkipOpenRange(last) => (None, last.as_str(), true),
                };

                let boundary = upper_bound.as_bytes().to_vec();
                let expected_op = if is_skipped {
                    Expect::Skip
                } else {
                    Expect::Read
                };

                expected_ops.insert((boundary.clone(), height), expected_op);
                level_segment_specs.push((first_key, boundary.clone()));
                level_boundaries.push(boundary);
            }

            all_segments.push(level_segment_specs);
            boundaries_per_level.push(level_boundaries);
        }

        // Infer all keys from the bottom level
        let bottom_segments = all_segments
            .last()
            .expect("tree_spec requires at least one level");
        let collection = Self::infer_keys_from_segments(bottom_segments);

        // Build rank map
        let ranks = build_rank_map(&boundaries_per_level);

        // Build tree with encoded keys
        let mut btree_collection = BTreeMap::new();
        for key in &collection {
            let rank = ranks.get(key).copied().unwrap_or(1);
            let mut encoded_key = key.clone();
            encoded_key.push(0x00);
            encoded_key.push(rank as u8);
            btree_collection.insert(encoded_key, key.clone());
        }

        let temp_tree = crate::Tree::from_collection(btree_collection, storage.clone()).await?;

        // Now build NodeSpec levels from the actual tree
        let max_height = self.0.len() - 1;
        let mut spec = vec![Vec::new(); self.0.len()];

        // Disable journaling during spec building to avoid tracking child loads
        storage.backend.disable_journal();

        let root_hash = if let Some(root) = temp_tree.root() {
            Box::pin(Self::build_spec_from_node(
                &mut spec,
                root,
                &storage,
                max_height,
                &expected_ops,
            ))
            .await;
            Some(*root.hash())
        } else {
            None
        };

        // Re-enable journaling to track root and differential reads
        storage.backend.enable_journal();

        // Load tree from hash so root is freshly loaded (not from temp_tree)
        let tree = if let Some(hash) = root_hash {
            crate::Tree::from_hash(&hash, storage.clone()).await?
        } else {
            temp_tree
        };

        Ok(TreeSpec {
            spec,
            tree,
            storage,
        })
    }

    /// Infer keys from segment specifications
    fn infer_keys_from_segments(segments: &[(Option<&str>, Vec<u8>)]) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        let mut expected_next = vec![b'a'];

        for (first_key_opt, upper_bound) in segments {
            let start_key = if let Some(first_key_str) = first_key_opt {
                first_key_str.as_bytes().to_vec()
            } else {
                expected_next.clone()
            };

            let mut current = start_key;
            loop {
                keys.push(current.clone());
                if &current == upper_bound {
                    break;
                }
                current = next_alpha_key(&current);
            }

            expected_next = next_alpha_key(upper_bound);
        }

        keys
    }

    /// Recursively build NodeSpecs from the tree structure
    async fn build_spec_from_node(
        spec: &mut [Vec<NodeSpec>],
        node: &crate::Node< Vec<u8>, Vec<u8>, dialog_storage::Blake3Hash>,
        storage: &dialog_storage::Storage<
            dialog_storage::CborEncoder,
            dialog_storage::JournaledStorage<
                dialog_storage::MemoryStorageBackend<[u8; 32], Vec<u8>>,
            >,
        >,
        height: usize,
        expected_ops: &HashMap<(Vec<u8>, usize), Expect>,
    ) {
        let decoded_boundary = decode_key(node.upper_bound());
        let hash = *node.hash();

        // Look up the expected operation for this node
        let expected_op = expected_ops
            .get(&(decoded_boundary.clone(), height))
            .cloned()
            .unwrap_or(Expect::Read);

        // Create and add the NodeSpec
        let level_idx = spec.len() - 1 - height;
        spec[level_idx].push(NodeSpec::new(decoded_boundary, height, hash, expected_op));

        if node.is_segment() {
            return;
        }

        // Only recurse if we have more levels to go and height won't underflow
        if height > 0 {
            if let Ok(children) = node.load_children(storage).await {
                for child in children {
                    Box::pin(Self::build_spec_from_node(
                        spec,
                        &child,
                        storage,
                        height - 1,
                        expected_ops,
                    ))
                    .await;
                }
            }
        }
    }
}

/// Specification for a single node in the tree
#[derive(Clone)]
pub struct NodeSpec {
    pub boundary: Vec<u8>,
    pub height: usize,
    pub hash: [u8; 32],
    pub expect: Expect,
}

impl NodeSpec {
    pub fn new(boundary: Vec<u8>, height: usize, hash: [u8; 32], expected_op: Expect) -> Self {
        Self {
            boundary,
            height,
            hash,
            expect: expected_op,
        }
    }
}

impl Debug for NodeSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeSpec")
            .field("boundary", &String::from_utf8_lossy(&self.boundary))
            .field("height", &self.height)
            .field("hash", &self.hash.display())
            .field("expect", &self.expect)
            .finish()
    }
}

/// Decode a key by removing the [0x00, rank] suffix
fn decode_key(encoded: &[u8]) -> Vec<u8> {
    if encoded.len() >= 2 && encoded[encoded.len() - 2] == 0x00 {
        encoded[..encoded.len() - 2].to_vec()
    } else {
        encoded.to_vec()
    }
}

/// Compiled TreeSpec with tree built and hashes populated
pub struct TreeSpec {
    pub spec: Vec<Vec<NodeSpec>>, // Node specs with hashes populated
    tree: crate::Tree<
        DistributionSimulator,
        Vec<u8>,
        Vec<u8>,
        dialog_storage::Blake3Hash,
        dialog_storage::Storage<
            dialog_storage::CborEncoder,
            dialog_storage::JournaledStorage<
                dialog_storage::MemoryStorageBackend<[u8; 32], Vec<u8>>,
            >,
        >,
    >,
    storage: dialog_storage::Storage<
        dialog_storage::CborEncoder,
        dialog_storage::JournaledStorage<dialog_storage::MemoryStorageBackend<[u8; 32], Vec<u8>>>,
    >,
}

impl TreeSpec {
    /// Get a reference to the compiled tree
    pub fn tree(
        &self,
    ) -> &crate::Tree<
        DistributionSimulator,
        Vec<u8>,
        Vec<u8>,
        dialog_storage::Blake3Hash,
        dialog_storage::Storage<
            dialog_storage::CborEncoder,
            dialog_storage::JournaledStorage<
                dialog_storage::MemoryStorageBackend<[u8; 32], Vec<u8>>,
            >,
        >,
    > {
        &self.tree
    }

    /// Visualize the full tree structure by loading all nodes
    /// Temporarily disables journaling during visualization to avoid polluting read tracking
    #[allow(dead_code)]
    pub async fn visualize(&self) -> String {
        // Disable journaling during visualization
        self.storage.backend.disable_journal();

        let mut output = String::new();

        if let Some(root) = self.tree.root() {
            Self::visualize_node(&mut output, root, &self.storage, "", true).await;
        } else {
            output.push_str("(empty tree)\n");
        }

        // Re-enable journaling after visualization
        self.storage.backend.enable_journal();

        output
    }

    #[allow(dead_code)]
    fn visualize_node<'a>(
        output: &'a mut String,
        node: &'a crate::Node< Vec<u8>, Vec<u8>, dialog_storage::Blake3Hash>,
        storage: &'a dialog_storage::Storage<
            dialog_storage::CborEncoder,
            dialog_storage::JournaledStorage<
                dialog_storage::MemoryStorageBackend<[u8; 32], Vec<u8>>,
            >,
        >,
        prefix: &'a str,
        is_last: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
        Box::pin(async move {
            let branch = if is_last { "└── " } else { "├── " };
            let boundary = node.upper_bound();
            let key_str = String::from_utf8_lossy(boundary).to_string();

            // Extract rank from encoded boundary
            let rank = if boundary.len() >= 2 && boundary[boundary.len() - 2] == 0x00 {
                boundary[boundary.len() - 1]
            } else {
                1
            };

            let hash = node.hash();
            let hash_str = format!(
                "{:02x}{:02x}{:02x}{:02x}",
                hash[0], hash[1], hash[2], hash[3]
            );

            if prefix.is_empty() {
                output.push_str(&format!("{} [{}]@{}\n", key_str, rank, hash_str));
            } else {
                output.push_str(&format!(
                    "{}{}{} [{}]@{}\n",
                    prefix, branch, key_str, rank, hash_str
                ));
            }

            if node.is_branch() {
                // Load children and recurse
                if let Ok(children) = node.load_children(storage).await {
                    let new_prefix = format!("{}{}", prefix, if is_last { "    " } else { "│   " });
                    let child_count = children.len();
                    for (i, child) in children.iter().enumerate() {
                        let is_last_child = i == child_count - 1;
                        Self::visualize_node(output, child, storage, &new_prefix, is_last_child)
                            .await;
                    }
                }
            }
        })
    }

    /// Assert that the expected read pattern matches the actual reads.
    ///
    /// Panics with a detailed diff if the pattern doesn't match.
    #[track_caller]
    pub fn assert(&self) {
        let reads = self.storage.backend.get_reads();

        // Build a set of hashes that were read
        let reads_set: HashSet<[u8; 32]> = reads.iter().copied().collect();

        // Build expected/actual based on NodeSpecs
        // Use (boundary, height) tuples as keys
        let mut expected_reads = HashSet::new();
        let mut unexpected_reads = HashSet::new();
        let mut actual_reads = HashSet::new();

        for level in &self.spec {
            for node in level {
                let hash = node.hash;
                let key = (node.boundary.clone(), node.height);

                match node.expect {
                    Expect::Read => {
                        expected_reads.insert(key.clone());
                    }
                    Expect::Skip => {
                        unexpected_reads.insert(key.clone());
                    }
                }

                if reads_set.contains(&hash) {
                    actual_reads.insert(key);
                }
            }
        }

        // Compare expected vs actual
        let missing_reads: Vec<_> = expected_reads.difference(&actual_reads).collect();
        let wrongly_read: Vec<_> = actual_reads.intersection(&unexpected_reads).collect();
        let unexpected_reads: Vec<_> = actual_reads
            .difference(&expected_reads)
            .filter(|n| !unexpected_reads.contains(n))
            .collect();

        // If everything matches, return early
        if missing_reads.is_empty() && wrongly_read.is_empty() && unexpected_reads.is_empty() {
            return;
        }

        // Build the comparison output as a string for the panic message
        let mut output = String::from("\n=== Read Pattern Mismatch ===\n");

        // ANSI color codes
        const GREEN: &str = "\x1b[32m";
        const RED: &str = "\x1b[31m";
        const RESET: &str = "\x1b[0m";

        // Calculate column widths for alignment
        let max_boundaries = self.spec.iter().map(|l| l.len()).max().unwrap_or(0);
        let mut column_widths = vec![0; max_boundaries];

        for level in &self.spec {
            for (i, node) in level.iter().enumerate() {
                let boundary_str = String::from_utf8_lossy(&node.boundary);
                let width = boundary_str.len() + 4; // "(..x)"
                column_widths[i] = column_widths[i].max(width);
            }
        }

        // Show expected pattern
        output.push_str("\nExpected:\n");
        for level in &self.spec {
            output.push_str("  [");
            for (i, node) in level.iter().enumerate() {
                let boundary_str = String::from_utf8_lossy(&node.boundary);
                let content = match node.expect {
                    Expect::Skip => format!("(..{})", boundary_str),
                    Expect::Read => format!("..{}", boundary_str),
                };
                if i > 0 {
                    output.push_str(", ");
                }
                output.push_str(&format!("{:width$}", content, width = column_widths[i]));
            }
            output.push_str("]\n");
        }

        // Show actual reads with color highlighting
        output.push_str("\nActual:\n");
        for level in &self.spec {
            output.push_str("  [");
            for (i, node) in level.iter().enumerate() {
                let key = (node.boundary.clone(), node.height);
                let boundary_str = String::from_utf8_lossy(&node.boundary);
                let was_read = actual_reads.contains(&key);

                let (content, color_len) = match node.expect {
                    Expect::Skip => {
                        if was_read {
                            (format!("{}(..{}){}", RED, boundary_str, RESET), 9)
                        } else {
                            (format!("(..{})", boundary_str), 0)
                        }
                    }
                    Expect::Read => {
                        if was_read {
                            (format!("{}..{}{}", GREEN, boundary_str, RESET), 9)
                        } else {
                            (format!("{}..{}{}", RED, boundary_str, RESET), 9)
                        }
                    }
                };

                if i > 0 {
                    output.push_str(", ");
                }
                output.push_str(&format!(
                    "{:width$}",
                    content,
                    width = column_widths[i] + color_len
                ));
            }
            output.push_str("]\n");
        }

        // Add detailed errors
        if !missing_reads.is_empty() {
            output.push_str("\n❌ Missing expected reads:\n");
            for node_ref in &missing_reads {
                let boundary = String::from_utf8_lossy(&node_ref.0);
                output.push_str(&format!("  - {} @ height {}\n", boundary, node_ref.1));
            }
        }

        if !wrongly_read.is_empty() {
            output.push_str("\n❌ Expected skips were read:\n");
            for node_ref in &wrongly_read {
                let boundary = String::from_utf8_lossy(&node_ref.0);
                output.push_str(&format!("  - {} @ height {}\n", boundary, node_ref.1));
            }
        }

        if !unexpected_reads.is_empty() {
            output.push_str("\n⚠️  Unexpected reads:\n");
            for node_ref in &unexpected_reads {
                let boundary = String::from_utf8_lossy(&node_ref.0);
                output.push_str(&format!("  - {} @ height {}\n", boundary, node_ref.1));
            }
        }

        panic!("{}", output);
    }
}

impl std::fmt::Debug for TreeSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(root) = self.tree.root() {
            Self::fmt_node(f, root, "", true)
        } else {
            write!(f, "(empty tree)")
        }
    }
}

impl TreeSpec {
    fn fmt_node(
        f: &mut std::fmt::Formatter<'_>,
        node: &crate::Node< Vec<u8>, Vec<u8>, dialog_storage::Blake3Hash>,
        prefix: &str,
        is_last: bool,
    ) -> std::fmt::Result {
        let branch = if is_last { "└── " } else { "├── " };
        let boundary = node.upper_bound();
        let key_str = String::from_utf8_lossy(boundary).to_string();

        // Extract rank from encoded boundary
        let rank = if boundary.len() >= 2 && boundary[boundary.len() - 2] == 0x00 {
            boundary[boundary.len() - 1]
        } else {
            1
        };

        let hash = node.hash();
        let hash_str = format!(
            "{:02x}{:02x}{:02x}{:02x}",
            hash[0], hash[1], hash[2], hash[3]
        );

        if prefix.is_empty() {
            writeln!(f, "{} [{}]@{}", key_str, rank, hash_str)?;
        } else {
            writeln!(f, "{}{}{} [{}]@{}", prefix, branch, key_str, rank, hash_str)?;
        }

        if node.is_branch() {
            if let Ok(refs) = node.references() {
                let new_prefix = format!("{}{}", prefix, if is_last { "    " } else { "│   " });
                let ref_count = refs.len();
                for (i, reference) in refs.iter().enumerate() {
                    let is_last_child = i == ref_count - 1;
                    let child_branch = if is_last_child {
                        "└── "
                    } else {
                        "├── "
                    };

                    let ref_boundary = reference.upper_bound();
                    let ref_key_str = String::from_utf8_lossy(ref_boundary).to_string();

                    let ref_rank = if ref_boundary.len() >= 2
                        && ref_boundary[ref_boundary.len() - 2] == 0x00
                    {
                        ref_boundary[ref_boundary.len() - 1]
                    } else {
                        1
                    };

                    let ref_hash = reference.hash();
                    let ref_hash_str = format!(
                        "{:02x}{:02x}{:02x}{:02x}",
                        ref_hash[0], ref_hash[1], ref_hash[2], ref_hash[3]
                    );

                    writeln!(
                        f,
                        "{}{}{} [{}]@{} (ref)",
                        new_prefix, child_branch, ref_key_str, ref_rank, ref_hash_str
                    )?;
                }
            }
        }

        Ok(())
    }
}
