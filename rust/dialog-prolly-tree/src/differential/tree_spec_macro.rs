/// # Tree Specification Macro
///
/// The `tree_spec!` macro allows you to visually define the exact structure of a prolly tree
/// for testing purposes. Instead of relying on unpredictable rank distributions, you specify
/// exactly which keys should be boundaries at which heights.
///
/// ## Syntax
///
/// ```ignore
/// let spec = tree_spec![
///     [..l]                          // Height 1 (index nodes)
///     [..a, c..e, f..f, g..g, h..l]  // Height 0 (segment nodes/leaves)
/// ];
///
/// let tree = spec.build_tree(storage).await?;
/// ```
///
/// ### Range Syntax Rules
///
/// 1. **Brackets `[...]`**: Each line represents one height level
///    - Top line = highest height (root/index nodes)
///    - Bottom line = height 0 (segment nodes/leaves)
///
/// 2. **Commas `,`**: Separate sibling segments within a level
///    - `[..a, c..e, ..l]` = three segments with upper bounds a, e, l
///
/// 3. **Range operators**: Define segment boundaries using Rust's range syntax
///    - `..x` = segment ending at 'x' (first key inferred from previous or starts at 'a')
///    - `a..b` = segment explicitly from 'a' to 'b' (inclusive)
///    - Multi-char keys supported: `..aa`, `ab..az` (Excel-style naming: a-z, aa-az, etc.)
///
/// 4. **Key inference**:
///    - `..a` after nothing = starts at 'a', ends at 'a' (contains only 'a')
///    - `..d` after `..a` = starts at 'b' (next after 'a'), ends at 'd' (contains b, c, d)
///    - `c..e` = explicitly starts at 'c', ends at 'e' (contains c, d, e)
///    - `f..f` = starts and ends at 'f' (contains only 'f')
///
/// ## Key Inference
///
/// The macro infers which keys exist in the tree based on segment specifications:
///
/// ```ignore
/// [..a, c..e, ..f, ..g, ..l]
/// ```
///
/// This creates:
/// - Segment `..a`: contains key 'a' only
/// - Segment `c..e`: contains keys c, d, e (first key explicit)
/// - Segment `..f`: contains key 'f' only (next after 'e')
/// - Segment `..g`: contains key 'g' only (next after 'f')
/// - Segment `..l`: contains keys h, i, j, k, l (starts after 'g')
/// - Note: 'b' is NOT in the tree (gap between 'a' and 'c')
///
/// ## Structure Validation
///
/// The macro validates that:
/// 1. Every boundary in a parent level has a corresponding child
/// 2. Boundaries are in strictly ascending order
/// 3. Child boundaries don't exceed parent boundaries
///
/// ## Boundary Checking
///
/// Check if boundaries exist at specific heights:
///
/// ```ignore
/// let spec = tree_spec![
///     [..d, ..g]
///     [..a, ..d, ..f, ..g]
/// ];
///
/// assert!(spec.has_boundary("d", 1));  // Index node at height 1
/// assert!(spec.has_boundary("a", 0));  // Segment at height 0
/// assert!(!spec.has_boundary("a", 1)); // 'a' doesn't exist at height 1
/// ```
///
/// ## Example: Overlapping Trees
///
/// ```ignore
/// let spec_a = tree_spec![
///     [..l]
///     [..a, ..d, ..e, ..f, ..g, ..l]
/// ];
///
/// let spec_b = tree_spec![
///     [..s]
///     [f..f, g..g, h..s]
/// ];
///
/// // Build trees
/// let tree_a = spec_a.build_tree(storage.clone()).await?;
/// let tree_b = spec_b.build_tree(storage.clone()).await?;
///
/// // Test differential
/// let diff = tree_b.differentiate(&tree_a, storage.clone()).await;
/// ```
///
/// In this example:
/// - Tree A has keys: a, b, c, d, e, f, g, h, i, j, k, l
/// - Tree B has keys: f, g, h, i, j, k, l, m, n, o, p, q, r, s
/// - Trees overlap in keys f-l, differ in a-e (only in A) and m-s (only in B)
///
/// ## How It Works
///
/// 1. **Parse**: Extract boundaries from each height level
/// 2. **Infer keys**: Fill in all keys between boundaries (a, b, c, ...)
/// 3. **Assign ranks**: Boundaries at height H get rank (H+1)
/// 4. **Build tree**: Use `Tree::from_collection` with a custom `DistributionSimulator`
/// 5. **Return wrapper**: Provides access methods for testing
///
/// ## Benefits
///
/// - **Deterministic**: Same spec always produces same tree structure
/// - **Visual**: Easy to see the tree shape at a glance
/// - **Testable**: Reference specific nodes in test assertions
/// - **Clear**: Documentation and test spec are the same thing

/// Parse a segment specification using range syntax
/// `..a` = segment ending at 'a' (first key inferred)
/// `c..e` = segment from 'c' to 'e' (explicit first key)
#[doc(hidden)]
#[macro_export]
macro_rules! parse_segment {
    // Pattern: first..last (e.g., c..e)
    ($first:ident .. $last:ident) => {{
        (Some(stringify!($first)), stringify!($last))
    }};
    // Pattern: ..last (e.g., ..a)
    (.. $last:ident) => {{
        (None, stringify!($last))
    }};
}

/// The `tree_spec!` macro - see module documentation for usage
#[macro_export]
macro_rules! tree_spec {
    // Match the bracket-based tree format with range syntax
    // Segments can be: ..x (inferred start) or a..b (explicit range)
    // Parentheses indicate pruned nodes: (..x) or (a..b)
    (
        $(
            [$($( .. $end:ident)? $($first:ident .. $last:ident)? $( ( .. $pend:ident ) )? $( ( $pfirst:ident .. $plast:ident ) )?),+ $(,)?]
        )+
    ) => {{
        use $crate::differential::tree_spec::*;

        // Parse each level - construct NodeDescriptor enums directly
        let mut levels: Vec<Vec<NodeDescriptor>> = Vec::new();

        $(
            let level = vec![
                $(
                    {
                        // Match: ..end, first..last, (..pend), or (pfirst..plast)
                        // Construct the appropriate NodeDescriptor variant
                        let descriptor: NodeDescriptor = {
                            // Normal (non-pruned) segments
                            $(
                                NodeDescriptor::OpenRange(stringify!($end).to_string())
                            )?
                            $(
                                NodeDescriptor::Range(stringify!($first).to_string(), stringify!($last).to_string())
                            )?
                            // Pruned segments
                            $(
                                NodeDescriptor::SkipOpenRange(stringify!($pend).to_string())
                            )?
                            $(
                                NodeDescriptor::SkipRange(stringify!($pfirst).to_string(), stringify!($plast).to_string())
                            )?
                        };
                        descriptor
                    }
                ),+
            ];
            levels.push(level);
        )+

        // Construct TreeDescriptor directly
        TreeDescriptor(levels)
    }};
}
