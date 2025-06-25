# Dialog-DB Diagnose Tool Implementation

## Overview

The `dialog-diagnose` tool is a Terminal User Interface (TUI) application built with Rust and ratatui that provides interactive visualization and exploration of dialog-db's prolly tree data structures. It allows users to load CSV data into a real prolly tree and explore both the raw facts and the internal tree organization.

## Architecture

### Core Components

```
dialog-diagnose/
├── src/bin/diagnose.rs    # Main application
├── Cargo.toml            # Dependencies 
├── pokemon_artifacts.csv # Sample data
└── IMPLEMENTATION.md     # This document
```

### Key Dependencies

- **ratatui**: Terminal UI framework for interactive display
- **dialog-artifacts**: Core dialog-db library with prolly tree implementation
- **dialog-storage**: Storage backend (using MemoryStorageBackend)
- **dialog-prolly-tree**: Prolly tree data structures
- **csv**: CSV parsing for data ingestion
- **tokio**: Async runtime
- **hex**: Hash display formatting

## Features Implemented

### 1. Dual-Tab Interface

**Facts Tab**: Interactive table view of loaded CSV data
- Scrollable list of all facts (Entity/Attribute/Value/Cause)
- Real-time filtering with `/` key
- Shows Pokemon data: names, stats, types, etc.
- Status bar with scroll position and filter info

**Tree Tab**: Interactive prolly tree visualization
- Real-time navigation through prolly tree structure
- Expandable/collapsible branch nodes
- Segment node data inspection
- Multi-line formatted display of actual tree entries

### 2. CSV Data Loading

```rust
async fn load_facts_from_csv(file_path: &str) -> Result<Vec<CsvFact>>
```

- Reads Pokemon CSV data (the/of/is/cause format)
- Parses entities as base58 strings (prefixed with "entity:")
- Detects value types (u128 for numbers, String for text)
- Creates proper Artifact structs for dialog-db

### 3. Real Prolly Tree Integration

```rust
async fn create_artifacts_from_csv(file_path: &str) -> Result<Artifacts<ArtifactsBackend>>
```

- Creates actual Artifacts instance with MemoryStorageBackend
- Loads CSV facts into real prolly tree structure
- Uses debug feature to access internal tree indexes
- Builds genuine prolly tree from Pokemon data

### 4. Interactive Tree Navigation

**Tree Structure Visualization**:
- Shows real prolly tree hierarchy with proper indentation
- Branch nodes display child count: `├─ abc123 branch (3 children)`
- Segment nodes show entry count: `├─ def456 segment (5 entries)`
- Visual expansion indicators: `├+` (collapsed) vs `├─` (expanded)

**Navigation Features**:
- `↑↓` arrow keys for node navigation
- `Enter` key for expand/collapse of branch nodes
- Automatic visibility filtering (children hidden when parent collapsed)
- Selection highlighting with dark gray background

**Real Data Extraction**:
```rust
async fn extract_segment_data(node: &Node, storage: &Storage) -> Vec<String>
```

- Extracts actual entries from prolly tree segment nodes
- Shows real EntityKey and State<Datum> values
- Multi-line formatting for readability
- Displays first 3 entries per segment automatically

### 5. Multi-Line Data Display

**Formatted Output**:
```
├─ ef567890 segment (3 entries - Select to view)  ← Selected
    Entry 1:
      Key: EntityKey(...)
      Val: State(Datum(...))
    
    Entry 2:  
      Key: EntityKey(...)
      Val: State(Datum(...))
```

- Clean separation between entries
- Proper indentation and spacing
- Readable format for long keys and complex values
- Automatic display when segment is selected

## Technical Implementation Details

### Data Structures

```rust
#[derive(Debug, Clone)]
pub struct CsvFact {
    pub attribute: String,  // "the" column
    pub entity: String,     // "of" column  
    pub value: String,      // "is" column
    pub cause: String,      // "cause" column
}

#[derive(Debug, Clone)]
pub struct TreeNode {
    pub hash: String,           // Node hash (hex encoded)
    pub level: u32,             // Tree depth level
    pub key: Option<String>,    // Display key/description
    pub is_expanded: bool,      // Expansion state for branches
    pub is_branch: bool,        // Branch vs segment node
    pub children: Vec<TreeNode>, // Child nodes (unused - flat structure)
    pub data: Option<String>,   // Display data preview
    pub all_entries: Vec<String>, // Full entry data for segments
}
```

### Key Algorithms

**Tree Traversal**:
```rust
async fn traverse_node_recursive(
    node: &Node, 
    storage: &Storage, 
    level: u32, 
    tree_nodes: &mut Vec<TreeNode>
) -> Result<()>
```

- Recursive depth-first traversal of prolly tree
- Extracts real data from segment nodes during traversal
- Builds flat list with level information for rendering
- Handles both branch and segment node types

**Visibility Filtering**:
```rust
fn get_visible_tree_nodes(&self) -> Vec<(usize, &TreeNode)>
```

- Filters tree nodes based on expansion state
- Skips children of collapsed branch nodes
- Maintains original indices for selection tracking
- Enables dynamic tree display based on user interaction

**Smart Navigation**:
```rust
fn move_tree_selection_up(&mut self)
fn move_tree_selection_down(&mut self)
```

- Navigation respects current visibility state
- Moves between only visible nodes
- Maintains proper selection tracking across expand/collapse operations

### UI Layout

```
┌─────────────────────────────────────────────────┐
│ Dialog Dev Tools                    Facts | Tree │ ← Header with tabs
├─────────────────────────────────────────────────┤
│                                                 │
│              Main Content Area                  │ ← Facts table OR Tree view
│            (Facts or Tree tab)                  │
│                                                 │
├─────────────────────────────────────────────────┤
│ ◄ ► change tab | ↑↓ navigate | Enter expand... │ ← Footer help
└─────────────────────────────────────────────────┘
```

## Controls Reference

### Global Controls
- `◄►` - Switch between Facts and Tree tabs
- `q` - Quit application

### Facts Tab Controls  
- `↑↓` - Scroll through facts
- `/` - Enter filter mode
- `Enter` - Apply filter (in filter mode)
- `Esc` - Cancel filter (in filter mode)
- `Backspace` - Delete filter characters

### Tree Tab Controls
- `↑↓` - Navigate through visible tree nodes
- `Enter` - Expand/collapse branch nodes
- Selection automatically shows segment data

## File Structure

### CSV Input Format
```csv
the,of,is,cause
pokemon/name,1br79uXm91vazzwv34GWpKLNFxzsesKHmVUGM4TVhNb,grimmsnarl,
pokemon/stat/attack,1br79uXm91vazzwv34GWpKLNFxzsesKHmVUGM4TVhNb,120,
pokemon/stat/defense,1br79uXm91vazzwv34GWpKLNFxzsesKHmVUGM4TVhNb,65,
```

- `the`: Attribute (e.g., "pokemon/name", "pokemon/stat/attack")
- `of`: Entity ID in base58 format
- `is`: Value (string or number)
- `cause`: Causal reference (empty in our data)

## Running the Application

```bash
# Build the application
cargo build

# Run with Pokemon data
cargo run --bin diagnose

# The application automatically loads pokemon_artifacts.csv
```

## Future Enhancements

### Potential Improvements
1. **Command-line arguments** for CSV file selection
2. **Real entity/attribute parsing** to show meaningful Pokemon names
3. **Export functionality** for tree visualization
4. **Multiple index support** (currently shows entity index only)
5. **Search functionality** within tree structure
6. **Tree statistics** (depth, node counts, etc.)
7. **Diff visualization** between tree versions
8. **GraphViz export** for external tree visualization

### API Extensions
1. **Direct dialog-db connection** instead of CSV-only
2. **Live data updates** with tree refresh
3. **Multiple storage backends** beyond memory
4. **Tree modification** capabilities
5. **Bulk data operations** with progress tracking

## Technical Notes

### Entity ID Handling
Entities in the CSV are base58 strings that must be prefixed with "entity:" for proper URI parsing:
```rust
let entity = Entity::from_str(&format!("entity:{}", csv_fact.entity))?;
```

### Async Considerations
The tree traversal is async due to storage operations, requiring `Box::pin()` for recursive calls:
```rust
Box::pin(Self::traverse_node_recursive(child, storage, level + 1, tree_nodes)).await?;
```

### Memory Usage
Currently loads entire CSV and builds complete tree in memory. For large datasets, consider:
- Streaming CSV processing
- Lazy tree node loading
- Memory usage monitoring

## Conclusion

The dialog-diagnose tool successfully demonstrates dialog-db's prolly tree structure through an interactive TUI. It bridges the gap between raw CSV data and internal tree organization, providing valuable insights into how dialog-db stores and organizes information. The tool serves both as a debugging aid and an educational resource for understanding prolly tree behavior.