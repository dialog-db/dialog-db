# Dialog-DB Tree Browser Implementation Guide

## Overview
Build a TUI (Terminal User Interface) application using Rust and ratatui to visualize dialog-db's prolly tree structure. The application will have two modes:
1. **Tree Mode**: Shows the internal prolly tree structure (like Okra's CLI)
2. **Facts Mode**: Shows the EAV (Entity-Attribute-Value) facts stored in the tree

## Project Setup

### Dependencies
```toml
[package]
name = "dialog-tree-browser"
version = "0.1.0"
edition = "2021"

[dependencies]
# Core dialog-db dependencies
dialog-artifacts = { path = "../dialog-db/rust/dialog-artifacts" }
dialog-prolly-tree = { path = "../dialog-db/rust/dialog-prolly-tree" }
dialog-storage = { path = "../dialog-db/rust/dialog-storage" }

# TUI dependencies
ratatui = "0.28"
crossterm = "0.28"

# Utilities
csv = "1.3"
anyhow = "1.0"
tokio = { version = "1", features = ["full"] }
clap = { version = "4", features = ["derive"] }
base58 = "0.2"
```

## Architecture

### Core Components

```rust
// src/main.rs
use clap::Parser;

#[derive(Parser)]
struct Args {
    /// CSV file containing facts to load
    #[arg(short, long)]
    csv: String,

    /// Start in tree mode (default) or facts mode
    #[arg(short, long, default_value = "tree")]
    mode: String,
}

// src/app.rs
pub struct DialogBrowser {
    // Core data
    artifacts: Artifacts<MemoryStorageBackend>,
    facts: Vec<Artifact>,

    // UI state
    mode: ViewMode,
    tree_view: TreeView,
    facts_view: FactsView,

    // Terminal
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

pub enum ViewMode {
    Tree,
    Facts,
}
```

### CSV Loading

```rust
// src/loader.rs
use csv::Reader;
use dialog_artifacts::{Artifact, Attribute, Entity, Value};

#[derive(Debug, serde::Deserialize)]
struct CsvRow {
    the: String,      // attribute
    of: String,       // entity (base58)
    is: String,       // value
    cause: String,    // causal reference (optional)
}

pub async fn load_facts_from_csv(path: &str) -> Result<Vec<Artifact>> {
    let mut reader = Reader::from_path(path)?;
    let mut facts = Vec::new();

    for result in reader.deserialize() {
        let row: CsvRow = result?;

        // Parse entity from base58
        let entity = Entity::from_str(&row.of)?;

        // Parse attribute (namespace/predicate format)
        let attribute = Attribute::from_str(&row.the)?;

        // Parse value - for now assume all are strings
        // In real implementation, detect type from content
        let value = Value::String(row.is);

        // Handle optional cause
        let cause = if row.cause.is_empty() {
            None
        } else {
            Some(Cause::from_str(&row.cause)?)
        };

        facts.push(Artifact {
            the: attribute,
            of: entity,
            is: value,
            cause,
        });
    }

    Ok(facts)
}
```

## Tree View Implementation

```rust
// src/views/tree.rs
pub struct TreeView {
    // View state
    scroll_offset: usize,
    selected_level: u32,
    selected_node: Option<NodeRef>,
    expanded_nodes: HashSet<NodeRef>,

    // Cached tree structure
    tree_lines: Vec<TreeLine>,
}

#[derive(Clone)]
struct TreeLine {
    level: u32,
    hash: [u8; 32],
    key: Option<Vec<u8>>,
    is_boundary: bool,
    parent_hash: Option<[u8; 32]>,
    connectors: Vec<Connector>,
}

#[derive(Clone)]
enum Connector {
    Horizontal,  // ─
    Vertical,    // │
    Corner,      // └
    Branch,      // ├
}

impl TreeView {
    pub async fn build_from_artifacts(artifacts: &Artifacts) -> Result<Self> {
        // Get the three index roots
        let entity_root = artifacts.entity_index_root();
        let attribute_root = artifacts.attribute_index_root();
        let value_root = artifacts.value_index_root();

        // For now, visualize entity index
        let tree_lines = Self::traverse_tree(entity_root).await?;

        Ok(TreeView {
            scroll_offset: 0,
            selected_level: 0,
            selected_node: None,
            expanded_nodes: HashSet::new(),
            tree_lines,
        })
    }

    async fn traverse_tree(root: &Node) -> Result<Vec<TreeLine>> {
        let mut lines = Vec::new();
        let mut queue = VecDeque::new();

        // BFS traversal to build tree structure
        queue.push_back((root, 0, true));

        while let Some((node, level, is_boundary)) = queue.pop_front() {
            let line = TreeLine {
                level,
                hash: node.hash().clone(),
                key: node.key().map(|k| k.to_vec()),
                is_boundary,
                parent_hash: node.parent_hash(),
                connectors: Vec::new(),
            };

            lines.push(line);

            // Add children to queue
            if let Ok(children) = node.children() {
                for (i, child) in children.iter().enumerate() {
                    let child_is_boundary = child.is_boundary();
                    queue.push_back((child, level + 1, child_is_boundary));
                }
            }
        }

        // Calculate connectors
        Self::calculate_connectors(&mut lines);

        Ok(lines)
    }

    pub fn render(&self, area: Rect, buf: &mut Buffer) {
        // Header
        let header = "level 4    level 3    level 2    level 1    level 0    key";
        buf.set_string(area.x, area.y, header, Style::default().bold());

        // Tree lines
        let visible_lines = self.tree_lines
            .iter()
            .skip(self.scroll_offset)
            .take(area.height as usize - 1);

        for (i, line) in visible_lines.enumerate() {
            let y = area.y + i as u16 + 1;

            // Render connectors and hash
            let x_offset = (4 - line.level) * 11; // spacing between levels

            // Draw connectors
            for connector in &line.connectors {
                let conn_char = match connector {
                    Connector::Horizontal => "─",
                    Connector::Vertical => "│",
                    Connector::Corner => "└",
                    Connector::Branch => "├",
                };
                buf.set_string(area.x + x_offset, y, conn_char, Style::default());
            }

            // Draw hash (truncated to 8 chars)
            let hash_str = format!("{:8}", base58::encode(&line.hash[..4]));
            buf.set_string(
                area.x + x_offset + 2,
                y,
                &hash_str,
                Style::default().fg(Color::Cyan)
            );

            // Draw key if it's a leaf
            if let Some(key) = &line.key {
                let key_str = format!("{:04x}", u32::from_be_bytes(key[..4].try_into().unwrap()));
                buf.set_string(
                    area.x + 55,
                    y,
                    &key_str,
                    Style::default().fg(Color::Green)
                );
            }
        }
    }
}
```

## Facts View Implementation

```rust
// src/views/facts.rs
pub struct FactsView {
    facts: Vec<Artifact>,
    filtered_facts: Vec<usize>, // indices into facts

    // UI state
    selected_index: usize,
    scroll_offset: usize,
    sort_by: SortColumn,
    filter: String,
}

#[derive(Clone, Copy)]
enum SortColumn {
    Entity,
    Attribute,
    Value,
}

impl FactsView {
    pub fn new(facts: Vec<Artifact>) -> Self {
        let filtered_facts = (0..facts.len()).collect();

        FactsView {
            facts,
            filtered_facts,
            selected_index: 0,
            scroll_offset: 0,
            sort_by: SortColumn::Entity,
            filter: String::new(),
        }
    }

    pub fn render(&self, area: Rect, buf: &mut Buffer) {
        // Create table
        let header = ["Entity", "Attribute", "Value", "Cause"];
        let header_style = Style::default().bold().fg(Color::Yellow);

        // Header row
        let col_widths = [20, 30, 40, 10];
        let mut x = area.x;
        for (i, h) in header.iter().enumerate() {
            buf.set_string(x, area.y, h, header_style);
            x += col_widths[i];
        }

        // Data rows
        let visible_facts = self.filtered_facts
            .iter()
            .skip(self.scroll_offset)
            .take(area.height as usize - 2);

        for (row_idx, fact_idx) in visible_facts.enumerate() {
            let fact = &self.facts[*fact_idx];
            let y = area.y + row_idx as u16 + 2;
            let mut x = area.x;

            let style = if row_idx == self.selected_index - self.scroll_offset {
                Style::default().bg(Color::DarkGray)
            } else {
                Style::default()
            };

            // Entity (truncated)
            let entity_str = format!("{:18}..", &fact.of.to_base58()[..18]);
            buf.set_string(x, y, &entity_str, style);
            x += col_widths[0];

            // Attribute
            let attr_str = format!("{:28}", fact.the.to_string());
            buf.set_string(x, y, &attr_str, style);
            x += col_widths[1];

            // Value
            let value_str = match &fact.is {
                Value::String(s) => format!("{:38}", s),
                Value::UnsignedInt(n) => format!("{:38}", n),
                // Handle other value types
                _ => format!("{:38}", "..."),
            };
            buf.set_string(x, y, &value_str, style);
            x += col_widths[2];

            // Cause
            let cause_str = if fact.cause.is_some() { "✓" } else { "" };
            buf.set_string(x, y, cause_str, style);
        }

        // Status bar
        let status = format!(
            "Showing {}-{} of {} facts | Sort: {:?} | Filter: {}",
            self.scroll_offset + 1,
            (self.scroll_offset + area.height as usize - 2).min(self.filtered_facts.len()),
            self.filtered_facts.len(),
            self.sort_by,
            if self.filter.is_empty() { "none" } else { &self.filter }
        );
        buf.set_string(
            area.x,
            area.y + area.height - 1,
            &status,
            Style::default().dim()
        );
    }

    pub fn apply_filter(&mut self, filter: &str) {
        self.filter = filter.to_string();
        self.filtered_facts = self.facts
            .iter()
            .enumerate()
            .filter(|(_, fact)| {
                fact.the.to_string().contains(filter) ||
                fact.is.to_string().contains(filter)
            })
            .map(|(i, _)| i)
            .collect();
    }
}
```

## Main Application Loop

```rust
// src/app.rs
impl DialogBrowser {
    pub async fn run(&mut self) -> Result<()> {
        loop {
            // Draw UI
            self.terminal.draw(|f| {
                let area = f.area();

                // Header
                let header = format!("Dialog Tree Browser - Mode: {:?}", self.mode);
                f.render_widget(
                    Paragraph::new(header)
                        .style(Style::default().bold())
                        .block(Block::default().borders(Borders::BOTTOM)),
                    Rect { x: area.x, y: area.y, width: area.width, height: 3 }
                );

                // Main content area
                let content_area = Rect {
                    x: area.x,
                    y: area.y + 3,
                    width: area.width,
                    height: area.height - 4,
                };

                match self.mode {
                    ViewMode::Tree => self.tree_view.render(content_area, f.buffer_mut()),
                    ViewMode::Facts => self.facts_view.render(content_area, f.buffer_mut()),
                }

                // Help line
                let help = "q: quit | tab: switch mode | arrows: navigate | /: filter";
                f.render_widget(
                    Paragraph::new(help)
                        .style(Style::default().dim())
                        .alignment(Alignment::Center),
                    Rect {
                        x: area.x,
                        y: area.y + area.height - 1,
                        width: area.width,
                        height: 1
                    }
                );
            })?;

            // Handle input
            if event::poll(Duration::from_millis(100))? {
                if let Event::Key(key) = event::read()? {
                    match key.code {
                        KeyCode::Char('q') => break,
                        KeyCode::Tab => self.toggle_mode(),
                        KeyCode::Up => self.handle_up(),
                        KeyCode::Down => self.handle_down(),
                        KeyCode::Left => self.handle_left(),
                        KeyCode::Right => self.handle_right(),
                        KeyCode::Char('/') => self.start_filter(),
                        _ => {}
                    }
                }
            }
        }

        Ok(())
    }
}
```

## Key Implementation Notes

1. **Tree Structure**: Dialog uses three separate prolly trees for Entity, Attribute, and Value indexes. Start by visualizing just the Entity index.

2. **Node Types**:
   - Branch nodes contain references to child nodes
   - Segment nodes contain actual key-value entries

3. **Boundary Detection**: Nodes with hash < (2^32 / BRANCH_FACTOR) are boundary nodes that create new parents

4. **CSV Type Detection**: Enhance the CSV loader to detect value types:
   - Numbers: parse as UnsignedInt
   - UUIDs/entities: parse as Entity references
   - Everything else: String

5. **Performance**: Cache tree traversal results to avoid repeated async storage reads

6. **Future Enhancements**:
   - Direct dialog-db instance connection
   - Diff visualization between revisions
   - Export to GraphViz DOT format
   - Search by hash/key prefix

This implementation provides a solid foundation for exploring dialog-db's internal structure while keeping the initial scope manageable with CSV loading.
