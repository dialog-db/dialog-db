use anyhow::Result;
use dialog_artifacts::{Artifacts, Artifact, Attribute, Entity, Value, Instruction, ArtifactStoreMut};
use dialog_storage::{MemoryStorageBackend, Blake3Hash};
use futures_util::stream;
use ratatui::{
    DefaultTerminal,
    buffer::Buffer,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Text},
    widgets::{Cell, HighlightSpacing, Row, Table, Tabs, Widget},
};
use std::str::FromStr;

// Type alias for the storage backend used by Artifacts
type ArtifactsBackend = MemoryStorageBackend<Blake3Hash, Vec<u8>>;

#[derive(Debug, Clone)]
pub struct CsvFact {
    pub attribute: String,  // "the" column
    pub entity: String,     // "of" column
    pub value: String,      // "is" column
    pub cause: String,      // "cause" column
}

async fn load_facts_from_csv(file_path: &str) -> Result<Vec<CsvFact>> {
    let mut facts = Vec::new();
    let content = tokio::fs::read_to_string(file_path).await?;
    let mut reader = csv::Reader::from_reader(content.as_bytes());
    
    for result in reader.records() {
        let record = result?;
        if record.len() >= 4 {
            facts.push(CsvFact {
                attribute: record[0].to_string(),
                entity: record[1].to_string(),
                value: record[2].to_string(),
                cause: record[3].to_string(),
            });
        }
    }
    
    Ok(facts)
}

async fn create_artifacts_from_csv(file_path: &str) -> Result<Artifacts<ArtifactsBackend>> {
    // Create Artifacts instance with memory backend
    let storage_backend = ArtifactsBackend::default();
    let mut artifacts = Artifacts::anonymous(storage_backend).await?;
    
    // Load CSV data
    let csv_facts = load_facts_from_csv(file_path).await?;
    
    // Convert CSV facts to Artifacts and load them
    let mut instructions = Vec::new();
    
    for csv_fact in csv_facts {
        // Parse entity from base58 - needs to be prefixed with "entity:"
        let entity = Entity::from_str(&format!("entity:{}", csv_fact.entity))?;
        
        // Parse attribute
        let attribute = Attribute::from_str(&csv_fact.attribute)?;
        
        // Parse value - detect type
        let value = if let Ok(num) = csv_fact.value.parse::<u128>() {
            Value::UnsignedInt(num)
        } else {
            Value::String(csv_fact.value)
        };
        
        // Create artifact
        let artifact = Artifact {
            the: attribute,
            of: entity,
            is: value,
            cause: None, // No causes in our CSV
        };
        
        instructions.push(Instruction::Assert(artifact));
    }
    
    // Commit all instructions
    let instruction_stream = stream::iter(instructions);
    artifacts.commit(instruction_stream).await?;
    
    Ok(artifacts)
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let diagnose = Diagnose::new().await?;
    let mut terminal = ratatui::init();
    terminal.clear()?;
    diagnose.run(terminal)?;
    ratatui::restore();
    Ok(())
}

pub struct Diagnose {
    exit: bool,
    selected_tab: usize,
    facts: Vec<CsvFact>,
    filtered_facts: Vec<usize>, // indices into facts
    scroll_offset: usize,
    filter: String,
    filter_mode: bool,
    tree_nodes: Vec<TreeNode>,
    tree_scroll_offset: usize,
    tree_selected_index: usize,
    artifacts: Artifacts<ArtifactsBackend>,
}


impl Diagnose {
    pub async fn new() -> Result<Self> {
        let facts = load_facts_from_csv("pokemon_artifacts.csv").await?;
        let filtered_facts = (0..facts.len()).collect();
        
        // Create artifacts with real data
        let artifacts = create_artifacts_from_csv("pokemon_artifacts.csv").await?;
        
        // Extract tree nodes from the real prolly tree
        let tree_nodes = Self::extract_tree_nodes(&artifacts).await?;
        
        Ok(Self {
            exit: false,
            selected_tab: 0,
            facts,
            filtered_facts,
            scroll_offset: 0,
            filter: String::new(),
            filter_mode: false,
            tree_nodes,
            tree_scroll_offset: 0,
            tree_selected_index: 0,
            artifacts,
        })
    }

    async fn extract_tree_nodes(artifacts: &Artifacts<ArtifactsBackend>) -> Result<Vec<TreeNode>> {
        let mut tree_nodes = Vec::new();
        
        // Access the entity index (requires debug feature)
        let entity_index = artifacts.entity_index();
        let index = entity_index.read().await;
        
        if let Some(root) = index.root() {
            let storage = index.storage();
            Self::traverse_node_recursive(root, storage, 0, &mut tree_nodes).await?;
        } else {
            tree_nodes.push(TreeNode {
                hash: "empty".to_string(),
                level: 0,
                key: Some("Empty tree".to_string()),
                is_expanded: false,
                is_branch: false,
                children: vec![],
                data: None,
                all_entries: Vec::new(),
            });
        }
        
        Ok(tree_nodes)
    }

    async fn traverse_node_recursive(
        node: &dialog_prolly_tree::Node<254, 32, dialog_artifacts::EntityKey, dialog_artifacts::State<dialog_artifacts::Datum>, dialog_storage::Blake3Hash>,
        storage: &dialog_storage::Storage<32, dialog_storage::CborEncoder, ArtifactsBackend>,
        level: u32,
        tree_nodes: &mut Vec<TreeNode>,
    ) -> Result<()> {
        // Convert hash to hex string
        let hash_bytes = node.hash().as_ref();
        let hash_str = hex::encode(&hash_bytes[..4]); // First 4 bytes as hex
        
        // Determine node type and extract real data
        let (node_type, key_info) = if node.is_branch() {
            // For branch nodes, try to get some info about children
            let child_count = match node.load_children(storage).await {
                Ok(children) => children.len(),
                Err(_) => 0,
            };
            ("branch", Some(format!("branch ({} children)", child_count)))
        } else {
            // This is a segment (leaf) node - extract real data during traversal
            let data_preview = Self::extract_segment_data(node, storage).await;
            ("segment", Some(format!("segment ({} entries)", data_preview.len())))
        };
        
        let is_branch_node = node.is_branch();
        let segment_data = if !is_branch_node {
            Self::extract_segment_data(node, storage).await
        } else {
            Vec::new()
        };
        
        let tree_node = TreeNode {
            hash: hash_str,
            level,
            key: key_info.or_else(|| Some(format!("{} node", node_type))),
            is_expanded: false, // Start collapsed
            is_branch: is_branch_node,
            children: vec![],
            data: if !is_branch_node { 
                if segment_data.is_empty() {
                    Some("No entries in segment".to_string())
                } else {
                    Some(format!("{} entries - Select to view", segment_data.len() / 4)) // Divide by 4 since we add 4 lines per entry
                }
            } else { 
                None 
            },
            all_entries: segment_data,
        };
        
        tree_nodes.push(tree_node);
        
        // Recursively traverse children for branch nodes
        if node.is_branch() {
            match node.load_children(storage).await {
                Ok(children) => {
                    for child in children.iter() {
                        Box::pin(Self::traverse_node_recursive(child, storage, level + 1, tree_nodes)).await?;
                    }
                }
                Err(_) => {
                    // If we can't load children, just note it
                    tree_nodes.push(TreeNode {
                        hash: "error".to_string(),
                        level: level + 1,
                        key: Some("Failed to load children".to_string()),
                        is_expanded: false,
                        is_branch: false,
                        children: vec![],
                        data: None,
                        all_entries: Vec::new(),
                    });
                }
            }
        }
        
        Ok(())
    }

    async fn extract_segment_data(
        node: &dialog_prolly_tree::Node<254, 32, dialog_artifacts::EntityKey, dialog_artifacts::State<dialog_artifacts::Datum>, dialog_storage::Blake3Hash>,
        storage: &dialog_storage::Storage<32, dialog_storage::CborEncoder, ArtifactsBackend>,
    ) -> Vec<String> {
        let mut entries = Vec::new();
        
        if !node.is_branch() {
            // Try to extract REAL entries from the segment node
            // For segment nodes, we can try to clone and get entries
            let node_clone = node.clone();
            match node_clone.into_entries() {
                Ok(node_entries) => {
                    for (i, entry) in node_entries.iter().take(3).enumerate() { // Show first 3 entries
                        // Format each entry nicely across multiple lines
                        entries.push(format!("Entry {}:", i + 1));
                        entries.push(format!("  Key: {:?}", entry.key));
                        entries.push(format!("  Val: {:?}", entry.value));
                        entries.push("".to_string()); // Empty line separator
                    }
                }
                Err(_) => {
                    entries.push("Error: Could not extract entries from segment".to_string());
                }
            }
        }
        
        if entries.is_empty() {
            entries.push("No entries found in segment".to_string());
        }
        
        entries
    }

    fn toggle_tree_node_expansion(&mut self) {
        if self.tree_selected_index < self.tree_nodes.len() {
            let node = &mut self.tree_nodes[self.tree_selected_index];
            if node.is_branch {
                node.is_expanded = !node.is_expanded;
            }
        }
    }

    fn get_visible_tree_nodes(&self) -> Vec<(usize, &TreeNode)> {
        let mut visible = Vec::new();
        let mut skip_until_level = None;
        
        for (i, node) in self.tree_nodes.iter().enumerate() {
            // If we're skipping children of a collapsed node
            if let Some(skip_level) = skip_until_level {
                if node.level > skip_level {
                    continue; // Skip this child node
                } else {
                    skip_until_level = None; // Reset, we're back to same/higher level
                }
            }
            
            visible.push((i, node));
            
            // If this is a collapsed branch, skip its children
            if node.is_branch && !node.is_expanded {
                skip_until_level = Some(node.level);
            }
        }
        
        visible
    }

    fn move_tree_selection_up(&mut self) {
        let visible = self.get_visible_tree_nodes();
        if let Some(current_pos) = visible.iter().position(|(i, _)| *i == self.tree_selected_index) {
            if current_pos > 0 {
                self.tree_selected_index = visible[current_pos - 1].0;
            }
        }
    }

    fn move_tree_selection_down(&mut self) {
        let visible = self.get_visible_tree_nodes();
        if let Some(current_pos) = visible.iter().position(|(i, _)| *i == self.tree_selected_index) {
            if current_pos < visible.len() - 1 {
                self.tree_selected_index = visible[current_pos + 1].0;
            }
        }
    }


}

impl Diagnose {
    pub fn run(mut self, mut terminal: DefaultTerminal) -> Result<()> {
        loop {
            if self.exit {
                break;
            }

            terminal.draw(|frame| frame.render_widget(&self, frame.area()))?;
            self.handle_events()?;
        }

        Ok(())
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        if self.filter_mode {
            match key_event.code {
                KeyCode::Esc => {
                    self.filter_mode = false;
                }
                KeyCode::Enter => {
                    self.filter_mode = false;
                    self.apply_filter();
                }
                KeyCode::Backspace => {
                    self.filter.pop();
                }
                KeyCode::Char(c) => {
                    self.filter.push(c);
                }
                _ => (),
            }
        } else {
            match key_event.code {
                KeyCode::Char('q') => {
                    self.exit = true;
                }
                KeyCode::Left => {
                    if self.selected_tab > 0 {
                        self.selected_tab -= 1;
                    }
                }
                KeyCode::Right => {
                    if self.selected_tab < 1 {
                        self.selected_tab += 1;
                    }
                }
                KeyCode::Up => {
                    if self.selected_tab == 0 && self.scroll_offset > 0 {
                        self.scroll_offset -= 1;
                    } else if self.selected_tab == 1 {
                        self.move_tree_selection_up();
                    }
                }
                KeyCode::Down => {
                    if self.selected_tab == 0 && self.scroll_offset < self.filtered_facts.len().saturating_sub(1) {
                        self.scroll_offset += 1;
                    } else if self.selected_tab == 1 {
                        self.move_tree_selection_down();
                    }
                }
                KeyCode::Enter => {
                    if self.selected_tab == 1 {
                        self.toggle_tree_node_expansion();
                    }
                }
                KeyCode::Char('/') => {
                    if self.selected_tab == 0 {
                        self.filter_mode = true;
                        self.filter.clear();
                    }
                }
                _ => (),
            }
        }
    }

    fn apply_filter(&mut self) {
        if self.filter.is_empty() {
            self.filtered_facts = (0..self.facts.len()).collect();
        } else {
            self.filtered_facts = self.facts
                .iter()
                .enumerate()
                .filter(|(_, fact)| {
                    fact.attribute.to_lowercase().contains(&self.filter.to_lowercase()) ||
                    fact.value.to_lowercase().contains(&self.filter.to_lowercase()) ||
                    fact.entity.to_lowercase().contains(&self.filter.to_lowercase())
                })
                .map(|(i, _)| i)
                .collect();
        }
        self.scroll_offset = 0;
    }

    fn handle_events(&mut self) -> Result<()> {
        match event::read()? {
            // it's important to check that the event is a key press event as
            // crossterm also emits key release and repeat events on Windows.
            Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                self.handle_key_event(key_event)
            }
            _ => {}
        };
        Ok(())
    }
}

impl Widget for &Diagnose {
    fn render(self, area: ratatui::prelude::Rect, buf: &mut ratatui::prelude::Buffer)
    where
        Self: Sized,
    {
        // use Constraint::{Length, Min};
        let vertical = Layout::vertical([
            Constraint::Length(1),
            Constraint::Min(0),
            Constraint::Length(1),
        ]);
        let [header_area, inner_area, footer_area] = vertical.areas(area);

        let horizontal = Layout::horizontal([Constraint::Min(0), Constraint::Length(20)]);
        let [tabs_area, title_area] = horizontal.areas(header_area);

        "Dialog Dev Tools".bold().render(title_area, buf);

        // let titles = SelectedTab::iter().map(SelectedTab::title);
        let highlight_style = (Color::default(), Color::DarkGray);
        // let selected_tab_index = self.selected_tab as usize;
        Tabs::new(["Facts", "Tree"])
            .highlight_style(highlight_style)
            .select(self.selected_tab)
            .padding("", "")
            .divider(" ")
            .render(tabs_area, buf);

        Line::raw("◄ ► change tab | ↑↓ navigate | Enter expand/collapse | / filter | q quit")
            .centered()
            .render(footer_area, buf);

        match self.selected_tab {
            0 => {
                // Facts tab
                let facts = Facts {
                    facts: &self.facts,
                    filtered_facts: &self.filtered_facts,
                    scroll_offset: self.scroll_offset,
                    filter: &self.filter,
                    filter_mode: self.filter_mode,
                };
                facts.render(inner_area, buf);
            }
            1 => {
                // Tree tab
                let visible_nodes = self.get_visible_tree_nodes();
                let tree = Tree {
                    visible_nodes,
                    scroll_offset: self.tree_scroll_offset,
                    selected_index: self.tree_selected_index,
                };
                tree.render(inner_area, buf);
            }
            _ => {}
        }
    }
}

struct Facts<'a> {
    facts: &'a [CsvFact],
    filtered_facts: &'a [usize],
    scroll_offset: usize,
    filter: &'a str,
    filter_mode: bool,
}

impl Widget for &Facts<'_> {
    fn render(self, area: Rect, buf: &mut Buffer)
    where
        Self: Sized,
    {
        let header_style = Style::default();
        let selected_row_style = Style::default();

        let selected_col_style = Style::default();
        let selected_cell_style = Style::default().add_modifier(Modifier::REVERSED);

        let header = ["Entity", "Attribute", "Value", "Cause"]
            .into_iter()
            .map(Cell::from)
            .collect::<Row>()
            .style(header_style)
            .height(1);
        // let rows = self.items.iter().enumerate().map(|(i, data)| {
        //     let color = match i % 2 {
        //         0 => self.colors.normal_row_color,
        //         _ => self.colors.alt_row_color,
        //     };
        //     let item = data.ref_array();
        //     item.into_iter()
        //         .map(|content| Cell::from(Text::from(format!("\n{content}\n"))))
        //         .collect::<Row>()
        //         .style(Style::new().fg(self.colors.row_fg).bg(color))
        //         .height(4)
        // });
        let visible_rows = area.height as usize - 3; // Account for header and status
        let rows: Vec<Row> = self.filtered_facts
            .iter()
            .skip(self.scroll_offset)
            .take(visible_rows)
            .map(|&fact_idx| {
                let fact = &self.facts[fact_idx];
                let entity_display = if fact.entity.len() > 18 {
                    format!("{}...", &fact.entity[..15])
                } else {
                    fact.entity.clone()
                };
                
                let value_display = if fact.value.len() > 20 {
                    format!("{}...", &fact.value[..17])
                } else {
                    fact.value.clone()
                };
                
                Row::new([
                    Cell::from(entity_display),
                    Cell::from(fact.attribute.clone()),
                    Cell::from(value_display),
                    Cell::from(if fact.cause.is_empty() { "" } else { "✓" }),
                ])
            }).collect();
        let bar = " █ ";

        Table::new(
            rows,
            [
                // + 1 is for padding.
                Constraint::Min(16),
                Constraint::Min(16),
                Constraint::Min(8),
                Constraint::Min(8),
            ],
        )
        .header(header)
        .row_highlight_style(selected_row_style)
        .column_highlight_style(selected_col_style)
        .cell_highlight_style(selected_cell_style)
        .highlight_symbol(Text::from(vec![
            "".into(),
            bar.into(),
            bar.into(),
            "".into(),
        ]))
        .highlight_spacing(HighlightSpacing::Always)
        .render(
            Rect {
                x: area.x,
                y: area.y,
                width: area.width,
                height: area.height - 1,
            },
            buf,
        );

        // Status bar
        let status_y = area.y + area.height - 1;
        let status_text = if self.filter_mode {
            format!("Filter: {}_", self.filter)
        } else {
            format!(
                "Showing {}-{} of {} facts | Filter: {} | ↑↓ scroll, / filter, ESC cancel",
                self.scroll_offset + 1,
                (self.scroll_offset + visible_rows).min(self.filtered_facts.len()),
                self.filtered_facts.len(),
                if self.filter.is_empty() { "none" } else { self.filter }
            )
        };
        
        Line::raw(status_text)
            .style(if self.filter_mode { 
                Style::default().fg(Color::Yellow) 
            } else { 
                Style::default().dim() 
            })
            .render(
                Rect {
                    x: area.x,
                    y: status_y,
                    width: area.width,
                    height: 1,
                },
                buf,
            );
    }
}

#[derive(Debug, Clone)]
pub struct TreeNode {
    pub hash: String,
    pub level: u32,
    pub key: Option<String>,
    pub is_expanded: bool,
    pub is_branch: bool,
    pub children: Vec<TreeNode>,
    pub data: Option<String>, // For segment nodes, show actual data
    pub all_entries: Vec<String>, // All entries in this segment
}

struct Tree<'a> {
    visible_nodes: Vec<(usize, &'a TreeNode)>,
    scroll_offset: usize,
    selected_index: usize,
}

impl Widget for &Tree<'_> {
    fn render(self, area: Rect, buf: &mut Buffer)
    where
        Self: Sized,
    {
        // Header
        let header_text = "Prolly Tree Structure (Real Data from CSV)";
        Line::raw(header_text)
            .style(Style::default().bold().fg(Color::Cyan))
            .render(
                Rect {
                    x: area.x,
                    y: area.y,
                    width: area.width,
                    height: 1,
                },
                buf,
            );

        // Tree visualization
        let tree_area = Rect {
            x: area.x,
            y: area.y + 2,
            width: area.width,
            height: area.height - 2,
        };

        self.render_tree_nodes(tree_area, buf);
    }
}

impl Tree<'_> {
    fn render_tree_nodes(&self, area: Rect, buf: &mut Buffer) {
        let mut y_offset = 0;
        
        for (_display_i, (original_i, node)) in self.visible_nodes.iter().skip(self.scroll_offset).enumerate() {
            if y_offset >= area.height as usize {
                break;
            }
            
            let y = area.y + y_offset as u16;
            let x_offset = (node.level as u16) * 4; // 4 spaces per level
            
            // Check if this is the selected node
            let is_selected = *original_i == self.selected_index;
            let style = if is_selected {
                Style::default().bg(Color::DarkGray)
            } else {
                Style::default()
            };
            
            // Draw tree connectors
            let connector = if node.is_branch {
                if node.is_expanded {
                    "├─"
                } else {
                    "├+"
                }
            } else {
                "├─"
            };
            
            // Draw the connector
            if x_offset < area.width {
                buf.set_string(
                    area.x + x_offset,
                    y,
                    connector,
                    if is_selected { style } else { Style::default().fg(Color::DarkGray) },
                );
            }
            
            // Draw hash (truncated)
            let hash_display = format!("{:8}", &node.hash[..node.hash.len().min(8)]);
            if x_offset + 3 < area.width {
                buf.set_string(
                    area.x + x_offset + 3,
                    y,
                    &hash_display,
                    if is_selected { style.fg(Color::Yellow) } else { Style::default().fg(Color::Yellow) },
                );
            }
            
            // Draw key/data if present
            if let Some(key) = &node.key {
                let display_text = format!(" {}", key);
                if x_offset + 12 < area.width {
                    buf.set_string(
                        area.x + x_offset + 12,
                        y,
                        &display_text,
                        if is_selected { style.fg(Color::Green) } else { Style::default().fg(Color::Green) },
                    );
                }
            }
            
            // For segment nodes, show detailed data when selected  
            if is_selected && !node.is_branch && !node.all_entries.is_empty() {
                let mut data_y_offset = 1;
                for data_line in node.all_entries.iter().take(10) { // Limit to prevent overflow
                    if y_offset + data_y_offset >= area.height as usize {
                        break;
                    }
                    
                    if !data_line.is_empty() {
                        let data_text = format!("    {}", data_line);
                        buf.set_string(
                            area.x + x_offset,
                            y + data_y_offset as u16,
                            &data_text,
                            Style::default().fg(Color::Cyan).dim(),
                        );
                    }
                    data_y_offset += 1;
                }
                y_offset += data_y_offset - 1; // Account for extra lines used
            }
            
            y_offset += 1;
        }
        
        // Status line
        let status = format!(
            "{} visible nodes | Select segment to view data | Real prolly tree",
            self.visible_nodes.len()
        );
        if area.height > 0 {
            Line::raw(status)
                .style(Style::default().dim())
                .render(
                    Rect {
                        x: area.x,
                        y: area.y + area.height - 1,
                        width: area.width,
                        height: 1,
                    },
                    buf,
                );
        }
    }
}
