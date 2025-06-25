use anyhow::Result;
use ratatui::{
    DefaultTerminal,
    buffer::Buffer,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Text},
    widgets::{Cell, HighlightSpacing, Row, Table, Tabs, Widget},
};

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
}

impl Default for Diagnose {
    fn default() -> Self {
        Self {
            exit: false,
            selected_tab: 0,
            facts: Vec::new(),
            filtered_facts: Vec::new(),
            scroll_offset: 0,
            filter: String::new(),
            filter_mode: false,
        }
    }
}

impl Diagnose {
    pub async fn new() -> Result<Self> {
        let facts = load_facts_from_csv("pokemon_artifacts.csv").await?;
        let filtered_facts = (0..facts.len()).collect();
        Ok(Self {
            exit: false,
            selected_tab: 0,
            facts,
            filtered_facts,
            scroll_offset: 0,
            filter: String::new(),
            filter_mode: false,
        })
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
                    }
                }
                KeyCode::Down => {
                    if self.selected_tab == 0 && self.scroll_offset < self.filtered_facts.len().saturating_sub(1) {
                        self.scroll_offset += 1;
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

        Line::raw("◄ ► change tab | ↑↓ scroll | / filter | q quit")
            .centered()
            .render(footer_area, buf);

        let facts = Facts {
            facts: &self.facts,
            filtered_facts: &self.filtered_facts,
            scroll_offset: self.scroll_offset,
            filter: &self.filter,
            filter_mode: self.filter_mode,
        };

        facts.render(inner_area, buf);
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
