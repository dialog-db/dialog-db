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

#[tokio::main]
pub async fn main() -> Result<()> {
    let diagnose = Diagnose::default();
    let mut terminal = ratatui::init();
    terminal.clear()?;
    diagnose.run(terminal)?;
    ratatui::restore();
    Ok(())
}

#[derive(Default)]
pub struct Diagnose {
    exit: bool,
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
        match key_event.code {
            KeyCode::Char('q') => {
                self.exit = true;
            }
            _ => (),
        }
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
            .select(0)
            .padding("", "")
            .divider(" ")
            .render(tabs_area, buf);

        Line::raw("◄ ► to change tab | Press 'q' to quit")
            .centered()
            .render(footer_area, buf);

        let facts = Facts {};

        facts.render(inner_area, buf);
    }
}

struct Facts {}

impl Widget for &Facts {
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
        let rows: Vec<Row> = vec![];
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
        .render(area, buf);
    }
}
