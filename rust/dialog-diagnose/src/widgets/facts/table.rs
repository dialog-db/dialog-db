use dialog_artifacts::{Artifact, Datum, Key, State};
use ratatui::{
    prelude::*,
    widgets::{Cell, HighlightSpacing, Row, Table},
};

use crate::Promise;

/// A loaded fact for rendering: its index key (needed to reconstruct the
/// entity, attribute, and inline value) paired with its payload, or pending.
type FactPromise<'a> = Promise<(&'a Key, &'a State<Datum>)>;

/// A table widget for rendering database facts.
///
/// This widget displays facts in a structured table format with columns for
/// entity, attribute, value, and cause. It handles both resolved facts and
/// pending promises, providing visual feedback for loading states.
pub struct FactTable<'a> {
    /// List of facts to display, each with its index key (needed to
    /// reconstruct the entity, attribute, and inline value), potentially
    /// including pending promises
    pub facts: Vec<FactPromise<'a>>,
    /// Index of currently selected row, if any
    pub selected: Option<usize>,
}

impl Widget for FactTable<'_> {
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

        let rows: Vec<Row<'_>> = self
            .facts
            .iter()
            .enumerate()
            .map(|(i, data)| {
                let color = match i % 2 {
                    0 => Color::Black,
                    _ => Color::Reset,
                };

                match data {
                    Promise::Pending => {
                        Row::new([Cell::from(Text::from("Loading fact...".to_string()))])
                    }
                    Promise::Resolved((key, State::Added(datum))) => {
                        let (entity, attribute, value) = match Artifact::from_key_datum(key, datum)
                        {
                            Ok(artifact) => (
                                artifact.of.to_string(),
                                artifact.the.to_string(),
                                artifact.is.to_utf8(),
                            ),
                            Err(error) => (String::new(), String::new(), format!("{error}")),
                        };

                        Row::new(
                            [entity, attribute, value, format!("{:?}", datum.cause)]
                                .into_iter()
                                .enumerate()
                                .map(|(index, value)| {
                                    Cell::from(Text::from(value)).style(Style::new().fg(
                                        match index {
                                            0 => Color::Green,
                                            1 => Color::Cyan,
                                            2 => Color::Magenta,
                                            _ => Color::Red,
                                        },
                                    ))
                                }),
                        )
                    }
                    Promise::Resolved((_key, State::Removed)) => {
                        Row::new([Cell::from(Text::from("<Retracted>".to_string()))])
                    }
                }
                .style(Style::new().bg(color))
            })
            .collect();

        let bar = " █ ";

        let table = Table::new(
            rows,
            [
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
        .highlight_spacing(HighlightSpacing::Always);

        Widget::render(table, area, buf);
    }
}
