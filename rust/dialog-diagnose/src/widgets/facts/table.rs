use dialog_artifacts::{Datum, State, Value, ValueDataType};
use ratatui::{
    prelude::*,
    widgets::{Cell, HighlightSpacing, Row, Table},
};

use crate::Promise;

pub struct FactTable<'a> {
    pub facts: Vec<Promise<&'a State<Datum>>>,
    pub selected: Option<usize>,
}

impl<'a> Widget for FactTable<'a> {
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
                        Row::new([Cell::from(Text::from(format!("Loading fact...")))])
                    }
                    Promise::Resolved(State::Added(datum)) => {
                        let value = Value::try_from((
                            ValueDataType::try_from(datum.value_type).unwrap_or_default(),
                            datum.value.clone(),
                        ))
                        .map(|value| value.to_utf8())
                        .unwrap_or_else(|error| format!("{error}"));

                        Row::new(
                            [
                                format!("{}", datum.entity),
                                format!("{}", datum.attribute),
                                value,
                                format!("{:?}", datum.cause),
                            ]
                            .into_iter()
                            .enumerate()
                            .map(|(index, value)| {
                                Cell::from(Text::from(value)).style(Style::new().fg(match index {
                                    0 => Color::Green,
                                    1 => Color::Cyan,
                                    2 => Color::Magenta,
                                    _ => Color::Red,
                                }))
                            }),
                        )
                    }
                    Promise::Resolved(State::Removed) => {
                        Row::new([Cell::from(Text::from(format!("<Retracted>")))])
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
