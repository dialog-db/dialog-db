use dialog_artifacts::{ATTRIBUTE_KEY_TAG, ENTITY_KEY_TAG, VALUE_KEY_TAG};
use ratatui::widgets::{StatefulWidget, Wrap};
use ratatui::{prelude::*, widgets::Paragraph};

use crate::{DiagnoseState, FactTable, Promise, TreeNode};

/// Widget for inspecting detailed information about selected tree nodes.
///
/// This widget displays detailed information about the currently selected
/// node in the tree explorer. For segment nodes, it shows the facts contained
/// within using a table format. For branch nodes, it displays the upper bound
/// key bytes in hexadecimal format.
///
/// The inspector adapts its display based on the node type:
/// - **Segment nodes**: Shows a table of facts/entries
/// - **Branch nodes**: Shows the upper bound key in hex format
/// - **Loading states**: Shows loading indicator
pub struct NodeInspector {}

impl StatefulWidget for NodeInspector {
    type State = DiagnoseState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        // "Inspector!".render(area, buf)
        let selected = state.store.node(&state.tree.selected_node);

        match selected {
            Promise::Resolved(node) => match node {
                TreeNode::Segment { entries } => {
                    let facts = entries
                        .iter()
                        .map(|entry| Promise::Resolved(&entry.value))
                        .collect::<Vec<_>>();

                    FactTable {
                        facts,
                        selected: None,
                    }
                    .render(area, buf)
                }
                TreeNode::Branch { upper_bound, .. } => {
                    let spans = match upper_bound.tag() {
                        ENTITY_KEY_TAG => vec![
                            (Color::Green, &upper_bound.as_ref()[0..1]),
                            (Color::Green, &upper_bound.as_ref()[1..65]),
                            (Color::Cyan, &upper_bound.as_ref()[65..129]),
                            (Color::LightMagenta, &upper_bound.as_ref()[129..130]),
                            (Color::Magenta, &upper_bound.as_ref()[130..]),
                        ],
                        ATTRIBUTE_KEY_TAG => vec![
                            (Color::Cyan, &upper_bound.as_ref()[0..1]),
                            (Color::Cyan, &upper_bound.as_ref()[1..65]),
                            (Color::Green, &upper_bound.as_ref()[65..129]),
                            (Color::LightMagenta, &upper_bound.as_ref()[129..130]),
                            (Color::Magenta, &upper_bound.as_ref()[130..]),
                        ],
                        VALUE_KEY_TAG => vec![
                            (Color::Magenta, &upper_bound.as_ref()[0..1]),
                            (Color::LightMagenta, &upper_bound.as_ref()[1..2]),
                            (Color::Magenta, &upper_bound.as_ref()[2..34]),
                            (Color::Cyan, &upper_bound.as_ref()[34..98]),
                            (Color::Green, &upper_bound.as_ref()[98..]),
                        ],
                        _ => vec![
                            (Color::Gray, &upper_bound.as_ref()[0..1]),
                            (Color::DarkGray, &upper_bound.as_ref()[1..]),
                        ],
                    }
                    .into_iter()
                    .enumerate()
                    .map(|(index, (color, bytes))| {
                        if index == 0 {
                            Span::from(format!("{:02X?}", bytes[0]))
                                .bold()
                                .fg(Color::Black)
                                .bg(color)
                        } else {
                            let mut span = Span::from(format!(
                                " {}",
                                bytes
                                    .iter()
                                    .map(|byte| format!("{:02X?}", byte))
                                    .collect::<Vec<_>>()
                                    .join(" ")
                            ))
                            .style(Style::new().fg(color));

                            if bytes.len() == 1 {
                                span = span.bold();
                            }

                            span
                        }
                    });

                    let mut line = Line::raw("");
                    for span in spans {
                        line.push_span(span);
                    }

                    let layout = Layout::vertical(vec![Constraint::Max(1), Constraint::Fill(1)]);
                    let [title, body] = layout.areas(area);

                    Line::from("Upper Bound Key Bytes")
                        .alignment(Alignment::Center)
                        .render(title, buf);

                    Paragraph::new(line)
                        .wrap(Wrap { trim: true })
                        .style(Style::new().fg(Color::Red))
                        .render(body, buf);
                }
            },
            Promise::Pending => "Loading node details...".render(area, buf),
        }
    }
}
