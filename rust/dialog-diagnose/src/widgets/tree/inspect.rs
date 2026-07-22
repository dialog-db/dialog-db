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
                        .map(|entry| Promise::Resolved((&entry.key, &entry.value)))
                        .collect::<Vec<_>>();

                    FactTable {
                        facts,
                        selected: None,
                    }
                    .render(area, buf)
                }
                TreeNode::Branch { separators, .. } => {
                    // Links carry truncated separators (variable-length
                    // prefixes of their subtree's minimum key), so render
                    // one row per child: the tag byte highlighted when
                    // present, the remaining bytes plain hex. The leftmost
                    // child's separator is empty by construction.
                    let lines: Vec<Line> = separators
                        .iter()
                        .map(|separator| {
                            let mut line = Line::raw("");
                            match separator.split_first() {
                                None => {
                                    line.push_span(
                                        Span::from("(empty: leftmost)")
                                            .style(Style::new().fg(Color::DarkGray)),
                                    );
                                }
                                Some((tag, rest)) => {
                                    let color = match *tag {
                                        ENTITY_KEY_TAG => Color::Green,
                                        ATTRIBUTE_KEY_TAG => Color::Cyan,
                                        VALUE_KEY_TAG => Color::Magenta,
                                        _ => Color::Gray,
                                    };
                                    line.push_span(
                                        Span::from(format!("{tag:02X?}"))
                                            .bold()
                                            .fg(Color::Black)
                                            .bg(color),
                                    );
                                    line.push_span(
                                        Span::from(format!(
                                            " {}",
                                            rest.iter()
                                                .map(|byte| format!("{byte:02X?}"))
                                                .collect::<Vec<_>>()
                                                .join(" ")
                                        ))
                                        .style(Style::new().fg(color)),
                                    );
                                }
                            }
                            line
                        })
                        .collect();

                    let layout = Layout::vertical(vec![Constraint::Max(1), Constraint::Fill(1)]);
                    let [title, body] = layout.areas(area);

                    Line::from("Child Separator Bytes")
                        .alignment(Alignment::Center)
                        .render(title, buf);

                    Paragraph::new(lines)
                        .wrap(Wrap { trim: true })
                        .render(body, buf);
                }
            },
            Promise::Pending => "Loading node details...".render(area, buf),
        }
    }
}
