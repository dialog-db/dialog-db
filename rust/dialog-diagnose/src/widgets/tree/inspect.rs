use ratatui::widgets::{Block, StatefulWidget, Wrap};
use ratatui::{prelude::*, widgets::Paragraph};

use crate::{DiagnoseState, FactTable, Promise, TreeNode};

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
                    let key = upper_bound
                        .as_ref()
                        .iter()
                        .map(|byte| format!("{:02X?}", byte))
                        .collect::<Vec<_>>()
                        .join(" ");
                    let layout = Layout::vertical(vec![Constraint::Max(1), Constraint::Fill(1)]);
                    let [title, body] = layout.areas(area);

                    Line::from("Upper Bound Key Bytes")
                        .alignment(Alignment::Center)
                        .render(title, buf);
                    Paragraph::new(key)
                        .wrap(Wrap { trim: true })
                        .style(Style::new().fg(Color::Red))
                        .render(body, buf);
                }
            },
            Promise::Pending => "Loading node details...".render(area, buf),
        }
    }
}
