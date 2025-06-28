use std::collections::VecDeque;

pub use ratatui::prelude::*;
use ratatui::widgets::{Block, List, ListState};

use crate::{DiagnoseState, Promise, TreeNode};
use base58::ToBase58;

/// Widget for exploring the prolly tree structure interactively.
///
/// This widget renders a tree view where users can navigate through branch
/// and segment nodes. It shows node hashes, types, and hierarchy with
/// visual indicators for expanded/collapsed states and selection.
///
/// Features:
/// - Hierarchical tree visualization with indentation
/// - Branch nodes can be expanded to show children
/// - Segment nodes display entry counts
/// - Visual highlighting for selected nodes
/// - Base58-encoded hash display for node identification
pub struct DiagnoseTreeExplore {}

impl StatefulWidget for &DiagnoseTreeExplore {
    type State = DiagnoseState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        let stats = state.store.stats();
        let root = state.store.node(&state.tree.root);

        let tree = Block::new();

        let mut nodes = VecDeque::from([(vec![], &state.tree.root, root)]);
        let mut lines = Vec::new();

        let [legend_area, tree_area] =
            Layout::vertical(vec![Constraint::Max(1), Constraint::Fill(1)]).areas(area);

        let mut selected_index = 0;
        let mut max_expanded_depth = 0usize;

        while let Some((depth, hash, promise)) = nodes.pop_front() {
            let selected = hash == &state.tree.selected_node;
            let is_root = hash == &state.tree.root;
            let is_expanded = state.tree.expanded.contains(hash);

            max_expanded_depth = max_expanded_depth.max(depth.len());

            let mut hash_string = hash.to_base58();
            hash_string.truncate(8);

            let mut hash_span = hash_string.bold().style(Style::new().fg(Color::Yellow));

            if selected {
                selected_index = lines.len();
                hash_span = hash_span.patch_style(Style::new().bg(Color::DarkGray));
            }

            let indentation = Span::from(
                depth
                    .iter()
                    .rev()
                    .enumerate()
                    .map(
                        |(index, has_next_sibling)| match (index, has_next_sibling) {
                            (0, true) => " ├",
                            (0, false) => " ╰",
                            (_, true) => " │",
                            _ => "  ",
                        },
                    )
                    .rev()
                    .collect::<String>(),
            )
            .style(Style::new().fg(Color::Cyan));

            match promise {
                Promise::Resolved(TreeNode::Branch {
                    upper_bound,
                    children,
                }) => {
                    let mut upper_bound = upper_bound.to_base58();
                    upper_bound.truncate(8);

                    let branch_type = if is_root { "Root" } else { "Branch" };

                    let bullet = match is_expanded {
                        true => " − ",
                        false => " + ",
                    }
                    .bold()
                    .style(Style::new().fg(Color::Yellow));

                    if is_expanded {
                        for (i, child) in children.iter().rev().enumerate() {
                            let mut depth = depth.clone();
                            if i == 0 {
                                depth.push(false);
                            } else {
                                depth.push(true);
                            }

                            nodes.push_front((depth, child, state.store.node(child)));
                        }
                    }

                    let label = format!("{} ({} children)", branch_type, children.len())
                        .bold()
                        .style(Style::new().fg(Color::Cyan));

                    let mut spans = vec![indentation, bullet, hash_span, " · ".into(), label];

                    if let Promise::Resolved(stats) = &stats {
                        if is_root {
                            spans.push(" · ".into());
                            spans.push(
                                Span::from(format!("Max. depth {}", stats.depth - 1))
                                    .style(Style::new().fg(Color::Green)),
                            );
                        }
                    }

                    lines.push(Line::from(spans));
                }
                Promise::Resolved(TreeNode::Segment { entries }) => {
                    let label = format!("Segment ({} entries)", entries.len())
                        .bold()
                        .style(Style::new().fg(Color::Cyan));

                    let bullet = Span::from(" • ").style(Style::new().fg(Color::Yellow));

                    lines.push(Line::from(vec![
                        indentation,
                        bullet,
                        hash_span,
                        " · ".into(),
                        label,
                    ]));
                }
                Promise::Pending => {
                    lines.push(Line::from(vec![
                        indentation,
                        // angle,
                        Span::from(format!(" Loading {}...", hash_span))
                            .bold()
                            .style(Style::new().fg(Color::Yellow)),
                    ]));
                }
            };
        }

        if let Promise::Resolved(stats) = &stats {
            let line = Line::from(format!(
                "{} <- Tree Level",
                (0..max_expanded_depth + 1)
                    .map(|depth| format!(" {}", stats.depth.saturating_sub(depth + 1)))
                    .collect::<String>()
            ))
            .style(Style::new().fg(Color::DarkGray));
            line.render(legend_area, buf);
        }

        let scroll_height = area.height as usize;
        let scroll_offset = state.tree.scroll_offset;
        let scroll_offset_height = scroll_height + scroll_offset;

        let scroll_offset = if selected_index > scroll_offset_height - 2 {
            selected_index - (scroll_height - 2)
        } else if selected_index < scroll_offset + 2 {
            selected_index.saturating_sub(2)
        } else {
            scroll_offset
        };

        state.tree.scroll_offset = scroll_offset;

        let list = List::new(lines);
        let mut list_state = ListState::default().with_offset(scroll_offset);

        StatefulWidget::render(list.block(tree), tree_area, buf, &mut list_state);
    }
}
