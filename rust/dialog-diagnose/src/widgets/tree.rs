pub use ratatui::prelude::*;

use crate::{DiagnoseState, Promise};

mod explore;
pub use explore::*;

mod inspect;
pub use inspect::*;

mod distribution;
pub use distribution::*;

/// Widget for displaying the prolly tree structure in the TUI.
///
/// This widget provides a comprehensive view of the prolly tree, including:
/// - Tree exploration with expandable nodes
/// - Node inspection showing detailed information
/// - Distribution chart showing segment size statistics
///
/// The layout is divided into three sections: distribution chart (top),
/// tree explorer (middle), and node inspector (bottom).
pub struct DiagnoseTree {}

impl StatefulWidget for &DiagnoseTree {
    type State = DiagnoseState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        let layout = Layout::vertical([
            Constraint::Fill(2),
            Constraint::Fill(3),
            Constraint::Fill(3),
        ])
        .spacing(1);
        let [top_area, middle_area, bottom_area] = layout.areas(area);

        DiagnoseTreeExplore {}.render(middle_area, buf, state);
        NodeInspector {}.render(bottom_area, buf, state);

        match state.store.stats() {
            Promise::Resolved(stats) => DistributionChart { stats }.render(top_area, buf),
            Promise::Pending => {
                Line::raw("Analyzing tree...").render(top_area, buf);
            }
        }
    }
}
