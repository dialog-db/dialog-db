use ratatui::prelude::*;

use crate::DiagnoseState;

mod table;
pub use table::*;

/// Widget for displaying database facts in a table format.
///
/// This widget renders a table view of database facts, displaying them in a paginated
/// format suitable for TUI display. It integrates with the `DiagnoseState` to show
/// facts from the current database store.
pub struct DiagnoseFacts {}

impl StatefulWidget for &DiagnoseFacts {
    type State = DiagnoseState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        let rows = state
            .store
            .facts(state.facts.index..(state.facts.index + area.height as usize - 1))
            .unwrap_or_default();

        FactTable {
            facts: rows,
            selected: None,
        }
        .render(area, buf);
    }
}
