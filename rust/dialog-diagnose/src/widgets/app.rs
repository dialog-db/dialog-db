//! Main application widget for the diagnose TUI.

use ratatui::{prelude::*, widgets::Tabs};

use crate::{DiagnoseFacts, DiagnoseState, DiagnoseTab, DiagnoseTree};

/// Main application widget that renders the overall TUI layout.
///
/// This widget handles the top-level layout including:
/// - Header with tab navigation
/// - Content area for the selected tab
/// - Footer with help text
pub struct DiagnoseApp {}

impl StatefulWidget for &DiagnoseApp {
    type State = DiagnoseState;

    /// Renders the main application layout with tabs and content area.
    ///
    /// This method:
    /// 1. Synchronizes data from background workers
    /// 2. Sets up the overall layout (header, content, footer)
    /// 3. Renders tab navigation and title
    /// 4. Delegates content rendering to the appropriate tab widget
    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        state.store.sync();

        let vertical = Layout::vertical([
            Constraint::Length(1),
            Constraint::Min(0),
            Constraint::Length(1),
        ]);
        let [header_area, inner_area, footer_area] = vertical.areas(area);

        let horizontal = Layout::horizontal([Constraint::Min(0), Constraint::Length(20)]);
        let [tabs_area, title_area] = horizontal.areas(header_area);

        "Dialog Dev Tools".bold().render(title_area, buf);

        let highlight_style = (Color::default(), Color::DarkGray);

        Tabs::new(["[F]acts", "[T]ree"])
            .highlight_style(highlight_style)
            .select(usize::from(&state.tab))
            .padding("", "")
            .divider(" ")
            .render(tabs_area, buf);

        Line::raw("Press 'q' to quit")
            .centered()
            .render(footer_area, buf);

        match &state.tab {
            DiagnoseTab::Facts => {
                DiagnoseFacts {}.render(inner_area, buf, state);
            }
            DiagnoseTab::Tree => DiagnoseTree {}.render(inner_area, buf, state),
        }
    }
}
