//! Tab selection for the diagnose TUI.

/// Represents the different views available in the diagnose TUI.
///
/// Users can switch between these tabs using keyboard shortcuts:
/// - 'f' or 'F' to switch to Facts view
/// - 't' or 'T' to switch to Tree view
#[derive(Default)]
pub enum DiagnoseTab {
    /// Facts table view - shows database facts in a tabular format
    #[default]
    Facts,
    /// Tree explorer view - shows the prolly tree structure with expandable nodes
    Tree,
}

impl From<&DiagnoseTab> for usize {
    fn from(value: &DiagnoseTab) -> Self {
        match value {
            DiagnoseTab::Facts => 0,
            DiagnoseTab::Tree => 1,
        }
    }
}
