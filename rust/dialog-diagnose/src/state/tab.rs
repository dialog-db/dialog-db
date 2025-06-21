#[derive(Default)]
pub enum DiagnoseTab {
    #[default]
    Facts,
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
