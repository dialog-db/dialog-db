use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use dialog_artifacts::Artifacts;
use dialog_diagnose::{DiagnoseApp, DiagnoseCli, DiagnoseState, DiagnoseTab, Promise, TreeNode};
use dialog_storage::MemoryStorageBackend;
use ratatui::{
    DefaultTerminal,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
};

#[tokio::main]
pub async fn main() -> Result<()> {
    let cli = DiagnoseCli::parse();
    let csv_path = std::env::current_dir()?.join(cli.csv);

    println!("CSV PATH: {}", csv_path.display());
    println!("{}", tokio::fs::try_exists(&csv_path).await?);

    let mut csv = tokio::fs::File::open(csv_path).await?;
    let mut artifacts = Artifacts::anonymous(MemoryStorageBackend::default()).await?;

    artifacts.import(&mut csv).await?;

    let starting_tab = match cli.tree {
        true => Some(DiagnoseTab::Tree),
        _ => None,
    };

    let diagnose = Diagnose::new(artifacts, starting_tab).await?;
    let mut terminal = ratatui::init();
    terminal.clear()?;
    diagnose.run(terminal)?;
    ratatui::restore();
    Ok(())
}

pub struct Diagnose {
    exit: bool,
    state: DiagnoseState,
}

impl Diagnose {
    pub async fn new(
        artifacts: Artifacts<MemoryStorageBackend<[u8; 32], Vec<u8>>>,
        starting_tab: Option<DiagnoseTab>,
    ) -> Result<Self> {
        let mut state = DiagnoseState::new(artifacts).await;

        if let Some(tab) = starting_tab {
            state.tab = tab;
        }

        Ok(Self { exit: false, state })
    }

    pub fn run(mut self, mut terminal: DefaultTerminal) -> Result<()> {
        loop {
            if self.exit {
                break;
            }

            terminal.draw(|frame| {
                frame.render_stateful_widget(&DiagnoseApp {}, frame.area(), &mut self.state)
            })?;

            self.handle_events()?;
        }

        Ok(())
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        match key_event.code {
            KeyCode::Char('q') => {
                self.exit = true;
            }
            KeyCode::Char('f') | KeyCode::Char('F') => self.state.tab = DiagnoseTab::Facts,
            KeyCode::Char('t') | KeyCode::Char('T') => self.state.tab = DiagnoseTab::Tree,
            KeyCode::Up => match self.state.tab {
                DiagnoseTab::Facts => {
                    self.state.facts.index = self.state.facts.index.saturating_sub(1);
                }
                DiagnoseTab::Tree => {
                    self.state.tree.select_previous(&self.state.store);
                }
            },
            KeyCode::Down => match self.state.tab {
                DiagnoseTab::Facts => {
                    self.state.facts.index = self.state.facts.index.saturating_add(1);
                }
                DiagnoseTab::Tree => {
                    self.state.tree.select_next(&self.state.store);
                }
            },
            KeyCode::Enter => match self.state.tab {
                DiagnoseTab::Facts => (),
                DiagnoseTab::Tree => {
                    let selected_hash = &self.state.tree.selected_node;
                    match self.state.store.node(selected_hash) {
                        Promise::Resolved(TreeNode::Branch { .. }) => {
                            if self.state.tree.expanded.contains(selected_hash) {
                                self.state.tree.expanded.remove(selected_hash);
                            } else {
                                self.state.tree.expanded.insert(selected_hash.to_owned());
                            }
                        }
                        _ => (),
                    }
                }
            },
            _ => (),
        }
    }

    fn handle_events(&mut self) -> Result<()> {
        if event::poll(Duration::from_millis(100))? {
            match event::read()? {
                // it's important to check that the event is a key press event as
                // crossterm also emits key release and repeat events on Windows.
                Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                    self.handle_key_event(key_event)
                }
                _ => {}
            };
        }

        Ok(())
    }
}
