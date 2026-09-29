#![cfg(not(target_arch = "wasm32"))]

//! # Dialog Diagnose Binary
//!
//! A command-line tool that provides a TUI for exploring Dialog databases.
//! This binary creates an interactive terminal interface for browsing database
//! facts and navigating the prolly tree structure.

use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use dialog_csv::CsvImporter;
use dialog_diagnose::{DiagnoseApp, DiagnoseCli, DiagnoseState, DiagnoseTab, Promise, TreeNode};
use futures_util::StreamExt as _;
use ratatui::{
    DefaultTerminal,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
};

/// Main entry point for the diagnose TUI application.
///
/// This function:
/// 1. Parses command-line arguments
/// 2. Commits the CSV's facts to a branch of an in-memory repository
/// 3. Initializes the TUI with the specified starting tab
/// 4. Runs the interactive terminal interface
#[tokio::main]
pub async fn main() -> Result<()> {
    let cli = DiagnoseCli::parse();
    let csv_path = std::env::current_dir()?.join(cli.csv);

    println!("CSV PATH: {}", csv_path.display());
    println!("{}", tokio::fs::try_exists(&csv_path).await?);

    let csv = tokio::fs::File::open(csv_path).await?;
    let artifacts = CsvImporter::new(csv).filter_map(|artifact| async move {
        artifact
            .inspect_err(|error| println!("Skipping invalid datum: {error}"))
            .ok()
    });
    let explored = dialog_diagnose::explore(artifacts).await?;

    let starting_tab = match cli.tree {
        true => Some(DiagnoseTab::Tree),
        _ => None,
    };

    let diagnose = Diagnose::new(explored, starting_tab).await?;
    let mut terminal = ratatui::init();
    terminal.clear()?;
    diagnose.run(terminal)?;
    ratatui::restore();
    Ok(())
}

/// Main application struct that manages the TUI state and event loop.
///
/// The `Diagnose` struct coordinates between user input, application state,
/// and the terminal rendering. It handles keyboard events and maintains
/// the overall application lifecycle.
pub struct Diagnose {
    /// Flag to control application exit
    exit: bool,
    /// Application state containing UI state and database store
    state: DiagnoseState,
}

impl Diagnose {
    /// Creates a new `Diagnose` instance over an explored tree and optional starting tab.
    ///
    /// # Arguments
    ///
    /// * `explored` - The committed tree to explore
    /// * `starting_tab` - Optional tab to open initially (defaults to Facts view)
    ///
    /// # Returns
    ///
    /// A new `Diagnose` instance ready to run the TUI
    pub async fn new(
        explored: dialog_diagnose::Explored,
        starting_tab: Option<DiagnoseTab>,
    ) -> Result<Self> {
        let mut state = DiagnoseState::new(explored).await;

        if let Some(tab) = starting_tab {
            state.tab = tab;
        }

        Ok(Self { exit: false, state })
    }

    /// Runs the main TUI event loop.
    ///
    /// This method handles the continuous cycle of:
    /// 1. Rendering the current state to the terminal
    /// 2. Processing keyboard events
    /// 3. Updating application state
    ///
    /// The loop continues until the user presses 'q' to quit.
    ///
    /// # Arguments
    ///
    /// * `terminal` - The ratatui terminal instance to render to
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

    /// Handles keyboard input events and updates application state accordingly.
    ///
    /// # Key Bindings
    ///
    /// * `q` - Quit the application
    /// * `f/F` - Switch to Facts tab
    /// * `t/T` - Switch to Tree tab
    /// * `Up/Down` - Navigate within the current tab
    /// * `Enter` - In Tree tab, expand/collapse selected node
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
                    if let Promise::Resolved(TreeNode::Branch { .. }) =
                        self.state.store.node(selected_hash)
                    {
                        if self.state.tree.expanded.contains(selected_hash) {
                            self.state.tree.expanded.remove(selected_hash);
                        } else {
                            self.state.tree.expanded.insert(selected_hash.to_owned());
                        }
                    }
                }
            },
            _ => (),
        }
    }

    /// Polls for and processes terminal events with a timeout.
    ///
    /// This method checks for keyboard events with a 100ms timeout,
    /// ensuring the UI remains responsive while not consuming excessive CPU.
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
