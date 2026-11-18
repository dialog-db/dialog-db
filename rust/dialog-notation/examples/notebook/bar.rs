//! How far each top-level form of a document gets through the three
//! phases: read, expanded, compiled. The example and the tests share it.

use std::collections::BTreeMap;

use dialog_notation::{Error, Expander, Segment, View, compile, parse_document};

/// Where a form stopped, and why.
#[derive(Debug, Clone)]
pub struct Stop {
    /// Which top-level form, counted from 0, among those read.
    pub form: Option<usize>,
    /// The phase it stopped in.
    pub phase: &'static str,
    /// The first error it met.
    pub error: Error,
}

/// How far a document's forms get.
#[derive(Debug, Clone, Default)]
pub struct Bar {
    /// Every top-level form, read or not.
    pub forms: usize,
    /// The forms the reader took.
    pub read: usize,
    /// The forms the walker expanded to kernel rules, or kept as a
    /// definition.
    pub expanded: usize,
    /// The expanded forms every kernel rule of which compiled.
    pub compiled: usize,
    /// The views that reach a description, `View::describe`, whatever
    /// Dialog-DB's compiler says of them.
    pub described: usize,
    /// The first error of each form that stopped.
    pub stops: Vec<Stop>,
}

impl Bar {
    /// Measure several documents read in order as one, as tonk reads a
    /// component joined with core.
    pub fn of_all(texts: &[&str]) -> Result<Bar, Error> {
        Bar::of(&texts.join("\n\n"))
    }

    /// Measure `text`, one document, on a walker that knows only the
    /// prelude.
    pub fn of(text: &str) -> Result<Bar, Error> {
        let parsed = parse_document(text);
        let mut bar = Bar {
            read: parsed.trees.len(),
            ..Bar::default()
        };
        for diagnostic in &parsed.diagnostics {
            if let Some(error) = &diagnostic.refusal {
                bar.stops.push(Stop {
                    form: None,
                    phase: "read",
                    error: error.clone(),
                });
            }
        }
        bar.forms = bar.read + bar.stops.len();
        let mut expander = Expander::new()?;
        let (kernel, diagnostics) = expander.check(parsed.trees.clone());
        let origin = expander.origin().to_vec();
        let mut stopped: BTreeMap<usize, Stop> = BTreeMap::new();
        for diagnostic in diagnostics {
            if let (Some(error), Some(Segment::Item(at))) =
                (diagnostic.refusal, diagnostic.path.first())
            {
                stopped.entry(*at).or_insert(Stop {
                    form: Some(*at),
                    phase: "expand",
                    error,
                });
            }
        }
        bar.expanded = bar.read - stopped.len();
        for (rule, at) in kernel.iter().zip(origin) {
            if stopped.contains_key(&at) {
                continue;
            }
            if rule.applies() == Some("view") && View::describe(rule).is_ok() {
                bar.described += 1;
            }
            if let Err(error) = compile(rule) {
                stopped.insert(
                    at,
                    Stop {
                        form: Some(at),
                        phase: "compile",
                        error,
                    },
                );
            }
        }
        bar.compiled = bar.read - stopped.len();
        bar.stops.extend(stopped.into_values());
        Ok(bar)
    }

    /// The stops grouped by what they say, most first: the message with
    /// the forms it quotes taken out, so one cause is one group.
    pub fn groups(&self) -> Vec<(String, Vec<&Stop>)> {
        let mut groups: BTreeMap<String, Vec<&Stop>> = BTreeMap::new();
        for stop in &self.stops {
            groups
                .entry(format!("{}: {}", stop.phase, said(&stop.error)))
                .or_default()
                .push(stop);
        }
        let mut groups: Vec<_> = groups.into_iter().collect();
        groups.sort_by_key(|(_, stops)| std::cmp::Reverse(stops.len()));
        groups
    }
}

/// What an error says, without the forms it quotes. An unknown form is
/// said by the name it applies.
fn said(error: &Error) -> String {
    if let Error::Unknown(form) = error {
        let name = form
            .trim_start_matches(['(', '['])
            .split_whitespace()
            .next()
            .unwrap_or_default();
        return format!("nothing says what `{name}` is");
    }
    let mut depth = 0usize;
    let mut kept = String::new();
    for c in error.to_string().chars() {
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            c if depth == 0 => kept.push(c),
            _ => {}
        }
    }
    kept.split_whitespace().collect::<Vec<_>>().join(" ")
}
