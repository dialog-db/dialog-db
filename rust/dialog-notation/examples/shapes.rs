//! Count the shapes of forms: how many parts of an application carry no
//! label, and what they are.
//!
//! ```sh
//! cargo run -p dialog-notation --example shapes -- \
//!     rust/dialog-notation/src/prelude.clj rust/dialog-notation/examples/*.clj
//! ```
//!
//! It tests one claim: that an application is a head over labelled
//! values, where the one part written without a label is `of:`. Every
//! application in the files given is counted, the ones inside patterns
//! and templates included, and then every application of the kernel
//! rules the files expand to.

use std::collections::BTreeMap;

use anyhow::Result;
use dialog_notation::{Expander, Form, Part, read};

/// How many of an application's parts carry no label.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Shape {
    /// None, or one: the claim holds.
    Fits,
    /// One, and `of:` is written beside it. If the one is `of:`, that is
    /// `of:` twice.
    BesideOf,
    /// Several. Whether their order carries anything is a question
    /// about the head, which the table leaves to its reader.
    Several,
}

#[derive(Default)]
struct Count {
    shapes: BTreeMap<Shape, usize>,
    repeats: usize,
    example: BTreeMap<Shape, String>,
}

fn head(form: &Form) -> String {
    match form.as_application().map(|(head, _)| head) {
        Some(Form::Variable(_)) => "?head".into(),
        Some(Form::Keyword(name)) => format!(":{name}"),
        Some(Form::Symbol(name)) if name.contains('.') => "a.domain".into(),
        Some(Form::Symbol(name)) if name.chars().all(|c| "+-*/<>=".contains(c)) => "+ - < …".into(),
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

fn count(form: &Form, into: &mut BTreeMap<String, Count>) {
    match form {
        Form::Application(inner, parts) => {
            let unlabelled: Vec<&Form> = parts
                .iter()
                .filter_map(|part| match part {
                    Part::Bare(Form::Ellipsis | Form::Rest(_)) | Part::Labelled(..) => None,
                    Part::Bare(form) => Some(form),
                })
                .collect();
            let shape = match unlabelled.as_slice() {
                [] => Shape::Fits,
                [_] if form.at("of").is_some() => Shape::BesideOf,
                [_] => Shape::Fits,
                _ => Shape::Several,
            };
            let entry = into.entry(head(form)).or_default();
            *entry.shapes.entry(shape).or_default() += 1;
            entry
                .example
                .entry(shape)
                .or_insert_with(|| form.to_string());
            if parts
                .iter()
                .any(|part| matches!(part, Part::Bare(Form::Ellipsis)))
            {
                entry.repeats += 1;
            }
            count(inner, into);
            for part in parts {
                match part {
                    Part::Bare(form) | Part::Labelled(_, form) => count(form, into),
                }
            }
        }
        Form::Vector(items) => items.iter().for_each(|item| count(item, into)),
        Form::Map(entries) => entries.iter().for_each(|(key, value)| {
            count(key, into);
            count(value, into);
        }),
        _ => {}
    }
}

fn table(title: &str, counts: &BTreeMap<String, Count>) {
    println!("\n{title}\n");
    println!("| head | none or one | one beside `of:` | several | with `...` |");
    println!("|---|---|---|---|---|");
    let mut total = Count::default();
    for (head, count) in counts {
        let of = |shape| count.shapes.get(&shape).copied().unwrap_or(0);
        println!(
            "| `{head}` | {} | {} | {} | {} |",
            of(Shape::Fits),
            of(Shape::BesideOf),
            of(Shape::Several),
            count.repeats
        );
        for (shape, number) in &count.shapes {
            *total.shapes.entry(*shape).or_default() += number;
        }
        total.repeats += count.repeats;
    }
    let of = |shape| total.shapes.get(&shape).copied().unwrap_or(0);
    println!(
        "| **all** | {} | {} | {} | {} |",
        of(Shape::Fits),
        of(Shape::BesideOf),
        of(Shape::Several),
        total.repeats
    );
    println!();
    for (head, count) in counts {
        for (shape, example) in &count.example {
            if *shape != Shape::Fits {
                println!("- `{head}`, {shape:?}: `{example}`");
            }
        }
    }
}

fn main() -> Result<()> {
    let mut written = BTreeMap::new();
    let mut kernel = BTreeMap::new();
    let mut expander = Expander::new()?;
    for path in std::env::args().skip(1) {
        let text = std::fs::read_to_string(&path)?;
        for form in read(&text)? {
            count(&form, &mut written);
        }
        if !path.ends_with("prelude.clj") {
            let (rules, _) = expander.submit(&text)?;
            for rule in &rules {
                count(&rule.form(), &mut kernel);
            }
        }
    }
    table("As written", &written);
    table("The kernel rules they expand to", &kernel);
    Ok(())
}
