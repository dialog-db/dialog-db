//! Forms: the notation as data.
//!
//! A form is an atom or an application. An application has a head and
//! parts; a part is bare or labelled, `label: part`. Reading a form
//! gives it no meaning: `(todo ?t text: "x")` is a head named `todo`, a
//! variable and a labelled text, nothing more.

use std::fmt::{self, Display, Formatter};

use crate::Error;

/// A piece of notation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Form {
    /// A name, a domain, an entity or an operator: `todo`, `xyz.todo`,
    /// `list:home`, `+`.
    Symbol(String),
    /// `?name`, held without its `?`.
    Variable(String),
    /// `_`.
    Blank,
    /// `:name`, held without its `:`. A keyword is the entity `name:`.
    Keyword(String),
    /// `"text"`.
    Text(String),
    /// A number as it was written.
    Number(String),
    /// `...`, which in a pattern or a template repeats what is before it.
    Ellipsis,
    /// `...?name`, the labelled parts a pattern does not name.
    Rest(String),
    /// `[a b]`.
    Vector(Vec<Form>),
    /// `(head part...)`.
    Application(Box<Form>, Vec<Part>),
}

/// One part of an application.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Part {
    /// A part with no label.
    Bare(Form),
    /// `label: part`. The label `:` alone is the empty label.
    Labelled(String, Form),
}

/// What kind of thing a symbol names, told from its spelling alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A reverse-dotted domain, where facts live: `xyz.todo`.
    Domain,
    /// A URI: `list:home`, `did:key:z6Mk`.
    Entity,
    /// A domain and a name in it: `xyz.todo/text`.
    Attribute,
    /// Only operator characters: `+`, `<=`.
    Operator,
    /// Anything else: the name of a concept, a command or other syntax.
    Name,
}

/// The characters an operator is made of.
const OPERATOR: &str = "+-*/<>=";

impl Form {
    /// An application of `head` to `parts`.
    pub fn application(head: Form, parts: Vec<Part>) -> Self {
        Form::Application(Box::new(head), parts)
    }

    /// A symbol.
    pub fn symbol(name: impl Into<String>) -> Self {
        Form::Symbol(name.into())
    }

    /// The head and parts of an application.
    pub fn as_application(&self) -> Option<(&Form, &[Part])> {
        match self {
            Form::Application(head, parts) => Some((head, parts)),
            _ => None,
        }
    }

    /// The symbol an application's head is, if it is one.
    pub fn head(&self) -> Option<&str> {
        match self.as_application()?.0 {
            Form::Symbol(name) => Some(name),
            _ => None,
        }
    }

    /// Whether this is an application whose head is the symbol `name`.
    pub fn is(&self, name: &str) -> bool {
        self.head() == Some(name)
    }

    /// The kind of thing a symbol names. `None` for anything but a symbol.
    pub fn kind(&self) -> Option<Kind> {
        let Form::Symbol(name) = self else {
            return None;
        };
        Some(if name.chars().all(|c| OPERATOR.contains(c)) {
            Kind::Operator
        } else if name.contains(':') {
            Kind::Entity
        } else if name.contains('/') {
            Kind::Attribute
        } else if name.contains('.') {
            Kind::Domain
        } else {
            Kind::Name
        })
    }

    /// Whether this applies a relation: its head is a domain.
    pub fn is_read(&self) -> bool {
        self.as_application()
            .is_some_and(|(head, _)| head.kind() == Some(Kind::Domain))
    }

    /// Whether this applies a formula: its head is a keyword or an
    /// operator.
    pub fn is_formula(&self) -> bool {
        self.as_application().is_some_and(|(head, _)| {
            matches!(head, Form::Keyword(_)) || head.kind() == Some(Kind::Operator)
        })
    }

    /// The bare parts of an application, a `...?rest` left out.
    pub fn bare(&self) -> Vec<&Form> {
        self.as_application()
            .map(|(_, parts)| {
                parts
                    .iter()
                    .filter_map(|part| match part {
                        Part::Bare(Form::Rest(_)) | Part::Labelled(..) => None,
                        Part::Bare(form) => Some(form),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The labelled parts of an application, in the order written.
    pub fn labelled(&self) -> Vec<(&str, &Form)> {
        self.as_application()
            .map(|(_, parts)| {
                parts
                    .iter()
                    .filter_map(|part| match part {
                        Part::Labelled(label, form) => Some((label.as_str(), form)),
                        Part::Bare(_) => None,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The form under `label`.
    pub fn at(&self, label: &str) -> Option<&Form> {
        self.labelled()
            .into_iter()
            .find_map(|(name, form)| (name == label).then_some(form))
    }
}

/// Every form in `text`, in order.
pub fn read(text: &str) -> Result<Vec<Form>, Error> {
    let tokens = tokens(text)?;
    let mut at = 0;
    let mut forms = Vec::new();
    while at < tokens.len() {
        forms.push(form(&tokens, &mut at)?);
    }
    Ok(forms)
}

/// The tokens of `text`: brackets, text literals and runs of anything
/// else. A `;` starts a comment that runs to the end of its line.
fn tokens(text: &str) -> Result<Vec<String>, Error> {
    let mut tokens = Vec::new();
    let mut chars = text.chars().peekable();
    while let Some(&next) = chars.peek() {
        match next {
            c if c.is_whitespace() => {
                chars.next();
            }
            ';' => {
                for c in chars.by_ref() {
                    if c == '\n' {
                        break;
                    }
                }
            }
            '(' | ')' | '[' | ']' => {
                tokens.push(next.to_string());
                chars.next();
            }
            '"' => {
                chars.next();
                let mut literal = String::from('"');
                loop {
                    match chars.next() {
                        None => return Err(Error::Read("a text that never ends".into())),
                        Some('"') => break,
                        Some('\\') => match chars.next() {
                            Some('n') => literal.push('\n'),
                            Some(escaped) => literal.push(escaped),
                            None => return Err(Error::Read("a text that never ends".into())),
                        },
                        Some(c) => literal.push(c),
                    }
                }
                tokens.push(literal);
            }
            _ => {
                let mut token = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_whitespace() || "()[];\"".contains(c) {
                        break;
                    }
                    token.push(c);
                    chars.next();
                }
                tokens.push(token);
            }
        }
    }
    Ok(tokens)
}

/// Whether a token is a label: it ends with `:` and is not all colons
/// past the first, so `text:` and `:` are labels and `::` is not.
fn label(token: &str) -> Option<&str> {
    let name = token.strip_suffix(':')?;
    (!token.starts_with('"') && !name.ends_with(':')).then_some(name)
}

fn atom(token: &str) -> Form {
    if token == "..." {
        Form::Ellipsis
    } else if let Some(name) = token.strip_prefix("...?") {
        Form::Rest(name.to_string())
    } else if let Some(name) = token.strip_prefix('?') {
        Form::Variable(name.to_string())
    } else if token == "_" {
        Form::Blank
    } else if let Some(text) = token.strip_prefix('"') {
        Form::Text(text.to_string())
    } else if is_number(token) {
        Form::Number(token.to_string())
    } else if let Some(name) = token.strip_prefix(':') {
        Form::Keyword(name.to_string())
    } else {
        Form::Symbol(token.to_string())
    }
}

fn is_number(token: &str) -> bool {
    let digits = token.strip_prefix('-').unwrap_or(token);
    let mut parts = digits.splitn(2, '.');
    let whole = parts.next().unwrap_or_default();
    let all_digits = |part: &str| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit());
    all_digits(whole) && parts.next().is_none_or(all_digits)
}

fn form(tokens: &[String], at: &mut usize) -> Result<Form, Error> {
    let ended = || Error::Read("a form that never closes".into());
    let token = tokens.get(*at).ok_or_else(ended)?;
    *at += 1;
    match token.as_str() {
        "[" => {
            let mut items = Vec::new();
            while tokens.get(*at).ok_or_else(ended)? != "]" {
                items.push(form(tokens, at)?);
            }
            *at += 1;
            Ok(Form::Vector(items))
        }
        "(" => {
            let head = form(tokens, at)?;
            let mut parts = Vec::new();
            loop {
                let next = tokens.get(*at).ok_or_else(ended)?;
                if next == ")" {
                    break;
                }
                match label(next) {
                    Some(name) => {
                        *at += 1;
                        parts.push(Part::Labelled(name.to_string(), form(tokens, at)?));
                    }
                    None => parts.push(Part::Bare(form(tokens, at)?)),
                }
            }
            *at += 1;
            Ok(Form::application(head, parts))
        }
        ")" | "]" => Err(Error::Read(format!("a `{token}` that closes nothing"))),
        other => Ok(atom(other)),
    }
}

impl Display for Form {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Form::Symbol(name) => write!(f, "{name}"),
            Form::Variable(name) => write!(f, "?{name}"),
            Form::Blank => write!(f, "_"),
            Form::Keyword(name) => write!(f, ":{name}"),
            Form::Text(text) => {
                write!(f, "\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
            }
            Form::Number(number) => write!(f, "{number}"),
            Form::Ellipsis => write!(f, "..."),
            Form::Rest(name) => write!(f, "...?{name}"),
            Form::Vector(items) => {
                write!(f, "[")?;
                for (at, item) in items.iter().enumerate() {
                    if at > 0 {
                        write!(f, " ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, "]")
            }
            Form::Application(head, parts) => {
                write!(f, "({head}")?;
                for part in parts {
                    match part {
                        Part::Bare(form) => write!(f, " {form}")?,
                        Part::Labelled(label, form) => write!(f, " {label}: {form}")?,
                    }
                }
                write!(f, ")")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_reads_and_prints_a_form_the_same() {
        let text = r#"(rule description: "adds one" (assert! (xyz.counter ?c count: (+ ?n : 1))) (when (increment (xyz.counter ?c)) (xyz.counter ?c count: (:natural ?n))))"#;
        let forms = read(text).unwrap();
        assert_eq!(forms.len(), 1);
        assert_eq!(forms[0].to_string(), text);
    }

    #[test]
    fn it_tells_a_label_from_an_entity_and_a_keyword() {
        let forms = read("(xyz.todo todo:1 list: list:home state: :open)").unwrap();
        assert_eq!(
            forms[0].labelled(),
            vec![
                ("list", &Form::symbol("list:home")),
                ("state", &Form::Keyword("open".into()))
            ]
        );
        assert_eq!(forms[0].bare(), vec![&Form::symbol("todo:1")]);
    }

    #[test]
    fn it_tells_a_domain_from_a_name_by_its_spelling() {
        assert_eq!(Form::symbol("xyz.todo").kind(), Some(Kind::Domain));
        assert_eq!(Form::symbol("todo").kind(), Some(Kind::Name));
        assert_eq!(Form::symbol("list:home").kind(), Some(Kind::Entity));
        assert_eq!(Form::symbol("<=").kind(), Some(Kind::Operator));
        assert_eq!(Form::symbol("xyz.todo/text").kind(), Some(Kind::Attribute));
    }

    #[test]
    fn it_skips_comments_and_refuses_what_does_not_close() {
        assert_eq!(read("; nothing\n(a) ; more").unwrap().len(), 1);
        assert!(read("(a (b)").is_err());
        assert!(read(")").is_err());
    }
}
