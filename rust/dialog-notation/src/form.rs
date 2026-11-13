//! Forms: the notation as data.
//!
//! A form is an atom or an application. An application has a head and
//! parts; a part is bare or labelled, `label: part`. Reading a form
//! gives it no meaning: `(todo ?t text: "x")` is a head named `todo`, a
//! variable and a labelled text, nothing more.

use std::fmt::{self, Display, Formatter};

use crate::Error;
use crate::written::{Comment, Point, Span};

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
    /// `{key value}` and `{label: value}`: entries in the order written,
    /// and `...?name` after them, the entries a pattern does not name.
    Map(Vec<(Form, Form)>, Option<String>),
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

impl Kind {
    /// The kind of thing the symbol `name` names.
    pub fn of(name: &str) -> Kind {
        if !name.is_empty() && name.chars().all(|c| OPERATOR.contains(c)) {
            Kind::Operator
        } else if name.contains(':') {
            Kind::Entity
        } else if name.contains('/') {
            Kind::Attribute
        } else if name.contains('.') {
            Kind::Domain
        } else {
            Kind::Name
        }
    }
}

impl Form {
    /// An application of `head` to `parts`.
    pub fn application(head: Form, parts: Vec<Part>) -> Self {
        Form::Application(Box::new(head), parts)
    }

    /// The label a map's key is written as, where it can be one: the
    /// name of a symbol that is not an entity or an operator.
    pub fn label(&self) -> Option<&str> {
        match (self, self.kind()) {
            (Form::Symbol(name), Some(Kind::Name | Kind::Domain | Kind::Attribute)) => Some(name),
            _ => None,
        }
    }

    /// Every part of an application as it was written.
    pub fn parts(&self) -> &[Part] {
        self.as_application()
            .map(|(_, parts)| parts)
            .unwrap_or_default()
    }

    /// The part labelled `of:`, where no part is written without a
    /// label: the primary, spelled out.
    pub(crate) fn of(&self) -> Option<&Form> {
        let mut bare = self
            .parts()
            .iter()
            .filter(|part| matches!(part, Part::Bare(form) if !matches!(form, Form::Rest(_))));
        if bare.next().is_some() {
            return None;
        }
        self.parts().iter().find_map(|part| match part {
            Part::Labelled(label, form) if label == "of" => Some(form),
            _ => None,
        })
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
        match self {
            Form::Symbol(name) => Some(Kind::of(name)),
            _ => None,
        }
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

    /// The parts of an application that stand without a label, a
    /// `...?rest` left out. The part with no label may also be written
    /// `of:`, anywhere, so where none stands without a label the part
    /// labelled `of:` is the one.
    pub fn bare(&self) -> Vec<&Form> {
        if let Some(primary) = self.of() {
            return vec![primary];
        }
        self.parts()
            .iter()
            .filter_map(|part| match part {
                Part::Bare(Form::Rest(_)) | Part::Labelled(..) => None,
                Part::Bare(form) => Some(form),
            })
            .collect()
    }

    /// The labelled parts of an application, in the order written,
    /// without an `of:` that is its primary.
    pub fn labelled(&self) -> Vec<(&str, &Form)> {
        let primary = self.of().is_some();
        self.parts()
            .iter()
            .filter_map(|part| match part {
                Part::Labelled(label, _) if primary && label == "of" => None,
                Part::Labelled(label, form) => Some((label.as_str(), form)),
                Part::Bare(_) => None,
            })
            .collect()
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
    let (forms, _, misread) = read_marked(text);
    match misread.into_iter().next() {
        Some((error, _)) => Err(error),
        None => Ok(forms.into_iter().map(|(form, _)| form).collect()),
    }
}

/// What in a text is not forms, and where it is.
pub(crate) type Misread = (Error, Span);

/// What reading a form leaves out of it: where it is, how its token was
/// spelled, and the comments above it. One a form, in the form's shape.
#[derive(Clone, Debug, Default)]
pub(crate) struct Mark {
    /// Where the form is.
    pub span: Span,
    /// An atom's token as written. Empty for anything in brackets.
    pub raw: String,
    /// The comments above the form.
    pub comments: Vec<Comment>,
    /// The comments before its closing bracket.
    pub closing: Vec<Comment>,
    /// The comment that ends the line the form ends on.
    pub after: Option<Comment>,
    /// A mark a form inside: an application's head and then its parts,
    /// a vector's items, a map's keys and values in turn.
    pub parts: Vec<Mark>,
    /// For an application, a part at a time: where its label is and the
    /// comments above the label, when it has one.
    pub labels: Vec<Option<(Point, Vec<Comment>)>>,
}

/// The comments after the last form of a text, and where the text ends.
#[derive(Clone, Debug, Default)]
pub(crate) struct Tail {
    pub comments: Vec<Comment>,
    pub end: Point,
}

/// Every form in `text`, in order, each with its mark, and everything
/// in it that is not forms. Reading goes on past what is not a form: the
/// top-level form it is in is left out, and what follows it is read.
pub(crate) fn read_marked(text: &str) -> (Vec<(Form, Mark)>, Tail, Vec<Misread>) {
    let (tokens, tail, unended) = tokens(text);
    let mut at = 0;
    let mut forms = Vec::new();
    let mut misread = Vec::new();
    while at < tokens.len() {
        let start = at;
        match form(&tokens, &mut at) {
            Ok(read) => forms.push(read),
            Err(not) => {
                misread.push(not);
                // Reading goes on after the form the refusal is in: at
                // the token after the bracket that closes what the
                // first token opened, or after the first token when it
                // opened nothing.
                let mut depth = 0i32;
                at = tokens.len();
                for (index, token) in tokens.iter().enumerate().skip(start) {
                    match token.text.as_str() {
                        "(" | "[" | "{" => depth += 1,
                        ")" | "]" | "}" => depth -= 1,
                        _ => {}
                    }
                    if depth <= 0 {
                        at = index + 1;
                        break;
                    }
                }
            }
        }
    }
    misread.extend(unended);
    (forms, tail, misread)
}

/// A token: a bracket, a text literal or a run of anything else.
struct Token {
    /// What it says: itself, or for a text literal `"` and then the
    /// text, its escapes read.
    text: String,
    /// The token as written.
    raw: String,
    span: Span,
    /// The comments between the token before it and it, but for one
    /// that ends the line a form ends on.
    comments: Vec<Comment>,
    /// The comment after it on its line, where the token ends a form.
    after: Option<Comment>,
}

/// The tokens of `text`, each with where it is. A `;` starts a comment
/// that runs to the end of its line. A comment that ends a line is about
/// the form that ends before it on that line, and is kept on that form's
/// last token. Any other is about the form below it, and is kept on the
/// token below it.
///
/// A text that never ends is the last thing read: it is said, and the
/// tokens before it are given.
fn tokens(text: &str) -> (Vec<Token>, Tail, Option<Misread>) {
    let mut tokens = Vec::new();
    let mut comments = Vec::new();
    let mut chars = text.chars().peekable();
    let mut here = Point::default();
    let step = |c: char, here: &mut Point| {
        if c == '\n' {
            here.line += 1;
            here.column = 0;
        } else {
            here.column += 1;
        }
    };
    while let Some(&next) = chars.peek() {
        let start = here;
        let mut raw = String::new();
        let mut take = |chars: &mut std::iter::Peekable<std::str::Chars>, here: &mut Point| {
            let c = chars.next()?;
            step(c, here);
            raw.push(c);
            Some(c)
        };
        let said = match next {
            c if c.is_whitespace() => {
                take(&mut chars, &mut here);
                continue;
            }
            ';' => {
                while chars.peek().is_some_and(|&c| c != '\n') {
                    take(&mut chars, &mut here);
                }
                let comment = Comment {
                    text: raw.trim_end().to_string(),
                    at: start,
                };
                // A token that opens a form or labels a part ends no
                // form, so a comment after it is about what is below.
                match tokens.last_mut() {
                    Some(Token {
                        text, span, after, ..
                    }) if span.end.line == start.line
                        && after.is_none()
                        && !matches!(text.as_str(), "(" | "[" | "{")
                        && label(text).is_none() =>
                    {
                        *after = Some(comment);
                    }
                    _ => comments.push(comment),
                }
                continue;
            }
            '(' | ')' | '[' | ']' | '{' | '}' => {
                take(&mut chars, &mut here);
                next.to_string()
            }
            '"' => {
                take(&mut chars, &mut here);
                let mut literal = String::from('"');
                let mut ended = false;
                while let Some(c) = take(&mut chars, &mut here) {
                    match c {
                        '"' => {
                            ended = true;
                            break;
                        }
                        '\\' => match take(&mut chars, &mut here) {
                            Some('n') => literal.push('\n'),
                            Some(escaped) => literal.push(escaped),
                            None => break,
                        },
                        c => literal.push(c),
                    }
                }
                if !ended {
                    let unended = (
                        Error::Read("a text that never ends".into()),
                        Span { start, end: here },
                    );
                    return (
                        tokens,
                        Tail {
                            comments,
                            end: here,
                        },
                        Some(unended),
                    );
                }
                literal
            }
            _ => {
                while chars
                    .peek()
                    .is_some_and(|&c| !c.is_whitespace() && !"()[]{};\"".contains(c))
                {
                    take(&mut chars, &mut here);
                }
                String::new()
            }
        };
        tokens.push(Token {
            text: if said.is_empty() { raw.clone() } else { said },
            raw,
            span: Span { start, end: here },
            comments: std::mem::take(&mut comments),
            after: None,
        });
    }
    (
        tokens,
        Tail {
            comments,
            end: here,
        },
        None,
    )
}

/// Whether a token is a label: it ends with `:` and is not all colons
/// past the first, so `text:` and `:` are labels and `::` is not.
fn label(token: &str) -> Option<&str> {
    let name = token.strip_suffix(':')?;
    (!token.starts_with('"') && !name.ends_with(':')).then_some(name)
}

/// The atom a token is. A symbol read back from data is told apart
/// the same way, by its spelling alone.
pub(crate) fn atom(token: &str) -> Form {
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

fn form(tokens: &[Token], at: &mut usize) -> Result<(Form, Mark), Misread> {
    // Where the text ends and a form was to come: after a label, or as
    // what is applied.
    let token = tokens.get(*at).ok_or_else(|| {
        (
            Error::Read("a form that never closes".into()),
            tokens.last().map(|last| last.span).unwrap_or_default(),
        )
    })?;
    *at += 1;
    // A form that never closes is said where it opens.
    let ended = || (Error::Read("a form that never closes".into()), token.span);
    let mut mark = Mark {
        span: token.span,
        comments: token.comments.clone(),
        ..Mark::default()
    };
    // The bracket that closes what `token` opened: where the form
    // ends, and the comments no part of it is below.
    let close = |mark: &mut Mark, at: &mut usize| {
        if let Some(closer) = tokens.get(*at) {
            mark.span.end = closer.span.end;
            mark.closing = closer.comments.clone();
            mark.after = closer.after.clone();
        }
        *at += 1;
    };
    let read = match token.text.as_str() {
        "[" => {
            let mut items = Vec::new();
            while tokens.get(*at).ok_or_else(ended)?.text != "]" {
                let (item, inside) = form(tokens, at)?;
                items.push(item);
                mark.parts.push(inside);
            }
            close(&mut mark, at);
            Form::Vector(items)
        }
        "{" => {
            let mut entries = Vec::new();
            let mut rest = None;
            loop {
                let next = tokens.get(*at).ok_or_else(ended)?;
                if next.text == "}" {
                    break;
                }
                if let Some(name) = next.text.strip_prefix("...?") {
                    *at += 1;
                    mark.parts.push(Mark {
                        span: next.span,
                        raw: next.raw.clone(),
                        comments: next.comments.clone(),
                        after: next.after.clone(),
                        ..Mark::default()
                    });
                    rest = Some(name.to_string());
                    let last = tokens.get(*at).ok_or_else(ended)?;
                    if last.text != "}" {
                        return Err((
                            Error::Read("a `...?rest` stands after the entries of a struct".into()),
                            last.span,
                        ));
                    }
                    break;
                }
                let key = match label(&next.text) {
                    Some("") => {
                        return Err((Error::Read("a map key with no name".into()), next.span));
                    }
                    Some(name) => {
                        *at += 1;
                        mark.parts.push(Mark {
                            span: next.span,
                            raw: next.raw.clone(),
                            comments: next.comments.clone(),
                            after: next.after.clone(),
                            ..Mark::default()
                        });
                        Form::symbol(name)
                    }
                    None => {
                        let (key, inside) = form(tokens, at)?;
                        mark.parts.push(inside);
                        key
                    }
                };
                let (value, inside) = form(tokens, at)?;
                mark.parts.push(inside);
                entries.push((key, value));
            }
            close(&mut mark, at);
            Form::Map(entries, rest)
        }
        "(" => {
            let (head, inside) = form(tokens, at)?;
            mark.parts.push(inside);
            let mut parts = Vec::new();
            loop {
                let next = tokens.get(*at).ok_or_else(ended)?;
                if next.text == ")" {
                    break;
                }
                match label(&next.text) {
                    Some(name) => {
                        *at += 1;
                        let (part, inside) = form(tokens, at)?;
                        mark.parts.push(inside);
                        mark.labels
                            .push(Some((next.span.start, next.comments.clone())));
                        parts.push(Part::Labelled(name.to_string(), part));
                    }
                    None => {
                        let (part, inside) = form(tokens, at)?;
                        mark.parts.push(inside);
                        mark.labels.push(None);
                        parts.push(Part::Bare(part));
                    }
                }
            }
            close(&mut mark, at);
            Form::application(head, parts)
        }
        closer @ (")" | "]" | "}") => {
            return Err((
                Error::Read(format!("a `{closer}` that closes nothing")),
                token.span,
            ));
        }
        other => {
            mark.raw = token.raw.clone();
            mark.after = token.after.clone();
            atom(other)
        }
    };
    Ok((read, mark))
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
            Form::Map(entries, rest) => {
                write!(f, "{{")?;
                for (at, (key, value)) in entries.iter().enumerate() {
                    if at > 0 {
                        write!(f, " ")?;
                    }
                    match key.label() {
                        Some(label) => write!(f, "{label}: {value}")?,
                        None => write!(f, "{key} {value}")?,
                    }
                }
                if let Some(name) = rest {
                    let space = if entries.is_empty() { "" } else { " " };
                    write!(f, "{space}...?{name}")?;
                }
                write!(f, "}}")
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
        let text = r#"(rule description: "adds one" (assert! (xyz.counter ?c count: (+ ?n : 1))) (increment (xyz.counter ?c)) (xyz.counter ?c count: (:natural ?n)))"#;
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

    /// One call, two ways to write it: the part with no label is `of:`.
    #[test]
    fn it_reads_the_primary_with_or_without_its_label() {
        let bare = &read("(todo ?t state: :open)").unwrap()[0];
        let labelled = &read("(todo state: :open of: ?t)").unwrap()[0];
        for form in [bare, labelled] {
            assert_eq!(form.bare(), vec![&Form::Variable("t".into())]);
            assert_eq!(
                form.labelled(),
                vec![("state", &Form::Keyword("open".into()))]
            );
        }
        // Beside a part with no label, `of:` is a label like any other.
        let twice = &read("(fam.parent ?a of: ?b)").unwrap()[0];
        assert_eq!(twice.bare(), vec![&Form::Variable("a".into())]);
        assert_eq!(twice.labelled().len(), 1);
    }

    /// A struct is a value like 5: written as the one part with no
    /// label it is that part, and its entries are not the labelled
    /// parts of what it is passed to.
    #[test]
    fn it_reads_a_struct_as_the_part_it_is() {
        let form = &read("(expand {note: ?n})").unwrap()[0];
        let Form::Map(entries, None) = form.bare()[0] else {
            panic!("a struct");
        };
        assert_eq!(entries.len(), 1);
        assert!(form.labelled().is_empty());
        assert_ne!(form, &read("(expand note: ?n)").unwrap()[0]);
    }

    #[test]
    fn it_reads_a_map_by_label_or_by_pair() {
        let text = r#"{name: "Ada" xyz.person/age: 36 :open true [1 2] ?x}"#;
        let forms = read(text).unwrap();
        let Form::Map(entries, None) = &forms[0] else {
            panic!("a map");
        };
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].0, Form::symbol("name"));
        assert_eq!(entries[2].0, Form::Keyword("open".into()));
        assert_eq!(forms[0].to_string(), text);
    }

    /// `...?name` after a struct's entries is the entries not named. It
    /// stands after them and nowhere else.
    #[test]
    fn it_reads_the_rest_of_a_struct_after_its_entries() {
        for text in ["{author: ?who ...?others}", "{...?all}"] {
            let forms = read(text).unwrap();
            let Form::Map(_, Some(rest)) = &forms[0] else {
                panic!("a struct with a rest");
            };
            assert!(rest == "others" || rest == "all");
            assert_eq!(forms[0].to_string(), text);
        }
        assert_eq!(
            read("{...?others author: ?who}"),
            Err(Error::Read(
                "a `...?rest` stands after the entries of a struct".into()
            ))
        );
    }

    #[test]
    fn it_skips_comments_and_refuses_what_does_not_close() {
        assert_eq!(read("; nothing\n(a) ; more").unwrap().len(), 1);
        assert!(read("(a (b)").is_err());
        assert!(read("{a: 1").is_err());
        assert!(read(")").is_err());
    }
}
