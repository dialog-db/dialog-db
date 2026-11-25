//! Markup: what a query whose answer is a view says, and how a back end
//! keeps it.
//!
//! A query answers with markup when its body says `(view ?x template)`:
//!
//! ```clj
//! (query (todo/row ?t list: ?l is: ?row)
//!   (todo ?t list: ?l title: ?title)
//!   (todo/chip ?t is: (collect ?chips))
//!   (view ?row [:li ?title ?chips]))
//! ```
//!
//! The template is Hiccup: an element is `[:tag {attributes} child…]`,
//! with events inline as `:on/<type>`, each the command it posts. A
//! variable is a value the query binds, or the markup of a query it
//! collects, which stands there as children, in the order of their keys.
//! [`template`] reads one into a [`Node`].
//!
//! A [`Plan`](crate::Plan) lays a query and every query it collects out
//! in layers, each a query of its own, and the tree keeps their
//! instances from the rows of those queries as they come and go, and
//! says each change as [`Patch`]es. No value is folded. The Dialog-DB back end,
//! [`compile`](crate::compile()), gives a query whose answer is markup as
//! its plan.
//!
//! A row's values come from any writer of the branch. A template makes
//! only the elements of one flat table, `ELEMENTS`: the HTML elements of
//! sections and text, lists, tables, forms and media, and custom
//! elements, which the page defines. Every other element is refused,
//! SVG and MathML among them. Of the attributes, it refuses the shapes
//! known to let a row's values reach script: a name or local name
//! `on…` that is no `on/` event, `xml/base`, `srcdoc`, a `style` a value
//! writes, and an attribute that takes a URL (`URLS`) unless fixed text
//! settles its scheme as `http`, `https` or `mailto` or starts it with
//! a path, a query or a fragment (`/`, `?`, `#`), a constant included.
//! A `srcset` a value is written into is refused, since each of its URLs
//! follows a comma; a constant one has each of its URLs checked. Names
//! are compared in any case, a namespaced one written with `/` as the
//! page reads it, `xlink:href`, and a fixed start with a control
//! character or a space is refused. This is a refusal of known dangerous shapes and no
//! sanitizer: the back end stays the boundary. It escapes every text and
//! attribute value it writes and sets them as text and attributes, never
//! as markup. An attribute's name such as `innerHTML` passes the checks
//! here: it is safe only where the renderer sets attributes, never
//! properties of the element by the attribute's name.

use dialog_corpus::Value;

use crate::Error;
use crate::syntax::Syntax;

pub(crate) mod plan;
pub(crate) mod tree;

pub use tree::{Address, Content, Given, Patch, Posted, Rendered, Shown, Slot};
/// What keeps a view's tree from rows handed to it, for a driver of its
/// own and for the tests; [`Live::watch`](crate::live::Live::watch)
/// keeps one itself.
#[cfg(feature = "inspect")]
pub use tree::{Change, Counts, Maintainer, Row, render};

/// The variable a view not found, shown as its source, names the entity
/// it reads by where the call names none.
pub const CALLED: &str = "[view-of]";

/// A part of a template.
#[derive(Clone, Debug, PartialEq)]
pub enum Node {
    /// `[:li {…} …]`.
    Element(Element),
    /// Text, as written.
    Text(String),
    /// A value other than text: a number.
    Value(Syntax),
    /// What a condition bound this variable to, or the markup of a query
    /// collected.
    Variable(String),
}

/// An element: its tag, its attributes, the events it binds and what it
/// holds.
#[derive(Clone, Debug, PartialEq)]
pub struct Element {
    /// The tag: `li` for `:li`.
    pub tag: String,
    /// The attributes, in the order written, events apart.
    pub attributes: Vec<(String, Attribute)>,
    /// The events the element binds, `:on/<type>`, in the order written.
    pub events: Vec<Event>,
    /// What it holds, in order.
    pub children: Vec<Node>,
}

/// An attribute's value.
#[derive(Clone, Debug, PartialEq)]
pub enum Attribute {
    /// Text, as written.
    Text(String),
    /// A value other than text: a number.
    Value(Syntax),
    /// What a condition bound this variable to.
    Variable(String),
    /// Its parts joined, in order, as text children are:
    /// `:href ["/notebook/" ?n]`.
    Parts(Vec<Piece>),
    /// Present, with no value: `:draft true`.
    Present,
}

/// A part of a value written as a list.
#[derive(Clone, Debug, PartialEq)]
pub enum Piece {
    /// Text, as written.
    Text(String),
    /// A value other than text: a number.
    Value(Syntax),
    /// What a condition bound this variable to.
    Variable(String),
}

/// An event an element binds: its DOM type, the command it posts, and
/// what each field of the command is given.
#[derive(Clone, Debug, PartialEq)]
pub struct Event {
    /// The event's type: `change` for `:on/change`.
    pub kind: String,
    /// The command it posts, by its name.
    pub command: String,
    /// Each field of the command and what fills it.
    pub fields: Vec<(String, Input)>,
}

/// What fills a field of the command an event posts.
#[derive(Clone, Debug, PartialEq)]
pub enum Input {
    /// What a condition bound this variable to.
    Variable(String),
    /// A value written in the binding.
    Value(Syntax),
    /// A path into the live event, `.currentTarget.checked`: its steps.
    Path(Vec<String>),
}

fn keyword(node: &Syntax) -> Option<&str> {
    node.keyword()
}

fn refused(why: String) -> Error {
    Error::Refused(why)
}

/// The node a template is: `[:li {:class ?s} ?title]`.
pub fn template(tree: &Syntax) -> Result<Node, Error> {
    match node(tree)? {
        element @ Node::Element(_) => Ok(element),
        other => Err(refused(format!(
            "{tree} as a template: a view's markup is an element, `[:li …]`, not {other:?}"
        ))),
    }
}

/// Every variable a template holds, events included, each once, in the
/// order written.
pub fn variables(node: &Node) -> Vec<String> {
    fn add(name: &str, into: &mut Vec<String>) {
        if !into.iter().any(|known| known == name) {
            into.push(name.to_string());
        }
    }
    fn walk(node: &Node, into: &mut Vec<String>) {
        match node {
            Node::Variable(name) => add(name, into),
            Node::Element(element) => {
                for (_, attribute) in &element.attributes {
                    match attribute {
                        Attribute::Variable(name) => add(name, into),
                        Attribute::Parts(pieces) => {
                            for piece in pieces {
                                if let Piece::Variable(name) = piece {
                                    add(name, into);
                                }
                            }
                        }
                        _ => {}
                    }
                }
                for event in &element.events {
                    for (_, input) in &event.fields {
                        if let Input::Variable(name) = input {
                            add(name, into);
                        }
                    }
                }
                for child in &element.children {
                    walk(child, into);
                }
            }
            Node::Text(_) | Node::Value(_) => {}
        }
    }
    let mut names = Vec::new();
    walk(node, &mut names);
    names
}

/// Every event a template binds, in the order written.
pub fn events(node: &Node) -> Vec<&Event> {
    fn walk<'a>(node: &'a Node, into: &mut Vec<&'a Event>) {
        if let Node::Element(element) = node {
            into.extend(&element.events);
            element.children.iter().for_each(|child| walk(child, into));
        }
    }
    let mut found = Vec::new();
    walk(node, &mut found);
    found
}

/// A part of a template.
fn node(tree: &Syntax) -> Result<Node, Error> {
    Ok(match tree {
        Syntax::Tuple(items) => {
            let Some((head, rest)) = items.split_first() else {
                return Err(refused(
                    "`[]` in a template: an element is `[:tag …]`".into(),
                ));
            };
            let tag = keyword(head)
                .ok_or_else(|| {
                    refused(format!(
                        "{tree} in a template: an element starts with its tag, `[:li …]`"
                    ))
                })?
                .to_string();
            if !made(&tag) {
                let said = format!(
                    "{tree}: [:{tag} …] is no element a view makes: a view makes the HTML \
                     elements of text, lists, tables, forms and media, and custom elements"
                );
                // Only an element that runs or loads what it holds, or
                // moves what every URL is read against, is a way to script.
                if scripted(tree) {
                    return Err(Error::Unsafe(said));
                }
                // An attribute of it a row's values reach script by is
                // said as one.
                if let Some(Syntax::Map(entries)) = rest.first() {
                    for (key, value) in entries {
                        if let Syntax::Reference(name) = key
                            && let Ok(attribute) = attribute(name, value)
                        {
                            safe(&tag, name, &attribute)?;
                        }
                    }
                }
                return Err(refused(said));
            }
            // A map right after the tag is the element's attributes, as in
            // Hiccup.
            let (map, children) = match rest.split_first() {
                Some((Syntax::Map(entries), children)) => (entries.as_slice(), children),
                _ => (&[][..], rest),
            };
            // A map that names a view is how a view was nested by
            // reference, `[:ul {:view :row :of ?t}]`.
            if map
                .iter()
                .any(|(key, _)| matches!(key, Syntax::Reference(name) if name == "view"))
            {
                return Err(refused(format!(
                    "{tree}: a view nested by reference is a query collected, `(todo/row _ is: \
                     (collect ?rows))`, its variable put where it stands"
                )));
            }
            let mut element = Element {
                tag,
                attributes: Vec::new(),
                events: Vec::new(),
                children: children.iter().map(node).collect::<Result<_, _>>()?,
            };
            for (key, value) in map {
                let Syntax::Reference(name) = key else {
                    return Err(refused(format!(
                        "{key} as an attribute of [:{} …]: an attribute's name is a keyword",
                        element.tag
                    )));
                };
                match name.strip_prefix("on/") {
                    Some(kind) => element.events.push(event(kind, value)?),
                    None => {
                        settable(&element.tag, name)?;
                        let attribute = attribute(name, value)?;
                        safe(&element.tag, name, &attribute)?;
                        element.attributes.push((name.clone(), attribute));
                    }
                }
            }
            Node::Element(element)
        }
        Syntax::Map(_) => {
            return Err(refused(format!(
                "{tree} in a template: a view nested by reference is a query collected, \
                 `(todo/row _ is: (collect ?rows))`, its variable put where it stands"
            )));
        }
        Syntax::Application(..) if tree.applies() == Some("view") => {
            return Err(refused(format!(
                "{tree} in a template: a view is a query whose answer is markup, and one in a \
                 template is that query collected, `(todo/chip ?t is: (collect ?chips))`, its \
                 variable put where it stands"
            )));
        }
        Syntax::Variable(name) => Node::Variable(name.clone()),
        Syntax::Constant(Value::String(text)) => Node::Text(text.clone()),
        Syntax::Constant(Value::Boolean(_)) => {
            return Err(Error::Unsupported(format!(
                "{tree} in a template: the design has no boolean term"
            )));
        }
        Syntax::Constant(_) => Node::Value(tree.clone()),
        other => {
            return Err(refused(format!(
                "{other} in a template: a template holds elements, text and variables"
            )));
        }
    })
}

/// The HTML elements a view makes: those of sections and text, lists,
/// tables, forms and media. Nothing that runs, loads or embeds what it
/// names (`script`, `iframe`, `object`, `embed`, `frame`), nothing that
/// changes how the page around it is read (`style`, `link`, `meta`,
/// `base`, `html`, `head`, `body`), no `template`, and no SVG or MathML,
/// whose animation elements set any attribute of another: every element
/// not here is refused.
const ELEMENTS: [&str; 94] = [
    // sections and grouping
    "div",
    "p",
    "section",
    "article",
    "aside",
    "header",
    "footer",
    "main",
    "nav",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "hgroup",
    "address",
    "blockquote",
    "figure",
    "figcaption",
    "hr",
    "pre",
    "details",
    "summary",
    "dialog",
    "search",
    // text
    "span",
    "a",
    "em",
    "strong",
    "small",
    "s",
    "cite",
    "q",
    "dfn",
    "abbr",
    "ruby",
    "rt",
    "rp",
    "data",
    "time",
    "code",
    "var",
    "samp",
    "kbd",
    "sub",
    "sup",
    "i",
    "b",
    "u",
    "mark",
    "bdi",
    "bdo",
    "br",
    "wbr",
    "ins",
    "del",
    // lists
    "ul",
    "ol",
    "li",
    "dl",
    "dt",
    "dd",
    "menu",
    // tables
    "table",
    "caption",
    "colgroup",
    "col",
    "thead",
    "tbody",
    "tfoot",
    "tr",
    "th",
    "td",
    // forms
    "form",
    "label",
    "input",
    "button",
    "select",
    "datalist",
    "optgroup",
    "option",
    "textarea",
    "output",
    "progress",
    "meter",
    "fieldset",
    "legend",
    // media
    "img",
    "picture",
    "source",
    "audio",
    "video",
    "track",
];

/// The names with a `-` that are SVG's and MathML's, not custom
/// elements.
const RESERVED: [&str; 8] = [
    "annotation-xml",
    "color-profile",
    "font-face",
    "font-face-src",
    "font-face-uri",
    "font-face-format",
    "font-face-name",
    "missing-glyph",
];

/// The elements not made that would let a row's values reach script:
/// each runs or loads what it holds, or, `base`, changes what every URL
/// on the page is read against.
/// `style`, `link` and `meta` load or move to what they name; the
/// animation elements and `foreignObject` set any attribute of another,
/// or hold HTML, inside SVG (gaps row 40).
const SCRIPTED: [&str; 18] = [
    "script",
    "iframe",
    "frame",
    "frameset",
    "object",
    "embed",
    "applet",
    "portal",
    "base",
    "style",
    "link",
    "meta",
    "set",
    "animate",
    "animatemotion",
    "animatetransform",
    "foreignobject",
    "noscript",
];

/// Whether `tree` is, or holds, an element of [`SCRIPTED`]: SVG or
/// MathML holding one is a way to script, and alone is only not made.
fn scripted(tree: &Syntax) -> bool {
    let mut open = vec![tree];
    while let Some(part) = open.pop() {
        if let Syntax::Tuple(items) = part {
            if let Some(tag) = items.first().and_then(Syntax::keyword)
                && SCRIPTED.contains(&tag.to_ascii_lowercase().as_str())
            {
                return true;
            }
            open.extend(items.iter());
        }
    }
    false
}

/// Whether a view makes the element `tag`: one of [`ELEMENTS`], or a
/// custom element, a lowercase name with a `-` that SVG and MathML do
/// not reserve, which the page defines.
fn made(tag: &str) -> bool {
    let tag = tag.trim().to_ascii_lowercase();
    let custom = tag.starts_with(|first: char| first.is_ascii_lowercase())
        && tag.contains('-')
        && tag
            .chars()
            .all(|part| part.is_ascii_lowercase() || part.is_ascii_digit() || part == '-')
        && !RESERVED.contains(&tag.as_str());
    ELEMENTS.contains(&tag.as_str()) || custom
}

/// The attributes whose value is a URL a browser follows or loads, older
/// HTML's among them.
const URLS: [&str; 20] = [
    "href",
    "xlink:href",
    "src",
    "srcset",
    "action",
    "formaction",
    "poster",
    "data",
    "ping",
    "background",
    "cite",
    "manifest",
    "lowsrc",
    "dynsrc",
    "longdesc",
    "usemap",
    "codebase",
    "classid",
    "profile",
    "archive",
];

/// The schemes a URL may name: the web's and mail's.
const SCHEMES: [&str; 3] = ["http", "https", "mailto"];

/// Whether the fixed start of a URL settles that it names no other
/// scheme than the web's or mail's, whatever follows it. `whole` says
/// the start is the whole URL. A start with a control character or a
/// space in it is refused outright, the classic ways round a check.
fn settled(start: &str, whole: bool) -> bool {
    if start.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return false;
    }
    match start.find([':', '/', '?', '#']) {
        // A scheme is what comes before a `:` with no `/`, `?` or `#`
        // before it.
        Some(at) if start[at..].starts_with(':') => {
            SCHEMES.contains(&start[..at].to_ascii_lowercase().as_str())
        }
        // A path, a query or a fragment comes first: no scheme follows.
        Some(_) => true,
        // No sign yet: what follows could still make a scheme, unless
        // nothing follows.
        None => whole,
    }
}

/// Refuse an attribute through which a row's values could reach script:
/// one named `on…`, which is an inline handler, `srcdoc`, which is a
/// document, a `style` a value writes, and a URL whose scheme is not
/// settled as the web's or mail's by fixed text. Names are compared in
/// any case. A value is written into the others as text, which the back
/// end escapes.
fn safe(tag: &str, name: &str, attribute: &Attribute) -> Result<(), Error> {
    // A namespaced name is written with `/`, `:xlink/href`, and compared
    // as the page reads it, `xlink:href`; its local name is what follows.
    let lowered = name.trim().to_ascii_lowercase().replace('/', ":");
    let local = lowered.rsplit(':').next().unwrap_or_default();
    let refuse = |why: &str| Err(Error::Unsafe(format!("[:{tag} {{:{name} …}}]: {why}")));
    let written = |attribute: &Attribute| match attribute {
        Attribute::Variable(_) => true,
        Attribute::Parts(pieces) => pieces
            .iter()
            .any(|piece| matches!(piece, Piece::Variable(_))),
        _ => false,
    };
    if lowered.starts_with("on") || local.starts_with("on") {
        return refuse("an attribute named on… is a handler: an event is :on/<type>");
    }
    if lowered == "xml:base" {
        return refuse("xml:base changes what every URL under it is read against");
    }
    if lowered == "srcdoc" {
        return refuse("srcdoc is a document of its own");
    }
    if lowered == "style" && written(attribute) {
        return refuse("a style a value writes");
    }
    // A srcset is URLs separated by commas, so a value anywhere in it
    // could start one.
    if lowered == "srcset" && written(attribute) {
        return refuse("a srcset a value is written into: each of its URLs follows a comma");
    }
    if URLS.contains(&lowered.as_str()) {
        let safe = match attribute {
            // Each candidate of a srcset is a URL and what it is drawn
            // at, after a comma.
            Attribute::Text(text) if lowered == "srcset" => text.split(',').all(|candidate| {
                let candidate = candidate.trim_start_matches([' ', '\t', '\n', '\r', '\x0c']);
                settled(
                    candidate
                        .split([' ', '\t', '\n', '\r', '\x0c'])
                        .next()
                        .unwrap_or_default(),
                    true,
                )
            }),
            Attribute::Text(text) => settled(text, true),
            Attribute::Parts(pieces) => match pieces.first() {
                Some(Piece::Text(text)) => settled(text, pieces.len() == 1),
                _ => false,
            },
            Attribute::Variable(_) | Attribute::Value(_) => false,
            Attribute::Present => true,
        };
        if !safe {
            return refuse(
                "a URL whose scheme fixed text does not settle as http, https or mailto: \
                 give it a fixed start, [\"/notebook/\" ?n]",
            );
        }
    }
    Ok(())
}

/// An attribute's value.
///
/// A list is its parts joined, as text children are, `:href ["/n/" ?n]`,
/// and `true` is the attribute present with no value, `:draft true`:
/// both the managing agent's readings. `false` is refused: an attribute
/// that is not there is left out.
fn attribute(name: &str, value: &Syntax) -> Result<Attribute, Error> {
    Ok(match value {
        Syntax::Constant(Value::String(text)) => Attribute::Text(text.clone()),
        Syntax::Variable(variable) => Attribute::Variable(variable.clone()),
        Syntax::Constant(Value::Boolean(true)) => Attribute::Present,
        Syntax::Constant(Value::Boolean(false)) => {
            return Err(refused(format!(
                ":{name} false: an attribute that is not there is left out, not written `false`"
            )));
        }
        Syntax::Tuple(items) => Attribute::Parts(pieces(name, items)?),
        Syntax::Constant(_) => Attribute::Value(value.clone()),
        other => {
            return Err(refused(format!(
                ":{name} {other}: an attribute's value is text, a value, a variable, a list of \
                 those or `true`"
            )));
        }
    })
}

/// The parts of a value written as a list: text, values and variables.
fn pieces(name: &str, items: &[Syntax]) -> Result<Vec<Piece>, Error> {
    items
        .iter()
        .map(|item| match item {
            Syntax::Constant(Value::String(text)) => Ok(Piece::Text(text.clone())),
            Syntax::Variable(variable) => Ok(Piece::Variable(variable.clone())),
            Syntax::Constant(Value::Boolean(_)) => Err(refused(format!(
                ":{name}: {item} is no part of a value; its parts are text, values and variables"
            ))),
            Syntax::Constant(_) => Ok(Piece::Value(item.clone())),
            other => Err(refused(format!(
                ":{name}: {other} is no part of a value; its parts are text, values and variables"
            ))),
        })
        .collect()
}

/// Refuse an attribute name a page cannot set: a letter or `_`, then
/// letters, digits, `-`, `_` and `.`, with at most one `/` between two
/// such parts, its namespace, `:xlink/href`.
fn settable(tag: &str, name: &str) -> Result<(), Error> {
    let part = |part: &str| {
        part.chars()
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
            && part
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    };
    let parts: Vec<&str> = name.split('/').collect();
    match parts.len() <= 2 && parts.iter().all(|each| part(each)) {
        true => Ok(()),
        false => Err(refused(format!(
            "[:{tag} {{:{name} …}}]: `:{name}` is no attribute name: a letter or `_`, then \
             letters, digits, `-`, `_` and `.`, and at most one `/` between two such parts, \
             its namespace, `:xlink/href`"
        ))),
    }
}

/// `:on/<kind> (command field: term …)`.
fn event(kind: &str, value: &Syntax) -> Result<Event, Error> {
    // A kind is free-form, as a custom DOM event's is, but says one.
    if kind.is_empty() {
        return Err(refused(format!(
            ":on/ {value}: an event names its kind, `:on/click`"
        )));
    }
    if crate::syntax::named(kind) == crate::syntax::Named::Reserved {
        return Err(Error::Reserved(format!("on/{kind}")));
    }
    let (Some(command), Some(operand)) = (value.applies(), value.operand()) else {
        return Err(refused(format!(
            ":on/{kind} {value}: an event binds the command it posts, `(todo/toggle todo: ?t)`"
        )));
    };
    if !operand.items.is_empty() {
        return Err(refused(format!(
            ":on/{kind} {value}: the command an event posts takes its fields by label"
        )));
    }
    let fields = operand
        .terms
        .iter()
        .map(|(label, term)| {
            let input = match term {
                Syntax::Variable(name) => Input::Variable(name.clone()),
                // A bare `.` is the empty path (gaps row 49); any other
                // has a step between each two `.`.
                Syntax::Reference(path) if path == "." => Input::Path(Vec::new()),
                Syntax::Reference(path) if path.starts_with('.') => {
                    let steps: Vec<String> = path[1..].split('.').map(str::to_string).collect();
                    if steps.iter().any(String::is_empty) {
                        return Err(refused(format!(
                            ":on/{kind}: {label}: `{path}` has an empty step: a path into the \
                             event is its steps, each after a `.`, `.target.value`"
                        )));
                    }
                    Input::Path(steps)
                }
                Syntax::Constant(_) => Input::Value(term.clone()),
                other => {
                    return Err(refused(format!(
                        ":on/{kind}: {label}: {other} is no variable, value or path into the event"
                    )));
                }
            };
            Ok((label.clone(), input))
        })
        .collect::<Result<_, Error>>()?;
    Ok(Event {
        kind: kind.to_string(),
        command: command.to_string(),
        fields,
    })
}

#[cfg(test)]
mod safety {
    use super::*;

    fn read(text: &str) -> Result<Node, Error> {
        template(&crate::parse(text)?.remove(0))
    }

    /// An attribute's name is one a page can set: a letter or `_`, then
    /// letters, digits, `-`, `_` and `.`, and at most one `/` between two
    /// such parts, its namespace. One that is not is refused, naming it.
    /// (One with a `:` is an entity to the reader, and no keyword at all.)
    #[test]
    fn it_refuses_an_attribute_name_a_page_cannot_set() {
        for name in ["a//b", "a/", "a/b/c", "-a", "a@b"] {
            let text = format!("[:span {{:{name} \"x\"}}]");
            let refused = read(&text);
            assert!(
                matches!(&refused, Err(Error::Refused(said)) if said.contains(&format!(":{name}"))
                    && said.contains("no attribute name")),
                "{text}: {refused:?}"
            );
        }
        for name in [
            "title",
            "data-x",
            "aria-label",
            "xlink/href",
            "x.y",
            "xyz.t/name",
            "_a",
        ] {
            let text = format!("[:span {{:{name} \"x\"}}]");
            assert!(read(&text).is_ok(), "{text}: {:?}", read(&text));
        }
    }

    /// A path into the event has a step between each two `.`: one with an
    /// empty step inside it or before another is refused, naming the path
    /// as written. A bare `.` is the empty path today, which is the
    /// maintainer's to decide (gaps row 49).
    #[test]
    fn it_refuses_an_event_path_with_an_empty_step() {
        for path in ["..x", ".a..b", ".a."] {
            let text = format!("[:button {{:on/click (todo/go todo: {path})}}]");
            let refused = read(&text);
            assert!(
                matches!(&refused, Err(Error::Refused(said)) if said.contains(&format!("`{path}`"))
                    && said.contains("empty step")),
                "{text}: {refused:?}"
            );
        }
        let Ok(Node::Element(element)) = read("[:button {:on/click (todo/go todo: .)}]") else {
            panic!("a bare `.` is read");
        };
        assert_eq!(
            element.events[0].fields,
            vec![("todo".to_string(), Input::Path(Vec::new()))]
        );
        assert!(read("[:button {:on/click (todo/go todo: .target.value)}]").is_ok());
    }

    /// A word with a `:` is read as an entity, so an event kind that
    /// holds one cannot be written (gaps row 52): it is refused saying so
    /// in the author's terms, not in the URI parser's.
    #[test]
    fn it_refuses_an_event_kind_with_a_colon_in_the_authors_terms() {
        let refused = read("[:button {:on/htmx:after-swap (todo/go todo: ?t)}]");
        assert!(
            matches!(&refused, Err(Error::Refused(said))
                if said.contains("`on/htmx:after-swap` holds a `:`")
                    && !said.contains("URI")
                    && !said.contains("relative URL")),
            "{refused:?}"
        );
    }

    /// An element a view does not make is refused saying so; only one
    /// that runs or loads what it holds, or changes what every URL is read
    /// against, is refused as a way for a row's values to reach script.
    /// SVG and MathML are not made (gaps row 40).
    #[test]
    fn it_says_why_an_element_is_not_made() {
        for tag in ["svg", "math", "marquee", "blink"] {
            let refused = read(&format!("[:{tag}]"));
            assert!(
                matches!(&refused, Err(Error::Refused(said))
                    if said.contains("is no element a view makes") && !said.contains("script")),
                "{tag}: {refused:?}"
            );
        }
        for tag in [
            "script", "iframe", "frame", "frameset", "object", "embed", "applet", "base",
        ] {
            let refused = read(&format!("[:{tag}]"));
            assert!(
                matches!(&refused, Err(Error::Unsafe(_))),
                "{tag}: {refused:?}"
            );
        }
    }

    /// An event's kind is free-form, as a custom DOM event's is, but
    /// says something: an empty one, `:on/`, and one under the notation's
    /// namespace are refused.
    #[test]
    fn it_refuses_an_empty_or_reserved_event_kind() {
        for kind in ["", "dialog.n", "dialog.n.x"] {
            let text = format!("[:button {{:on/{kind} (todo/go todo: ?t)}}]");
            assert!(
                matches!(read(&text), Err(Error::Refused(_) | Error::Reserved(_))),
                "{text}: {:?}",
                read(&text)
            );
        }
        for kind in ["click", "my-Event.thing", "x_y"] {
            let text = format!("[:button {{:on/{kind} (todo/go todo: ?t)}}]");
            assert!(read(&text).is_ok(), "{text}: {:?}", read(&text));
        }
    }

    /// A template whose values could reach script is refused, naming the
    /// part: an element that runs what it holds, an attribute named
    /// `on…` that is no `on/` event, `srcdoc`, a `style` a value writes,
    /// and a URL a variable writes from its start. A URL with a fixed
    /// start that cannot turn into a scheme, and text and attributes
    /// otherwise, are taken.
    #[test]
    fn it_refuses_a_template_that_lets_data_reach_script() {
        for (text, part) in [
            ("[:script ?title]", "script"),
            ("[:iframe {:title ?t}]", "iframe"),
            ("[:object]", "object"),
            ("[:embed {:src \"/x\"}]", "embed"),
            ("[:a {:onclick ?title}]", "onclick"),
            ("[:a {:onClick \"go()\"}]", "onClick"),
            ("[:iframe {:srcdoc ?title}]", "iframe"),
            ("[:div {:srcdoc \"<b>x</b>\"}]", "srcdoc"),
            ("[:div {:style ?css}]", "style"),
            ("[:div {:style [\"color: \" ?c]}]", "style"),
            ("[:a {:href ?url}]", "href"),
            ("[:img {:src ?url}]", "src"),
            ("[:form {:action ?url}]", "action"),
            ("[:button {:formaction ?url}]", "formaction"),
            ("[:a {:href [?scheme \"://x\"]}]", "href"),
            ("[:a {:href [\"java\" ?rest]}]", "href"),
        ] {
            assert!(
                matches!(read(text), Err(Error::Unsafe(said)) if said.contains(part)),
                "{text}: {:?}",
                read(text)
            );
        }
        for text in [
            "[:a {:href [\"/notebook/\" ?n]} ?title]",
            "[:a {:href [\"https://example.org/\" ?n]}]",
            "[:a {:href \"#top\"}]",
            "[:div {:style \"display: block;\" :class ?c :title ?t} ?text]",
            "[:button {:on/click (todo/add text: ?t)} \"Add\"]",
        ] {
            assert!(read(text).is_ok(), "{text}: {:?}", read(text));
        }
    }

    /// The rest of the shapes a row's values reach script by: a tag or an
    /// attribute's name that is no fixed text, more elements that load or
    /// run, every attribute that takes a URL, a constant URL with a
    /// scheme other than the web's or mail's, names in any case, and a
    /// fixed start with a control character or a space before the scheme.
    #[test]
    fn it_refuses_the_rest_of_the_shapes_that_reach_script() {
        for text in ["[?tag \"x\"]", "[:a {?attr \"x\"}]"] {
            assert!(matches!(read(text), Err(Error::Refused(_))), "{text}");
        }
        for (text, part) in [
            ("[:style ?css]", "style"),
            ("[:link {:rel \"stylesheet\"}]", "link"),
            ("[:meta {:content ?c}]", "meta"),
            ("[:base {:href \"/\"}]", "base"),
            ("[:frame]", "frame"),
            ("[:frameset]", "frameset"),
            ("[:applet]", "applet"),
            ("[:SCRIPT ?x]", "SCRIPT"),
            ("[:a {:HREF ?url}]", "HREF"),
            ("[:img {:srcset ?url}]", "srcset"),
            ("[:video {:poster ?url}]", "poster"),
            ("[:a {:ping ?url}]", "ping"),
            ("[:td {:background ?url}]", "background"),
            ("[:q {:cite ?url}]", "cite"),
            ("[:html {:manifest ?url}]", "html"),
            ("[:a {:href \"javascript:alert(1)\"}]", "href"),
            ("[:a {:href \"JavaScript:go()\"}]", "href"),
            ("[:a {:href \"data:text/html,x\"}]", "href"),
            ("[:a {:href \" javascript:go()\"}]", "href"),
            ("[:a {:href \"java\tscript:go()\"}]", "href"),
            ("[:a {:href [\" /x/\" ?n]}]", "href"),
            ("[:a {:href [\"notes\" ?n]}]", "href"),
        ] {
            assert!(
                matches!(read(text), Err(Error::Unsafe(said)) if said.to_lowercase().contains(&part.to_lowercase())),
                "{text}: {:?}",
                read(text)
            );
        }
        for text in [
            "[:a {:href \"notes/today.html\"}]",
            "[:a {:href \"https://example.org\"}]",
            "[:a {:href \"mailto:a@example.org\"}]",
            "[:a {:href [\"notes/\" ?n]}]",
            "[:img {:src [\"/image/\" ?n] :alt ?t}]",
        ] {
            assert!(read(text).is_ok(), "{text}: {:?}", read(text));
        }
    }

    /// A `srcset` is a list of URLs, each after a comma, so a value
    /// written anywhere in it could start one: it is refused whenever a
    /// variable is written into it, and a constant keeps the check of its
    /// start. A fixed start that is a path, a query or a fragment
    /// settles that no scheme follows.
    #[test]
    fn it_refuses_a_srcset_a_value_is_written_into() {
        for text in [
            "[:img {:srcset [\"/a.png 1x, \" ?url]}]",
            "[:img {:srcset [\"/image/\" ?n \" 1x\"]}]",
            "[:img {:srcset \"javascript:go() 1x\"}]",
            "[:img {:srcset \"/a.png 1x, javascript:go() 2x\"}]",
            "[:img {:srcset \"/a.png 1x,\u{1}javascript:go() 2x\"}]",
        ] {
            assert!(
                matches!(read(text), Err(Error::Unsafe(said)) if said.contains("srcset")),
                "{text}: {:?}",
                read(text)
            );
        }
        for text in [
            "[:img {:srcset \"/a.png 1x, /b.png 2x\"}]",
            "[:a {:href [\"/notebook/\" ?n]}]",
            "[:a {:href [\"#\" ?id]}]",
            "[:a {:href [\"?page=\" ?n]}]",
            "[:img {:src [\"/image/\" ?n]}]",
        ] {
            assert!(read(text).is_ok(), "{text}: {:?}", read(text));
        }
    }

    /// What the third review got past the denylist: an element outside
    /// the table of HTML elements a view may make (svg, math, every
    /// animation element, anything unlisted), a namespaced attribute
    /// written with `/`, `xml/base`, and the URL attributes of older
    /// HTML. One case each.
    #[test]
    fn it_refuses_what_got_past_the_list_of_elements() {
        for (text, part) in [
            (
                "[:svg [:a [:set {:attributeName \"href\" :to ?url}]]]",
                "svg",
            ),
            (
                "[:svg [:animate {:attributeName \"href\" :values ?url}]]",
                "svg",
            ),
            ("[:div [:set {:attributeName \"href\" :to ?url}]]", "set"),
            (
                "[:div [:animate {:attributeName \"href\" :values ?url}]]",
                "animate",
            ),
            ("[:a {:xlink/href ?u}]", "xlink/href"),
            ("[:a {:xml/base ?u}]", "xml/base"),
            ("[:a {:xml/base \"https://example.org/\"}]", "xml/base"),
            ("[:img {:lowsrc ?u}]", "lowsrc"),
            ("[:img {:dynsrc ?u}]", "dynsrc"),
            ("[:img {:longdesc ?u}]", "longdesc"),
            ("[:img {:usemap ?u}]", "usemap"),
            ("[:div {:codebase ?u}]", "codebase"),
            ("[:div {:classid ?u}]", "classid"),
            ("[:div {:profile ?u}]", "profile"),
            ("[:div {:archive ?u}]", "archive"),
            ("[:a {:xlink/onclick \"x\"}]", "xlink/onclick"),
        ] {
            assert!(
                matches!(read(text), Err(Error::Unsafe(said)) if said.contains(part)),
                "{text}: {:?}",
                read(text)
            );
        }
        // An element only not made says so, and nothing of script.
        for text in [
            "[:math [:mi ?x]]",
            "[:marquee ?x]",
            "[:template [:p ?x]]",
            "[:font-face]",
        ] {
            assert!(
                matches!(read(text), Err(Error::Refused(said)) if said.contains("is no element a view makes")),
                "{text}: {:?}",
                read(text)
            );
        }
        // The elements the fixtures and tonk's tour make, custom ones
        // among them.
        for text in [
            "[:div [:section [:article [:h2 ?t] [:p ?x] [:ul [:li [:span ?s]]] [:ol [:li ?y]]]]]",
            "[:a {:href [\"/n/\" ?n]} [:img {:src [\"/i/\" ?n] :alt ?t}]]",
            "[:form [:button {:on/click (todo/add text: ?t)} \"Add\"]]",
            "[:table [:tr [:td ?x]]]",
            "[:video {:poster [\"/p/\" ?n]}]",
            "[:tonk-notebook [:notebook-save] [:view-source ?s] [:tonk-table] [:tonk-notebook-index]]",
        ] {
            assert!(read(text).is_ok(), "{text}: {:?}", read(text));
        }
    }
}
