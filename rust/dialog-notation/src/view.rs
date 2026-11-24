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
//! A row's values come from any writer of the branch. A template is
//! refused in the shapes known to let them reach script: an element that
//! runs, loads or changes how the page is read (`RUNS`: `script`,
//! `iframe`, `object`, `embed`, `style`, `link`, `meta`, `base`,
//! `frame`, `frameset`, `applet`), an attribute named `on…` that is no
//! `on/` event, `srcdoc`, a `style` a value writes, and an attribute that
//! takes a URL (`URLS`) unless fixed text settles its scheme as `http`,
//! `https` or `mailto`, a constant included. Names are compared in any
//! case, and a fixed start with a control character or a space is
//! refused. This is a refusal of known dangerous shapes and no
//! sanitizer: the back end stays the boundary. It escapes every text and
//! attribute value it writes and sets them as text and attributes, never
//! as markup.

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
            if RUNS.contains(&tag.trim().to_ascii_lowercase().as_str()) {
                return Err(Error::Unsafe(format!(
                    "{tree}: a [:{tag} …] runs what it holds"
                )));
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

/// The elements that run or load what they hold or name, or change how
/// the page around them is read: a template never makes one, since a
/// row's values would be in it.
const RUNS: [&str; 11] = [
    "script", "iframe", "object", "embed", "style", "link", "meta", "base", "frame", "frameset",
    "applet",
];

/// The attributes whose value is a URL a browser follows or loads.
const URLS: [&str; 12] = [
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
    let lowered = name.trim().to_ascii_lowercase();
    let refuse = |why: &str| Err(Error::Unsafe(format!("[:{tag} {{:{name} …}}]: {why}")));
    let written = |attribute: &Attribute| match attribute {
        Attribute::Variable(_) => true,
        Attribute::Parts(pieces) => pieces
            .iter()
            .any(|piece| matches!(piece, Piece::Variable(_))),
        _ => false,
    };
    if lowered.starts_with("on") {
        return refuse("an attribute named on… is a handler: an event is :on/<type>");
    }
    if lowered == "srcdoc" {
        return refuse("srcdoc is a document of its own");
    }
    if lowered == "style" && written(attribute) {
        return refuse("a style a value writes");
    }
    if URLS.contains(&lowered.as_str()) {
        let safe = match attribute {
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

/// `:on/<kind> (command field: term …)`.
fn event(kind: &str, value: &Syntax) -> Result<Event, Error> {
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
                Syntax::Reference(path) if path.starts_with('.') => Input::Path(
                    path.split('.')
                        .filter(|step| !step.is_empty())
                        .map(str::to_string)
                        .collect(),
                ),
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
            ("[:html {:manifest ?url}]", "manifest"),
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
}
