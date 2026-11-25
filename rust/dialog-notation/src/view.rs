//! Markup: what a query whose answer is a view says, and how a back end
//! keeps it.
//!
//! A query answers with markup when its body says `(view ?x template)`:
//!
//! ```clj
//! (query (todo/row ?t list: ?l is: ?row)
//!   (todo ?t list: ?l title: ?title)
//!   (todo/chip ?t is: (distinct ?chips))
//!   (view ?row [:li ?title ?chips]))
//! ```
//!
//! The template is Hiccup: an element is `[:tag {attributes} child…]`,
//! with events inline as `:on/<type>`, each the command it posts. A
//! variable is a value the query binds, or the markup of a query it
//! collects, which stands there as children, in the order of their keys.
//! The crate reads one into a tree of its own and keeps it.
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

mod markup;
pub(crate) mod plan;
pub(crate) mod tree;

/// A template's tree and the reading of one: for the tests. An embedder
/// renders a view with [`answer`](crate::answer) or keeps one with
/// [`Live::watch`](crate::live::Live::watch).
#[cfg(feature = "inspect")]
pub use markup::{
    Attribute, CALLED, Element, Event, Input, Node, Piece, events, template, variables,
};
#[cfg(not(feature = "inspect"))]
pub(crate) use markup::{
    Attribute, CALLED, Element, Event, Input, Node, Piece, events, template, variables,
};

pub use tree::{Address, Content, Given, Patch, Posted, Rendered, Shown, Slot};
/// What keeps a view's tree from rows handed to it, for a driver of its
/// own and for the tests; [`Live::watch`](crate::live::Live::watch)
/// keeps one itself.
#[cfg(feature = "inspect")]
pub use tree::{Change, Counts, Maintainer, Row, render};
