//! A form as data: what it is, what names it, and how it is laid out.
//!
//! Three things that change at different rates are kept apart, so the
//! one that changes most often touches the least:
//!
//! - **The data types.** A form is a value Dialog-DB has (a truth, a
//!   number, text, an entity or a symbol), a list, or a map. Nothing
//!   here knows what a form means. `?x`, `_` and `...` are symbols like
//!   any other, and it is the syntax above that reads a symbol starting
//!   with `?` as a variable. `( )` is a tuple, which is a list: the
//!   head, the parts with no label in order, and the labelled parts as
//!   one map at its end. `[ ]` writes the same list.
//! - **The name.** A form's [`reference`](Form::reference) is a merkle
//!   reference, made of the references of its parts under the type of
//!   each. It does not depend on the bytes a form is laid out in, nor
//!   on the order its labelled parts or its map's entries were written
//!   in.
//! - **The layout.** [`encode`](Form::encode) writes a form as bytes
//!   under a version, and [`decode`](Form::decode) refuses a version it
//!   does not know, by number. The bytes are what a `record:` value
//!   holds. A new layout is a new version, and no name changes with it.
//!
//! | written | data | Dialog-DB value | referenced under |
//! |---|---|---|---|
//! | `"text"` | text | `Value::String` | `text:` |
//! | `42`, `-3`, `1.5` | number | natural, integer, float | the number type |
//! | `true`, `false` | truth | `Value::Boolean` | `boolean:` |
//! | `todo:1`, `:open` | entity | `Value::Entity` | `entity:` |
//! | `todo`, `xyz.todo`, `?x`, `_`, `+` | symbol of no domain | `Value::Symbol`, `/todo` | `symbol:` |
//! | `xyz.todo/text` | symbol | `Value::Symbol` | `symbol:` |
//! | `[a b]`, `(a b)` | list | none yet | the list type |
//! | `{k v}`, `{label: v}` | map | none yet | the map type |
//! | `(head part… label: part…)` | the list `head part… {label: part…}` | none yet | the list type |
//!
//! What a call's parts mean is not here either. `(todo ?t state: :open)`
//! and `(todo state: :open of: ?t)` are one call and two lists: that the
//! part with no label is `of:` is the syntax's to say, in
//! [`Form::bare`].
//!
//! The layout written here is a stand-in for the one the record values
//! research decides. That is the reason it carries a version.

use std::str::FromStr;

use base58::ToBase58;
use bijoux::{Decode as _, Encode as _};
use dialog_corpus::{Entity, Relation, Value};
use merkle_reference::{Reference, Type};

use crate::Error;
use crate::form::{Form, Kind, Part, atom};

/// The layout [`Form::encode`] writes.
pub const VERSION: u8 = 1;

const TRUTH: u8 = 1;
const NATURAL: u8 = 2;
const INTEGER: u8 = 3;
const FLOAT: u8 = 4;
const TEXT: u8 = 5;
const ENTITY: u8 = 6;
const SYMBOL: u8 = 7;
const LIST: u8 = 8;
const MAP: u8 = 9;

fn refused(what: &str, error: impl std::fmt::Display) -> Error {
    Error::Refused(format!("{what}: {error}"))
}

/// The symbol a name is: itself where it has a domain, and of no
/// domain otherwise.
fn symbol(name: &str) -> Result<Value, Error> {
    let spelled = if name.contains('/') {
        name.to_string()
    } else {
        format!("/{name}")
    };
    Relation::from_str(&spelled)
        .map(Value::Symbol)
        .map_err(|error| refused("not a symbol", error))
}

/// A number as written, as the value it is: a natural where it can be,
/// then an integer, then a float.
fn number(text: &str) -> Result<Value, Error> {
    if let Ok(natural) = text.parse::<u64>() {
        Ok(Value::UnsignedInt(natural.into()))
    } else if let Ok(integer) = text.parse::<i64>() {
        Ok(Value::SignedInt(integer.into()))
    } else {
        text.parse::<f64>()
            .ok()
            .filter(|float| float.is_finite())
            .map(Value::Float)
            .ok_or_else(|| Error::Refused(format!("{text} is not a number")))
    }
}

fn length(count: usize, out: &mut Vec<u8>) {
    (count as u64).encode(out);
}

fn bytes(content: &[u8], out: &mut Vec<u8>) {
    length(content.len(), out);
    out.extend_from_slice(content);
}

/// Sorted, with no key twice.
fn sorted<T>(mut entries: Vec<(Vec<u8>, T)>, what: &str) -> Result<Vec<(Vec<u8>, T)>, Error> {
    entries.sort_by(|(a, _), (b, _)| a.cmp(b));
    if entries.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(Error::Refused(format!("{what} twice")));
    }
    Ok(entries)
}

impl Form {
    /// The Dialog-DB value a form is, where it is one. `None` for a list
    /// and a map, which Dialog-DB has no value for yet.
    pub fn value(&self) -> Result<Option<Value>, Error> {
        Ok(Some(match self {
            Form::Text(text) => Value::String(text.clone()),
            Form::Number(text) => number(text)?,
            Form::Keyword(name) => Value::Entity(
                Entity::from_str(&format!("{name}:"))
                    .map_err(|error| refused("not an entity", error))?,
            ),
            Form::Symbol(name) if name == "true" || name == "false" => {
                Value::Boolean(name == "true")
            }
            Form::Symbol(name) if self.kind() == Some(Kind::Entity) => Value::Entity(
                Entity::from_str(name).map_err(|error| refused("not an entity", error))?,
            ),
            Form::Symbol(name) => symbol(name)?,
            Form::Variable(name) => symbol(&format!("?{name}"))?,
            Form::Blank => symbol("_")?,
            Form::Ellipsis => symbol("...")?,
            Form::Rest(name) => symbol(&format!("...?{name}"))?,
            Form::Vector(_) | Form::Map(_) | Form::Application(..) => return Ok(None),
        }))
    }

    /// What names the form: a merkle reference, made of the references
    /// of its parts, a list under the type every list has and a map
    /// under the type every map has. Two forms that say the same thing
    /// in another order of labels have one reference, and no layout has
    /// a part in it.
    pub fn reference(&self) -> Result<Reference, Error> {
        if let Some(value) = self.value()? {
            return Ok(value.reference());
        }
        Ok(match self {
            Form::Map(entries) => Type::MAP.map(
                entries
                    .iter()
                    .map(|(key, value)| Ok((key.reference()?, value.reference()?)))
                    .collect::<Result<Vec<_>, Error>>()?,
            ),
            _ => Type::LIST.list(
                self.tuple()
                    .iter()
                    .map(Form::reference)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        })
    }

    /// The entity that names the form: `form:` and its reference.
    pub fn entity(&self) -> Result<Entity, Error> {
        Entity::from_str(&format!(
            "form:{}",
            self.reference()?.as_bytes().to_base58()
        ))
        .map_err(|error| refused("not an entity", error))
    }

    /// The list a `[ ]` or a `( )` is. For a tuple: the head, then the
    /// parts with no label as written, a `...?rest` among them, then
    /// the labelled parts as one map, where there are any.
    fn tuple(&self) -> Vec<Form> {
        match self {
            Form::Vector(items) => items.clone(),
            Form::Application(head, parts) => {
                let mut items = vec![(**head).clone()];
                let mut labelled = Vec::new();
                for part in parts {
                    match part {
                        Part::Bare(form) => items.push(form.clone()),
                        Part::Labelled(label, form) => {
                            labelled.push((Form::symbol(label), form.clone()));
                        }
                    }
                }
                if !labelled.is_empty() {
                    items.push(Form::Map(labelled));
                }
                items
            }
            _ => Vec::new(),
        }
    }

    /// The form as bytes: the version, then the form. One form has one
    /// byte string, whatever order its labels and entries were written
    /// in.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut out = vec![VERSION];
        self.write(&mut out)?;
        Ok(out)
    }

    fn write(&self, out: &mut Vec<u8>) -> Result<(), Error> {
        match self.value()? {
            Some(Value::Boolean(truth)) => out.extend([TRUTH, u8::from(truth)]),
            Some(Value::UnsignedInt(natural)) => {
                out.push(NATURAL);
                (natural as u64).encode(out);
            }
            Some(Value::SignedInt(integer)) => {
                out.push(INTEGER);
                out.extend((integer as i64).to_be_bytes());
            }
            Some(Value::Float(float)) => {
                out.push(FLOAT);
                out.extend(float.to_bits().to_be_bytes());
            }
            Some(Value::String(text)) => {
                out.push(TEXT);
                bytes(text.as_bytes(), out);
            }
            Some(Value::Entity(entity)) => {
                out.push(ENTITY);
                bytes(entity.as_str().as_bytes(), out);
            }
            Some(Value::Symbol(relation)) => {
                out.push(SYMBOL);
                bytes(relation.to_string().as_bytes(), out);
            }
            Some(Value::Bytes(_) | Value::Record(_)) => {
                return Err(Error::Refused(format!("{self} is not data")));
            }
            None => match self {
                Form::Map(entries) => {
                    out.push(MAP);
                    let mut written = Vec::new();
                    for (key, value) in entries {
                        let mut at = Vec::new();
                        key.write(&mut at)?;
                        written.push((at, value));
                    }
                    let written = sorted(written, "a map holds a key")?;
                    length(written.len(), out);
                    for (key, value) in written {
                        out.extend(key);
                        value.write(out)?;
                    }
                }
                _ => {
                    let items = self.tuple();
                    out.push(LIST);
                    length(items.len(), out);
                    for item in &items {
                        item.write(out)?;
                    }
                }
            },
        }
        Ok(())
    }

    /// The form `bytes` hold. A version this does not know is refused
    /// by its number, and so are bytes that are not the one way the form
    /// is written.
    pub fn decode(bytes: &[u8]) -> Result<Form, Error> {
        let (version, rest) = bytes
            .split_first()
            .ok_or_else(|| Error::Read("a form of no bytes".into()))?;
        if *version != VERSION {
            return Err(Error::Read(format!(
                "a form laid out as version {version}, and this reads version {VERSION}"
            )));
        }
        let mut reader = Reader { bytes: rest };
        let form = reader.form()?;
        if !reader.bytes.is_empty() {
            return Err(Error::Read("bytes after the form".into()));
        }
        if form.encode()? != bytes {
            return Err(Error::Read(
                "bytes that are not the one way the form is written".into(),
            ));
        }
        Ok(form)
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
}

impl Reader<'_> {
    fn short() -> Error {
        Error::Read("a form that ends early".into())
    }

    fn take(&mut self, count: usize) -> Result<&[u8], Error> {
        if self.bytes.len() < count {
            return Err(Self::short());
        }
        let (taken, rest) = self.bytes.split_at(count);
        self.bytes = rest;
        Ok(taken)
    }

    fn length(&mut self) -> Result<usize, Error> {
        let (value, consumed) = u64::decode(self.bytes).map_err(|_| Self::short())?;
        self.bytes = &self.bytes[consumed..];
        usize::try_from(value).map_err(|_| Self::short())
    }

    fn text(&mut self) -> Result<String, Error> {
        let count = self.length()?;
        String::from_utf8(self.take(count)?.to_vec())
            .map_err(|_| Error::Read("text that is not UTF-8".into()))
    }

    /// A symbol's name as written: without the `/` of no domain.
    fn name(&mut self) -> Result<String, Error> {
        let spelled = self.text()?;
        Ok(match spelled.strip_prefix('/') {
            Some(name) => name.to_string(),
            None => spelled,
        })
    }

    fn form(&mut self) -> Result<Form, Error> {
        let tag = self.take(1)?[0];
        Ok(match tag {
            TRUTH => Form::Symbol((self.take(1)?[0] != 0).to_string()),
            NATURAL => {
                let (natural, consumed) = u64::decode(self.bytes).map_err(|_| Self::short())?;
                self.bytes = &self.bytes[consumed..];
                Form::Number(natural.to_string())
            }
            INTEGER => {
                let mut integer = [0; 8];
                integer.copy_from_slice(self.take(8)?);
                Form::Number(i64::from_be_bytes(integer).to_string())
            }
            FLOAT => {
                let mut bits = [0; 8];
                bits.copy_from_slice(self.take(8)?);
                Form::Number(format!("{:?}", f64::from_bits(u64::from_be_bytes(bits))))
            }
            TEXT => Form::Text(self.text()?),
            ENTITY => {
                let uri = self.text()?;
                match uri.strip_suffix(':') {
                    Some(name) if !name.is_empty() && !name.contains(':') => {
                        Form::Keyword(name.to_string())
                    }
                    _ => Form::Symbol(uri),
                }
            }
            SYMBOL => atom(&self.name()?),
            // A list reads back as a tuple, its first member the head.
            // A map at its end whose keys are labels is its labelled
            // parts, which is what one written out there is anyway.
            LIST => {
                let count = self.length()?;
                let mut items = (0..count)
                    .map(|_| self.form())
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter();
                match items.next() {
                    None => Form::Vector(Vec::new()),
                    Some(head) => Form::application(head, items.map(Part::Bare).collect()),
                }
            }
            MAP => {
                let count = self.length()?;
                Form::Map(
                    (0..count)
                        .map(|_| Ok((self.form()?, self.form()?)))
                        .collect::<Result<_, Error>>()?,
                )
            }
            other => {
                return Err(Error::Read(format!(
                    "a form of no kind this reads: {other}"
                )));
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::form::read;

    fn one(text: &str) -> Form {
        read(text).unwrap().remove(0)
    }

    /// Every form the prelude and the examples are written in is data:
    /// each lays out as bytes and reads back as the form it was.
    #[test]
    fn it_lays_out_every_form_of_the_prelude_and_the_examples() {
        for text in [
            include_str!("prelude.clj"),
            include_str!("../examples/todo.clj"),
            include_str!("../examples/done.clj"),
        ] {
            for form in read(text).unwrap() {
                let bytes = form.encode().unwrap();
                let back = Form::decode(&bytes).unwrap();
                assert_eq!(back.encode().unwrap(), bytes, "{form}");
                assert_eq!(
                    back.reference().unwrap(),
                    form.reference().unwrap(),
                    "{form}"
                );
            }
        }
    }

    #[test]
    fn it_reads_back_what_was_written_where_the_order_is_the_layouts_own() {
        let form =
            one(r#"(xyz.todo ?t state: :open list: list:home text: "Buy milk" n: -3 ok: true)"#);
        assert_eq!(
            Form::decode(&form.encode().unwrap()).unwrap().to_string(),
            r#"(xyz.todo ?t n: -3 ok: true list: list:home text: "Buy milk" state: :open)"#
        );
        let pattern = one("(syntax (?name ?this ...?fields) [?body ...] {kind: _ 1 1.5})");
        assert_eq!(
            Form::decode(&pattern.encode().unwrap())
                .unwrap()
                .to_string(),
            "(syntax (?name ?this ...?fields) (?body ...) {1 1.5 kind: _})"
        );
    }

    /// The name is of what a form says, not of how it was written or
    /// laid out.
    #[test]
    fn it_names_a_form_by_what_it_says() {
        let form = one("(xyz.todo ?t list: ?l text: ?x)");
        assert_eq!(
            form.reference().unwrap(),
            one("(xyz.todo ?t text: ?x list: ?l)").reference().unwrap()
        );
        assert_eq!(
            form.encode().unwrap(),
            one("(xyz.todo ?t text: ?x list: ?l)").encode().unwrap()
        );
        assert_ne!(
            form.reference().unwrap(),
            one("(xyz.todo ?t list: ?l text: ?y)").reference().unwrap()
        );
        assert_eq!(
            one("(f a b)").reference().unwrap(),
            one("[f a b]").reference().unwrap(),
            "a tuple is the list of its parts"
        );
        assert_eq!(
            one("(f a k: 1)").reference().unwrap(),
            one("[f a {k: 1}]").reference().unwrap(),
            "its labelled parts are the map at its end"
        );
        assert_eq!(
            one("{a: 1 b: 2}").reference().unwrap(),
            one("{b: 2 a: 1}").reference().unwrap()
        );
        assert!(form.entity().unwrap().as_str().starts_with("form:"));
    }

    /// One call written three ways is two lists: the primary under its
    /// label is the same data as the record written out, and the primary
    /// with no label is another list. That they are one call is for the
    /// syntax to say.
    #[test]
    fn it_keeps_a_call_as_it_was_written() {
        let labelled = one("(todo state: :open of: ?t)");
        let record = one("(todo {of: ?t state: :open})");
        let bare = one("(todo ?t state: :open)");
        assert_eq!(labelled.reference().unwrap(), record.reference().unwrap());
        assert_ne!(labelled.reference().unwrap(), bare.reference().unwrap());
        assert_eq!(labelled.bare(), bare.bare());
        assert_eq!(labelled.labelled(), bare.labelled());
    }

    /// The natural 3 and the float 3.0 are one number to Dialog-DB and
    /// two byte strings here: a name does not follow the layout.
    #[test]
    fn it_names_apart_from_the_layout() {
        let natural = one("(f 3)");
        let float = one("(f 3.0)");
        assert_eq!(natural.reference().unwrap(), float.reference().unwrap());
        assert_ne!(natural.encode().unwrap(), float.encode().unwrap());
    }

    #[test]
    fn it_refuses_a_version_it_does_not_know_and_bytes_out_of_order() {
        let mut bytes = one("(f a: 1 b: 2)").encode().unwrap();
        bytes[0] = 2;
        assert_eq!(
            Form::decode(&bytes),
            Err(Error::Read(
                "a form laid out as version 2, and this reads version 1".into()
            ))
        );

        // The two labelled parts, swapped.
        let a = one("(f a: 1)").encode().unwrap();
        let b = one("(f b: 2)").encode().unwrap();
        let both = one("(f a: 1 b: 2)").encode().unwrap();
        let part = |bytes: &[u8]| bytes[bytes.len() - 6..].to_vec();
        let mut swapped = both[..both.len() - 12].to_vec();
        swapped.extend(part(&b));
        swapped.extend(part(&a));
        assert_eq!(swapped.len(), both.len());
        assert!(matches!(Form::decode(&swapped), Err(Error::Read(_))));
    }

    #[test]
    fn it_refuses_a_label_or_a_key_twice() {
        assert!(matches!(
            one("(f a: 1 a: 2)").encode(),
            Err(Error::Refused(_))
        ));
        assert!(matches!(
            one("{a: 1 a: 2}").encode(),
            Err(Error::Refused(_))
        ));
    }
}
