use allocator_api2::alloc::Allocator;
use dialog_common::Blake3Hash;
use dialog_encoding::{BufOrRef, Cellular, DialogEncodingError, Width};
use itertools::Itertools;
use nonempty::NonEmpty;

use crate::{
    AppendCache, DialogTreeError, Entry, EntryRef, Key, KeyRef, Link, LinkRef, Node, Value,
    ValueRef,
};

#[derive(Debug, Clone)]
pub enum NodeBody<'a, Key, Value, Allocator>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: self::ValueRef<'a, Value>,
    Allocator: self::Allocator,
{
    Index {
        index: Index<'a, Key>,
        child_cache: AppendCache<Blake3Hash, Node<'a, Key, Value, Allocator>>,
    },
    Segment(Segment<'a, Key, Value>),
}

impl<'a, Key, Value, Allocator> NodeBody<'a, Key, Value, Allocator>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: self::ValueRef<'a, Value>,
    Allocator: self::Allocator,
{
    pub fn upper_bound(&'a self) -> Key::Ref {
        match self {
            NodeBody::Index { index, .. } => index.upper_bound(),
            NodeBody::Segment(segment) => segment.upper_bound(),
        }
    }

    pub fn is_index(&self) -> bool {
        match self {
            NodeBody::Index { .. } => true,
            _ => false,
        }
    }

    pub fn is_segment(&self) -> bool {
        match self {
            NodeBody::Segment(..) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Segment<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: self::ValueRef<'a, Value>,
{
    entries: NonEmpty<BufOrRef<'a, Entry<'a, Key, Value>>>,
}

impl<'a, Key, Value> Segment<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: self::ValueRef<'a, Value>,
{
    pub fn new(entry: Entry<'a, Key, Value>) -> Self {
        Self {
            entries: NonEmpty::new(BufOrRef::Buf(entry)),
        }
    }

    pub fn upper_bound(&'a self) -> Key::Ref {
        self.entries.last().to_ref().key().clone()
    }

    pub fn entries(&self) -> &NonEmpty<BufOrRef<'a, Entry<'a, Key, Value>>> {
        &self.entries
    }

    pub fn upsert(&self, entry: Entry<'a, Key, Value>) -> Result<Self, DialogTreeError> {
        let mut node = self.clone();

        match node.find(entry.key()) {
            Ok(index) => {
                let Some(previous_entry) = node.entries.get_mut(index) else {
                    return Err(DialogTreeError::Operation(format!(
                        "Entry at index {} not found",
                        index,
                    )));
                };
                let _ = std::mem::replace(previous_entry, BufOrRef::Buf(entry));
            }
            Err(index) => node.entries.insert(index, BufOrRef::Buf(entry)),
        };

        Ok(node)
    }

    pub fn remove(&self, key: &Key) -> Option<Self> {
        let mut node = self.clone();

        match node.find(key) {
            Ok(index) => {
                let mut entries = Vec::from(node.entries);
                entries.remove(index);
                match NonEmpty::from_vec(entries) {
                    Some(entries) => {
                        node.entries = entries;
                    }
                    None => return None,
                }
            }
            Err(_) => (),
        }

        Some(node)
    }

    fn find(&self, key: &Key) -> Result<usize, usize> {
        self.entries.binary_search_by(|probe| match probe {
            BufOrRef::Buf(buffer) => buffer.key().cmp(key),
            BufOrRef::Ref(reference) => PartialOrd::<Key>::partial_cmp(reference.key(), key)
                .unwrap_or(std::cmp::Ordering::Equal),
        })
    }
}

impl<'a, Key, Value> From<NonEmpty<BufOrRef<'a, Entry<'a, Key, Value>>>> for Segment<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: self::ValueRef<'a, Value>,
{
    fn from(entries: NonEmpty<BufOrRef<'a, Entry<'a, Key, Value>>>) -> Self {
        Self { entries }
    }
}

impl<'a, Key, Value> Cellular<'a> for Segment<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: self::ValueRef<'a, Value>,
{
    fn cell_width() -> Width {
        Width::Unbounded
    }

    fn cells(&self) -> impl Iterator<Item = &[u8]> {
        self.entries.iter().flat_map(|entry| {
            let iter: Box<dyn Iterator<Item = &[u8]>> = match entry {
                BufOrRef::Buf(buffer) => Box::new(buffer.cells()),
                BufOrRef::Ref(reference) => Box::new(reference.cells()),
            };
            iter
        })
    }

    fn try_from_cells<I>(cells: &mut I) -> Result<Self, dialog_encoding::DialogEncodingError>
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let mut entries = Vec::new();
        let chunk_size = match EntryRef::<'a, Key, Value>::cell_width() {
            Width::Bounded(size) => size,
            _ => {
                return Err(DialogEncodingError::InvalidLayout(format!(
                    "Entries must have bounded cell width"
                )));
            }
        };

        for mut chunk in &cells.chunks(chunk_size) {
            entries.push(BufOrRef::Ref(EntryRef::try_from_cells(&mut chunk)?));
        }

        Ok(Self {
            entries: NonEmpty::from_vec(entries).ok_or_else(|| {
                DialogEncodingError::InvalidLayout(
                    "Segment node must have at least one entry".into(),
                )
            })?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Index<'a, Key>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
{
    links: NonEmpty<BufOrRef<'a, Link<'a, Key>>>,
}

impl<'a, Key> Index<'a, Key>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
{
    pub fn new(link: Link<'a, Key>) -> Self {
        Self {
            links: NonEmpty::new(BufOrRef::Buf(link)),
        }
    }

    pub fn upper_bound(&'a self) -> Key::Ref {
        self.links.last().to_ref().upper_bound().clone()
    }

    pub fn links(&self) -> &NonEmpty<BufOrRef<'a, Link<'a, Key>>> {
        &self.links
    }

    pub fn upsert(&'a self, link: Link<'a, Key>) -> Result<Self, DialogTreeError> {
        let mut node = self.clone();

        match node.find(link.upper_bound()) {
            Ok(index) => {
                let Some(previous_entry) = node.links.get_mut(index) else {
                    return Err(DialogTreeError::Operation(format!(
                        "Link at index {} not found",
                        index,
                    )));
                };
                let _ = std::mem::replace(previous_entry, BufOrRef::Buf(link));
            }
            Err(index) => node.links.insert(index, BufOrRef::Buf(link)),
        };

        Ok(node)
    }

    pub fn remove(&'a self, key: &Key) -> Option<Self> {
        let mut node = self.clone();

        match node.find(key) {
            Ok(index) => {
                let mut links = Vec::from(node.links);
                links.remove(index);
                match NonEmpty::from_vec(links) {
                    Some(links) => {
                        node.links = links;
                    }
                    None => return None,
                }
            }
            Err(_) => (),
        }

        Some(node)
    }

    fn find(&self, key: &Key) -> Result<usize, usize> {
        self.links.binary_search_by(|probe| match probe {
            BufOrRef::Buf(buffer) => buffer.upper_bound().cmp(key),
            BufOrRef::Ref(reference) => {
                PartialOrd::<Key>::partial_cmp(reference.upper_bound(), key)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
        })
    }
}

impl<'a, Key> From<NonEmpty<BufOrRef<'a, Link<'a, Key>>>> for Index<'a, Key>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
{
    fn from(links: NonEmpty<BufOrRef<'a, Link<'a, Key>>>) -> Self {
        Self { links }
    }
}

impl<'a, Key> Cellular<'a> for Index<'a, Key>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
{
    fn cell_width() -> Width {
        Width::Unbounded
    }

    fn cells(&self) -> impl Iterator<Item = &[u8]> {
        self.links.iter().flat_map(|link| {
            let cells: Box<dyn Iterator<Item = &[u8]>> = match link {
                BufOrRef::Buf(buffer) => Box::new(buffer.cells()),
                BufOrRef::Ref(reference) => Box::new(reference.cells()),
            };
            cells
        })
    }

    fn try_from_cells<I>(cells: &mut I) -> Result<Self, DialogEncodingError>
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let mut links = Vec::new();
        let chunk_size = match LinkRef::<'a, Key>::cell_width() {
            Width::Bounded(size) => size,
            _ => {
                return Err(DialogEncodingError::InvalidLayout(format!(
                    "Links must have bounded cell width"
                )));
            }
        };

        for mut chunk in &cells.chunks(chunk_size) {
            links.push(BufOrRef::Ref(LinkRef::try_from_cells(&mut chunk)?));
        }

        Ok(Self {
            links: NonEmpty::from_vec(links).ok_or_else(|| {
                DialogEncodingError::InvalidLayout("Index node must have at least one entry".into())
            })?,
        })
    }
}
