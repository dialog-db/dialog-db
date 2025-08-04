use std::marker::PhantomData;

use dialog_encoding::{Buf, Cellular, Ref, Width};

use crate::{Key, KeyRef, Value, ValueRef};

#[derive(Clone, Debug)]
pub struct Entry<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: ValueRef<'a, Value>,
{
    key: Key,
    value: Value,
    lifetime: PhantomData<&'a ()>,
}

impl<'a, Key, Value> Entry<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: ValueRef<'a, Value>,
{
    pub fn new(key: Key, value: Value) -> Self {
        Self {
            key,
            value,
            lifetime: PhantomData,
        }
    }

    pub fn key(&self) -> &Key {
        &self.key
    }

    pub fn value(&self) -> &Value {
        &self.value
    }
}

impl<'a, Key, Value> Buf<'a> for Entry<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: ValueRef<'a, Value>,
{
    type Ref = EntryRef<'a, Key, Value>;

    fn to_ref(&'a self) -> Self::Ref {
        EntryRef {
            key: self.key.to_ref(),
            value: self.value.to_ref(),
        }
    }
}

impl<'a, Key, Value> Cellular<'a> for Entry<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: ValueRef<'a, Value>,
{
    fn cell_width() -> Width {
        Key::cell_width() + Value::cell_width()
    }

    fn cells(&self) -> impl Iterator<Item = &[u8]> {
        self.key.cells().chain(self.value.cells())
    }

    fn try_from_cells<I>(cells: &mut I) -> Result<Self, dialog_encoding::DialogEncodingError>
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let key = Key::try_from_cells(cells)?;
        let value = Value::try_from_cells(cells)?;

        Ok(Entry {
            key,
            value,
            lifetime: PhantomData,
        })
    }
}

#[derive(Clone, Debug)]
pub struct EntryRef<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: ValueRef<'a, Value>,
{
    key: Key::Ref,
    value: Value::Ref,
}

impl<'a, Key, Value> EntryRef<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: ValueRef<'a, Value>,
{
    pub fn key(&self) -> &Key::Ref {
        &self.key
    }

    pub fn value(&self) -> &Value::Ref {
        &self.value
    }
}

impl<'a, Key, Value> Ref<'a, Entry<'a, Key, Value>> for EntryRef<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: ValueRef<'a, Value>,
{
    fn to_buf(&self) -> Entry<'a, Key, Value> {
        Entry {
            key: self.key.to_buf(),
            value: self.value.to_buf(),
            lifetime: PhantomData,
        }
    }
}

impl<'a, Key, Value> Cellular<'a> for EntryRef<'a, Key, Value>
where
    Key: self::Key<'a>,
    Key::Ref: KeyRef<'a, Key>,
    Value: self::Value<'a>,
    Value::Ref: ValueRef<'a, Value>,
{
    fn cell_width() -> Width {
        Key::Ref::cell_width() + Value::Ref::cell_width()
    }

    fn cells(&self) -> impl Iterator<Item = &[u8]> {
        self.key.cells().chain(self.value.cells())
    }

    fn try_from_cells<I>(cells: &mut I) -> Result<Self, dialog_encoding::DialogEncodingError>
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let key = Key::Ref::try_from_cells(cells)?;
        let value = Value::Ref::try_from_cells(cells)?;

        Ok(Self { key, value })
    }
}
