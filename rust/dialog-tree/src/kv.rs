use dialog_common::{Blake3Hashed, ConditionalSync};
use dialog_encoding::{Buf, Cellular, Ref};

pub trait Key<'a>:
    Buf<'a> + Cellular<'a> + Blake3Hashed + PartialEq + Ord + std::fmt::Debug + ConditionalSync
where
    Self: 'static,
    Self::Ref: KeyRef<'a, Self> + 'a,
{
}

pub trait KeyRef<'a, Key>:
    Cellular<'a>
    + Blake3Hashed
    + Ref<'a, Key>
    + PartialEq
    + PartialOrd<Key>
    + Ord
    + std::fmt::Debug
    + ConditionalSync
where
    Self: 'a,
    Key: self::Key<'a, Ref = Self>,
{
}

pub trait Value<'a>: Buf<'a> + Cellular<'a> + std::fmt::Debug + ConditionalSync
where
    Self: 'static,
    Self::Ref: ValueRef<'a, Self>,
{
}

pub trait ValueRef<'a, Value>:
    Cellular<'a> + Ref<'a, Value> + Blake3Hashed + std::fmt::Debug + ConditionalSync
where
    Value: self::Value<'a, Ref = Self>,
{
}
