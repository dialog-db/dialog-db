use std::borrow::Cow;

use bytes::Bytes;
use dialog_common::Blake3Hash;
use stable_deref_trait::StableDeref;
use zerocopy::TryFromBytes;
// use zerocopy::{KnownLayout, TryFromBytes};
// use yoke::Yokeable;

use crate::{Key, Value};

pub struct StableBytes(pub Bytes);
impl std::ops::Deref for StableBytes {
    type Target = <Bytes as std::ops::Deref>::Target;

    fn deref(&self) -> &Self::Target {
        Bytes::deref(&self.0)
    }
}
unsafe impl StableDeref for StableBytes {}

#[derive(Clone, yoke::Yokeable, KnownLayout, TryFromBytes, Immutable, IntoBytes)]
#[repr(C)]
pub struct Link<Key> {
    pub upper_bound: Key,
    pub node: Blake3Hash,
}

use zerocopy_derive::{Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

#[derive(
    yoke::Yokeable,
    KnownLayout,
    TryFromBytes,
    IntoBytes,
    Immutable,
    Unaligned,
    Clone,
    Debug,
    Eq,
    PartialEq,
)]
#[repr(C)]
struct MyKey([u8; 4]);

impl MyKey {
    pub fn inner(&self) -> &[u8; 4] {
        &self.0
    }
}

impl Key for MyKey {
    const LENGTH: usize = 4;
}

impl From<[u8; 4]> for MyKey {
    fn from(value: [u8; 4]) -> Self {
        MyKey(value)
    }
}

use rkyv::{Archive, Deserialize, Serialize};

#[derive(Clone, Debug, Archive, Serialize, Deserialize)]
struct MyValue {
    pub entity: String,
    pub attribute: String,
    pub value_type: u8,
    pub value: Vec<u8>,
}

impl Value for MyValue {}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use dialog_common::Blake3Hash;
    use rkyv::{Serialize, rancor};
    use std::borrow::Cow;

    use zerocopy::TryFromBytes;

    use super::{ArchivedMyValue, Key, Link, MyKey, MyValue, StableBytes, Value};

    use yoke::Yoke;

    #[test]
    fn it_archives_values_into_and_from_buffers() {
        let value = MyValue {
            entity: "did:unknown".into(),
            attribute: "foo/bar".into(),
            value_type: 1,
            value: vec![1, 2, 3],
        };

        let archived = rkyv::to_bytes::<rancor::Error>(&value).unwrap();
        let bytes = archived.to_vec();

        let pulled = rkyv::access::<ArchivedMyValue, rancor::Error>(&bytes).unwrap();
        let deserialized = rkyv::from_bytes::<MyValue, rancor::Error>(&bytes).unwrap();
        // let v = MyValue {
        //     time: 0,
        //     buffer: [1, 2, 3],
        // };
    }

    #[test]
    fn it_can_represent_a_link_from_borrowed_buffer() {
        // let my_key = MyKey::from(rand::random::<[u8; 4]>());
        let my_key = MyKey::from([1, 2, 3, 4]);
        let link = Link {
            upper_bound: my_key.clone(),
            node: Blake3Hash::hash(my_key.inner()),
        };

        let bytes = zerocopy::IntoBytes::as_bytes(&link).to_owned();

        println!("{} BYTES: {:?}", bytes.len(), bytes);

        let bytes = StableBytes(Bytes::from_owner(bytes));

        let yoked: Yoke<Cow<'_, Link<MyKey>>, StableBytes> = Yoke::attach_to_cart(bytes, |bytes| {
            Cow::Borrowed(Link::try_ref_from_bytes(bytes).unwrap())
        });

        let yoked_link = yoked.get();

        assert_eq!(link.upper_bound, yoked_link.upper_bound);
        assert_eq!(link.node, yoked_link.node);
    }
}
