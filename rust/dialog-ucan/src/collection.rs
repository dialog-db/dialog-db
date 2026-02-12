//! Collection types for [`Ipld`] values

use ipld_core::ipld::Ipld;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[cfg(any(test, feature = "test_utils"))]
use arbitrary::{self, Arbitrary, Unstructured};

#[cfg(any(test, feature = "test_utils"))]
use crate::ipld::InternalIpld;

/// [`Ipld`] collection types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Collection {
    /// Array of [`Ipld`] values
    Array(Vec<Ipld>),

    /// Map of string keys to [`Ipld`] values
    Map(BTreeMap<String, Ipld>),
}

impl Collection {
    /// Returns the array elements or map values.
    #[must_use]
    pub fn to_vec(&self) -> Vec<&Ipld> {
        self.into()
    }

    /// Returns `true` if the collection is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self {
            Collection::Array(xs) => xs.is_empty(),
            Collection::Map(xs) => xs.is_empty(),
        }
    }
}

impl FromIterator<Ipld> for Collection {
    fn from_iter<T: IntoIterator<Item = Ipld>>(iter: T) -> Self {
        let mut map = BTreeMap::new();
        for item in iter {
            if let Ipld::Map(m) = item {
                for (k, v) in m {
                    map.insert(k, v);
                }
            } else {
                return Collection::Array(vec![item]);
            }
        }
        Collection::Map(map)
    }
}

impl From<Collection> for Vec<Ipld> {
    fn from(collection: Collection) -> Self {
        match collection {
            Collection::Array(xs) => xs,
            Collection::Map(xs) => xs.into_values().collect(),
        }
    }
}

impl<'a> From<&'a Collection> for Vec<&'a Ipld> {
    fn from(collection: &'a Collection) -> Self {
        match collection {
            Collection::Array(xs) => xs.iter().collect(),
            Collection::Map(xs) => xs.values().collect(),
        }
    }
}

impl From<Vec<Ipld>> for Collection {
    fn from(xs: Vec<Ipld>) -> Self {
        Collection::Array(xs)
    }
}

impl From<BTreeMap<String, Ipld>> for Collection {
    fn from(xs: BTreeMap<String, Ipld>) -> Self {
        Collection::Map(xs)
    }
}

impl From<HashMap<String, Ipld>> for Collection {
    fn from(xs: HashMap<String, Ipld>) -> Self {
        Collection::Map(xs.into_iter().collect())
    }
}

impl From<Collection> for Ipld {
    fn from(collection: Collection) -> Self {
        match collection {
            Collection::Array(xs) => Ipld::List(xs),
            Collection::Map(xs) => Ipld::Map(xs),
        }
    }
}

#[cfg(any(test, feature = "test_utils"))]
impl<'a> Arbitrary<'a> for Collection {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self, arbitrary::Error> {
        if u.arbitrary()? {
            let map = u.arbitrary::<BTreeMap<String, InternalIpld>>()?;
            Ok(Collection::Map(
                map.into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect::<BTreeMap<String, Ipld>>(),
            ))
        } else {
            let vec = u.arbitrary::<Vec<InternalIpld>>()?;
            Ok(Collection::Array(vec.into_iter().map(Into::into).collect()))
        }
    }
}

// #[cfg(any feature = "test_utils")]
// impl Arbitrary for Collection {
//     type Parameters = ();
//     type Strategy = BoxedStrategy<Self>;
//
//     fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
//         prop_oneof![
//             prop::collection::vec(ipld::Newtype::arbitrary(), 0..10).prop_map(Collection::Array),
//             prop::collection::btree_map(".*", ipld::Newtype::arbitrary(), 0..10)
//                 .prop_map(Collection::Map),
//         ]
//         .boxed()
//     }
// }
