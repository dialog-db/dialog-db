use rkyv::Archive;
use std::fmt::Debug;

pub trait Key:
    Clone + Debug + Sized + AsRef<[u8]> + std::hash::Hash + PartialOrd + Ord + PartialEq + Eq + Archive
where
    // Self: PartialOrd<Self::Archived> + PartialEq<Self::Archived>,
    Self::Archived: PartialOrd<Self> + PartialEq<Self>,
{
    const LENGTH: usize;
}

impl<const N: usize> Key for [u8; N] {
    const LENGTH: usize = N;
}

pub trait Value: Clone + Debug + Sized + Archive {}

impl Value for Vec<u8> {}
