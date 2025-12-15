use dialog_common::Blake3Hash;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Clone, Debug, Archive, Serialize, Deserialize)]
pub struct Link<Key> {
    pub upper_bound: Key,
    pub node: Blake3Hash,
}
