//! CID helpers.

use ipld_core::cid::{Cid, multihash::Multihash};
use serde::Serialize;
use sha2::Digest;

/// Serialize a value to a DAG-CBOR/SHA2-256 CID.
///
/// # Panics
///
/// Will panic if the value cannot be serialized or if the multihash cannot be created.
/// We assume that all UCANs are comparible with IPLD, and so serialization (and hashing)
/// "should" never fail unless something is deeply wrong.
pub fn to_dagcbor_cid<T: Serialize>(t: &T) -> Cid {
    #[allow(clippy::expect_used)]
    let bytes = serde_ipld_dagcbor::to_vec(t).expect("not serializable");
    let digest = sha2::Sha256::digest(bytes);
    #[allow(clippy::expect_used)]
    let multihash = Multihash::wrap(0x12, &digest).expect("unable to create multihash");
    ipld_core::cid::Cid::new_v1(0x71, multihash)
}
