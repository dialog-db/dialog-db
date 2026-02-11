//! Concrete codec types for UCAN.

use ipld_core::codec::Codec as IpldCodec;
use serde::{Deserialize, Serialize};
use serde_ipld_dagcbor::{codec::DagCborCodec, error::CodecError};
use std::io::{BufRead, Write};
use varsig::codec::{Codec, Format};

/// DAG-CBOR codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CborCodec;

impl Format for CborCodec {
    fn multicodec_code(&self) -> u64 {
        <DagCborCodec as IpldCodec<()>>::CODE
    }

    fn try_from_tags(code: &[u64]) -> Option<Self> {
        if code.len() == 1 && *code.first()? == <DagCborCodec as IpldCodec<()>>::CODE {
            Some(CborCodec)
        } else {
            None
        }
    }
}

impl<T: Serialize + for<'de> Deserialize<'de>> Codec<T> for CborCodec {
    type EncodingError = CodecError;
    type DecodingError = CodecError;

    fn encode_payload<W: Write>(
        &self,
        payload: &T,
        buffer: &mut W,
    ) -> Result<(), Self::EncodingError> {
        <DagCborCodec as IpldCodec<T>>::encode(buffer, payload)
    }

    fn decode_payload<R: BufRead>(&self, reader: &mut R) -> Result<T, Self::DecodingError> {
        <DagCborCodec as IpldCodec<T>>::decode(reader)
    }
}
