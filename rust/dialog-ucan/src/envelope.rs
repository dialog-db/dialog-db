//! Top-level Varsig envelope.

pub mod payload_tag;

use crate::codec::CborCodec;
use dialog_varsig::{Signature, Varsig};
use ipld_core::ipld::Ipld;
use payload_tag::PayloadTag;
use serde::{
    Deserialize, Serialize,
    de::{self, Deserializer, MapAccess, SeqAccess, Visitor},
    ser::{SerializeMap, SerializeTuple},
};
use std::{fmt, marker::PhantomData};

/// Top-level Varsig envelope type.
///
/// `S` is the signature type (e.g. `Ed25519Signature`).
/// `T` is the payload type (e.g. `DelegationPayload`).
#[derive(Debug, Clone)]
pub struct Envelope<S: Signature, T: Serialize + for<'ze> Deserialize<'ze>>(
    /// Envelope signature.
    pub S,
    /// Varsig envelope
    pub EnvelopePayload<S, T>,
);

impl<S: Signature, T: Serialize + PayloadTag + for<'ze> Deserialize<'ze>> Serialize
    for Envelope<S, T>
{
    fn serialize<Ser: serde::Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        let mut seq = serializer.serialize_tuple(2)?;
        // Wrap signature bytes in serde_bytes::Bytes to ensure it serializes as CBOR bytes
        seq.serialize_element(&serde_bytes::Bytes::new(self.0.to_bytes().as_ref()))?;
        seq.serialize_element(&self.1)?;
        seq.end()
    }
}

impl<'de, S, T> Deserialize<'de> for Envelope<S, T>
where
    S: Signature + for<'ze> Deserialize<'ze>,
    T: Serialize + for<'ze> Deserialize<'ze>,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct EnvelopeVisitor<S, T>
        where
            S: Signature,
            T: Serialize + for<'ze> Deserialize<'ze>,
        {
            marker: std::marker::PhantomData<(S, T)>,
        }

        impl<'de, S, T> Visitor<'de> for EnvelopeVisitor<S, T>
        where
            S: Signature + Deserialize<'de>,
            T: Serialize + for<'ze> Deserialize<'ze>,
        {
            type Value = Envelope<S, T>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a 2-element sequence [signature, payload]")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let sig_ipld: Ipld = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;

                let Ipld::Bytes(sig_bytes) = sig_ipld else {
                    return Err(de::Error::custom("expected signature to be bytes"));
                };

                let signature = S::try_from(sig_bytes.as_slice())
                    .map_err(|_| de::Error::custom("invalid signature bytes"))?;

                let payload: EnvelopePayload<S, T> = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;

                Ok(Envelope(signature, payload))
            }
        }

        deserializer.deserialize_tuple(
            2,
            EnvelopeVisitor {
                marker: std::marker::PhantomData,
            },
        )
    }
}

/// Inner Varsig envelope payload type.
#[derive(Debug, Clone)]
pub struct EnvelopePayload<S: Signature, T: Serialize + for<'de> Deserialize<'de>> {
    /// Varsig header.
    pub header: Varsig<S::Algorithm, CborCodec, Self>,

    /// Payload data.
    pub payload: T,
}

impl<S: Signature, T: Serialize + for<'de> Deserialize<'de>> From<T> for EnvelopePayload<S, T> {
    fn from(payload: T) -> Self {
        EnvelopePayload {
            header: Varsig::new(CborCodec),
            payload,
        }
    }
}

impl<S: Signature, T: PayloadTag + Serialize + for<'de> Deserialize<'de>> EnvelopePayload<S, T> {
    /// Encode this envelope payload for signing/verification using the header's codec.
    ///
    /// # Errors
    ///
    /// Returns the codec's encoding error if encoding fails.
    pub fn encode(
        &self,
    ) -> Result<Vec<u8>, <CborCodec as dialog_varsig::Codec<Self>>::EncodingError>
    where
        CborCodec: dialog_varsig::Codec<Self>,
    {
        self.header.encode(self)
    }
}

impl<S: Signature, T: PayloadTag + Serialize + for<'de> Deserialize<'de>> Serialize
    for EnvelopePayload<S, T>
{
    fn serialize<Ser: serde::Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        // Serialize as nested format: {"h": <varsig>, "<type_tag>": <payload>}
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("h", &self.header)?;
        map.serialize_entry(&T::tag(), &self.payload)?;
        map.end()
    }
}

impl<'de, S, T> Deserialize<'de> for EnvelopePayload<S, T>
where
    S: Signature,
    T: Serialize + for<'any> Deserialize<'any>,
    Varsig<S::Algorithm, CborCodec, EnvelopePayload<S, T>>: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct InnerVisitor<S, T>(PhantomData<(S, T)>);

        // Note the different lifetime parameter on the Visitor:
        impl<'vde, S, T> Visitor<'vde> for InnerVisitor<S, T>
        where
            S: Signature,
            T: Serialize + for<'any> Deserialize<'any>,
            Varsig<S::Algorithm, CborCodec, EnvelopePayload<S, T>>: Deserialize<'vde>,
        {
            type Value = EnvelopePayload<S, T>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(r#"a map with "h" and a payload tag"#)
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'vde>,
            {
                let mut header: Option<Varsig<S::Algorithm, CborCodec, EnvelopePayload<S, T>>> =
                    None;
                let mut payload: Option<T> = None;

                while let Some(key) = map.next_key::<&str>()? {
                    if key == "h" {
                        if header.is_some() {
                            return Err(de::Error::duplicate_field("h"));
                        }
                        let varsig_header_ipld: Ipld = map.next_value()?;
                        let varsig_header_bytes: Vec<u8> = if let Ipld::Bytes(bytes) =
                            varsig_header_ipld
                        {
                            bytes
                        } else {
                            return Err(de::Error::custom("expected varsig header to be bytes"));
                        };
                        let bytes_de = serde::de::value::BytesDeserializer::<M::Error>::new(
                            &varsig_header_bytes,
                        );

                        let varsig_header: Varsig<S::Algorithm, CborCodec, EnvelopePayload<S, T>> =
                            Varsig::<S::Algorithm, CborCodec, EnvelopePayload<S, T>>::deserialize(
                                bytes_de,
                            )?;

                        header = Some(varsig_header);
                    } else {
                        if payload.is_some() {
                            return Err(de::Error::custom("multiple payload fields"));
                        }
                        let value: serde_value::Value = map.next_value()?;
                        payload = Some(T::deserialize(value).map_err(de::Error::custom)?);
                    }
                }

                let header = header.ok_or_else(|| de::Error::missing_field("h"))?;
                let payload = payload.ok_or_else(|| de::Error::custom("missing payload"))?;

                Ok(EnvelopePayload { header, payload })
            }
        }

        deserializer.deserialize_map(InnerVisitor::<S, T>(PhantomData))
    }
}
