use dialog_storage::DialogStorageError;
use std::{
    cell::Cell,
    io::{Cursor, Write},
};

/// Byte writer.
pub struct Writer {
    cursor: Cursor<Vec<u8>>,
}

impl Default for Writer {
    fn default() -> Self {
        Self::new()
    }
}

impl Writer {
    /// Create a new [`Writer`].
    pub fn new() -> Self {
        Self {
            cursor: Cursor::new(vec![]),
        }
    }

    /// Write a `u8` into the writer.
    pub fn write_u8(&mut self, value: u8) -> Result<(), DialogStorageError> {
        self.write_bytes(&[value])?;
        Ok(())
    }

    /// Write a `u16` into the writer.
    pub fn write_u16(&mut self, value: u16) -> Result<(), DialogStorageError> {
        self.write_bytes(&value.to_le_bytes())?;
        Ok(())
    }

    /// Write a `u32` into the writer.
    pub fn write_u32(&mut self, value: u32) -> Result<(), DialogStorageError> {
        self.write_bytes(&value.to_le_bytes())?;
        Ok(())
    }

    /// Write a `u64` into the writer.
    pub fn write_u64(&mut self, value: u64) -> Result<(), DialogStorageError> {
        self.write_bytes(&value.to_le_bytes())?;
        Ok(())
    }

    /// Write bytes into the writer.
    pub fn write_bytes(&mut self, value: &[u8]) -> Result<(), DialogStorageError> {
        let _ = self
            .cursor
            .write(value)
            .map_err(|error| DialogStorageError::EncodeFailed(format!("{error}")))?;
        Ok(())
    }

    /// Write a type implementing [`WriteInto`] into the writer.
    pub fn write<W: WriteInto>(&mut self, target: &W) -> Result<(), DialogStorageError> {
        target.write_into(self).map_err(|error| error.into())
    }

    /// Convert this writer into the bytes that were written.
    pub fn into_inner(self) -> Vec<u8> {
        self.cursor.into_inner()
    }
}

/// Types implementing [`WriteInto`] define how they are written via a
/// [`Writer`].
pub trait WriteInto {
    /// The error type produced by this [`WriteInto`]
    type Error: Into<DialogStorageError>;

    /// Write this struct into a [`Writer`].
    fn write_into(&self, writer: &mut Writer) -> Result<(), Self::Error>;
}

impl WriteInto for &[u8] {
    type Error = DialogStorageError;

    fn write_into(&self, writer: &mut Writer) -> Result<(), Self::Error> {
        writer.write_u32(u32::try_from(self.len()).map_err(|error| {
            DialogStorageError::EncodeFailed(format!("Slice too long: {error}"))
        })?)?;
        writer.write_bytes(self)
    }
}

macro_rules! read_type {
    ( $struct:ident, $fn_name:ident, $ty:ty, $size:expr ) => {
        impl<'a> $struct<'a> {
            #[doc = "Read a `"]
            #[doc = stringify!($ty)]
            #[doc = "` from the reader."]
            pub fn $fn_name(&self) -> Result<$ty, DialogStorageError> {
                const SIZE: usize = $size;
                let (index, next) = self.check_indices(SIZE)?;
                let mut buff = [0u8; SIZE];
                buff.copy_from_slice(&self.bytes[index..next]);
                let out = <$ty>::from_le_bytes(buff);
                self.index.set(next);
                Ok(out)
            }
        }
    };
}

/// Read bytes as references from a source byte slice.
pub struct Reader<'a> {
    bytes: &'a [u8],
    bytes_len: usize,
    index: Cell<usize>,
}

impl<'a> Reader<'a> {
    /// Create a new [`Reader`].
    pub fn new(bytes: &'a [u8]) -> Self {
        Reader {
            bytes,
            bytes_len: bytes.len(),
            index: 0.into(),
        }
    }

    /// Read a `u8` from the reader.
    pub fn read_u8(&self) -> Result<u8, DialogStorageError> {
        let (index, next) = self.check_indices(1)?;
        self.index.set(next);
        Ok(self.bytes[index])
    }

    /// Read a sequence of `count` bytes from the reader.
    pub fn read_bytes(&self, count: usize) -> Result<&[u8], DialogStorageError> {
        let (index, next) = self.check_indices(count)?;
        let out = &self.bytes[index..next];

        self.index.set(next);
        Ok(out)
    }

    /// Read `R` from the reader.
    pub fn read<R: ReadFrom<'a>>(&'a self) -> Result<R, DialogStorageError> {
        R::read_from(self).map_err(|error| error.into())
    }

    /// Skip forward `count` bytes.
    pub fn skip(&self, count: usize) -> Result<(), DialogStorageError> {
        let (_, next) = self.check_indices(count)?;
        self.index.set(next);
        Ok(())
    }

    fn check_indices(&self, size: usize) -> Result<(usize, usize), DialogStorageError> {
        let index = self.index.get();
        let next = index + size;
        if next > self.bytes_len {
            return Err(DialogStorageError::DecodeFailed(
                "Attempted to read out of bounds".into(),
            ));
        }
        Ok((index, next))
    }
}

read_type!(Reader, read_u16, u16, 2);
read_type!(Reader, read_u32, u32, 4);
read_type!(Reader, read_u64, u64, 8);

/// Types implementing [`ReadFrom`] define how they
/// can be instantiated from a [`Reader`].
pub trait ReadFrom<'a>: Sized {
    /// The error type produced by this [`ReadFrom`]
    type Error: Into<DialogStorageError>;

    /// Instantiate `Self` from a [`Reader`].
    fn read_from<'r>(reader: &'r Reader<'a>) -> Result<Self, Self::Error>
    where
        'r: 'a;
}

impl<'a> ReadFrom<'a> for &'a [u8] {
    type Error = DialogStorageError;

    fn read_from<'r>(reader: &'r Reader<'a>) -> Result<Self, DialogStorageError>
    where
        'r: 'a,
    {
        let length = reader.read_u32()?;
        reader.read_bytes(length.try_into().map_err(|error| {
            DialogStorageError::DecodeFailed(format!("Slice too long: {error}"))
        })?)
    }
}

impl<'a> ReadFrom<'a> for Vec<u8> {
    type Error = DialogStorageError;

    fn read_from<'r>(reader: &'r Reader<'a>) -> Result<Self, Self::Error>
    where
        'r: 'a,
    {
        Ok(<&'a [u8] as ReadFrom<'a>>::read_from(reader)?.to_owned())
    }
}

impl<'a, T> ReadFrom<'a> for Vec<T>
where
    T: ReadFrom<'a>,
{
    type Error = DialogStorageError;

    fn read_from<'r>(reader: &'r Reader<'a>) -> Result<Self, Self::Error>
    where
        'r: 'a,
    {
        let length = reader.read_u32()?;
        let mut collection = vec![];
        for _ in 0..length {
            collection.push(T::read_from(reader).map_err(|error| error.into())?);
        }
        Ok(collection)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use dialog_storage::DialogStorageError;

    use crate::{ReadFrom, Reader, WriteInto, Writer};

    #[test]
    fn it_can_round_trip_complex_data() -> Result<()> {
        #[derive(PartialEq, Debug)]
        pub struct ComplexData {
            bytes: [u8; 32],
            value_8: u8,
            pad_8: u8,
            value_16: u16,
            value_32: u32,
        }

        impl WriteInto for ComplexData {
            type Error = DialogStorageError;

            fn write_into(
                &self,
                writer: &mut super::Writer,
            ) -> std::result::Result<(), Self::Error> {
                writer.write_bytes(&self.bytes)?;
                writer.write_u8(self.value_8)?;
                writer.write_u8(self.pad_8)?;
                writer.write_u16(self.value_16)?;
                writer.write_u32(self.value_32)?;
                Ok(())
            }
        }

        impl<'a> ReadFrom<'a> for ComplexData {
            type Error = DialogStorageError;

            fn read_from<'r>(
                reader: &'r super::Reader<'a>,
            ) -> std::result::Result<Self, Self::Error>
            where
                'r: 'a,
            {
                Ok(Self {
                    bytes: reader.read_bytes(32)?.try_into().unwrap(),
                    value_8: reader.read_u8()?,
                    pad_8: reader.read_u8()?,
                    value_16: reader.read_u16()?,
                    value_32: reader.read_u32()?,
                })
            }
        }

        let data = ComplexData {
            bytes: blake3::hash(&[1, 2, 3]).as_bytes().to_owned(),
            value_8: 123,
            pad_8: 231,
            value_16: 1024,
            value_32: 64000,
        };

        let mut writer = Writer::new();
        writer.write(&data)?;

        let bytes = writer.into_inner();

        let reader = Reader::new(&bytes);

        let deserialized_data = reader.read::<ComplexData>()?;

        assert_eq!(data, deserialized_data);

        Ok(())
    }
}
