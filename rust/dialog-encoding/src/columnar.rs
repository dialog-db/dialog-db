use std::{
    collections::BTreeMap,
    io::{Cursor, Read, Seek, SeekFrom, Write},
};

fn into_io_error(error: leb128::read::Error) -> std::io::Error {
    match error {
        leb128::read::Error::IoError(error) => error,
        leb128::read::Error::Overflow => std::io::ErrorKind::InvalidInput.into(),
    }
}

pub trait Columnar<'a, const COLUMNS: usize> {
    fn cells(&'a self) -> impl Iterator<Item = &'a [u8]>;
}

pub fn compress<'a, const COLUMNS: usize, Layout, Buffer>(
    layout: &'a Layout,
    mut buffer: Buffer,
) -> Result<(), std::io::Error>
where
    Layout: Columnar<'a, COLUMNS>,
    Buffer: Write,
{
    let mut data = Cursor::new(Vec::new());
    let mut ranges = Cursor::new(Vec::new());
    let mut cells = Cursor::new(Vec::new());
    let mut bytes_to_index = BTreeMap::<&'a [u8], u64>::new();
    let mut next_index = 0u64;
    let mut data_length = 0usize;

    for cell in layout.cells() {
        if let Some(index) = bytes_to_index.get(cell) {
            leb128::write::unsigned(&mut cells, *index)?;
        } else {
            leb128::write::unsigned(&mut ranges, data_length as u64)?;
            leb128::write::unsigned(&mut ranges, cell.len() as u64)?;

            data_length += cell.len();
            data.write(cell)?;
            bytes_to_index.insert(cell, next_index);

            leb128::write::unsigned(&mut cells, next_index)?;

            next_index += 1;
        }
    }

    let data = data.into_inner();
    let ranges = ranges.into_inner();
    let cells = cells.into_inner();

    // [ data length ][ data ]
    leb128::write::unsigned(&mut buffer, data.len() as u64)?;
    buffer.write(&data)?;

    // [ ranges length ][ ranges ]
    leb128::write::unsigned(&mut buffer, ranges.len() as u64)?;
    buffer.write(&ranges)?;

    // [ cells ]
    buffer.write(&cells)?;

    Ok(())
}

pub fn decompress<'a, const COLUMNS: usize, Buffer>(
    buffer: &'a Buffer,
) -> Result<Rows<'a, COLUMNS>, std::io::Error>
where
    Buffer: AsRef<[u8]>,
{
    let mut cursor = Cursor::new(buffer.as_ref());

    let data_length = leb128::read::unsigned(&mut cursor).map_err(into_io_error)?;
    let data_range = cursor.position() as usize..(cursor.position() + data_length) as usize;
    let data = &buffer.as_ref()[data_range];

    cursor.seek(SeekFrom::Start(cursor.position() + data_length))?;

    let ranges_length = leb128::read::unsigned(&mut cursor).map_err(into_io_error)?;
    let ranges_range = cursor.position() as usize..(cursor.position() + ranges_length) as usize;
    let mut range_data = Cursor::new(&buffer.as_ref()[ranges_range]);
    let mut ranges = Vec::new();

    while (range_data.position() as usize) < range_data.get_ref().as_ref().len() {
        ranges.push((
            leb128::read::unsigned(&mut range_data).map_err(into_io_error)? as usize,
            leb128::read::unsigned(&mut range_data).map_err(into_io_error)? as usize,
        ))
    }

    cursor.seek(SeekFrom::Start(cursor.position() + ranges_length))?;

    Ok(Rows {
        data,
        ranges,
        cells: cursor,
    })
}

pub struct Rows<'a, const WIDTH: usize> {
    data: &'a [u8],
    ranges: Vec<(usize, usize)>,
    cells: Cursor<&'a [u8]>,
}

impl<'a, const WIDTH: usize> Iterator for Rows<'a, WIDTH> {
    type Item = [&'a [u8]; WIDTH];

    fn next(&mut self) -> Option<Self::Item> {
        let mut row: [&'a [u8]; WIDTH] = [&[]; WIDTH];
        // println!("{:#?}", self.cells);
        for cell_index in 0..WIDTH {
            let Some(index) = leb128::read::unsigned(&mut self.cells).ok() else {
                return None;
            };

            // println!("{:#?}", self.ranges);
            let (data_index, data_length) = self.ranges[index as usize];
            let cell = &self.data[data_index..(data_index + data_length)];

            row[cell_index] = cell;
        }
        Some(row)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use anyhow::Result;
    use base58::ToBase58;
    use rand::Rng;
    use serde::{Deserialize, Serialize};

    use super::{Columnar, Rows, compress, decompress};

    #[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
    struct ExampleColumnar {
        rows: Vec<ExampleRow>,
    }

    #[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
    struct ExampleRow {
        pub a: String,
        pub b: Vec<u8>,
        pub c: [u8; 8],
        pub d: Vec<u8>,
    }

    impl<'a> Columnar<'a, 4> for ExampleColumnar {
        fn cells(&'a self) -> impl Iterator<Item = &'a [u8]> {
            self.rows.iter().flat_map(|row| {
                [
                    row.a.as_bytes(),
                    row.b.as_ref(),
                    row.c.as_ref(),
                    row.d.as_ref(),
                ]
            })
        }
    }

    impl<'a> TryFrom<Rows<'a, 4>> for ExampleColumnar {
        type Error = std::io::Error;

        fn try_from(value: Rows<'a, 4>) -> std::result::Result<Self, Self::Error> {
            let mut rows = Vec::new();

            for row in value {
                rows.push(ExampleRow {
                    a: String::from_utf8(row[0].into()).unwrap(),
                    b: row[1].into(),
                    c: row[2].try_into().unwrap(),
                    d: row[3].try_into().unwrap(),
                })
            }

            Ok(ExampleColumnar { rows })
        }
    }

    fn generate_columnar(row_count: usize) -> ExampleColumnar {
        let mut rows = Vec::with_capacity(row_count);

        let mut ids = Vec::<[u8; 32]>::new();

        for _ in 0..(row_count / 8) {
            ids.push(rand::thread_rng().r#gen())
        }

        for i in 0..row_count {
            let id = ids[i % ids.len()];
            let predicate = i % 3;

            rows.push(ExampleRow {
                a: format!("{}", id.to_base58()),
                b: vec![0, 1, 2, predicate as u8],
                c: rand::thread_rng().r#gen(),
                d: [id.as_ref(), predicate.to_le_bytes().as_ref()].concat(),
            })
        }

        ExampleColumnar { rows }
    }

    use brotli::CompressorWriter;

    fn brotli_compress(bytes: &[u8]) -> Vec<u8> {
        let mut compressed = Cursor::new(Vec::new());
        {
            let mut writer = CompressorWriter::new(&mut compressed, 4096, 11, 20);
            writer.write_all(bytes).unwrap();
        }
        compressed.into_inner()
    }

    #[test]
    pub fn it_encodes_and_decodes_a_columnar_struct() -> Result<()> {
        let expected_columnar = generate_columnar(1024);

        let mut compressed = Vec::new();
        compress(&expected_columnar, &mut compressed)?;

        let cbor = serde_ipld_dagcbor::to_vec(&expected_columnar)?;

        let rows = decompress::<4, _>(&compressed)?;

        let decompressed_columnar = ExampleColumnar::try_from(rows)?;

        assert_eq!(expected_columnar, decompressed_columnar);

        let cbor_brotli = brotli_compress(&cbor);
        let compressed_brotli = brotli_compress(&compressed);

        println!("CBOR: {}B", cbor.len());
        println!("CBOR + Brotli: {}B", cbor_brotli.len());
        println!("Column-compressed: {}B", compressed.len());
        println!("Column-compressed + Brotli: {}B", compressed_brotli.len());

        Ok(())
    }
}
