//! The wire format of a sealed asset.
//!
//! ```text
//! version(1) ‖ content generation(32) ‖ salt(32) ‖ chunk₀ ‖ … ‖ chunkₙ
//! chunkᵢ = AES-256-GCM(key, nonceᵢ, plainᵢ, aad = header)
//! key    = keyed_hash(content_secret, salt)
//! nonceᵢ = i (8, big-endian) ‖ last (1) ‖ 0 (3)
//! ```
//!
//! An asset can be far larger than anything held in memory, and is read in
//! ranges, so it is sealed in [`CHUNK`]-byte pieces rather than whole. Every
//! piece but the last is full; the last holds the remainder, and an empty
//! asset is one empty last piece. Each piece's nonce names its position and
//! whether it ends the asset, so pieces cannot be reordered, and a sealed
//! asset cannot be cut short at a piece boundary without the cut showing.
//! Every piece authenticates the header, so the generation and salt cannot
//! be swapped either.
//!
//! The sealed asset is stored under the hash of its own bytes, like an
//! envelope. That address is what a tree records for the asset, inside a
//! sealed node, so only a party that can read the node learns where the
//! asset lives, and only one holding the content generation opens it.
//!
//! Because the layout depends only on the plaintext's length, the sealed
//! length and the sealed span of any plaintext range are known from the
//! length a tree records, without reading anything ([`sealed_len`],
//! [`span`]).

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use dialog_common::Blake3Hash;

use super::keys::{Access, Writer};
use crate::{EpochId, KeyringError};

/// The asset format this build writes and reads.
const VERSION: u8 = 1;

/// The authenticated header: version, content generation and salt.
pub const HEADER: usize = 1 + 32 + 32;

/// Plaintext bytes in every piece but the last.
pub const CHUNK: usize = 64 * 1024;

/// What sealing adds to each piece.
pub const TAG: usize = 16;

/// The number of pieces an asset of `size` plaintext bytes seals into.
#[must_use]
pub fn chunks(size: u64) -> u64 {
    size.div_ceil(CHUNK as u64).max(1)
}

/// The length of an asset of `size` plaintext bytes, sealed.
#[must_use]
pub fn sealed_len(size: u64) -> u64 {
    HEADER as u64 + size + chunks(size) * TAG as u64
}

/// The pieces covering a plaintext range of an asset of `size` bytes, and
/// where they sit in the sealed asset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Span {
    /// The first piece the range touches.
    pub first: u64,
    /// The number of pieces the range touches.
    pub count: u64,
    /// The sealed offset of the first piece.
    pub offset: u64,
    /// The sealed length of those pieces together.
    pub length: u64,
    /// Plaintext bytes to drop from the front of the first piece.
    pub skip: u64,
    /// Plaintext bytes the range holds.
    pub take: u64,
}

/// The pieces covering `length` plaintext bytes from `offset` (to the end
/// when `None`) of an asset of `size` bytes. A range reaching past the end
/// is cut at it; one starting at or past the end touches no piece.
#[must_use]
pub fn span(size: u64, offset: u64, length: Option<u64>) -> Span {
    let end = length.map_or(size, |length| offset.saturating_add(length).min(size));
    if offset >= end {
        return Span {
            first: 0,
            count: 0,
            offset: HEADER as u64,
            length: 0,
            skip: 0,
            take: 0,
        };
    }
    let chunk = CHUNK as u64;
    let first = offset / chunk;
    let last = (end - 1) / chunk;
    let sealed_offset = HEADER as u64 + first * (chunk + TAG as u64);
    let sealed_end = HEADER as u64 + last * (chunk + TAG as u64) + piece_len(size, last) as u64;
    Span {
        first,
        count: last - first + 1,
        offset: sealed_offset,
        length: sealed_end - sealed_offset,
        skip: offset - first * chunk,
        take: end - offset,
    }
}

/// Seals an asset piece by piece as its bytes arrive.
///
/// Write [`header`](Self::header) first, then whatever
/// [`update`](Self::update) returns as it returns it, then what
/// [`finish`](Self::finish) returns. A piece is sealed only once a byte
/// beyond it has arrived, so the last piece is always the one `finish`
/// seals.
pub struct AssetSealer {
    cipher: Aes256Gcm,
    header: [u8; HEADER],
    index: u64,
    pending: Vec<u8>,
}

impl AssetSealer {
    /// A sealer for content whose plaintext hashes to `reference`, with a
    /// salt derived from it: the same content seals to the same bytes.
    #[must_use]
    pub fn convergent(writer: &Writer, reference: &Blake3Hash) -> Self {
        Self::with_salt(writer, writer.asset_salt(reference))
    }

    /// A sealer with a random salt, for content whose hash is not known
    /// until all of it has been sealed.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::Entropy`] if no randomness is available.
    pub fn fresh(writer: &Writer) -> Result<Self, KeyringError> {
        let mut salt = [0u8; 32];
        getrandom::getrandom(&mut salt)
            .map_err(|error| KeyringError::Entropy(error.to_string()))?;
        Ok(Self::with_salt(writer, salt))
    }

    fn with_salt(writer: &Writer, salt: [u8; 32]) -> Self {
        let mut header = [0u8; HEADER];
        header[0] = VERSION;
        header[1..33].copy_from_slice(writer.content_generation().as_bytes());
        header[33..].copy_from_slice(&salt);
        Self {
            cipher: Aes256Gcm::new((&writer.asset_key(&salt)).into()),
            header,
            index: 0,
            pending: Vec::with_capacity(CHUNK),
        }
    }

    /// The header, which starts the sealed asset.
    #[must_use]
    pub fn header(&self) -> &[u8] {
        &self.header
    }

    /// Take `bytes`, returning every piece they complete, sealed.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::Crypto`] if the cipher fails.
    pub fn update(&mut self, mut bytes: &[u8]) -> Result<Vec<u8>, KeyringError> {
        let mut sealed = Vec::new();
        while !bytes.is_empty() {
            if self.pending.len() == CHUNK {
                let piece = std::mem::take(&mut self.pending);
                sealed.extend(self.seal(&piece, false)?);
                self.pending.reserve(CHUNK);
            }
            let room = CHUNK - self.pending.len();
            let (now, rest) = bytes.split_at(room.min(bytes.len()));
            self.pending.extend_from_slice(now);
            bytes = rest;
        }
        Ok(sealed)
    }

    /// Seal the last piece.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::Crypto`] if the cipher fails.
    pub fn finish(mut self) -> Result<Vec<u8>, KeyringError> {
        let piece = std::mem::take(&mut self.pending);
        self.seal(&piece, true)
    }

    fn seal(&mut self, piece: &[u8], last: bool) -> Result<Vec<u8>, KeyringError> {
        let sealed = self
            .cipher
            .encrypt(
                Nonce::from_slice(&nonce(self.index, last)),
                Payload {
                    msg: piece,
                    aad: &self.header,
                },
            )
            .map_err(|error| KeyringError::Crypto(error.to_string()))?;
        self.index += 1;
        Ok(sealed)
    }
}

/// Opens the pieces of one sealed asset.
pub struct AssetOpener {
    cipher: Aes256Gcm,
    header: [u8; HEADER],
    size: u64,
}

impl AssetOpener {
    /// An opener for the sealed asset starting with `header`, whose
    /// plaintext is `size` bytes.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::Malformed`] or
    /// [`KeyringError::UnsupportedVersion`] if `header` is not a sealed
    /// asset's, and [`KeyringError::MissingGeneration`] without its content
    /// generation.
    pub fn new(access: &Access, header: &[u8], size: u64) -> Result<Self, KeyringError> {
        let header: [u8; HEADER] = header.try_into().map_err(|_| KeyringError::Malformed)?;
        if header[0] != VERSION {
            return Err(KeyringError::UnsupportedVersion(header[0]));
        }
        let mut generation = [0u8; 32];
        generation.copy_from_slice(&header[1..33]);
        let mut salt = [0u8; 32];
        salt.copy_from_slice(&header[33..]);
        let key = access.asset_key(&EpochId::from(generation), &salt)?;
        Ok(Self {
            cipher: Aes256Gcm::new((&key).into()),
            header,
            size,
        })
    }

    /// The sealed length of piece `index`.
    #[must_use]
    pub fn piece_len(&self, index: u64) -> usize {
        piece_len(self.size, index)
    }

    /// Open piece `index`.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::Malformed`] if `index` is past the last
    /// piece, and [`KeyringError::Failed`] if the piece does not open as
    /// that piece of this asset.
    pub fn open(&self, index: u64, sealed: &[u8]) -> Result<Vec<u8>, KeyringError> {
        let count = chunks(self.size);
        if index >= count || sealed.len() != self.piece_len(index) {
            return Err(KeyringError::Malformed);
        }
        self.cipher
            .decrypt(
                Nonce::from_slice(&nonce(index, index + 1 == count)),
                Payload {
                    msg: sealed,
                    aad: &self.header,
                },
            )
            .map_err(|_| KeyringError::Failed)
    }
}

/// The sealed length of piece `index` of an asset of `size` bytes; the
/// last piece's for any index past it.
fn piece_len(size: u64, index: u64) -> usize {
    let chunk = CHUNK as u64;
    let last = chunks(size) - 1;
    let plain = if index < last {
        chunk
    } else {
        size - last * chunk
    };
    plain as usize + TAG
}

fn nonce(index: u64, last: bool) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[..8].copy_from_slice(&index.to_be_bytes());
    nonce[8] = u8::from(last);
    nonce
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layered::keys::{Level, LevelSecret};

    fn range() -> LevelSecret {
        LevelSecret::new(EpochId::from([1; 32]), [11; 32])
    }

    fn content() -> LevelSecret {
        LevelSecret::new(EpochId::from([2; 32]), [22; 32])
    }

    fn writer() -> Writer {
        Writer::new(range(), content())
    }

    fn member() -> Access {
        Access::content(Level::new().with(range()), Level::new().with(content()))
    }

    fn seal(sealer: AssetSealer, plain: &[u8], step: usize) -> Vec<u8> {
        let mut sealer = sealer;
        let mut sealed = sealer.header().to_vec();
        for piece in plain.chunks(step.max(1)) {
            sealed.extend(sealer.update(piece).expect("update"));
        }
        sealed.extend(sealer.finish().expect("finish"));
        sealed
    }

    fn open(sealed: &[u8], size: u64, offset: u64, length: Option<u64>) -> Vec<u8> {
        let opener = AssetOpener::new(&member(), &sealed[..HEADER], size).expect("opener");
        let span = span(size, offset, length);
        let mut at = span.offset as usize;
        let mut plain = Vec::new();
        for index in span.first..span.first + span.count {
            let len = opener.piece_len(index);
            plain.extend(opener.open(index, &sealed[at..at + len]).expect("piece"));
            at += len;
        }
        assert_eq!(at as u64, span.offset + span.length);
        plain
            .into_iter()
            .skip(span.skip as usize)
            .take(span.take as usize)
            .collect()
    }

    fn content_of(size: usize) -> Vec<u8> {
        (0..size).map(|i| (i % 251) as u8).collect()
    }

    /// Assets of every awkward length seal to their computed length,
    /// whatever sizes their bytes arrive in, and open back whole and in
    /// ranges, piece boundaries included.
    #[test]
    fn it_seals_and_opens_assets_whole_and_in_ranges() {
        for size in [0, 1, CHUNK - 1, CHUNK, CHUNK + 1, 3 * CHUNK, 3 * CHUNK + 7] {
            let plain = content_of(size);
            for step in [1000, CHUNK, 3 * CHUNK + 11] {
                let sealed = seal(AssetSealer::fresh(&writer()).expect("sealer"), &plain, step);
                assert_eq!(sealed.len() as u64, sealed_len(size as u64), "size {size}");
                let size = size as u64;
                assert_eq!(open(&sealed, size, 0, None), plain);
                for (offset, length) in [
                    (0, Some(1)),
                    (CHUNK as u64 - 1, Some(2)),
                    (CHUNK as u64, None),
                    (size / 2, Some(size / 3 + 1)),
                    (size.saturating_sub(1), Some(10)),
                    (size, None),
                ] {
                    let end = length.map_or(size, |length| (offset + length).min(size));
                    let expected = plain
                        .get(offset.min(size) as usize..end.max(offset).min(size) as usize)
                        .unwrap_or(&[]);
                    assert_eq!(
                        open(&sealed, size, offset, length),
                        expected,
                        "size {size}, range {offset}+{length:?}"
                    );
                }
            }
        }
    }

    /// Convergent sealing gives the same bytes for the same content;
    /// fresh sealing does not. Neither carries the plaintext.
    #[test]
    fn it_seals_convergently_or_freshly() {
        let plain = content_of(CHUNK + 100);
        let reference = Blake3Hash::hash(&plain);
        let one = seal(AssetSealer::convergent(&writer(), &reference), &plain, 4096);
        let two = seal(
            AssetSealer::convergent(&writer(), &reference),
            &plain,
            CHUNK,
        );
        let fresh = seal(AssetSealer::fresh(&writer()).expect("sealer"), &plain, 4096);
        assert_eq!(one, two);
        assert_ne!(one, fresh);
        for sealed in [&one, &fresh] {
            assert!(!sealed.windows(64).any(|window| window == &plain[..64]));
            assert!(
                !sealed
                    .windows(32)
                    .any(|window| window == reference.as_bytes())
            );
        }
    }

    /// A party without the content generation cannot open an asset; a
    /// piece moved, cut short, or under a swapped header fails rather
    /// than opening wrong.
    #[test]
    fn it_refuses_an_asset_without_its_generation_or_out_of_place() {
        let plain = content_of(2 * CHUNK + 5);
        let size = plain.len() as u64;
        let sealed = seal(
            AssetSealer::fresh(&writer()).expect("sealer"),
            &plain,
            CHUNK,
        );

        assert!(matches!(
            AssetOpener::new(&Access::ranges(Level::new().with(range())), &sealed[..HEADER], size),
            Err(KeyringError::MissingGeneration(generation)) if &generation == content().generation()
        ));

        let opener = AssetOpener::new(&member(), &sealed[..HEADER], size).expect("opener");
        let piece = CHUNK + TAG;
        let first = &sealed[HEADER..HEADER + piece];
        let second = &sealed[HEADER + piece..HEADER + 2 * piece];
        assert!(matches!(opener.open(1, first), Err(KeyringError::Failed)));
        assert!(matches!(opener.open(0, second), Err(KeyringError::Failed)));

        // Cut after the first piece: claiming that piece is the last fails.
        let cut = AssetOpener::new(&member(), &sealed[..HEADER], CHUNK as u64).expect("opener");
        assert!(matches!(cut.open(0, first), Err(KeyringError::Failed)));

        let other = seal(
            AssetSealer::fresh(&writer()).expect("sealer"),
            &plain,
            CHUNK,
        );
        let swapped = AssetOpener::new(&member(), &other[..HEADER], size).expect("opener");
        assert!(matches!(swapped.open(0, first), Err(KeyringError::Failed)));

        assert!(matches!(
            opener.open(3, first),
            Err(KeyringError::Malformed)
        ));
        assert!(matches!(
            AssetOpener::new(&member(), &sealed[..HEADER - 1], size),
            Err(KeyringError::Malformed)
        ));
    }
}
