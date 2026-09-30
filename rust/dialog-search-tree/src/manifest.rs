//! The format manifest carried by every tree node.
//!
//! The tree's format parameters — the branching parameter, the separator-length
//! bound, the value inline-vs-spill threshold, the pacing knobs — determine
//! node bytes, and node bytes are the content address, so those parameters are
//! part of the format. Keeping them only in code would let two peers on
//! different builds silently produce non-convergent trees for the same data.
//! The manifest makes them **data**: it is carried by every node, so any node
//! hash stays a complete, self-describing tree root (the differ, structural
//! sharing, and `from_hash` all rely on a bare node hash being a usable root).
//!
//! # Encoding
//!
//! A manifest is encoded as a sequence of fields, each `code, length, value`,
//! every integer a [bijoux](https://crates.io/crates/bijoux) (bijou64) varint,
//! so each number has exactly one encoding. Codes come from the code table in
//! `format/table.csv`. The encoding is canonical: codes strictly ascending, and
//! a field equal to its table default is omitted, so the default manifest
//! encodes to no bytes at all.
//!
//! A field this build does not know is kept, not dropped, and written back
//! unchanged ([`Extensions`]), so an older program editing a newer peer's tree
//! does not strip its settings. What an unknown field means for this build is
//! carried by its code: an **even** code only shapes the tree, and ignoring it
//! costs at worst a tree shaped differently from its canonical form; an
//! **odd** code is critical — it changes how data is read — and a tree
//! carrying one this build does not know cannot be read by it.
//!
//! Nodes written before this encoding (the legacy layout) carry the manifest
//! as a fixed rkyv struct, [`LegacyManifest`], which reads into the same
//! [`Manifest`] value.
//!
//! Every edit runs under the manifest of the tree it edits: an edit over a
//! stored root adopts the manifest that root carries (see
//! `TransientTree::load`), and a new tree states its manifest when it is
//! created. [`Manifest::default`] is only the format a new tree starts with;
//! code working with an existing tree reads that tree's manifest.

use std::sync::Arc;

use bijoux::{Decode as _, Encode as _};
use rkyv::{Archive, Deserialize, Serialize};

use crate::DialogSearchTreeError;

/// Manifest field codes, as assigned in `format/table.csv`. Odd codes are
/// critical (they change how data is read), even codes only shape the tree.
mod code {
    pub const INLINE_N: u64 = 0x01;
    pub const FANOUT_N: u64 = 0x02;
    pub const SPILL_PREFIX: u64 = 0x03;
    pub const MAX_SEPARATOR: u64 = 0x04;
    pub const MAX_SEGMENT: u64 = 0x06;
    pub const FRAME_CEILING_FACTOR: u64 = 0x08;
    pub const ANCHOR_SELECTOR: u64 = 0x0a;
    pub const ENTRY_OVERHEAD: u64 = 0x0c;
    pub const KEY_OVERHEAD: u64 = 0x0e;
    pub const LINK_OVERHEAD: u64 = 0x10;

    /// Whether a field with this code must be understood to read the tree.
    pub fn is_critical(code: u64) -> bool {
        code % 2 == 1
    }
}

/// The format parameters of a tree, carried by every node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// Branching parameter `n`; expected fanout is `2^n`.
    pub fanout_n: u8,
    /// Keys longer than this never become boundaries (separator bound).
    pub max_separator: u32,
    /// Values longer than this spill to the block store, leaving a key-prefix
    /// plus whole-value hash in the key.
    pub inline_n: u32,
    /// How many leading raw value bytes a spilled value's key carries as its
    /// order-preserving prefix.
    pub spill_prefix: u16,
    /// Node weight target between coin-decided cuts; 0 turns byte pacing
    /// off. A run between accepted seams whose summed entry weight exceeds
    /// it is force-split at deterministic, leaf-level-only positions.
    pub max_segment: u32,
    /// Hard ceiling on a frame's weight, as a multiple of `max_segment`; 0
    /// disables it. A frame — the entries between coin-decided cuts — over
    /// `frame_ceiling_factor * max_segment` is force-split at deterministic,
    /// leaf-level-only accepted seams.
    pub frame_ceiling_factor: u32,
    /// Which candidate seam a forced cut anchors at: 0 = rendezvous
    /// (hash-minimal), 1 = hybrid (shortest-separator class, then
    /// hash-minimal within it).
    pub anchor_selector: u32,
    /// Weight charged per leaf entry beyond its key bytes and its value's
    /// payload weight: the columnar bookkeeping each entry costs in an
    /// encoded leaf (front-coding offsets, dictionary and value-table
    /// framing, polarity). Calibrated against measured leaf encodings on the
    /// SE dataset: without it encoded bytes drifted to 1.85x the metered
    /// weight at p90; with it bytes/weight is p50 1.02 / p90 1.05, so
    /// `max_segment` and the frame ceiling denominate in effective bytes.
    /// Buffered ops are metered the same way for the buffer byte cap.
    pub entry_overhead: u32,
    /// Weight the per-key cut floor charges beyond a key's bytes, where the
    /// value's payload is not in hand: a stand-in for the value slot and the
    /// per-entry encoding. Lower than `entry_overhead` plus any payload, so
    /// the floor never predicts a cut the full charge would not make.
    pub key_overhead: u32,
    /// Weight charged per index link beyond its separator bytes and the
    /// 32-byte child hash: per-link encoding overhead (offsets, front-coding
    /// bookkeeping).
    pub link_overhead: u32,
    /// Fields this build does not know, kept so they are written back.
    pub extensions: Extensions,
}

/// Manifest fields this build does not know, by code, with their raw value
/// bytes. Only even (shape) codes can be here: a manifest carrying an unknown
/// odd (critical) code does not decode. They shape nothing locally; they are
/// kept so a tree keeps them across this build's edits.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Extensions(Option<Arc<[Field]>>);

/// One manifest field kept as written: its code and raw value bytes.
type Field = (u64, Box<[u8]>);

impl Extensions {
    /// The fields, in ascending code order.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &[u8])> {
        self.0
            .iter()
            .flat_map(|fields| fields.iter())
            .map(|(code, value)| (*code, value.as_ref()))
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    fn from_fields(fields: Vec<Field>) -> Self {
        if fields.is_empty() {
            Self(None)
        } else {
            Self(Some(fields.into()))
        }
    }
}

/// The code table's defaults: what a missing field means. Only the codec uses
/// these, to decide what to omit and what an absent field stands for; a new
/// tree takes [`Manifest::default`], which the experiment environment can
/// override.
const TABLE_DEFAULTS: [(u64, u64); 10] = [
    (code::INLINE_N, 4096),
    (code::FANOUT_N, 8),
    (code::SPILL_PREFIX, 64),
    (code::MAX_SEPARATOR, 512),
    (code::MAX_SEGMENT, 65536),
    (code::FRAME_CEILING_FACTOR, 3),
    (code::ANCHOR_SELECTOR, 1),
    (code::ENTRY_OVERHEAD, 64),
    (code::KEY_OVERHEAD, 32),
    (code::LINK_OVERHEAD, 16),
];

/// The table default for a known field `code`.
fn table_default(code: u64) -> Option<u64> {
    TABLE_DEFAULTS
        .iter()
        .find(|(known, _)| *known == code)
        .map(|(_, default)| *default)
}

impl Manifest {
    /// The manifest every field of which is its code-table default: the one
    /// that encodes to no bytes.
    fn from_table() -> Self {
        let mut manifest = Self {
            fanout_n: 0,
            max_separator: 0,
            inline_n: 0,
            spill_prefix: 0,
            max_segment: 0,
            frame_ceiling_factor: 0,
            anchor_selector: 0,
            entry_overhead: 0,
            key_overhead: 0,
            link_overhead: 0,
            extensions: Extensions::default(),
        };
        for (code, default) in TABLE_DEFAULTS {
            manifest
                .set_known(code, default)
                .expect("the table defaults fit their fields");
        }
        manifest
    }

    /// Whether this manifest encodes to no fields: every known field at its
    /// table default and no unknown ones. Compares against the table's
    /// constants directly, cheap enough to ask for every node written.
    #[inline]
    pub(crate) fn is_fieldless(&self) -> bool {
        self.extensions.is_empty() && self.known_fields() == TABLE_DEFAULTS
    }

    /// The known fields as `(code, value)` pairs, in ascending code order.
    fn known_fields(&self) -> [(u64, u64); 10] {
        [
            (code::INLINE_N, u64::from(self.inline_n)),
            (code::FANOUT_N, u64::from(self.fanout_n)),
            (code::SPILL_PREFIX, u64::from(self.spill_prefix)),
            (code::MAX_SEPARATOR, u64::from(self.max_separator)),
            (code::MAX_SEGMENT, u64::from(self.max_segment)),
            (
                code::FRAME_CEILING_FACTOR,
                u64::from(self.frame_ceiling_factor),
            ),
            (code::ANCHOR_SELECTOR, u64::from(self.anchor_selector)),
            (code::ENTRY_OVERHEAD, u64::from(self.entry_overhead)),
            (code::KEY_OVERHEAD, u64::from(self.key_overhead)),
            (code::LINK_OVERHEAD, u64::from(self.link_overhead)),
        ]
    }

    /// Sets the known field `code` to `value`, refusing a value its type
    /// cannot hold. Returns `false` for a code this build does not know.
    fn set_known(&mut self, code: u64, value: u64) -> Result<bool, DialogSearchTreeError> {
        fn fit<T: TryFrom<u64>>(code: u64, value: u64) -> Result<T, DialogSearchTreeError> {
            T::try_from(value).map_err(|_| {
                DialogSearchTreeError::Encoding(format!(
                    "Manifest field {code:#x} holds {value}, out of its range"
                ))
            })
        }
        match code {
            code::INLINE_N => self.inline_n = fit(code, value)?,
            code::FANOUT_N => self.fanout_n = fit(code, value)?,
            code::SPILL_PREFIX => self.spill_prefix = fit(code, value)?,
            code::MAX_SEPARATOR => self.max_separator = fit(code, value)?,
            code::MAX_SEGMENT => self.max_segment = fit(code, value)?,
            code::FRAME_CEILING_FACTOR => self.frame_ceiling_factor = fit(code, value)?,
            code::ANCHOR_SELECTOR => self.anchor_selector = fit(code, value)?,
            code::ENTRY_OVERHEAD => self.entry_overhead = fit(code, value)?,
            code::KEY_OVERHEAD => self.key_overhead = fit(code, value)?,
            code::LINK_OVERHEAD => self.link_overhead = fit(code, value)?,
            _ => return Ok(false),
        }
        Ok(true)
    }

    /// Appends this manifest's canonical encoding to `out`: every field that
    /// differs from its table default, known and kept alike, in ascending
    /// code order, each as `code, length, value`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let mut fields: Vec<(u64, Vec<u8>)> = self
            .known_fields()
            .into_iter()
            .filter(|(code, value)| table_default(*code) != Some(*value))
            .map(|(code, value)| {
                let mut bytes = Vec::new();
                value.encode(&mut bytes);
                (code, bytes)
            })
            .collect();
        fields.extend(
            self.extensions
                .iter()
                .map(|(code, value)| (code, value.to_vec())),
        );
        fields.sort_by_key(|(code, _)| *code);
        for (code, value) in fields {
            code.encode(out);
            (value.len() as u64).encode(out);
            out.extend_from_slice(&value);
        }
    }

    /// Decodes a manifest from its canonical encoding (see
    /// [`encode`](Self::encode)).
    ///
    /// Refuses bytes that are not canonical — codes out of order or
    /// repeated, a length running past the end, a known field whose value is
    /// not one exact integer or equals its default — and a critical (odd)
    /// field this build does not know. Unknown shape (even) fields are kept.
    pub fn decode(mut bytes: &[u8]) -> Result<Self, DialogSearchTreeError> {
        fn malformed(what: &str) -> DialogSearchTreeError {
            DialogSearchTreeError::Encoding(format!("Malformed manifest: {what}"))
        }
        let mut manifest = Self::from_table();
        let mut unknown: Vec<Field> = Vec::new();
        let mut previous: Option<u64> = None;
        while !bytes.is_empty() {
            let (code, read) = u64::decode(bytes).map_err(|_| malformed("a field code"))?;
            bytes = &bytes[read..];
            if previous.is_some_and(|previous| code <= previous) {
                return Err(malformed("field codes out of order"));
            }
            previous = Some(code);
            let (length, read) = u64::decode(bytes).map_err(|_| malformed("a field length"))?;
            bytes = &bytes[read..];
            let value = usize::try_from(length)
                .ok()
                .and_then(|length| bytes.get(..length))
                .ok_or_else(|| malformed("a field running past the end"))?;
            bytes = &bytes[value.len()..];

            match table_default(code) {
                Some(default) => {
                    let integer = u64::decode(value)
                        .ok()
                        .filter(|(_, read)| *read == value.len())
                        .map(|(integer, _)| integer)
                        .ok_or_else(|| malformed("a field value that is not one integer"))?;
                    if integer == default {
                        return Err(malformed("a field written with its default value"));
                    }
                    manifest.set_known(code, integer)?;
                }
                None if code::is_critical(code) => {
                    return Err(DialogSearchTreeError::Encoding(format!(
                        "The tree carries critical manifest field {code:#x}, which this build \
                         does not know how to read"
                    )));
                }
                None => unknown.push((code, value.into())),
            }
        }
        manifest.extensions = Extensions::from_fields(unknown);
        Ok(manifest)
    }

    /// The geometric split factor `m = 2^n` that the boundary coin uses. This
    /// is the effective average branching factor of the tree.
    ///
    /// Clamped so `n` in `1..=63` maps to a real `u64` factor; `n = 0` would
    /// mean fanout 1 (no branching) and is disallowed, and `n >= 64` would
    /// overflow, so both saturate to the representable extremes.
    pub fn branch_factor(&self) -> u64 {
        match self.fanout_n {
            0 => 2,
            n if n >= 64 => u64::MAX,
            n => 1u64 << n,
        }
    }

    /// The effective frame ceiling in weighted bytes:
    /// `frame_ceiling_factor * max_segment`. Zero — disabled — when either
    /// knob is zero, so the ceiling can never outlive the coin it bounds.
    pub fn frame_ceiling(&self) -> usize {
        self.frame_ceiling_factor as usize * self.max_segment as usize
    }

    /// [`entry_overhead`](Self::entry_overhead) as a weight.
    #[inline]
    pub fn entry_overhead(&self) -> usize {
        self.entry_overhead as usize
    }

    /// [`key_overhead`](Self::key_overhead) as a weight.
    #[inline]
    pub fn key_overhead(&self) -> usize {
        self.key_overhead as usize
    }

    /// [`link_overhead`](Self::link_overhead) as a weight.
    #[inline]
    pub fn link_overhead(&self) -> usize {
        self.link_overhead as usize
    }
}

/// The format a new tree is created under.
///
/// Only for choosing the format of a tree that does not exist yet. An
/// existing tree's format is the manifest its nodes carry (read with
/// [`PersistentTree::manifest`](crate::PersistentTree::manifest)), which may
/// differ from this in any field, so code working with a tree reads that and
/// never these values.
impl Default for Manifest {
    fn default() -> Self {
        // Experiment plumbing for the boundary-policy arms (see
        // notes/boundary-policy-experiment.md): the manifest a fresh tree is
        // created under can be overridden through the environment, so the
        // whole artifact stack runs a capture under an arm's format without
        // threading configuration through every layer. Unset variables leave
        // the table defaults untouched; existing trees always keep the
        // manifest their root node carries.
        //
        // Read once per process: tree creation can sit on a per-commit path,
        // and the environment scan showed up as ~4% of a profiled commit
        // before this memo. The environment of a running process does not
        // change underneath it.
        //
        // `DIALOG_TREE_FANOUT_N` overrides the branching parameter `n` for
        // fresh trees (clamped to the representable 0..=63; see
        // `branch_factor`), so the sync soak harness can sweep expected
        // fanout (e.g. 5 = 32, 8 = 256) across processes without a code
        // change.
        static DEFAULT: std::sync::OnceLock<Manifest> = std::sync::OnceLock::new();
        DEFAULT
            .get_or_init(|| {
                let table = Self::from_table();
                Self {
                    fanout_n: env_override("DIALOG_TREE_FANOUT_N", u32::from(table.fanout_n))
                        .min(63) as u8,
                    inline_n: env_override("DIALOG_TREE_INLINE_N", table.inline_n),
                    max_segment: env_override("DIALOG_TREE_MAX_SEGMENT", table.max_segment),
                    frame_ceiling_factor: env_override(
                        "DIALOG_TREE_CEILING_FACTOR",
                        table.frame_ceiling_factor,
                    ),
                    anchor_selector: env_override(
                        "DIALOG_TREE_ANCHOR_SELECTOR",
                        table.anchor_selector,
                    ),
                    ..table
                }
            })
            .clone()
    }
}

/// Reads a `u32` manifest override from the environment, falling back to the
/// table value when the variable is unset or unparsable. On targets without
/// an environment (wasm) the fallback always wins.
fn env_override(name: &str, fallback: u32) -> u32 {
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(fallback)
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = name;
        fallback
    }
}

/// The manifest as nodes in the legacy (untagged) layout carry it: a fixed
/// rkyv struct inlined into the node body. Read only, to decode those nodes;
/// nothing writes it.
///
/// Its layout is frozen: it is part of how every legacy node archives, so a
/// change here would stop those nodes from reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedLegacyManifest)]
pub struct LegacyManifest {
    /// The legacy format version. Version 1 is the only one ever written.
    pub version: u8,
    /// See [`Manifest::fanout_n`].
    pub fanout_n: u8,
    /// See [`Manifest::max_separator`].
    pub max_separator: u32,
    /// See [`Manifest::inline_n`].
    pub inline_n: u32,
    /// See [`Manifest::spill_prefix`].
    pub spill_prefix: u16,
    /// See [`Manifest::max_segment`].
    pub max_segment: u32,
    /// See [`Manifest::frame_ceiling_factor`].
    pub frame_ceiling_factor: u32,
    /// See [`Manifest::anchor_selector`].
    pub anchor_selector: u32,
}

impl From<LegacyManifest> for Manifest {
    /// The legacy header's fields, with everything it did not carry (the
    /// weight overheads, which version 1 fixed at the table defaults) taken
    /// from the code table. A legacy version other than 1 was never written;
    /// it reads the same way rather than being refused.
    fn from(legacy: LegacyManifest) -> Self {
        Self {
            fanout_n: legacy.fanout_n,
            max_separator: legacy.max_separator,
            inline_n: legacy.inline_n,
            spill_prefix: legacy.spill_prefix,
            max_segment: legacy.max_segment,
            frame_ceiling_factor: legacy.frame_ceiling_factor,
            anchor_selector: legacy.anchor_selector,
            ..Self::from_table()
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]
    // The dialog_common::test macro requires async test fns; these pure tests
    // await nothing.
    #![allow(clippy::unused_async)]

    use super::{Extensions, Manifest, TABLE_DEFAULTS, code};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    fn table() -> Manifest {
        Manifest::from_table()
    }

    fn encoded(manifest: &Manifest) -> Vec<u8> {
        let mut out = Vec::new();
        manifest.encode(&mut out);
        out
    }

    /// `n` maps to `2^n`, and the default gives the intended fanout.
    #[dialog_common::test]
    async fn it_maps_fanout_n_to_two_to_the_n() -> anyhow::Result<()> {
        let with = |fanout_n| Manifest {
            fanout_n,
            ..table()
        };
        assert_eq!(with(8).branch_factor(), 256);
        assert_eq!(with(1).branch_factor(), 2);
        assert_eq!(with(10).branch_factor(), 1024);
        // Degenerate n saturate rather than overflow or divide by one.
        assert_eq!(with(0).branch_factor(), 2);
        assert_eq!(with(200).branch_factor(), u64::MAX);
        Ok(())
    }

    /// The table-default manifest encodes to nothing, and a field that
    /// differs is written as `code, length, value`.
    #[dialog_common::test]
    async fn it_omits_default_fields() -> anyhow::Result<()> {
        assert!(encoded(&table()).is_empty());
        let narrow = Manifest {
            fanout_n: 4,
            ..table()
        };
        assert_eq!(encoded(&narrow), vec![0x02, 0x01, 0x04]);
        assert_eq!(Manifest::decode(&[])?, table());
        assert_eq!(Manifest::decode(&[0x02, 0x01, 0x04])?, narrow);
        Ok(())
    }

    /// Every field round-trips, in ascending code order, whatever order the
    /// struct lists them in.
    #[dialog_common::test]
    async fn it_round_trips_every_field() -> anyhow::Result<()> {
        let manifest = Manifest {
            fanout_n: 5,
            max_separator: 300,
            inline_n: 70_000,
            spill_prefix: 32,
            max_segment: 0,
            frame_ceiling_factor: 0,
            anchor_selector: 0,
            entry_overhead: 80,
            key_overhead: 40,
            link_overhead: 20,
            extensions: Extensions::default(),
        };
        let bytes = encoded(&manifest);
        assert_eq!(Manifest::decode(&bytes)?, manifest);
        let codes: Vec<u8> = {
            let mut codes = Vec::new();
            let mut rest = bytes.as_slice();
            while !rest.is_empty() {
                use bijoux::Decode as _;
                let (code, read) = u64::decode(rest)?;
                rest = &rest[read..];
                let (length, read) = u64::decode(rest)?;
                rest = &rest[read + length as usize..];
                codes.push(code as u8);
            }
            codes
        };
        let mut sorted = codes.clone();
        sorted.sort_unstable();
        assert_eq!(codes, sorted);
        assert_eq!(codes.len(), 10);
        Ok(())
    }

    /// Bytes that are not the canonical encoding are refused: codes out of
    /// order or repeated, a length past the end, a default written out, a
    /// value that is not one exact integer, a value out of the field's range.
    #[dialog_common::test]
    async fn it_refuses_non_canonical_encodings() -> anyhow::Result<()> {
        for bytes in [
            vec![0x04, 0x01, 0x05, 0x02, 0x01, 0x04],
            vec![0x02, 0x01, 0x04, 0x02, 0x01, 0x05],
            vec![0x02, 0x05, 0x04],
            vec![0x02, 0x01, 0x08],
            vec![0x02, 0x02, 0x04, 0x00],
            vec![0x02, 0x02, 0xf8, 0x08],
        ] {
            assert!(Manifest::decode(&bytes).is_err(), "{bytes:02x?}");
        }
        Ok(())
    }

    /// An unknown shape (even) field is kept and written back in order; an
    /// unknown critical (odd) field refuses the manifest.
    #[dialog_common::test]
    async fn it_keeps_unknown_shape_fields_and_refuses_unknown_critical_ones() -> anyhow::Result<()>
    {
        let bytes = vec![0x02, 0x01, 0x04, 0x42, 0x02, 0xaa, 0xbb];
        let manifest = Manifest::decode(&bytes)?;
        assert_eq!(manifest.fanout_n, 4);
        assert_eq!(
            manifest.extensions.iter().collect::<Vec<_>>(),
            vec![(0x42, &[0xaa, 0xbb][..])]
        );
        assert_eq!(encoded(&manifest), bytes, "written back unchanged");

        assert!(Manifest::decode(&[0x43, 0x01, 0x00]).is_err());
        Ok(())
    }

    /// Integers are bijoux's (bijou64) encoding, byte for byte, at every
    /// length boundary: the node format depends on it, so a crate release
    /// that changed it must fail here.
    #[dialog_common::test]
    async fn it_pins_the_integer_encoding() -> anyhow::Result<()> {
        use bijoux::Encode as _;
        for (value, bytes) in [
            (0u64, vec![0x00]),
            (247, vec![0xf7]),
            (248, vec![0xf8, 0x00]),
            (503, vec![0xf8, 0xff]),
            (504, vec![0xf9, 0x00, 0x00]),
            (66_039, vec![0xf9, 0xff, 0xff]),
            (66_040, vec![0xfa, 0x00, 0x00, 0x00]),
            // 0xff announces 8 bytes holding the value minus 72,340,172,838,076,920
            // (the count of every shorter encoding), big-endian.
            (
                u64::MAX,
                vec![0xff, 0xfe, 0xfe, 0xfe, 0xfe, 0xfe, 0xfe, 0xfe, 0x07],
            ),
        ] {
            let mut out = Vec::new();
            value.encode(&mut out);
            assert_eq!(out, bytes, "{value}");
        }
        Ok(())
    }

    /// The codes and defaults here are the ones `format/table.csv` assigns,
    /// and a field is critical exactly when its code is odd.
    #[dialog_common::test]
    async fn it_matches_the_code_table() -> anyhow::Result<()> {
        let table = include_str!("../format/table.csv");
        let mut rows = 0;
        for line in table.lines().skip(1) {
            let columns: Vec<&str> = line.splitn(6, ',').collect();
            let [name, tag, code, status, default, _] = columns[..] else {
                panic!("malformed table row: {line}");
            };
            if tag != "manifest" || status == "reserved" {
                continue;
            }
            let code = u64::from_str_radix(code.trim_start_matches("0x"), 16)?;
            let (known, value) = TABLE_DEFAULTS
                .iter()
                .find(|(known, _)| *known == code)
                .unwrap_or_else(|| panic!("{name} ({code:#x}) is in the table but not the code"));
            assert_eq!(*known, code);
            assert_eq!(value.to_string(), default, "{name}'s default");
            let critical = matches!(name, "inline_n" | "spill_prefix");
            assert_eq!(code::is_critical(code), critical, "{name}'s parity");
            rows += 1;
        }
        assert_eq!(
            rows,
            TABLE_DEFAULTS.len(),
            "every coded field is in the table"
        );
        Ok(())
    }
}
