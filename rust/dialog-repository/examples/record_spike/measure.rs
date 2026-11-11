//! Sizes, the cost of reading one field, and a model of the single-file
//! archive's tokens, for each dataset, printed as markdown tables.

use super::codec;
use super::data::{self, Bindings};
use anyhow::Result;
use dialog_corpus::history::RevisionRecord;
use std::collections::BTreeSet;
use std::hint::black_box;
use std::time::Instant;

/// One dataset: each record as stored today and as pairs.
struct Set {
    name: &'static str,
    cbor: Vec<Vec<u8>>,
    pairs: Vec<Vec<u8>>,
}

fn read_revisions(path: &str) -> Result<Vec<Vec<u8>>> {
    let bytes = std::fs::read(path)?;
    let mut out = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        let len = u32::from_be_bytes(bytes[at..at + 4].try_into()?) as usize;
        out.push(bytes[at + 4..at + 4 + len].to_vec());
        at += 4 + len;
    }
    Ok(out)
}

fn stats(sizes: &[usize]) -> (usize, usize, usize, usize, usize) {
    let mut sorted = sizes.to_vec();
    sorted.sort_unstable();
    let at = |q: f64| sorted[((sorted.len() - 1) as f64 * q).round() as usize];
    (sorted.iter().sum(), at(0.0), at(0.5), at(0.9), at(1.0))
}

fn sizes(sets: &[Set]) {
    println!("### Bytes per record\n");
    println!("| dataset | records | form | total | min | median | p90 | max | ÷ dag-cbor |");
    println!("|---|--:|---|--:|--:|--:|--:|--:|--:|");
    for set in sets {
        let cbor: Vec<usize> = set.cbor.iter().map(Vec::len).collect();
        let pairs: Vec<usize> = set.pairs.iter().map(Vec::len).collect();
        let (ct, c0, c5, c9, c1) = stats(&cbor);
        let (pt, p0, p5, p9, p1) = stats(&pairs);
        println!(
            "| {} | {} | dag-cbor | {ct} | {c0} | {c5} | {c9} | {c1} | 1.00 |",
            set.name,
            cbor.len()
        );
        println!(
            "| {} | {} | pairs | {pt} | {p0} | {p5} | {p9} | {p1} | {:.2} |",
            set.name,
            pairs.len(),
            pt as f64 / ct as f64
        );
    }
    println!();
}

/// Where the pairs' bytes go, and what writing each attribute once per
/// record would save: a repeat of an attribute anywhere in a record (its
/// nested records included) as a two-byte reference to its first use.
fn composition(sets: &[Set]) {
    println!("### Where a pairs record's bytes go\n");
    println!(
        "| dataset | attributes | value slots | record ends | total | repeated attributes | total, attributes once |"
    );
    println!("|---|--:|--:|--:|--:|--:|--:|");
    for set in sets {
        let (mut attributes, mut values, mut ends, mut repeated, mut repeats) = (0, 0, 0, 0, 0);
        fn walk(
            bytes: &[u8],
            seen: &mut BTreeSet<Vec<u8>>,
            a: &mut usize,
            v: &mut usize,
            e: &mut usize,
            r: &mut usize,
            n: &mut usize,
        ) {
            *e += 1;
            for part in codec::parts(bytes).expect("parts") {
                *a += part.attribute.len();
                if !seen.insert(part.attribute.to_vec()) {
                    *r += part.attribute.len();
                    *n += 1;
                }
                if part.slot[0] == dialog_corpus::ValueDataType::Record as u8 {
                    *v += 1;
                    walk(&part.slot[1..], seen, a, v, e, r, n);
                } else {
                    *v += part.slot.len();
                }
            }
        }
        for bytes in &set.pairs {
            let mut seen = BTreeSet::new();
            walk(
                bytes,
                &mut seen,
                &mut attributes,
                &mut values,
                &mut ends,
                &mut repeated,
                &mut repeats,
            );
        }
        let total = attributes + values + ends;
        println!(
            "| {} | {attributes} | {values} | {ends} | {total} | {repeated} | {} |",
            set.name,
            total - repeated + 2 * repeats
        );
    }
    println!();
}

/// Times `run` over every record, `repeat` times, in nanoseconds a record.
fn time(records: usize, repeat: usize, mut run: impl FnMut() -> usize) -> f64 {
    let start = Instant::now();
    let mut found = 0;
    for _ in 0..repeat {
        found += run();
    }
    black_box(found);
    start.elapsed().as_nanos() as f64 / (records * repeat) as f64
}

fn spread(samples: &mut [f64]) -> (f64, f64, f64) {
    samples.sort_by(|a, b| a.partial_cmp(b).expect("no NaN"));
    (
        samples[0],
        samples[samples.len() / 2],
        samples[samples.len() - 1],
    )
}

type Reader<'a> = (&'static str, &'static str, Box<dyn FnMut() -> usize + 'a>);

fn decoded(slots: Vec<&[u8]>) -> usize {
    slots.iter().filter_map(|s| codec::field_of(s)).count()
}

/// Each variant run once a round, in turn, `rounds` rounds.
fn reads(sets: &[Set], rounds: usize) {
    let (as_records, as_bytes, views) = (&sets[0], &sets[1], &sets[2]);
    let parent = codec::attribute_key("dialog.revision/parent");
    let issuer = codec::attribute_key("dialog.revision/issuer");
    let edition = codec::attribute_key("dialog.version/edition");
    let event = codec::attribute_key("tonk.view/event");
    let name = codec::attribute_key("tonk.event/name");
    let cbor = |b: &[u8]| RevisionRecord::try_from_bytes(b).expect("decodes");
    let mut readers: Vec<Reader> = vec![
        (
            "revision: its parents",
            "dag-cbor, decoded whole",
            Box::new(|| as_records.cbor.iter().map(|b| cbor(b).parents.len()).sum()),
        ),
        (
            "revision: its parents",
            "pairs, versions as bytes",
            Box::new(|| {
                as_bytes
                    .pairs
                    .iter()
                    .map(|b| decoded(codec::slots(b, &parent).expect("walks")))
                    .sum()
            }),
        ),
        (
            "revision: its parents",
            "pairs, versions as records",
            Box::new(|| {
                as_records
                    .pairs
                    .iter()
                    .map(|b| decoded(codec::slots(b, &parent).expect("walks")))
                    .sum()
            }),
        ),
        (
            "revision: its parents' editions",
            "dag-cbor, decoded whole",
            Box::new(|| {
                as_records
                    .cbor
                    .iter()
                    .map(|b| {
                        cbor(b)
                            .parents
                            .iter()
                            .map(|v| v.edition.key_bytes()[7] as usize)
                            .sum::<usize>()
                    })
                    .sum()
            }),
        ),
        (
            "revision: its parents' editions",
            "pairs, versions as records",
            Box::new(|| {
                as_records
                    .pairs
                    .iter()
                    .map(|b| {
                        codec::slots(b, &parent)
                            .expect("walks")
                            .iter()
                            .map(|slot| decoded(codec::slots(&slot[1..], &edition).expect("walks")))
                            .sum::<usize>()
                    })
                    .sum()
            }),
        ),
        (
            "revision: its issuer",
            "dag-cbor, decoded whole",
            Box::new(|| as_records.cbor.iter().map(|b| cbor(b).issuer.len()).sum()),
        ),
        (
            "revision: its issuer",
            "pairs, versions as records",
            Box::new(|| {
                as_records
                    .pairs
                    .iter()
                    .map(|b| decoded(codec::slots(b, &issuer).expect("walks")))
                    .sum()
            }),
        ),
        (
            "view: its event names",
            "dag-cbor, decoded whole",
            Box::new(|| {
                views
                    .cbor
                    .iter()
                    .map(|b| {
                        serde_ipld_dagcbor::from_slice::<Bindings>(b)
                            .expect("decodes")
                            .events
                            .len()
                    })
                    .sum()
            }),
        ),
        (
            "view: its event names",
            "pairs",
            Box::new(|| {
                views
                    .pairs
                    .iter()
                    .map(|b| {
                        codec::slots(b, &event)
                            .expect("walks")
                            .iter()
                            .map(|slot| decoded(codec::slots(&slot[1..], &name).expect("walks")))
                            .sum::<usize>()
                    })
                    .sum()
            }),
        ),
    ];
    // Records each reader walks, and how often a sample repeats them.
    let shape = [
        (as_records.cbor.len(), 20),
        (as_records.cbor.len(), 20),
        (as_records.cbor.len(), 20),
        (as_records.cbor.len(), 20),
        (as_records.cbor.len(), 20),
        (as_records.cbor.len(), 20),
        (as_records.cbor.len(), 20),
        (views.cbor.len(), 400),
        (views.cbor.len(), 400),
    ];
    let mut samples: Vec<Vec<f64>> = vec![Vec::new(); readers.len()];
    for _ in 0..rounds {
        for (index, (_, _, run)) in readers.iter_mut().enumerate() {
            let (records, repeat) = shape[index];
            samples[index].push(time(records, repeat, run));
        }
    }
    println!(
        "### Reading one field, ns a record ({rounds} rounds, interleaved; min / median / max)\n"
    );
    println!("| read | form | min | median | max |");
    println!("|---|---|--:|--:|--:|");
    for (index, (what, form, _)) in readers.iter().enumerate() {
        let (low, mid, high) = spread(&mut samples[index]);
        println!("| {what} | {form} | {low:.0} | {mid:.0} | {high:.0} |");
    }
    println!();
}

/// A token's cost in the file as it is (16-aligned, its length in the four
/// bytes before) and packed (a varint length, no padding).
fn aligned(len: usize) -> usize {
    (len + 4).div_ceil(16) * 16
}

fn packed(len: usize) -> usize {
    len + if len < 128 { 1 } else { 2 }
}

/// The parts of a pairs record the file would intern, recursively: each
/// attribute and each value slot; a nested record is its parts.
fn split(bytes: &[u8], tokens: &mut Vec<Vec<u8>>, records: &mut usize) {
    *records += 1;
    for part in codec::parts(bytes).expect("parts") {
        tokens.push(part.attribute.to_vec());
        if part.slot[0] == dialog_corpus::ValueDataType::Record as u8 {
            split(&part.slot[1..], tokens, records);
        } else {
            tokens.push(part.slot.to_vec());
        }
    }
}

struct Model {
    distinct: usize,
    token_bytes: usize,
    aligned: usize,
    packed: usize,
    references: usize,
}

fn model<'a>(units: impl Iterator<Item = &'a Vec<u8>>, per_unit_refs: &[usize]) -> Model {
    let distinct: BTreeSet<&Vec<u8>> = units.collect();
    let references: usize = per_unit_refs.iter().map(|n| n * 4).sum();
    Model {
        distinct: distinct.len(),
        token_bytes: distinct.iter().map(|t| t.len()).sum(),
        aligned: distinct.iter().map(|t| aligned(t.len())).sum::<usize>() + references,
        packed: distinct.iter().map(|t| packed(t.len())).sum::<usize>() + references,
        references,
    }
}

fn tokens(sets: &[Set]) {
    println!("### The file's tokens, modelled\n");
    println!(
        "Whole: each distinct record one token, a row naming it (4 bytes). Split: each distinct \
         attribute and value slot one token, a record a row of 4-byte names of its parts, a \
         nested record's parts in its parent's row. Aligned is the file as built (16-aligned, \
         a 4-byte length before each); packed is a varint length and no padding.\n"
    );
    println!("| dataset | layout | tokens | token bytes | names | aligned total | packed total |");
    println!("|---|---|--:|--:|--:|--:|--:|");
    for set in sets {
        let ones = vec![1; set.cbor.len()];
        for (label, units) in [("dag-cbor, whole", &set.cbor), ("pairs, whole", &set.pairs)] {
            let m = model(units.iter(), &ones);
            println!(
                "| {} | {label} | {} | {} | {} | {} | {} |",
                set.name, m.distinct, m.token_bytes, m.references, m.aligned, m.packed
            );
        }
        let mut all = Vec::new();
        let mut refs = Vec::new();
        for bytes in &set.pairs {
            let mut tokens = Vec::new();
            let mut nested = 0;
            split(bytes, &mut tokens, &mut nested);
            refs.push(tokens.len());
            all.extend(tokens);
        }
        let m = model(all.iter(), &refs);
        println!(
            "| {} | pairs, split | {} | {} | {} | {} | {} |",
            set.name, m.distinct, m.token_bytes, m.references, m.aligned, m.packed
        );
    }
    println!();
}

/// Whether a record's versions, written as bytes, are the file's version
/// tokens: a history key holds a version as its 40 raw bytes, a record as a
/// `bytes:` slot (the type byte, then the bytes escaped and ended).
fn version_tokens(set: &Set, keys: &BTreeSet<Vec<u8>>) {
    let bytes_kind = dialog_corpus::ValueDataType::Bytes as u8;
    let (mut values, mut same_token, mut same_payload) = (0, 0, 0);
    for record in &set.pairs {
        for part in codec::parts(record).expect("parts") {
            let names_a_version = part.attribute.starts_with(b"dialog.revision/parent")
                || part.attribute.starts_with(b"dialog.revision/skip");
            if part.slot[0] != bytes_kind || !names_a_version {
                continue;
            }
            values += 1;
            if keys.contains(&part.slot.to_vec()) {
                same_token += 1;
            }
            if let Some((raw, _)) = dialog_corpus::decode_bytes(&part.slot[1..])
                && keys.contains(&raw)
            {
                same_payload += 1;
            }
        }
    }
    println!(
        "### A record's versions against the history keys' version tokens\n\n\
         | versions in records | the same token | the same bytes once unescaped |\n|--:|--:|--:|\n\
         | {values} | {same_token} | {same_payload} |\n"
    );
}

pub fn run(revisions: &str, bindings: &str, rounds: usize) -> Result<()> {
    let revision_cbor = read_revisions(revisions)?;
    let encode_all = |versions: data::Versions| -> Result<Vec<Vec<u8>>> {
        revision_cbor
            .iter()
            .map(|b| data::revision_pairs(b, versions).map(|r| codec::encode(&r)))
            .collect()
    };
    let as_records = encode_all(data::Versions::Records)?;
    let as_bytes = encode_all(data::Versions::Bytes)?;
    let views = data::bindings(bindings)?;
    let view_cbor = views
        .iter()
        .map(serde_ipld_dagcbor::to_vec)
        .collect::<Result<Vec<_>, _>>()?;
    let view_pairs: Vec<Vec<u8>> = views
        .iter()
        .map(|b| codec::encode(&data::binding_pairs(b)))
        .collect();
    let mut keys = BTreeSet::new();
    for bytes in &revision_cbor {
        keys.extend(data::version_keys(bytes)?);
    }
    let issuers: BTreeSet<String> = revision_cbor
        .iter()
        .map(|b| {
            RevisionRecord::try_from_bytes(b)
                .map(|r| r.issuer)
                .map_err(|e| anyhow::anyhow!("{e:?}"))
        })
        .collect::<Result<_>>()?;
    println!(
        "revision records: {}, distinct issuers: {}, distinct versions named: {}; views with bindings: {}\n",
        revision_cbor.len(),
        issuers.len(),
        keys.len(),
        views.len()
    );
    for bytes in as_records.iter().chain(&as_bytes).chain(&view_pairs) {
        let record =
            codec::decode(bytes).ok_or_else(|| anyhow::anyhow!("a record does not decode"))?;
        anyhow::ensure!(
            &codec::encode(&record) == bytes,
            "a record does not re-encode"
        );
    }
    let sets = [
        Set {
            name: "revisions, versions as records",
            cbor: revision_cbor.clone(),
            pairs: as_records,
        },
        Set {
            name: "revisions, versions as bytes",
            cbor: revision_cbor,
            pairs: as_bytes,
        },
        Set {
            name: "tonk view bindings",
            cbor: view_cbor,
            pairs: view_pairs,
        },
    ];
    // With RECORD_SPIKE_DUMP set, each form's records are written there,
    // length-prefixed, for measuring outside (compression).
    if let Ok(dir) = std::env::var("RECORD_SPIKE_DUMP") {
        for (index, set) in sets.iter().enumerate() {
            for (form, records) in [("cbor", &set.cbor), ("pairs", &set.pairs)] {
                let mut out = Vec::new();
                for r in records {
                    out.extend_from_slice(&(r.len() as u32).to_be_bytes());
                    out.extend_from_slice(r);
                }
                std::fs::write(format!("{dir}/set{index}-{form}.bin"), out)?;
            }
        }
    }
    sizes(&sets);
    composition(&sets);
    tokens(&sets);
    version_tokens(&sets[1], &keys);
    reads(&sets, rounds);
    Ok(())
}
