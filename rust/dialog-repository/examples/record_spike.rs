//! A research spike for `research/record-values.md`: a record as its
//! attribute and value pairs laid out as key tails, against dag-cbor.
//!
//! Modes:
//!
//! - `revisions <steps> <seed> <out>`: replays a history of commits and
//!   pulls across three branches through the repository, then writes every
//!   revision record the tree holds, as the dag-cbor bytes stored, to
//!   `<out>` (each record length-prefixed, `u32` big-endian).
//! - `check <seeds>`: the canonical form's properties over generated
//!   records.
//! - `measure <revisions> <bindings.json> <rounds>`: sizes, the cost of
//!   reading one field, and a model of the single-file archive's tokens,
//!   for revision records and tonk-shaped view bindings.
//!
//! Run with `cargo run --release -p dialog-repository --features helpers
//! --example record_spike -- <mode> ...`. Nothing here is used by the
//! crate; it is measurement code, kept so the numbers can be reproduced.

#[cfg(not(target_arch = "wasm32"))]
#[path = "record_spike/check.rs"]
mod check;
#[cfg(not(target_arch = "wasm32"))]
#[path = "record_spike/codec.rs"]
mod codec;
#[cfg(not(target_arch = "wasm32"))]
#[path = "record_spike/data.rs"]
mod data;
#[cfg(not(target_arch = "wasm32"))]
#[path = "record_spike/measure.rs"]
mod measure;
#[cfg(not(target_arch = "wasm32"))]
#[path = "record_spike/merkle.rs"]
mod merkle;
#[cfg(not(target_arch = "wasm32"))]
#[path = "record_spike/variants.rs"]
mod variants;

/// A small deterministic generator, so a run replays from its seed.
#[cfg(not(target_arch = "wasm32"))]
pub struct Seeded(pub u64);

#[cfg(not(target_arch = "wasm32"))]
impl Seeded {
    pub fn step(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    pub fn below(&mut self, bound: usize) -> usize {
        (self.step() % bound as u64) as usize
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn arg<T: std::str::FromStr>(args: &[String], at: usize, what: &str) -> anyhow::Result<T> {
    use anyhow::Context as _;
    args.get(at)
        .with_context(|| format!("missing {what}"))?
        .parse()
        .map_err(|_| anyhow::anyhow!("{what} does not parse"))
}

#[cfg(not(target_arch = "wasm32"))]
fn main() -> anyhow::Result<()> {
    use anyhow::bail;
    use std::io::Write as _;
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("revisions") => {
            let steps: usize = arg(&args, 1, "steps")?;
            let seed: u64 = arg(&args, 2, "seed")?;
            let out: String = arg(&args, 3, "output path")?;
            let records = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(data::revisions(steps, seed))?;
            let mut file = std::fs::File::create(&out)?;
            for record in &records {
                file.write_all(&(record.len() as u32).to_be_bytes())?;
                file.write_all(record)?;
            }
            println!("{} revision records to {out}", records.len());
        }
        Some("check") => {
            let seeds: u64 = arg(&args, 1, "seed count")?;
            let (records, pairs) = check::run(seeds)?;
            println!("{records} records and {pairs} pairs of records hold every property");
        }
        Some("measure") => {
            let revisions: String = arg(&args, 1, "revisions path")?;
            let bindings: String = arg(&args, 2, "bindings path")?;
            let rounds: usize = arg(&args, 3, "rounds")?;
            measure::run(&revisions, &bindings, rounds)?;
        }
        _ => bail!(
            "usage: revisions <steps> <seed> <out> | check <seeds> | measure <revisions> <bindings.json> <rounds>"
        ),
    }
    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn main() {}
