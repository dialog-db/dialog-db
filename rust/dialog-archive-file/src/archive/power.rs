//! Power losses played on the file (see `file::disk`).
//!
//! A workload runs against a watched file: blocks appended in the turn and
//! into extents (another process holding the turn), folded in, a cell
//! updated after each round's blocks naming them, as a branch head names
//! its tree, its value spilling to a block when it outgrows the cell, and
//! a sync point now and then. Then, for every point in what the archive
//! wrote and several ways the disk could have kept it, the image is opened
//! as after a reboot and checked:
//!
//! - it opens;
//! - every block appended before the last sync point that finished reads
//!   back as itself;
//! - no block ever reads back as anything but itself;
//! - the cell holds a value it held at that sync point or was given after;
//! - every block the cell's value names reads back.
//!
//! The last two hold because a cell's every value is a record appended in
//! the writer's turn after what it names, as a commit in SQLite's
//! write-ahead log follows its pages (see the `cells` module). A failure
//! names the point and the seed that built its image.

use std::fs::OpenOptions;
use std::path::Path;

use dialog_common::{Blake3Hash, Buffer};
use dialog_effects::memory::Version;

use super::concurrency::hold;
use super::slot::{Slot, boot};
use super::{FileArchive, Options};
use crate::file::disk::{self, Draw};
use crate::locks;

fn block(name: &str) -> Buffer {
    Buffer::from(name.as_bytes().to_vec())
}

fn version(content: &[u8]) -> Version {
    Version::from(Blake3Hash::hash(content).as_bytes())
}

/// What the workload did, by how many of the archive's operations on the
/// file had happened when it did it.
#[derive(Default)]
struct Done {
    /// Each block, and the operations by the time its append returned.
    blocks: Vec<(String, usize)>,
    /// Each value the cell was given: the operations before the publish
    /// started, and by the time it returned.
    published: Vec<(String, usize, usize)>,
    /// The operations by the time each sync point returned.
    synced: Vec<usize>,
}

/// The cell's value for a round: the blocks it names, and, every other
/// round, a few hundred bytes more, so values of both sizes are written.
fn head(round: usize, names: &[String]) -> String {
    let mut value = format!("{round}|{}", names.join(","));
    if round % 2 == 1 {
        value.push('|');
        value.push_str(&"-".repeat(300));
    }
    value
}

fn named(value: &str) -> Vec<String> {
    value
        .split('|')
        .nth(1)
        .map(|names| names.split(',').map(String::from).collect())
        .unwrap_or_default()
}

fn workload(path: &Path, watch: &disk::Watch) -> anyhow::Result<Done> {
    let archive = FileArchive::open_with(
        path,
        Options {
            extent: 4096,
            ..Options::default()
        },
    )?;
    let mut done = Done::default();
    for round in 0..8 {
        // Every third round another process holds the turn, so the blocks
        // go into an extent, folded in by the next round's first append.
        let holder = if round % 3 == 1 {
            Some(hold(path, locks::TURN, true)?)
        } else {
            None
        };
        let mut names = Vec::new();
        for n in 0..4 {
            let name = format!("round {round} block {n}");
            archive.append("index", &[block(&name)])?;
            done.blocks.push((name.clone(), watch.len()));
            names.push(name);
        }
        if let Some(holder) = holder {
            holder.finish()?;
        }
        let value = head(round, &names);
        let before = watch.len();
        let current = archive.resolve_cell("local", "head")?;
        archive.publish_cell(
            "local",
            "head",
            value.as_bytes(),
            current.as_deref().map(version).as_ref(),
        )?;
        done.published.push((value, before, watch.len()));
        if round % 2 == 1 {
            archive.sync()?;
            done.synced.push(watch.len());
        }
    }
    // The power goes: nothing closes the file.
    archive
        .inner
        .dies
        .store(true, std::sync::atomic::Ordering::Relaxed);
    Ok(done)
}

/// Marks the image's slots as written in another boot, as they would be
/// after the power came back, so opening checks what it can no longer
/// trust. A slot torn by the power loss stays torn.
fn as_if_rebooted(path: &Path) -> anyhow::Result<()> {
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    for mut slot in Slot::read(&file)? {
        slot.boot = boot().wrapping_add(1);
        slot.write(&file)?;
    }
    Ok(())
}

/// What an image got wrong: of the file and its blocks, of the cell's
/// value, or of what the value names (its spilled block among them).
enum Wrong {
    File(String),
    Cell(String),
    Names(String),
}

/// What is wrong with the image the power loss after `at` operations
/// left, if anything.
fn check(image: &[u8], at: usize, done: &Done, path: &Path) -> anyhow::Result<Vec<Wrong>> {
    std::fs::write(path, image)?;
    as_if_rebooted(path)?;
    let archive = match FileArchive::open_with(
        path,
        Options {
            leave_extent: false,
            extent: 4096,
            ..Options::default()
        },
    ) {
        Ok(archive) => archive,
        Err(error) => return Ok(vec![Wrong::File(format!("does not open: {error}"))]),
    };
    let mut wrong = Vec::new();
    let synced = done.synced.iter().copied().filter(|&ops| ops <= at).max();
    let read = |name: &str| archive.get("index", block(name).blake3_hash());
    for (name, appended) in &done.blocks {
        let durable = synced.is_some_and(|synced| *appended <= synced);
        match read(name) {
            Ok(Some(bytes)) if bytes == name.as_bytes() => {}
            Ok(Some(_)) => wrong.push(Wrong::File(format!("{name} reads back as other bytes"))),
            Ok(None) if durable => wrong.push(Wrong::File(format!("{name}, synced, is gone"))),
            Ok(None) => {}
            Err(error) => wrong.push(Wrong::File(format!("{name} fails to read: {error}"))),
        }
    }
    // The value durable at the sync point, and those given after it that
    // had started by the power loss.
    let durable = synced.and_then(|synced| {
        done.published
            .iter()
            .rposition(|(_, _, returned)| *returned <= synced)
    });
    let allowed: Vec<&str> = done
        .published
        .iter()
        .enumerate()
        .filter(|(index, (_, started, _))| {
            durable.is_none_or(|durable| *index >= durable) && *started <= at
        })
        .map(|(_, (value, _, _))| value.as_str())
        .collect();
    match archive.resolve_cell("local", "head") {
        Ok(None) if durable.is_none() => {}
        Ok(None) => wrong.push(Wrong::Cell("the cell, synced, is empty".into())),
        Ok(Some(value)) => {
            let value = String::from_utf8_lossy(&value).into_owned();
            if !allowed.contains(&value.as_str()) {
                let round = value.split('|').next().unwrap_or_default().to_string();
                wrong.push(Wrong::Cell(format!(
                    "the cell holds round {round}, older than the sync point or never given"
                )));
            }
            for name in named(&value) {
                if !matches!(read(&name), Ok(Some(_))) {
                    wrong.push(Wrong::Names(format!(
                        "the cell names {name}, which the file does not hold"
                    )));
                }
            }
        }
        Err(error) => wrong.push(Wrong::Names(format!("the cell fails to read: {error}"))),
    }
    archive
        .inner
        .dies
        .store(true, std::sync::atomic::Ordering::Relaxed);
    Ok(wrong)
}

/// Plays every power loss on the workload and reports what `keep` says
/// went wrong, grouped. See the module docs. `DIALOG_POWER_SEED` and
/// `DIALOG_POWER_TRIALS` replay a failure or search further.
fn play(
    workload: fn(&Path, &disk::Watch) -> anyhow::Result<Done>,
    keep: fn(&Wrong) -> Option<&str>,
) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("archive");
    std::fs::write(&path, b"")?;
    let watch = disk::watch(&path)?;
    let done = workload(&path, &watch)?;
    let ops = watch.ops();
    drop(watch);

    let seed: u64 = std::env::var("DIALOG_POWER_SEED")
        .ok()
        .and_then(|seed| seed.parse().ok())
        .unwrap_or(0x5eed);
    let trials: usize = std::env::var("DIALOG_POWER_TRIALS")
        .ok()
        .and_then(|trials| trials.parse().ok())
        .unwrap_or(30);
    let image_path = directory.path().join("after");
    let mut failures: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    let mut images = 0;
    for at in 0..=ops.len() {
        for trial in 0..trials {
            let seed = seed ^ ((at as u64) << 16) ^ trial as u64;
            let image = disk::after_power_loss(&ops, at, &mut Draw::new(seed));
            images += 1;
            for wrong in check(&image, at, &done, &image_path)? {
                let Some(wrong) = keep(&wrong) else {
                    continue;
                };
                // Grouped by what went wrong, round and block left out.
                let kind: String = wrong
                    .split(' ')
                    .filter(|word| word.parse::<u64>().is_err())
                    .collect::<Vec<_>>()
                    .join(" ");
                failures.entry(kind).or_default().push(format!(
                    "after {at} of {} (seed {seed}): {wrong}",
                    ops.len()
                ));
            }
        }
    }
    let report: Vec<String> = failures
        .iter()
        .map(|(kind, cases)| format!("{} times: {kind}\n    e.g. {}", cases.len(), cases[0]))
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {images} images went wrong:\n{}",
        failures.values().map(Vec::len).sum::<usize>(),
        report.join("\n")
    );
    Ok(())
}

/// Every power loss the workload can meet leaves a file that opens, keeps
/// every block synced, reads no block as other bytes, and a cell holding a
/// value it held at the last sync point or was given after, every block of
/// which is in the file.
#[dialog_common::test]
fn every_power_loss_recovers_to_what_was_synced() -> anyhow::Result<()> {
    play(workload, |wrong| match wrong {
        Wrong::File(wrong) | Wrong::Cell(wrong) | Wrong::Names(wrong) => Some(wrong),
    })
}

/// A workload whose slots each take more than a sector: blocks in four
/// catalogs with long names, then rounds of blocks with a sync point after
/// each. Its durable slot, written over in place at each sync point,
/// spans two sectors, so a power loss can keep one of them only.
fn wide_slots(path: &Path, watch: &disk::Watch) -> anyhow::Result<Done> {
    let archive = FileArchive::open_with(
        path,
        Options {
            extent: 4096,
            ..Options::default()
        },
    )?;
    let catalog = |n: usize| format!("{n}{}", "c".repeat(200));
    for n in 0..4 {
        archive.append(&catalog(n), &[block(&format!("catalog {n}"))])?;
    }
    let mut done = Done::default();
    archive.sync()?;
    done.synced.push(watch.len());
    for round in 0..3 {
        for n in 0..4 {
            let name = format!("round {round} block {n}");
            archive.append("index", &[block(&name)])?;
            archive.append(&catalog(n), &[block(&format!("{name} beside"))])?;
            done.blocks.push((name, watch.len()));
        }
        archive.sync()?;
        done.synced.push(watch.len());
    }
    archive
        .inner
        .dies
        .store(true, std::sync::atomic::Ordering::Relaxed);
    Ok(done)
}

/// Every power loss leaves a file that opens and keeps every block synced,
/// also when a slot takes more than one sector. Today the durable slot is
/// one copy written over in place, so a power loss during a sync point that
/// keeps one of its sectors and not the other leaves no whole durable slot,
/// and the file does not open.
#[dialog_common::test]
fn it_recovers_from_every_power_loss_with_slots_over_a_sector() -> anyhow::Result<()> {
    play(wide_slots, |wrong| match wrong {
        Wrong::File(wrong) | Wrong::Cell(wrong) | Wrong::Names(wrong) => Some(wrong),
    })
}

/// A sync point cut off by a power loss after it wrote the first sector of
/// the durable slot and before it wrote the rest, or the other way round,
/// leaves a file that opens and keeps every block synced before it. Today
/// the durable slot is one copy written over in place, so either tear
/// leaves no whole durable slot and the file does not open.
#[dialog_common::test]
fn it_opens_after_a_durable_slot_torn_at_a_sector() -> anyhow::Result<()> {
    use super::slot::SLOT;
    use crate::file::MAGIC;

    let directory = tempfile::tempdir()?;
    let path = directory.path().join("archive");
    let archive = FileArchive::open(&path)?;
    let catalog = |n: usize| format!("{n}{}", "c".repeat(200));
    let mut synced = Vec::new();
    for n in 0..4 {
        let name = format!("first {n}");
        archive.append(&catalog(n), &[block(&name)])?;
        synced.push((catalog(n), name));
    }
    archive.sync()?;
    for n in 0..40 {
        archive.append(&catalog(n % 4), &[block(&format!("later {n}"))])?;
    }
    let before = std::fs::read(&path)?;
    archive.sync()?;
    let after = std::fs::read(&path)?;
    archive
        .inner
        .dies
        .store(true, std::sync::atomic::Ordering::Relaxed);
    drop(archive);

    let durable = MAGIC.len() + 2 * SLOT as usize;
    let sector = (durable / 512 + 1) * 512;
    let length = |bytes: &[u8]| u32::from_le_bytes(bytes[durable..durable + 4].try_into().unwrap());
    assert!(
        durable + 8 + length(&before) as usize > sector,
        "the durable slot spans a sector boundary"
    );
    for (kept, torn) in [(&before, &after), (&after, &before)] {
        let mut image = kept.clone();
        image[durable..sector].copy_from_slice(&torn[durable..sector]);
        let image_path = directory.path().join("torn");
        std::fs::write(&image_path, &image)?;
        as_if_rebooted(&image_path)?;
        let reopened = FileArchive::open_with(
            &image_path,
            Options {
                leave_extent: false,
                ..Options::default()
            },
        )
        .map_err(|error| anyhow::anyhow!("the torn image does not open: {error}"))?;
        for (catalog, name) in &synced {
            assert_eq!(
                reopened.get(catalog, block(name).blake3_hash())?.as_deref(),
                Some(name.as_bytes()),
                "{name}, synced, reads back"
            );
        }
        reopened
            .inner
            .dies
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(())
}
