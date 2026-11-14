//! `dialog-migrate`: runs the migrations this build of `dialog-migration`
//! has on a replica.
//!
//! Run as `dialog-migrate [--dry-run] [--step <id>]... [--remove-source]
//! <replica directory>` to run every step this build has (or only those
//! named by `--step`) on a profile directory. `--dry-run` prints what each
//! step would do and changes nothing. `--remove-source` makes a step that
//! moves data delete what it moved it from.
//!
//! Run as `dialog-migrate [--dry-run] --source <store> --target <file>
//! [--catalog <name>]` to move one store into the single-file archive (the
//! `chronicle` migration): `source` is a space's directory, or with
//! `--catalog` a flat directory of that catalog's blocks.

// The library is native-only, so on wasm32 there is nothing to run.
#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(not(target_arch = "wasm32"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    native::main()
}

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::path::PathBuf;

    use dialog_migration::Migrations;

    const USAGE: &str = "usage: dialog-migrate [--dry-run] [--step <id>]... [--remove-source] \
                         <replica directory>\n       \
                         dialog-migrate [--dry-run] --source <store> --target <file> \
                         [--catalog <name>]";

    /// What the command line asked for.
    #[derive(Default)]
    struct Arguments {
        dry_run: bool,
        remove_source: bool,
        steps: Vec<String>,
        source: Option<PathBuf>,
        target: Option<PathBuf>,
        catalog: Option<String>,
        replica: Option<PathBuf>,
    }

    fn usage() -> ! {
        eprintln!("{USAGE}");
        std::process::exit(2);
    }

    fn arguments() -> Arguments {
        let mut parsed = Arguments::default();
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            let mut value = || args.next().unwrap_or_else(|| usage());
            match arg.as_str() {
                "--dry-run" => parsed.dry_run = true,
                "--remove-source" => parsed.remove_source = true,
                "--step" => parsed.steps.push(value()),
                "--source" => parsed.source = Some(value().into()),
                "--target" => parsed.target = Some(value().into()),
                "--catalog" => parsed.catalog = Some(value()),
                "--help" | "-h" => usage(),
                flag if flag.starts_with("--") => usage(),
                _ if parsed.replica.is_none() => parsed.replica = Some(arg.into()),
                _ => usage(),
            }
        }
        parsed
    }

    /// Every step this build has, in order.
    fn available(arguments: &Arguments) -> Migrations {
        #[allow(unused_mut)]
        let mut migrations = Migrations::new();
        #[cfg(feature = "chronicle")]
        {
            migrations = migrations.with(dialog_migration::chronicle::IntoChronicle {
                remove_source: arguments.remove_source,
            });
        }
        let _ = arguments;
        migrations
    }

    pub fn main() -> Result<(), Box<dyn std::error::Error>> {
        let arguments = arguments();
        if arguments.source.is_some() || arguments.target.is_some() {
            return store(&arguments);
        }
        let Some(replica) = &arguments.replica else {
            usage()
        };
        let all = available(&arguments);
        if all.steps().next().is_none() {
            eprintln!(
                "this build has no migrations: install it with the feature of the one to run, \
                 such as `--features chronicle`"
            );
            std::process::exit(2);
        }
        let known: Vec<&str> = all.steps().map(|step| step.id()).collect();
        for step in &arguments.steps {
            if !known.contains(&step.as_str()) {
                eprintln!("no step {step} in this build; it has {}", known.join(", "));
                std::process::exit(2);
            }
        }
        let chosen =
            |id: &str| arguments.steps.is_empty() || arguments.steps.iter().any(|step| step == id);
        #[allow(unused_mut)]
        let mut migrations = Migrations::new();
        #[cfg(feature = "chronicle")]
        if chosen("chronicle") {
            migrations = migrations.with(dialog_migration::chronicle::IntoChronicle {
                remove_source: arguments.remove_source,
            });
        }
        let _ = chosen;

        if arguments.dry_run {
            let plan = migrations.plan(replica)?;
            if plan.is_empty() {
                println!("nothing to do");
            }
            for pending in plan {
                println!(
                    "{} at {}: {}",
                    pending.step,
                    pending.at.display(),
                    pending.detail
                );
            }
            return Ok(());
        }
        let started = std::time::Instant::now();
        let applied = migrations.run(replica)?;
        if applied.is_empty() {
            println!("nothing to do");
        }
        for applied in applied {
            println!(
                "{} at {}: {}",
                applied.step,
                applied.at.display(),
                applied.detail
            );
        }
        println!("in {:.1}s", started.elapsed().as_secs_f64());
        Ok(())
    }

    #[cfg(feature = "chronicle")]
    fn store(arguments: &Arguments) -> Result<(), Box<dyn std::error::Error>> {
        use dialog_chronicle::Chronicle;
        use dialog_migration::chronicle::{BlockSource, Directory, transcode};

        let (Some(source), Some(target)) = (&arguments.source, &arguments.target) else {
            usage()
        };
        let source = match &arguments.catalog {
            Some(catalog) => Directory::catalog(source, catalog.clone()),
            None => Directory::space(source),
        };
        let mib = |bytes: u64| format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0));
        if arguments.dry_run {
            for catalog in source.catalogs()? {
                println!(
                    "{catalog}: {} blocks into {}",
                    source.hashes(&catalog)?.len(),
                    target.display()
                );
            }
            return Ok(());
        }
        let started = std::time::Instant::now();
        let archive = Chronicle::open(target)?;
        let report = transcode(&source, &archive)?;
        archive.sync()?;
        for catalog in &report.catalogs {
            println!(
                "{}: {} roots; {} tree nodes as frames ({} as blocks), {} kept whole ({}), \
                 {} other blocks ({})",
                catalog.catalog,
                catalog.roots,
                catalog.frames,
                mib(catalog.frame_source_bytes),
                catalog.whole_nodes,
                mib(catalog.whole_node_bytes),
                catalog.others,
                mib(catalog.other_bytes),
            );
        }
        let stats = archive.stats();
        println!(
            "{}: {} ({} frames, {} tokens, {} whole) in {:.1}s",
            target.display(),
            mib(stats.file_bytes),
            mib(stats.frame_bytes),
            mib(stats.token_bytes),
            mib(stats.block_bytes),
            started.elapsed().as_secs_f64()
        );
        Ok(())
    }

    #[cfg(not(feature = "chronicle"))]
    fn store(_: &Arguments) -> Result<(), Box<dyn std::error::Error>> {
        eprintln!("moving a store needs the chronicle migration: `--features chronicle`");
        std::process::exit(2);
    }
}
