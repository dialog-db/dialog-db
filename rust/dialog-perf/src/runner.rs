//! The machine a sweep ran on.
//!
//! Callgrind counts the instructions a binary retires, and valgrind passes
//! the host CPU's feature flags through to it. Code that picks a SIMD path
//! at runtime (hashing, byte searching) retires a different number of
//! instructions on a different CPU, so two sweeps of one binary compare
//! exactly only when they ran on the same CPU model with the same flags.
//! A sweep records both beside its reports, as `runner.txt`, so a
//! comparison can say which case it is in.

use std::fs;
use std::path::Path;

use anyhow::{Context as _, Result};

/// The file a sweep records its runner in, beside the reports.
pub const RUNNER_FILE: &str = "runner.txt";

/// The CPU a sweep ran on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Runner {
    /// The CPU's model name, as `/proc/cpuinfo` gives it.
    pub model: String,
    /// A short stable hash of the CPU's feature flags.
    pub flags: String,
}

impl Runner {
    /// The CPU this process runs on, from `/proc/cpuinfo`; `unknown` for
    /// what it does not say.
    pub fn current() -> Self {
        let cpuinfo = fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
        let field = |name: &str| {
            cpuinfo.lines().find_map(|line| {
                let (key, value) = line.split_once(':')?;
                (key.trim() == name).then(|| value.trim().to_string())
            })
        };
        Self {
            model: field("model name").unwrap_or_else(|| "unknown".to_string()),
            flags: field("flags").map_or_else(|| "unknown".to_string(), |flags| fnv1a(&flags)),
        }
    }

    /// Record the runner in a sweep directory.
    pub fn write(&self, dir: &Path) -> Result<()> {
        let path = dir.join(RUNNER_FILE);
        fs::write(
            &path,
            format!("model: {}\nflags: {}\n", self.model, self.flags),
        )
        .with_context(|| format!("writing {}", path.display()))
    }

    /// The runner a sweep directory records, if it records one: sweeps
    /// from before the record existed do not.
    pub fn read(dir: &Path) -> Option<Self> {
        let text = fs::read_to_string(dir.join(RUNNER_FILE)).ok()?;
        let field = |name: &str| {
            text.lines()
                .find_map(|line| line.strip_prefix(name)?.strip_prefix(": "))
                .map(str::to_string)
        };
        Some(Self {
            model: field("model")?,
            flags: field("flags")?,
        })
    }
}

impl std::fmt::Display for Runner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (flags {})", self.model, self.flags)
    }
}

/// 64-bit FNV-1a, in hex: stable across toolchains, unlike std's hasher,
/// so sweeps built by different compilers hash the same flags alike.
fn fnv1a(text: &str) -> String {
    let hash = text.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    });
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What a sweep writes, a comparison reads back.
    #[test]
    fn a_runner_round_trips_through_its_file() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("dialog-perf-runner-{}", std::process::id()));
        fs::create_dir_all(&dir)?;
        let runner = Runner {
            model: "Some CPU @ 2.10GHz".into(),
            flags: fnv1a("fpu sse2 avx2"),
        };
        runner.write(&dir)?;
        let read = Runner::read(&dir);
        fs::remove_dir_all(&dir)?;
        assert_eq!(read, Some(runner));
        Ok(())
    }

    /// The flags hash is FNV-1a: fixed, whatever compiled it.
    #[test]
    fn the_flags_hash_is_stable() {
        assert_eq!(fnv1a(""), "cbf29ce484222325");
        assert_eq!(fnv1a("a"), "af63dc4c8601ec8c");
    }
}
