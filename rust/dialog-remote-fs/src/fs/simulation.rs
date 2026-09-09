//! Simulated-network shaping and transfer metering for the [`Fs`] transport.
//!
//! The [`Fs`](crate::Fs) site is the one remote transport that runs with no
//! real network underneath, which makes it the natural place to *simulate*
//! one: point a repository's remote at a local directory, describe the link
//! you want to model, and every fork invocation against that remote pays the
//! modeled cost — while a tally records exactly what crossed the simulated
//! wire. This is how the sync/join soak harness (`dialog-soak`) measures
//! replication cost in round trips, bytes, and modeled wall-clock time
//! without standing up real infrastructure.
//!
//! Two independent facilities, both process-global:
//!
//! - **Metering** ([`tally`], [`reset_tally`]): counts every remote effect
//!   and the payload bytes it moved, per effect kind. Always on; the cost is
//!   a few relaxed atomic increments per invocation.
//! - **Shaping** ([`configure`], [`NetworkShape`]): when configured, each
//!   invocation awaits the modeled cost of its round trip before completing:
//!   a fixed per-request authorization delay (modeling the access-service
//!   redeem that a UCAN-backed remote pays per object), the link's round-trip
//!   latency, and the payload's serialization time on a *shared* bottleneck
//!   link (concurrent transfers queue for bandwidth, as they do on a real
//!   last-mile link, while their latencies overlap). Unconfigured, nothing
//!   sleeps and the transport behaves exactly as before.
//!
//! Shaping can be set programmatically ([`configure`]) or through the
//! `DIALOG_FS_NETWORK` environment variable (read once, at first use), e.g.:
//!
//! ```text
//! DIALOG_FS_NETWORK="latency=80ms,bandwidth=20mbps,auth=120ms"
//! ```
//!
//! Shaping is native-only: on wasm the variable does not exist and
//! [`configure`] is compiled to a no-op, so the browser build carries only
//! the (inert) tally counters.
//!
//! The simulated clock composes with `tokio`'s paused test time: the delays
//! are ordinary `tokio::time` sleeps, so a harness running under
//! `start_paused` measures modeled time deterministically and instantly.

use std::sync::atomic::{AtomicU64, Ordering};

/// The kinds of remote effect the [`Fs`](crate::Fs) transport serves, as
/// metering buckets. One bucket per provider impl, so a tally maps directly
/// onto what a real remote would serve: `ArchiveGet` is a block fetch,
/// `MemoryResolve` is a head-cell read, and so on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum Traffic {
    /// A content-addressed block read (`archive::Get`).
    ArchiveGet = 0,
    /// A single block write (`archive::Put`).
    ArchivePut = 1,
    /// A batched block write (`archive::Import`).
    ArchiveImport = 2,
    /// A head-cell read (`memory::Resolve`).
    MemoryResolve = 3,
    /// A head-cell compare-and-swap write (`memory::Publish`).
    MemoryPublish = 4,
    /// A head-cell retract (`memory::Retract`).
    MemoryRetract = 5,
    /// A blob open (`blob::Read`). Bytes are not metered here — they move
    /// through the returned reader, outside the invocation.
    BlobRead = 6,
    /// A blob import (`blob::Import`). Bytes are not metered here — they
    /// move through the returned writer, outside the invocation.
    BlobImport = 7,
}

/// How many [`Traffic`] buckets exist.
const BUCKETS: usize = 8;

/// All bucket variants, for iteration in [`TransferTally::total`].
const ALL: [Traffic; BUCKETS] = [
    Traffic::ArchiveGet,
    Traffic::ArchivePut,
    Traffic::ArchiveImport,
    Traffic::MemoryResolve,
    Traffic::MemoryPublish,
    Traffic::MemoryRetract,
    Traffic::BlobRead,
    Traffic::BlobImport,
];

impl Traffic {
    /// A short stable label for reports (`archive.get`, `memory.resolve`, ...).
    pub fn label(&self) -> &'static str {
        match self {
            Traffic::ArchiveGet => "archive.get",
            Traffic::ArchivePut => "archive.put",
            Traffic::ArchiveImport => "archive.import",
            Traffic::MemoryResolve => "memory.resolve",
            Traffic::MemoryPublish => "memory.publish",
            Traffic::MemoryRetract => "memory.retract",
            Traffic::BlobRead => "blob.read",
            Traffic::BlobImport => "blob.import",
        }
    }
}

/// Process-global counters: `[count, bytes]` per bucket.
static COUNTS: [AtomicU64; BUCKETS] = [const { AtomicU64::new(0) }; BUCKETS];
static BYTES: [AtomicU64; BUCKETS] = [const { AtomicU64::new(0) }; BUCKETS];

/// A snapshot of the transfer tally: per-bucket invocation counts and
/// payload bytes since the last [`reset_tally`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransferTally {
    counts: [u64; BUCKETS],
    bytes: [u64; BUCKETS],
}

impl TransferTally {
    /// Invocations recorded in `bucket`.
    pub fn count(&self, bucket: Traffic) -> u64 {
        self.counts[bucket as usize]
    }

    /// Payload bytes recorded in `bucket`.
    pub fn bytes(&self, bucket: Traffic) -> u64 {
        self.bytes[bucket as usize]
    }

    /// Total invocations and bytes across every bucket.
    pub fn total(&self) -> (u64, u64) {
        ALL.iter().fold((0, 0), |(count, bytes), bucket| {
            (count + self.count(*bucket), bytes + self.bytes(*bucket))
        })
    }

    /// The buckets with at least one recorded invocation, with their
    /// `(count, bytes)`, for rendering reports without empty rows.
    pub fn rows(&self) -> impl Iterator<Item = (Traffic, u64, u64)> + '_ {
        ALL.iter()
            .filter(|bucket| self.count(**bucket) > 0)
            .map(|bucket| (*bucket, self.count(*bucket), self.bytes(*bucket)))
    }

    /// The difference `self - earlier`, for per-phase deltas over a
    /// monotonically growing tally.
    pub fn since(&self, earlier: &TransferTally) -> TransferTally {
        let mut delta = TransferTally::default();
        for index in 0..BUCKETS {
            delta.counts[index] = self.counts[index].saturating_sub(earlier.counts[index]);
            delta.bytes[index] = self.bytes[index].saturating_sub(earlier.bytes[index]);
        }
        delta
    }
}

/// The current transfer tally.
pub fn tally() -> TransferTally {
    let mut snapshot = TransferTally::default();
    for index in 0..BUCKETS {
        snapshot.counts[index] = COUNTS[index].load(Ordering::Relaxed);
        snapshot.bytes[index] = BYTES[index].load(Ordering::Relaxed);
    }
    snapshot
}

/// Reset the transfer tally to zero.
pub fn reset_tally() {
    for index in 0..BUCKETS {
        COUNTS[index].store(0, Ordering::Relaxed);
        BYTES[index].store(0, Ordering::Relaxed);
    }
}

/// An in-flight simulated request: [`begin`] has charged the request's
/// authorization and latency; [`InFlight::complete`] charges the payload's
/// transfer time and records its bytes.
#[must_use = "call `complete` with the payload size to finish metering"]
pub(crate) struct InFlight {
    bucket: Traffic,
}

impl InFlight {
    /// Record `bytes` of payload for this request and, when shaping is
    /// configured, await the payload's serialization slot on the shared
    /// link.
    pub(crate) async fn complete(self, bytes: usize) {
        BYTES[self.bucket as usize].fetch_add(bytes as u64, Ordering::Relaxed);
        shaping::transfer(bytes).await;
    }
}

/// Meter one remote invocation in `bucket` and, when shaping is configured,
/// await the request's fixed costs (authorization + round-trip latency).
pub(crate) async fn begin(bucket: Traffic) -> InFlight {
    COUNTS[bucket as usize].fetch_add(1, Ordering::Relaxed);
    shaping::request().await;
    InFlight { bucket }
}

#[cfg(not(target_arch = "wasm32"))]
pub use shaping::{NetworkShape, configure};

#[cfg(not(target_arch = "wasm32"))]
mod shaping {
    //! The native-only half: the link model and its delays.

    use std::sync::{Mutex, OnceLock, RwLock};
    use std::time::Duration;

    use tokio::time::Instant;

    /// A link model: what one simulated request costs.
    ///
    /// `latency` and `auth_latency` are paid per request, overlapping
    /// freely across concurrent requests (propagation delay). `bandwidth`
    /// is a shared bottleneck: concurrent payloads queue for it, so ten
    /// parallel 64 KiB responses on a 20 Mbit/s link take ten payloads'
    /// serialization time, not one.
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub struct NetworkShape {
        /// Round-trip latency paid by every request.
        pub latency: Duration,
        /// Fixed per-request authorization delay, modeling the per-object
        /// access-service redeem a UCAN-backed remote pays before its
        /// storage round trip. Zero models a transport whose authorization
        /// is amortized (or a permit cache that always hits).
        pub auth_latency: Duration,
        /// Shared link bandwidth in bytes per second; `None` models an
        /// infinitely fast link (latency only).
        pub bandwidth: Option<u64>,
    }

    impl NetworkShape {
        /// Parse a shape from the `DIALOG_FS_NETWORK` syntax:
        /// comma-separated `key=value` with keys `latency`, `auth`
        /// (durations: `120ms`, `1.5s`) and `bandwidth` (`20mbps`,
        /// `500kbps`, `1gbps`). Missing keys default to zero cost.
        pub fn parse(text: &str) -> Result<Self, String> {
            let mut shape = NetworkShape {
                latency: Duration::ZERO,
                auth_latency: Duration::ZERO,
                bandwidth: None,
            };
            for part in text.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                let (key, value) = part
                    .split_once('=')
                    .ok_or_else(|| format!("expected key=value, got {part:?}"))?;
                match key.trim() {
                    "latency" => shape.latency = parse_duration(value.trim())?,
                    "auth" => shape.auth_latency = parse_duration(value.trim())?,
                    "bandwidth" => shape.bandwidth = Some(parse_bandwidth(value.trim())?),
                    other => return Err(format!("unknown key {other:?}")),
                }
            }
            Ok(shape)
        }
    }

    /// Parse `80ms` / `1.5s` into a [`Duration`].
    fn parse_duration(text: &str) -> Result<Duration, String> {
        let (number, unit) = split_unit(text);
        let value: f64 = number
            .parse()
            .map_err(|_| format!("bad duration {text:?}"))?;
        match unit {
            "ms" => Ok(Duration::from_secs_f64(value / 1000.0)),
            "s" => Ok(Duration::from_secs_f64(value)),
            _ => Err(format!("bad duration unit in {text:?} (use ms or s)")),
        }
    }

    /// Parse `20mbps` / `500kbps` / `1gbps` into bytes per second.
    fn parse_bandwidth(text: &str) -> Result<u64, String> {
        let (number, unit) = split_unit(text);
        let value: f64 = number
            .parse()
            .map_err(|_| format!("bad bandwidth {text:?}"))?;
        let bits_per_second = match unit {
            "kbps" => value * 1_000.0,
            "mbps" => value * 1_000_000.0,
            "gbps" => value * 1_000_000_000.0,
            _ => {
                return Err(format!(
                    "bad bandwidth unit in {text:?} (use kbps/mbps/gbps)"
                ));
            }
        };
        Ok((bits_per_second / 8.0) as u64)
    }

    /// Split `"80ms"` into `("80", "ms")`.
    fn split_unit(text: &str) -> (&str, &str) {
        let split = text
            .find(|c: char| c.is_ascii_alphabetic())
            .unwrap_or(text.len());
        text.split_at(split)
    }

    /// The active shape: an explicit [`configure`] wins; otherwise the
    /// environment default applies.
    static CONFIGURED: RwLock<Option<Option<NetworkShape>>> = RwLock::new(None);

    /// The environment default, read once.
    static FROM_ENV: OnceLock<Option<NetworkShape>> = OnceLock::new();

    /// When the shared link next frees up, for bandwidth queuing.
    static LINK_FREE_AT: Mutex<Option<Instant>> = Mutex::new(None);

    /// Set (or, with `None`, clear) the active link model, overriding the
    /// `DIALOG_FS_NETWORK` environment default. Takes effect for every
    /// subsequent [`Fs`](crate::Fs) invocation in this process.
    pub fn configure(shape: Option<NetworkShape>) {
        *CONFIGURED.write().expect("network shape lock poisoned") = Some(shape);
    }

    /// The shape in effect right now, if any.
    fn active() -> Option<NetworkShape> {
        if let Some(explicit) = *CONFIGURED.read().expect("network shape lock poisoned") {
            return explicit;
        }
        *FROM_ENV.get_or_init(|| {
            std::env::var("DIALOG_FS_NETWORK").ok().and_then(|text| {
                match NetworkShape::parse(&text) {
                    Ok(shape) => Some(shape),
                    Err(reason) => {
                        eprintln!("ignoring unparsable DIALOG_FS_NETWORK: {reason}");
                        None
                    }
                }
            })
        })
    }

    /// The fixed per-request delays: authorization, then round-trip latency.
    pub(super) async fn request() {
        if let Some(shape) = active() {
            let fixed = shape.auth_latency + shape.latency;
            if fixed > Duration::ZERO {
                tokio::time::sleep(fixed).await;
            }
        }
    }

    /// The payload's serialization time on the shared link: reserve the
    /// next free slot, then wait until it has passed.
    pub(super) async fn transfer(bytes: usize) {
        let Some(shape) = active() else { return };
        let Some(bandwidth) = shape.bandwidth else {
            return;
        };
        if bytes == 0 || bandwidth == 0 {
            return;
        }
        let duration = Duration::from_secs_f64(bytes as f64 / bandwidth as f64);
        let until = {
            let mut free_at = LINK_FREE_AT.lock().expect("link timeline lock poisoned");
            let now = Instant::now();
            let start = free_at.filter(|at| *at > now).unwrap_or(now);
            let end = start + duration;
            *free_at = Some(end);
            end
        };
        tokio::time::sleep_until(until).await;
    }
}

#[cfg(target_arch = "wasm32")]
mod shaping {
    //! On wasm there is no environment and no simulated link: requests are
    //! metered but never delayed.

    pub(super) async fn request() {}

    pub(super) async fn transfer(_bytes: usize) {}
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::shaping::NetworkShape;
    use std::time::Duration;

    /// The `DIALOG_FS_NETWORK` syntax parses into the intended shape, and
    /// bad input is rejected rather than silently zeroed.
    #[test]
    fn it_parses_a_network_shape() {
        let shape = NetworkShape::parse("latency=80ms, bandwidth=20mbps, auth=120ms").unwrap();
        assert_eq!(shape.latency, Duration::from_millis(80));
        assert_eq!(shape.auth_latency, Duration::from_millis(120));
        assert_eq!(shape.bandwidth, Some(2_500_000));

        let seconds = NetworkShape::parse("latency=1.5s").unwrap();
        assert_eq!(seconds.latency, Duration::from_millis(1500));
        assert_eq!(seconds.bandwidth, None);

        assert!(NetworkShape::parse("latency=80").is_err());
        assert!(NetworkShape::parse("bandwidth=20").is_err());
        assert!(NetworkShape::parse("nonsense").is_err());
        assert!(NetworkShape::parse("speed=20mbps").is_err());
    }
}
