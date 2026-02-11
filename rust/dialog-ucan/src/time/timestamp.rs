//! A JavaScript-wrapper for [`Timestamp`][crate::time::Timestamp].

use super::error::{NumberIsNotATimestamp, OutOfRangeError};
use ipld_core::ipld::Ipld;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

#[cfg(not(target_arch = "wasm32"))]
pub use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(target_arch = "wasm32")]
pub use web_time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(any(test, feature = "test_utils"))]
use arbitrary::{self, Arbitrary, Unstructured};

/// A [`Timestamp`][super::Timestamp] with safe JavaScript interop.
///
/// Per the UCAN spec, timestamps MUST respect [IEEE-754]
/// (64-bit double precision = 53-bit truncated integer) for
/// JavaScript interoperability.
///
/// This range can represent millions of years into the future,
/// and is thus sufficient for "nearly" all auth use cases.
///
/// This type internally deserializes permissively from any [`SystemTime`],
/// but checks that any time created is in the 53-bit bound when created via
/// the public API.
///
/// [IEEE-754]: https://en.wikipedia.org/wiki/IEEE_754
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(SystemTime);

impl Timestamp {
    /// Create a [`Timestamp`] from a [`SystemTime`].
    ///
    /// # Arguments
    ///
    /// * `time` — The time to convert
    ///
    /// # Errors
    ///
    /// * [`OutOfRangeError`] — If the time is more than 2⁵³ seconds since the Unix epoch
    pub fn new(time: SystemTime) -> Result<Self, OutOfRangeError> {
        if time
            .duration_since(UNIX_EPOCH)
            .map_err(|_| OutOfRangeError { tried: time })?
            .as_secs()
            > 0x001F_FFFF_FFFF_FFFF
        {
            Err(OutOfRangeError { tried: time })
        } else {
            Ok(Timestamp(time))
        }
    }

    /// Get the current time in seconds since [`UNIX_EPOCH`] as a [`Timestamp`].
    ///
    /// # Panics
    ///
    /// This function will panic if the current time is before the Unix epoch.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn now() -> Timestamp {
        Self::new(SystemTime::now())
            .expect("the current time to be somtime in the 3rd millenium CE")
    }

    /// Get a timestamp 5 minutes from now.
    ///
    /// # Panics
    ///
    /// This function will panic if the current time is before the Unix epoch.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn five_minutes_from_now() -> Timestamp {
        Self::new(SystemTime::now() + Duration::from_secs(5 * 60))
            .expect("the current time to be somtime in the 3rd millenium CE")
    }

    /// Get a timestamp 5 hours from now.
    ///
    /// # Panics
    ///
    /// This function will panic if the current time is before the Unix epoch.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn five_years_from_now() -> Timestamp {
        Self::new(SystemTime::now() + Duration::from_secs(5 * 365 * 24 * 60 * 60))
            .expect("the current time to be somtime in the 3rd millenium CE")
    }

    /// Convert a [`Timestamp`] to a [Unix timestamp].
    ///
    /// [Unix timestamp]: https://en.wikipedia.org/wiki/Unix_time
    ///
    /// # Panics
    ///
    /// This function will panic if the [`SystemTime`] is before the Unix epoch.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn to_unix(&self) -> u64 {
        self.0
            .duration_since(UNIX_EPOCH)
            .expect("System time to be after the Unix epoch")
            .as_secs()
    }

    /// An intentionally permissive variant of `new` for
    /// deseriazation. See the note on the struct.
    pub(crate) const fn postel(time: SystemTime) -> Self {
        Timestamp(time)
    }
}

#[cfg(target_arch = "wasm32")]
impl Timestamp {
    /// Lift a [`js_sys::Date`] into a Rust [`Timestamp`]
    pub fn from_date(date_time: js_sys::Date) -> Result<Timestamp, OutOfRangeError> {
        let millis = date_time.get_time() as u64;
        let secs: u64 = millis / 1000;
        let duration = Duration::new(secs, 0); // Just round off the nanos
        Timestamp::new(UNIX_EPOCH + duration)
    }

    /// Lower the [`Timestamp`] to a [`js_sys::Date`]
    pub fn to_date(&self) -> js_sys::Date {
        js_sys::Date::new(&wasm_bindgen::JsValue::from(
            self.0
                .duration_since(UNIX_EPOCH)
                .expect("time should be in range since it's getting a JS Date")
                .as_millis() as f64,
        ))
    }
}

impl TryFrom<SystemTime> for Timestamp {
    type Error = OutOfRangeError;

    fn try_from(sys_time: SystemTime) -> Result<Timestamp, Self::Error> {
        Timestamp::new(sys_time)
    }
}

impl From<Timestamp> for SystemTime {
    fn from(js_time: Timestamp) -> Self {
        js_time.0
    }
}

impl From<Timestamp> for Ipld {
    fn from(timestamp: Timestamp) -> Self {
        timestamp.to_unix().into()
    }
}

impl TryFrom<Ipld> for Timestamp {
    type Error = TimestampFromIpldError;

    fn try_from(ipld: Ipld) -> Result<Self, Self::Error> {
        match ipld {
            Ipld::Integer(secs) => secs
                .try_into()
                .map_err(TimestampFromIpldError::NotATimestamp),
            Ipld::Null
            | Ipld::Bool(_)
            | Ipld::Float(_)
            | Ipld::String(_)
            | Ipld::Bytes(_)
            | Ipld::List(_)
            | Ipld::Map(_)
            | Ipld::Link(_) => Err(TimestampFromIpldError::TimestampIsNotAnInteger),
        }
    }
}

/// Errors when an IPLD value is not a valid timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TimestampFromIpldError {
    /// The IPLD value was not an integer.
    #[error("the timestamp is not an integer")]
    TimestampIsNotAnInteger,

    /// The integer could not be converted to a timestamp.
    #[error("the timestamp is out of bounds (2^53)")]
    NotATimestamp(#[from] NumberIsNotATimestamp),
}

impl From<Timestamp> for i128 {
    fn from(timestamp: Timestamp) -> i128 {
        i128::from(timestamp.to_unix())
    }
}

impl TryFrom<i128> for Timestamp {
    type Error = NumberIsNotATimestamp;

    fn try_from(secs: i128) -> Result<Self, Self::Error> {
        Ok(Timestamp::new(
            UNIX_EPOCH
                + Duration::from_secs(
                    u64::try_from(secs).map_err(|_| NumberIsNotATimestamp::TriedIpldInt(secs))?,
                ),
        )?)
    }
}

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_unix().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let seconds = u64::deserialize(deserializer)?;
        Ok(Timestamp::postel(UNIX_EPOCH + Duration::from_secs(seconds)))
    }
}

#[cfg(any(test, feature = "test_utils"))]
impl<'a> Arbitrary<'a> for Timestamp {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self, arbitrary::Error> {
        let secs = u.int_in_range(std::ops::RangeInclusive::new(0, u64::pow(2, 53) - 1))?;
        Ok(Timestamp::postel(UNIX_EPOCH + Duration::from_secs(secs)))
    }
}
