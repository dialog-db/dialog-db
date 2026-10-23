//! A readout of the causal context behind a branch head.
//!
//! A head's ancestry includes a watermark for every origin that ever
//! wrote into it, so how many there are, and how many of them are still
//! writing, decides what a pull that compares contexts costs. This module
//! answers both: the origin count, and how many origins wrote within the
//! last 100, 1 000 and 10 000 editions of the head.
//!
//! A head minted before the context region carries its whole context
//! inline, and [`ContextProfile::of`] reads it from the head alone. A head
//! minted since carries none: its context is in its tree's context region,
//! exact for every origin except the head's own, and
//! [`ContextProfile::from_region`] profiles it from the entries that
//! region holds, deriving the head's own entry as every reader does.

use std::fmt::{self, Display};

use dialog_capability::history::{Context, HistoryError};
use dialog_capability::identity::Revision;

/// The edition windows the readout counts recent origins in.
pub const RECENCY_WINDOWS: [u64; 3] = [100, 1_000, 10_000];

/// What one head's causal context holds, and what the head costs to move.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextProfile {
    /// The head's own edition, the point the windows count back from.
    pub edition: u64,
    /// How many origins the head's context holds.
    pub origins: usize,
    /// For each of [`RECENCY_WINDOWS`], how many origins have an edition
    /// within that many editions of the head.
    pub recent: Vec<(u64, usize)>,
    /// The head's dag-cbor encoding, in bytes: what every publish, pull
    /// and cell watcher moves.
    pub encoded: usize,
    /// The head's signing payload, in bytes: what every commit signs.
    pub payload: usize,
}

impl ContextProfile {
    /// Profile `head` from the context it carries inline, or `None` when
    /// it carries none: a head whose context is in its tree's region (see
    /// [`from_region`](Self::from_region)), or one minted before heads
    /// carried a context.
    pub fn of(head: &Revision) -> Option<Self> {
        head.inline_context()
            .map(|context| Self::with_context(head, context))
    }

    /// Profile `head` from `region`, the entries its tree's context region
    /// holds, refusing a region no head minted.
    pub fn from_region(head: &Revision, region: Context) -> Result<Self, HistoryError> {
        Ok(Self::with_context(head, &head.context_from_region(region)?))
    }

    fn with_context(head: &Revision, context: &Context) -> Self {
        let edition = head.edition.value();
        let recent = RECENCY_WINDOWS
            .iter()
            .map(|window| {
                let floor = edition.saturating_sub(*window);
                let origins = context
                    .iter()
                    .filter(|(_, watermark)| watermark.edition.value() >= floor)
                    .count();
                (*window, origins)
            })
            .collect();
        let encoded = serde_ipld_dagcbor::to_vec(head)
            .map(|bytes| bytes.len())
            .unwrap_or_default();
        Self {
            edition,
            origins: context.len(),
            recent,
            encoded,
            payload: head.payload().len(),
        }
    }
}

impl Display for ContextProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "head edition      {}", self.edition)?;
        writeln!(f, "origins           {}", self.origins)?;
        for (window, origins) in &self.recent {
            writeln!(f, "  within {window:>6}  {origins}")?;
        }
        writeln!(f, "head bytes        {}", self.encoded)?;
        write!(f, "payload bytes     {}", self.payload)
    }
}

#[cfg(test)]
mod tests {
    use dialog_capability::history::{Edition, Origin, Version, Watermark};
    use dialog_capability::identity::{Entity, TreeReference};

    use super::*;

    fn head(edition: u64) -> Revision {
        let mut head = Revision::new(
            TreeReference::from([0u8; 32]),
            "branch:opaque".parse::<Entity>().expect("entity parses"),
            "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
                .parse()
                .expect("did parses"),
        );
        head.edition = Edition::new(edition);
        head
    }

    /// The windows count origins by how far below the head their
    /// watermark sits, and an origin exactly at a window's edge counts.
    #[test]
    fn it_counts_origins_by_recency() {
        let mut head = head(20_000);
        let mut context = Context::new();
        for (seed, edition) in [(1u8, 20_000), (2, 19_900), (3, 19_000), (4, 5)] {
            context.record(Version::new(
                Origin::from([seed; 32]),
                Edition::new(edition),
            ));
        }
        head.context = Some(context);

        let profile = ContextProfile::of(&head).expect("the head has a context");
        assert_eq!(profile.origins, 4);
        assert_eq!(profile.recent, vec![(100, 2), (1_000, 3), (10_000, 3)]);
        assert!(profile.payload > 4 * 48, "the payload carries every entry");
    }

    /// A head whose context is in its tree's region is profiled from the
    /// region, with its own entry derived from the head; its own size
    /// does not depend on the origins.
    #[test]
    fn it_profiles_a_region_head_from_its_region() {
        let mut head = head(20_000);
        head.publish_region();
        assert_eq!(ContextProfile::of(&head), None);

        let mut region = Context::new();
        for seed in 0..100u8 {
            region.record(Version::new(
                Origin::from([seed; 32]),
                Edition::new(19_000 + u64::from(seed)),
            ));
        }
        region.set(
            head.origin(),
            Watermark {
                edition: Edition::new(19_995),
                count: 1,
            },
        );

        let profile = ContextProfile::from_region(&head, region).expect("a minted region");
        assert_eq!(profile.origins, 101);
        assert_eq!(profile.recent, vec![(100, 1), (1_000, 101), (10_000, 101)]);
        assert!(profile.encoded < 400 && profile.payload < 400);
    }

    /// A region without an entry for the head's own origin is not one a
    /// head minted, and is refused rather than profiled.
    #[test]
    fn it_refuses_a_region_without_the_heads_origin() {
        let mut head = head(10);
        head.publish_region();
        assert!(matches!(
            ContextProfile::from_region(&head, Context::new()),
            Err(HistoryError::InvalidReference(_))
        ));
    }

    #[test]
    fn it_has_nothing_to_profile_without_a_context() {
        assert_eq!(ContextProfile::of(&head(0)), None);
    }
}
