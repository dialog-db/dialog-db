//! A readout of the causal context a branch head publishes.
//!
//! A head carries a watermark for every origin its ancestry has ever
//! included, so how many there are, and how many of them are still
//! writing, decides what the head costs to sign, publish, and pull. This
//! module answers both from one head: the origin count, and how many
//! origins wrote within the last 100, 1 000 and 10 000 editions of the
//! head. The second number is the one that sizes the set of origins a
//! head needs to carry inline.

use std::fmt::{self, Display};

use dialog_capability::identity::Revision;

/// The edition windows the readout counts recent origins in.
pub const RECENCY_WINDOWS: [u64; 3] = [100, 1_000, 10_000];

/// What one head publishes about its causal context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextProfile {
    /// The head's own edition, the point the windows count back from.
    pub edition: u64,
    /// How many origins the head publishes a watermark for.
    pub origins: usize,
    /// For each of [`RECENCY_WINDOWS`], how many published origins have an
    /// edition within that many editions of the head.
    pub recent: Vec<(u64, usize)>,
    /// The head's dag-cbor encoding, in bytes: what every publish, pull
    /// and cell watcher moves.
    pub encoded: usize,
    /// The head's signing payload, in bytes: what every commit signs.
    pub payload: usize,
}

impl ContextProfile {
    /// Profile `head`, or `None` when the head publishes no context (a
    /// head minted before heads carried one).
    pub fn of(head: &Revision) -> Option<Self> {
        let context = head.context.as_ref()?;
        let edition = head.edition.value();
        let recent = RECENCY_WINDOWS
            .iter()
            .map(|window| {
                let floor = edition.saturating_sub(*window);
                let count = context
                    .iter()
                    .filter(|(_, watermark)| watermark.edition.value() >= floor)
                    .count();
                (*window, count)
            })
            .collect();
        let encoded = serde_ipld_dagcbor::to_vec(head)
            .map(|bytes| bytes.len())
            .unwrap_or_default();
        Some(Self {
            edition,
            origins: context.len(),
            recent,
            encoded,
            payload: head.payload().len(),
        })
    }
}

impl Display for ContextProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "head edition      {}", self.edition)?;
        writeln!(f, "origins           {}", self.origins)?;
        for (window, count) in &self.recent {
            writeln!(f, "  within {window:>6}  {count}")?;
        }
        writeln!(f, "head bytes        {}", self.encoded)?;
        write!(f, "payload bytes     {}", self.payload)
    }
}

#[cfg(test)]
mod tests {
    use dialog_capability::history::{Context, Edition, Origin, Version};
    use dialog_capability::identity::{Entity, TreeReference};

    use super::*;

    /// The windows count origins by how far below the head their
    /// watermark sits, and an origin exactly at a window's edge counts.
    #[test]
    fn it_counts_origins_by_recency() {
        let mut head = Revision::new(
            TreeReference::from([0u8; 32]),
            "branch:opaque".parse::<Entity>().expect("entity parses"),
            "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
                .parse()
                .expect("did parses"),
        );
        head.edition = Edition::new(20_000);
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

    #[test]
    fn it_has_nothing_to_profile_without_a_context() {
        let head = Revision::new(
            TreeReference::from([0u8; 32]),
            "branch:opaque".parse::<Entity>().expect("entity parses"),
            "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
                .parse()
                .expect("did parses"),
        );
        assert_eq!(ContextProfile::of(&head), None);
    }
}
