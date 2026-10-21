//! A readout of the causal context a branch head publishes.
//!
//! A head's ancestry includes a watermark for every origin that ever
//! wrote into it, so how many there are, and how many of them are still
//! writing, decides what the head costs to sign, publish, and pull. This
//! module answers both from one head: the origin count, and how many
//! origins wrote within the last 100, 1 000 and 10 000 editions of the
//! head. The second number is the one that sizes the active set a head
//! carries inline.
//!
//! A head that carries its active set and a spill names only the active
//! watermarks; the spilled ones are counted, but their editions are in the
//! tree's context region. A window reaching below the highest spilled
//! edition may therefore count more origins than the head shows, and the
//! readout marks its count as a lower bound.

use std::fmt::{self, Display};

use dialog_capability::identity::Revision;

/// The edition windows the readout counts recent origins in.
pub const RECENCY_WINDOWS: [u64; 3] = [100, 1_000, 10_000];

/// How many origins wrote within one edition window of a head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recency {
    /// The window, in editions below the head.
    pub window: u64,
    /// How many of the head's published watermarks sit within it.
    pub origins: usize,
    /// Whether that is every origin within it, or a lower bound because
    /// spilled watermarks may sit within it too.
    pub exact: bool,
}

/// What one head publishes about its causal context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextProfile {
    /// The head's own edition, the point the windows count back from.
    pub edition: u64,
    /// How many origins the head's context holds, active and spilled.
    pub origins: u64,
    /// How many of them the head carries inline.
    pub active: usize,
    /// How many of them are spilled to the tree's context region.
    pub spilled: u64,
    /// For each of [`RECENCY_WINDOWS`], how many origins have an edition
    /// within that many editions of the head.
    pub recent: Vec<Recency>,
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
        let spilled = head.spill.map(|spill| spill.count).unwrap_or(0);
        let highest_spilled = head
            .spill
            .filter(|spill| !spill.is_empty())
            .map(|spill| spill.highest.value());
        let edition = head.edition.value();
        let recent = RECENCY_WINDOWS
            .iter()
            .map(|window| {
                let floor = edition.saturating_sub(*window);
                let origins = context
                    .iter()
                    .filter(|(_, watermark)| watermark.edition.value() >= floor)
                    .count();
                Recency {
                    window: *window,
                    origins,
                    exact: highest_spilled.is_none_or(|highest| highest < floor),
                }
            })
            .collect();
        let encoded = serde_ipld_dagcbor::to_vec(head)
            .map(|bytes| bytes.len())
            .unwrap_or_default();
        Some(Self {
            edition,
            origins: context.len() as u64 + spilled,
            active: context.len(),
            spilled,
            recent,
            encoded,
            payload: head.payload().len(),
        })
    }
}

impl Display for ContextProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "head edition      {}", self.edition)?;
        writeln!(
            f,
            "origins           {} ({} active, {} spilled)",
            self.origins, self.active, self.spilled
        )?;
        for recency in &self.recent {
            let bound = if recency.exact { "" } else { "at least " };
            writeln!(
                f,
                "  within {:>6}  {bound}{}",
                recency.window, recency.origins
            )?;
        }
        writeln!(f, "head bytes        {}", self.encoded)?;
        write!(f, "payload bytes     {}", self.payload)
    }
}

#[cfg(test)]
mod tests {
    use dialog_capability::history::{ACTIVE_CONTEXT, Context, Edition, Origin, Version};
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
        let counts: Vec<_> = profile
            .recent
            .iter()
            .map(|recency| (recency.window, recency.origins, recency.exact))
            .collect();
        assert_eq!(
            counts,
            vec![(100, 2, true), (1_000, 3, true), (10_000, 3, true)]
        );
        assert!(profile.payload > 4 * 48, "the payload carries every entry");
    }

    /// A head carrying its active set counts its spilled origins, and a
    /// window reaching below the highest spilled edition is a lower bound.
    #[test]
    fn it_counts_spilled_origins_and_bounds_the_windows_they_reach() {
        let mut head = Revision::new(
            TreeReference::from([0u8; 32]),
            "branch:opaque".parse::<Entity>().expect("entity parses"),
            "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
                .parse()
                .expect("did parses"),
        );
        head.edition = Edition::new(20_000);
        let mut context = Context::new();
        for seed in 0..100u8 {
            context.record(Version::new(
                Origin::from([seed; 32]),
                Edition::new(19_000 + u64::from(seed)),
            ));
        }
        head.publish_context(&context);

        let profile = ContextProfile::of(&head).expect("the head has a context");
        assert_eq!(profile.origins, 100);
        assert_eq!(profile.active, ACTIVE_CONTEXT);
        assert_eq!(profile.spilled, (100 - ACTIVE_CONTEXT) as u64);
        let windows: Vec<_> = profile
            .recent
            .iter()
            .map(|recency| (recency.window, recency.exact))
            .collect();
        assert_eq!(
            windows,
            vec![(100, true), (1_000, false), (10_000, false)],
            "only the windows reaching the spilled editions are bounds"
        );
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
