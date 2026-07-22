use dialog_search_tree::Cache;

use crate::DialogArtifactsError;

use super::{Causality, Claim, History, Version, causality, common_ancestor};

/// The content identity of one side of a causality question: a claim
/// together with the version that produced it. Two sides with the same
/// fingerprint ask about the same claim, so they get the same answer.
type Fingerprint = [u8; 32];

fn fingerprint(claim: &Claim, version: &Version) -> Fingerprint {
    let bytes = serde_ipld_dagcbor::to_vec(&(claim, version))
        .expect("a claim and version encode canonically");
    *blake3::hash(&bytes).as_bytes()
}

/// Memoized causal resolution over a [`History`] index.
///
/// The facts this caches are immutable: history is append-only and
/// content-addressed, so the causal relationship between two *fixed*
/// claims — and the common ancestor of two *fixed* revisions — can never
/// change once determined. Later revisions extend the DAG above them;
/// nothing can insert causality between versions that already exist.
/// Even `Concurrent` and "no shared history" are permanent verdicts.
/// A memoized verdict therefore never needs invalidation, which is what
/// makes sharing one cache across queries, transactions, and pulls sound.
///
/// The one outcome that IS revisable —
/// [`IncompleteHistory`](DialogArtifactsError::IncompleteHistory), where a
/// partial replica cannot answer *yet* — surfaces as an error, and errors
/// are never cached: once the missing history replicates, the next call
/// walks the DAG and memoizes the definitive verdict.
///
/// Causality is keyed by the *claims'* content, not their versions alone:
/// one revision can mint sibling claims on the same `(entity, attribute)`
/// (cardinality-many) with different causes, and the verdict follows the
/// cause chain of the specific claim asked about. Common ancestry is a
/// pure function of the version pair, so it is keyed by that. Both keys
/// are order-normalized — asking `(a, b)` after `(b, a)` hits the same
/// entry, with [`Causality::inverse`] reorienting the verdict.
///
/// Storage is bounded ([`Cache`] evicts with SIEVE), so a long-lived
/// handle stays small; an evicted verdict simply re-derives.
#[derive(Clone, Debug, Default)]
pub struct CausalityCache {
    claims: Cache<(Fingerprint, Fingerprint), Causality>,
    ancestors: Cache<(Version, Version), Option<Version>>,
}

impl CausalityCache {
    /// A fresh, empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// [`causality`], remembering the verdict: the first call for a pair
    /// of claims walks the DAG, every later call — in either argument
    /// order — answers from memory.
    pub async fn causality<H: History>(
        &self,
        a: (&Claim, &Version),
        b: (&Claim, &Version),
        history: &H,
    ) -> Result<Causality, DialogArtifactsError> {
        let side_a = fingerprint(a.0, a.1);
        let side_b = fingerprint(b.0, b.1);
        // Normalize to a canonical orientation so both argument orders
        // share one entry; the stored verdict is the canonical one.
        let (key, swapped) = if side_a <= side_b {
            ((side_a, side_b), false)
        } else {
            ((side_b, side_a), true)
        };
        let verdict = self
            .claims
            .get_or_fetch::<_, DialogArtifactsError>(&key, async |_| {
                let verdict = causality(a, b, history).await?;
                Ok(Some(if swapped { verdict.inverse() } else { verdict }))
            })
            .await?
            .expect("causality always yields a verdict");
        Ok(if swapped { verdict.inverse() } else { verdict })
    }

    /// [`common_ancestor`], remembering the result — including `None`
    /// (two lineages that share no history never will).
    pub async fn common_ancestor<H: History>(
        &self,
        a: &Version,
        b: &Version,
        history: &H,
    ) -> Result<Option<Version>, DialogArtifactsError> {
        let key = if a <= b { (*a, *b) } else { (*b, *a) };
        Ok(self
            .ancestors
            .get_or_fetch::<_, DialogArtifactsError>(&key, async |_| {
                Ok(Some(common_ancestor(a, b, history).await?))
            })
            .await?
            .expect("the traversal always yields an outcome"))
    }
}
