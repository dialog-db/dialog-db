//! Resolving a domain's DKIM public key over DNS-over-HTTPS.
//!
//! This is deliberately **not** a `Provider<Resolve>`: the `Resolve` capability
//! maps a DID to a verifier document, but a DKIM key lookup resolves a
//! *domain's* signing key from the `s=`/`d=` tags of a captured proof, keyed by
//! a `(selector, domain)` pair rather than by a DID. So it is its own provider
//! seam, consumed by [`DidMailtoVerifier`](super::DidMailtoVerifier) during
//! verification. Its output is a [`DkimPublicKey`], not a `MultiVerifier`.
//!
//! It reuses the same injectable [`Fetch`] as `did:web`, so tests mock it and
//! there is no real network in the test suite. The lookup uses a JSON
//! DNS-over-HTTPS endpoint (`dns.google`) whose response is fully determined by
//! the request URL's query string, which is what a GET-only [`Fetch`] supports.
//!
//! Resolutions are cached with a TTL, like [`CachingResolver`](crate::CachingResolver).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use dialog_dkim::DkimPublicKey;
use serde::Deserialize;
use web_time::Instant;

use crate::error::ResolveError;
use crate::fetch::{Fetch, ReqwestFetch};

/// The default DNS-over-HTTPS JSON endpoint. The response is determined entirely
/// by the `name`/`type` query parameters, so a GET with no custom headers is
/// enough (a JSON DoH server does not require the `application/dns-json` Accept
/// header on this endpoint).
const DEFAULT_DOH_ENDPOINT: &str = "https://dns.google/resolve";

/// The default cache TTL for a resolved DKIM key.
pub const DEFAULT_DKIM_KEY_TTL: Duration = Duration::from_secs(3600);

/// Resolves a domain's DKIM public key by DNS-over-HTTPS TXT lookup.
///
/// Generic over a [`Fetch`] so the network dependency is mockable.
/// [`DkimKeyProvider::new`] uses [`ReqwestFetch`].
pub struct DkimKeyProvider<F = ReqwestFetch> {
    fetch: F,
    endpoint: String,
    ttl: Duration,
    cache: Mutex<HashMap<String, (Instant, DkimPublicKey)>>,
}

impl DkimKeyProvider<ReqwestFetch> {
    /// A provider backed by the default `reqwest` fetcher and DoH endpoint.
    #[must_use]
    pub fn new() -> Self {
        Self::with_fetch(ReqwestFetch::new())
    }
}

impl Default for DkimKeyProvider<ReqwestFetch> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F: Fetch> DkimKeyProvider<F> {
    /// A provider backed by a custom [`Fetch`].
    pub fn with_fetch(fetch: F) -> Self {
        Self {
            fetch,
            endpoint: DEFAULT_DOH_ENDPOINT.to_string(),
            ttl: DEFAULT_DKIM_KEY_TTL,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Override the DNS-over-HTTPS endpoint (for a different resolver or a test).
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    /// Override the cache TTL.
    #[must_use]
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// The DoH query URL for a `(selector, domain)` pair.
    ///
    /// The DKIM record lives at `<selector>._domainkey.<domain>`.
    #[must_use]
    pub fn query_url(&self, selector: &str, domain: &str) -> String {
        format!(
            "{}?name={selector}._domainkey.{domain}&type=TXT",
            self.endpoint
        )
    }

    /// Resolve the DKIM public key for `(selector, domain)`, caching the result.
    ///
    /// # Errors
    ///
    /// Returns [`ResolveError::Fetch`] on a transport failure, or a DKIM-shaped
    /// error surfaced as [`ResolveError::UnsupportedKey`] /
    /// [`ResolveError::MalformedDocument`] when the record is missing or invalid.
    pub async fn resolve_key(
        &self,
        selector: &str,
        domain: &str,
    ) -> Result<DkimPublicKey, ResolveError> {
        let cache_key = format!("{selector}._domainkey.{domain}");
        if let Some(hit) = self.cached(&cache_key) {
            return Ok(hit);
        }

        let url = self.query_url(selector, domain);
        let body = self.fetch.get(&url).await?;
        let record = parse_doh_txt(&body)?;
        let key = DkimPublicKey::from_dns_txt(&record).map_err(|e| {
            ResolveError::UnsupportedKey(format!("DKIM record for {cache_key} invalid: {e}"))
        })?;

        self.store(cache_key, key.clone());
        Ok(key)
    }

    fn cached(&self, key: &str) -> Option<DkimPublicKey> {
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        match cache.get(key) {
            Some((expires, value)) if *expires > Instant::now() => Some(value.clone()),
            Some(_) => {
                cache.remove(key);
                None
            }
            None => None,
        }
    }

    fn store(&self, key: String, value: DkimPublicKey) {
        let expires = Instant::now() + self.ttl;
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key, (expires, value));
    }
}

/// The subset of a JSON DNS-over-HTTPS response we read: the `Answer` array of
/// TXT records.
#[derive(Debug, Deserialize)]
struct DohResponse {
    #[serde(rename = "Answer", default)]
    answer: Vec<DohAnswer>,
}

#[derive(Debug, Deserialize)]
struct DohAnswer {
    /// The TXT record data. JSON DoH wraps each TXT string in double quotes; a
    /// long record may be split into several quoted strings concatenated here.
    #[serde(default)]
    data: String,
}

/// Parse a JSON DoH response body into a single DKIM TXT record string.
///
/// The TXT `data` field is a (possibly multi-part) double-quoted string; this
/// strips the quoting and concatenates the parts into the raw record body.
fn parse_doh_txt(body: &[u8]) -> Result<String, ResolveError> {
    let response: DohResponse = serde_json::from_slice(body)
        .map_err(|e| ResolveError::MalformedDocument(format!("DoH response not JSON: {e}")))?;

    // Pick the first answer that looks like a DKIM record (contains `p=`), so a
    // co-located non-DKIM TXT record does not shadow it.
    let record = response
        .answer
        .iter()
        .map(|a| unquote_txt(&a.data))
        .find(|r| r.contains("p="))
        .ok_or_else(|| ResolveError::UnsupportedKey("no DKIM TXT record in DoH response".into()))?;

    Ok(record)
}

/// Strip JSON-DoH double-quoting from a TXT `data` value and concatenate any
/// space-separated quoted chunks (`"part1" "part2"` becomes `part1part2`).
fn unquote_txt(data: &str) -> String {
    let mut out = String::with_capacity(data.len());
    let mut in_quote = false;
    let mut prev_escape = false;
    for ch in data.chars() {
        match ch {
            '"' if !prev_escape => in_quote = !in_quote,
            '\\' if !prev_escape => {
                prev_escape = true;
                continue;
            }
            _ if in_quote => out.push(ch),
            _ => {}
        }
        prev_escape = false;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_domainkey_query_url() {
        let provider = DkimKeyProvider::with_fetch(crate::fetch::MapFetch::new());
        assert_eq!(
            provider.query_url("sel", "example.com"),
            "https://dns.google/resolve?name=sel._domainkey.example.com&type=TXT"
        );
    }

    #[test]
    fn parses_single_quoted_txt_record() {
        let body = br#"{"Answer":[{"data":"\"v=DKIM1; k=rsa; p=ABC123\""}]}"#;
        let record = parse_doh_txt(body).unwrap();
        assert_eq!(record, "v=DKIM1; k=rsa; p=ABC123");
    }

    #[test]
    fn concatenates_multipart_txt_record() {
        let body = br#"{"Answer":[{"data":"\"v=DKIM1; k=rsa; p=AAAA\" \"BBBB\""}]}"#;
        let record = parse_doh_txt(body).unwrap();
        assert_eq!(record, "v=DKIM1; k=rsa; p=AAAABBBB");
    }

    #[test]
    fn skips_non_dkim_txt_records() {
        let body = br#"{"Answer":[{"data":"\"some-other-txt\""},{"data":"\"v=DKIM1; p=KEY\""}]}"#;
        let record = parse_doh_txt(body).unwrap();
        assert_eq!(record, "v=DKIM1; p=KEY");
    }

    #[test]
    fn empty_answer_is_an_error() {
        let body = br#"{"Answer":[]}"#;
        assert!(matches!(
            parse_doh_txt(body),
            Err(ResolveError::UnsupportedKey(_))
        ));
    }
}
