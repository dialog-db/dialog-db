//! S3 storage address types.
//!
//! This module provides the [`Address`] type for specifying S3-compatible storage locations.
//! Use [`Address::builder`] to construct with validation.

use super::S3;
use crate::S3Error;
use dialog_capability::access::AuthorizeError;
use dialog_capability::site::SiteAddress;
use dialog_capability::{Capability, Provider, SiteId};
use dialog_common::ConditionalSync;
use dialog_effects::authority;
use dialog_effects::credential::Secret;
use dialog_effects::credential::prelude::*;
use serde::{Deserialize, Serialize};
use url::{Host, Url};

/// Address for S3-compatible storage.
///
/// The endpoint URL is normalized at construction: for virtual-hosted style,
/// the bucket is prepended to the host. All URL construction at request time
/// is just path manipulation.
///
/// # Examples
///
/// ```no_run
/// # use dialog_remote_s3::Address;
/// let addr = Address::builder("https://s3.us-east-1.amazonaws.com")
///     .region("us-east-1")
///     .bucket("my-bucket")
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Address {
    /// Normalized endpoint URL.
    /// Virtual-hosted: `https://{bucket}.s3.{region}.amazonaws.com/`
    /// Path-style: `https://s3.{region}.amazonaws.com/`
    endpoint: Url,
    /// AWS region for signing (e.g., "us-east-1", "auto" for R2)
    region: String,
    /// Bucket name
    bucket: String,
    /// Whether to use path-style URLs
    #[serde(default)]
    path_style: bool,
    /// Pre-computed host:port for HTTP Host header
    authority: String,
}

impl PartialEq for Address {
    fn eq(&self, other: &Self) -> bool {
        self.endpoint.as_str() == other.endpoint.as_str()
            && self.region == other.region
            && self.bucket == other.bucket
            && self.path_style == other.path_style
    }
}

impl Eq for Address {}

impl std::hash::Hash for Address {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.endpoint.as_str().hash(state);
        self.region.hash(state);
        self.bucket.hash(state);
        self.path_style.hash(state);
    }
}

impl Address {
    /// Create an [`AddressBuilder`].
    pub fn builder(endpoint: impl Into<String>) -> AddressBuilder {
        AddressBuilder {
            endpoint: endpoint.into(),
            region: None,
            bucket: None,
            path_style: None,
        }
    }

    /// Get the normalized endpoint URL.
    pub fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// Get the region.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Whether path-style URLs are used.
    pub fn path_style(&self) -> bool {
        self.path_style
    }

    /// Get the pre-computed authority (host + port) for HTTP headers.
    pub(crate) fn authority(&self) -> &str {
        &self.authority
    }

    /// A stable identifier for this address as a URI.
    pub fn id(&self) -> String {
        let mut id = format!(
            "{}?region={}&bucket={}",
            self.endpoint, self.region, self.bucket,
        );
        if self.path_style {
            id.push_str("&path_style");
        }
        id
    }

    /// Resolve a key path into a full S3 URL.
    pub fn resolve(&self, path: &str) -> Url {
        let mut url = self.endpoint.clone();
        if self.path_style {
            url.set_path(&format!("{}/{}", self.bucket, path));
        } else if path.is_empty() {
            url.set_path("/");
        } else {
            url.set_path(path);
        }
        url
    }
}

impl Address {
    /// Authorize a capability for execution at this S3 site.
    ///
    /// Loads credentials from the secret store using the address id as key.
    pub async fn authorize<Env>(
        &self,
        operator: &Capability<authority::Operator>,
        env: &Env,
    ) -> Result<super::S3Authorization, AuthorizeError>
    where
        Env: Provider<dialog_effects::credential::Load<Secret>> + ConditionalSync,
    {
        use authority::OperatorExt;

        let secret: Secret = operator
            .profile()
            .clone()
            .credential()
            .site(self)
            .load()
            .perform(env)
            .await?;

        secret.try_into().map_err(Into::into)
    }
}

/// `Address` is the canonical address type for the `S3` site.
impl SiteAddress for Address {
    type Site = S3;
}

impl From<Address> for SiteId {
    fn from(address: Address) -> Self {
        address.id().into()
    }
}

/// Builder for constructing an [`Address`] with validation.
///
/// # Examples
///
/// ```no_run
/// # use dialog_remote_s3::Address;
/// let addr = Address::builder("https://s3.us-east-1.amazonaws.com")
///     .region("us-east-1")
///     .bucket("my-bucket")
///     .build()
///     .unwrap();
/// ```
pub struct AddressBuilder {
    endpoint: String,
    region: Option<String>,
    bucket: Option<String>,
    path_style: Option<bool>,
}

impl AddressBuilder {
    /// Set the AWS region for signing.
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Set the bucket name.
    pub fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = Some(bucket.into());
        self
    }

    /// Force path-style URL addressing.
    pub fn path_style(mut self, path_style: bool) -> Self {
        self.path_style = Some(path_style);
        self
    }

    /// Build the address, parsing and validating the endpoint URL.
    ///
    /// For virtual-hosted style, the bucket is prepended to the endpoint
    /// host so that `resolve` only needs to set the path.
    pub fn build(self) -> Result<Address, S3Error> {
        let mut endpoint =
            Url::parse(&self.endpoint).map_err(|e| S3Error::Configuration(e.to_string()))?;

        let host = endpoint
            .host_str()
            .ok_or_else(|| S3Error::Configuration("endpoint URL must have a host".into()))?
            .to_string();

        let region = self
            .region
            .ok_or_else(|| S3Error::Configuration("region is required".into()))?;

        let bucket = self
            .bucket
            .ok_or_else(|| S3Error::Configuration("bucket is required".into()))?;

        let path_style = self.path_style.unwrap_or_else(|| {
            matches!(
                endpoint.host(),
                Some(Host::Ipv4(_)) | Some(Host::Ipv6(_)) | Some(Host::Domain("localhost"))
            )
        });

        // For virtual-hosted style, normalize the endpoint by prepending
        // the bucket to the host.
        let authority = if path_style {
            match endpoint.port() {
                Some(port) => format!("{}:{}", host, port),
                None => host,
            }
        } else {
            let new_host = format!("{}.{}", bucket, host);
            endpoint
                .set_host(Some(&new_host))
                .map_err(|e| S3Error::Configuration(format!("Invalid host: {}", e)))?;
            match endpoint.port() {
                Some(port) => format!("{}:{}", new_host, port),
                None => new_host,
            }
        };

        Ok(Address {
            endpoint,
            region,
            bucket,
            path_style,
            authority,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    fn it_builds_s3_address() {
        let addr = Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("my-bucket")
            .build()
            .unwrap();

        assert_eq!(addr.region(), "us-east-1");
        assert_eq!(addr.bucket(), "my-bucket");
        assert!(!addr.path_style());
    }

    #[dialog_common::test]
    fn it_builds_r2_address() {
        let addr = Address::builder("https://account-id.r2.cloudflarestorage.com")
            .region("auto")
            .bucket("my-bucket")
            .build()
            .unwrap();

        assert_eq!(addr.region(), "auto");
        assert_eq!(addr.bucket(), "my-bucket");
    }

    #[dialog_common::test]
    fn it_detects_path_style_for_localhost() {
        let addr = Address::builder("http://localhost:9000")
            .region("us-east-1")
            .bucket("my-bucket")
            .build()
            .unwrap();

        assert!(addr.path_style());
    }

    #[dialog_common::test]
    fn it_roundtrips_through_serde() {
        let addr = Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("my-bucket")
            .build()
            .unwrap();

        let json = serde_json::to_string(&addr).unwrap();
        let parsed: Address = serde_json::from_str(&json).unwrap();
        assert_eq!(addr, parsed);
    }

    #[dialog_common::test]
    fn it_supports_explicit_path_style() {
        let addr = Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("my-bucket")
            .path_style(true)
            .build()
            .unwrap();

        assert!(addr.path_style());
    }

    #[dialog_common::test]
    fn it_errors_on_missing_region() {
        let result = Address::builder("https://s3.amazonaws.com")
            .bucket("my-bucket")
            .build();

        assert!(result.is_err());
    }

    #[dialog_common::test]
    fn it_errors_on_missing_bucket() {
        let result = Address::builder("https://s3.amazonaws.com")
            .region("us-east-1")
            .build();

        assert!(result.is_err());
    }

    #[dialog_common::test]
    fn it_errors_on_invalid_url() {
        let result = Address::builder("not a url")
            .region("us-east-1")
            .bucket("my-bucket")
            .build();

        assert!(result.is_err());
    }
}
