use super::s3;
#[cfg(feature = "ucan")]
use super::ucan;
/// Unified credentials enum supporting multiple authorization backends.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum Credentials {
    /// Direct S3 credentials (public or private).
    S3(s3::Credentials),
    /// UCAN-based authorization via external access service.
    #[cfg(feature = "ucan")]
    Ucan(ucan::Credentials),
}

impl std::hash::Hash for Credentials {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::S3(c) => c.hash(state),
            #[cfg(feature = "ucan")]
            Self::Ucan(c) => c.hash(state),
        }
    }
}

impl From<s3::Credentials> for Credentials {
    fn from(credentials: s3::Credentials) -> Self {
        Self::S3(credentials)
    }
}

#[cfg(feature = "ucan")]
impl From<ucan::Credentials> for Credentials {
    fn from(credentials: ucan::Credentials) -> Self {
        Self::Ucan(credentials)
    }
}

/// Unified site type that dispatches to S3 or UCAN backends.
#[derive(Debug, Clone)]
pub enum UnifiedSite {
    /// Direct S3 site.
    S3(s3::S3Site),
    /// UCAN-delegated site.
    #[cfg(feature = "ucan")]
    Ucan(ucan::site::UcanSite),
}

impl From<s3::S3Site> for UnifiedSite {
    fn from(site: s3::S3Site) -> Self {
        Self::S3(site)
    }
}

#[cfg(feature = "ucan")]
impl From<ucan::site::UcanSite> for UnifiedSite {
    fn from(site: ucan::site::UcanSite) -> Self {
        Self::Ucan(site)
    }
}
