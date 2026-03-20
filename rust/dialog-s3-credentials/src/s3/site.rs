//! S3 site configuration — pure config, no credentials.

use crate::Address;
use crate::capability::AuthorizedRequest;
use dialog_capability::Capability;
use dialog_capability::Effect;
use dialog_capability::access::Access;
use dialog_capability::authorization::Authorized;
use dialog_capability::command::Command;
use dialog_capability::site::Site;
use url::Url;

/// S3 access format — carries addressing info needed by the Authorize provider.
///
/// The Authorize provider reads the address from this value and uses its own
/// credentials to presign the request.
#[derive(Debug, Clone)]
pub struct S3Access {
    /// S3 address (endpoint, region, bucket).
    pub address: Address,
    /// Parsed endpoint URL.
    pub endpoint: Url,
    /// Whether to use path-style URLs.
    pub path_style: bool,
}

impl Access for S3Access {
    type Authorization = AuthorizedRequest;
}

/// S3 invocation — an authorized S3 request ready to execute via HTTP.
pub struct S3Invocation<Fx: Effect> {
    /// The presigned request.
    pub request: AuthorizedRequest,
    /// The capability (needed for request body context, e.g. Set value).
    pub capability: Capability<Fx>,
}

impl<Fx: Effect> Command for S3Invocation<Fx> {
    type Input = Self;
    type Output = Fx::Output;
}

impl<Fx: Effect> S3Invocation<Fx> {
    /// Perform the S3 invocation against a provider.
    pub async fn perform<Env>(self, env: &Env) -> Fx::Output
    where
        Env: dialog_capability::Provider<Self>,
    {
        env.execute(self).await
    }
}

impl<Fx: Effect> From<Authorized<Fx, S3Access>> for S3Invocation<Fx> {
    fn from(auth: Authorized<Fx, S3Access>) -> Self {
        S3Invocation {
            request: auth.authorization,
            capability: auth.capability,
        }
    }
}

/// S3 site configuration for direct S3 access.
///
/// Contains the endpoint and addressing info needed to build S3 URLs.
/// No credential material — that lives in the environment's credential store.
#[derive(Debug, Clone)]
pub struct S3Site {
    /// S3 address (endpoint, region, bucket).
    pub(crate) address: Address,
    /// Parsed endpoint URL.
    pub(crate) endpoint: Url,
    /// Whether to use path-style URLs.
    pub(crate) path_style: bool,
}

impl S3Site {
    /// Create a new S3 site from an address.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint URL is invalid.
    pub fn new(address: Address) -> Result<Self, crate::AccessError> {
        let endpoint = Url::parse(address.endpoint())
            .map_err(|e| crate::AccessError::Configuration(e.to_string()))?;
        let path_style = super::is_path_style_default(&endpoint);

        Ok(Self {
            address,
            endpoint,
            path_style,
        })
    }

    /// Enable path-style URL addressing.
    pub fn with_path_style(mut self) -> Self {
        self.path_style = true;
        self
    }

    /// Get the address.
    pub fn address(&self) -> &Address {
        &self.address
    }

    /// Get the endpoint URL.
    pub fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// Get the region.
    pub fn region(&self) -> &str {
        self.address.region()
    }

    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        self.address.bucket()
    }

    /// Whether path-style URLs are used.
    pub fn path_style(&self) -> bool {
        self.path_style
    }
}

// Blanket: route S3Invocation<Fx> to Environment's remote field.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Local, Credentials, Remote, Fx> dialog_capability::Provider<S3Invocation<Fx>>
    for dialog_effects::environment::Environment<Local, Credentials, Remote>
where
    Fx: Effect + 'static,
    S3Invocation<Fx>: dialog_common::ConditionalSend,
    Remote: dialog_capability::Provider<S3Invocation<Fx>> + dialog_common::ConditionalSync,
    Self: dialog_common::ConditionalSend + dialog_common::ConditionalSync,
{
    async fn execute(&self, input: S3Invocation<Fx>) -> Fx::Output {
        self.remote.execute(input).await
    }
}

impl Site for S3Site {
    type Access = S3Access;
    type Invocation<Fx: Effect> = S3Invocation<Fx>;

    fn access(&self) -> S3Access {
        S3Access {
            address: self.address.clone(),
            endpoint: self.endpoint.clone(),
            path_style: self.path_style,
        }
    }
}
