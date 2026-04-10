//! Test environment for S3 integration tests.

#![cfg(feature = "s3-integration-tests")]

use dialog_capability::{Did, Subject};
use dialog_remote_s3::{Address, S3Credential, helpers::S3Network};

/// S3 test environment loaded from environment variables.
pub struct Environment {
    pub address: Address,
    subject: Did,
    pub network: S3Network,
}

impl Environment {
    /// Create an S3 test environment from environment variables.
    pub fn open() -> Self {
        #![allow(clippy::option_env_unwrap)]
        let address =
            Address::builder(option_env!("R2S3_ENDPOINT").expect("R2S3_ENDPOINT not set"))
                .region(option_env!("R2S3_REGION").expect("R2S3_REGION not set"))
                .bucket(option_env!("R2S3_BUCKET").expect("R2S3_BUCKET not set"))
                .build()
                .expect("Invalid S3 address configuration");

        let subject: Did = option_env!("R2S3_SUBJECT")
            .unwrap_or("did:key:zTestSubject")
            .parse()
            .expect("Invalid DID in R2S3_SUBJECT");

        let credentials = S3Credential::new(
            option_env!("R2S3_ACCESS_KEY_ID").expect("R2S3_ACCESS_KEY_ID not set"),
            option_env!("R2S3_SECRET_ACCESS_KEY").expect("R2S3_SECRET_ACCESS_KEY not set"),
        );

        Self {
            address,
            subject,
            network: S3Network::from(credentials),
        }
    }

    /// Get the subject for building capability chains.
    pub fn subject(&self) -> Subject {
        Subject::from(self.subject.clone())
    }

    /// Whether this environment targets Cloudflare R2.
    pub fn is_r2(&self) -> bool {
        self.address
            .endpoint()
            .host_str()
            .is_some_and(|h| h.ends_with(".r2.cloudflarestorage.com"))
    }

    /// Generate a unique string for test isolation.
    pub fn unique(base: &str) -> String {
        let millis = dialog_common::time::now()
            .duration_since(dialog_common::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        format!("{}-{}", base, millis)
    }
}
