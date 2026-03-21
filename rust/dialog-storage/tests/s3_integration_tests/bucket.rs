//! Test helpers for S3 integration tests.

#![cfg(feature = "s3-integration-tests")]

use dialog_capability::Did;
use dialog_capability::Subject;
use dialog_effects::memory::prelude::{
    CellExt, MemoryExt, SpaceExt, SubjectExt as MemorySubjectExt,
};
use dialog_effects::memory::{MemoryError, Publication};
use dialog_effects::storage::StorageError;
use dialog_effects::storage::prelude::{StorageExt, StoreExt, SubjectExt as StorageSubjectExt};
use dialog_remote_s3::{S3, S3Address, S3Credentials, S3StorageError, helpers::Session};

/// Adds timestamp to the given string to make it unique
pub fn unique(base: &str) -> String {
    let millis = dialog_common::time::now()
        .duration_since(dialog_common::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{}-{}", base, millis)
}

/// Test context with S3 backend, credentials, subject, and session for integration tests.
pub struct TestBucket {
    pub s3: S3,
    pub address: S3Address,
    pub subject: Did,
    pub session: Session,
    pub store: String,
}

impl TestBucket {
    pub fn at(&self, path: &str) -> Self {
        TestBucket {
            s3: self.s3,
            address: self.address.clone(),
            subject: self.subject.clone(),
            session: self.session.clone(),
            store: format!("{}/{}", self.store, path),
        }
    }

    pub async fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), S3StorageError> {
        let authorized = Subject::from(self.subject.clone())
            .storage()
            .store(&self.store)
            .set(key, value)
            .fork(&self.address)
            .acquire(&self.session)
            .await
            .map_err(|e: dialog_capability::credential::AuthorizeError| {
                S3StorageError::AuthorizationError(e.to_string())
            })?;

        let result: Result<(), StorageError> = authorized.perform(&self.s3).await;
        result.map_err(|e| S3StorageError::ServiceError(e.to_string()))
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, S3StorageError> {
        let authorized = Subject::from(self.subject.clone())
            .storage()
            .store(&self.store)
            .get(key)
            .fork(&self.address)
            .acquire(&self.session)
            .await
            .map_err(|e: dialog_capability::credential::AuthorizeError| {
                S3StorageError::AuthorizationError(e.to_string())
            })?;

        let result: Result<Option<Vec<u8>>, StorageError> = authorized.perform(&self.s3).await;
        result.map_err(|e| S3StorageError::ServiceError(e.to_string()))
    }

    pub async fn delete(&self, key: &[u8]) -> Result<(), S3StorageError> {
        let authorized = Subject::from(self.subject.clone())
            .storage()
            .store(&self.store)
            .delete(key)
            .fork(&self.address)
            .acquire(&self.session)
            .await
            .map_err(|e: dialog_capability::credential::AuthorizeError| {
                S3StorageError::AuthorizationError(e.to_string())
            })?;

        let result: Result<(), StorageError> = authorized.perform(&self.s3).await;
        result.map_err(|e| S3StorageError::ServiceError(e.to_string()))
    }

    pub async fn resolve(
        &self,
        space: &str,
        cell: &str,
    ) -> Result<Option<Publication>, S3StorageError> {
        let authorized = Subject::from(self.subject.clone())
            .memory()
            .space(space)
            .cell(cell)
            .resolve()
            .fork(&self.address)
            .acquire(&self.session)
            .await
            .map_err(|e: dialog_capability::credential::AuthorizeError| {
                S3StorageError::AuthorizationError(e.to_string())
            })?;

        let result: Result<Option<Publication>, MemoryError> = authorized.perform(&self.s3).await;
        result.map_err(|e| S3StorageError::ServiceError(e.to_string()))
    }

    pub async fn publish(
        &self,
        space: &str,
        cell: &str,
        content: Vec<u8>,
        when: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, S3StorageError> {
        let authorized = Subject::from(self.subject.clone())
            .memory()
            .space(space)
            .cell(cell)
            .publish(content, when)
            .fork(&self.address)
            .acquire(&self.session)
            .await
            .map_err(|e: dialog_capability::credential::AuthorizeError| {
                S3StorageError::AuthorizationError(e.to_string())
            })?;

        let result: Result<Vec<u8>, MemoryError> = authorized.perform(&self.s3).await;
        result.map_err(|e| S3StorageError::ServiceError(e.to_string()))
    }

    #[allow(dead_code)]
    pub async fn retract(
        &self,
        space: &str,
        cell: &str,
        when: Vec<u8>,
    ) -> Result<(), S3StorageError> {
        let authorized = Subject::from(self.subject.clone())
            .memory()
            .space(space)
            .cell(cell)
            .retract(when)
            .fork(&self.address)
            .acquire(&self.session)
            .await
            .map_err(|e: dialog_capability::credential::AuthorizeError| {
                S3StorageError::AuthorizationError(e.to_string())
            })?;

        let result: Result<(), MemoryError> = authorized.perform(&self.s3).await;
        result.map_err(|e| S3StorageError::ServiceError(e.to_string()))
    }
}

/// Helper to create an S3 test context from environment variables.
pub fn open() -> TestBucket {
    #![allow(clippy::option_env_unwrap)]
    let address = S3Address::new(
        option_env!("R2S3_ENDPOINT").expect("R2S3_ENDPOINT not set"),
        option_env!("R2S3_REGION").expect("R2S3_REGION not set"),
        option_env!("R2S3_BUCKET").expect("R2S3_BUCKET not set"),
    );

    let subject: Did = option_env!("R2S3_SUBJECT")
        .unwrap_or("did:key:zTestSubject")
        .parse()
        .expect("Invalid DID in R2S3_SUBJECT");

    let credentials = S3Credentials::new(
        option_env!("R2S3_ACCESS_KEY_ID").expect("R2S3_ACCESS_KEY_ID not set"),
        option_env!("R2S3_SECRET_ACCESS_KEY").expect("R2S3_SECRET_ACCESS_KEY not set"),
    );

    let s3 = S3;
    let session = Session::new(subject.clone()).with_s3_credentials(credentials);

    TestBucket {
        s3,
        address,
        subject,
        session,
        store: "integration-tests".to_string(),
    }
}

pub fn open_unique_at(base: &str) -> TestBucket {
    open().at(&unique(base))
}
