//! Native provider implementations: real transport over the global node.

use dialog_capability::{ForkInvocation, Policy, Provider};
use dialog_effects::archive::{self, ArchiveError};
use dialog_effects::blob::{self, BlobError, BlobReader, BlobWriter};
use dialog_effects::memory::{self, Edition, MemoryError, Version};
use ipld_core::ipld::Ipld;
use iroh::endpoint::Connection;

use crate::iroh::transport::request;
use crate::protocol::WireError;
use crate::{Iroh, IrohAddress, IrohAuthorization, IrohRemoteError, node};

/// Serialize the attested invocation for the wire.
fn container(authorization: &IrohAuthorization) -> Result<Vec<u8>, IrohRemoteError> {
    authorization
        .invocation()
        .to_bytes()
        .map_err(IrohRemoteError::Protocol)
}

/// Dial the addressed peer over the process-global node.
async fn connect(address: &IrohAddress) -> Result<Connection, IrohRemoteError> {
    node().await?.connect(address).await
}

fn archive_error(e: impl Into<TransportError>) -> ArchiveError {
    match e.into() {
        TransportError::Denied(e) => ArchiveError::AuthorizationError(e),
        TransportError::Other(e) => ArchiveError::ExecutionError(e),
    }
}

fn blob_error(e: impl Into<TransportError>) -> BlobError {
    match e.into() {
        TransportError::Denied(e) => BlobError::AuthorizationError(e),
        TransportError::Other(e) => BlobError::ExecutionError(e),
    }
}

fn memory_wire_error(e: WireError) -> MemoryError {
    match e {
        WireError::Denied(e) => MemoryError::Authorization(e),
        WireError::VersionMismatch { expected, actual } => {
            MemoryError::VersionMismatch { expected, actual }
        }
        other => MemoryError::Storage(other.to_string()),
    }
}

/// Flattens [`IrohRemoteError`] and [`WireError`] into denied-vs-other for
/// mapping onto effect error types.
enum TransportError {
    Denied(String),
    Other(String),
}

impl From<IrohRemoteError> for TransportError {
    fn from(e: IrohRemoteError) -> Self {
        match e {
            IrohRemoteError::Denied(e) => TransportError::Denied(e),
            other => TransportError::Other(other.to_string()),
        }
    }
}

impl From<WireError> for TransportError {
    fn from(e: WireError) -> Self {
        match e {
            WireError::Denied(e) => TransportError::Denied(e),
            other => TransportError::Other(other.to_string()),
        }
    }
}

/// Extract the catalog name from the invocation's argument map (for the
/// swarm `Want`).
fn catalog_of(authorization: &IrohAuthorization) -> Option<String> {
    let arguments = authorization.invocation().chain().arguments();
    arguments.iter().find_map(|(key, value)| {
        if key.as_str() != "catalog" {
            return None;
        }
        match Ipld::try_from(value) {
            Ok(Ipld::String(catalog)) => Some(catalog),
            _ => None,
        }
    })
}

#[async_trait::async_trait]
impl Provider<ForkInvocation<Iroh, archive::Get>> for Iroh {
    async fn execute(
        &self,
        input: ForkInvocation<Iroh, archive::Get>,
    ) -> Result<Option<Vec<u8>>, ArchiveError> {
        let invocation = container(&input.authorization).map_err(archive_error)?;

        // Direct attempt against the addressed peer.
        let direct = async {
            let connection = connect(&input.address).await?;
            request::archive_get(&connection, invocation.clone()).await
        }
        .await;

        let miss = !matches!(&direct, Ok(Ok(Some(_))));

        // Swarm fallback: any peer replicating this space may have the
        // block. The invocation is rooted in the subject, so it is valid
        // at whichever peer answers.
        if miss
            && let Ok(node) = node().await
            && let Some(swarm) = node.swarm(input.capability.subject()).await
        {
            let digest = <archive::Get as Policy>::of(&input.capability)
                .digest
                .clone();
            let catalog = catalog_of(&input.authorization).unwrap_or_else(|| "index".to_string());
            if let Some(block) = swarm.fetch(&node, &catalog, &digest, &invocation).await {
                return Ok(Some(block));
            }
        }

        match direct {
            Ok(Ok(block)) => Ok(block),
            Ok(Err(wire)) => Err(archive_error(wire)),
            Err(transport) => Err(archive_error(transport)),
        }
    }
}

#[async_trait::async_trait]
impl Provider<ForkInvocation<Iroh, archive::Put>> for Iroh {
    async fn execute(&self, input: ForkInvocation<Iroh, archive::Put>) -> Result<(), ArchiveError> {
        let invocation = container(&input.authorization).map_err(archive_error)?;
        let block = <archive::Put as Policy>::of(&input.capability)
            .block
            .as_ref()
            .to_vec();
        let connection = connect(&input.address).await.map_err(archive_error)?;
        request::archive_put(&connection, invocation, block)
            .await
            .map_err(archive_error)?
            .map_err(archive_error)
    }
}

#[async_trait::async_trait]
impl Provider<ForkInvocation<Iroh, archive::Import>> for Iroh {
    async fn execute(
        &self,
        input: ForkInvocation<Iroh, archive::Import>,
    ) -> Result<(), ArchiveError> {
        let invocation = container(&input.authorization).map_err(archive_error)?;
        let blocks = <archive::Import as Policy>::of(&input.capability)
            .blocks
            .iter()
            .map(|block| serde_bytes::ByteBuf::from(block.as_ref().to_vec()))
            .collect();
        let connection = connect(&input.address).await.map_err(archive_error)?;
        request::archive_import(&connection, invocation, blocks)
            .await
            .map_err(archive_error)?
            .map_err(archive_error)
    }
}

#[async_trait::async_trait]
impl Provider<ForkInvocation<Iroh, memory::Resolve>> for Iroh {
    async fn execute(
        &self,
        input: ForkInvocation<Iroh, memory::Resolve>,
    ) -> Result<Option<Edition<Vec<u8>>>, MemoryError> {
        let invocation =
            container(&input.authorization).map_err(|e| MemoryError::Storage(e.to_string()))?;
        let connection = connect(&input.address)
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;
        request::memory_resolve(&connection, invocation)
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?
            .map_err(memory_wire_error)
    }
}

#[async_trait::async_trait]
impl Provider<ForkInvocation<Iroh, memory::Publish>> for Iroh {
    async fn execute(
        &self,
        input: ForkInvocation<Iroh, memory::Publish>,
    ) -> Result<Version, MemoryError> {
        let invocation =
            container(&input.authorization).map_err(|e| MemoryError::Storage(e.to_string()))?;
        let content = <memory::Publish as Policy>::of(&input.capability)
            .content
            .clone();
        let connection = connect(&input.address)
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;
        request::memory_publish(&connection, invocation, content)
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?
            .map_err(memory_wire_error)
    }
}

#[async_trait::async_trait]
impl Provider<ForkInvocation<Iroh, memory::Retract>> for Iroh {
    async fn execute(
        &self,
        input: ForkInvocation<Iroh, memory::Retract>,
    ) -> Result<(), MemoryError> {
        let invocation =
            container(&input.authorization).map_err(|e| MemoryError::Storage(e.to_string()))?;
        let connection = connect(&input.address)
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;
        request::memory_retract(&connection, invocation)
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?
            .map_err(memory_wire_error)
    }
}

#[async_trait::async_trait]
impl Provider<ForkInvocation<Iroh, blob::Read>> for Iroh {
    async fn execute(
        &self,
        input: ForkInvocation<Iroh, blob::Read>,
    ) -> Result<BlobReader, BlobError> {
        let invocation = container(&input.authorization).map_err(blob_error)?;
        let connection = connect(&input.address).await.map_err(blob_error)?;
        let source = request::blob_read(&connection, invocation)
            .await
            .map_err(blob_error)?
            .map_err(blob_error)?;
        Ok(Box::new(source))
    }
}

#[async_trait::async_trait]
impl Provider<ForkInvocation<Iroh, blob::Import>> for Iroh {
    async fn execute(
        &self,
        input: ForkInvocation<Iroh, blob::Import>,
    ) -> Result<BlobWriter, BlobError> {
        let invocation = container(&input.authorization).map_err(blob_error)?;
        let connection = connect(&input.address).await.map_err(blob_error)?;
        let sink = request::blob_import(&connection, invocation)
            .await
            .map_err(blob_error)?;
        Ok(Box::new(sink))
    }
}
