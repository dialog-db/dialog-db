//! A repository held at a peer.

use crate::schema::{DidExt as _, Replica};
use crate::{PublishError, RemoteAddress, ResolveError, SiteAddress, site_address};
use dialog_artifacts::Entity;
use dialog_capability::{Did, Subject};
use dialog_effects::Rejection;
use dialog_effects::archive::ArchiveError;
use dialog_effects::blob::BlobError;
use dialog_effects::memory::MemoryError;
use dialog_effects::peer::PeerConnection;
use dialog_varsig::Principal;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A replica on a connected peer: which peer holds it, the addresses
/// the host reaches that peer at, and which repository it is a replica
/// of. The local repository is a replica too; this is one held
/// elsewhere, reached through
/// [`contact(..).connect()`](crate::ContactReference::connect).
///
/// What was a named remote is these two things together. The peer is
/// who holds it and where to reach them; the repository is which of the
/// peer's replicas this is. Their state is cached locally, keyed by
/// entity.
///
/// Requests go to one of the peer's addresses at a time, starting with
/// the one that last answered, as the host's connection to the peer
/// records it. One that cannot be reached is passed over for the next.
#[derive(Debug, Clone)]
pub struct ConnectedReplica {
    host: Subject,
    peer: Entity,
    name: Option<String>,
    addresses: Vec<SiteAddress>,
    subject: Did,
    /// The address that last answered, shared by every clone of this
    /// handle so that each request does not rediscover it.
    answered: Arc<AtomicUsize>,
}

impl ConnectedReplica {
    /// A repository `subject` held at `peer`, reached at `addresses`,
    /// with its state cached under `host`. `addresses` is not empty: a
    /// peer with nowhere to reach it is not connected to.
    pub(crate) fn new(
        host: Subject,
        peer: Entity,
        name: Option<String>,
        addresses: Vec<SiteAddress>,
        subject: Did,
    ) -> Self {
        Self {
            host,
            peer,
            name,
            addresses,
            subject,
            answered: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The replica of `subject` at the peer `connection` reaches, with
    /// its state cached under `host`. Reaching it goes through the
    /// connection's addresses, and shares with every other user of the
    /// connection which of them answered last.
    pub(crate) fn connected(
        host: Subject,
        connection: &PeerConnection,
        name: Option<String>,
        subject: Did,
    ) -> Result<Self, crate::PeerError> {
        let addresses = connection
            .addresses()
            .iter()
            .map(site_address)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            host,
            peer: connection.peer().clone(),
            name,
            addresses,
            subject,
            answered: connection.answers(),
        })
    }

    /// This replica, reaching its peer through `answered`: the record of
    /// which address answered last that the host's connection keeps.
    pub(crate) fn sharing(mut self, answered: Arc<AtomicUsize>) -> Self {
        self.answered = answered;
        self
    }

    /// The subject DID of the repository.
    pub fn did(&self) -> Did {
        self.subject.clone()
    }

    /// The peer holding the repository.
    pub fn peer(&self) -> &Entity {
        &self.peer
    }

    /// The peer's local name, or its entity when it has none: how the
    /// peer is named in messages.
    pub fn name(&self) -> String {
        self.name.clone().unwrap_or_else(|| self.peer.to_string())
    }

    /// The peer's local name, if it has one.
    pub fn label(&self) -> Option<String> {
        self.name.clone()
    }

    /// Every address the peer is reached at.
    pub fn addresses(&self) -> &[SiteAddress] {
        &self.addresses
    }

    /// Where the next request goes: the address that last answered (the
    /// peer's first, until one has), and the repository there.
    pub fn address(&self) -> RemoteAddress {
        self.at(self.answered.load(Ordering::Relaxed))
    }

    /// The repository at the peer's address `index`.
    fn at(&self, index: usize) -> RemoteAddress {
        let address = &self.addresses[index % self.addresses.len()];
        RemoteAddress::new(address.clone(), self.subject.clone())
    }

    /// Run `request` against the peer's addresses in turn, starting from
    /// the one that last answered, until one answers.
    ///
    /// Any answer is final, a refusal or a conflict as much as a success:
    /// the next address reaches the same peer, which would say the same.
    /// Only an address that could not be reached is passed over. When
    /// none can be, the last one's error is returned.
    pub(crate) async fn reach<T, E, F, Fut>(&self, request: F) -> Result<T, E>
    where
        F: FnMut(RemoteAddress) -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: Unreachable,
    {
        self.reach_unless(E::unreachable, request).await
    }

    /// [`reach`](Self::reach), passing a failure on to the next address
    /// only when `elsewhere` says the request may be sent there.
    pub(crate) async fn reach_unless<T, E, F, Fut>(
        &self,
        elsewhere: impl Fn(&E) -> bool,
        mut request: F,
    ) -> Result<T, E>
    where
        F: FnMut(RemoteAddress) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let count = self.addresses.len();
        let first = self.answered.load(Ordering::Relaxed) % count;
        let mut index = first;
        loop {
            let result = request(self.at(index)).await;
            match &result {
                Err(error) if elsewhere(error) && (index + 1) % count != first => {
                    index = (index + 1) % count;
                }
                Err(error) if elsewhere(error) => return result,
                _ => {
                    self.answered.store(index, Ordering::Relaxed);
                    return result;
                }
            }
        }
    }

    /// The peer's replica of the repository.
    pub fn replica(&self) -> Replica {
        Replica::derive(self.peer.clone(), self.subject.this())
    }

    /// Whether `other` is the same repository at the same peer, however
    /// it was reached.
    pub fn same(&self, other: &ConnectedReplica) -> bool {
        self.peer == other.peer && self.subject == other.subject
    }

    /// The local repository this handle's state is cached under.
    pub(crate) fn host(&self) -> &Subject {
        &self.host
    }
}

/// Whether an error means the address could not serve the request, so
/// another address of the same peer might.
///
/// The backends report a failed connection, a timeout, or a response that
/// could not be read as a storage error, and a responder that says it is
/// temporarily unable to serve as [`Rejection::Unavailable`]. Everything
/// else is the peer's answer.
pub(crate) trait Unreachable {
    /// Whether another address is worth trying.
    fn unreachable(&self) -> bool;
}

impl Unreachable for ArchiveError {
    fn unreachable(&self) -> bool {
        matches!(
            self,
            ArchiveError::Storage(_) | ArchiveError::Rejected(Rejection::Unavailable { .. })
        )
    }
}

impl Unreachable for MemoryError {
    fn unreachable(&self) -> bool {
        matches!(
            self,
            MemoryError::Storage(_) | MemoryError::Rejected(Rejection::Unavailable { .. })
        )
    }
}

impl Unreachable for BlobError {
    fn unreachable(&self) -> bool {
        matches!(
            self,
            BlobError::Storage(_) | BlobError::Rejected(Rejection::Unavailable { .. })
        )
    }
}

impl Unreachable for ResolveError {
    fn unreachable(&self) -> bool {
        matches!(
            self,
            ResolveError::Storage(_)
                | ResolveError::Io(_)
                | ResolveError::Rejected(Rejection::Unavailable { .. })
        )
    }
}

/// A publish is a conditional write, so only one that never left may be
/// sent elsewhere: one that failed on the wire may have landed, and would
/// then conflict with itself at the next address.
impl Unreachable for PublishError {
    fn unreachable(&self) -> bool {
        matches!(self, PublishError::Rejected(Rejection::Unavailable { .. }))
    }
}

impl Principal for ConnectedReplica {
    fn did(&self) -> Did {
        self.subject.clone()
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::ConnectedReplica;
    use crate::PublishError;
    use crate::SiteAddress;
    use dialog_artifacts::Entity;
    use dialog_capability::Subject;
    use dialog_effects::Rejection;
    use dialog_effects::memory::MemoryError;
    use dialog_remote_s3::Address as S3Address;
    use dialog_varsig::did;
    use std::sync::Mutex;

    fn site(endpoint: &str) -> SiteAddress {
        S3Address::builder(endpoint)
            .region("us-east-1")
            .bucket("bucket")
            .build()
            .unwrap()
            .into()
    }

    /// A repository at a peer reached at `endpoints`, in that order.
    fn remote(endpoints: &[&str]) -> ConnectedReplica {
        ConnectedReplica::new(
            Subject::from(did!("key:z6MkkZfZmshVFcBYo9RS6ZyUstxYdjjStQaFaL2TSTVdsiJh")),
            Entity::new().unwrap(),
            None,
            endpoints.iter().map(|endpoint| site(endpoint)).collect(),
            did!("key:z6MkkZfZmshVFcBYo9RS6ZyUstxYdjjStQaFaL2TSTVdsiJh"),
        )
    }

    fn unreachable() -> MemoryError {
        MemoryError::Storage("connection refused".into())
    }

    #[dialog_common::test]
    async fn it_passes_over_an_address_it_cannot_reach() {
        let remote = remote(&["https://a.example", "https://b.example"]);
        let tried = Mutex::new(Vec::new());
        let answered = remote
            .reach(|address| {
                tried.lock().unwrap().push(address.site().clone());
                let reached = address.site() == &site("https://b.example");
                async move { if reached { Ok(()) } else { Err(unreachable()) } }
            })
            .await;
        assert!(answered.is_ok());
        assert_eq!(
            *tried.lock().unwrap(),
            vec![site("https://a.example"), site("https://b.example")]
        );

        // The next request starts at the address that answered, and so do
        // requests through clones of the handle.
        assert_eq!(remote.address().site(), &site("https://b.example"));
        assert_eq!(remote.clone().address().site(), &site("https://b.example"));
    }

    #[dialog_common::test]
    async fn it_takes_an_answer_as_final() {
        let remote = remote(&["https://a.example", "https://b.example"]);
        let mut attempts = 0;
        let answered: Result<(), MemoryError> = remote
            .reach(|_| {
                attempts += 1;
                async {
                    Err(MemoryError::VersionMismatch {
                        expected: None,
                        actual: None,
                    })
                }
            })
            .await;
        assert!(matches!(answered, Err(MemoryError::VersionMismatch { .. })));
        assert_eq!(
            attempts, 1,
            "a conflict is the peer's answer, not a failure to reach it"
        );
    }

    #[dialog_common::test]
    async fn it_tries_each_address_once_when_none_can_be_reached() {
        let remote = remote(&[
            "https://a.example",
            "https://b.example",
            "https://c.example",
        ]);
        let mut attempts = 0;
        let answered: Result<(), MemoryError> = remote
            .reach(|_| {
                attempts += 1;
                async { Err(unreachable()) }
            })
            .await;
        assert!(matches!(answered, Err(MemoryError::Storage(_))));
        assert_eq!(attempts, 3);
        assert_eq!(remote.address().site(), &site("https://a.example"));
    }

    /// A publish is a conditional write. One that failed on the wire may
    /// still have landed, and sending it to another address would then
    /// report a conflict with itself: it is not sent again.
    #[dialog_common::test]
    async fn it_does_not_resend_a_publish_that_may_have_landed() {
        let remote = remote(&["https://a.example", "https://b.example"]);
        let mut attempts = 0;
        let answered: Result<(), PublishError> = remote
            .reach(|_| {
                attempts += 1;
                async { Err(PublishError::Storage("timed out".into())) }
            })
            .await;
        assert!(matches!(answered, Err(PublishError::Storage(_))));
        assert_eq!(attempts, 1, "the publish may have landed at the first");
    }

    /// A publish that never left, because the address could not be
    /// connected to, is sent to the next address.
    #[dialog_common::test]
    async fn it_sends_a_publish_that_never_left_to_the_next_address() {
        let remote = remote(&["https://a.example", "https://b.example"]);
        let mut attempts = 0;
        let answered: Result<(), PublishError> = remote
            .reach(|_| {
                attempts += 1;
                let first = attempts == 1;
                async move {
                    if first {
                        Err(PublishError::Rejected(Rejection::Unavailable {
                            reason: "connection refused".into(),
                        }))
                    } else {
                        Ok(())
                    }
                }
            })
            .await;
        assert!(answered.is_ok());
        assert_eq!(attempts, 2);
    }
}
