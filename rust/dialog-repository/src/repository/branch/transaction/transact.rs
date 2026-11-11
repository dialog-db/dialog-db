//! A transaction that runs again when it loses its branch's race.
//!
//! [`Branch::transact`] takes the code that builds a transaction, not the
//! transaction: each try hands it a fresh [`Transaction`] on the branch's
//! head as it is then, commits what it returns, and publishes that
//! compare-and-swapped against the head it read. When another writer moved
//! the head first, the head that won is read and the code runs again, its
//! queries now reading over the winner, so a write computed from a read is
//! computed again, as a transaction in a software transactional memory is.
//!
//! ```no_run
//! # use dialog_capability::{Fork, Provider};
//! # use dialog_corpus::{Entity, Fact, Instruction, Pick, Value};
//! # use dialog_query::attribute::Relation;
//! # use dialog_query::query::Output as _;
//! # use dialog_query::{AttributeQuery, Term, the};
//! # use dialog_corpus::tree::CorpusNode;
//! # use dialog_effects::{archive, authority, blob, memory};
//! # use dialog_repository::{Branch, RemoteSite};
//! # async fn example<Env>(branch: &Branch, env: &Env) -> anyhow::Result<()>
//! # where
//! #     Env: Provider<blob::Size> + Provider<blob::Import> + Provider<archive::Get>
//! #         + Provider<blob::Read> + Provider<archive::Put> + Provider<archive::Import>
//! #         + Provider<dialog_search_tree::nodes::Write<CorpusNode>>
//! #         + Provider<memory::Resolve> + Provider<memory::Publish>
//! #         + Provider<authority::Identify> + Provider<authority::Attest>
//! #         + Provider<archive::Load> + Provider<dialog_search_tree::nodes::Read<CorpusNode>>
//! #         + Provider<dialog_search_tree::nodes::Preload<CorpusNode>> + Provider<blob::Load>
//! #         + Provider<dialog_corpus::Preload> + Provider<dialog_corpus::Speculation>
//! #         + Provider<Fork<RemoteSite, memory::Resolve>> + dialog_common::Holds
//! #         + dialog_common::ConditionalSync + 'static,
//! # {
//! // Records how many names the branch holds: on a lost race the names
//! // are read again, over the head that won.
//! branch
//!     .transact(|transaction| async move {
//!         let names = transaction
//!             .query()
//!             .select(AttributeQuery::from(
//!                 Term::<Relation>::from(the!("user/name"))
//!                     .of(Term::<Entity>::var("person"))
//!                     .is(Term::<String>::var("name")),
//!             ))
//!             .perform(env)
//!             .try_vec()
//!             .await?;
//!         let count = Fact {
//!             the: "user/count".parse()?,
//!             of: "user:all".parse()?,
//!             is: Value::UnsignedInt(names.len() as u128),
//!             cause: None,
//!         };
//!         anyhow::Ok(transaction.integrate(
//!             [Instruction::Assert(count, Pick::All)].into_iter().collect(),
//!         ))
//!     })
//!     .perform(env)
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::future::Future;

use super::Transaction;
use crate::repository::branch::commit::RETRIES;
use crate::{Branch, CommitError, PublishError, RemoteSite, Revision};
use dialog_capability::{Fork, Provider};
use dialog_common::{ConditionalSync, Holds};
use dialog_corpus::tree::CorpusNode;
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::Import as BlobImport;
use dialog_effects::blob::Load as BlobLoad;
use dialog_effects::blob::Read as BlobRead;
use dialog_effects::blob::Size as BlobSize;
use dialog_effects::memory::{Publish, Resolve};
use dialog_search_tree::nodes;

impl Branch {
    /// A transaction built by `build`, run again over the head that won
    /// whenever another writer moves the head first. See the
    /// [module docs](self).
    ///
    /// `build` is called once per try, with a transaction on the head as
    /// it is then; whatever it read is read again on the next try. It
    /// returns the transaction to commit, or an error that ends the
    /// transaction with nothing written.
    pub fn transact<'a, Build, Built, Error>(&'a self, build: Build) -> Transact<'a, Build>
    where
        Build: FnMut(Transaction<&'a Branch>) -> Built,
        Built: Future<Output = Result<Transaction<&'a Branch>, Error>>,
    {
        Transact {
            branch: self,
            build,
            allow_empty: false,
            canonicalize: false,
        }
    }
}

/// Command running a transaction built by a closure, again over the head
/// that won while another writer wins the race. Created by
/// [`Branch::transact`].
pub struct Transact<'a, Build> {
    branch: &'a Branch,
    build: Build,
    allow_empty: bool,
    canonicalize: bool,
}

impl<'a, Build> Transact<'a, Build> {
    /// Mint a revision even when the settled change batch leaves the
    /// indexes untouched. See [`Commit::allow_empty`](crate::Commit::allow_empty).
    pub fn allow_empty(mut self) -> Self {
        self.allow_empty = true;
        self
    }

    /// Flush write buffers to the leaves before publishing. See
    /// [`Commit::canonicalize`](crate::Commit::canonicalize).
    pub fn canonicalize(mut self) -> Self {
        self.canonicalize = true;
        self
    }

    /// Build, commit and publish the transaction, building it again over
    /// the new head while the publish loses the race, up to the number of
    /// times a commit tries. Returns the published [`Revision`]; fails with
    /// [`VersionMismatch`](PublishError::VersionMismatch) only once those
    /// tries run out, and with the error `build` returns as soon as it
    /// returns one.
    pub async fn perform<Env, Built, Error>(mut self, env: &Env) -> Result<Revision, Error>
    where
        Build: FnMut(Transaction<&'a Branch>) -> Built,
        Built: Future<Output = Result<Transaction<&'a Branch>, Error>>,
        Error: From<CommitError>,
        Env: Provider<BlobSize>
            + Provider<BlobImport>
            + Provider<Get>
            + Provider<BlobRead>
            + Provider<Put>
            + Provider<Import>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<ArchiveLoad>
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + Holds
            + 'static,
    {
        let mut tries = 0;
        loop {
            let transaction = (self.build)(self.branch.transaction()).await?;
            let mut commit = transaction.commit();
            if self.allow_empty {
                commit = commit.allow_empty();
            }
            if self.canonicalize {
                commit = commit.canonicalize();
            }
            match Box::pin(commit.publish().once(env)).await {
                Err(CommitError::Publish(PublishError::VersionMismatch { .. }))
                    if tries < RETRIES =>
                {
                    tries += 1;
                    self.branch
                        .revision
                        .resolve()
                        .perform(env)
                        .await
                        .map_err(CommitError::from)?;
                }
                done => return done.map_err(Error::from),
            }
        }
    }
}
