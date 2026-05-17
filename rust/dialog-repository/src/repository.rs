//! Capability-based repository system.
//!
//! This module provides a repository abstraction built on top of the
//! capability-based effect system (`dialog-capability` / `dialog-effects`).
//!
//! - [`archive`] — CAS adapter bridging capabilities with search tree storage
//! - [`memory`] — Transactional memory cells with edition tracking
//! - [`revision`] — Revision tracking and logical timestamps
use dialog_capability::{Capability, Did, Subject};
use dialog_credentials::{Credential, Ed25519Signer, SignerCredential};
use dialog_effects::space::SpaceSubjectExt;
use dialog_operator::access::Access as ProfileAccess;
use dialog_operator::{Profile, SpaceHandle};
use dialog_varsig::Principal;

mod archive;
pub use archive::*;

mod branch;
pub use branch::*;

mod create;
pub use create::*;

mod error;
pub use error::*;

mod load;
pub use load::*;

mod memory;
pub use memory::*;

mod open;
pub use open::*;

mod remote;
pub use remote::*;

mod revision;
pub use revision::*;

mod tree;
pub use tree::*;

/// A repository scoped to a specific subject.
///
/// The credential type parameter determines access level:
/// - `Repository<SignerCredential>` -- owns the keypair, can delegate
/// - `Repository<Credential>` -- either signer or verifier, determined at runtime
pub struct Repository<C: Principal = Credential> {
    credential: C,
}

impl<C: Principal> Repository<C> {
    fn new(credential: C) -> Self {
        Self { credential }
    }

    /// Get the credential.
    pub fn credential(&self) -> &C {
        &self.credential
    }

    /// The subject DID.
    pub fn did(&self) -> Did {
        self.credential.did()
    }

    /// The subject.
    pub fn subject(&self) -> Subject {
        self.did().into()
    }

    /// Get a branch reference for the given name.
    ///
    /// Call `.open()` or `.load()` on the returned reference.
    pub fn branch(&self, name: impl Into<String>) -> BranchReference {
        self.subject().branch(name)
    }

    /// Get a remote reference for the given name.
    ///
    /// Call `.create(address)` or `.load()` on the returned reference.
    pub fn remote(&self, name: impl Into<String>) -> RemoteReference {
        self.subject().remote(name)
    }
}

impl<C: Principal> Principal for Repository<C> {
    fn did(&self) -> Did {
        self.credential.did()
    }
}

impl<C: Principal> From<&Repository<C>> for Capability<Subject> {
    fn from(r: &Repository<C>) -> Self {
        Subject::from(r.did()).into()
    }
}

impl Repository<SignerCredential> {
    /// Access handle for claiming and delegating capabilities.
    pub fn access(&self) -> ProfileAccess<'_> {
        ProfileAccess::new(&self.credential)
    }
}

impl Repository {
    /// Access handle for claiming and delegating capabilities.
    ///
    /// Returns `None` if the credential is verifier-only.
    pub fn try_access(&self) -> Option<ProfileAccess<'_>> {
        match &self.credential {
            Credential::Signer(s) => Some(ProfileAccess::new(s)),
            Credential::Verifier(_) => None,
        }
    }
}

impl From<Credential> for Repository {
    fn from(credential: Credential) -> Self {
        Self::new(credential)
    }
}

impl From<SignerCredential> for Repository<SignerCredential> {
    fn from(credential: SignerCredential) -> Self {
        Self::new(credential)
    }
}

impl TryFrom<Credential> for Repository<SignerCredential> {
    type Error = SignerRequiredError;

    fn try_from(credential: Credential) -> Result<Self, SignerRequiredError> {
        match credential {
            Credential::Signer(s) => Ok(Self::new(s)),
            Credential::Verifier(_) => Err(SignerRequiredError),
        }
    }
}

impl From<Ed25519Signer> for Repository<SignerCredential> {
    fn from(signer: Ed25519Signer) -> Self {
        SignerCredential::from(signer).into()
    }
}

impl From<Profile> for Repository<SignerCredential> {
    fn from(profile: Profile) -> Self {
        Self::new(profile.signer().clone())
    }
}

impl From<&Profile> for Repository<SignerCredential> {
    fn from(profile: &Profile) -> Self {
        Self::new(profile.signer().clone())
    }
}

/// Extension trait for opening repositories from a [`SpaceHandle`].
///
/// Enables `profile.repository("name").open().perform(&operator)`.
pub trait RepositoryExt {
    /// Open or create a repository, loading existing or creating new.
    fn open(self) -> OpenRepository;

    /// Load an existing repository, failing if not found.
    fn load(self) -> LoadRepository;

    /// Create a new repository, failing if one already exists.
    fn create(self) -> CreateRepository;
}

impl RepositoryExt for SpaceHandle {
    fn open(self) -> OpenRepository {
        OpenRepository(self.profile_did.space(self.name))
    }

    fn load(self) -> LoadRepository {
        LoadRepository(self.profile_did.space(self.name))
    }

    fn create(self) -> CreateRepository {
        CreateRepository(self.profile_did.space(self.name))
    }
}
#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::helpers::{test_operator_with_profile, test_repo, unique_name};
    use anyhow::Result;
    use dialog_artifacts::{Artifact, ArtifactSelector, Instruction, Value};
    use dialog_remote_s3::Address as S3Address;
    use futures_util::StreamExt;
    use futures_util::stream;

    fn test_site_address() -> S3Address {
        S3Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("bucket")
            .build()
            .unwrap()
    }

    #[dialog_common::test]
    async fn open_creates_repository() {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = profile
            .repository(unique_name("open"))
            .open()
            .perform(&operator)
            .await
            .unwrap();

        assert!(!repo.did().to_string().is_empty());
    }

    #[dialog_common::test]
    async fn create_then_load() {
        let (operator, profile) = test_operator_with_profile().await;
        let name = unique_name("create-load");

        let created = profile
            .repository(name.clone())
            .create()
            .perform(&operator)
            .await
            .unwrap();

        let loaded = profile
            .repository(name)
            .load()
            .perform(&operator)
            .await
            .unwrap();
        assert_eq!(created.did(), loaded.did());
    }

    #[dialog_common::test]
    async fn it_opens_branch_via_repository() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let branch = repo.branch("main").open().perform(&operator).await?;

        assert_eq!(branch.name(), "main");
        assert!(
            branch.revision().is_none(),
            "New branch should have no revision"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_loads_branch_via_repository() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        // A branch only materializes once it has a commit — open + no commits
        // leaves no revision for load to find.
        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:1".parse()?,
            is: Value::String("Alice".into()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

        let branch = repo.branch("main").load().perform(&operator).await?;
        assert_eq!(branch.name(), "main");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_commits_via_repository() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let branch = repo.branch("main").open().perform(&operator).await?;
        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:123".parse()?,
            is: Value::String("Alice".to_string()),
            cause: None,
        };
        let _hash = branch
            .commit(stream::iter(vec![Instruction::Assert(artifact)]))
            .perform(&operator)
            .await?;

        assert!(
            branch.revision().is_some(),
            "Branch should have a revision after commit"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_adds_and_loads_remote_via_repository() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let site = repo
            .remote("origin")
            .create(test_site_address())
            .perform(&operator)
            .await?;
        assert_eq!(site.site().name(), "origin");

        let loaded = repo.remote("origin").load().perform(&operator).await?;
        assert_eq!(loaded.site().name(), "origin");
        assert_eq!(loaded.address().site(), &test_site_address().into());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_opens_repository_by_name() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;

        let repo = profile
            .repository(unique_name("home"))
            .open()
            .perform(&operator)
            .await?;
        assert!(
            !repo.subject().to_string().is_empty(),
            "should produce a valid subject DID"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_reopens_same_repository() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let name = unique_name("home");

        let did1 = profile
            .repository(name.clone())
            .open()
            .perform(&operator)
            .await?
            .subject();
        let did2 = profile
            .repository(name)
            .open()
            .perform(&operator)
            .await?
            .subject();

        assert_eq!(did1, did2, "reopening should return same subject DID");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_repositories_by_name() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;

        let repo1 = profile
            .repository(unique_name("home"))
            .open()
            .perform(&operator)
            .await?;
        let repo2 = profile
            .repository(unique_name("work"))
            .open()
            .perform(&operator)
            .await?;

        assert_ne!(
            repo1.subject(),
            repo2.subject(),
            "different names should produce different subjects"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_commits_and_selects_by_attribute() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let branch = repo.branch("main").open().perform(&operator).await?;

        let artifacts = vec![
            Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:1".parse()?,
                is: Value::String("Alice".into()),
                cause: None,
            }),
            Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:1".parse()?,
                is: Value::String("alice@example.com".into()),
                cause: None,
            }),
            Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:2".parse()?,
                is: Value::String("Bob".into()),
                cause: None,
            }),
        ];

        branch
            .commit(stream::iter(artifacts))
            .perform(&operator)
            .await?;

        let results: Vec<_> = branch
            .claims()
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(results.len(), 2, "should find 2 user/name artifacts");
        let names: Vec<_> = results.iter().map(|a| &a.is).collect();
        assert!(
            names.contains(&&Value::String("Alice".into())),
            "should contain Alice"
        );
        assert!(
            names.contains(&&Value::String("Bob".into())),
            "should contain Bob"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_commits_and_selects_by_entity() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let branch = repo.branch("main").open().perform(&operator).await?;

        let artifacts = vec![
            Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:alice".parse()?,
                is: Value::String("Alice".into()),
                cause: None,
            }),
            Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:bob".parse()?,
                is: Value::String("Bob".into()),
                cause: None,
            }),
            Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:alice".parse()?,
                is: Value::String("alice@example.com".into()),
                cause: None,
            }),
        ];

        branch
            .commit(stream::iter(artifacts))
            .perform(&operator)
            .await?;

        let results: Vec<_> = branch
            .claims()
            .select(ArtifactSelector::new().of("user:alice".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(results.len(), 2, "should find 2 artifacts for user:alice");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_selects_empty_branch() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let branch = repo.branch("main").open().perform(&operator).await?;

        let results: Vec<_> = branch
            .claims()
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(results.len(), 0, "empty branch should have no artifacts");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_retracts_artifact() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let branch = repo.branch("main").open().perform(&operator).await?;

        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:1".parse()?,
            is: Value::String("Alice".into()),
            cause: None,
        };

        branch
            .commit(stream::iter(vec![Instruction::Assert(artifact.clone())]))
            .perform(&operator)
            .await?;

        // Verify it's there
        let before: Vec<_> = branch
            .claims()
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(before.len(), 1, "should have 1 artifact before retract");

        // Retract it
        branch
            .commit(stream::iter(vec![Instruction::Retract(artifact)]))
            .perform(&operator)
            .await?;

        let after: Vec<_> = branch
            .claims()
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(after.len(), 0, "should have 0 artifacts after retract");

        Ok(())
    }

    mod delegation_tests {
        use super::*;
        use crate::helpers::{test_operator_with_profile, unique_name};
        use dialog_effects::memory as fx_memory;

        #[dialog_common::test]
        async fn it_delegates_repo_to_profile_and_claims() -> Result<()> {
            let (operator, profile) = test_operator_with_profile().await;
            let repo = profile
                .repository(unique_name("home"))
                .create()
                .perform(&operator)
                .await?;

            // Repo delegates full ownership to the profile
            let chain = repo
                .access()
                .claim(&repo)
                .delegate(profile.did())
                .perform(&operator)
                .await?;
            profile.access().save(chain).perform(&operator).await?;

            // Profile should be able to claim access to any memory space
            let capability = repo
                .subject()
                .attenuate(fx_memory::Memory)
                .attenuate(fx_memory::Space::new("data"));

            let result = profile.access().claim(capability).perform(&operator).await;
            assert!(
                result.is_ok(),
                "should find delegation chain: {:?}",
                result.err()
            );

            Ok(())
        }

        #[dialog_common::test]
        async fn it_enforces_scoped_delegation_policy() -> Result<()> {
            let (operator, profile) = test_operator_with_profile().await;
            let repo = profile
                .repository(unique_name("home"))
                .create()
                .perform(&operator)
                .await?;

            // Repo delegates only memory/space("data") to the profile
            let scoped_cap = repo
                .subject()
                .attenuate(fx_memory::Memory)
                .attenuate(fx_memory::Space::new("data"));
            let chain = repo
                .access()
                .claim(scoped_cap)
                .delegate(profile.did())
                .perform(&operator)
                .await?;
            profile.access().save(chain).perform(&operator).await?;

            // Claiming "data" space should succeed
            let data_cap = repo
                .subject()
                .attenuate(fx_memory::Memory)
                .attenuate(fx_memory::Space::new("data"));
            let result = profile.access().claim(data_cap).perform(&operator).await;
            assert!(
                result.is_ok(),
                "claim on delegated space 'data' should succeed: {:?}",
                result.err()
            );

            // Claiming "secret" space should fail
            let secret_cap = repo
                .subject()
                .attenuate(fx_memory::Memory)
                .attenuate(fx_memory::Space::new("secret"));
            let result = profile.access().claim(secret_cap).perform(&operator).await;
            assert!(
                result.is_err(),
                "claim on non-delegated space 'secret' should be denied"
            );

            Ok(())
        }

        #[dialog_common::test]
        async fn it_validates_delegation_against_policy() -> Result<()> {
            let (operator, profile) = test_operator_with_profile().await;
            let repo = profile
                .repository(unique_name("home"))
                .create()
                .perform(&operator)
                .await?;

            // Repo delegates memory/space("data") to the profile
            let scoped_cap = repo
                .subject()
                .attenuate(fx_memory::Memory)
                .attenuate(fx_memory::Space::new("data"));
            let chain = repo
                .access()
                .claim(scoped_cap)
                .delegate(profile.did())
                .perform(&operator)
                .await?;
            profile.access().save(chain).perform(&operator).await?;

            // Profile can re-delegate "data" space to operator
            let data_cap = repo
                .subject()
                .attenuate(fx_memory::Memory)
                .attenuate(fx_memory::Space::new("data"));
            let result = profile
                .access()
                .claim(data_cap)
                .delegate(operator.did())
                .perform(&operator)
                .await;
            assert!(
                result.is_ok(),
                "delegation for space 'data' should succeed: {:?}",
                result.err()
            );

            // Profile cannot delegate "secret" space (no chain)
            let secret_cap = repo
                .subject()
                .attenuate(fx_memory::Memory)
                .attenuate(fx_memory::Space::new("secret"));
            let result = profile
                .access()
                .claim(secret_cap)
                .delegate(operator.did())
                .perform(&operator)
                .await;
            assert!(
                result.is_err(),
                "delegation for non-delegated space 'secret' should fail"
            );

            Ok(())
        }
    }

    mod query_engine {
        use crate::helpers::{test_operator_with_profile, test_repo};
        use dialog_query::query::Output;
        use dialog_query::{Concept, Entity, Query, Term};

        pub mod employee {
            #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            pub struct Name(pub String);

            #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            pub struct Role(pub String);
        }

        #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub struct Employee {
            pub this: Entity,
            pub name: employee::Name,
            pub role: employee::Role,
        }

        #[dialog_common::test]
        async fn it_queries_via_session() -> anyhow::Result<()> {
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            branch
                .transaction()
                .assert(Employee {
                    this: Entity::new()?,
                    name: employee::Name("Alice".into()),
                    role: employee::Role("Engineer".into()),
                })
                .assert(Employee {
                    this: Entity::new()?,
                    name: employee::Name("Bob".into()),
                    role: employee::Role("Designer".into()),
                })
                .commit()
                .perform(&operator)
                .await?;

            let results: Vec<Employee> = branch
                .query()
                .select(Query::<Employee> {
                    this: Term::var("this"),
                    name: Term::var("name"),
                    role: Term::var("role"),
                })
                .perform(&operator)
                .try_vec()
                .await?;

            assert_eq!(results.len(), 2);
            Ok(())
        }

        #[dialog_common::test]
        async fn it_resolves_only_latest_name_target_via_name_concept() -> anyhow::Result<()> {
            /// The `dialog.meta/named-entity` attribute — the entity a
            /// name currently points at. Cardinality `one` (the derive
            /// default), so re-pointing a name supersedes the prior
            /// claim instead of accumulating.
            #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            #[domain("dialog.meta")]
            pub struct NamedEntity(pub Entity);

            /// A user-published name — an `id:<n>` entity carrying a
            /// single `entity` claim that points at the target the name
            /// currently identifies.
            #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
            pub struct Name {
                /// The name entity — `id:<n>` for user-published names,
                /// `db:<n>` for built-ins.
                pub this: Entity,
                /// The target this name currently identifies.
                pub entity: NamedEntity,
            }

            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            // Use the `concept:` scheme for targets — this matches
            // the real-world case where `concept!: &page` derives a
            // content-hashed `concept:…` entity URI for each body.
            // Same supersession path, different value scheme; this
            // catches a bug where the cardinality-one filter was
            // sensitive to the value's URI scheme.
            let id_page: Entity = "id:page".parse()?;
            let page_v1: Entity =
                "concept:Fx8sv1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".parse()?;
            let page_v2: Entity =
                "concept:AfmLeBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB".parse()?;

            // tx1 — point id:page at v1.
            let v1 = branch
                .transaction()
                .assert(Name {
                    this: id_page.clone(),
                    entity: NamedEntity(page_v1.clone()),
                })
                .commit()
                .perform(&operator)
                .await?;

            assert_eq!(
                branch
                    .query()
                    .select(Query::<Name> {
                        this: id_page.clone().into(),
                        entity: Term::var("entity"),
                    })
                    .perform(&operator)
                    .try_vec()
                    .await?,
                vec![Name {
                    this: id_page.clone(),
                    entity: NamedEntity(page_v1.clone())
                }]
            );

            // tx2 — point id:page at v2. Cardinality-one supersedes v1.
            let v2 = branch
                .transaction()
                .assert(Name {
                    this: id_page.clone(),
                    entity: NamedEntity(page_v2.clone()),
                })
                .commit()
                .perform(&operator)
                .await?;

            assert_ne!(v1, v2);

            assert_eq!(
                branch
                    .query()
                    .select(Query::<Name> {
                        this: id_page.clone().into(),
                        entity: Term::var("entity"),
                    })
                    .perform(&operator)
                    .try_vec()
                    .await?,
                vec![Name {
                    this: id_page.clone(),
                    entity: NamedEntity(page_v2.clone())
                }]
            );

            Ok(())
        }

        #[dialog_common::test]
        async fn it_accumulates_two_cardinality_many_values_in_one_tx() -> anyhow::Result<()> {
            // Regression guard: asserting two distinct values for the same
            // (entity, attribute) inside a single transaction must keep
            // both — cardinality-many accumulates, it does not collapse.
            #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            #[cardinality(many)]
            #[domain("dialog.meta")]
            pub struct Tag(pub String);

            #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
            pub struct Tagged {
                pub this: Entity,
                pub tag: Tag,
            }

            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let post: Entity = "id:post".parse()?;

            branch
                .transaction()
                .assert(Tagged {
                    this: post.clone(),
                    tag: Tag("red".into()),
                })
                .assert(Tagged {
                    this: post.clone(),
                    tag: Tag("blue".into()),
                })
                .commit()
                .perform(&operator)
                .await?;

            let mut results: Vec<Tagged> = branch
                .query()
                .select(Query::<Tagged> {
                    this: post.clone().into(),
                    tag: Term::var("tag"),
                })
                .perform(&operator)
                .try_vec()
                .await?;
            results.sort();

            assert_eq!(
                results,
                vec![
                    Tagged {
                        this: post.clone(),
                        tag: Tag("blue".into()),
                    },
                    Tagged {
                        this: post.clone(),
                        tag: Tag("red".into()),
                    },
                ]
            );

            Ok(())
        }

        #[dialog_common::test]
        async fn it_is_noop_when_reasserting_same_cardinality_one_value() -> anyhow::Result<()> {
            // Reasserting the same value for a cardinality-one attribute
            // must be a no-op at the storage layer: the tree must not
            // change, so the revision's tree hash stays the same.
            #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            #[domain("dialog.meta")]
            pub struct NamedEntity(pub Entity);

            #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
            pub struct Name {
                pub this: Entity,
                pub entity: NamedEntity,
            }

            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let id_page: Entity = "id:page".parse()?;
            let page_v1: Entity =
                "concept:Fx8sv1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".parse()?;

            let r1 = branch
                .transaction()
                .assert(Name {
                    this: id_page.clone(),
                    entity: NamedEntity(page_v1.clone()),
                })
                .commit()
                .perform(&operator)
                .await?;

            let r2 = branch
                .transaction()
                .assert(Name {
                    this: id_page.clone(),
                    entity: NamedEntity(page_v1.clone()),
                })
                .commit()
                .perform(&operator)
                .await?;

            assert_eq!(
                r1.tree, r2.tree,
                "reasserting the same value must not change the tree"
            );

            // And the query must still yield exactly the one claim.
            assert_eq!(
                branch
                    .query()
                    .select(Query::<Name> {
                        this: id_page.clone().into(),
                        entity: Term::var("entity"),
                    })
                    .perform(&operator)
                    .try_vec()
                    .await?,
                vec![Name {
                    this: id_page.clone(),
                    entity: NamedEntity(page_v1.clone()),
                }]
            );

            Ok(())
        }
    }

    mod layer {
        use super::query_engine::{Employee, employee};
        use crate::helpers::{test_operator_with_profile, test_repo};
        use dialog_query::layer::Layer;
        use dialog_query::query::Output;
        use dialog_query::{Concept, Entity, Query, Term, the};

        mod branch_meta {
            #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            #[domain("dialog.meta")]
            pub struct Name(pub String);

            #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            #[domain("dialog.meta")]
            pub struct RevisionHash(pub String);
        }

        #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub struct BranchMeta {
            this: Entity,
            name: branch_meta::Name,
        }

        #[dialog_common::test]
        async fn layer_exposes_asserted_facts() -> anyhow::Result<()> {
            // Build a Layer end-to-end, then attach via .with(...).
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let synthetic: Entity = "id:branch".parse()?;
            let layer = Layer::new().assert(BranchMeta {
                this: synthetic.clone(),
                name: branch_meta::Name("main".into()),
            });

            let results: Vec<BranchMeta> = branch
                .query()
                .with(layer)?
                .select(Query::<BranchMeta> {
                    this: synthetic.clone().into(),
                    name: Term::var("name"),
                })
                .perform(&operator)
                .try_vec()
                .await?;

            assert_eq!(results.len(), 1);
            assert_eq!(results[0].this, synthetic);
            assert_eq!(results[0].name.0, "main");
            Ok(())
        }

        #[dialog_common::test]
        async fn retract_on_layer_removes_fact() -> anyhow::Result<()> {
            // assert + retract on the layer net to no fact.
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let synthetic: Entity = "id:branch".parse()?;
            let asserted = BranchMeta {
                this: synthetic.clone(),
                name: branch_meta::Name("main".into()),
            };
            let layer = Layer::new().assert(asserted.clone()).retract(asserted);

            let results: Vec<BranchMeta> = branch
                .query()
                .with(layer)?
                .select(Query::<BranchMeta> {
                    this: synthetic.into(),
                    name: Term::var("name"),
                })
                .perform(&operator)
                .try_vec()
                .await?;

            assert!(results.is_empty());
            Ok(())
        }

        #[dialog_common::test]
        async fn branch_metadata_layer_exposes_branch_internals() -> anyhow::Result<()> {
            // The built-in branch metadata layer should make the branch
            // name and revision queryable as ordinary facts.
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            // Commit something so the branch has a revision to expose.
            branch
                .transaction()
                .assert(the!("user/name").of(Entity::new()?).is("Alice".to_string()))
                .commit()
                .perform(&operator)
                .await?;
            // Reload the branch so its revision cell reflects the commit.
            let branch = repo.branch("main").load().perform(&operator).await?;

            let synthetic: Entity = "id:branch".parse()?;
            let names: Vec<BranchMeta> = branch
                .query()
                .with(branch.metadata())?
                .select(Query::<BranchMeta> {
                    this: synthetic.clone().into(),
                    name: Term::var("name"),
                })
                .perform(&operator)
                .try_vec()
                .await?;

            assert_eq!(names.len(), 1);
            assert_eq!(names[0].name.0, "main");

            // The revision-hash fact should be present.
            let revision: Vec<branch_meta::RevisionHash> = branch
                .query()
                .with(branch.metadata())?
                .select(Query::<RevisionConcept> {
                    this: synthetic.clone().into(),
                    revision_hash: Term::var("hash"),
                })
                .perform(&operator)
                .try_vec()
                .await?
                .into_iter()
                .map(|c| c.revision_hash)
                .collect();

            assert_eq!(revision.len(), 1);
            assert!(!revision[0].0.is_empty(), "tree hash should be non-empty");
            Ok(())
        }

        #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub struct RevisionConcept {
            pub this: Entity,
            pub revision_hash: branch_meta::RevisionHash,
        }

        #[dialog_common::test]
        async fn layer_unions_two_branches() -> anyhow::Result<()> {
            // Layering a second branch onto a query session should union
            // both branches' facts during select.
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let main = repo.branch("main").open().perform(&operator).await?;
            let feature = repo.branch("feature").open().perform(&operator).await?;

            main.transaction()
                .assert(Employee {
                    this: Entity::new()?,
                    name: employee::Name("Alice".into()),
                    role: employee::Role("Engineer".into()),
                })
                .commit()
                .perform(&operator)
                .await?;

            feature
                .transaction()
                .assert(Employee {
                    this: Entity::new()?,
                    name: employee::Name("Bob".into()),
                    role: employee::Role("Designer".into()),
                })
                .commit()
                .perform(&operator)
                .await?;

            // Reload so the branch handles know about their revisions.
            let main = repo.branch("main").load().perform(&operator).await?;
            let feature = repo.branch("feature").load().perform(&operator).await?;

            let mut names: Vec<String> = feature
                .query()
                .with(&main)?
                .select(Query::<Employee> {
                    this: Term::var("this"),
                    name: Term::var("name"),
                    role: Term::var("role"),
                })
                .perform(&operator)
                .try_vec()
                .await?
                .into_iter()
                .map(|e| e.name.0)
                .collect();
            names.sort();
            assert_eq!(names, vec!["Alice".to_string(), "Bob".to_string()]);
            Ok(())
        }

        #[dialog_common::test]
        async fn layer_chains_branch_and_memory() -> anyhow::Result<()> {
            // Same session can layer a branch *and* an in-memory Layer.
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let main = repo.branch("main").open().perform(&operator).await?;
            let scratch = repo.branch("scratch").open().perform(&operator).await?;

            main.transaction()
                .assert(Employee {
                    this: Entity::new()?,
                    name: employee::Name("Alice".into()),
                    role: employee::Role("Engineer".into()),
                })
                .commit()
                .perform(&operator)
                .await?;
            let main = repo.branch("main").load().perform(&operator).await?;
            // `scratch` has no commits yet — `open()` is enough, the branch
            // simply selects against the empty tree.
            let synthetic_layer = Layer::new().assert(Employee {
                this: Entity::new()?,
                name: employee::Name("Synthetic".into()),
                role: employee::Role("Bot".into()),
            });
            let mut names: Vec<String> = scratch
                .query()
                .with(&main)?
                .with(synthetic_layer)?
                .select(Query::<Employee> {
                    this: Term::var("this"),
                    name: Term::var("name"),
                    role: Term::var("role"),
                })
                .perform(&operator)
                .try_vec()
                .await?
                .into_iter()
                .map(|e| e.name.0)
                .collect();
            names.sort();
            assert_eq!(
                names,
                vec!["Alice".to_string(), "Synthetic".to_string()]
            );
            Ok(())
        }

        #[dialog_common::test]
        async fn layer_facts_union_with_stored_facts() -> anyhow::Result<()> {
            // Asserting a layered fact under the same attribute as a stored
            // fact should yield both rows when queried.
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let alice = Entity::new()?;
            branch
                .transaction()
                .assert(
                    the!("dialog.meta/name")
                        .of(alice.clone())
                        .is("Alice".to_string()),
                )
                .commit()
                .perform(&operator)
                .await?;

            let synthetic: Entity = "id:branch".parse()?;
            let layer = Layer::new().assert(BranchMeta {
                this: synthetic,
                name: branch_meta::Name("main".into()),
            });
            let names: Vec<BranchMeta> = branch
                .query()
                .with(layer)?
                .select(Query::<BranchMeta> {
                    this: Term::var("this"),
                    name: Term::var("name"),
                })
                .perform(&operator)
                .try_vec()
                .await?;

            assert_eq!(names.len(), 2);
            let mut values: Vec<String> = names.into_iter().map(|m| m.name.0).collect();
            values.sort();
            assert_eq!(values, vec!["Alice".to_string(), "main".to_string()]);
            Ok(())
        }

        // -- Branch::metadata coverage -------------------------------------

        mod repo_meta {
            #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            #[domain("dialog.meta")]
            pub struct Did(pub String);
        }

        #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub struct RepoConcept {
            pub this: Entity,
            pub did: repo_meta::Did,
        }

        #[dialog_common::test]
        async fn branch_metadata_exposes_repository_did() -> anyhow::Result<()> {
            // dialog.meta/did on the synthetic `id:repository` entity should
            // match the branch's host DID.
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;
            let expected_did = branch.of().to_string();

            let repo_entity: Entity = "id:repository".parse()?;
            let results: Vec<RepoConcept> = branch
                .query()
                .with(branch.metadata())?
                .select(Query::<RepoConcept> {
                    this: repo_entity.into(),
                    did: Term::var("did"),
                })
                .perform(&operator)
                .try_vec()
                .await?;

            assert_eq!(results.len(), 1, "repository DID fact must be present");
            assert_eq!(results[0].did.0, expected_did);
            Ok(())
        }

        mod upstream_meta {
            #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            #[domain("dialog.meta")]
            pub struct Kind(pub String);

            #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            #[domain("dialog.meta")]
            pub struct Branch(pub String);
        }

        #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub struct UpstreamConcept {
            pub this: Entity,
            pub kind: upstream_meta::Kind,
            pub branch: upstream_meta::Branch,
        }

        #[dialog_common::test]
        async fn branch_metadata_exposes_upstream_when_configured() -> anyhow::Result<()> {
            // After set_upstream, the metadata layer should report kind=local
            // and the upstream branch name on the `id:upstream` entity.
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let main = repo.branch("main").open().perform(&operator).await?;
            let feature = repo.branch("feature").open().perform(&operator).await?;
            feature.set_upstream(&main).perform(&operator).await?;

            let upstream_entity: Entity = "id:upstream".parse()?;
            let results: Vec<UpstreamConcept> = feature
                .query()
                .with(feature.metadata())?
                .select(Query::<UpstreamConcept> {
                    this: upstream_entity.into(),
                    kind: Term::var("kind"),
                    branch: Term::var("branch"),
                })
                .perform(&operator)
                .try_vec()
                .await?;

            assert_eq!(results.len(), 1);
            assert_eq!(results[0].kind.0, "local");
            assert_eq!(results[0].branch.0, "main");
            Ok(())
        }

        #[dialog_common::test]
        async fn branch_metadata_omits_upstream_facts_when_none() -> anyhow::Result<()> {
            // Without an upstream configured, no UpstreamConcept should match.
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let upstream_entity: Entity = "id:upstream".parse()?;
            let results: Vec<UpstreamConcept> = branch
                .query()
                .with(branch.metadata())?
                .select(Query::<UpstreamConcept> {
                    this: upstream_entity.into(),
                    kind: Term::var("kind"),
                    branch: Term::var("branch"),
                })
                .perform(&operator)
                .try_vec()
                .await?;

            assert!(results.is_empty());
            Ok(())
        }

        // -- QuerySession accessors ----------------------------------------

        #[dialog_common::test]
        async fn query_session_accessors_reflect_layers() -> anyhow::Result<()> {
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let main = repo.branch("main").open().perform(&operator).await?;
            let feature = repo.branch("feature").open().perform(&operator).await?;

            let synthetic = Layer::new().assert(Employee {
                this: Entity::new()?,
                name: employee::Name("Synth".into()),
                role: employee::Role("Bot".into()),
            });

            let session = feature.query().with(&main)?.with(synthetic)?;
            assert_eq!(session.layered_branches().len(), 1);
            assert_eq!(session.layered_branches()[0].name(), "main");
            // Each Employee concept asserts two attribute triples (name, role).
            assert_eq!(session.layer().facts().facts().len(), 2);
            Ok(())
        }

        #[dialog_common::test]
        async fn empty_session_has_no_layers() -> anyhow::Result<()> {
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let session = branch.query();
            assert!(session.layered_branches().is_empty());
            assert!(session.layer().facts().facts().is_empty());
            Ok(())
        }

        // -- Layer::install end-to-end via a session ---------------------

        mod stuff_role {
            #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            #[domain("stuff")]
            pub struct Name(pub String);

            #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            #[domain("stuff")]
            pub struct Role(pub String);
        }

        #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub struct Stuff {
            pub this: Entity,
            pub name: stuff_role::Name,
            pub role: stuff_role::Role,
        }

        mod cardinality_one_attr {
            #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            #[domain("person")]
            pub struct Nickname(pub String);
        }

        #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub struct WithNickname {
            pub this: Entity,
            pub nickname: cardinality_one_attr::Nickname,
        }

        #[dialog_common::test]
        async fn cross_branch_layer_preserves_cardinality_one_winner() -> anyhow::Result<()> {
            // Regression for the streaming merge: two branches both holding
            // facts for the same (attribute, entity) pair under a
            // cardinality-one attribute must still yield exactly one winner
            // per entity when queried through a composed session. The
            // sliding-window in `only.rs` assumes consecutive `(the, of)`
            // grouping, so the composite env must merge — not chain —
            // branch streams.
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let main = repo.branch("main").open().perform(&operator).await?;
            let feature = repo.branch("feature").open().perform(&operator).await?;

            let alice: Entity = "id:alice".parse()?;
            let bob: Entity = "id:bob".parse()?;

            main.transaction()
                .assert(WithNickname {
                    this: alice.clone(),
                    nickname: cardinality_one_attr::Nickname("Ali".into()),
                })
                .assert(WithNickname {
                    this: bob.clone(),
                    nickname: cardinality_one_attr::Nickname("Bobby".into()),
                })
                .commit()
                .perform(&operator)
                .await?;

            feature
                .transaction()
                .assert(WithNickname {
                    this: alice.clone(),
                    nickname: cardinality_one_attr::Nickname("Ally".into()),
                })
                .assert(WithNickname {
                    this: bob.clone(),
                    nickname: cardinality_one_attr::Nickname("Rob".into()),
                })
                .commit()
                .perform(&operator)
                .await?;

            let main = repo.branch("main").load().perform(&operator).await?;
            let feature = repo.branch("feature").load().perform(&operator).await?;

            let results: Vec<WithNickname> = feature
                .query()
                .with(&main)?
                .select(Query::<WithNickname> {
                    this: Term::var("this"),
                    nickname: Term::var("nickname"),
                })
                .perform(&operator)
                .try_vec()
                .await?;

            assert_eq!(
                results.len(),
                2,
                "cardinality-one across composed branches must yield one row per entity"
            );
            let mut entities: Vec<Entity> = results.into_iter().map(|w| w.this).collect();
            entities.sort();
            let mut expected = vec![alice, bob];
            expected.sort();
            assert_eq!(entities, expected);
            Ok(())
        }

        #[dialog_common::test]
        async fn install_rule_on_layer_derives_facts_in_query() -> anyhow::Result<()> {
            // End-to-end: rule installed on an Layer layer derives Employee
            // facts from Stuff facts stored on the branch.
            use dialog_query::rule::When;

            fn employee_from_stuff(employee: Query<Employee>) -> impl When {
                // Term<String> erases the Attribute wrapper, so the Stuff
                // query reuses the same terms directly.
                (Query::<Stuff> {
                    this: employee.this,
                    name: employee.name,
                    role: employee.role,
                },)
            }

            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            branch
                .transaction()
                .assert(Stuff {
                    this: Entity::new()?,
                    name: stuff_role::Name("Alice".into()),
                    role: stuff_role::Role("PM".into()),
                })
                .commit()
                .perform(&operator)
                .await?;

            let layer = Layer::new().install(employee_from_stuff)?;
            let derived: Vec<Employee> = branch
                .query()
                .with(layer)?
                .select(Query::<Employee> {
                    this: Term::var("this"),
                    name: Term::var("name"),
                    role: Term::var("role"),
                })
                .perform(&operator)
                .try_vec()
                .await?;

            assert_eq!(derived.len(), 1, "rule should derive one employee");
            assert_eq!(derived[0].name.0, "Alice");
            assert_eq!(derived[0].role.0, "PM");
            Ok(())
        }
    }

    mod profile_as_repository {
        use super::*;
        use crate::helpers::test_operator_with_profile;
        use dialog_query::{Entity, the};

        #[dialog_common::test]
        async fn repository_from_profile_shares_did() {
            let (_operator, profile) = test_operator_with_profile().await;
            let repo = Repository::from(&profile);
            assert_eq!(
                repo.did(),
                profile.did(),
                "Repository::from(&profile) must inherit the profile DID"
            );
        }

        #[dialog_common::test]
        async fn repository_from_profile_commits_through_profile_mount() -> Result<()> {
            let (operator, profile) = test_operator_with_profile().await;
            let repo = Repository::from(&profile);

            let branch = repo.branch("main").open().perform(&operator).await?;
            let alice = Entity::new()?;
            branch
                .transaction()
                .assert(the!("user/name").of(alice).is("Alice".to_string()))
                .commit()
                .perform(&operator)
                .await?;

            assert!(
                branch.revision().is_some(),
                "commit on profile-DID repo must succeed through the existing profile mount"
            );
            Ok(())
        }

        #[dialog_common::test]
        async fn reopening_profile_as_repository_sees_prior_commits() -> Result<()> {
            let (operator, profile) = test_operator_with_profile().await;

            let writer = Repository::from(&profile);
            let w_branch = writer.branch("trunk").open().perform(&operator).await?;
            let alice = Entity::new()?;
            w_branch
                .transaction()
                .assert(the!("user/name").of(alice).is("Alice".to_string()))
                .commit()
                .perform(&operator)
                .await?;

            let reader = Repository::from(&profile);
            let r_branch = reader.branch("trunk").load().perform(&operator).await?;

            let results: Vec<_> = r_branch
                .claims()
                .select(ArtifactSelector::new().the("user/name".parse()?))
                .perform(&operator)
                .await?
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;

            assert_eq!(
                results.len(),
                1,
                "a second Repository::from(&profile) must read what the first wrote"
            );
            Ok(())
        }

        #[dialog_common::test]
        async fn profile_repo_and_named_repo_are_distinct_spaces() -> Result<()> {
            let (operator, profile) = test_operator_with_profile().await;

            let profile_repo = Repository::from(&profile);
            let named_repo = profile
                .repository(unique_name("named"))
                .open()
                .perform(&operator)
                .await?;

            assert_ne!(
                profile_repo.did(),
                named_repo.did(),
                "named repo must have its own DID, not the profile DID"
            );

            let profile_branch = profile_repo
                .branch("main")
                .open()
                .perform(&operator)
                .await?;
            let item = Entity::new()?;
            profile_branch
                .transaction()
                .assert(the!("item/tag").of(item).is("in-profile".to_string()))
                .commit()
                .perform(&operator)
                .await?;

            let named_branch = named_repo.branch("main").open().perform(&operator).await?;
            let results: Vec<_> = named_branch
                .claims()
                .select(ArtifactSelector::new().the("item/tag".parse()?))
                .perform(&operator)
                .await?
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;

            assert_eq!(
                results.len(),
                0,
                "named repo must not see artifacts written through the profile-DID repo"
            );
            Ok(())
        }
    }
}
