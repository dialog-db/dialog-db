//! Chain-search cost: legacy certificate stores vs the tree-backed walk.
//!
//! Populates the legacy filesystem and volatile certificate stores and a
//! branch's delegation region with the same N certificates, then measures
//! `prove` on each backend under two shapes:
//!
//! - `direct`: every certificate is a direct grant to the principal for the
//!   subject (the accumulation pathology observed in the field). The legacy
//!   stores read and decode all N before the first direct grant wins; the
//!   tree walk admits its first candidate.
//! - `miss`: every certificate is an indirect grant whose issuer holds
//!   nothing further, so the walk must exhaust all N candidates and fail.
//!   Worst case for every backend.
//!
//! Plus `chain3`: a clean three-hop powerline chain, the intended topology,
//! at a single size.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dialog_capability::Subject;
use dialog_capability::access::{CertificateStore, Prove, TimeRange};
use dialog_credentials::Ed25519Signer;
use dialog_effects::storage::{Directory, Location};
use dialog_network::Network;
use dialog_operator::{Operator, Profile};
use dialog_repository::{Branch, RepositoryExt as _};
use dialog_storage::provider::storage::{Storage, VolatileSpace};
use dialog_storage::provider::{FileSystem, Volatile};
use dialog_storage::resource::Resource as _;
use dialog_ucan::{Parameters, Scope, Ucan, UcanDelegation};
use dialog_ucan_core::command::Command;
use dialog_ucan_core::subject::Subject as UcanSubject;
use dialog_ucan_core::{DelegationBuilder, DelegationChain};
use dialog_varsig::{Did, Principal as _};

const SIZES: &[usize] = &[32, 512, 6144];

async fn delegate(issuer: &Ed25519Signer, audience: &Did, subject: UcanSubject) -> UcanDelegation {
    let delegation = DelegationBuilder::new()
        .issuer(issuer.clone())
        .audience(audience)
        .subject(subject)
        .command(vec!["storage".to_string()])
        .try_build()
        .await
        .unwrap();
    UcanDelegation::new(DelegationChain::new(delegation))
}

fn scope(subject: &Did) -> Scope {
    Scope {
        subject: UcanSubject::Specific(subject.clone()),
        command: Command(vec!["storage".to_string()]),
        parameters: Parameters::default(),
    }
}

async fn open_branch(name: &str) -> (Branch, Operator<VolatileSpace>) {
    let storage = Storage::volatile();
    let profile = Profile::open(name.to_string())
        .perform(&storage)
        .await
        .unwrap();
    let operator = profile
        .derive(b"bench")
        .allow(Subject::any())
        .network(Network::default())
        .build(storage)
        .await
        .unwrap();
    let repo = profile
        .repository(format!("{name}-repo"))
        .open()
        .perform(&operator)
        .await
        .unwrap();
    let branch = repo.branch("main").open().perform(&operator).await.unwrap();
    (branch, operator)
}

/// All three backends holding the same delegations.
struct Backends {
    fs: FileSystem,
    volatile: Volatile,
    branch: Branch,
    operator: Operator<VolatileSpace>,
}

async fn populate(name: &str, chains: Vec<UcanDelegation>) -> Backends {
    let location = Location::new(
        Directory::Temp,
        format!("delegation-prove-{name}-{}", std::process::id()),
    );
    let fs = FileSystem::open(&location).await.unwrap();
    let volatile = Volatile::new();
    let (branch, operator) = open_branch(&format!("delegation-prove-{name}")).await;

    for chain in &chains {
        CertificateStore::<Ucan>::save(&fs, chain).await.unwrap();
        CertificateStore::<Ucan>::save(&volatile, chain)
            .await
            .unwrap();
    }
    branch
        .delegations()
        .retain_all(chains)
        .perform(&operator)
        .await
        .unwrap();

    Backends {
        fs,
        volatile,
        branch,
        operator,
    }
}

fn bench_prove(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    for (shape, expect_ok) in [("direct", true), ("miss", false)] {
        let mut group = c.benchmark_group(format!("prove_{shape}"));
        group.sample_size(10);

        for &n in SIZES {
            let (backends, principal, access) = rt.block_on(async {
                let space = Ed25519Signer::generate().await.unwrap();
                let holder = Ed25519Signer::generate().await.unwrap();
                let mut chains = Vec::with_capacity(n);
                for _ in 0..n {
                    let chain = match shape {
                        // Direct grant: issuer is the subject itself.
                        "direct" => {
                            delegate(&space, &holder.did(), UcanSubject::Specific(space.did()))
                                .await
                        }
                        // Indirect dead end: issued by a stranger who holds
                        // nothing, so the walk must exhaust everything.
                        _ => {
                            let stranger = Ed25519Signer::generate().await.unwrap();
                            delegate(&stranger, &holder.did(), UcanSubject::Specific(space.did()))
                                .await
                        }
                    };
                    chains.push(chain);
                }
                let backends = populate(&format!("{shape}-{n}"), chains).await;
                (backends, holder.did(), scope(&space.did()))
            });

            group.bench_function(BenchmarkId::new("legacy-fs", n), |b| {
                b.to_async(&rt).iter(|| {
                    let mut claim = Prove::<Ucan>::new(principal.clone(), access.clone());
                    claim.duration = TimeRange::unbounded();
                    async {
                        let result = CertificateStore::<Ucan>::prove(&backends.fs, claim).await;
                        assert_eq!(result.is_ok(), expect_ok);
                    }
                })
            });

            group.bench_function(BenchmarkId::new("legacy-volatile", n), |b| {
                b.to_async(&rt).iter(|| {
                    let mut claim = Prove::<Ucan>::new(principal.clone(), access.clone());
                    claim.duration = TimeRange::unbounded();
                    async {
                        let result =
                            CertificateStore::<Ucan>::prove(&backends.volatile, claim).await;
                        assert_eq!(result.is_ok(), expect_ok);
                    }
                })
            });

            group.bench_function(BenchmarkId::new("tree", n), |b| {
                b.to_async(&rt).iter(|| async {
                    let result = backends
                        .branch
                        .delegations()
                        .prove(principal.clone(), access.clone())
                        .perform(&backends.operator)
                        .await;
                    assert_eq!(result.is_ok(), expect_ok);
                })
            });
        }
        group.finish();
    }

    // The intended topology: space -> account -> profile -> operator, all
    // powerlines except the root grant. Depth cost with no accumulation.
    let mut group = c.benchmark_group("prove_chain3");
    group.sample_size(10);
    let (backends, principal, access) = rt.block_on(async {
        let space = Ed25519Signer::generate().await.unwrap();
        let account = Ed25519Signer::generate().await.unwrap();
        let profile = Ed25519Signer::generate().await.unwrap();
        let operator = Ed25519Signer::generate().await.unwrap();
        let chains = vec![
            delegate(&space, &account.did(), UcanSubject::Specific(space.did())).await,
            delegate(&account, &profile.did(), UcanSubject::Any).await,
            delegate(&profile, &operator.did(), UcanSubject::Any).await,
        ];
        let backends = populate("chain3", chains).await;
        (backends, operator.did(), scope(&space.did()))
    });

    group.bench_function("legacy-fs", |b| {
        b.to_async(&rt).iter(|| {
            let mut claim = Prove::<Ucan>::new(principal.clone(), access.clone());
            claim.duration = TimeRange::unbounded();
            async {
                CertificateStore::<Ucan>::prove(&backends.fs, claim)
                    .await
                    .unwrap();
            }
        })
    });
    group.bench_function("legacy-volatile", |b| {
        b.to_async(&rt).iter(|| {
            let mut claim = Prove::<Ucan>::new(principal.clone(), access.clone());
            claim.duration = TimeRange::unbounded();
            async {
                CertificateStore::<Ucan>::prove(&backends.volatile, claim)
                    .await
                    .unwrap();
            }
        })
    });
    group.bench_function("tree", |b| {
        b.to_async(&rt).iter(|| async {
            backends
                .branch
                .delegations()
                .prove(principal.clone(), access.clone())
                .perform(&backends.operator)
                .await
                .unwrap();
        })
    });
    group.finish();
}

criterion_group!(benches, bench_prove);
criterion_main!(benches);
