//! The join scenario: a fresh client replicates a seeded space through a
//! simulated network, phase by phase. The site it joins, and the measured
//! phase, are [`crate::scenario`]'s; the other scenarios share them.
//!
//! The phases mirror the shape of a real application join (tonk's space
//! join runs this exact sequence of reads against a freshly adopted head):
//!
//! 1. **pull** — adopt the upstream head by reference (the cheap part).
//! 2. **probe** — the point reads a join validates before accepting
//!    (space name / metadata).
//! 3. **roster** — membership reads (who is in the space, what roles),
//!    run as the sequential selects a join performs.
//! 4. **claim** — commit the joiner's own membership facts and push them.
//! 5. **render** — the first content query a landing page runs.
//! 6. **entity** — a point read of one entity's detail (opening an item).
//! 7. **requery** — the render query again, warm (should be free).
//! 8. **concept** — a fresh cold client runs the landing page as the query
//!    engine runs it: a five-attribute concept join on the shared entity.
//!    This is the sequential probe chain issue #492 is about, and the
//!    yardstick for parallelized query-driven replication.
//! 9. **filtered** — another fresh client runs the same concept with the
//!    status pinned: the selective shape where a value-bound scan turns the
//!    other premises into entity probes (the block-count-versus-rounds
//!    tradeoff recorded in `notes/set-at-a-time-joins.md`).
//! 10. **subscribe** — a fresh cold client registers the same concept as a
//!     standing query and pays its first poll: the exact path a UI drives,
//!     preloading through the same ambient queue every evaluation shares
//!     (bead dialog-db-82).
//! 11. **overlap** — a fresh cold client runs two independent concepts
//!     concurrently (the full board and a compact listing sharing three of
//!     its five attribute ranges): the everyday two-views-of-one-space
//!     shape, gating that overlapping evaluations hydrate shared ranges
//!     once through the env-owned flight and queue.
//! 12. **rule** — a fresh cold client queries a concept that exists only
//!     through a seeded deductive rule (no conclusion facts are ever
//!     written): rule discovery, body hydration, and the body's join all
//!     run cold. The yardstick for binding-aware rule-join speculation
//!     (bead dialog-db-80).
//! 13. **download** — a second fresh client materializes the entire space
//!     (`pull().download()`): the eager-replication cost the lazy join
//!     avoids up front but pays incrementally.

use anyhow::{Context as _, Result};
use dialog_artifacts::{Artifact, ArtifactSelector, Instruction, Value};
use dialog_credentials::{Credential, Ed25519Signer, SignerCredential};
use dialog_effects::credential::prelude::*;
use dialog_effects::storage::Location;
use dialog_peer::Peer;
use dialog_peer::helpers::unique_name;
use dialog_query::rule::DeductiveRuleDescriptor;
use dialog_query::{
    Concept, ConceptConclusion, ConceptDescriptor, ConceptQuery, DeductiveRule, Entity,
    Output as _, Parameters, Query, Term,
};
use dialog_remote_fs::FsAddress;
use dialog_repository::{Branch, Repository, RepositoryExt as _, SiteAddress, contact};
use dialog_storage::provider::FileSystem;
use dialog_storage::provider::storage::VolatileSpace;
use dialog_storage::resource::Resource as _;
use dialog_varsig::{Did, Principal};
use futures_util::StreamExt as _;

use crate::report::PhaseReport;
use crate::scenario::{Scenario, Site, measured};

/// Facts asserted per seeded entity.
pub const FACTS_PER_ENTITY: usize = 6;

/// Attribute markers for the [`Card`] concept, matching the facts
/// [`entity_facts`] seeds (`bug/title`, `bug/status`, ...). The long
/// `bug/detail` body is deliberately not a concept field, the same way a
/// board render reads the card fields and not the description.
mod card {
    use dialog_query::Attribute;

    /// The `bug/title` attribute.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("bug")]
    pub struct Title(pub String);

    /// The `bug/status` attribute.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("bug")]
    pub struct Status(pub String);

    /// The `bug/rank` attribute.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("bug")]
    pub struct Rank(pub String);

    /// The `bug/reporter` attribute.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("bug")]
    pub struct Reporter(pub String);

    /// The `bug/created` attribute.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("bug")]
    pub struct Created(pub String);
}

/// The landing-page record as the query engine sees it: a five-attribute
/// concept join on the shared entity. Querying it with every field free is
/// the balanced join shape; pinning `status` is the selective shape.
#[derive(Clone, Debug, PartialEq, Concept)]
pub struct Card {
    /// The bug entity the card renders.
    pub this: Entity,
    /// Its title.
    pub title: card::Title,
    /// Its status.
    pub status: card::Status,
    /// Its ordering key.
    pub rank: card::Rank,
    /// Who reported it.
    pub reporter: card::Reporter,
    /// When it was filed.
    pub created: card::Created,
}

/// A compact list view beside the full board: the same entities through
/// a second, independent concept that shares three of [`Card`]'s five
/// attribute ranges (title, status, rank). Two apps rendering different
/// views of one space is the everyday shape of concurrent overlapping
/// queries.
#[derive(Clone, Debug, PartialEq, Concept)]
pub struct Listing {
    /// The bug entity the row lists.
    pub this: Entity,
    /// Its title.
    pub title: card::Title,
    /// Its status.
    pub status: card::Status,
    /// Its ordering key.
    pub rank: card::Rank,
}

/// Build one entity's facts: sizes chosen to look like an issue-tracker
/// row (short fields plus one few-hundred-byte body), the shape tonk
/// spaces carry.
pub(crate) fn entity_facts(index: usize) -> Result<Vec<Instruction>> {
    let of: dialog_artifacts::Entity = format!("bug:{index}").parse()?;
    let title = format!(
        "Bug #{index}: the {} widget misbehaves on load",
        index * 7 % 100
    );
    let detail = format!(
        "Reproduction notes for issue {index}: {}",
        "steps and observations, ".repeat(12)
    );
    let facts = vec![
        ("bug/title", Value::String(title)),
        ("bug/detail", Value::String(detail)),
        (
            "bug/status",
            Value::String(["open", "triaged", "closed"][index % 3].into()),
        ),
        (
            "bug/rank",
            Value::String(format!("rank-{:04}", index % 500)),
        ),
        (
            "bug/reporter",
            Value::String(format!("member:{}", index % 7)),
        ),
        (
            "bug/created",
            Value::String(format!("2026-{:02}-{:02}", 1 + index % 12, 1 + index % 28)),
        ),
    ];
    facts
        .into_iter()
        .map(|(the, is)| {
            Ok(Instruction::Assert(
                Artifact {
                    the: the.parse()?,
                    of: of.clone(),
                    is,
                    cause: None,
                },
                dialog_artifacts::Pick::All,
            ))
        })
        .collect()
}

/// The space's metadata and membership facts (what a join probes).
pub(crate) fn meta_facts(members: usize) -> Result<Vec<Instruction>> {
    let mut facts = vec![Instruction::Assert(
        Artifact {
            the: "db/name".parse()?,
            of: "id:space".parse()?,
            is: Value::String("soak space".into()),
            cause: None,
        },
        dialog_artifacts::Pick::All,
    )];
    for member in 0..members {
        let of: dialog_artifacts::Entity = format!("member:{member}").parse()?;
        facts.push(Instruction::Assert(
            Artifact {
                the: "member/name".parse()?,
                of: of.clone(),
                is: Value::String(format!("Member {member}")),
                cause: None,
            },
            dialog_artifacts::Pick::All,
        ));
        facts.push(Instruction::Assert(
            Artifact {
                the: "member/role".parse()?,
                of,
                is: Value::String(if member == 0 { "owner" } else { "editor" }.into()),
                cause: None,
            },
            dialog_artifacts::Pick::All,
        ));
    }
    Ok(facts)
}

/// The concept the seeded rule derives: an open card, title and rank
/// projected off the bug entity. No `open/*` fact is ever written, so
/// every row exists only through the rule.
fn open_card_descriptor() -> Result<ConceptDescriptor> {
    Ok(serde_json::from_value(serde_json::json!({
        "with": {
            "title": { "the": "open/title", "as": "text:" },
            "rank": { "the": "open/rank", "as": "text:" }
        }
    }))?)
}

/// The deductive rule shipped with the space: a bug whose status is
/// "open" concludes an open card carrying its title and rank. The body
/// is the general rule-join shape (two bound scans plus a value-pinned
/// guard) that bead dialog-db-80's speculation targets.
pub(crate) fn open_card_rule() -> Result<DeductiveRule> {
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": {
            "with": {
                "title": { "the": "open/title", "as": "text:" },
                "rank": { "the": "open/rank", "as": "text:" }
            }
        },
        "when": [{
            "assert": {
                "with": {
                    "title": { "the": "bug/title", "as": "text:" },
                    "rank": { "the": "bug/rank", "as": "text:" },
                    "status": { "the": "bug/status", "as": "text:" }
                }
            },
            "where": {
                "this": { "?": { "name": "this" } },
                "title": { "?": { "name": "title" } },
                "rank": { "?": { "name": "rank" } },
                "status": "open"
            }
        }]
    }))?;
    descriptor
        .compile()
        .map_err(|error| anyhow::anyhow!("open-card rule should compile: {error}"))
}

/// The joiner's claim: the membership facts a join commits.
fn claim_facts() -> Result<Vec<Instruction>> {
    let of: dialog_artifacts::Entity = "member:joiner".parse()?;
    Ok(vec![
        Instruction::Assert(
            Artifact {
                the: "member/name".parse()?,
                of: of.clone(),
                is: Value::String("The Joiner".into()),
                cause: None,
            },
            dialog_artifacts::Pick::All,
        ),
        Instruction::Assert(
            Artifact {
                the: "member/role".parse()?,
                of: of.clone(),
                is: Value::String("editor".into()),
                cause: None,
            },
            dialog_artifacts::Pick::All,
        ),
        Instruction::Assert(
            Artifact {
                the: "member/joined".parse()?,
                of,
                is: Value::String("2026-09-01".into()),
                cause: None,
            },
            dialog_artifacts::Pick::All,
        ),
    ])
}

/// Seed a fresh directory as the space for `repo` by writing its
/// credential to `credential/key/self` (the same precondition
/// `dialog-remote-fs` documents: the directory must already be a space).
pub(crate) async fn seed_vault(
    repo: &Repository<SignerCredential>,
    location: &Location,
) -> Result<FsAddress> {
    let filesystem = FileSystem::open(location).await?;
    let credential = Credential::Signer(repo.credential().clone());
    repo.did()
        .credential()
        .key("self")
        .save(credential)
        .perform(&filesystem)
        .await?;
    Ok(FsAddress::new(location.clone()))
}

/// Open a repository for `profile`, wire `origin` at `address` for the
/// server's subject, and track its `main` branch.
///
/// Every client of a run shares the operator's contacts, so they all name
/// the one `vault` peer `origin`: recording the same peer under the same
/// name again is idempotent, where a second peer under the name would
/// make `origin` ambiguous and refuse every client after the first.
pub(crate) async fn mount_client(
    operator: &Peer<VolatileSpace, dialog_peer::Session>,
    profile: &Peer<VolatileSpace>,
    server: &Repository<SignerCredential>,
    vault: &Did,
    address: &FsAddress,
    name: &str,
) -> Result<Branch> {
    let repo = profile
        .space(unique_name(name))
        .open()
        .perform(operator)
        .await?;
    let origin = {
        let site = SiteAddress::Fs(address.clone());
        contact(vault)
            .add_address(site)
            .name("origin")
            .perform(operator)
            .await?;
        contact("origin")
            .connect()
            .repository(server.did())
            .open()
            .perform(operator)
            .await?
    };
    let branch = repo.branch("main").open().perform(operator).await?;
    let remote_branch = origin.branch("main").open().perform(operator).await?;
    branch.set_upstream(remote_branch).perform(operator).await?;
    Ok(branch)
}

/// Run `select` on `branch` and count the rows, failing on any row error
/// — a phase must observe real data, not a lazily erred stream.
async fn select_count(
    branch: &Branch,
    operator: &Peer<VolatileSpace, dialog_peer::Session>,
    selector: ArtifactSelector<dialog_artifacts::selector::Constrained>,
) -> Result<usize> {
    let rows = branch
        .claims()
        .select(selector)
        .to_owned()
        .perform(operator)
        .await?
        .collect::<Vec<_>>()
        .await;
    let mut count = 0;
    for row in rows {
        row?;
        count += 1;
    }
    Ok(count)
}

/// The join's phases over a seeded site, in execution order.
pub(crate) async fn phases(
    site: &Site,
    scenario: &Scenario,
    phases: &mut Vec<PhaseReport>,
) -> Result<()> {
    let operator = &site.operator;

    let client = site.mount("soak-client").await?;

    measured("pull", phases, async {
        client.pull().perform(operator).await?;
        Ok(())
    })
    .await?;

    measured("probe", phases, async {
        let named = select_count(
            &client,
            operator,
            ArtifactSelector::new().the("db/name".parse()?),
        )
        .await?;
        anyhow::ensure!(named == 1, "probe should find the space name");
        Ok(())
    })
    .await?;

    measured("roster", phases, async {
        let names = select_count(
            &client,
            operator,
            ArtifactSelector::new().the("member/name".parse()?),
        )
        .await?;
        let roles = select_count(
            &client,
            operator,
            ArtifactSelector::new().the("member/role".parse()?),
        )
        .await?;
        let joined = select_count(
            &client,
            operator,
            ArtifactSelector::new().the("member/joined".parse()?),
        )
        .await?;
        anyhow::ensure!(
            names == scenario.members && roles == scenario.members && joined == 0,
            "roster reads should see the seeded membership"
        );
        Ok(())
    })
    .await?;

    measured("claim", phases, async {
        client
            .transaction()
            .integrate(claim_facts()?.into_iter().collect())
            .commit()
            .publish()
            .perform(operator)
            .await?;
        client
            .push()
            .perform(operator)
            .await?
            .context("claim push should publish")?;
        Ok(())
    })
    .await?;

    let expected = scenario.entities;
    measured("render", phases, async {
        let titles = select_count(
            &client,
            operator,
            ArtifactSelector::new().the("bug/title".parse()?),
        )
        .await?;
        anyhow::ensure!(titles == expected, "render should see every title");
        Ok(())
    })
    .await?;

    measured("entity", phases, async {
        let details = select_count(
            &client,
            operator,
            ArtifactSelector::new()
                .the("bug/detail".parse()?)
                .of(format!("bug:{}", expected / 2).parse()?),
        )
        .await?;
        anyhow::ensure!(details == 1, "entity read should find the detail");
        Ok(())
    })
    .await?;

    measured("requery", phases, async {
        let titles = select_count(
            &client,
            operator,
            ArtifactSelector::new().the("bug/title".parse()?),
        )
        .await?;
        anyhow::ensure!(titles == expected, "requery should see every title");
        Ok(())
    })
    .await?;

    // A cold client's first landing-page render through the query engine's
    // concept join, rather than the raw attribute selects above. Today the
    // evaluator resolves one premise at a time and awaits one Select per
    // outer row, so on a cold replica the join's fetches serialize; this
    // phase is the yardstick issue #492 is judged by. The client is fresh
    // so every block is cold; the pull that adopts the head runs outside
    // the measured window.
    let concept_client = site.mount("soak-concept").await?;
    concept_client.pull().perform(operator).await?;
    measured("concept", phases, async {
        let layer = concept_client.query();
        let query = layer.select(Query::<Card> {
            this: Term::var("this"),
            title: Term::var("title"),
            status: Term::var("status"),
            rank: Term::var("rank"),
            reporter: Term::var("reporter"),
            created: Term::var("created"),
        });
        let cards: Vec<Card> = query.perform(operator).try_vec().await?;
        anyhow::ensure!(
            cards.len() == expected,
            "concept join should see every card"
        );
        Ok(())
    })
    .await?;

    // The selective variant: status pinned to one value, so the value-bound
    // scan drives the join and the other premises become entity probes. The
    // contrast between this phase and `concept` is the block-count versus
    // round-trip tradeoff the merge-versus-fold decision weighs.
    let filtered_client = site.mount("soak-filtered").await?;
    filtered_client.pull().perform(operator).await?;
    let closed = (0..scenario.entities)
        .filter(|index| index % 3 == 2)
        .count();
    measured("filtered", phases, async {
        let layer = filtered_client.query();
        let query = layer.select(Query::<Card> {
            this: Term::var("this"),
            title: Term::var("title"),
            status: Term::from("closed".to_string()),
            rank: Term::var("rank"),
            reporter: Term::var("reporter"),
            created: Term::var("created"),
        });
        let cards: Vec<Card> = query.perform(operator).try_vec().await?;
        anyhow::ensure!(
            cards.len() == closed,
            "filtered join should see the closed cards"
        );
        Ok(())
    })
    .await?;

    // The UI's actual cold path: a standing query's first poll on a fresh
    // client. With the ambient queue the subscription's evaluation enqueues
    // and drives its own hints, so this phase gates bead dialog-db-82's
    // subscription parity.
    let subscribe_client = site.mount("soak-subscribe").await?;
    subscribe_client.pull().perform(operator).await?;
    measured("subscribe", phases, async {
        let mut subscription = subscribe_client.subscribe(Query::<Card> {
            this: Term::var("this"),
            title: Term::var("title"),
            status: Term::var("status"),
            rank: Term::var("rank"),
            reporter: Term::var("reporter"),
            created: Term::var("created"),
        });
        let delta = subscription
            .poll(operator)
            .await
            .map_err(|error| anyhow::anyhow!("first poll failed: {error}"))?;
        let added = delta.map(|delta| delta.asserted.len()).unwrap_or(0);
        anyhow::ensure!(
            added == expected,
            "the first poll should see every card, saw {added}"
        );
        Ok(())
    })
    .await?;

    // Two independent concepts, run concurrently on one fresh cold
    // client, overlapping on three of five attribute ranges. What the
    // ledger gates: the overlap hydrates ONCE (the env-owned flight and
    // queue are shared across evaluations, so the unique count reads as
    // the union of the two footprints and duplicates stay zero), and the
    // rounds read as overlapped work, not the sum of two sequential runs.
    let overlap_client = site.mount("soak-overlap").await?;
    overlap_client.pull().perform(operator).await?;
    measured("overlap", phases, async {
        let board_layer = overlap_client.query();
        let board = board_layer.select(Query::<Card> {
            this: Term::var("this"),
            title: Term::var("title"),
            status: Term::var("status"),
            rank: Term::var("rank"),
            reporter: Term::var("reporter"),
            created: Term::var("created"),
        });
        let list_layer = overlap_client.query();
        let list = list_layer.select(Query::<Listing> {
            this: Term::var("this"),
            title: Term::var("title"),
            status: Term::var("status"),
            rank: Term::var("rank"),
        });
        let (cards, rows): (Vec<Card>, Vec<Listing>) = futures_util::future::try_join(
            board.perform(operator).try_vec(),
            list.perform(operator).try_vec(),
        )
        .await?;
        anyhow::ensure!(
            cards.len() == expected && rows.len() == expected,
            "both overlapping queries should see every entity"
        );
        Ok(())
    })
    .await?;

    // A concept that exists only through the seeded rule, on a fresh
    // cold client: the query must discover the rule (conclusion index
    // scan), hydrate its body (source fetch), and run the body's join
    // cold. The general rule-join speculation of bead dialog-db-80 is
    // measured against this phase.
    let rule_client = site.mount("soak-rule").await?;
    rule_client.pull().perform(operator).await?;
    let open = (0..scenario.entities)
        .filter(|index| index % 3 == 0)
        .count();
    measured("rule", phases, async {
        let layer = rule_client.query();
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("title".into(), Term::var("title"));
        terms.insert("rank".into(), Term::var("rank"));
        let query = ConceptQuery {
            predicate: open_card_descriptor()?,
            terms,
        };
        let rows: Vec<ConceptConclusion> = layer.select(query).perform(operator).try_vec().await?;
        anyhow::ensure!(rows.len() == open, "the rule should derive every open card");
        Ok(())
    })
    .await?;

    let downloader = site.mount("soak-download").await?;
    measured("download", phases, async {
        downloader.pull().download().perform(operator).await?;
        Ok(())
    })
    .await?;

    // A device that seeded itself before its first contact with the
    // space. That is how a profile joins an account: its defaults are
    // committed locally first, so the pull replays the seed onto the
    // remote tree (the reverse-replay arm) instead of adopting the head
    // by reference as the `pull` phase does. The seed is dense in key
    // order, consecutive entities under the same attributes, which is
    // the shape whose integrate read the remote tree leaf by leaf while
    // its opening pass looked ahead a fixed number of changes: the
    // rounds here should track the tree's depth, never its leaf count.
    let seeded = site.mount("soak-seeded").await?;
    let mut seed = Vec::new();
    for index in scenario.entities..scenario.entities + scenario.entities / 4 {
        seed.extend(entity_facts(index)?);
    }
    seeded
        .transaction()
        .integrate(seed.into_iter().collect())
        .commit()
        .publish()
        .perform(operator)
        .await?;
    measured("seeded", phases, async {
        seeded.pull().perform(operator).await?;
        Ok(())
    })
    .await?;

    Ok(())
}

/// A DID to name the peer a vault directory is by. A directory has no DID
/// of its own, so the soak gives it one, as an application names a peer
/// by the DID it was given. Minted once per run: the clients of a run
/// share the operator's contacts, and a name is one peer's.
pub(crate) async fn vault_peer() -> Result<Did> {
    Ok(Principal::did(&Ed25519Signer::generate().await?))
}
