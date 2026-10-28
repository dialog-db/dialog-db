//! Compare file-per-block and shared-column archives on the same repository
//! replay. Run `cargo run -p dialog-baseline --example single_file -- 2048`.
//! DIALOG_SE_CSV selects real data; otherwise the existing synthetic SE model
//! is used. No fsync in either provider. Memory/credentials/assets stay volatile.
#[cfg(target_arch = "wasm32")]
fn main() {}
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod native {
    use dialog_artifacts::{Datum, Key, State};
    use dialog_baseline::metered::{Meter, Metered};
    use dialog_baseline::{
        repo::DialogRepo,
        se::{SeLog, se_instructions},
    };
    use dialog_capability::{Capability, Command, Provider};
    use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync, Held, Holds};
    use dialog_effects::archive::{Get, Import, Put};
    use dialog_effects::prelude::*;
    use dialog_search_tree::TreeFrameCodec;
    use dialog_storage::{
        provider::{FileSystem, SingleFile},
        resource::Resource,
    };
    use std::{
        any::Any,
        collections::HashMap,
        sync::{Arc, Mutex},
        time::Instant,
    };
    type Codec = TreeFrameCodec<Key, State<Datum>>;
    #[derive(Clone)]
    struct ArchiveOverride<E, A> {
        inner: E,
        archive: A,
    }
    impl<E: Holds, A> Holds for ArchiveOverride<E, A> {
        fn held(&self, key: &str) -> Option<Held> {
            self.inner.held(key)
        }
        fn hold(&self, key: String, value: Held) {
            self.inner.hold(key, value)
        }
        fn held_or(&self, key: &str, make: &dyn Fn() -> Held) -> Held {
            self.inner.held_or(key, make)
        }
    }
    #[async_trait::async_trait]
    impl<C, E, A> Provider<C> for ArchiveOverride<E, A>
    where
        C: Command + 'static,
        C::Input: ConditionalSend + 'static,
        C::Output: 'static,
        E: Provider<C> + ConditionalSync,
        A: Provider<Get> + Provider<Put> + Provider<Import> + ConditionalSync,
    {
        async fn execute(&self, input: C::Input) -> C::Output {
            fn output<T: 'static, U: 'static>(value: T) -> U {
                *(Box::new(value) as Box<dyn Any>)
                    .downcast::<U>()
                    .expect("matching archive output")
            }
            let (get, put, import) = {
                let any = &input as &dyn Any;
                (
                    any.downcast_ref::<Capability<Get>>().cloned(),
                    any.downcast_ref::<Capability<Put>>().cloned(),
                    any.downcast_ref::<Capability<Import>>().cloned(),
                )
            };
            if let Some(effect) = get {
                return output(<A as Provider<Get>>::execute(&self.archive, effect).await);
            }
            if let Some(effect) = put {
                return output(<A as Provider<Put>>::execute(&self.archive, effect).await);
            }
            if let Some(effect) = import {
                return output(<A as Provider<Import>>::execute(&self.archive, effect).await);
            }
            self.inner.execute(input).await
        }
    }
    struct ArchiveNodes {
        archive: SingleFile<Codec>,
        catalogs: HashMap<Blake3Hash, String>,
    }
    #[async_trait::async_trait]
    impl Provider<dialog_search_tree::LoadBlock> for ArchiveNodes {
        async fn execute(
            &self,
            effect: dialog_search_tree::LoadBlock,
        ) -> Result<Option<dialog_common::Buffer>, dialog_search_tree::DialogSearchTreeError>
        {
            let Some(catalog) = self.catalogs.get(&effect.hash) else {
                return Ok(None);
            };
            self.archive
                .get(catalog, *effect.hash.as_bytes())
                .await
                .map(|bytes| bytes.map(dialog_common::Buffer::from))
                .map_err(|e| dialog_search_tree::DialogSearchTreeError::Access(e.to_string()))
        }
    }
    #[derive(Clone, Default)]
    struct Blocks(Arc<Mutex<HashMap<Blake3Hash, usize>>>);
    impl Meter for Blocks {
        fn wrote(&self, block: &[u8]) {
            self.0
                .lock()
                .unwrap()
                .insert(Blake3Hash::hash(block), block.len());
        }
        fn read(&self, _: &[u8]) {}
    }
    fn disk(path: &std::path::Path) -> std::io::Result<u64> {
        if path.is_file() {
            return Ok(std::fs::metadata(path)?.len());
        }
        let mut size = 0;
        for entry in std::fs::read_dir(path)? {
            size += disk(&entry?.path())?;
        }
        Ok(size)
    }
    async fn run<A>(
        archive: A,
        log: &SeLog,
    ) -> anyhow::Result<(u128, Blocks, Vec<Blake3Hash>, usize)>
    where
        A: Provider<Get> + Provider<Put> + Provider<Import> + Clone + Send + Sync + 'static,
    {
        let seed = dialog_peer::helpers::Seed::new(7);
        let _entropy = seed.entropy();
        let blocks = Blocks::default();
        let repo = DialogRepo::seeded_through(&seed, |inner| {
            Metered::new(ArchiveOverride { inner, archive }, blocks.clone())
        })
        .await?;
        let start = Instant::now();
        let mut roots = Vec::new();
        for commit in &log.transactions {
            repo.branch()
                .transaction()
                .integrate(se_instructions(commit)?.into_iter().collect())
                .commit()
                .publish()
                .perform(repo.operator())
                .await?;
            roots.push(repo.root().expect("committed root"));
        }
        let elapsed = start.elapsed().as_millis();
        // Query a relation actually present in this workload, including --wide.
        let relation = log
            .transactions
            .iter()
            .flatten()
            .next()
            .map(|fact| fact.the.as_str());
        let Some(relation) = relation else {
            anyhow::bail!("measurement requires at least one fact");
        };
        let selector = dialog_artifacts::ArtifactSelector::new()
            .the(relation.parse::<dialog_artifacts::Relation>()?);
        let warm = repo.collect(selector.clone()).await?;
        assert!(!warm.is_empty(), "selected written relation must have rows");
        // Picks retain history, so each entity must expose its latest asserted
        // value even when earlier assertions of that relation are also retained.
        let mut expected = HashMap::new();
        for fact in log
            .transactions
            .iter()
            .flatten()
            .filter(|fact| fact.the == relation)
        {
            expected.insert(fact.of.clone(), fact.value.to_dialog()?);
        }
        for (entity, value) in expected {
            let entity = entity.parse::<dialog_artifacts::Entity>()?;
            assert!(
                warm.iter().any(|row| row.of == entity && row.is == value),
                "latest written value missing for {entity}"
            );
        }
        let reopened = repo.reopen().await?;
        let cold = repo.collect_from(&reopened, selector).await?;
        assert_eq!(warm, cold, "cold branch scan preserves results");
        let mut live_bytes = 0;
        if let Some(root) = roots.last() {
            dialog_baseline::nodes::walk(&repo.index(), root.clone(), |node| {
                live_bytes += node.size;
                Ok(())
            })
            .await?;
        }
        Ok((elapsed, blocks, roots, live_bytes))
    }
    #[cfg(test)]
    pub async fn check_repository_roundtrip(wide: bool) -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("archive");
        let archive = SingleFile::open(&path, Codec::new()).await?;
        let (_, _, roots, _) = run(
            archive.clone(),
            &if wide {
                wide_log(12)
            } else {
                SeLog::synthetic(12)
            },
        )
        .await?;
        drop(archive);
        let archive = SingleFile::open(&path, Codec::new()).await?;
        let catalogs = archive
            .entries()?
            .into_iter()
            .map(|(catalog, hash)| (Blake3Hash::from(hash), catalog))
            .collect();
        let nodes = ArchiveNodes { archive, catalogs };
        for root in roots {
            dialog_baseline::nodes::walk(&nodes, root, |_| Ok(())).await?;
        }
        Ok(())
    }
    // Wide live keys with repeated small updates: intentionally distinct from
    // the SE distribution, and not claimed to reproduce a production replica.
    fn wide_log(count: usize) -> SeLog {
        use dialog_baseline::se::{SeFact, SeValue};
        let fact = |entity: usize, revision: usize| SeFact {
            the: "se.post/body".to_owned(),
            of: format!("post:{entity}"),
            value: SeValue::Text(format!(
                "{entity:08}-{revision:08}-{}",
                "wide body ".repeat(200)
            )),
        };
        let mut transactions = Vec::new();
        if count > 0 {
            transactions.push((0..1000).map(|i| fact(i, 0)).collect());
        }
        for i in 1..count {
            transactions.push(vec![fact(i % 1000, i)]);
        }
        SeLog { transactions }
    }

    pub fn main() -> anyhow::Result<()> {
        let count = std::env::args()
            .nth(1)
            .map(|n| n.parse())
            .transpose()?
            .unwrap_or(2048);
        let wide = std::env::args().any(|arg| arg == "--wide");
        let log = if wide {
            wide_log(count)
        } else {
            SeLog::load(count)?
        };
        let mut live = HashMap::<(&str, &str), std::collections::HashSet<String>>::new();
        for fact in log.transactions.iter().flatten() {
            let values = live.entry((&fact.of, &fact.the)).or_default();
            if !dialog_baseline::se::is_multi_valued(&fact.the) {
                values.clear();
            }
            values.insert(format!("{:?}", fact.value));
        }
        let live_facts = live
            .values()
            .map(std::collections::HashSet::len)
            .sum::<usize>();
        let real = std::env::var_os("DIALOG_SE_CSV").is_some();
        let temp = tempfile::tempdir()?;
        tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
            let location = dialog_effects::storage::Location::new(dialog_effects::storage::Directory::At(temp.path().display().to_string()), "fs");
            let fs = FileSystem::open(&location).await?;
            let (fs_ms, fs_blocks, fs_roots, _) = run(fs.clone(), &log).await?;
            let path = temp.path().join("archive");
            let archive = SingleFile::open(&path, Codec::new()).await?;
            let (file_ms, blocks, roots, live_bytes) = run(archive.clone(), &log).await?;
            assert_eq!(*fs_blocks.0.lock().unwrap(), *blocks.0.lock().unwrap(), "identical seeded archive block sets");
            assert_eq!(fs_roots, roots, "identical seeded historical roots");
            let stats = archive.stats()?;
            drop(archive);
            let start = Instant::now();
            let archive = SingleFile::open(&path, Codec::new()).await?;
            let open_ms = start.elapsed().as_millis();
            let catalogs = archive.entries()?.into_iter().map(|(catalog, hash)| (Blake3Hash::from(hash), catalog)).collect();
            let nodes = ArchiveNodes { archive: archive.clone(), catalogs };
            for root in roots.first().into_iter().chain(roots.get(roots.len()/2)).chain(roots.last()) {
                dialog_baseline::nodes::walk(&nodes, root.clone(), |_| Ok(())).await?;
            }
            let blocks: Vec<_> = blocks.0.lock().unwrap().keys().cloned().collect();
            let start = Instant::now();
            let mut read_bytes = 0;
            let mut entries = archive.entries()?;
            entries.sort();
            // Sorted catalog/hash order is identical for both archive providers.
            for (catalog, hash) in &entries {
                let bytes = archive.get(catalog, *hash).await?.expect("indexed block");
                assert_eq!(Blake3Hash::hash(&bytes).as_bytes(), hash);
                read_bytes += bytes.len();
            }
            let read_ms = start.elapsed().as_millis();
            let start = Instant::now();
            let fs = FileSystem::open(&location).await?;
            let fs_open_ms = start.elapsed().as_millis();
            let subject = dialog_storage::unique_did().await;
            let start = Instant::now();
            let mut fs_read_bytes = 0;
            for (catalog, hash) in &entries {
                let bytes = subject.clone().reader().archive().catalog(catalog)
                    .get(Blake3Hash::from(*hash)).perform(&fs).await?.expect("filesystem counterpart");
                assert_eq!(Blake3Hash::hash(&bytes).as_bytes(), hash);
                fs_read_bytes += bytes.len();
            }
            let fs_read_ms = start.elapsed().as_millis();
            assert_eq!(read_bytes, fs_read_bytes);
            let start = Instant::now();
            let snapshot = archive.snapshot().await?;
            let snapshot_ms = start.elapsed().as_millis();
            let length = snapshot.len().min(4096);
            let proof = snapshot.prove(0, length).await?;
            dialog_storage::provider::single_file::RangeSnapshot::verify(snapshot.root(), 0, length, &proof)?;
            let fs_bytes = disk(&temp.path().join("fs"))?;
            let file_bytes = disk(&path)?;
            println!("source={} commits={} facts={} unique_blocks={} fs_bytes={} file_bytes={} ratio={:.3} fs_commit_ms={} file_commit_ms={} reopen_ms={} all_blocks_read_ms={} read_bytes={} tokens={} token_bytes={} index_bytes_lower_bound={} live_tree_bytes={} fs_reopen_ms={} fs_all_blocks_read_ms={} bao_snapshot_ms={} bao_outboard_bytes={} live_facts={} file_bytes_per_live_fact={:.1}",
                if wide {"synthetic-wide"} else if real {"csv"} else {"synthetic-se"}, count, log.fact_count(), blocks.len(), fs_bytes, file_bytes,
                fs_bytes as f64 / file_bytes as f64, fs_ms, file_ms, open_ms, read_ms, read_bytes, stats.tokens, stats.token_bytes, stats.index_bytes_lower_bound, live_bytes, fs_open_ms, fs_read_ms, snapshot_ms, snapshot.outboard_bytes(), live_facts, file_bytes as f64 / live_facts as f64);
            Ok(())
        })
    }
}
#[cfg(not(target_arch = "wasm32"))]
fn main() -> anyhow::Result<()> {
    native::main()
}
