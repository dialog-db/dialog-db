use anyhow::Result;
use dialog_prolly_tree::{GeometricDistribution, Tree};
use dialog_storage::{CborEncoder, MeasuredStorage, MemoryStorageBackend, Storage, StorageCache};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;

fn bytes(s: &str) -> Vec<u8> {
    String::from(s).into_bytes()
}

#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test;
#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn basic_set_and_get() -> Result<()> {
    let mut storage = Storage {
        backend: MemoryStorageBackend::default(),
        encoder: CborEncoder,
    };
    let mut tree = Tree::<GeometricDistribution, _, _, _>::new();

    tree.set(bytes("foo1"), bytes("bar1"), &mut storage).await?;
    tree.set(bytes("foo2"), bytes("bar2"), &mut storage).await?;
    tree.set(bytes("foo3"), bytes("bar3"), &mut storage).await?;

    assert_eq!(tree.get(&bytes("bar"), &storage).await?, None);
    assert_eq!(
        tree.get(&bytes("foo1"), &storage).await?,
        Some(bytes("bar1"))
    );
    assert_eq!(
        tree.get(&bytes("foo2"), &storage).await?,
        Some(bytes("bar2"))
    );
    assert_eq!(
        tree.get(&bytes("foo3"), &storage).await?,
        Some(bytes("bar3"))
    );

    let mut inverse_tree = Tree::<GeometricDistribution, _, _, _>::new();

    inverse_tree
        .set(bytes("foo3"), bytes("bar3"), &mut storage)
        .await?;
    inverse_tree
        .set(bytes("foo2"), bytes("bar2"), &mut storage)
        .await?;
    inverse_tree
        .set(bytes("foo1"), bytes("bar1"), &mut storage)
        .await?;

    assert_eq!(
        tree.hash(),
        inverse_tree.hash(),
        "alternate insertion order results in same hash"
    );

    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn basic_delete() -> Result<()> {
    let mut storage = Storage {
        backend: MemoryStorageBackend::default(),
        encoder: CborEncoder,
    };
    let mut expected_tree = Tree::<GeometricDistribution, _, _, _>::new();

    expected_tree
        .set(bytes("foo1"), bytes("bar1"), &mut storage)
        .await?;
    expected_tree
        .set(bytes("foo3"), bytes("bar3"), &mut storage)
        .await?;

    let mut tree = Tree::<GeometricDistribution, _, _, _>::new();

    tree.set(bytes("foo1"), bytes("bar1"), &mut storage).await?;
    tree.set(bytes("foo2"), bytes("bar2"), &mut storage).await?;
    tree.set(bytes("foo3"), bytes("bar3"), &mut storage).await?;

    tree.delete(&bytes("foo2"), &mut storage).await?;

    assert_eq!(
        tree.get(&bytes("foo1"), &storage).await?,
        Some(bytes("bar1"))
    );
    assert_eq!(tree.get(&bytes("foo2"), &storage).await?, None);
    assert_eq!(
        tree.get(&bytes("foo3"), &storage).await?,
        Some(bytes("bar3"))
    );

    assert_eq!(tree.hash(), expected_tree.hash());

    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn delete_from_tree_with_one_entry() -> Result<()> {
    let mut storage = Storage {
        backend: MemoryStorageBackend::default(),
        encoder: CborEncoder,
    };

    let mut tree = Tree::<GeometricDistribution, _, _, _>::new();

    tree.set(bytes("foo1"), bytes("bar1"), &mut storage).await?;

    tree.delete(&bytes("foo1"), &mut storage).await?;

    assert_eq!(tree.get(&bytes("foo1"), &storage).await?, None);
    assert_eq!(tree.hash(), None);

    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn create_tree_from_set() -> Result<()> {
    let mut iter_storage = Storage {
        backend: MemoryStorageBackend::default(),
        encoder: CborEncoder,
    };
    let mut collection_storage = Storage {
        backend: MemoryStorageBackend::default(),
        encoder: CborEncoder,
    };
    let mut iter_tree = Tree::<GeometricDistribution, _, _, _>::new();
    let mut collection = BTreeMap::default();

    for i in 0..=255 {
        let key = vec![i];
        let value = vec![255 - i];
        collection.insert(key.clone(), value.clone());
        iter_tree.set(key, value, &mut iter_storage).await?;
    }
    let collection_tree = Tree::<GeometricDistribution, _, _, _>::from_collection(
        collection,
        &mut collection_storage,
    )
    .await?;

    for i in 0..=255 {
        let key = vec![i];
        let value = vec![255 - i];
        assert_eq!(
            collection_tree.get(&key, &collection_storage).await?,
            Some(value.clone())
        );
        assert_eq!(iter_tree.get(&key, &iter_storage).await?, Some(value));
    }

    assert!(iter_tree.hash().is_some());
    assert_eq!(
        iter_tree.hash(),
        collection_tree.hash(),
        "arrives at same root hash"
    );
    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn larger_random_tree() -> Result<()> {
    use rand::{Rng, thread_rng as rng};

    fn random() -> Vec<u8> {
        let mut buffer = [0u8; 32];
        rng().fill(&mut buffer[..]);
        buffer.to_vec()
    }

    let mut ledger = vec![];
    let mut storage = Storage {
        backend: MemoryStorageBackend::default(),
        encoder: CborEncoder,
    };
    let mut tree = Tree::<GeometricDistribution, _, _, _>::new();
    for _ in 1..1024 {
        let key_value = (random(), random());
        ledger.push(key_value.clone());
        tree.set(key_value.0, key_value.1, &mut storage).await?;
    }

    for entry in ledger {
        assert_eq!(tree.get(&entry.0, &storage).await?, Some(entry.1));
    }

    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn restores_tree_from_hash() -> Result<()> {
    let mut storage = Storage {
        backend: MemoryStorageBackend::default(),
        encoder: CborEncoder,
    };
    let mut tree = Tree::<GeometricDistribution, _, _, _>::new();

    tree.set(bytes("foo1"), bytes("bar1"), &mut storage).await?;
    tree.set(bytes("foo2"), bytes("bar2"), &mut storage).await?;
    tree.set(bytes("foo3"), bytes("bar3"), &mut storage).await?;

    let root_hash = tree.hash().unwrap().to_owned();

    let tree = Tree::<GeometricDistribution, _, _, _>::from_hash(&root_hash, &storage).await?;

    assert_eq!(
        tree.get(&bytes("foo1"), &storage).await?,
        Some(bytes("bar1"))
    );
    assert_eq!(
        tree.get(&bytes("foo2"), &storage).await?,
        Some(bytes("bar2"))
    );
    assert_eq!(
        tree.get(&bytes("foo3"), &storage).await?,
        Some(bytes("bar3"))
    );

    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn lru_store_caches() -> Result<()> {
    let backend = MemoryStorageBackend::default();
    let root_hash = {
        let mut storage = Storage {
            backend: backend.clone(),
            encoder: CborEncoder,
        };
        let mut collection = BTreeMap::default();
        for i in 0..1024u32 {
            let key = i.to_be_bytes().to_vec();
            let value = <[u8; 32] as From<blake3::Hash>>::from(blake3::hash(&key)).to_vec();
            collection.insert(key, value);
        }
        let tree =
            Tree::<GeometricDistribution, _, _, _>::from_collection(collection, &mut storage)
                .await?;
        tree.hash().unwrap().to_owned()
    };

    let tracking = Arc::new(Mutex::new(MeasuredStorage::new(backend)));
    let lru = StorageCache::new(tracking.clone(), 10)?;
    let mut storage = Storage {
        backend: lru,
        encoder: CborEncoder,
    };
    let mut tree = Tree::<GeometricDistribution, _, _, _>::from_hash(&root_hash, &storage).await?;

    {
        let tracking = tracking.lock().await;

        assert_eq!(tracking.writes(), 0);
        assert_eq!(tracking.reads(), 1); // read root hash
    }

    let key = 1023u32.to_be_bytes().to_vec();
    let _ = tree.get(&key, &storage).await?;

    {
        let tracking = tracking.lock().await;
        assert_eq!(tracking.writes(), 0);
        assert_eq!(tracking.reads(), 4);
    }

    let _ = tree.get(&key, &storage).await?;

    {
        let tracking = tracking.lock().await;
        assert_eq!(tracking.writes(), 0);
        assert_eq!(tracking.reads(), 4); // reads cached
    }

    tree.set(key.to_vec(), vec![1], &mut storage).await?;

    let tracking = tracking.lock().await;
    assert_eq!(tracking.writes(), 4); // writes on insertion
    assert_eq!(tracking.reads(), 4); // reads cached

    Ok(())
}
