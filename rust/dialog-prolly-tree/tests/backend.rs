use anyhow::Result;
use dialog_prolly_tree::{GeometricDistribution, Tree};
use dialog_storage::{CborEncoder, Storage, make_target_storage};
use rand::{Rng, thread_rng as rng};

fn random() -> Vec<u8> {
    let mut buffer = [0u8; 32];
    rng().fill(&mut buffer[..]);
    buffer.to_vec()
}

#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test;
#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn platform_specific_storage() -> Result<()> {
    let (backend, _temp) = make_target_storage().await?;
    let storage = Storage {
        backend,
        encoder: CborEncoder,
    };
    let mut tree = Tree::<GeometricDistribution, _, _, _, _>::new(storage);

    let mut ledger = vec![];
    for _ in 1..1024 {
        let key_value = (random(), random());
        ledger.push(key_value.clone());
        tree.set(key_value.0, key_value.1).await?;
    }

    for entry in ledger {
        assert_eq!(tree.get(&entry.0).await?, Some(entry.1));
    }
    Ok(())
}
