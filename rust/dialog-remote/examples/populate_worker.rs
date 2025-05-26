use std::sync::Arc;

use anyhow::Result;
use base58::ToBase58;
use dialog_artifacts::{Artifacts, Revision, make_reference};
use dialog_storage::{CborEncoder, Encoder, MemoryStorageBackend, StorageBackend};
use dialog_storage::{TapOperation, TappedStorage};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use url::Url;

#[tokio::main]
pub async fn main() -> Result<()> {
    let worker_url: Url = std::env::var("WORKER_URL")?.parse()?;
    let csv_path = std::env::var("CSV_PATH")?;

    eprintln!("WORKER URL: {worker_url}");
    eprintln!("CSV PATH: {csv_path}");

    let storage = Arc::new(Mutex::new(TappedStorage::new(
        MemoryStorageBackend::default(),
    )));
    let mut artifacts = Artifacts::open("did:key:pokemon-database".into(), storage.clone()).await?;

    {
        eprintln!("IMPORTING...");
        let mut csv_file = tokio::fs::File::open(csv_path).await?;
        artifacts.import(&mut csv_file).await?;
    }

    let mut rx = { storage.lock().await.tap()? };

    Artifacts::open("did:key:pokemon-database".into(), storage.clone()).await?;

    let populate_task = tokio::task::spawn(async move {
        let mut requests = JoinSet::<Result<(), reqwest::Error>>::new();

        while let Some(operation) = rx.recv().await {
            match operation {
                TapOperation::Get((key, value)) => {
                    let mut url = worker_url.clone();
                    // requests.spawn(async move {
                    url.set_path(&format!("/block/{}", key.as_ref().to_base58()));
                    println!("POST {}", url);
                    let response = reqwest::Client::new()
                        .post(url.clone())
                        .body(value)
                        .send()
                        .await
                        .unwrap();
                    println!("POST {} -> STATUS {}", url, response.status());
                    // Ok(())
                    // });
                }
                _ => (),
            }
        }

        while let Some(result) = requests.join_next().await {
            match result {
                Ok(result) => match result {
                    Err(error) => {
                        eprintln!("Request error: {}", error);
                    }
                    _ => (),
                },
                Err(error) => {
                    eprintln!("Join error: {}", error);
                }
            }
        }
    });

    // {
    //     let key: [u8; 32] =
    //         base58::FromBase58::from_base58("BX2wVYdesHCdS35SkXpQUnXrDrL2P9jreeyPitp7zvYh")
    //             // base58::FromBase58::from_base58("5KuVU6iYtRXwquPsBoVjtXnAeZ7Nmn3qa1rQzdLa4oNU")
    //             .unwrap()
    //             .try_into()
    //             .unwrap();
    //     let block = storage.get(&key).await?.unwrap();
    //     let segment = CborEncoder
    //         .decode::<dialog_prolly_tree::Block<32, , Vec<u8>, [u8; 32]>>(&block)
    //         .await?;

    //     eprintln!("MISSING BLOCK: {:?}", segment);
    //     // let key: [u8; 32] = [u8; 32]::from_base58("5KuVU6iYtRXwquPsBoVjtXnAeZ7Nmn3qa1rQzdLa4oNU")?;
    // }

    artifacts.export(&mut tokio::io::sink()).await?;

    {
        let storage = storage.lock().await;
        match storage
            .get(&make_reference("did:key:pokemon-database".as_bytes()))
            .await?
        {
            None => {
                panic!("ERROR: Mutable pointer not found!");
            }
            Some(key) => {
                eprintln!("MUTABLE POINTER => {}", key.to_base58());

                let key: [u8; 32] = key.try_into().unwrap();

                match storage.get(&key.try_into().unwrap()).await? {
                    None => {
                        panic!("ERROR: Mutable pointer referred to missing block")
                    }
                    Some(block) => {
                        let reference = CborEncoder.decode::<Revision>(&block).await?;

                        storage.get(&reference.entity_index()).await?;
                        storage.get(&reference.attribute_index()).await?;
                        storage.get(&reference.value_index()).await?;

                        // eprintln!(
                        //     "E: {}, A: {}, V: {}",
                        //     reference.entity_index().to_base58(),
                        //     reference.attribute_index().to_base58(),
                        //     reference.value_index().to_base58()
                        // );
                    }
                }
            }
        };
    }

    storage.lock().await.untap()?;

    populate_task.await?;

    eprintln!("FIN!");

    Ok(())
}
