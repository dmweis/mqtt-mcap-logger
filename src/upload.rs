use std::{fs, sync::Arc};

use anyhow::Context;
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::{BlobServiceClient, BlockId, BlockList};
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc::channel, Mutex};

const MB_BYTES_MULTIPLIER: usize = 1024 * 1024;
const CHUNK_SIZE: usize = 4 * MB_BYTES_MULTIPLIER;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UploadConfig {
    sas_token: String,
    storage_account: String,
    container: String,
    storage_path: String,
}

pub async fn upload_file(path: &str, blob_name: &str, config: &UploadConfig) -> anyhow::Result<()> {
    let file = map_file(path)?;

    let chunks_with_offsets = file
        .chunks(CHUNK_SIZE)
        .enumerate()
        .map(|(index, data)| (index * CHUNK_SIZE, data));

    let storage_credentials = StorageCredentials::sas_token(&config.sas_token)?;

    let blob_path = format!("{}/{}", config.storage_path, blob_name);

    let blob_client = BlobServiceClient::builder(&config.storage_account, storage_credentials)
        .blob_client(&config.container, blob_path);

    if blob_client.exists().await? {
        anyhow::bail!("Blob already exists");
    }

    // start upload workers
    let (sender, receiver) = channel::<(BlockId, Vec<u8>)>(10);
    let receiver = Arc::new(Mutex::new(receiver));

    let mut workers = vec![];

    for _ in 0..10 {
        let blob_client = blob_client.clone();
        let receiver = receiver.clone();
        let future = tokio::spawn(async move {
            loop {
                let next = receiver.lock().await.recv().await;
                if let Some((block_id, chunk)) = next {
                    blob_client.put_block(block_id, chunk).await?;
                } else {
                    return Ok::<(), anyhow::Error>(());
                }
            }
        });
        workers.push(future);
    }
    let upload_finished = futures::future::join_all(workers);

    let mut block_list = BlockList::default();

    // upload chunks
    for (offset, chunk) in chunks_with_offsets {
        let block_id_name = format!("block-{}", offset);
        let block_id = BlockId::new(block_id_name.as_bytes().to_vec());

        block_list
            .blocks
            .push(azure_storage_blobs::blob::BlobBlockType::Uncommitted(
                block_id.clone(),
            ));

        sender.send((block_id, chunk.to_vec())).await?;
    }

    drop(sender);
    for worker in upload_finished.await {
        worker??;
    }

    let _block_list_place_res = blob_client.put_block_list(block_list).await?;

    Ok(())
}

fn map_file(p: &str) -> anyhow::Result<Mmap> {
    let fd = fs::File::open(p).context("Couldn't open MCAP file")?;
    unsafe { Mmap::map(&fd) }.context("Couldn't map MCAP file")
}
