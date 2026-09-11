//! Archive providers for S3.

use std::sync::Arc;

use async_trait::async_trait;
use dialog_capability::ForkInvocation;
use dialog_capability::Provider;
use dialog_capability::access::AuthorizeError;
use dialog_effects::archive::*;
use reqwest::StatusCode;

use crate::S3Error;
use crate::flight::Flight;
use crate::s3::{S3, S3Invocation};

/// TEMPORARY (#492): a serial number per traced request, so a start can
/// be matched to its end in an interleaved log.
fn request_tag() -> usize {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// TEMPORARY (#492): print one line per request boundary.
///
/// The ORDER of these lines is the measurement. `start 0 / end 0 / start
/// 1 / end 1` is a serial chain: each request was awaited before the next
/// was issued, so each cost its own round trip. `start 0 / start 1 / ...
/// / end 0 / end 1` means they were genuinely in flight together. Unlike
/// a counter this cannot be inflated by joiners on a shared flight, and
/// it reads the same on native and in a service worker.
fn trace_request(phase: &str, tag: usize, block: &str) {
    let line = format!("[s3 {phase} #{tag}] {block}");
    #[cfg(target_arch = "wasm32")]
    {
        // The worker has no stderr; its console is where a probe lands.
        web_sys::console::log_1(&wasm_bindgen::JsValue::from_str(&line));
    }
    #[cfg(not(target_arch = "wasm32"))]
    eprintln!("{line}");
}

/// In-flight block GETs, joined by presigned URL.
///
/// A block is immutable content, so every caller holding the same
/// presigned URL gets the same bytes — the one read that is always safe
/// to share. The URL's signature binds it to the permit (and through it
/// the operator) that redeemed it, so a process-wide registry shares
/// nothing across operators: a different operator's request for the
/// same object carries a different signature and never joins.
///
/// Mutable reads (memory cells) deliberately do not come through here.
type BlockGets = Flight<String, Result<(u16, Arc<Vec<u8>>), S3Error>>;

#[cfg(not(target_arch = "wasm32"))]
fn block_gets() -> &'static BlockGets {
    static BLOCK_GETS: std::sync::LazyLock<BlockGets> = std::sync::LazyLock::new(Flight::default);
    &BLOCK_GETS
}

/// See the native arm; a worker context is single-threaded, so the
/// registry lives in a thread-local and is handed out by `Rc`.
#[cfg(target_arch = "wasm32")]
fn block_gets() -> std::rc::Rc<BlockGets> {
    thread_local! {
        static BLOCK_GETS: std::rc::Rc<BlockGets> = Default::default();
    }
    BLOCK_GETS.with(std::rc::Rc::clone)
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<S3, Get>> for S3 {
    async fn execute(
        &self,
        input: ForkInvocation<S3, Get>,
    ) -> Result<Option<Vec<u8>>, ArchiveError> {
        input
            .authorization
            .redeem(&input.address)
            .await?
            .invoke(input.capability)
            .perform(self)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<S3Invocation<Get>> for S3 {
    async fn execute(&self, input: S3Invocation<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        // Concurrent readers of one block join a single request; see
        // `block_gets`. The future owns its permit, so it outlives any
        // one caller and whoever still cares drives it.
        let key = input.permit.url.to_string();
        let permit = input.permit;
        let (status, bytes) = block_gets()
            .join(key, move || async move {
                // TEMPORARY (#492): bracket the ACTUAL request so the log
                // says whether requests interleave. Serial reads print
                // start/end/start/end; overlapping ones print
                // start/start/.../end/end. This sits at the last point
                // before the socket, so unlike a counter over effect
                // dispatches it cannot be inflated by joiners on a shared
                // flight or by work the transport later serializes.
                let tag = request_tag();
                let block = permit
                    .url
                    .path()
                    .rsplit('/')
                    .next()
                    .unwrap_or_default()
                    .chars()
                    .take(12)
                    .collect::<String>();
                trace_request("start", tag, &block);
                let response = match permit.send().await {
                    Ok(response) => response,
                    Err(error) => {
                        trace_request("fail", tag, &block);
                        return Err(error);
                    }
                };
                let status = response.status().as_u16();
                let bytes = response.bytes().await.map_err(S3Error::from)?;
                trace_request("end", tag, &block);
                Ok((status, Arc::new(bytes.to_vec())))
            })
            .await?;

        let status = StatusCode::from_u16(status)
            .map_err(|error| ArchiveError::Storage(format!("invalid status: {error}")))?;
        if status.is_success() {
            Ok(Some(bytes.as_ref().clone()))
        } else if status == StatusCode::NOT_FOUND {
            Ok(None)
        } else if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
            Err(ArchiveError::Authorization(
                AuthorizeError::UnavailableProof {
                    link: format!("Failed to get value: {status}"),
                },
            ))
        } else {
            Err(ArchiveError::Storage(format!(
                "Failed to get value: {status}"
            )))
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<S3, Put>> for S3 {
    async fn execute(&self, input: ForkInvocation<S3, Put>) -> Result<(), ArchiveError> {
        input
            .authorization
            .redeem(&input.address)
            .await?
            .invoke(input.capability)
            .perform(self)
            .await
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use dialog_capability::{Provider, Subject, did};
    use dialog_effects::Use;
    use dialog_effects::archive::{Archive, Catalog, Get};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::s3::{Permit, S3, S3Invocation};

    /// A one-shot object server: answers every GET with the same bytes,
    /// counting how many requests arrived.
    async fn counting_server(body: &'static [u8]) -> (String, Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let count = Arc::new(AtomicUsize::new(0));
        let counted = count.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                counted.fetch_add(1, Ordering::SeqCst);
                let mut buffer = [0u8; 4096];
                let _ = stream.read(&mut buffer).await;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.write_all(body).await;
            }
        });
        (endpoint, count)
    }

    /// Concurrent readers holding one presigned URL share one request:
    /// a block is immutable, so the second read joins the first's
    /// in-flight fetch instead of paying its own round trip.
    #[dialog_common::test]
    async fn it_shares_one_fetch_between_concurrent_block_reads() {
        let (endpoint, hits) = counting_server(b"block bytes").await;

        let permit = Permit {
            url: format!("{endpoint}/bucket/subject/index/shared-block")
                .parse()
                .unwrap(),
            method: "GET".to_string(),
            headers: vec![],
        };
        let capability = || {
            Subject::from(did!("key:zSharedBlockReadTest"))
                .attenuate(Use)
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Get::new([4u8; 32]))
        };

        let first = Provider::<S3Invocation<Get>>::execute(
            &S3,
            S3Invocation::new(permit.clone(), capability()),
        );
        let second = Provider::<S3Invocation<Get>>::execute(
            &S3,
            S3Invocation::new(permit.clone(), capability()),
        );
        let (first, second) = tokio::join!(first, second);

        assert_eq!(first.unwrap(), Some(b"block bytes".to_vec()));
        assert_eq!(second.unwrap(), Some(b"block bytes".to_vec()));
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "concurrent reads of one presigned URL must share one fetch"
        );

        // The flight holds in-flight work only: a later read fetches
        // afresh rather than being served yesterday's response.
        let third =
            Provider::<S3Invocation<Get>>::execute(&S3, S3Invocation::new(permit, capability()))
                .await
                .unwrap();
        assert_eq!(third, Some(b"block bytes".to_vec()));
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<S3Invocation<Put>> for S3 {
    async fn execute(&self, input: S3Invocation<Put>) -> Result<(), ArchiveError> {
        let put = input.capability.into_effect();
        let response = input.permit.upload(put.block.as_ref().to_vec()).await?;

        if response.status().is_success() {
            Ok(())
        } else if matches!(
            response.status(),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
        ) {
            Err(ArchiveError::Authorization(
                AuthorizeError::UnavailableProof {
                    link: format!("Failed to put value: {}", response.status()),
                },
            ))
        } else {
            Err(ArchiveError::Storage(format!(
                "Failed to put value: {}",
                response.status()
            )))
        }
    }
}
