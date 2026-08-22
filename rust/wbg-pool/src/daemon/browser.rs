//! Headless Chrome ownership and a minimal Chrome DevTools Protocol client.
//! Only three CDP operations are needed: create a tab, close a tab, and (on
//! timeout) scrape the harness output out of a wedged page.

use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncBufReadExt;
use tokio::sync::{mpsc, oneshot, Mutex};

/// Process group of the running browser, if any. The daemon's shutdown
/// watchdog runs on a plain thread with no access to async state, so it
/// needs a way to take the browser down without going through [`Browser`].
static BROWSER_GROUP: AtomicI32 = AtomicI32::new(0);

/// Kills the browser and everything it spawned, from anywhere, without the
/// async runtime. Safe to call when no browser is running.
pub fn kill_browser_group() {
    let group = BROWSER_GROUP.swap(0, Ordering::SeqCst);
    if group > 0 {
        unsafe { libc::killpg(group, libc::SIGKILL) };
    }
}

pub struct BrowserPool {
    profile_dir: PathBuf,
    inner: Mutex<Option<Browser>>,
}

impl BrowserPool {
    pub fn new(profile_dir: PathBuf) -> Self {
        BrowserPool {
            profile_dir,
            inner: Mutex::new(None),
        }
    }

    /// Opens a tab at `url`, relaunching the browser once if the previous
    /// instance died.
    pub async fn create_tab(&self, url: &str) -> Result<String> {
        let mut inner = self.inner.lock().await;
        for attempt in 0..2 {
            if inner.is_none() {
                *inner = Some(Browser::launch(&self.profile_dir).await?);
            }
            let browser = inner.as_ref().unwrap();
            match browser
                .cdp
                .call("Target.createTarget", json!({ "url": url }), None)
                .await
            {
                Ok(result) => {
                    let target_id = result
                        .get("targetId")
                        .and_then(Value::as_str)
                        .context("Target.createTarget returned no targetId")?;
                    return Ok(target_id.to_string());
                }
                Err(error) if attempt == 0 && !browser.cdp.is_alive() => {
                    let _ = error;
                    if let Some(mut browser) = inner.take() {
                        browser.kill().await;
                    }
                }
                Err(error) => return Err(error),
            }
        }
        bail!("failed to create a browser tab after relaunching the browser")
    }

    pub async fn close_tab(&self, target_id: &str) {
        let inner = self.inner.lock().await;
        if let Some(browser) = inner.as_ref() {
            let _ = browser
                .cdp
                .call("Target.closeTarget", json!({ "targetId": target_id }), None)
                .await;
        }
    }

    /// Reads `#output` / `#console_output` from a page that never reported,
    /// so a timed-out test still surfaces whatever the harness printed.
    pub async fn scrape_output(&self, target_id: &str) -> Result<(String, String)> {
        let inner = self.inner.lock().await;
        let browser = inner.as_ref().context("browser is not running")?;
        let session = browser
            .cdp
            .call(
                "Target.attachToTarget",
                json!({ "targetId": target_id, "flatten": true }),
                None,
            )
            .await?;
        let session_id = session
            .get("sessionId")
            .and_then(Value::as_str)
            .context("Target.attachToTarget returned no sessionId")?
            .to_string();

        let expression = "JSON.stringify({\
             output: (document.getElementById('output') || {}).textContent || '',\
             console_output: (document.getElementById('console_output') || {}).textContent || ''\
         })";
        let evaluated = browser
            .cdp
            .call(
                "Runtime.evaluate",
                json!({ "expression": expression, "returnByValue": true }),
                Some(&session_id),
            )
            .await?;
        let raw = evaluated
            .pointer("/result/value")
            .and_then(Value::as_str)
            .context("Runtime.evaluate returned no value")?;
        let parsed: Value = serde_json::from_str(raw)?;
        let output = parsed
            .get("output")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let console_output = parsed
            .get("console_output")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        Ok((output, console_output))
    }

    pub async fn kill(&self) {
        let mut inner = self.inner.lock().await;
        if let Some(mut browser) = inner.take() {
            browser.kill().await;
        }
    }
}

struct Browser {
    child: tokio::process::Child,
    cdp: CdpClient,
}

impl Browser {
    async fn launch(profile_dir: &PathBuf) -> Result<Browser> {
        let binary = find_browser()?;
        std::fs::create_dir_all(profile_dir)?;

        let mut command = tokio::process::Command::new(&binary);
        command
            .arg("--headless")
            .arg("--remote-debugging-port=0")
            .arg(format!("--user-data-dir={}", profile_dir.display()))
            .arg("--no-first-run")
            .arg("--no-default-browser-check")
            .arg("--disable-background-networking")
            .arg("--disable-gpu")
            .arg("--disable-dev-shm-usage")
            // Every run lives on its own origin, which would normally give
            // each test its own partition of the HTTP cache and compiled-code
            // cache and force the wasm module to be refetched and recompiled
            // per test. Assets are served from one fixed origin
            // (assets.localhost), and these flags let all test origins share
            // its cache entries. Site-isolation origin locks also key the
            // compiled-code cache, hence the last flag; test code is trusted,
            // so relaxing cross-tab process isolation is an acceptable trade.
            .arg("--disable-features=SplitCacheByNetworkIsolationKey,SplitCodeCacheByNetworkIsolationKey,SplitHttpCacheByNetworkIsolationKey")
            .arg("--disable-site-isolation-trials")
            // Only compile the wasm functions a test actually calls; a
            // single test typically touches a small fraction of a large
            // debug-build module.
            .arg("--js-flags=--wasm-lazy-compilation");

        let is_root = unsafe { libc::geteuid() } == 0;
        if is_root || std::env::var_os("WBG_POOL_NO_SANDBOX").is_some() {
            command.arg("--no-sandbox");
        }
        if let Ok(extra) = std::env::var("WBG_POOL_BROWSER_ARGS") {
            for arg in extra.split_whitespace() {
                command.arg(arg);
            }
        }
        command.arg("about:blank");

        command
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            // Chrome's renderers, GPU process and zygote are its children,
            // and a renderer wedged in a runaway test loop never services
            // the IPC teardown that is supposed to stop it. Putting the
            // browser in its own process group lets `kill` reach the whole
            // tree instead of leaving orphans behind burning cores.
            .process_group(0)
            .kill_on_drop(true);

        let mut child = command
            .spawn()
            .with_context(|| format!("failed to launch browser {}", binary.display()))?;
        let group = child.id().unwrap_or_default() as i32;

        let stderr = child.stderr.take().context("browser stderr not piped")?;
        let mut lines = tokio::io::BufReader::new(stderr).lines();
        // Keep the browser's own diagnostics so a failed launch reports why
        // Chrome exited (a missing library, a sandbox it could not enter, ...)
        // instead of just "exited before printing its DevTools address".
        let mut log = String::new();
        let ws_url = tokio::time::timeout(Duration::from_secs(30), async {
            while let Some(line) = lines.next_line().await? {
                if let Some(index) = line.find("DevTools listening on ") {
                    return Ok::<_, anyhow::Error>(
                        line[index + "DevTools listening on ".len()..]
                            .trim()
                            .to_string(),
                    );
                }
                log.push_str(&line);
                log.push('\n');
            }
            let detail = log.trim_end();
            if detail.is_empty() {
                bail!("browser exited before printing its DevTools address")
            }
            bail!("browser exited before printing its DevTools address:\n{detail}")
        })
        .await
        .context("timed out waiting for the browser DevTools address")??;

        // Keep draining stderr so the browser never blocks on a full pipe.
        tokio::spawn(async move { while lines.next_line().await.ok().flatten().is_some() {} });

        let cdp = CdpClient::connect(&ws_url).await?;
        // Only published once the browser is fully up: every earlier return
        // drops `child`, which kills it (`kill_on_drop`), and a stale group
        // recorded here could later name an unrelated recycled pid.
        BROWSER_GROUP.store(group, Ordering::SeqCst);
        Ok(Browser { child, cdp })
    }

    async fn kill(&mut self) {
        kill_browser_group();
        let _ = self.child.start_kill();
        // Reaping goes through the runtime's signal driver; if that is not
        // making progress the daemon must still be able to exit, so this
        // wait is bounded. The browser is already dead either way.
        let _ = tokio::time::timeout(Duration::from_secs(5), self.child.wait()).await;
    }
}

fn find_browser() -> Result<PathBuf> {
    for var in ["WBG_POOL_BROWSER", "CHROME"] {
        if let Ok(path) = std::env::var(var) {
            if !path.is_empty() {
                return Ok(PathBuf::from(path));
            }
        }
    }
    let candidates = [
        "chromium",
        "chromium-browser",
        "google-chrome",
        "google-chrome-stable",
        "chrome",
        "headless_shell",
    ];
    let path_var = std::env::var_os("PATH").unwrap_or_default();
    for dir in std::env::split_paths(&path_var) {
        for candidate in candidates {
            let full = dir.join(candidate);
            if full.is_file() {
                return Ok(full);
            }
        }
    }
    bail!(
        "no Chrome/Chromium binary found; set WBG_POOL_BROWSER or CHROME, \
         or put chromium/google-chrome on PATH"
    )
}

#[derive(Clone)]
struct CdpClient {
    tx: mpsc::Sender<CdpCall>,
    alive: Arc<AtomicBool>,
    next_id: Arc<AtomicU64>,
}

struct CdpCall {
    id: u64,
    method: String,
    params: Value,
    session_id: Option<String>,
    reply: oneshot::Sender<Result<Value>>,
}

impl CdpClient {
    async fn connect(ws_url: &str) -> Result<CdpClient> {
        let (stream, _) = tokio_tungstenite::connect_async(ws_url)
            .await
            .with_context(|| format!("failed to connect to DevTools at {ws_url}"))?;
        let (mut sink, mut source) = stream.split();

        let (tx, mut rx) = mpsc::channel::<CdpCall>(64);
        let alive = Arc::new(AtomicBool::new(true));

        let task_alive = alive.clone();
        tokio::spawn(async move {
            let mut pending: HashMap<u64, oneshot::Sender<Result<Value>>> = HashMap::new();
            loop {
                tokio::select! {
                    call = rx.recv() => {
                        let Some(call) = call else { break };
                        if !task_alive.load(Ordering::SeqCst) {
                            let _ = call.reply.send(Err(anyhow!("browser connection lost")));
                            continue;
                        }
                        let mut message = json!({
                            "id": call.id,
                            "method": call.method,
                            "params": call.params,
                        });
                        if let Some(session_id) = &call.session_id {
                            message["sessionId"] = json!(session_id);
                        }
                        match sink.send(tokio_tungstenite::tungstenite::Message::text(message.to_string())).await {
                            Ok(()) => { pending.insert(call.id, call.reply); }
                            Err(error) => {
                                task_alive.store(false, Ordering::SeqCst);
                                let _ = call.reply.send(Err(anyhow!("browser connection lost: {error}")));
                            }
                        }
                    }
                    incoming = source.next() => {
                        let text = match incoming {
                            Some(Ok(message)) => match message.into_text() {
                                Ok(text) => text,
                                Err(_) => continue,
                            },
                            // The browser went away. A closed WebSocketStream
                            // is fused: it answers every subsequent poll
                            // immediately, so the reader has to stop here
                            // rather than loop and burn a core forever.
                            _ => break,
                        };
                        let Ok(value) = serde_json::from_str::<Value>(text.as_str()) else { continue };
                        let Some(id) = value.get("id").and_then(Value::as_u64) else { continue };
                        if let Some(reply) = pending.remove(&id) {
                            let result = if let Some(error) = value.get("error") {
                                Err(anyhow!("CDP error: {error}"))
                            } else {
                                Ok(value.get("result").cloned().unwrap_or(Value::Null))
                            };
                            let _ = reply.send(result);
                        }
                    }
                }
            }
            task_alive.store(false, Ordering::SeqCst);
            for (_, reply) in pending.drain() {
                let _ = reply.send(Err(anyhow!("browser connection lost")));
            }
        });

        Ok(CdpClient {
            tx,
            alive,
            next_id: Arc::new(AtomicU64::new(1)),
        })
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    async fn call(&self, method: &str, params: Value, session_id: Option<&str>) -> Result<Value> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let call = CdpCall {
            id: self.next_id.fetch_add(1, Ordering::SeqCst),
            method: method.to_string(),
            params,
            session_id: session_id.map(str::to_string),
            reply: reply_tx,
        };
        self.tx
            .send(call)
            .await
            .map_err(|_| anyhow!("browser connection lost"))?;
        tokio::time::timeout(Duration::from_secs(30), reply_rx)
            .await
            .context("timed out waiting for the browser to answer a CDP call")?
            .map_err(|_| anyhow!("browser connection lost"))?
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// CPU time this process has burned, in milliseconds.
    fn cpu_millis() -> u128 {
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
        let millis = |time: libc::timeval| time.tv_sec as u128 * 1000 + time.tv_usec as u128 / 1000;
        millis(usage.ru_utime) + millis(usage.ru_stime)
    }

    /// Serves one WebSocket handshake and then drops the connection, the way
    /// the browser's DevTools endpoint goes away when Chrome exits.
    async fn closing_devtools_endpoint() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            drop(tokio_tungstenite::accept_async(stream).await.unwrap());
        });
        format!("ws://{addr}")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_reader_stops_when_the_browser_goes_away() {
        let client = CdpClient::connect(&closing_devtools_endpoint().await)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        // A closed WebSocketStream answers every poll immediately, so a
        // reader that keeps polling it pegs a core for as long as the daemon
        // lives -- which is until its idle timeout, or forever when the
        // spinning starves the shutdown path that killing the browser began.
        let before = cpu_millis();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let burned = cpu_millis() - before;
        assert!(
            burned < 100,
            "the CDP reader burned {burned}ms of CPU while idle after the browser exited"
        );

        assert!(!client.is_alive());
        assert!(client
            .call("Target.createTarget", json!({}), None)
            .await
            .is_err());
    }
}
