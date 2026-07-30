//! HTTP surface of the daemon: the control API used by runner shims, plus
//! the per-run pages and per-binary assets served to the browser.

use super::{codegen, harness, Daemon};
use crate::protocol::{RunReport, RunRequest};
use crate::suite::{self, TestMode};
use anyhow::{Context, Result};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

const INDEX_TEMPLATE: &str = include_str!("index.html");

pub struct RunSlot {
    pub index_html: String,
    pub run_js: String,
    pub worker_js: Option<String>,
    pub report_tx: Option<oneshot::Sender<PageReport>>,
}

#[derive(Debug, Deserialize)]
pub struct PageReport {
    pub ok: bool,
    #[serde(default)]
    pub output: String,
    #[serde(default)]
    pub console_output: String,
}

pub fn router(daemon: Arc<Daemon>) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/run", post(run))
        .route("/api/shutdown", post(shutdown))
        .route("/r/{run_id}/", get(run_index))
        .route("/r/{run_id}/run.js", get(run_js))
        .route("/r/{run_id}/worker.js", get(run_worker_js))
        .route("/r/{run_id}/service.js", get(run_worker_js))
        .route("/r/{run_id}/report", post(run_report))
        .route("/__wasm_bindgen/coverage", post(coverage))
        .route("/a/{binary_id}/{*path}", get(asset))
        .layer(DefaultBodyLimit::max(256 * 1024 * 1024))
        .layer(axum::middleware::map_response(isolate_origin_headers))
        .with_state(daemon)
}

/// COOP/COEP headers matching wasm-bindgen-test-runner's defaults, required
/// for tests that use SharedArrayBuffer and friends. Opt out with
/// WASM_BINDGEN_TEST_NO_ORIGIN_ISOLATION, same as the stock runner.
async fn isolate_origin_headers(mut response: Response) -> Response {
    if std::env::var_os("WASM_BINDGEN_TEST_NO_ORIGIN_ISOLATION").is_none() {
        response.headers_mut().insert(
            "Cross-Origin-Opener-Policy",
            HeaderValue::from_static("same-origin"),
        );
        response.headers_mut().insert(
            "Cross-Origin-Embedder-Policy",
            HeaderValue::from_static("require-corp"),
        );
    }
    response
}

async fn health() -> String {
    format!("wbg-pool {}", env!("CARGO_PKG_VERSION"))
}

async fn shutdown(State(daemon): State<Arc<Daemon>>) -> &'static str {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        super::shutdown(&daemon).await;
    });
    "shutting down"
}

async fn coverage() -> StatusCode {
    // Coverage collection is not supported yet; acknowledge the dump so the
    // page does not error, and rely on the harness output for results.
    StatusCode::NO_CONTENT
}

struct ActiveGuard(Arc<Daemon>);

impl ActiveGuard {
    fn new(daemon: &Arc<Daemon>) -> ActiveGuard {
        daemon.touch();
        daemon.active.fetch_add(1, Ordering::SeqCst);
        ActiveGuard(daemon.clone())
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        self.0.touch();
    }
}

async fn run(State(daemon): State<Arc<Daemon>>, Json(request): Json<RunRequest>) -> Response {
    let _guard = ActiveGuard::new(&daemon);
    match execute_run(&daemon, request).await {
        Ok(report) => Json(report).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")).into_response(),
    }
}

async fn execute_run(daemon: &Arc<Daemon>, request: RunRequest) -> Result<RunReport> {
    let binary = ensure_binary(daemon, &request).await?;

    let mode = binary.suite.effective_mode();
    if matches!(mode, TestMode::Node | TestMode::Emscripten) {
        anyhow::bail!(
            "wbg-pool only runs browser-family tests, but {} is configured for {} \
             (the wbg-pool shim delegates such binaries to wasm-bindgen-test-runner)",
            request.binary.display(),
            mode.describe()
        );
    }

    if binary.suite.tests.is_empty() {
        return Ok(RunReport {
            ok: true,
            output: "no tests to run!\n".to_string(),
            console_output: String::new(),
            timed_out: false,
        });
    }

    let filtered = suite::filter(&binary.suite.tests, &request.args);
    let exports: Vec<&str> = filtered
        .to_run
        .iter()
        .map(|test| test.export.as_str())
        .collect();

    let run_id = daemon.run_counter.fetch_add(1, Ordering::SeqCst);
    let scripts = harness::generate(&harness::RunConfig {
        mode,
        module_js: &format!("/a/{}/{}.js", binary.id, codegen::MODULE_NAME),
        module_wasm: &format!("/a/{}/{}_bg.wasm", binary.id, codegen::MODULE_NAME),
        exports: &exports,
        include_ignored: request.args.include_ignored,
        filtered_count: filtered.filtered,
        nocapture: request.nocapture,
    })?;
    let index_html = INDEX_TEMPLATE.replace(
        "/* {NOCAPTURE} */ false",
        if request.nocapture { "true" } else { "false" },
    );

    let (report_tx, report_rx) = oneshot::channel();
    daemon.runs.lock().unwrap().insert(
        run_id,
        RunSlot {
            index_html,
            run_js: scripts.run_js,
            worker_js: scripts.worker_js,
            report_tx: Some(report_tx),
        },
    );

    // A unique loopback host per run: every test gets its own web origin,
    // and with it fresh IndexedDB, OPFS, localStorage, caches and cookies.
    let url = format!("http://t-{run_id}.localhost:{}/r/{run_id}/", daemon.port);

    let target = match daemon.browser.create_tab(&url).await {
        Ok(target) => target,
        Err(error) => {
            daemon.runs.lock().unwrap().remove(&run_id);
            return Err(error).context("failed to open a browser tab");
        }
    };

    let timeout = Duration::from_secs(request.timeout_secs.max(1));
    let report = match tokio::time::timeout(timeout, report_rx).await {
        Ok(Ok(page)) => RunReport {
            ok: page.ok,
            output: page.output,
            console_output: page.console_output,
            timed_out: false,
        },
        Ok(Err(_)) => RunReport {
            ok: false,
            output: "wbg-pool daemon dropped the test run unexpectedly\n".to_string(),
            console_output: String::new(),
            timed_out: false,
        },
        Err(_) => {
            let (mut output, console_output) = daemon
                .browser
                .scrape_output(&target)
                .await
                .unwrap_or_default();
            output.push_str(&format!(
                "\ntest run timed out after {}s (set WASM_BINDGEN_TEST_TIMEOUT to change)\n",
                request.timeout_secs
            ));
            RunReport {
                ok: false,
                output,
                console_output,
                timed_out: true,
            }
        }
    };

    daemon.browser.close_tab(&target).await;
    daemon.runs.lock().unwrap().remove(&run_id);
    daemon.touch();

    Ok(report)
}

async fn ensure_binary(daemon: &Arc<Daemon>, request: &RunRequest) -> Result<Arc<codegen::Binary>> {
    let canonical = std::fs::canonicalize(&request.binary)
        .with_context(|| format!("failed to resolve {}", request.binary.display()))?;
    let fingerprint = codegen::fingerprint(&canonical)?;

    let cell = {
        let mut binaries = daemon.binaries.lock().await;
        binaries
            .entry((canonical.clone(), fingerprint))
            .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
            .clone()
    };

    let out_dir = daemon.work_dir.join("bins");
    let binary = cell
        .get_or_try_init(|| async {
            let path = canonical.clone();
            let out_dir = out_dir.clone();
            let generated = tokio::task::spawn_blocking(move || codegen::generate(&path, &out_dir))
                .await
                .context("codegen task panicked")??;
            Ok::<_, anyhow::Error>(Arc::new(generated))
        })
        .await?
        .clone();
    Ok(binary)
}

async fn run_index(State(daemon): State<Arc<Daemon>>, Path(run_id): Path<u64>) -> Response {
    let html = daemon
        .runs
        .lock()
        .unwrap()
        .get(&run_id)
        .map(|slot| slot.index_html.clone());
    match html {
        Some(html) => ([(header::CONTENT_TYPE, "text/html")], html).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn run_js(State(daemon): State<Arc<Daemon>>, Path(run_id): Path<u64>) -> Response {
    let js = daemon
        .runs
        .lock()
        .unwrap()
        .get(&run_id)
        .map(|slot| slot.run_js.clone());
    match js {
        Some(js) => ([(header::CONTENT_TYPE, "text/javascript")], js).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn run_worker_js(State(daemon): State<Arc<Daemon>>, Path(run_id): Path<u64>) -> Response {
    let js = daemon
        .runs
        .lock()
        .unwrap()
        .get(&run_id)
        .and_then(|slot| slot.worker_js.clone());
    match js {
        Some(js) => ([(header::CONTENT_TYPE, "text/javascript")], js).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn run_report(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<u64>,
    Json(report): Json<PageReport>,
) -> StatusCode {
    let sender = daemon
        .runs
        .lock()
        .unwrap()
        .get_mut(&run_id)
        .and_then(|slot| slot.report_tx.take());
    match sender {
        Some(sender) => {
            let _ = sender.send(report);
            StatusCode::NO_CONTENT
        }
        None => StatusCode::NOT_FOUND,
    }
}

async fn asset(
    State(daemon): State<Arc<Daemon>>,
    Path((binary_id, path)): Path<(String, String)>,
) -> Response {
    if binary_id.contains(['/', '\\', '.']) || path.split('/').any(|part| part == "..") {
        return StatusCode::NOT_FOUND.into_response();
    }
    let full = daemon.work_dir.join("bins").join(&binary_id).join(&path);
    let bytes = match tokio::fs::read(&full).await {
        Ok(bytes) => bytes,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    let content_type = match full.extension().and_then(|ext| ext.to_str()) {
        Some("js") | Some("mjs") => "text/javascript",
        Some("wasm") => "application/wasm",
        Some("html") => "text/html",
        Some("json") | Some("map") => "application/json",
        _ => "application/octet-stream",
    };
    ([(header::CONTENT_TYPE, content_type)], bytes).into_response()
}
