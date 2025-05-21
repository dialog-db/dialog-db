use std::sync::Arc;

use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    handler::Handler,
    routing::{get, get_service},
};
use tower_service::Service;
use worker::{Context, Env, HttpRequest, kv::KvStore};

fn router(kv: Arc<KvStore>) -> Router {
    Router::new().route("/", get(root)).route(
        "/block/{key}",
        get_service(get_block.with_state(kv.clone())).post_service(put_block.with_state(kv)),
    )
}

#[worker::event(fetch)]
async fn fetch(
    req: HttpRequest,
    env: Env,
    _ctx: Context,
) -> worker::Result<axum::http::Response<axum::body::Body>> {
    let kv = Arc::new(env.kv("dialog_db_demo")?);

    console_error_panic_hook::set_once();
    Ok(router(kv).call(req).await?)
}

pub async fn root() -> &'static str {
    "Hello Axum!"
}

// use axum::debug_handler;

#[worker::send]
pub async fn get_block(Path(key): Path<String>, State(kv): State<Arc<KvStore>>) -> Vec<u8> {
    kv.get(&key)
        .bytes()
        .await
        .ok()
        .and_then(|bytes| bytes)
        .unwrap_or_default()
}

pub async fn put_block(
    Path(key): Path<String>,
    State(kv): State<Arc<KvStore>>,
    value: Bytes,
) -> () {
    kv.put_bytes(&key, value.as_ref()).unwrap();
}
