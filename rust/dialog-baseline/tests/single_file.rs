#![cfg(not(target_arch = "wasm32"))]
// Exercise the exact provider-switching harness used for the measurements.
#[allow(dead_code)]
#[path = "../examples/single_file.rs"]
mod measurement;

#[tokio::test]
async fn repository_commits_cold_scans_and_historical_roots() -> anyhow::Result<()> {
    measurement::native::check_repository_roundtrip(false).await
}

#[tokio::test]
async fn wide_workload_reads_written_body_values() -> anyhow::Result<()> {
    measurement::native::check_repository_roundtrip(true).await
}
