//! Virtual filesystem abstraction for cross-platform file operations.
//!
//! Provides a sandboxed filesystem API that works on both native (`tokio::fs`)
//! and web (Origin Private File System) targets. All paths are resolved
//! relative to a root directory and path traversal is prevented.

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod web;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use web::*;

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub use native::*;

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use tokio::io::AsyncWriteExt;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    use wasm_bindgen_test::wasm_bindgen_test_configure;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test_configure!(run_in_service_worker);

    #[cfg(not(target_arch = "wasm32"))]
    struct TestHarness {
        vfs: Vfs,
        _dir: tempfile::TempDir,
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn harness() -> TestHarness {
        let dir = tempfile::tempdir().unwrap();
        let vfs = Vfs::new(dir.path().to_path_buf());
        TestHarness { vfs, _dir: dir }
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    struct TestHarness {
        vfs: Vfs,
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    fn harness() -> TestHarness {
        let vfs = Vfs::new(format!("test/{}", ulid::Ulid::new()));
        TestHarness { vfs }
    }

    async fn collect_reader(mut reader: FileReader) -> Vec<u8> {
        let mut buf = Vec::new();
        while let Some(chunk) = reader.next().await {
            buf.extend_from_slice(&chunk.unwrap());
        }
        buf
    }

    #[dialog_common::test]
    async fn it_writes_and_reads_back() {
        let h = harness();
        let data = b"hello world";

        let mut w = h.vfs.write_to("test.txt").await.unwrap();
        w.write_all(data).await.unwrap();
        w.shutdown().await.unwrap();

        let reader = h.vfs.read_from("test.txt").await.unwrap();
        assert_eq!(collect_reader(reader).await, data);
    }

    #[dialog_common::test]
    async fn it_writes_to_nested_path() {
        let h = harness();
        let data = b"nested content";

        let mut w = h.vfs.write_to("a/b/c.txt").await.unwrap();
        w.write_all(data).await.unwrap();
        w.shutdown().await.unwrap();

        let reader = h.vfs.read_from("a/b/c.txt").await.unwrap();
        assert_eq!(collect_reader(reader).await, data);
    }

    #[dialog_common::test]
    async fn it_moves_a_file() {
        let h = harness();
        let data = b"move me";

        let mut w = h.vfs.write_to("src.txt").await.unwrap();
        w.write_all(data).await.unwrap();
        w.shutdown().await.unwrap();

        h.vfs.move_file("src.txt", "dst/moved.txt").await.unwrap();

        assert!(h.vfs.read_from("src.txt").await.is_err());

        let reader = h.vfs.read_from("dst/moved.txt").await.unwrap();
        assert_eq!(collect_reader(reader).await, data);
    }

    #[dialog_common::test]
    async fn it_rejects_path_traversal() {
        let h = harness();
        assert!(h.vfs.read_from("../../etc/passwd").await.is_err());
        assert!(h.vfs.write_to("../escape.txt").await.is_err());
    }

    #[dialog_common::test]
    async fn it_reads_nonexistent_file_as_not_found() {
        let h = harness();
        let err = h.vfs.read_from("no_such_file.bin").await.unwrap_err();
        assert!(
            matches!(err, crate::DialogBlobsError::NotFound(_)),
            "expected NotFound, got: {err:?}"
        );
    }
}
