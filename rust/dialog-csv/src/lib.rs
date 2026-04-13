#![warn(missing_docs)]

//! CSV format support for the Dialog artifact exchange system.
//!
//! Provides [`CsvExporter`] and [`CsvImporter`] that implement the
//! [`Exporter`] and [`Importer`] traits from `dialog-artifacts`.
//!
//! Each CSV row represents a single artifact with columns:
//! `the` (attribute), `of` (entity), `as` (value type), `is` (value),
//! `cause` (optional causal reference).

mod row;

mod exporter;
pub use exporter::CsvExporter;

mod importer;
pub use importer::CsvImporter;

#[cfg(test)]
mod tests {
    use dialog_artifacts::Exporter;
    use dialog_artifacts::{Artifact, Cause, Value};
    use futures_util::StreamExt;
    use std::io::Cursor;

    use super::*;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_service_worker);

    fn test_artifacts() -> Vec<Artifact> {
        vec![
            Artifact {
                the: "user/name".parse().unwrap(),
                of: "user:alice".parse().unwrap(),
                is: Value::String("Alice".into()),
                cause: None,
            },
            Artifact {
                the: "user/email".parse().unwrap(),
                of: "user:alice".parse().unwrap(),
                is: Value::String("alice@example.com".into()),
                cause: None,
            },
            Artifact {
                the: "user/name".parse().unwrap(),
                of: "user:bob".parse().unwrap(),
                is: Value::String("Bob".into()),
                cause: None,
            },
        ]
    }

    async fn export_artifacts(artifacts: &[Artifact]) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut exporter = CsvExporter::from(&mut buf);
            for artifact in artifacts {
                exporter.write(artifact).await.unwrap();
            }
            exporter.close().await.unwrap();
        }
        buf
    }

    async fn import_artifacts(csv: Vec<u8>) -> Vec<Artifact> {
        CsvImporter::from(Cursor::new(csv))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    #[dialog_common::test]
    async fn it_roundtrips_artifacts() {
        let artifacts = test_artifacts();
        let csv = export_artifacts(&artifacts).await;
        let imported = import_artifacts(csv).await;

        assert_eq!(artifacts.len(), imported.len());
        for (original, imported) in artifacts.iter().zip(imported.iter()) {
            assert_eq!(original.the, imported.the);
            assert_eq!(original.of, imported.of);
            assert_eq!(original.is, imported.is);
            assert_eq!(original.cause, imported.cause);
        }
    }

    #[dialog_common::test]
    async fn it_produces_valid_csv() {
        let artifacts = test_artifacts();
        let csv = export_artifacts(&artifacts).await;
        let content = String::from_utf8(csv).unwrap();

        // Header + 3 data rows
        assert_eq!(content.lines().count(), 4);
        assert!(content.starts_with("the,of,as,is,cause\n"));
    }

    #[dialog_common::test]
    async fn it_exports_empty_input() {
        let csv = export_artifacts(&[]).await;
        let content = String::from_utf8(csv).unwrap();

        // Header only
        assert!(
            content.is_empty() || content.lines().count() <= 1,
            "empty export should have at most a header"
        );
    }

    #[dialog_common::test]
    async fn it_roundtrips_all_value_types() {
        let artifacts = vec![
            Artifact {
                the: "test/string".parse().unwrap(),
                of: "item:1".parse().unwrap(),
                is: Value::String("hello world".into()),
                cause: None,
            },
            Artifact {
                the: "test/uint".parse().unwrap(),
                of: "item:1".parse().unwrap(),
                is: Value::UnsignedInt(12345),
                cause: None,
            },
            Artifact {
                the: "test/sint".parse().unwrap(),
                of: "item:1".parse().unwrap(),
                is: Value::SignedInt(-42),
                cause: None,
            },
            Artifact {
                the: "test/bool".parse().unwrap(),
                of: "item:1".parse().unwrap(),
                is: Value::Boolean(false),
                cause: None,
            },
            Artifact {
                the: "test/float".parse().unwrap(),
                of: "item:1".parse().unwrap(),
                is: Value::Float(1.23),
                cause: None,
            },
            Artifact {
                the: "test/bytes".parse().unwrap(),
                of: "item:1".parse().unwrap(),
                is: Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]),
                cause: None,
            },
            Artifact {
                the: "test/entity".parse().unwrap(),
                of: "item:1".parse().unwrap(),
                is: Value::Entity("ref:other".parse().unwrap()),
                cause: None,
            },
            Artifact {
                the: "test/symbol".parse().unwrap(),
                of: "item:1".parse().unwrap(),
                is: Value::Symbol("meta/attribute".parse().unwrap()),
                cause: None,
            },
        ];

        let csv = export_artifacts(&artifacts).await;
        let imported = import_artifacts(csv).await;

        assert_eq!(artifacts.len(), imported.len());
        for (original, imported) in artifacts.iter().zip(imported.iter()) {
            assert_eq!(original.is, imported.is, "mismatch for {:?}", original.the);
        }
    }

    #[dialog_common::test]
    async fn it_roundtrips_cause() {
        let base = Artifact {
            the: "test/name".parse().unwrap(),
            of: "item:1".parse().unwrap(),
            is: Value::String("v1".into()),
            cause: None,
        };
        let cause = Cause::from(&base);
        let updated = Artifact {
            the: "test/name".parse().unwrap(),
            of: "item:1".parse().unwrap(),
            is: Value::String("v2".into()),
            cause: Some(cause.clone()),
        };

        let csv = export_artifacts(std::slice::from_ref(&updated)).await;
        let imported = import_artifacts(csv).await;

        assert_eq!(imported.len(), 1);
        assert_eq!(imported[0].cause, Some(cause));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn it_roundtrips_via_file() {
        let artifacts = test_artifacts();

        // Export to a temp file
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.csv");

        let file = tokio::fs::File::create(&path).await.unwrap();
        let mut exporter = CsvExporter::from(file);
        for artifact in &artifacts {
            exporter.write(artifact).await.unwrap();
        }
        exporter.close().await.unwrap();

        // Import from the file
        let file = tokio::fs::File::open(&path).await.unwrap();
        let importer = CsvImporter::from(file);
        let imported: Vec<Artifact> = importer
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(artifacts.len(), imported.len());
        for (original, imported) in artifacts.iter().zip(imported.iter()) {
            assert_eq!(original.the, imported.the);
            assert_eq!(original.of, imported.of);
            assert_eq!(original.is, imported.is);
        }
    }
}
