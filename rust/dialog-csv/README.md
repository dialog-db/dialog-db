# dialog-csv

CSV import/export for Dialog artifacts.

Implements the `Exporter` and `Importer` traits from `dialog-artifacts`,
enabling artifacts to be serialized to and deserialized from CSV files.

## CSV format

Each row represents a single artifact with five columns:

| Column  | Description | Example |
|---------|-------------|---------|
| `the`   | Attribute (predicate) | `user/name` |
| `of`    | Entity (subject URI) | `user:alice` |
| `as`    | Value type | `text` |
| `is`    | Value | `Alice` |
| `cause` | Causal reference (base58, optional) | |

Supported value types: `text`, `natural`, `integer`, `boolean`,
`float`, `bytes` (base58), `entity` (URI), `attribute` (namespace/name),
`record` (base58).

## Usage

### Export a branch to a CSV file

```rs
let file = tokio::fs::File::create("artifacts.csv").await?;
branch
    .export(CsvExporter::from(file))
    .perform(&operator)
    .await?;
```

### Import a CSV file into a branch

```rs
let file = tokio::fs::File::open("artifacts.csv").await?;
branch
    .import(CsvImporter::from(file))
    .perform(&operator)
    .await?;
```

### Standalone usage without a repository

```rs
// Export
let mut exporter = CsvExporter::from(writer);
exporter.write(&artifact).await?;
exporter.close().await?;

// Import
let importer = CsvImporter::from(reader);
while let Some(result) = importer.next().await {
    let artifact = result?;
    println!("{artifact}");
}
```
