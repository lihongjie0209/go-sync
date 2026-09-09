# Embedding referenced files

Any supported source can turn explicitly selected path columns into embedded Base64 content. The original path remains available:

```json
"file_columns": [
  {
    "schema": "sales",
    "table": "documents",
    "column": "file_path",
    "root_dir": "D:\\pms\\uploads",
    "max_bytes": 16777216
  }
]
```

For an original value such as `contracts/a.pdf`, the emitted column is shaped like:

```json
{
  "name": "file_path",
  "type": "varchar(500)",
  "value": "JVBERi0xLjQK...",
  "encoding": "base64",
  "source_value": "contracts/a.pdf"
}
```

`root_dir` must be absolute on the collector machine. Relative database values are resolved below it; absolute values are accepted only when they remain below it. OS-rooted file access rejects `..` and symbolic-link escapes. Only regular files up to `max_bytes` are read. NULL remains NULL. Missing, unreadable, oversized or unsafe references stop capture without advancing the source checkpoint.

Transformation happens before a complete source transaction is published to the durable queue. Once published, retries use the stored Base64 content and do not reread the file. A database transaction and an external filesystem update cannot be made atomic: the application writing the database row must finish and durably close the referenced file before committing that row.

Base64 expands data by roughly one third. Set `max_row_bytes`, `batch_bytes`, queue capacity and HTTP receiver limits accordingly. File columns are embedded in row `columns`; identity keys keep their original database path so UPDATE/DELETE matching remains stable. DELETE messages that contain only keys do not reread deleted files.
