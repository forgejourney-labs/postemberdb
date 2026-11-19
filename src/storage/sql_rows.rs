//! Typed SQL row records stored in RecordStore-backed EPG1 pages.
//!
//! This is the page-backed row primitive for the SQL engine. It deliberately
//! accepts the table schema's column count from its owner; schema/catalog
//! persistence and Database integration are handled by the SQL storage layer.

use super::record_store::RecordStore;
use crate::{DbError, PageCacheStats, Value};
use std::path::Path;

const MAGIC: &[u8; 4] = b"SRW1";
const MAX_COLUMNS: usize = 1_000_000;
const MAX_TEXT_BYTES: usize = 64 * 1024 * 1024;
const MAX_ROW_BYTES: usize = 64 * 1024 * 1024;
const MAX_BATCH_ROWS: usize = 5_000;
const MAX_BATCH_BYTES: usize = 256 * 1024 * 1024;

/// Page-backed storage for one SQL table's rows.
///
/// Each stable row ID maps to a typed SRW1 record. RecordStore handles
/// multi-page values, checksums, bounded scans, and durable root publication.
pub struct SqlRowStore {
    records: RecordStore,
    column_count: usize,
}

impl SqlRowStore {
    /// Opens a table row store using the default 128-page cache.
    pub fn open(path: impl AsRef<Path>, column_count: usize) -> Result<Self, DbError> {
        Self::open_with_cache_capacity(path, column_count, 128)
    }

    /// Opens a table row store with an explicit decoded-page cache capacity.
    pub fn open_with_cache_capacity(
        path: impl AsRef<Path>,
        column_count: usize,
        cache_capacity_pages: usize,
    ) -> Result<Self, DbError> {
        if column_count == 0 || column_count > MAX_COLUMNS {
            return Err(DbError::Constraint(format!(
                "SQL row store column count must be in 1..={MAX_COLUMNS}"
            )));
        }
        Ok(Self {
            records: RecordStore::open_with_cache_capacity(path, cache_capacity_pages)?,
            column_count,
        })
    }

    /// Inserts or replaces one row using its stable table-local row ID.
    pub fn put(&mut self, row_id: u64, values: &[Value]) -> Result<(), DbError> {
        let encoded = self.encode_row(values)?;
        self.records.put(row_id, &encoded)
    }

    /// Inserts or replaces up to 5,000 rows with one descriptor-root publication.
    pub fn put_many<I>(&mut self, rows: I) -> Result<usize, DbError>
    where
        I: IntoIterator<Item = (u64, Vec<Value>)>,
    {
        let mut encoded = Vec::new();
        let mut total_bytes = 0usize;
        for (row_id, values) in rows {
            if encoded.len() == MAX_BATCH_ROWS {
                return Err(DbError::Constraint(format!(
                    "SQL row batch exceeds {MAX_BATCH_ROWS} rows"
                )));
            }
            let bytes = self.encode_row(&values)?;
            total_bytes = total_bytes.checked_add(bytes.len())
                .ok_or_else(|| DbError::Constraint("SQL row batch byte count overflow".into()))?;
            if total_bytes > MAX_BATCH_BYTES {
                return Err(DbError::Constraint(format!(
                    "SQL row batch exceeds the {MAX_BATCH_BYTES}-byte aggregate limit"
                )));
            }
            encoded.push((row_id, bytes));
        }
        self.records.put_many(encoded)
    }

    /// Applies typed row upserts and deletions with one descriptor-tree WAL
    /// frame and one atomic root publication. Upserts win for IDs present in
    /// both collections. The combined mutation count is capped at 5,000.
    pub fn apply_batch<U, D>(
        &mut self,
        rows: U,
        row_ids_to_delete: D,
    ) -> Result<(usize, usize), DbError>
    where
        U: IntoIterator<Item = (u64, Vec<Value>)>,
        D: IntoIterator<Item = u64>,
    {
        let mut encoded = Vec::new();
        let mut total_bytes = 0usize;
        for (row_id, values) in rows {
            if encoded.len() >= MAX_BATCH_ROWS {
                return Err(DbError::Constraint(format!(
                    "SQL row batch exceeds {MAX_BATCH_ROWS} mutations"
                )));
            }
            let bytes = self.encode_row(&values)?;
            total_bytes = total_bytes.checked_add(bytes.len())
                .ok_or_else(|| DbError::Constraint("SQL row batch byte count overflow".into()))?;
            if total_bytes > MAX_BATCH_BYTES {
                return Err(DbError::Constraint(format!(
                    "SQL row batch exceeds the {MAX_BATCH_BYTES}-byte aggregate limit"
                )));
            }
            encoded.push((row_id, bytes));
        }
        let mut deletions = Vec::new();
        for row_id in row_ids_to_delete {
            if encoded.len() + deletions.len() >= MAX_BATCH_ROWS {
                return Err(DbError::Constraint(format!(
                    "SQL row batch exceeds {MAX_BATCH_ROWS} mutations"
                )));
            }
            deletions.push(row_id);
        }
        self.records.apply_batch(encoded, deletions)
    }

    /// Reads one row by stable ID. Missing rows return None.
    pub fn get(&mut self, row_id: u64) -> Result<Option<Vec<Value>>, DbError> {
        self.records.get(row_id)?
            .map(|bytes| Self::decode_row(&bytes, self.column_count))
            .transpose()
    }

    /// Reads a bounded set of rows by stable ID.
    ///
    /// Duplicate IDs are read once; results preserve the first-occurrence input
    /// order and omit missing rows. The operation is capped at 5,000 distinct IDs
    /// and 256 MiB of encoded row payloads.
    pub fn get_many<I>(&mut self, row_ids: I) -> Result<Vec<(u64, Vec<Value>)>, DbError>
    where
        I: IntoIterator<Item = u64>,
    {
        let mut seen = std::collections::BTreeSet::new();
        let mut output = Vec::new();
        let mut total_bytes = 0usize;
        for row_id in row_ids {
            if !seen.insert(row_id) { continue; }
            if seen.len() > MAX_BATCH_ROWS {
                return Err(DbError::Constraint(format!(
                    "SQL row read batch exceeds {MAX_BATCH_ROWS} distinct IDs"
                )));
            }
            if let Some(bytes) = self.records.get(row_id)? {
                total_bytes = total_bytes.checked_add(bytes.len())
                    .ok_or_else(|| DbError::Constraint("SQL row read byte count overflow".into()))?;
                if total_bytes > MAX_BATCH_BYTES {
                    return Err(DbError::Constraint(format!(
                        "SQL row read batch exceeds the {MAX_BATCH_BYTES}-byte aggregate limit"
                    )));
                }
                output.push((row_id, Self::decode_row(&bytes, self.column_count)?));
            }
        }
        Ok(output)
    }
    /// Returns whether a row ID exists.
    pub fn contains(&mut self, row_id: u64) -> Result<bool, DbError> {
        self.records.contains(row_id)
    }

    /// Returns stable row IDs in ascending order without decoding row payloads.
    pub fn scan_ids(&mut self, after: Option<u64>, limit: usize) -> Result<Vec<u64>, DbError> {
        self.records.scan_ids(after, limit)
    }

    /// Reads a bounded page of rows in stable-ID order.
    ///
    /// The underlying RecordStore caps a page at 5,000 rows and 256 MiB of
    /// encoded payload. Use the last returned ID as the next cursor.
    pub fn scan_rows(
        &mut self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<(u64, Vec<Value>)>, DbError> {
        self.records.scan_records(after, limit)?
            .into_iter()
            .map(|(row_id, bytes)| Ok((row_id, Self::decode_row(&bytes, self.column_count)?)))
            .collect()
    }

    /// Deletes one row and returns whether it existed.
    pub fn delete(&mut self, row_id: u64) -> Result<bool, DbError> {
        self.records.delete(row_id)
    }

    /// Deletes a batch of rows through one descriptor-root publication.
    pub fn delete_many<I>(&mut self, row_ids: I) -> Result<usize, DbError>
    where
        I: IntoIterator<Item = u64>,
    {
        self.records.delete_many(row_ids)
    }

    /// Returns page-cache counters for this table's row pages and descriptors.
    pub fn page_cache_stats(&self) -> PageCacheStats {
        self.records.page_cache_stats()
    }

    /// Changes the bounded page-cache capacity.
    pub fn set_page_cache_capacity(&mut self, capacity_pages: usize) -> Result<(), DbError> {
        self.records.set_page_cache_capacity(capacity_pages)
    }

    fn encode_row(&self, values: &[Value]) -> Result<Vec<u8>, DbError> {
        Self::encode_values(values, self.column_count)
    }

    /// Encodes a typed SQL row independently of a RecordStore handle so it can
    /// be reused by the explicit PDB5-to-PDB6 migration path.
    pub(crate) fn encode_values(values: &[Value], expected_columns: usize) -> Result<Vec<u8>, DbError> {
        if values.len() != expected_columns {
            return Err(DbError::Constraint(format!(
                "row has {} values; table schema requires {}",
                values.len(), expected_columns
            )));
        }
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&(values.len() as u32).to_le_bytes());
        for value in values {
            let additional = match value {
                Value::Null => 1,
                Value::Integer(_) => 9,
                Value::Text(value) => {
                    if value.len() > MAX_TEXT_BYTES {
                        return Err(DbError::Constraint("SQL text value exceeds 64 MiB".into()));
                    }
                    5usize.checked_add(value.len())
                        .ok_or_else(|| DbError::Constraint("SQL text length overflow".into()))?
                }
                Value::Boolean(_) => 2,
            };
            if out.len().checked_add(additional).is_none_or(|length| length > MAX_ROW_BYTES) {
                return Err(DbError::Constraint("encoded SQL row exceeds 64 MiB".into()));
            }
            match value {
                Value::Null => out.push(0),
                Value::Integer(value) => {
                    out.push(1);
                    out.extend_from_slice(&value.to_le_bytes());
                }
                Value::Text(value) => {
                    out.push(2);
                    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
                    out.extend_from_slice(value.as_bytes());
                }
                Value::Boolean(value) => {
                    out.push(3);
                    out.push(u8::from(*value));
                }
            }
        }
        Ok(out)
    }

    fn decode_row(bytes: &[u8], expected_columns: usize) -> Result<Vec<Value>, DbError> {
        Self::decode_values(bytes, expected_columns)
    }

    /// Decodes a typed SQL row independently of a RecordStore handle.
    pub(crate) fn decode_values(bytes: &[u8], expected_columns: usize) -> Result<Vec<Value>, DbError> {
        let mut input = RowDecoder { bytes, offset: 0 };
        if input.take(4)? != MAGIC {
            return Err(DbError::Storage("invalid SQL row record magic".into()));
        }
        let count = u32::from_le_bytes(input.take(4)?.try_into().unwrap()) as usize;
        if count != expected_columns || count == 0 || count > MAX_COLUMNS {
            return Err(DbError::Storage("SQL row record does not match the table schema".into()));
        }
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let value = match input.byte()? {
                0 => Value::Null,
                1 => Value::Integer(i64::from_le_bytes(input.take(8)?.try_into().unwrap())),
                2 => {
                    let length = u32::from_le_bytes(input.take(4)?.try_into().unwrap()) as usize;
                    if length > MAX_TEXT_BYTES {
                        return Err(DbError::Storage("SQL text value exceeds 64 MiB".into()));
                    }
                    let value = std::str::from_utf8(input.take(length)?)
                        .map_err(|_| DbError::Storage("SQL row contains invalid UTF-8".into()))?;
                    Value::Text(value.to_owned())
                }
                3 => match input.byte()? {
                    0 => Value::Boolean(false),
                    1 => Value::Boolean(true),
                    _ => return Err(DbError::Storage("invalid SQL boolean encoding".into())),
                },
                _ => return Err(DbError::Storage("unknown SQL row value tag".into())),
            };
            values.push(value);
        }
        if input.offset != bytes.len() {
            return Err(DbError::Storage("trailing bytes in SQL row record".into()));
        }
        Ok(values)
    }
}

struct RowDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> RowDecoder<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], DbError> {
        let end = self.offset.checked_add(count)
            .ok_or_else(|| DbError::Storage("SQL row record offset overflow".into()))?;
        if end > self.bytes.len() {
            return Err(DbError::Storage("truncated SQL row record".into()));
        }
        let bytes = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn byte(&mut self) -> Result<u8, DbError> {
        Ok(self.take(1)?[0])
    }
}

#[cfg(test)]
mod tests {
    use super::SqlRowStore;
    use crate::Value;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "postemberdb-sql-row-{label}-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ))
    }

    fn cleanup(path: &PathBuf) {
        for suffix in [".lock", ".pages", ".pages.wal"] {
            let mut candidate = path.as_os_str().to_os_string();
            candidate.push(suffix);
            let _ = fs::remove_file(PathBuf::from(candidate));
        }
    }

    #[test]
    fn typed_sql_rows_round_trip_and_scan_across_reopen() {
        let path = temp_path("round-trip");
        {
            let mut rows = SqlRowStore::open(&path, 4).unwrap();
            rows.put_many(vec![
                (3, vec![Value::Integer(3), Value::Text("three".into()), Value::Boolean(true), Value::Null]),
                (1, vec![Value::Integer(1), Value::Text("one".into()), Value::Boolean(false), Value::Null]),
                (2, vec![Value::Integer(2), Value::Text("two".into()), Value::Boolean(true), Value::Null]),
            ]).unwrap();
            assert_eq!(rows.scan_ids(None, 2).unwrap(), vec![1, 2]);
            assert_eq!(
                rows.scan_rows(Some(1), 2).unwrap(),
                vec![
                    (2, vec![Value::Integer(2), Value::Text("two".into()), Value::Boolean(true), Value::Null]),
                    (3, vec![Value::Integer(3), Value::Text("three".into()), Value::Boolean(true), Value::Null]),
                ]
            );
            rows.put(2, &[Value::Integer(22), Value::Text("updated".into()), Value::Boolean(false), Value::Null]).unwrap();
        }
        let mut reopened = SqlRowStore::open(&path, 4).unwrap();
        assert_eq!(
            reopened.get(2).unwrap(),
            Some(vec![Value::Integer(22), Value::Text("updated".into()), Value::Boolean(false), Value::Null])
        );
        assert!(reopened.delete(1).unwrap());
        assert_eq!(reopened.scan_ids(None, 10).unwrap(), vec![2, 3]);
        drop(reopened);
        cleanup(&path);
    }

    #[test]
    fn get_many_is_bounded_deduplicated_and_preserves_input_order() {
        let path = temp_path("get-many");
        let mut rows = SqlRowStore::open(&path, 1).unwrap();
        rows.put_many(vec![
            (1, vec![Value::Integer(10)]),
            (2, vec![Value::Integer(20)]),
            (3, vec![Value::Integer(30)]),
        ]).unwrap();
        assert_eq!(rows.get_many(vec![3, 1, 3, 99, 2]).unwrap(), vec![
            (3, vec![Value::Integer(30)]),
            (1, vec![Value::Integer(10)]),
            (2, vec![Value::Integer(20)]),
        ]);
        assert!(rows.get_many(1..=5_001).is_err());
        assert_eq!(rows.scan_ids(None, 10).unwrap(), vec![1, 2, 3]);
        drop(rows);
        cleanup(&path);
    }
    #[test]
    fn sql_row_store_applies_mixed_upserts_and_deletes_atomically() {
        let path = temp_path("mixed-batch");
        {
            let mut rows = SqlRowStore::open(&path, 1).unwrap();
            rows.put_many(vec![
                (1, vec![Value::Integer(10)]),
                (2, vec![Value::Integer(20)]),
                (3, vec![Value::Integer(30)]),
            ]).unwrap();

            let (inserted, removed) = rows.apply_batch(
                vec![
                    (2, vec![Value::Integer(200)]),
                    (4, vec![Value::Integer(40)]),
                ],
                vec![1, 2, 3],
            ).unwrap();
            assert_eq!(inserted, 2);
            assert_eq!(removed, 2);
            assert_eq!(rows.scan_ids(None, 10).unwrap(), vec![2, 4]);
            assert_eq!(rows.get(2).unwrap(), Some(vec![Value::Integer(200)]));
            assert_eq!(rows.get(4).unwrap(), Some(vec![Value::Integer(40)]));
        }
        let mut reopened = SqlRowStore::open(&path, 1).unwrap();
        assert_eq!(reopened.scan_ids(None, 10).unwrap(), vec![2, 4]);
        assert_eq!(reopened.get(2).unwrap(), Some(vec![Value::Integer(200)]));
        drop(reopened);
        cleanup(&path);
    }

    #[test]
    fn sql_row_store_rejects_mixed_batch_over_limit_before_mutation() {
        let path = temp_path("mixed-limit");
        let mut rows = SqlRowStore::open(&path, 1).unwrap();
        rows.put(1, &[Value::Integer(1)]).unwrap();
        let upserts = (2..=2_501u64)
            .map(|id| (id, vec![Value::Integer(id as i64)]))
            .collect::<Vec<_>>();
        let deletes = (3_000..=5_500u64).collect::<Vec<_>>();
        assert!(rows.apply_batch(upserts, deletes).is_err());
        assert_eq!(rows.scan_ids(None, 10).unwrap(), vec![1]);
        drop(rows);
        cleanup(&path);
    }

    #[test]
    fn sql_row_store_supports_multi_page_text_and_rejects_wrong_width() {
        let path = temp_path("large");
        let mut rows = SqlRowStore::open(&path, 1).unwrap();
        let large = "x".repeat(150_000);
        rows.put(1, &[Value::Text(large.clone())]).unwrap();
        assert_eq!(rows.get(1).unwrap(), Some(vec![Value::Text(large)]));
        assert!(rows.put(2, &[Value::Integer(1), Value::Integer(2)]).is_err());
        drop(rows);
        cleanup(&path);
    }

    #[test]
    fn sql_row_store_rejects_oversized_batches_before_writing() {
        let path = temp_path("batch-limit");
        let mut rows = SqlRowStore::open(&path, 1).unwrap();
        let batch = (1..=5_001u64)
            .map(|row_id| (row_id, vec![Value::Integer(row_id as i64)]))
            .collect::<Vec<_>>();
        assert!(rows.put_many(batch).is_err());
        assert!(rows.scan_ids(None, 10).unwrap().is_empty());
        drop(rows);
        cleanup(&path);
    }

    #[test]
    fn sql_row_decoder_rejects_truncated_and_trailing_data() {
        assert!(SqlRowStore::decode_row(b"SRW1", 1).is_err());
        let mut bytes = b"SRW1".to_vec();
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.push(0);
        bytes.push(0xAA);
        assert!(SqlRowStore::decode_row(&bytes, 1).is_err());
    }
}
