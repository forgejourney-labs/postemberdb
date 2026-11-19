//! PDB6 shared SQL keyspace and strict catalog metadata codec.
//!
//! These codecs are not yet wired into Database. PDB5 remains authoritative
//! until one WAL/root-publication protocol covers catalog and row changes.

use crate::{DataType, DbError};

const CATALOG_MAGIC: &[u8; 4] = b"PCT1";
const KEYSPACE_VERSION: u8 = 1;
const MAX_NAME_BYTES: usize = 4 * 1024;
const MAX_COLUMNS: usize = 65_536;
const MAX_INDEXES: usize = 65_536;
const MAX_KEY_BYTES: usize = 1024;

pub(crate) const KEY_TABLE: u8 = 0x10;
pub(crate) const KEY_ROW: u8 = 0x20;
pub(crate) const KEY_INDEX: u8 = 0x30;
pub(crate) const KEY_COUNTERS: u8 = 0x40;
pub(crate) const KEY_DATABASE_ROOT: u8 = 0x7f;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogIndex {
    pub name: String,
    pub columns: Vec<u32>,
    pub unique: bool,
}

/// Compact database-wide metadata stored at the reserved root key.
/// The physical EmberTree root remains in EAM3; this manifest versions the
/// logical SQL catalog visible through that physical root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DatabaseManifest {
    pub generation: u64,
    pub last_tx_id: u64,
    pub next_table_id: u64,
    pub table_count: u32,
}

impl DatabaseManifest {
    pub(crate) fn encode(self) -> Result<Vec<u8>, DbError> {
        if self.next_table_id == 0 || self.table_count > 1_000_000 {
            return Err(DbError::Constraint("invalid PDB6 database manifest counters".into()));
        }
        let mut out = Vec::with_capacity(36);
        out.extend_from_slice(b"PDM1");
        out.push(KEYSPACE_VERSION);
        out.extend_from_slice(&[0, 0, 0]);
        out.extend_from_slice(&self.generation.to_le_bytes());
        out.extend_from_slice(&self.last_tx_id.to_le_bytes());
        out.extend_from_slice(&self.next_table_id.to_le_bytes());
        out.extend_from_slice(&self.table_count.to_le_bytes());
        Ok(out)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        if bytes.len() != 36 || &bytes[..4] != b"PDM1" || bytes[4] != KEYSPACE_VERSION
            || bytes[5..8] != [0, 0, 0]
        {
            return Err(DbError::Storage("invalid PDB6 database manifest header or length".into()));
        }
        let manifest = Self {
            generation: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            last_tx_id: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            next_table_id: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            table_count: u32::from_le_bytes(bytes[32..36].try_into().unwrap()),
        };
        if manifest.next_table_id == 0 || manifest.table_count > 1_000_000 {
            return Err(DbError::Storage("invalid PDB6 database manifest counters".into()));
        }
        Ok(manifest)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogTable {
    pub table_id: u64,
    pub name: String,
    pub columns: Vec<CatalogColumn>,
    pub indexes: Vec<CatalogIndex>,
    pub next_row_id: u64,
}

pub(crate) fn table_key(table_id: u64) -> Result<Vec<u8>, DbError> {
    checked_id(table_id, "table")?;
    Ok(prefixed_id(KEY_TABLE, table_id))
}

pub(crate) fn row_key(table_id: u64, row_id: u64) -> Result<Vec<u8>, DbError> {
    checked_id(table_id, "table")?;
    checked_id(row_id, "row")?;
    let mut key = prefixed_id(KEY_ROW, table_id);
    key.extend_from_slice(&row_id.to_be_bytes());
    Ok(key)
}

pub(crate) fn index_prefix(table_id: u64, index_id: u64) -> Result<Vec<u8>, DbError> {
    checked_id(table_id, "table")?;
    checked_id(index_id, "index")?;
    let mut key = prefixed_id(KEY_INDEX, table_id);
    key.extend_from_slice(&index_id.to_be_bytes());
    Ok(key)
}

pub(crate) fn index_key(table_id: u64, index_id: u64, encoded_value: &[u8]) -> Result<Vec<u8>, DbError> {
    let mut key = index_prefix(table_id, index_id)?;
    key.extend_from_slice(encoded_value);
    if key.len() > MAX_KEY_BYTES {
        return Err(DbError::Constraint("PDB6 index key exceeds the 1,024-byte B+ tree key limit".into()));
    }
    Ok(key)
}

pub(crate) fn counters_key(table_id: u64) -> Result<Vec<u8>, DbError> {
    checked_id(table_id, "table")?;
    Ok(prefixed_id(KEY_COUNTERS, table_id))
}

pub(crate) fn database_root_key() -> Vec<u8> {
    vec![KEY_DATABASE_ROOT, KEYSPACE_VERSION]
}

fn checked_id(id: u64, kind: &str) -> Result<(), DbError> {
    if id == 0 {
        return Err(DbError::Constraint(format!("PDB6 {kind} ID zero is reserved")));
    }
    Ok(())
}

fn prefixed_id(prefix: u8, id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.push(prefix);
    key.extend_from_slice(&id.to_be_bytes());
    key
}

impl CatalogTable {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, DbError> {
        validate_table(self).map_err(DbError::Constraint)?;
        let mut out = Vec::new();
        out.extend_from_slice(CATALOG_MAGIC);
        out.push(KEYSPACE_VERSION);
        out.extend_from_slice(&self.table_id.to_le_bytes());
        put_string(&mut out, &self.name)?;
        put_u32(&mut out, self.columns.len())?;
        for column in &self.columns {
            put_string(&mut out, &column.name)?;
            out.push(match column.data_type {
                DataType::Integer => 1,
                DataType::Text => 2,
                DataType::Boolean => 3,
            });
            out.push(u8::from(column.nullable));
        }
        put_u32(&mut out, self.indexes.len())?;
        for index in &self.indexes {
            put_string(&mut out, &index.name)?;
            out.push(u8::from(index.unique));
            put_u32(&mut out, index.columns.len())?;
            for column in &index.columns {
                out.extend_from_slice(&column.to_le_bytes());
            }
        }
        out.extend_from_slice(&self.next_row_id.to_le_bytes());
        if out.len() > super::extent::page_payload_size() {
            return Err(DbError::Constraint("PDB6 catalog record exceeds one B+ tree value page".into()));
        }
        Ok(out)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        if bytes.len() > super::extent::page_payload_size() {
            return Err(DbError::Storage("PDB6 catalog record exceeds one page".into()));
        }
        let mut input = Decoder { bytes, offset: 0 };
        if input.take(4)? != CATALOG_MAGIC || input.byte()? != KEYSPACE_VERSION {
            return Err(DbError::Storage("unsupported PDB6 catalog record format".into()));
        }
        let table_id = input.u64()?;
        let name = input.string()?;
        let column_count = input.count(MAX_COLUMNS, "catalog column")?;
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let name = input.string()?;
            let data_type = match input.byte()? {
                1 => DataType::Integer,
                2 => DataType::Text,
                3 => DataType::Boolean,
                _ => return Err(DbError::Storage("unknown PDB6 catalog column type".into())),
            };
            let nullable = match input.byte()? {
                0 => false,
                1 => true,
                _ => return Err(DbError::Storage("invalid PDB6 nullable flag".into())),
            };
            columns.push(CatalogColumn { name, data_type, nullable });
        }
        let index_count = input.count(MAX_INDEXES, "catalog index")?;
        let mut indexes = Vec::with_capacity(index_count);
        for _ in 0..index_count {
            let name = input.string()?;
            let unique = match input.byte()? {
                0 => false,
                1 => true,
                _ => return Err(DbError::Storage("invalid PDB6 unique-index flag".into())),
            };
            let width = input.count(MAX_COLUMNS, "index column")?;
            let mut index_columns = Vec::with_capacity(width);
            for _ in 0..width {
                index_columns.push(input.u32()?);
            }
            indexes.push(CatalogIndex { name, columns: index_columns, unique });
        }
        let next_row_id = input.u64()?;
        if input.offset != bytes.len() {
            return Err(DbError::Storage("trailing bytes in PDB6 catalog record".into()));
        }
        let table = Self { table_id, name, columns, indexes, next_row_id };
        validate_table(&table).map_err(DbError::Storage)?;
        Ok(table)
    }
}

fn validate_table(table: &CatalogTable) -> Result<(), String> {
    if table.table_id == 0 {
        return Err("PDB6 table ID zero is reserved".into());
    }
    validate_name(&table.name, "table")?;
    if table.columns.is_empty() || table.columns.len() > MAX_COLUMNS {
        return Err("PDB6 table column count is outside supported bounds".into());
    }
    if table.indexes.len() > MAX_INDEXES {
        return Err("PDB6 index count exceeds the limit".into());
    }
    if table.next_row_id == 0 {
        return Err("PDB6 next row ID zero is reserved".into());
    }
    let mut column_names = std::collections::BTreeSet::new();
    for column in &table.columns {
        validate_name(&column.name, "column")?;
        if !column_names.insert(column.name.as_str()) {
            return Err(format!("duplicate PDB6 column name: {}", column.name));
        }
    }
    let mut index_names = std::collections::BTreeSet::new();
    for index in &table.indexes {
        validate_name(&index.name, "index")?;
        if !index_names.insert(index.name.as_str()) {
            return Err(format!("duplicate PDB6 index name: {}", index.name));
        }
        if index.columns.is_empty() || index.columns.len() > table.columns.len() {
            return Err(format!("invalid column count for PDB6 index {}", index.name));
        }
        let mut seen = std::collections::BTreeSet::new();
        for column in &index.columns {
            if *column as usize >= table.columns.len() || !seen.insert(*column) {
                return Err(format!("invalid or duplicate column reference in PDB6 index {}", index.name));
            }
        }
    }
    Ok(())
}

fn validate_name(name: &str, kind: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > MAX_NAME_BYTES || name.contains('\0') {
        return Err(format!("invalid PDB6 {kind} name length or NUL byte"));
    }
    if name != name.to_ascii_lowercase() {
        return Err(format!("PDB6 {kind} names must use canonical lowercase form"));
    }
    Ok(())
}

fn put_u32(out: &mut Vec<u8>, value: usize) -> Result<(), DbError> {
    let value = u32::try_from(value)
        .map_err(|_| DbError::Constraint("PDB6 catalog count exceeds u32".into()))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn put_string(out: &mut Vec<u8>, value: &str) -> Result<(), DbError> {
    if value.len() > MAX_NAME_BYTES || value.contains('\0') {
        return Err(DbError::Constraint("PDB6 catalog name exceeds its limit or contains NUL".into()));
    }
    put_u32(out, value.len())?;
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], DbError> {
        let end = self.offset.checked_add(count)
            .ok_or_else(|| DbError::Storage("PDB6 catalog offset overflow".into()))?;
        if end > self.bytes.len() {
            return Err(DbError::Storage("truncated PDB6 catalog record".into()));
        }
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, DbError> { Ok(self.take(1)?[0]) }
    fn u32(&mut self) -> Result<u32, DbError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, DbError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn count(&mut self, max: usize, kind: &str) -> Result<usize, DbError> {
        let count = self.u32()? as usize;
        if count > max {
            return Err(DbError::Storage(format!("PDB6 {kind} count exceeds the safety limit")));
        }
        Ok(count)
    }
    fn string(&mut self) -> Result<String, DbError> {
        let len = self.u32()? as usize;
        if len == 0 || len > MAX_NAME_BYTES {
            return Err(DbError::Storage("invalid PDB6 catalog name length".into()));
        }
        let value = std::str::from_utf8(self.take(len)?)
            .map_err(|_| DbError::Storage("PDB6 catalog name is not UTF-8".into()))?;
        if value.contains('\0') {
            return Err(DbError::Storage("PDB6 catalog name contains NUL".into()));
        }
        Ok(value.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> CatalogTable {
        CatalogTable {
            table_id: 7,
            name: "players".into(),
            columns: vec![
                CatalogColumn { name: "uuid".into(), data_type: DataType::Text, nullable: false },
                CatalogColumn { name: "last_seen".into(), data_type: DataType::Integer, nullable: true },
            ],
            indexes: vec![CatalogIndex { name: "players_uuid_idx".into(), columns: vec![0], unique: true }],
            next_row_id: 42,
        }
    }

    #[test]
    fn database_manifest_round_trips_and_rejects_invalid_header() {
        let manifest = DatabaseManifest { generation: 19, last_tx_id: 23, next_table_id: 8, table_count: 7 };
        assert_eq!(DatabaseManifest::decode(&manifest.encode().unwrap()).unwrap(), manifest);
        let mut bad = manifest.encode().unwrap();
        bad[5] = 1;
        assert!(DatabaseManifest::decode(&bad).is_err());
        assert!(DatabaseManifest { next_table_id: 0, ..manifest }.encode().is_err());
    }

    #[test]
    fn catalog_codec_round_trips_schema_and_indexes() {
        let table = sample();
        assert_eq!(CatalogTable::decode(&table.encode().unwrap()).unwrap(), table);
    }

    #[test]
    fn keyspace_is_prefix_separated_and_big_endian_ordered() {
        assert_eq!(table_key(1).unwrap(), vec![KEY_TABLE, 0, 0, 0, 0, 0, 0, 0, 1]);
        assert!(row_key(1, 0).is_err());
        assert!(row_key(0, 1).is_err());
        assert!(row_key(1, 2).unwrap() < row_key(1, 10).unwrap());
        assert_eq!(index_prefix(3, 4).unwrap().len(), 17);
        assert_eq!(counters_key(9).unwrap()[0], KEY_COUNTERS);
        assert_eq!(database_root_key(), vec![KEY_DATABASE_ROOT, KEYSPACE_VERSION]);
    }

    #[test]
    fn catalog_decoder_rejects_truncation_trailing_data_and_bad_references() {
        let encoded = sample().encode().unwrap();
        assert!(CatalogTable::decode(&encoded[..encoded.len() - 1]).is_err());
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(CatalogTable::decode(&trailing).is_err());
        let mut bad = sample();
        bad.indexes[0].columns[0] = 99;
        assert!(bad.encode().is_err());
    }

    #[test]
    fn catalog_rejects_noncanonical_and_duplicate_names() {
        let mut bad = sample();
        bad.name = "Players".into();
        assert!(bad.encode().is_err());
        let mut bad = sample();
        bad.columns[1].name = "uuid".into();
        assert!(bad.encode().is_err());
    }
}
