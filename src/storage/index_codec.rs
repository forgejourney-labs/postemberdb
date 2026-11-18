//! Stable, bounded on-disk encoding for SQL index keys and row references.
//!
//! This codec is deliberately independent of Rust's derived ordering and debug
//! representation. Integer bytes preserve signed numeric order; all lengths and
//! tags are explicit and decoding rejects non-canonical/trailing data.

use super::super::{IndexAtom, IndexKey};
use super::DbError;

const KEY_MAGIC: &[u8; 4] = b"EIK1";
const ROWS_MAGIC: &[u8; 4] = b"EIR1";
const MAX_KEY_BYTES: usize = 1024;
const MAX_COMPONENTS: usize = 256;

pub(crate) fn encode_index_key(key: &IndexKey) -> Result<Vec<u8>, DbError> {
    let mut out = KEY_MAGIC.to_vec();
    match key {
        IndexKey::Null => out.push(0),
        IndexKey::Integer(value) => {
            out.push(1);
            put_ordered_i64(&mut out, *value);
        }
        IndexKey::Text(value) => {
            out.push(2);
            put_text(&mut out, value)?;
        }
        IndexKey::Boolean(value) => {
            out.push(3);
            out.push(u8::from(*value));
        }
        IndexKey::Composite(atoms) => {
            if atoms.len() < 2 || atoms.len() > MAX_COMPONENTS {
                return Err(storage_error("composite index key has an invalid component count"));
            }
            out.push(4);
            put_u16(&mut out, atoms.len())?;
            for atom in atoms {
                encode_atom(&mut out, atom)?;
            }
        }
    }
    if out.len() > MAX_KEY_BYTES {
        return Err(storage_error("encoded SQL index key exceeds 1024 bytes"));
    }
    Ok(out)
}

pub(crate) fn decode_index_key(bytes: &[u8]) -> Result<IndexKey, DbError> {
    if bytes.len() > MAX_KEY_BYTES || bytes.get(..4) != Some(KEY_MAGIC.as_slice()) {
        return Err(storage_error("invalid SQL index key header or length"));
    }
    let mut input = Decoder { bytes, offset: 4 };
    let key = match input.byte()? {
        0 => IndexKey::Null,
        1 => IndexKey::Integer(input.ordered_i64()?),
        2 => IndexKey::Text(input.text()?),
        3 => IndexKey::Boolean(input.boolean()?),
        4 => {
            let count = input.u16()?;
            if !(2..=MAX_COMPONENTS).contains(&count) {
                return Err(storage_error("invalid composite index key component count"));
            }
            let mut atoms = Vec::with_capacity(count);
            for _ in 0..count {
                atoms.push(input.atom()?);
            }
            IndexKey::Composite(atoms)
        }
        _ => return Err(storage_error("unknown SQL index key tag")),
    };
    input.finish()?;
    Ok(key)
}

/// Encodes row positions as a bounded value suitable for one EmberTree page.
pub(crate) fn encode_row_refs(rows: &[usize]) -> Result<Vec<u8>, DbError> {
    let mut out = ROWS_MAGIC.to_vec();
    put_u32(&mut out, rows.len())?;
    for row in rows {
        let row = u64::try_from(*row)
            .map_err(|_| storage_error("row reference does not fit in u64"))?;
        out.extend_from_slice(&row.to_le_bytes());
    }
    if out.len() > super::extent::page_payload_size() {
        return Err(storage_error("SQL index row-reference list exceeds one EmberTree page"));
    }
    Ok(out)
}

pub(crate) fn decode_row_refs(bytes: &[u8]) -> Result<Vec<usize>, DbError> {
    if bytes.get(..4) != Some(ROWS_MAGIC.as_slice()) {
        return Err(storage_error("invalid SQL index row-reference header"));
    }
    let mut input = Decoder { bytes, offset: 4 };
    let count = input.u32()? as usize;
    if count > (super::extent::page_payload_size().saturating_sub(8) / 8) {
        return Err(storage_error("SQL index row-reference count exceeds page capacity"));
    }
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let value = u64::from_le_bytes(input.take(8)?.try_into().unwrap());
        rows.push(usize::try_from(value)
            .map_err(|_| storage_error("row reference does not fit this platform"))?);
    }
    input.finish()?;
    Ok(rows)
}

fn encode_atom(out: &mut Vec<u8>, atom: &IndexAtom) -> Result<(), DbError> {
    match atom {
        IndexAtom::Null => out.push(0),
        IndexAtom::Integer(value) => {
            out.push(1);
            put_ordered_i64(out, *value);
        }
        IndexAtom::Text(value) => {
            out.push(2);
            put_text(out, value)?;
        }
        IndexAtom::Boolean(value) => {
            out.push(3);
            out.push(u8::from(*value));
        }
    }
    Ok(())
}

fn put_ordered_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&((value as u64) ^ (1u64 << 63)).to_be_bytes());
}

fn put_text(out: &mut Vec<u8>, value: &str) -> Result<(), DbError> {
    let len = u16::try_from(value.len())
        .map_err(|_| storage_error("SQL index text component exceeds 65535 bytes"))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_u16(out: &mut Vec<u8>, value: usize) -> Result<(), DbError> {
    let value = u16::try_from(value)
        .map_err(|_| storage_error("SQL index component count exceeds u16"))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn put_u32(out: &mut Vec<u8>, value: usize) -> Result<(), DbError> {
    let value = u32::try_from(value)
        .map_err(|_| storage_error("SQL index row count exceeds u32"))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn storage_error(message: &str) -> DbError { DbError::Storage(message.into()) }

struct Decoder<'a> { bytes: &'a [u8], offset: usize }

impl<'a> Decoder<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], DbError> {
        let end = self.offset.checked_add(count)
            .ok_or_else(|| storage_error("SQL index codec offset overflow"))?;
        if end > self.bytes.len() { return Err(storage_error("truncated SQL index codec value")); }
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8, DbError> { Ok(self.take(1)?[0]) }
    fn u16(&mut self) -> Result<usize, DbError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()) as usize)
    }
    fn u32(&mut self) -> Result<u32, DbError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn ordered_i64(&mut self) -> Result<i64, DbError> {
        let value = u64::from_be_bytes(self.take(8)?.try_into().unwrap()) ^ (1u64 << 63);
        Ok(value as i64)
    }
    fn text(&mut self) -> Result<String, DbError> {
        let len = self.u16()?;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|_| storage_error("SQL index text component is not UTF-8"))
    }
    fn boolean(&mut self) -> Result<bool, DbError> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(storage_error("invalid SQL index boolean tag")),
        }
    }
    fn atom(&mut self) -> Result<IndexAtom, DbError> {
        match self.byte()? {
            0 => Ok(IndexAtom::Null),
            1 => Ok(IndexAtom::Integer(self.ordered_i64()?)),
            2 => Ok(IndexAtom::Text(self.text()?)),
            3 => Ok(IndexAtom::Boolean(self.boolean()?)),
            _ => Err(storage_error("unknown SQL index atom tag")),
        }
    }
    fn finish(&self) -> Result<(), DbError> {
        if self.offset == self.bytes.len() { Ok(()) }
        else { Err(storage_error("trailing bytes in SQL index codec value")) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;

    #[test]
    fn scalar_and_composite_keys_round_trip() {
        let keys = [
            IndexKey::from(&Value::Null),
            IndexKey::from(&Value::Integer(i64::MIN)),
            IndexKey::from(&Value::Integer(-42)),
            IndexKey::from(&Value::Integer(i64::MAX)),
            IndexKey::from(&Value::Text("éclair".into())),
            IndexKey::from(&Value::Boolean(true)),
            IndexKey::Composite(vec![
                IndexAtom::Integer(-9),
                IndexAtom::Text("player-uuid".into()),
                IndexAtom::Boolean(false),
            ]),
        ];
        for key in keys {
            assert_eq!(decode_index_key(&encode_index_key(&key).unwrap()).unwrap(), key);
        }
    }

    #[test]
    fn signed_integer_encoding_preserves_numeric_byte_order() {
        let values = [i64::MIN, -100, -1, 0, 1, 100, i64::MAX];
        let encoded = values.iter().map(|value| encode_index_key(&IndexKey::Integer(*value)).unwrap()).collect::<Vec<_>>();
        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn row_reference_lists_round_trip_and_reject_trailing_data() {
        let rows = vec![0, 1, 17, usize::MAX.min(u32::MAX as usize)];
        let encoded = encode_row_refs(&rows).unwrap();
        assert_eq!(decode_row_refs(&encoded).unwrap(), rows);
        let mut corrupt = encoded;
        corrupt.push(0);
        assert!(decode_row_refs(&corrupt).is_err());
    }

    #[test]
    fn codec_rejects_truncated_and_noncanonical_values() {
        assert!(decode_index_key(b"EIK1").is_err());
        assert!(decode_index_key(b"EIK1\x03\x02").is_err());
        assert!(decode_index_key(b"EIK1\x00extra").is_err());
    }
}
