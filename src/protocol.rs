//! Versioned, bounded binary framing for the PostemberDB server protocol.
//!
//! Header layout (24 bytes, little-endian integers):
//! magic[4] = "PDBF", version:u8, kind:u8, flags:u16,
//! request_id:u64, payload_len:u32, payload_crc32:u32.
//! CRC32 covers the payload only. Reserved flags must be zero.

use std::io::{self, Read, Write};

pub const MAGIC: [u8; 4] = *b"PDBF";
pub const VERSION: u8 = 1;
pub const HEADER_LEN: usize = 24;
/// The default transport frame cap. Large records require a future streaming
/// upload/download sequence; this cap prevents untrusted length allocations.
pub const DEFAULT_MAX_PAYLOAD: usize = 8 * 1024 * 1024;
const RESERVED_FLAGS: u16 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    Query = 1,
    QueryResult = 2,
    Error = 3,
    Ping = 4,
    Pong = 5,
    ChunkPut = 16,
    ChunkGet = 17,
    ChunkDelete = 18,
    ChunkData = 19,
    ChunkScan = 20,
    ChunkScanResult = 21,
    Catalog = 6,
    CatalogResult = 7,
    TablePage = 8,
    TablePageResult = 9,
    ServerStatus = 10,
    ServerStatusResult = 11,
    AdminRequest = 12,
    AdminResponse = 13,
    BatchQuery = 14,
    BatchQueryResult = 15,
}

impl TryFrom<u8> for FrameKind {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, io::Error> {
        match value {
            1 => Ok(Self::Query),
            2 => Ok(Self::QueryResult),
            3 => Ok(Self::Error),
            4 => Ok(Self::Ping),
            5 => Ok(Self::Pong),
            6 => Ok(Self::Catalog),
            7 => Ok(Self::CatalogResult),
            8 => Ok(Self::TablePage),
            9 => Ok(Self::TablePageResult),
            10 => Ok(Self::ServerStatus),
            11 => Ok(Self::ServerStatusResult),
            12 => Ok(Self::AdminRequest),
            13 => Ok(Self::AdminResponse),
            14 => Ok(Self::BatchQuery),
            15 => Ok(Self::BatchQueryResult),
            16 => Ok(Self::ChunkPut),
            17 => Ok(Self::ChunkGet),
            18 => Ok(Self::ChunkDelete),
            19 => Ok(Self::ChunkData),
            20 => Ok(Self::ChunkScan),
            21 => Ok(Self::ChunkScanResult),
            _ => Err(invalid_data("unknown binary frame kind")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub kind: FrameKind,
    pub request_id: u64,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn new(kind: FrameKind, request_id: u64, payload: impl Into<Vec<u8>>) -> Self {
        Self { kind, request_id, payload: payload.into() }
    }

    pub fn write_to<W: Write>(&self, writer: &mut W, max_payload: usize) -> io::Result<()> {
        if self.payload.len() > max_payload || self.payload.len() > u32::MAX as usize {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "binary frame payload exceeds configured limit"));
        }
        let mut header = [0u8; HEADER_LEN];
        header[..4].copy_from_slice(&MAGIC);
        header[4] = VERSION;
        header[5] = self.kind as u8;
        header[6..8].copy_from_slice(&RESERVED_FLAGS.to_le_bytes());
        header[8..16].copy_from_slice(&self.request_id.to_le_bytes());
        header[16..20].copy_from_slice(&(self.payload.len() as u32).to_le_bytes());
        header[20..24].copy_from_slice(&crc32(&self.payload).to_le_bytes());
        writer.write_all(&header)?;
        writer.write_all(&self.payload)?;
        writer.flush()
    }

    pub fn read_from<R: Read>(reader: &mut R, max_payload: usize) -> io::Result<Self> {
        Self::read_from_or_eof(reader, max_payload)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "end of stream"))
    }

    /// Reads one frame, returning None only when the stream ends before any
    /// byte of the next header. A partial header or payload is always an error.
    pub fn read_from_or_eof<R: Read>(reader: &mut R, max_payload: usize) -> io::Result<Option<Self>> {
        let mut header = [0u8; HEADER_LEN];
        let first = reader.read(&mut header[..1])?;
        if first == 0 {
            return Ok(None);
        }
        reader.read_exact(&mut header[1..])?;
        if header[..4] != MAGIC {
            return Err(invalid_data("invalid binary frame magic"));
        }
        if header[4] != VERSION {
            return Err(invalid_data("unsupported binary protocol version"));
        }
        if u16::from_le_bytes(header[6..8].try_into().expect("fixed header slice")) != RESERVED_FLAGS {
            return Err(invalid_data("unsupported binary frame flags"));
        }
        let kind = FrameKind::try_from(header[5])?;
        let request_id = u64::from_le_bytes(header[8..16].try_into().expect("fixed header slice"));
        let payload_len = u32::from_le_bytes(header[16..20].try_into().expect("fixed header slice")) as usize;
        if payload_len > max_payload {
            return Err(invalid_data("binary frame payload exceeds configured limit"));
        }
        let expected_crc = u32::from_le_bytes(header[20..24].try_into().expect("fixed header slice"));
        let mut payload = vec![0u8; payload_len];
        reader.read_exact(&mut payload)?;
        if crc32(&payload) != expected_crc {
            return Err(invalid_data("binary frame payload checksum mismatch"));
        }
        Ok(Some(Self { kind, request_id, payload }))
    }
}

pub fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn frame_round_trip_preserves_kind_request_and_binary_payload() {
        let frame = Frame::new(FrameKind::ChunkPut, 0x1020_3040_5060_7080, vec![0, 1, 0xff, 0, 0x7f]);
        let mut encoded = Vec::new();
        frame.write_to(&mut encoded, 1024).unwrap();
        assert_eq!(&encoded[..4], b"PDBF");
        assert_eq!(Frame::read_from(&mut Cursor::new(encoded), 1024).unwrap(), frame);
    }

    #[test]
    fn rejects_oversized_length_before_allocating_payload() {
        let mut header = [0u8; HEADER_LEN];
        header[..4].copy_from_slice(&MAGIC);
        header[4] = VERSION;
        header[5] = FrameKind::Query as u8;
        header[16..20].copy_from_slice(&1025u32.to_le_bytes());
        let error = Frame::read_from(&mut Cursor::new(header), 1024).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("configured limit"));
    }

    #[test]
    fn rejects_bad_magic_version_flags_and_kind() {
        for (offset, value) in [(0usize, b'X'), (4, 99), (5, 99), (6, 1)] {
            let mut header = [0u8; HEADER_LEN];
            header[..4].copy_from_slice(&MAGIC);
            header[4] = VERSION;
            header[5] = FrameKind::Ping as u8;
            header[offset] = value;
            let error = Frame::read_from(&mut Cursor::new(header), 1024).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        }
    }

    #[test]
    fn rejects_payload_corruption_and_truncated_frames() {
        let frame = Frame::new(FrameKind::Query, 7, b"SELECT 1".to_vec());
        let mut encoded = Vec::new();
        frame.write_to(&mut encoded, 1024).unwrap();
        *encoded.last_mut().unwrap() ^= 0x40;
        assert!(Frame::read_from(&mut Cursor::new(encoded.clone()), 1024)
            .unwrap_err().to_string().contains("checksum"));
        encoded.pop();
        assert_eq!(Frame::read_from(&mut Cursor::new(encoded), 1024).unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn clean_eof_is_distinct_from_a_truncated_header() {
        let mut empty = Cursor::new(Vec::<u8>::new());
        assert!(Frame::read_from_or_eof(&mut empty, 1024).unwrap().is_none());

        let mut partial = Cursor::new(b"PDB".to_vec());
        assert_eq!(
            Frame::read_from_or_eof(&mut partial, 1024).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn crc32_matches_standard_check_vector() {
        assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
    }
}
