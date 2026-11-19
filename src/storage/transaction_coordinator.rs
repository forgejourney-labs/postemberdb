//! Durable transaction-decision journal for cross-domain transactions.
//!
//! This module records coordinator decisions only. It deliberately does not
//! claim atomicity for SQL/KV/Registry/EF7 until each participant implements
//! durable prepare, idempotent commit/rollback, and recovery integration.

use crate::DbError;
use fs2::FileExt;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const RECORD_MAGIC: &[u8; 4] = b"PBTX";
const RECORD_VERSION: u8 = 1;
const RECORD_BYTES: usize = 24;
const MAX_PARTICIPANTS: u8 = 32;

/// A durable page-tree participant that can replay a prepared coordinator batch.
pub trait CoordinatorParticipant {
    /// Stable domain ID recorded in the coordinator journal.
    fn participant_id(&self) -> u8;
    /// Whether the participant still has its durable prepare marker.
    fn has_prepared(&self, transaction_id: u64) -> bool;
    /// Apply a prepared transaction. Must be idempotent until finalize_prepared.
    fn commit_prepared(&mut self, transaction_id: u64) -> Result<(), DbError>;
    /// Remove the prepare marker after the coordinator's APPLIED record is synced.
    fn finalize_prepared(&mut self, transaction_id: u64) -> Result<(), DbError>;
    /// Discard a prepared transaction after a durable ABORT decision.
    fn rollback_prepared(&mut self, transaction_id: u64) -> Result<(), DbError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState { Preparing, Committed, Aborted }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredTransaction {
    pub transaction_id: u64,
    pub participant_count: u8,
    pub prepared_participants: Vec<u8>,
    /// Participants whose durable commit application has been acknowledged.
    pub applied_participants: Vec<u8>,
    pub state: TransactionState,
}

#[derive(Debug)]
struct TransactionRecord {
    participant_count: u8,
    prepared: BTreeSet<u8>,
    applied: BTreeSet<u8>,
    state: TransactionState,
}

pub struct TransactionCoordinator {
    path: PathBuf,
    journal: File,
    _lock_file: File,
    next_transaction_id: u64,
    transactions: BTreeMap<u64, TransactionRecord>,
}

impl TransactionCoordinator {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() { fs::create_dir_all(parent).map_err(storage_error)?; }
        }
        let lock_file = OpenOptions::new().create(true).read(true).write(true)
            .open(with_suffix(&path, ".lock")).map_err(storage_error)?;
        FileExt::try_lock_exclusive(&lock_file)
            .map_err(|error| DbError::Storage(format!("transaction coordinator is already open: {error}")))?;
        let mut journal = OpenOptions::new().create(true).read(true).write(true)
            .open(&path).map_err(storage_error)?;
        let mut bytes = Vec::new();
        journal.read_to_end(&mut bytes).map_err(storage_error)?;
        let complete_len = bytes.len() / RECORD_BYTES * RECORD_BYTES;
        if complete_len != bytes.len() {
            journal.set_len(complete_len as u64).map_err(storage_error)?;
            journal.sync_all().map_err(storage_error)?;
            bytes.truncate(complete_len);
        }

        let mut transactions = BTreeMap::new();
        let mut last_id = 0u64;
        for record in bytes.chunks_exact(RECORD_BYTES) {
            let (kind, participant, txid, participant_count) = decode_record(record)?;
            if txid == 0 { return Err(DbError::Storage("transaction coordinator journal contains transaction ID zero".into())); }
            match kind {
                1 => {
                    if txid <= last_id || participant_count == 0 || participant_count > MAX_PARTICIPANTS || participant != 0 {
                        return Err(DbError::Storage("invalid transaction coordinator BEGIN record".into()));
                    }
                    transactions.insert(txid, TransactionRecord {
                        participant_count,
                        prepared: BTreeSet::new(),
                        applied: BTreeSet::new(),
                        state: TransactionState::Preparing,
                    });
                    last_id = txid;
                }
                2 => {
                    let tx = transactions.get_mut(&txid).ok_or_else(|| DbError::Storage("PREPARED record references an unknown transaction".into()))?;
                    if tx.state != TransactionState::Preparing || participant >= tx.participant_count
                        || !tx.prepared.insert(participant) || participant_count != tx.participant_count {
                        return Err(DbError::Storage("invalid or duplicate transaction PREPARED record".into()));
                    }
                }
                3 | 4 => {
                    let tx = transactions.get_mut(&txid).ok_or_else(|| DbError::Storage("decision record references an unknown transaction".into()))?;
                    if tx.state != TransactionState::Preparing || participant != 0 || participant_count != tx.participant_count
                        || (kind == 3 && tx.prepared.len() != usize::from(tx.participant_count)) {
                        return Err(DbError::Storage("invalid transaction COMMIT/ABORT decision".into()));
                    }
                    tx.state = if kind == 3 { TransactionState::Committed } else { TransactionState::Aborted };
                }
                5 => {
                    let tx = transactions.get_mut(&txid).ok_or_else(|| DbError::Storage("APPLIED record references an unknown transaction".into()))?;
                    if tx.state != TransactionState::Committed
                        || participant >= tx.participant_count
                        || participant_count != tx.participant_count
                        || !tx.prepared.contains(&participant)
                        || !tx.applied.insert(participant) {
                        return Err(DbError::Storage("invalid or duplicate transaction APPLIED record".into()));
                    }
                }
                _ => return Err(DbError::Storage("unknown transaction coordinator journal record".into())),
            }
        }
        journal.seek(SeekFrom::End(0)).map_err(storage_error)?;
        Ok(Self {
            path, journal, _lock_file: lock_file,
            next_transaction_id: last_id.checked_add(1).ok_or_else(|| DbError::Storage("transaction ID space exhausted".into()))?,
            transactions,
        })
    }

    /// The BEGIN record is synced before the transaction ID is returned.
    pub fn begin(&mut self, participant_count: u8) -> Result<u64, DbError> {
        if participant_count == 0 || participant_count > MAX_PARTICIPANTS {
            return Err(DbError::Constraint(format!("transaction participant count must be 1..={MAX_PARTICIPANTS}")));
        }
        let txid = self.next_transaction_id;
        let next = txid.checked_add(1).ok_or_else(|| DbError::Storage("transaction ID space exhausted".into()))?;
        self.append(1, 0, txid, participant_count)?;
        self.transactions.insert(txid, TransactionRecord {
            participant_count,
            prepared: BTreeSet::new(),
            applied: BTreeSet::new(),
            state: TransactionState::Preparing,
        });
        self.next_transaction_id = next;
        Ok(txid)
    }

    pub fn mark_prepared(&mut self, txid: u64, participant_id: u8) -> Result<(), DbError> {
        let tx = self.transactions.get(&txid).ok_or_else(|| DbError::Storage("unknown coordinator transaction".into()))?;
        if tx.state != TransactionState::Preparing || participant_id >= tx.participant_count || tx.prepared.contains(&participant_id) {
            return Err(DbError::Constraint("participant cannot be marked prepared in the current transaction state".into()));
        }
        let count = tx.participant_count;
        self.append(2, participant_id, txid, count)?;
        self.transactions.get_mut(&txid).unwrap().prepared.insert(participant_id);
        Ok(())
    }

    /// COMMIT is durable only after every participant has a PREPARED record.
    pub fn commit(&mut self, txid: u64) -> Result<(), DbError> {
        let tx = self.transactions.get(&txid).ok_or_else(|| DbError::Storage("unknown coordinator transaction".into()))?;
        if tx.state != TransactionState::Preparing || tx.prepared.len() != usize::from(tx.participant_count) {
            return Err(DbError::Constraint("cannot commit before every participant is prepared".into()));
        }
        let count = tx.participant_count;
        self.append(3, 0, txid, count)?;
        self.transactions.get_mut(&txid).unwrap().state = TransactionState::Committed;
        Ok(())
    }

    /// Records that a participant has durably applied a committed decision.
    ///
    /// The acknowledgement is itself synced. If a process crashes after the
    /// participant commit but before this record, recovery retries commit; therefore
    /// participant commit implementations MUST be idempotent by transaction ID.
    pub fn mark_applied(&mut self, txid: u64, participant_id: u8) -> Result<(), DbError> {
        let tx = self.transactions.get(&txid).ok_or_else(|| DbError::Storage("unknown coordinator transaction".into()))?;
        if tx.state != TransactionState::Committed
            || participant_id >= tx.participant_count
            || !tx.prepared.contains(&participant_id)
            || tx.applied.contains(&participant_id) {
            return Err(DbError::Constraint("participant cannot be marked applied in the current transaction state".into()));
        }
        let count = tx.participant_count;
        self.append(5, participant_id, txid, count)?;
        self.transactions.get_mut(&txid).unwrap().applied.insert(participant_id);
        Ok(())
    }

    /// Returns committed transactions that still need one or more participants
    /// to apply the durable decision. Callers must retry those commits idempotently.
    pub fn committed_pending_apply(&self) -> Vec<RecoveredTransaction> {
        self.transactions.iter().filter_map(|(id, tx)| {
            (tx.state == TransactionState::Committed && tx.applied.len() < usize::from(tx.participant_count))
                .then(|| RecoveredTransaction {
                    transaction_id: *id,
                    participant_count: tx.participant_count,
                    prepared_participants: tx.prepared.iter().copied().collect(),
                    applied_participants: tx.applied.iter().copied().collect(),
                    state: tx.state,
                })
        }).collect()
    }

    pub fn rollback(&mut self, txid: u64) -> Result<(), DbError> {
        let tx = self.transactions.get(&txid).ok_or_else(|| DbError::Storage("unknown coordinator transaction".into()))?;
        if tx.state != TransactionState::Preparing {
            return Err(DbError::Constraint("cannot abort a transaction after a final decision".into()));
        }
        let count = tx.participant_count;
        self.append(4, 0, txid, count)?;
        self.transactions.get_mut(&txid).unwrap().state = TransactionState::Aborted;
        Ok(())
    }

    pub fn in_doubt(&self) -> Vec<RecoveredTransaction> {
        self.transactions.iter().filter_map(|(id, tx)| (tx.state == TransactionState::Preparing).then(|| RecoveredTransaction {
            transaction_id: *id, participant_count: tx.participant_count,
            prepared_participants: tx.prepared.iter().copied().collect(),
            applied_participants: tx.applied.iter().copied().collect(),
            state: tx.state,
        })).collect()
    }

    /// Replays every prepared participant after a durable COMMIT decision.
    /// APPLIED is synced before its prepare marker is removed, so recovery can
    /// safely retry a participant commit or finish marker cleanup after a crash.
    pub fn apply_committed(
        &mut self,
        txid: u64,
        participants: &mut [&mut dyn CoordinatorParticipant],
    ) -> Result<(), DbError> {
        let tx = self.transactions.get(&txid)
            .ok_or_else(|| DbError::Storage("unknown coordinator transaction".into()))?;
        if tx.state != TransactionState::Committed {
            return Err(DbError::Constraint("participants can only apply a committed transaction".into()));
        }
        let prepared = tx.prepared.iter().copied().collect::<Vec<_>>();
        let applied = tx.applied.clone();
        let mut supplied = BTreeSet::new();
        for participant in participants.iter() {
            let id = participant.participant_id();
            if !supplied.insert(id) {
                return Err(DbError::Constraint("duplicate coordinator participant ID".into()));
            }
        }
        for id in prepared {
            let participant = participants.iter_mut()
                .find(|participant| participant.participant_id() == id)
                .ok_or_else(|| DbError::Storage(format!("coordinator participant {id} is not available for recovery")))?;
            if !applied.contains(&id) {
                if !participant.has_prepared(txid) {
                    return Err(DbError::Storage(format!("participant {id} is missing its durable prepare marker")));
                }
                participant.commit_prepared(txid)?;
                self.mark_applied(txid, id)?;
            }
            if participant.has_prepared(txid) {
                participant.finalize_prepared(txid)?;
            }
        }
        Ok(())
    }

    pub fn state(&self, txid: u64) -> Option<TransactionState> {
        self.transactions.get(&txid).map(|tx| tx.state)
    }

    pub fn journal_path(&self) -> &Path { &self.path }

    fn append(&mut self, kind: u8, participant: u8, txid: u64, count: u8) -> Result<(), DbError> {
        self.journal.write_all(&encode_record(kind, participant, txid, count)).map_err(storage_error)?;
        self.journal.sync_all().map_err(storage_error)?;
        Ok(())
    }
}

fn encode_record(kind: u8, participant: u8, txid: u64, count: u8) -> [u8; RECORD_BYTES] {
    let mut record = [0u8; RECORD_BYTES];
    record[..4].copy_from_slice(RECORD_MAGIC);
    record[4] = RECORD_VERSION;
    record[5] = kind;
    record[6] = participant;
    record[7] = count;
    record[8..16].copy_from_slice(&txid.to_le_bytes());
    let checksum = crc32(&record[..20]);
    record[20..24].copy_from_slice(&checksum.to_le_bytes());
    record
}

fn decode_record(record: &[u8]) -> Result<(u8, u8, u64, u8), DbError> {
    if record.len() != RECORD_BYTES || &record[..4] != RECORD_MAGIC || record[4] != RECORD_VERSION
        || record[16..20] != [0, 0, 0, 0]
        || u32::from_le_bytes(record[20..24].try_into().unwrap()) != crc32(&record[..20]) {
        return Err(DbError::Storage("transaction coordinator journal record is corrupt".into()));
    }
    Ok((record[5], record[6], u64::from_le_bytes(record[8..16].try_into().unwrap()), record[7]))
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 { let mask = 0u32.wrapping_sub(crc & 1); crc = (crc >> 1) ^ (0xedb8_8320 & mask); }
    }
    !crc
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string(); value.push(suffix); PathBuf::from(value)
}
fn storage_error(error: std::io::Error) -> DbError { DbError::Storage(error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};
    fn path() -> PathBuf {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir().join(format!("postemberdb-coordinator-{nonce}.wal"))
    }
    fn cleanup(path: &Path) { let _ = fs::remove_file(path); let _ = fs::remove_file(with_suffix(path, ".lock")); }

    #[test]
    fn commit_requires_all_prepared_and_decisions_survive_reopen() {
        let path = path();
        {
            let mut c = TransactionCoordinator::open(&path).unwrap();
            let tx = c.begin(3).unwrap();
            c.mark_prepared(tx, 0).unwrap();
            assert!(c.commit(tx).is_err());
            c.mark_prepared(tx, 1).unwrap(); c.mark_prepared(tx, 2).unwrap(); c.commit(tx).unwrap();
            let aborted = c.begin(2).unwrap(); c.mark_prepared(aborted, 0).unwrap(); c.rollback(aborted).unwrap();
        }
        {
            let c = TransactionCoordinator::open(&path).unwrap();
            assert_eq!(c.state(1), Some(TransactionState::Committed));
            assert_eq!(c.state(2), Some(TransactionState::Aborted));
            assert!(c.in_doubt().is_empty());
        }
        cleanup(&path);
    }

    #[test]
    fn recovery_reports_in_doubt_and_truncates_only_a_torn_tail() {
        let path = path(); let tx;
        {
            let mut c = TransactionCoordinator::open(&path).unwrap();
            tx = c.begin(2).unwrap(); c.mark_prepared(tx, 0).unwrap();
        }
        OpenOptions::new().append(true).open(&path).unwrap().write_all(b"PBT").unwrap();
        {
            let c = TransactionCoordinator::open(&path).unwrap();
            let pending = c.in_doubt();
            assert_eq!(pending.len(), 1); assert_eq!(pending[0].transaction_id, tx);
            assert_eq!(pending[0].prepared_participants, vec![0]);
        }
        cleanup(&path);
    }

    #[test]
    fn committed_participant_application_is_durable_and_retryable() {
        let path = path();
        let tx;
        {
            let mut c = TransactionCoordinator::open(&path).unwrap();
            tx = c.begin(2).unwrap();
            c.mark_prepared(tx, 0).unwrap();
            c.mark_prepared(tx, 1).unwrap();
            c.commit(tx).unwrap();
            c.mark_applied(tx, 0).unwrap();
            assert_eq!(c.committed_pending_apply()[0].applied_participants, vec![0]);
            assert!(c.mark_applied(tx, 0).is_err());
        }
        {
            let mut c = TransactionCoordinator::open(&path).unwrap();
            let pending = c.committed_pending_apply();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].transaction_id, tx);
            assert_eq!(pending[0].prepared_participants, vec![0, 1]);
            assert_eq!(pending[0].applied_participants, vec![0]);
            c.mark_applied(tx, 1).unwrap();
            assert!(c.committed_pending_apply().is_empty());
        }
        cleanup(&path);
    }

    #[test]
    fn coordinator_applies_and_finalizes_a_prepared_kv_participant() {
        let coordinator_path = path();
        let kv_path = coordinator_path.with_extension("kv");
        let tx;
        {
            let mut coordinator = TransactionCoordinator::open(&coordinator_path).unwrap();
            let mut kv = crate::KeyValueStore::open(&kv_path).unwrap();
            kv.prepare_batch(1, &[b"old".to_vec()], &[(b"new".to_vec(), b"value".to_vec())]).unwrap();
            tx = coordinator.begin(1).unwrap();
            coordinator.mark_prepared(tx, 1).unwrap();
            coordinator.commit(tx).unwrap();
            let mut participants: [&mut dyn CoordinatorParticipant; 1] = [&mut kv];
            coordinator.apply_committed(tx, &mut participants).unwrap();
            assert_eq!(kv.get(b"new").unwrap(), Some(b"value".to_vec()));
            assert!(kv.get(b"old").unwrap().is_none());
            assert!(kv.pending_prepared_transactions().is_empty());
            assert!(coordinator.committed_pending_apply().is_empty());
        }
        for suffix in ["", ".lock", ".pages", ".pages.wal", ".pages.wal.prepared.1"] {
            let mut candidate = kv_path.as_os_str().to_os_string();
            candidate.push(suffix);
            let _ = fs::remove_file(PathBuf::from(candidate));
        }
        cleanup(&coordinator_path);
    }

    #[test]
    fn complete_record_checksum_corruption_fails_closed() {
        let path = path();
        { let mut c = TransactionCoordinator::open(&path).unwrap(); c.begin(1).unwrap(); }
        let mut bytes = fs::read(&path).unwrap(); bytes[12] ^= 0x80; fs::write(&path, bytes).unwrap();
        assert!(TransactionCoordinator::open(&path).is_err());
        cleanup(&path);
    }
}
