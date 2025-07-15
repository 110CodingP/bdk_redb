// Superset of all errors that can occur.
use std::io::Error as IoError;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("Transaction error: {0}")]
    RedbTx(#[from] redb::TransactionError),
    #[error("Table error: {0}")]
    RedbTable(#[from] redb::TableError),
    #[error("Commit error: {0}")]
    RedbCommit(#[from] redb::CommitError),
    #[error("Storage error: {0}")]
    RedbStorage(#[from] redb::StorageError),
    #[error("Database error: {0}")]
    RedbDatabase(#[from] redb::DatabaseError),
    #[error("ciborium serialization error: {0}")]
    Ser(#[from] ciborium::ser::Error<IoError>),
    #[error("ciborium deserialization error: {0}")]
    Deser(#[from] ciborium::de::Error<IoError>),
    #[error("BlockHash deserialization error: {0}")]
    BlockHashFromSlice(#[from] bdk_chain::bitcoin::hashes::FromSliceError),
}
