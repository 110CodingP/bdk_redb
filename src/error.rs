//! This module contains the crate's error type.
use std::io::Error as IoError;

#[derive(Debug, thiserror::Error)]
/// Superset of all errors that can occur.
pub enum StoreError {
    /// Error while creating read or write db transactions
    #[error("Transaction error: {0}")]
    RedbTx(#[from] redb::TransactionError),
    /// Error while opening db tables
    #[error("Table error: {0}")]
    RedbTable(#[from] redb::TableError),
    /// Error while commiting write db transactions.
    #[error("Commit error: {0}")]
    RedbCommit(#[from] redb::CommitError),
    /// Error while inserting, removing, retrieving or iterating over db table entries
    #[error("Storage error: {0}")]
    RedbStorage(#[from] redb::StorageError),
    /// Error while creating redb database
    // for convenience of downstream users
    #[error("Database error: {0}")]
    RedbDatabase(#[from] redb::DatabaseError),
    /// Error while serializing transaction using [`ciborium`]
    #[error("ciborium serialization error: {0}")]
    Ser(#[from] ciborium::ser::Error<IoError>),
    /// Error while deserializing transaction using [`ciborium`]
    ///
    /// [`ciborium`]: <https://docs.rs/ciborium/0.2.2/ciborium/index.html>
    #[error("ciborium deserialization error: {0}")]
    Deser(#[from] ciborium::de::Error<IoError>),
    /// Error while deserializing [`BlockHash`] from slice
    ///
    /// [`BlockHash`]: <https://docs.rs/bitcoin/latest/bitcoin/struct.BlockHash.html>
    #[error("BlockHash deserialization error: {0}")]
    BlockHashFromSlice(#[from] bdk_chain::bitcoin::hashes::FromSliceError),
}
