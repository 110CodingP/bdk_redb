// Superset of all errors that can occur.
use std::io::Error as IoError;

#[derive(Debug, thiserror::Error)]
pub enum BdkRedbError {
    #[error("Database error: {0}")]
    Redb(#[from] redb::Error),
    #[error("Serialization error: {0}")]
    Ser(#[from] ciborium::ser::Error<IoError>),
    #[error("Deserialization error: {0}")]
    Deser(#[from] ciborium::de::Error<IoError>),
    #[error("Deserialization error: {0}")]
    BlockHashFromSlice(#[from] bdk_chain::bitcoin::hashes::FromSliceError),
}
