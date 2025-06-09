#[derive(Debug, thiserror::Error)]
pub enum BdkRedbError {
    #[error(transparent)]
    RedbError(#[from] redb::Error),

    #[error(transparent)]
    DataMissingError(#[from] MissingError),
}

#[derive(Debug, thiserror::Error)]
pub enum MissingError {
    #[error("network yet to be persisted")]
    NetworkPersistError,

    #[error("descriptor yet to be persisted")]
    DescPersistError { num_descs: u64 }, // upper bound on the number of descriptors found in the db
}
