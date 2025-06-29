// Superset of all errors that can occur.
#[derive(Debug, thiserror::Error)]
pub enum BdkRedbError {
    #[error("Database error: {0}")]
    RedbError(#[from] redb::Error),
}
