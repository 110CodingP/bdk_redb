use redb::Database;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum BdkRedbError {
    #[error(transparent)]
    DBError(#[from] redb::DatabaseError),
}

pub struct Store {
    db: Database,
}

impl Store {
    pub fn create<P>(file_path: P) -> Result<Self, BdkRedbError>
    where
        P: AsRef<Path>,
    {
        let db = Database::create(file_path)?;
        Ok(Store { db })
    }
}
