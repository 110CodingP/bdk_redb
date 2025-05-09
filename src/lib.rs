use bdk_wallet::ChangeSet;
use bdk_wallet::bitcoin::Network;
use redb::{Database, ReadTransaction, TableDefinition, WriteTransaction};
use std::{path::Path, str::FromStr};

const NETWORK: TableDefinition<&str, String> = TableDefinition::new("network");

#[derive(Debug, thiserror::Error)]
pub enum BdkRedbError {
    #[error(transparent)]
    DBError(#[from] redb::DatabaseError),

    #[error(transparent)]
    TableError(#[from] redb::TableError),

    #[error("network yet to be persisted")]
    NetworkPersistError,

    #[error(transparent)]
    RedbStorageError(#[from] redb::StorageError),

    #[error(transparent)]
    RedbCommitError(#[from] redb::CommitError),

    #[error(transparent)]
    RedbTransactionError(#[from] redb::TransactionError),
}

pub struct Store {
    db: Database,
    wallet_name: String,
}

impl Store {
    pub fn load_or_create<P>(file_path: P, wallet_name: String) -> Result<Self, BdkRedbError>
    where
        P: AsRef<Path>,
    {
        let db = Database::create(file_path)?;
        Ok(Store { db, wallet_name })
    }

    pub fn persist_network(
        &self,
        db_tx: &WriteTransaction,
        network: &Network,
    ) -> Result<(), BdkRedbError> {
        let mut table = db_tx.open_table(NETWORK)?;
        let _ = table.insert(&*self.wallet_name, network.to_string());
        Ok(())
    }

    pub fn create_tables(&mut self) -> Result<(), BdkRedbError> {
        let db_tx = self.db.begin_write()?;
        let _ = db_tx.open_table(NETWORK);
        db_tx.commit()?;
        Ok(())
    }

    pub fn read_network(
        &self,
        db_tx: &ReadTransaction,
        changeset: &mut ChangeSet,
    ) -> Result<(), BdkRedbError> {
        let table = db_tx.open_table(NETWORK)?;
        changeset.network = match table.get(&*self.wallet_name)? {
            Some(network) => Some(Network::from_str(&network.value()).expect("parse network")),
            None => return Err(BdkRedbError::NetworkPersistError),
        };
        Ok(())
    }
}
