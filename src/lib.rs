use bdk_wallet::ChangeSet;
use bdk_wallet::bitcoin::Network;
use redb::{Database, ReadTransaction, TableDefinition, WriteTransaction};
use std::{path::Path, str::FromStr};

const NETWORK: TableDefinition<&str, String> = TableDefinition::new("network");

#[derive(Debug, thiserror::Error)]
pub enum BdkRedbError {
    #[error(transparent)]
    RedbError(#[from] redb::Error),

    #[error("network yet to be persisted")]
    NetworkPersistError,
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
        let db = Database::create(file_path).map_err(redb::Error::from)?;
        Ok(Store { db, wallet_name })
    }

    pub fn persist_network(
        &self,
        db_tx: &WriteTransaction,
        network: &Network,
    ) -> Result<(), BdkRedbError> {
        let mut table = db_tx.open_table(NETWORK).map_err(redb::Error::from)?;
        let _ = table.insert(&*self.wallet_name, network.to_string());
        Ok(())
    }

    pub fn create_tables(&mut self) -> Result<(), BdkRedbError> {
        let db_tx = self.db.begin_write().map_err(redb::Error::from)?;
        let _ = db_tx.open_table(NETWORK);
        db_tx.commit().map_err(redb::Error::from)?;
        Ok(())
    }

    pub fn read_network(
        &self,
        db_tx: &ReadTransaction,
        changeset: &mut ChangeSet,
    ) -> Result<(), BdkRedbError> {
        let table = db_tx.open_table(NETWORK).map_err(redb::Error::from)?;
        changeset.network = match table.get(&*self.wallet_name).map_err(redb::Error::from)? {
            Some(network) => Some(Network::from_str(&network.value()).expect("parse network")),
            None => return Err(BdkRedbError::NetworkPersistError),
        };
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::remove_file;

    fn create_test_store() -> Store {
        let mut store = Store::load_or_create("path", "wallet1".to_string()).unwrap();
        store.create_tables().unwrap();
        store
    }

    fn test_network_persistence(store: &Store) {
        let db_tx = store.db.begin_write().unwrap();
        store.persist_network(&db_tx, &Network::Bitcoin).unwrap();
        db_tx.commit().unwrap();

        let db_tx = store.db.begin_read().unwrap();
        let mut changeset = ChangeSet::default();
        store.read_network(&db_tx, &mut changeset).unwrap();

        assert_eq!(changeset.network, Some(Network::Bitcoin));
    }

    #[test]
    fn test_persistence() {
        let store = create_test_store();

        test_network_persistence(&store);

        remove_file("path").unwrap();
    }
}
