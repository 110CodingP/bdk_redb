use bdk_wallet::ChangeSet;
use bdk_wallet::bitcoin::Network;
use redb::{Database, ReadTransaction, ReadableTable, TableDefinition, WriteTransaction};
use std::collections::HashMap;
use std::{path::Path, str::FromStr};

const NETWORK: TableDefinition<&str, String> = TableDefinition::new("network");
const KEYCHAINS: TableDefinition<(&str, String), (String, &[u8])> =
    TableDefinition::new("keychains");

#[derive(Debug, thiserror::Error)]
pub enum BdkRedbError {
    #[error(transparent)]
    RedbError(#[from] redb::Error),

    #[error("network yet to be persisted")]
    NetworkPersistError,

    #[error("descriptor yet to be persisted")]
    DescPersistError { num_descs: u64 }, // upper bound on the number of descriptors found in the db
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

    pub fn persist_keychains(
        &self,
        db_tx: &WriteTransaction,
        descriptors: Vec<String>,
        descriptor_ids: Vec<&[u8]>,
        labels: Vec<String>,
    ) -> Result<(), BdkRedbError> {
        let mut table = db_tx.open_table(KEYCHAINS).map_err(redb::Error::from)?;

        for ((descriptor, descriptor_id), label) in
            descriptors.into_iter().zip(descriptor_ids).zip(labels)
        {
            table
                .insert((&*self.wallet_name, label), (descriptor, descriptor_id))
                .map_err(redb::Error::from)?;
        }

        Ok(())
    }

    pub fn create_tables(&mut self) -> Result<(), BdkRedbError> {
        let db_tx = self.db.begin_write().map_err(redb::Error::from)?;

        let _ = db_tx.open_table(NETWORK);
        let _ = db_tx.open_table(KEYCHAINS);

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

    pub fn read_keychains(
        &self,
        db_tx: &ReadTransaction,
        changeset: &mut ChangeSet,
        num_keychains: u64,
    ) -> Result<(), BdkRedbError> {
        let table = db_tx.open_table(KEYCHAINS).map_err(redb::Error::from)?;

        let mut descriptors: HashMap<String, String> = HashMap::new();

        // ToDo: Make the following idiomatic
        for entry in table.iter().map_err(redb::Error::from)? {
            let (key, value) = entry.map_err(redb::Error::from)?;
            if key.value().0 == &*self.wallet_name {
                descriptors.insert(key.value().1, value.value().0);
            }
        }

        if descriptors.len() as u64 != num_keychains {
            return Err(BdkRedbError::DescPersistError {
                num_descs: descriptors.len() as u64,
            });
        }

        changeset.descriptor = Some(
            descriptors
                .get("External")
                .ok_or(BdkRedbError::DescPersistError { num_descs: 0 })?
                .parse()
                .expect("parse descriptor"),
        );

        if num_keychains == 2 {
            changeset.change_descriptor = Some(
                descriptors
                    .get("Internal")
                    .ok_or(BdkRedbError::DescPersistError { num_descs: 1 })?
                    .parse()
                    .expect("parse change descriptor"),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bdk_chain::DescriptorExt;
    use bdk_wallet::bitcoin::hashes::Hash;
    use bdk_wallet::{descriptor::Descriptor, keys::DescriptorPublicKey};
    use std::fs::remove_file;

    fn create_test_store(path: &str, wallet_name: &str) -> Store {
        let mut store = Store::load_or_create(path, wallet_name.to_string()).unwrap();
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

    fn test_keychains_persistence(store: &Store) {
        let db_tx = store.db.begin_write().unwrap();

        let descriptor: Descriptor<DescriptorPublicKey> = "tr([5940b9b9/86'/0'/0']tpubDDVNqmq75GNPWQ9UNKfP43UwjaHU4GYfoPavojQbfpyfZp2KetWgjGBRRAy4tYCrAA6SB11mhQAkqxjh1VtQHyKwT4oYxpwLaGHvoKmtxZf/0/*)#44aqnlam".parse().unwrap();
        let change_descriptor: Descriptor<DescriptorPublicKey> = "tr([5940b9b9/86'/0'/0']tpubDDVNqmq75GNPWQ9UNKfP43UwjaHU4GYfoPavojQbfpyfZp2KetWgjGBRRAy4tYCrAA6SB11mhQAkqxjh1VtQHyKwT4oYxpwLaGHvoKmtxZf/1/*)#ypcpw2dr".parse().unwrap();
        let descriptors = vec![descriptor.to_string(), change_descriptor.to_string()];
        let descriptor_id = descriptor.descriptor_id().to_byte_array();
        let change_descriptor_id = change_descriptor.descriptor_id().to_byte_array();
        let descriptor_ids = vec![descriptor_id.as_slice(), change_descriptor_id.as_slice()];
        let labels = vec!["External".to_string(), "Internal".to_string()];

        store
            .persist_keychains(&db_tx, descriptors, descriptor_ids, labels)
            .unwrap();
        db_tx.commit().unwrap();

        let db_tx = store.db.begin_read().unwrap();
        let mut changeset = ChangeSet::default();
        store.read_keychains(&db_tx, &mut changeset, 2).unwrap();

        assert_eq!(changeset.descriptor, Some(descriptor));
        assert_eq!(changeset.change_descriptor, Some(change_descriptor));
    }

    fn delete_store(path: &str) {
        remove_file(path).unwrap();
    }

    #[test]
    fn test_persistence() {
        let store = create_test_store("test_persistence", "wallet1");

        test_network_persistence(&store);
        test_keychains_persistence(&store);

        delete_store("test_persistence");
    }

    #[test]
    fn test_single_desc_persistence() {
        let store = create_test_store("test_single_desc_persistence", "wallet1");

        let db_tx = store.db.begin_write().unwrap();

        let descriptor: Descriptor<DescriptorPublicKey> = "tr([5940b9b9/86'/0'/0']tpubDDVNqmq75GNPWQ9UNKfP43UwjaHU4GYfoPavojQbfpyfZp2KetWgjGBRRAy4tYCrAA6SB11mhQAkqxjh1VtQHyKwT4oYxpwLaGHvoKmtxZf/0/*)#44aqnlam".parse().unwrap();
        let descriptor_id = descriptor.descriptor_id().to_byte_array();

        store
            .persist_keychains(
                &db_tx,
                vec![descriptor.to_string()],
                vec![descriptor_id.as_slice()],
                vec!["External".to_string()],
            )
            .unwrap();
        db_tx.commit().unwrap();

        let db_tx = store.db.begin_read().unwrap();
        let mut changeset = ChangeSet::default();
        store.read_keychains(&db_tx, &mut changeset, 1).unwrap();

        assert_eq!(changeset.descriptor, Some(descriptor));
        assert_eq!(changeset.change_descriptor, None);

        delete_store("test_single_desc_persistence");
    }
}
