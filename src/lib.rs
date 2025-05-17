mod error;

use bdk_wallet::{chain::Merge, ChangeSet};
use bdk_wallet::bitcoin::Network;
use error::MissingError;
use redb::{
    Database, MultimapTableDefinition, ReadTransaction, TableDefinition, TypeName, Value,
    WriteTransaction,
};
use serde::{Deserialize, Serialize};
use std::{path::Path, str::FromStr};

const NETWORK: TableDefinition<&str, String> = TableDefinition::new("network");
const KEYCHAINS: MultimapTableDefinition<&str, String> = MultimapTableDefinition::new("keychains");
const LOCALCHAIN: TableDefinition<&str, LocalChainChangesetWrapper> =
    TableDefinition::new("local_chain");

#[derive(Debug, Serialize, Deserialize)]
struct LocalChainChangesetWrapper(bdk_wallet::chain::local_chain::ChangeSet);

impl Value for LocalChainChangesetWrapper {
    type SelfType<'a> = LocalChainChangesetWrapper;
    type AsBytes<'a> = Vec<u8>;
    fn fixed_width() -> Option<usize> {
        None
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        let mut vec: Vec<u8> = Vec::new();
        ciborium::into_writer(value, &mut vec).unwrap();
        vec
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        ciborium::from_reader(data).unwrap()
    }
    fn type_name() -> redb::TypeName {
        TypeName::new("local_chain")
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BdkRedbError {
    #[error(transparent)]
    RedbError(#[from] redb::Error),

    #[error(transparent)]
    DataMissingError(#[from] MissingError),
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
        changeset: &mut ChangeSet,
    ) -> Result<(), BdkRedbError> {
        let mut table = db_tx
            .open_multimap_table(KEYCHAINS)
            .map_err(redb::Error::from)?;

        // assuming descriptor would be persisted once and only once for whole lifetime of wallet.
        if let Some(desc) = &changeset.descriptor {
            table
                .insert(&*self.wallet_name, desc.to_string())
                .map_err(redb::Error::from)?;
        }

        if let Some(change_desc) = &changeset.change_descriptor {
            table
                .insert(&*self.wallet_name, change_desc.to_string())
                .map_err(redb::Error::from)?;
        }

        Ok(())
    }

    pub fn persist_local_chain(
        &self,
        db_tx: &WriteTransaction,
        changeset: &mut ChangeSet,
    ) -> Result<(), BdkRedbError> {
        let mut table = db_tx.open_table(LOCALCHAIN).map_err(redb::Error::from)?;
        let LocalChainChangesetWrapper(mut aggregated_changeset) = table.remove(&*self.wallet_name).unwrap().unwrap().value();
        aggregated_changeset.merge(changeset.local_chain.clone());
        table.insert(&*self.wallet_name, LocalChainChangesetWrapper(aggregated_changeset)).unwrap();
        Ok(())
    }

    pub fn create_tables(&mut self) -> Result<(), BdkRedbError> {
        let db_tx = self.db.begin_write().map_err(redb::Error::from)?;

        let _ = db_tx.open_table(NETWORK);
        let _ = db_tx.open_multimap_table(KEYCHAINS);

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
            None => {
                return Err(BdkRedbError::DataMissingError(
                    MissingError::NetworkPersistError,
                ));
            }
        };
        Ok(())
    }

    pub fn read_keychains(
        &self,
        db_tx: &ReadTransaction,
        changeset: &mut ChangeSet,
    ) -> Result<(), BdkRedbError> {
        let table = db_tx
            .open_multimap_table(KEYCHAINS)
            .map_err(redb::Error::from)?;

        // ToDo: Make the following idiomatic
        for value in table
            .get(&*self.wallet_name)
            .map_err(redb::Error::from)
            .expect("wallet keychains should be persisted")
        {
            if changeset.descriptor.is_none() {
                changeset.descriptor =
                    Some(value.unwrap().value().parse().expect("pars descriptor"))
            } else {
                changeset.change_descriptor = Some(
                    value
                        .unwrap()
                        .value()
                        .parse()
                        .expect(" parse change descriptor"),
                )
            }
        }

        Ok(())
    }

    pub fn read_local_chain(
        &self,
        db_tx: &ReadTransaction,
        changeset: &mut ChangeSet,
    ) -> Result<(), BdkRedbError> {
        let table = db_tx.open_table(LOCALCHAIN).map_err(redb::Error::from)?;
        let LocalChainChangesetWrapper(local_chain) =
            table.get(&*self.wallet_name).unwrap().unwrap().value();
        changeset.local_chain = local_chain;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
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
        let mut changeset = ChangeSet::default();
        changeset.descriptor = Some(descriptor.clone());
        changeset.change_descriptor = Some(change_descriptor.clone());

        store.persist_keychains(&db_tx, &mut changeset).unwrap();
        db_tx.commit().unwrap();

        let db_tx = store.db.begin_read().unwrap();
        let mut changeset = ChangeSet::default();
        store.read_keychains(&db_tx, &mut changeset).unwrap();

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

        let mut changeset = ChangeSet::default();
        changeset.descriptor = Some(descriptor.clone());

        store.persist_keychains(&db_tx, &mut changeset).unwrap();
        db_tx.commit().unwrap();

        let db_tx = store.db.begin_read().unwrap();
        let mut changeset = ChangeSet::default();
        store.read_keychains(&db_tx, &mut changeset).unwrap();

        assert_eq!(changeset.descriptor, Some(descriptor));
        assert_eq!(changeset.change_descriptor, None);

        delete_store("test_single_desc_persistence");
    }
}
