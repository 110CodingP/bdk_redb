mod error;

use bdk_wallet::bitcoin::{self, Network, hashes::Hash};
use bdk_wallet::chain::{ConfirmationBlockTime, keychain_txout, local_chain, tx_graph};
use bdk_wallet::descriptor::{Descriptor, DescriptorPublicKey};
use bdk_wallet::{ChangeSet, chain::Merge};
use error::MissingError;
use redb::{
    Database, MultimapTableDefinition, ReadTransaction, ReadableTable, TableDefinition, TypeName,
    Value, WriteTransaction,
};
use serde::{Deserialize, Serialize};
use std::{path::Path, str::FromStr};

const NETWORK: TableDefinition<&str, String> = TableDefinition::new("network");
const KEYCHAINS: MultimapTableDefinition<&str, String> = MultimapTableDefinition::new("keychains");
const LOCALCHAIN: TableDefinition<(&str, u32), [u8; 32]> = TableDefinition::new("local_chain");
const TXGRAPH: TableDefinition<&str, TxGraphChangeSetWrapper> = TableDefinition::new("tx_graph");
const LAST_REVEALED: TableDefinition<&str, KeychainChangeSetWrapper> =
    TableDefinition::new("last_revealed");

#[derive(Debug, Serialize, Deserialize)]
struct TxGraphChangeSetWrapper(tx_graph::ChangeSet<ConfirmationBlockTime>);

impl Value for TxGraphChangeSetWrapper {
    type SelfType<'a> = TxGraphChangeSetWrapper;
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
        TypeName::new("tx_graph")
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct KeychainChangeSetWrapper(keychain_txout::ChangeSet);

impl Value for KeychainChangeSetWrapper {
    type SelfType<'a> = KeychainChangeSetWrapper;
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
        TypeName::new("last_revealed")
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
        network: &Option<bitcoin::Network>,
    ) -> Result<(), BdkRedbError> {
        let mut table = db_tx.open_table(NETWORK).map_err(redb::Error::from)?;

        // assuming network will be persisted once and only once
        if let Some(network) = network {
            let _ = table.insert(&*self.wallet_name, network.to_string());
        }
        Ok(())
    }

    pub fn persist_keychains(
        &self,
        db_tx: &WriteTransaction,
        desc: &Option<Descriptor<DescriptorPublicKey>>,
        change_desc: &Option<Descriptor<DescriptorPublicKey>>,
    ) -> Result<(), BdkRedbError> {
        let mut table = db_tx
            .open_multimap_table(KEYCHAINS)
            .map_err(redb::Error::from)?;

        // assuming descriptor would be persisted once and only once for whole lifetime of wallet.
        if let Some(desc) = desc {
            table
                .insert(&*self.wallet_name, desc.to_string())
                .map_err(redb::Error::from)?;
        }

        if let Some(change_desc) = change_desc {
            table
                .insert(&*self.wallet_name, change_desc.to_string())
                .map_err(redb::Error::from)?;
        }

        Ok(())
    }

    pub fn persist_local_chain(
        &self,
        db_tx: &WriteTransaction,
        changeset: &local_chain::ChangeSet,
    ) -> Result<(), BdkRedbError> {
        let mut table = db_tx.open_table(LOCALCHAIN).map_err(redb::Error::from)?;
        for (ht, hash) in &changeset.blocks {
            match hash {
                Some(hash) => table
                    .insert((&*self.wallet_name, *ht), hash.as_ref())
                    .unwrap(),
                None => table.remove((&*self.wallet_name, *ht)).unwrap(),
            };
        }
        Ok(())
    }

    pub fn persist_tx_graph(
        &self,
        db_tx: &WriteTransaction,
        changeset: &tx_graph::ChangeSet<ConfirmationBlockTime>,
    ) -> Result<(), BdkRedbError> {
        let mut table = db_tx.open_table(TXGRAPH).map_err(redb::Error::from)?;
        let mut aggregated_changeset = match table.remove(&*self.wallet_name).unwrap() {
            Some(value) => match value.value() {
                TxGraphChangeSetWrapper(changeset) => changeset,
            },
            None => tx_graph::ChangeSet::default(),
        };
        aggregated_changeset.merge(changeset.clone());
        table
            .insert(
                &*self.wallet_name,
                TxGraphChangeSetWrapper(aggregated_changeset),
            )
            .unwrap();
        Ok(())
    }

    pub fn persist_last_revealed(
        &self,
        db_tx: &WriteTransaction,
        changeset: &keychain_txout::ChangeSet,
    ) -> Result<(), BdkRedbError> {
        let mut table = db_tx.open_table(LAST_REVEALED).map_err(redb::Error::from)?;
        let mut aggregated_changeset = match table.remove(&*self.wallet_name).unwrap() {
            Some(value) => match value.value() {
                KeychainChangeSetWrapper(changeset) => changeset,
            },
            None => keychain_txout::ChangeSet::default(),
        };
        aggregated_changeset.merge(changeset.clone());
        table
            .insert(
                &*self.wallet_name,
                KeychainChangeSetWrapper(aggregated_changeset),
            )
            .unwrap();
        Ok(())
    }

    pub fn persist_changeset(&self, changeset: &ChangeSet) -> Result<(), BdkRedbError> {
        let db_tx = self.db.begin_write().unwrap();

        self.persist_network(&db_tx, &changeset.network)?;
        self.persist_keychains(&db_tx, &changeset.descriptor, &changeset.change_descriptor)?;
        self.persist_local_chain(&db_tx, &changeset.local_chain)?;
        self.persist_tx_graph(&db_tx, &changeset.tx_graph)?;
        self.persist_last_revealed(&db_tx, &changeset.indexer)?;

        db_tx.commit().unwrap();
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
        network: &mut Option<bitcoin::Network>,
    ) -> Result<(), BdkRedbError> {
        let table = db_tx.open_table(NETWORK).map_err(redb::Error::from)?;
        *network = match table.get(&*self.wallet_name).map_err(redb::Error::from)? {
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
        desc: &mut Option<Descriptor<DescriptorPublicKey>>,
        change_desc: &mut Option<Descriptor<DescriptorPublicKey>>,
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
            if desc.is_none() {
                *desc = Some(value.unwrap().value().parse().expect("parse descriptor"))
            } else {
                *change_desc = Some(
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
        changeset: &mut local_chain::ChangeSet,
        chain_height: Option<u32>,
    ) -> Result<(), BdkRedbError> {
        let table = db_tx
            .open_table(LOCALCHAIN)
            .map_err(redb::Error::from)
            .unwrap();

        match chain_height {
            Some(chain_height) => {
                for pos in 0..=chain_height {
                    let hash = table
                        .get((&*self.wallet_name, pos))
                        .unwrap()
                        .unwrap()
                        .value();
                    changeset
                        .blocks
                        .insert(pos, Some(bitcoin::BlockHash::from_byte_array(hash)));
                }
            }
            None => {
                // iterate over all entries in the table 😱 (increased loading times!)
                table
                    .iter()
                    .unwrap()
                    .filter(|entry| *entry.as_ref().unwrap().0.value().0 == *self.wallet_name)
                    .for_each(|entry| {
                        changeset.blocks.insert(
                            entry.as_ref().unwrap().0.value().1,
                            Some(bitcoin::BlockHash::from_byte_array(
                                entry.as_ref().unwrap().1.value(),
                            )),
                        );
                    });
            }
        }
        Ok(())
    }

    pub fn read_tx_graph(
        &self,
        db_tx: &ReadTransaction,
        changeset: &mut tx_graph::ChangeSet<ConfirmationBlockTime>,
    ) -> Result<(), BdkRedbError> {
        let table = db_tx.open_table(TXGRAPH).map_err(redb::Error::from)?;
        let TxGraphChangeSetWrapper(tx_graph) =
            table.get(&*self.wallet_name).unwrap().unwrap().value();
        *changeset = tx_graph;
        Ok(())
    }

    pub fn read_last_revealed(
        &self,
        db_tx: &ReadTransaction,
        changeset: &mut keychain_txout::ChangeSet,
    ) -> Result<(), BdkRedbError> {
        let table = db_tx.open_table(LAST_REVEALED).map_err(redb::Error::from)?;
        let KeychainChangeSetWrapper(indexer) =
            table.get(&*self.wallet_name).unwrap().unwrap().value();
        *changeset = indexer;
        Ok(())
    }

    pub fn read_changeset(&self, changeset: &mut ChangeSet) -> Result<(), BdkRedbError> {
        let db_tx = self.db.begin_read().unwrap();

        self.read_network(&db_tx, &mut changeset.network)?;
        self.read_keychains(
            &db_tx,
            &mut changeset.descriptor,
            &mut changeset.change_descriptor,
        )?;
        self.read_local_chain(&db_tx, &mut changeset.local_chain, None)?;
        self.read_tx_graph(&db_tx, &mut changeset.tx_graph)?;
        self.read_last_revealed(&db_tx, &mut changeset.indexer)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bdk_wallet::chain::BlockId;
    use bdk_wallet::{
        bitcoin::{
            self, Amount, BlockHash, OutPoint, ScriptBuf, Transaction, TxIn, TxOut, absolute,
            transaction,
        },
        chain::{DescriptorExt, local_chain},
        descriptor::Descriptor,
        keys::DescriptorPublicKey,
    };
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    macro_rules! hash {
        ($index:literal) => {{ bitcoin::hashes::Hash::hash($index.as_bytes()) }};
    }

    fn create_test_store(path: impl AsRef<Path>, wallet_name: &str) -> Store {
        let mut store = Store::load_or_create(path, wallet_name.to_string()).unwrap();
        store.create_tables().unwrap();
        store
    }

    fn test_network_persistence(store: &Store) {
        let db_tx = store.db.begin_write().unwrap();
        let changeset = ChangeSet {
            network: Some(Network::Bitcoin),
            ..Default::default()
        };
        store.persist_network(&db_tx, &changeset.network).unwrap();
        db_tx.commit().unwrap();

        let db_tx = store.db.begin_read().unwrap();
        let mut changeset = ChangeSet::default();
        store.read_network(&db_tx, &mut changeset.network).unwrap();

        assert_eq!(changeset.network, Some(Network::Bitcoin));
    }

    fn test_keychains_persistence(store: &Store) {
        let db_tx = store.db.begin_write().unwrap();

        let descriptor: Descriptor<DescriptorPublicKey> = "tr([5940b9b9/86'/0'/0']tpubDDVNqmq75GNPWQ9UNKfP43UwjaHU4GYfoPavojQbfpyfZp2KetWgjGBRRAy4tYCrAA6SB11mhQAkqxjh1VtQHyKwT4oYxpwLaGHvoKmtxZf/0/*)#44aqnlam".parse().unwrap();
        let change_descriptor: Descriptor<DescriptorPublicKey> = "tr([5940b9b9/86'/0'/0']tpubDDVNqmq75GNPWQ9UNKfP43UwjaHU4GYfoPavojQbfpyfZp2KetWgjGBRRAy4tYCrAA6SB11mhQAkqxjh1VtQHyKwT4oYxpwLaGHvoKmtxZf/1/*)#ypcpw2dr".parse().unwrap();

        store
            .persist_keychains(
                &db_tx,
                &Some(descriptor.clone()),
                &Some(change_descriptor.clone()),
            )
            .unwrap();
        db_tx.commit().unwrap();

        let db_tx = store.db.begin_read().unwrap();
        let mut changeset = ChangeSet::default();
        store
            .read_keychains(
                &db_tx,
                &mut changeset.descriptor,
                &mut changeset.change_descriptor,
            )
            .unwrap();

        assert_eq!(changeset.descriptor, Some(descriptor));
        assert_eq!(changeset.change_descriptor, Some(change_descriptor));
    }

    #[test]
    fn test_persistence() {
        let tmpfile = NamedTempFile::new().unwrap();
        let store = create_test_store(tmpfile.path(), "wallet1");

        test_network_persistence(&store);
        test_keychains_persistence(&store);
        test_local_chain_persistence(&store);
        test_tx_graph_persistence(&store);
        test_last_revealed_persistence(&store);
    }

    #[test]
    fn test_single_desc_persistence() {
        let tmpfile = NamedTempFile::new().unwrap();
        let store = create_test_store(tmpfile.path(), "wallet1");

        let db_tx = store.db.begin_write().unwrap();

        let descriptor: Descriptor<DescriptorPublicKey> = "tr([5940b9b9/86'/0'/0']tpubDDVNqmq75GNPWQ9UNKfP43UwjaHU4GYfoPavojQbfpyfZp2KetWgjGBRRAy4tYCrAA6SB11mhQAkqxjh1VtQHyKwT4oYxpwLaGHvoKmtxZf/0/*)#44aqnlam".parse().unwrap();

        store
            .persist_keychains(&db_tx, &Some(descriptor.clone()), &None)
            .unwrap();
        db_tx.commit().unwrap();

        let db_tx = store.db.begin_read().unwrap();
        let mut changeset = ChangeSet::default();
        store
            .read_keychains(
                &db_tx,
                &mut changeset.descriptor,
                &mut changeset.change_descriptor,
            )
            .unwrap();

        assert_eq!(changeset.descriptor, Some(descriptor));
        assert_eq!(changeset.change_descriptor, None);
    }

    fn test_local_chain_persistence(store: &Store) {
        let mut blocks: BTreeMap<u32, Option<BlockHash>> = BTreeMap::new();
        blocks.insert(0u32, Some(hash!("B")));
        blocks.insert(1u32, Some(hash!("D")));
        blocks.insert(2u32, Some(hash!("K")));

        let local_chain_changeset = local_chain::ChangeSet { blocks };
        let db_tx = store.db.begin_write().unwrap();
        store
            .persist_local_chain(&db_tx, &local_chain_changeset)
            .unwrap();
        db_tx.commit().unwrap();
        let db_tx = store.db.begin_read().unwrap();
        let mut changeset = local_chain::ChangeSet::default();
        store
            .read_local_chain(&db_tx, &mut changeset, None)
            .unwrap();
        assert_eq!(local_chain_changeset, changeset);

        let mut blocks: BTreeMap<u32, Option<BlockHash>> = BTreeMap::new();
        blocks.insert(2u32, None);
        let local_chain_changeset = local_chain::ChangeSet { blocks };

        let db_tx = store.db.begin_write().unwrap();
        store
            .persist_local_chain(&db_tx, &local_chain_changeset)
            .unwrap();
        db_tx.commit().unwrap();
        let db_tx = store.db.begin_read().unwrap();
        let mut changeset = ChangeSet::default();
        store
            .read_local_chain(&db_tx, &mut changeset.local_chain, None)
            .unwrap();

        let mut blocks: BTreeMap<u32, Option<BlockHash>> = BTreeMap::new();
        blocks.insert(0u32, Some(hash!("B")));
        blocks.insert(1u32, Some(hash!("D")));
        let local_chain_changeset = local_chain::ChangeSet { blocks };

        assert_eq!(local_chain_changeset, changeset.local_chain);
    }

    fn test_tx_graph_persistence(store: &Store) {
        let tx = Transaction {
            version: transaction::Version::ONE,
            lock_time: absolute::LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                ..Default::default()
            }],
            output: vec![TxOut {
                value: Amount::from_sat(30_000),
                script_pubkey: ScriptBuf::new(),
            }],
        };

        let block_id = BlockId {
            height: 100,
            hash: hash!("BDK"),
        };

        let conf_anchor: ConfirmationBlockTime = ConfirmationBlockTime {
            block_id,
            confirmation_time: 1,
        };

        let mut tx_graph_changeset1 = tx_graph::ChangeSet::<ConfirmationBlockTime> {
            txs: [Arc::new(tx.clone())].into(),
            txouts: [].into(),
            anchors: [(conf_anchor, tx.compute_txid())].into(),
            last_seen: [].into(),
        };

        let db_tx = store.db.begin_write().unwrap();
        store
            .persist_tx_graph(&db_tx, &tx_graph_changeset1)
            .unwrap();
        db_tx.commit().unwrap();

        let mut changeset = tx_graph::ChangeSet::default();
        let db_tx = store.db.begin_read().unwrap();
        store.read_tx_graph(&db_tx, &mut changeset).unwrap();
        assert_eq!(changeset, tx_graph_changeset1);

        let tx2 = Transaction {
            version: transaction::Version::ONE,
            lock_time: absolute::LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: tx.compute_txid(),
                    vout: 0,
                },
                ..Default::default()
            }],
            output: vec![TxOut {
                value: Amount::from_sat(25_000),
                script_pubkey: ScriptBuf::new(),
            }],
        };

        let block_id = BlockId {
            height: 101,
            hash: hash!("REDB"),
        };

        let conf_anchor: ConfirmationBlockTime = ConfirmationBlockTime {
            block_id,
            confirmation_time: 1,
        };

        let tx_graph_changeset2 = tx_graph::ChangeSet::<ConfirmationBlockTime> {
            txs: [Arc::new(tx2.clone())].into(),
            txouts: [].into(),
            anchors: [(conf_anchor, tx2.compute_txid())].into(),
            last_seen: [].into(),
        };

        let db_tx = store.db.begin_write().unwrap();
        store
            .persist_tx_graph(&db_tx, &tx_graph_changeset2)
            .unwrap();
        db_tx.commit().unwrap();

        let mut changeset = tx_graph::ChangeSet::default();
        let db_tx = store.db.begin_read().unwrap();
        store.read_tx_graph(&db_tx, &mut changeset).unwrap();

        tx_graph_changeset1.merge(tx_graph_changeset2);

        assert_eq!(tx_graph_changeset1, changeset);
    }

    fn test_last_revealed_persistence(store: &Store) {
        let secp = bitcoin::secp256k1::Secp256k1::signing_only();

        pub const DESCRIPTORS: [&str; 2] = [
            "tr([73c5da0a/86'/0'/0']xprv9xgqHN7yz9MwCkxsBPN5qetuNdQSUttZNKw1dcYTV4mkaAFiBVGQziHs3NRSWMkCzvgjEe3n9xV8oYywvM8at9yRqyaZVz6TYYhX98VjsUk/0/*)",
            "wpkh([73c5da0a/86'/0'/0']xprv9xgqHN7yz9MwCkxsBPN5qetuNdQSUttZNKw1dcYTV4mkaAFiBVGQziHs3NRSWMkCzvgjEe3n9xV8oYywvM8at9yRqyaZVz6TYYhX98VjsUk/1/0)",
        ];

        let descriptor_ids = DESCRIPTORS.map(|d| {
            Descriptor::<DescriptorPublicKey>::parse_descriptor(&secp, d)
                .unwrap()
                .0
                .descriptor_id()
        });

        let keychain_txout_changeset = keychain_txout::ChangeSet {
            last_revealed: [(descriptor_ids[0], 1), (descriptor_ids[1], 100)].into(),
        };

        let db_tx = store.db.begin_write().unwrap();
        store
            .persist_last_revealed(&db_tx, &keychain_txout_changeset)
            .unwrap();
        db_tx.commit().unwrap();

        let mut changeset = keychain_txout::ChangeSet::default();
        let db_tx = store.db.begin_read().unwrap();
        store.read_last_revealed(&db_tx, &mut changeset).unwrap();

        assert_eq!(changeset, keychain_txout_changeset);
    }

    #[test]
    fn test_persist_changeset() {
        let tmpfile = NamedTempFile::new().unwrap();
        let store = create_test_store(tmpfile.path(), "wallet1");

        let descriptor: Descriptor<DescriptorPublicKey> = "tr([5940b9b9/86'/0'/0']tpubDDVNqmq75GNPWQ9UNKfP43UwjaHU4GYfoPavojQbfpyfZp2KetWgjGBRRAy4tYCrAA6SB11mhQAkqxjh1VtQHyKwT4oYxpwLaGHvoKmtxZf/0/*)#44aqnlam".parse().unwrap();
        let change_descriptor: Descriptor<DescriptorPublicKey> = "tr([5940b9b9/86'/0'/0']tpubDDVNqmq75GNPWQ9UNKfP43UwjaHU4GYfoPavojQbfpyfZp2KetWgjGBRRAy4tYCrAA6SB11mhQAkqxjh1VtQHyKwT4oYxpwLaGHvoKmtxZf/1/*)#ypcpw2dr".parse().unwrap();

        let mut blocks: BTreeMap<u32, Option<BlockHash>> = BTreeMap::new();
        blocks.insert(0u32, Some(hash!("B")));
        blocks.insert(1u32, Some(hash!("T")));
        blocks.insert(2u32, Some(hash!("C")));
        let local_chain_changeset = local_chain::ChangeSet { blocks };

        let tx = Transaction {
            version: transaction::Version::TWO,
            lock_time: absolute::LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                ..Default::default()
            }],
            output: vec![TxOut {
                value: Amount::from_sat(25_000),
                script_pubkey: ScriptBuf::new(),
            }],
        };

        let block_id = BlockId {
            height: 1,
            hash: hash!("BDK"),
        };

        let conf_anchor: ConfirmationBlockTime = ConfirmationBlockTime {
            block_id,
            confirmation_time: 123,
        };

        let tx_graph_changeset = tx_graph::ChangeSet::<ConfirmationBlockTime> {
            txs: [Arc::new(tx.clone())].into(),
            txouts: [].into(),
            anchors: [(conf_anchor, tx.compute_txid())].into(),
            last_seen: [].into(),
        };

        let keychain_txout_changeset = keychain_txout::ChangeSet {
            last_revealed: [
                (descriptor.descriptor_id(), 12),
                (change_descriptor.descriptor_id(), 10),
            ]
            .into(),
        };

        let changeset_persisted = ChangeSet {
            descriptor: Some(descriptor),
            change_descriptor: Some(change_descriptor),
            network: Some(Network::Bitcoin),
            local_chain: local_chain_changeset,
            tx_graph: tx_graph_changeset,
            indexer: keychain_txout_changeset,
        };

        store.persist_changeset(&changeset_persisted).unwrap();
        let mut changeset_read = ChangeSet::default();
        store.read_changeset(&mut changeset_read).unwrap();

        assert_eq!(changeset_persisted, changeset_read);
    }
}
