//! This crate provides an alternative to the sqlite persistence backend for [`BDK`] using [`redb`],
//! a lightweight key-value store in Rust.
//!
//! [`BDK`]: <https://github.com/bitcoindevkit>
//! [`redb`]: <https://github.com/cberner/redb>
//!
//! <div class="warning">Warning: Descriptors in the following example are on a test network and
//! only serve as an example. MAINNET funds sent to addresses controlled by these will be lost!
//! </div>
//!
//! # Example
//!
//! Add this to your Cargo.toml :
//!
//! ```toml
//! [dependencies]
//! anyhow = "1.0.98"
//! bdk_redb = { git = "https://github.com/110CodingP/bdk_redb" }
//! bdk_wallet = "2.0.0"
//! tempfile = "3.20.0"
//! ```
//!
//! Now:
//!
//! ```rust
//! # #[cfg(feature="wallet")]
//! # mod wrapper {
//!     use bdk_wallet::bitcoin::Network;
//!     use bdk_wallet::{KeychainKind, Wallet};
//!     use std::sync::Arc;
//!     use tempfile::NamedTempFile;
//!     
//!     use anyhow::Result;
//!     
//!     const EXTERNAL_DESCRIPTOR: &str = "tr(tprv8ZgxMBicQKsPdrjwWCyXqqJ4YqcyG4DmKtjjsRt29v1PtD3r3PuFJAjWytzcvSTKnZAGAkPSmnrdnuHWxCAwy3i1iPhrtKAfXRH7dVCNGp6/86'/1'/0'/0/*)#g9xn7wf9";
//!     const INTERNAL_DESCRIPTOR: &str = "tr(tprv8ZgxMBicQKsPdrjwWCyXqqJ4YqcyG4DmKtjjsRt29v1PtD3r3PuFJAjWytzcvSTKnZAGAkPSmnrdnuHWxCAwy3i1iPhrtKAfXRH7dVCNGp6/86'/1'/0'/1/*)#e3rjrmea";
//!     
//!     fn main() -> Result<()> {
//!         let network = Network::Signet;
//!         let file_path = NamedTempFile::new()?;
//!         let db = Arc::new(bdk_redb::redb::Database::create(file_path.path())?);
//!         let mut store = bdk_redb::Store::new(db, "wallet1".to_string())?;
//!     
//!         let wallet_opt = Wallet::load()
//!             .descriptor(KeychainKind::External, Some(EXTERNAL_DESCRIPTOR))
//!             .descriptor(KeychainKind::Internal, Some(INTERNAL_DESCRIPTOR))
//!             .extract_keys()
//!             .check_network(network)
//!             .load_wallet(&mut store)?;
//!     
//!         let mut wallet = match wallet_opt {
//!             Some(wallet) => {
//!                 println!("Loaded existing wallet database.");
//!                 wallet
//!             }
//!             None => {
//!                 println!("Creating new wallet database.");
//!                 Wallet::create(EXTERNAL_DESCRIPTOR, INTERNAL_DESCRIPTOR)
//!                     .network(network)
//!                     .create_wallet(&mut store)?
//!             }
//!         };
//!         // Reveal a new address from your external keychain
//!         let address = wallet.reveal_next_address(KeychainKind::External);
//!         wallet.persist(&mut store)?;
//!         // Only share new address with user after successfully persisting wallet
//!         println!("Wallet address[{}]: {}", address.index, address.address);
//!     
//!         Ok(())
//!     }
//! # }
//! ```

//! Also note that [`BDK`] uses structures called ChangeSets for persistence so while the
//! documentation of each function links to the structures it is trying to eventually persist, the
//! function actually uses the corresponding ChangeSets.
pub use redb;

pub mod anchor_trait;
pub mod error;

use anchor_trait::AnchorWithMetaData;
use bdk_chain::bitcoin::{self, Network, OutPoint, Transaction, Txid};
use bdk_chain::bitcoin::{Amount, BlockHash, ScriptBuf, TxOut, hashes::Hash};
use bdk_chain::miniscript::descriptor::{Descriptor, DescriptorPublicKey};
use bdk_chain::{BlockId, DescriptorId, keychain_txout, local_chain, tx_graph};
#[cfg(feature = "wallet")]
use bdk_wallet::{ChangeSet, WalletPersister};
use error::StoreError;
use redb::{Database, ReadTransaction, ReadableTable, TableDefinition, WriteTransaction};
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::Arc;

#[cfg(feature = "wallet")]
use bdk_chain::ConfirmationBlockTime;

/// The following table stores (wallet_name, network) pairs. This is common to all wallets in
/// a database file.
const NETWORK: TableDefinition<&str, String> = TableDefinition::new("network");

/// Persists the [`bdk_chain`] and [`bdk_wallet`] structures in a [`redb`] database.
///
/// [`bdk_chain`]: <https://docs.rs/bdk_chain/0.23.0/bdk_chain/index.html>
/// [`bdk_wallet`]: <https://docs.rs/bdk_wallet/2.0.0/bdk_wallet/index.html>
///
/// This is the primary struct of this crate. It holds the database corresponding to a wallet.
/// It also holds the table names of redb tables which are specific to each wallet in a database
/// file.
pub struct Store {
    // We use a reference so as to avoid taking ownership of the Database, allowing other
    // applications to write to it. Arc is for thread safety.
    db: Arc<Database>,
    wallet_name: String,

    // These could be removed if we can find a way to combine a String and an &str to create a
    // String without using unsafe Rust.
    keychain_table_name: String,
    last_revealed_table_name: String,
    blocks_table_name: String,
    txouts_table_name: String,
    last_seen_table_name: String,
    txs_table_name: String,
    anchors_table_name: String,
    last_evicted_table_name: String,
    first_seen_table_name: String,
    spk_table_name: String,
}

impl Store {
    // This table stores (KeychainKind, Descriptor) pairs on a high level.
    fn keychains_table_defn(&self) -> TableDefinition<u64, String> {
        TableDefinition::new(&self.keychain_table_name)
    }

    // This table stores (height, BlockHash) pairs on a high level.
    fn blocks_table_defn(&self) -> TableDefinition<u32, [u8; 32]> {
        TableDefinition::new(&self.blocks_table_name)
    }

    // This table stores (Txid, Transaction) pairs on a high level.
    fn txs_table_defn(&self) -> TableDefinition<[u8; 32], Vec<u8>> {
        TableDefinition::new(&self.txs_table_name)
    }

    // This table stores (Outpoint, TxOut) pairs on a high level.
    // where Outpoint = (Txid, vout) and TxOut = (value, script_pubkey)
    fn txouts_table_defn(&self) -> TableDefinition<([u8; 32], u32), (u64, Vec<u8>)> {
        TableDefinition::new(&self.txouts_table_name)
    }

    // This table stores ((Txid, BlockId), Metadata) pairs on a high level where Metadata refers to
    // extra information stored inside the anchor. For example confirmation time would be metadata
    // in case of ConfirmationBlockTime and None in case of BlockId.
    // The key was chosen like this because a transaction can be anchored in multiple Blocks
    // (in different chains ) and a Block can anchor multiple transactions.
    fn anchors_table_defn<A: AnchorWithMetaData>(
        &self,
    ) -> TableDefinition<([u8; 32], [u8; 36]), A::MetaDataType> {
        TableDefinition::new(&self.anchors_table_name)
    }

    // This table stores (Txid, last_seen) pairs on a high level.
    fn last_seen_defn(&self) -> TableDefinition<[u8; 32], u64> {
        TableDefinition::new(&self.last_seen_table_name)
    }

    // This table stores (Txid, last_evicted) pairs on a high level.
    fn last_evicted_table_defn(&self) -> TableDefinition<[u8; 32], u64> {
        TableDefinition::new(&self.last_evicted_table_name)
    }

    // This table stores (Txid, first_seen) pairs on a high level.
    fn first_seen_table_defn(&self) -> TableDefinition<[u8; 32], u64> {
        TableDefinition::new(&self.first_seen_table_name)
    }

    // This table stores (DescriptorId, last_revealed_index) pairs on a high level.
    fn last_revealed_table_defn(&self) -> TableDefinition<[u8; 32], u32> {
        TableDefinition::new(&self.last_revealed_table_name)
    }

    // This table stores ((DescriptorId, index), ScriptPubKey) pairs on a high level.
    fn spk_table_defn(&self) -> TableDefinition<([u8; 32], u32), Vec<u8>> {
        TableDefinition::new(&self.spk_table_name)
    }

    /// This function creates a brand new [`Store`].
    ///
    /// [`Store`]: crate::Store
    pub fn new(db: Arc<Database>, wallet_name: String) -> Result<Self, StoreError> {
        // Create table names to be stored in the Store.
        let mut keychain_table_name = wallet_name.clone();
        keychain_table_name.push_str("_keychain");
        let mut blocks_table_name = wallet_name.clone();
        blocks_table_name.push_str("_blocks");
        let mut txs_table_name = wallet_name.clone();
        txs_table_name.push_str("_txs");
        let mut txouts_table_name = wallet_name.clone();
        txouts_table_name.push_str("_txouts");
        let mut anchors_table_name = wallet_name.clone();
        anchors_table_name.push_str("_anchors");
        let mut last_seen_table_name = wallet_name.clone();
        last_seen_table_name.push_str("_last_seen");
        let mut last_evicted_table_name = wallet_name.clone();
        last_evicted_table_name.push_str("_last_evicted");
        let mut first_seen_table_name = wallet_name.clone();
        first_seen_table_name.push_str("_first_seen");
        let mut last_revealed_table_name = wallet_name.clone();
        last_revealed_table_name.push_str("_last_revealed");
        let mut spk_table_name = wallet_name.clone();
        spk_table_name.push_str("_spk");
        Ok(Store {
            db,
            wallet_name,
            keychain_table_name,
            blocks_table_name,
            txs_table_name,
            txouts_table_name,
            anchors_table_name,
            last_seen_table_name,
            last_evicted_table_name,
            first_seen_table_name,
            last_revealed_table_name,
            spk_table_name,
        })
    }

    /// This function creates or opens (if already created) all redb tables corresponding to a
    /// [`Wallet`].
    ///
    /// [`Wallet`]: <https://docs.rs/bdk_wallet/2.0.0/bdk_wallet/struct.Wallet.html>
    pub fn create_tables<A: AnchorWithMetaData>(&self) -> Result<(), StoreError> {
        let write_tx = self.db.begin_write()?;

        let _ = write_tx.open_table(NETWORK)?;
        let _ = write_tx.open_table(self.keychains_table_defn())?;
        write_tx.commit()?;

        self.create_local_chain_tables()?;
        self.create_tx_graph_tables::<A>()?;
        self.create_indexer_tables()?;

        Ok(())
    }

    /// This function creates or opens (if already created) the redb tables corresponding to
    /// local_chain.
    pub fn create_local_chain_tables(&self) -> Result<(), StoreError> {
        let write_tx = self.db.begin_write()?;
        let _ = write_tx.open_table(self.blocks_table_defn())?;
        write_tx.commit()?;
        Ok(())
    }

    /// This function creates or opens (if already created) the redb tables corresponding to
    /// [`TxGraph`].
    ///
    /// [`TxGraph`]: <http://docs.rs/bdk_chain/0.23.0/bdk_chain/tx_graph/struct.TxGraph.html>
    pub fn create_tx_graph_tables<A: AnchorWithMetaData>(&self) -> Result<(), StoreError> {
        let write_tx = self.db.begin_write()?;
        let _ = write_tx.open_table(self.txs_table_defn())?;
        let _ = write_tx.open_table(self.txouts_table_defn())?;
        let _ = write_tx.open_table(self.anchors_table_defn::<A>())?;
        let _ = write_tx.open_table(self.last_seen_defn())?;
        let _ = write_tx.open_table(self.last_evicted_table_defn())?;
        let _ = write_tx.open_table(self.first_seen_table_defn())?;

        write_tx.commit()?;
        Ok(())
    }

    /// This function creates or opens (if already created) the redb tables corresponding to
    /// [`indexer`].
    ///
    /// [`indexer`]: <https://docs.rs/bdk_chain/0.23.0/bdk_chain/indexer/index.html>
    pub fn create_indexer_tables(&self) -> Result<(), StoreError> {
        let write_tx = self.db.begin_write()?;
        let _ = write_tx.open_table(self.spk_table_defn())?;

        let _ = write_tx.open_table(self.last_revealed_table_defn())?;
        write_tx.commit()?;
        Ok(())
    }

    /// This function creates or opens (if already created) the keychains redb table corresponding
    /// to the wallet.
    pub fn create_keychains_table(&self) -> Result<(), StoreError> {
        let write_tx = self.db.begin_write()?;
        let _ = write_tx.open_table(self.keychains_table_defn())?;
        write_tx.commit()?;
        Ok(())
    }

    /// This function creates or opens (if already created) the [`Network`] redb table. This table
    /// is common to all wallets persisted in the database file.
    ///
    /// [`Network`]: <https://docs.rs/bitcoin/latest/bitcoin/enum.Network.html>
    pub fn create_network_table(&self) -> Result<(), StoreError> {
        let write_tx = self.db.begin_write()?;
        let _ = write_tx.open_table(NETWORK)?;
        write_tx.commit()?;
        Ok(())
    }

    #[cfg(feature = "wallet")]
    /// This function persists the [`Wallet`] into our db. It persists each field by calling
    /// corresponding persistence functions.
    ///
    /// [`Wallet`]: <https://docs.rs/bdk_wallet/2.0.0/bdk_wallet/struct.Wallet.html>
    pub fn persist_wallet(&self, changeset: &ChangeSet) -> Result<(), StoreError> {
        self.persist_network(&changeset.network)?;
        let mut desc_changeset: BTreeMap<u64, Descriptor<DescriptorPublicKey>> = BTreeMap::new();
        if let Some(desc) = &changeset.descriptor {
            desc_changeset.insert(0, desc.clone());
            if let Some(change_desc) = &changeset.change_descriptor {
                desc_changeset.insert(1, change_desc.clone());
            }
        }
        self.persist_keychains(&desc_changeset)?;
        self.persist_local_chain(&changeset.local_chain)?;
        self.persist_indexer(&changeset.indexer)?;
        self.persist_tx_graph::<ConfirmationBlockTime>(&changeset.tx_graph)?;
        Ok(())
    }

    /// This function persists the [`TxGraph`] into our db. It persists each field
    /// by calling corresponding persistence functions.
    ///
    /// [`TxGraph`]: <http://docs.rs/bdk_chain/0.23.0/bdk_chain/tx_graph/struct.TxGraph.html>
    pub fn persist_tx_graph<A: AnchorWithMetaData>(
        &self,
        changeset: &tx_graph::ChangeSet<A>,
    ) -> Result<(), StoreError> {
        let write_tx = self.db.begin_write()?;
        let read_tx = self.db.begin_read()?;
        self.persist_txs(&write_tx, &changeset.txs)?;
        self.persist_txouts(&write_tx, &changeset.txouts)?;
        self.persist_anchors::<A>(&write_tx, &read_tx, &changeset.anchors, &changeset.txs)?;
        self.persist_last_seen(&write_tx, &read_tx, &changeset.last_seen, &changeset.txs)?;
        self.persist_last_evicted(&write_tx, &read_tx, &changeset.last_evicted, &changeset.txs)?;
        self.persist_first_seen(&write_tx, &read_tx, &changeset.first_seen, &changeset.txs)?;
        write_tx.commit()?;
        Ok(())
    }

    /// This function persists the [`indexer`] structures into our db. It persists each
    /// field by calling corresponding persistence functions.
    ///
    /// [`indexer`]: <https://docs.rs/bdk_chain/0.23.0/bdk_chain/indexer/index.html>
    pub fn persist_indexer(&self, changeset: &keychain_txout::ChangeSet) -> Result<(), StoreError> {
        let write_tx = self.db.begin_write()?;
        self.persist_last_revealed(&write_tx, &changeset.last_revealed)?;
        self.persist_spks(&write_tx, &changeset.spk_cache)?;
        write_tx.commit()?;
        Ok(())
    }

    /// This function persists the descriptors into our db.
    pub fn persist_keychains(
        &self,
        // maps label to descriptor
        changeset: &BTreeMap<u64, Descriptor<DescriptorPublicKey>>,
    ) -> Result<(), StoreError> {
        let write_tx = self.db.begin_write()?;
        {
            let mut table = write_tx.open_table(self.keychains_table_defn())?;

            // assuming descriptors corresponding to a label(keychain) are never modified.
            for (label, desc) in changeset {
                table.insert(label, desc.to_string())?;
            }
        }
        write_tx.commit()?;
        Ok(())
    }

    /// This function persists the [`Network`] into our db.
    /// <div class="warning">Warning: Do Not use with MAINNET</div>
    ///
    /// [`Network`]: <https://docs.rs/bitcoin/latest/bitcoin/enum.Network.html>
    pub fn persist_network(&self, network: &Option<bitcoin::Network>) -> Result<(), StoreError> {
        let write_tx = self.db.begin_write()?;
        {
            let mut table = write_tx.open_table(NETWORK)?;
            // assuming network will be persisted once and only once
            if let Some(network) = network {
                table.insert(&*self.wallet_name, network.to_string())?;
            }
        }
        write_tx.commit()?;
        Ok(())
    }

    /// This function persists the [`LocalChain`] structure into our db. It persists each
    /// field by calling corresponding persistence functions.
    ///
    /// [`LocalChain`]: <http://docs.rs/bdk_chain/0.23.0/bdk_chain/local_chain/struct.LocalChain.html>
    pub fn persist_local_chain(
        &self,
        changeset: &local_chain::ChangeSet,
    ) -> Result<(), StoreError> {
        let write_tx = self.db.begin_write()?;
        self.persist_blocks(&write_tx, &changeset.blocks)?;
        write_tx.commit()?;
        Ok(())
    }

    // This function persists blocks corresponding to a local_chain.
    fn persist_blocks(
        &self,
        write_tx: &WriteTransaction,
        blocks: &BTreeMap<u32, Option<BlockHash>>,
    ) -> Result<(), StoreError> {
        let mut table = write_tx.open_table(self.blocks_table_defn())?;
        for (ht, hash) in blocks {
            match hash {
                &Some(hash) => table.insert(*ht, hash.to_byte_array())?,
                // remove the block if hash is None
                // assuming it is guaranteed that (ht, None) => there is an entry of form (ht,_) in
                // the Table.
                None => table.remove(*ht)?,
            };
        }
        Ok(())
    }

    // This function persists txs corresponding to a tx_graph.
    fn persist_txs(
        &self,
        write_tx: &WriteTransaction,
        txs: &BTreeSet<Arc<Transaction>>,
    ) -> Result<(), StoreError> {
        let mut table = write_tx.open_table(self.txs_table_defn())?;
        for tx in txs {
            let mut vec: Vec<u8> = Vec::new();
            ciborium::into_writer(tx, &mut vec)?;
            table.insert(tx.compute_txid().to_byte_array(), vec)?;
        }
        Ok(())
    }

    // This function persists txouts corresponding to a tx_graph.
    fn persist_txouts(
        &self,
        write_tx: &WriteTransaction,
        txouts: &BTreeMap<OutPoint, TxOut>,
    ) -> Result<(), StoreError> {
        let mut table = write_tx.open_table(self.txouts_table_defn())?;
        for (outpoint, txout) in txouts {
            table.insert(
                (outpoint.txid.to_byte_array(), outpoint.vout),
                (
                    txout.value.to_sat(),
                    txout.script_pubkey.clone().into_bytes(),
                ),
            )?;
        }
        Ok(())
    }

    // This function persists anchors corresponding to a tx_graph.
    fn persist_anchors<A: AnchorWithMetaData>(
        &self,
        write_tx: &WriteTransaction,
        read_tx: &ReadTransaction,
        anchors: &BTreeSet<(A, Txid)>,
        txs: &BTreeSet<Arc<Transaction>>,
    ) -> Result<(), StoreError> {
        let mut table = write_tx.open_table(self.anchors_table_defn::<A>())?;
        let txs_table = read_tx.open_table(self.txs_table_defn())?;
        for (anchor, txid) in anchors {
            // if the corresponding txn exists in Txs table (trying to imitate the
            // referential behavior in case of sqlite)
            let found = txs.iter().any(|tx| tx.compute_txid() == *txid);
            if txs_table.get(txid.to_byte_array())?.is_some() || found {
                let mut bytes: [u8; 36] = [0; 36];
                let anchor_block = anchor.anchor_block();
                bytes[0..4].copy_from_slice(&anchor_block.height.to_le_bytes());
                bytes[4..].copy_from_slice(&anchor_block.hash.to_byte_array());
                table.insert((txid.to_byte_array(), bytes), &anchor.metadata())?;
            } else {
                panic!("txn corresponding to anchor must exist");
            }
        }
        Ok(())
    }

    // This function persists last_seen flags corresponding to a tx_graph.
    fn persist_last_seen(
        &self,
        write_tx: &WriteTransaction,
        read_tx: &ReadTransaction,
        last_seen: &BTreeMap<Txid, u64>,
        txs: &BTreeSet<Arc<Transaction>>,
    ) -> Result<(), StoreError> {
        let mut table = write_tx.open_table(self.last_seen_defn())?;
        let txs_table = read_tx.open_table(self.txs_table_defn())?;
        for (txid, last_seen_time) in last_seen {
            // if the corresponding txn exists in Txs table (trying to duplicate the
            // referential behavior in case of sqlite)
            let found = txs.iter().any(|tx| tx.compute_txid() == *txid);
            if txs_table.get(txid.to_byte_array())?.is_some() || found {
                table.insert(txid.to_byte_array(), *last_seen_time)?;
            } else {
                panic!("txn must exist before persisting last_seen");
            }
        }
        Ok(())
    }

    // This function persists last_evicted flags corresponding to a tx_graph .
    fn persist_last_evicted(
        &self,
        write_tx: &WriteTransaction,
        read_tx: &ReadTransaction,
        last_evicted: &BTreeMap<Txid, u64>,
        txs: &BTreeSet<Arc<Transaction>>,
    ) -> Result<(), StoreError> {
        let mut table = write_tx.open_table(self.last_evicted_table_defn())?;
        let txs_table = read_tx.open_table(self.txs_table_defn())?;
        for (txid, last_evicted_time) in last_evicted {
            // if the corresponding txn exists in Txs table (trying to duplicate the
            // referential behavior in case of sqlite)
            let found = txs.iter().any(|tx| tx.compute_txid() == *txid);
            if txs_table.get(txid.to_byte_array())?.is_some() || found {
                table.insert(txid.to_byte_array(), last_evicted_time)?;
            } else {
                panic!("txn must exist before persisting last_evicted");
            }
        }
        Ok(())
    }

    // This function persists first_seen flags corresponding to a tx_graph .
    fn persist_first_seen(
        &self,
        write_tx: &WriteTransaction,
        read_tx: &ReadTransaction,
        first_seen: &BTreeMap<Txid, u64>,
        txs: &BTreeSet<Arc<Transaction>>,
    ) -> Result<(), StoreError> {
        let mut table = write_tx.open_table(self.first_seen_table_defn())?;
        let txs_table = read_tx.open_table(self.txs_table_defn())?;
        for (txid, first_seen_time) in first_seen {
            // if the corresponding txn exists in Txs table (trying to duplicate the
            // referential behavior in case of sqlite)
            let found = txs.iter().any(|tx| tx.compute_txid() == *txid);
            if txs_table.get(txid.to_byte_array())?.is_some() || found {
                table.insert(txid.to_byte_array(), first_seen_time)?;
            } else {
                panic!("txn must exist before persisting first_seen");
            }
        }
        Ok(())
    }

    // This function persists last_revealed corresponding to keychain_txout .
    fn persist_last_revealed(
        &self,
        write_tx: &WriteTransaction,
        last_revealed: &BTreeMap<DescriptorId, u32>,
    ) -> Result<(), StoreError> {
        let mut table = write_tx.open_table(self.last_revealed_table_defn())?;
        for (&desc, &idx) in last_revealed {
            table.insert(desc.to_byte_array(), idx)?;
        }
        Ok(())
    }

    // This function persists spk_cache corresponding to keychain_txout .
    fn persist_spks(
        &self,
        write_tx: &WriteTransaction,
        spk_cache: &BTreeMap<DescriptorId, BTreeMap<u32, ScriptBuf>>,
    ) -> Result<(), StoreError> {
        let mut table = write_tx.open_table(self.spk_table_defn())?;
        for (&desc, map) in spk_cache {
            map.iter().try_for_each(|entry| {
                table
                    .insert((desc.to_byte_array(), *entry.0), entry.1.to_bytes())
                    .map(|_| ())
            })?;
        }
        Ok(())
    }

    #[cfg(feature = "wallet")]
    /// This function loads the [`Wallet`]  by calling corresponding load functions for each of its
    /// fields.s
    ///
    /// [`Wallet`]: <https://docs.rs/bdk_wallet/2.0.0/bdk_wallet/struct.Wallet.html>
    pub fn read_wallet(&self, changeset: &mut ChangeSet) -> Result<(), StoreError> {
        self.read_network(&mut changeset.network)?;
        let mut desc_changeset: BTreeMap<u64, Descriptor<DescriptorPublicKey>> = BTreeMap::new();
        self.read_keychains(&mut desc_changeset)?;
        if let Some(desc) = desc_changeset.get(&0) {
            changeset.descriptor = Some(desc.clone());
            if let Some(change_desc) = desc_changeset.get(&1) {
                changeset.change_descriptor = Some(change_desc.clone());
            }
        }
        self.read_local_chain(&mut changeset.local_chain)?;
        self.read_tx_graph::<ConfirmationBlockTime>(&mut changeset.tx_graph)?;
        self.read_indexer(&mut changeset.indexer)?;

        Ok(())
    }

    /// This function loads the [`TxGraph`] from db. It loads each field
    /// by calling corresponding load functions.
    ///
    /// [`TxGraph`]: <http://docs.rs/bdk_chain/0.23.0/bdk_chain/tx_graph/struct.TxGraph.html>
    pub fn read_tx_graph<A: AnchorWithMetaData>(
        &self,
        changeset: &mut tx_graph::ChangeSet<A>,
    ) -> Result<(), StoreError> {
        let read_tx = self.db.begin_read()?;
        self.read_txs(&read_tx, &mut changeset.txs)?;
        self.read_txouts(&read_tx, &mut changeset.txouts)?;
        self.read_anchors::<A>(&read_tx, &mut changeset.anchors)?;
        self.read_last_seen(&read_tx, &mut changeset.last_seen)?;
        self.read_last_evicted(&read_tx, &mut changeset.last_evicted)?;
        self.read_first_seen(&read_tx, &mut changeset.first_seen)?;
        Ok(())
    }

    /// This function loads the [`indexer`] structures from our db. It loads each
    /// field by calling corresponding load functions.
    ///
    /// [`indexer`]: <https://docs.rs/bdk_chain/0.23.0/bdk_chain/indexer/index.html>
    pub fn read_indexer(
        &self,
        changeset: &mut keychain_txout::ChangeSet,
    ) -> Result<(), StoreError> {
        let read_tx = self.db.begin_read()?;
        self.read_last_revealed(&read_tx, &mut changeset.last_revealed)?;
        self.read_spks(&read_tx, &mut changeset.spk_cache)?;
        Ok(())
    }

    /// This function loads descriptors from db.
    pub fn read_keychains(
        &self,
        desc_changeset: &mut BTreeMap<u64, Descriptor<DescriptorPublicKey>>,
    ) -> Result<(), StoreError> {
        let read_tx = self.db.begin_read()?;
        let table = read_tx.open_table(self.keychains_table_defn())?;

        for entry in table.iter()? {
            let (label, keychain) = entry?;
            desc_changeset.insert(
                label.value(),
                Descriptor::<DescriptorPublicKey>::from_str(keychain.value().as_str())
                    .expect("should be valid descriptors"),
            );
        }

        Ok(())
    }

    /// This function loads the [`Network`] from our db.
    /// <div class="warning">Warning: Do Not use with MAINNET</div>
    ///
    /// [`Network`]: <https://docs.rs/bitcoin/latest/bitcoin/enum.Network.html>
    pub fn read_network(&self, network: &mut Option<bitcoin::Network>) -> Result<(), StoreError> {
        let read_tx = self.db.begin_read()?;
        let table = read_tx.open_table(NETWORK)?;
        *network = table
            .get(&*self.wallet_name)?
            .map(|network| Network::from_str(&network.value()).expect("should be valid network"));
        Ok(())
    }

    /// This function loads the [`LocalChain`] structure from our db. It loads each
    /// field by calling corresponding load functions.
    ///
    /// [`LocalChain`]: <http://docs.rs/bdk_chain/0.23.0/bdk_chain/local_chain/struct.LocalChain.html>
    pub fn read_local_chain(
        &self,
        changeset: &mut local_chain::ChangeSet,
    ) -> Result<(), StoreError> {
        let read_tx = self.db.begin_read()?;
        self.read_blocks(&read_tx, &mut changeset.blocks)?;
        Ok(())
    }

    // This function loads blocks corresponding to local_chain .
    fn read_blocks(
        &self,
        read_tx: &ReadTransaction,
        blocks: &mut BTreeMap<u32, Option<BlockHash>>,
    ) -> Result<(), StoreError> {
        let table = read_tx.open_table(self.blocks_table_defn())?;

        for entry in table.iter()? {
            let (height, hash) = entry?;
            blocks.insert(
                height.value(),
                Some(BlockHash::from_byte_array(hash.value())),
            );
        }

        Ok(())
    }

    // This function loads txs corresponding to tx_graph.
    fn read_txs(
        &self,
        read_tx: &ReadTransaction,
        txs: &mut BTreeSet<Arc<Transaction>>,
    ) -> Result<(), StoreError> {
        let table = read_tx.open_table(self.txs_table_defn())?;

        for entry in table.iter()? {
            let tx_vec = entry?.1.value();
            let tx = ciborium::from_reader(tx_vec.as_slice())?;
            txs.insert(Arc::new(tx));
        }
        Ok(())
    }

    // This function loads txouts corresponding to tx_graph.
    fn read_txouts(
        &self,
        read_tx: &ReadTransaction,
        txouts: &mut BTreeMap<OutPoint, TxOut>,
    ) -> Result<(), StoreError> {
        let table = read_tx.open_table(self.txouts_table_defn())?;

        for entry in table.iter()? {
            let (outpoint, txout) = entry?;
            txouts.insert(
                OutPoint {
                    txid: Txid::from_byte_array(outpoint.value().0),
                    vout: outpoint.value().1,
                },
                TxOut {
                    value: Amount::from_sat(txout.value().0),
                    script_pubkey: ScriptBuf::from_bytes(txout.value().1),
                },
            );
        }

        Ok(())
    }

    // This function loads anchors corresponding to tx_graph.
    fn read_anchors<A: AnchorWithMetaData>(
        &self,
        read_tx: &ReadTransaction,
        anchors: &mut BTreeSet<(A, Txid)>,
    ) -> Result<(), StoreError> {
        let table = read_tx.open_table(self.anchors_table_defn::<A>())?;

        for entry in table.iter()? {
            let (anchor, metadata) = entry?;
            let (txid_bytes, block_id_bytes) = anchor.value();
            let block_id = BlockId {
                height: u32::from_le_bytes(
                    block_id_bytes[0..4].try_into().expect("slice has length 4"),
                ),
                hash: BlockHash::from_slice(&block_id_bytes[4..])?,
            };
            anchors.insert((
                A::from_id(block_id, metadata.value()),
                Txid::from_byte_array(txid_bytes),
            ));
        }

        Ok(())
    }

    // This function loads last_seen flags corresponding to tx_graph.
    fn read_last_seen(
        &self,
        read_tx: &ReadTransaction,
        last_seen: &mut BTreeMap<Txid, u64>,
    ) -> Result<(), StoreError> {
        let table = read_tx.open_table(self.last_seen_defn())?;

        for entry in table.iter()? {
            let (txid, last_seen_num) = entry?;
            last_seen.insert(Txid::from_byte_array(txid.value()), last_seen_num.value());
        }
        Ok(())
    }

    // This function loads last_evicted flags corresponding to tx_graph .
    fn read_last_evicted(
        &self,
        read_tx: &ReadTransaction,
        last_evicted: &mut BTreeMap<Txid, u64>,
    ) -> Result<(), StoreError> {
        let table = read_tx.open_table(self.last_evicted_table_defn())?;

        for entry in table.iter()? {
            let (txid, last_evicted_num) = entry?;
            last_evicted.insert(
                Txid::from_byte_array(txid.value()),
                last_evicted_num.value(),
            );
        }
        Ok(())
    }

    // This function loads first_seen flags corresponding to tx_graph.
    fn read_first_seen(
        &self,
        read_tx: &ReadTransaction,
        first_seen: &mut BTreeMap<Txid, u64>,
    ) -> Result<(), StoreError> {
        let table = read_tx.open_table(self.first_seen_table_defn())?;

        for entry in table.iter()? {
            let (txid, first_seen_num) = entry?;
            first_seen.insert(Txid::from_byte_array(txid.value()), first_seen_num.value());
        }
        Ok(())
    }

    // This function loads last_revealed corresponding to keychain_txout .
    fn read_last_revealed(
        &self,
        read_tx: &ReadTransaction,
        last_revealed: &mut BTreeMap<DescriptorId, u32>,
    ) -> Result<(), StoreError> {
        let table = read_tx.open_table(self.last_revealed_table_defn())?;

        for entry in table.iter()? {
            let (desc, last_revealed_idx) = entry?;
            last_revealed.insert(
                DescriptorId::from_byte_array(desc.value()),
                last_revealed_idx.value(),
            );
        }
        Ok(())
    }

    // This function loads spk_cache corresponding to keychain_txout .
    fn read_spks(
        &self,
        read_tx: &ReadTransaction,
        spk_cache: &mut BTreeMap<DescriptorId, BTreeMap<u32, ScriptBuf>>,
    ) -> Result<(), StoreError> {
        let table = read_tx.open_table(self.spk_table_defn())?;

        for entry in table.iter()? {
            let (desc, spk) = entry?;
            spk_cache
                .entry(DescriptorId::from_byte_array(desc.value().0))
                .or_default()
                .insert(desc.value().1, ScriptBuf::from_bytes(spk.value()));
        }
        Ok(())
    }
}

#[cfg(feature = "wallet")]
impl WalletPersister for Store {
    type Error = StoreError;
    fn initialize(persister: &mut Self) -> Result<ChangeSet, Self::Error> {
        persister.create_tables::<ConfirmationBlockTime>()?;
        let mut changeset = ChangeSet::default();
        persister.read_wallet(&mut changeset)?;
        Ok(changeset)
    }

    fn persist(persister: &mut Self, changeset: &ChangeSet) -> Result<(), Self::Error> {
        persister.persist_wallet(changeset)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bdk_chain::BlockId;
    use bdk_chain::ConfirmationBlockTime;
    use bdk_chain::{
        DescriptorExt, Merge,
        bitcoin::{
            self, Amount, BlockHash, OutPoint, ScriptBuf, Transaction, TxIn, TxOut, absolute,
            hashes::Hash, transaction, transaction::Txid,
        },
        keychain_txout, local_chain,
        miniscript::descriptor::Descriptor,
    };
    #[cfg(feature = "wallet")]
    use bdk_wallet::keys::DescriptorPublicKey;
    use std::sync::Arc;
    use std::{collections::BTreeMap, path::Path};
    use tempfile::NamedTempFile;

    use bdk_testenv::{block_id, hash, utils};

    const DESCRIPTORS: [&str; 4] = [
        "tr([5940b9b9/86'/0'/0']tpubDDVNqmq75GNPWQ9UNKfP43UwjaHU4GYfoPavojQbfpyfZp2KetWgjGBRRAy4tYCrAA6SB11mhQAkqxjh1VtQHyKwT4oYxpwLaGHvoKmtxZf/1/*)#ypcpw2dr",
        "tr([5940b9b9/86'/0'/0']tpubDDVNqmq75GNPWQ9UNKfP43UwjaHU4GYfoPavojQbfpyfZp2KetWgjGBRRAy4tYCrAA6SB11mhQAkqxjh1VtQHyKwT4oYxpwLaGHvoKmtxZf/0/*)#44aqnlam",
        "wpkh([41f2aed0/84h/1h/0h]tpubDDFSdQWw75hk1ewbwnNpPp5DvXFRKt68ioPoyJDY752cNHKkFxPWqkqCyCf4hxrEfpuxh46QisehL3m8Bi6MsAv394QVLopwbtfvryFQNUH/1/*)#emtwewtk",
        "wpkh([41f2aed0/84h/1h/0h]tpubDDFSdQWw75hk1ewbwnNpPp5DvXFRKt68ioPoyJDY752cNHKkFxPWqkqCyCf4hxrEfpuxh46QisehL3m8Bi6MsAv394QVLopwbtfvryFQNUH/0/*)#g0w0ymmw",
    ];

    fn create_db(path: impl AsRef<Path>) -> Database {
        Database::create(path).unwrap()
    }

    fn create_test_store(db: Arc<Database>, wallet_name: &str) -> Store {
        Store::new(db, wallet_name.to_string()).unwrap()
    }

    #[test]
    fn test_network_persistence() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = create_db(tmpfile.path());
        let store = create_test_store(Arc::new(db), "wallet1");
        store.create_network_table().unwrap();
        let network_changeset = Some(Network::Bitcoin);
        store.persist_network(&network_changeset).unwrap();

        let mut network_changeset = Some(Network::Regtest);
        store.read_network(&mut network_changeset).unwrap();

        assert_eq!(network_changeset, Some(Network::Bitcoin));
    }

    #[cfg(feature = "wallet")]
    #[test]
    fn test_keychains_persistence() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = create_db(tmpfile.path());
        let store = create_test_store(Arc::new(db), "wallet1");
        store.create_keychains_table().unwrap();

        let descriptor: Descriptor<DescriptorPublicKey> = DESCRIPTORS[0].parse().unwrap();
        let change_descriptor: Descriptor<DescriptorPublicKey> = DESCRIPTORS[1].parse().unwrap();
        let desc_changeset: BTreeMap<u64, Descriptor<DescriptorPublicKey>> =
            [(0, descriptor.clone()), (1, change_descriptor.clone())].into();

        store.persist_keychains(&desc_changeset).unwrap();

        let mut desc_changeset: BTreeMap<u64, Descriptor<DescriptorPublicKey>> = BTreeMap::new();
        store.read_keychains(&mut desc_changeset).unwrap();

        assert_eq!(*desc_changeset.get(&0).unwrap(), descriptor);
        assert_eq!(*desc_changeset.get(&1).unwrap(), change_descriptor);
    }

    #[cfg(feature = "wallet")]
    #[test]
    fn test_keychains_persistence_second() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = create_db(tmpfile.path());
        let store = create_test_store(Arc::new(db), "wallet1");
        store.create_keychains_table().unwrap();

        let descriptor: Descriptor<DescriptorPublicKey> = DESCRIPTORS[1].parse().unwrap();
        let change_descriptor: Descriptor<DescriptorPublicKey> = DESCRIPTORS[0].parse().unwrap();

        let desc_changeset: BTreeMap<u64, Descriptor<DescriptorPublicKey>> =
            [(0, descriptor.clone()), (1, change_descriptor.clone())].into();

        store.persist_keychains(&desc_changeset).unwrap();

        let mut desc_changeset: BTreeMap<u64, Descriptor<DescriptorPublicKey>> = BTreeMap::new();
        store.read_keychains(&mut desc_changeset).unwrap();

        assert_eq!(*desc_changeset.get(&0).unwrap(), descriptor);
        assert_eq!(*desc_changeset.get(&1).unwrap(), change_descriptor);
    }

    #[cfg(feature = "wallet")]
    #[test]
    fn test_single_desc_persistence() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = create_db(tmpfile.path());
        let store = create_test_store(Arc::new(db), "wallet1");
        store.create_keychains_table().unwrap();

        let descriptor: Descriptor<DescriptorPublicKey> = DESCRIPTORS[0].parse().unwrap();

        store
            .persist_keychains(&[(0, descriptor.clone())].into())
            .unwrap();

        let mut desc_changeset: BTreeMap<u64, Descriptor<DescriptorPublicKey>> = BTreeMap::new();
        store.read_keychains(&mut desc_changeset).unwrap();

        assert_eq!(*desc_changeset.get(&0).unwrap(), descriptor);
        assert_eq!(desc_changeset.get(&1), None);
    }

    #[test]
    fn test_local_chain_persistence() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = create_db(tmpfile.path());
        let store = create_test_store(Arc::new(db), "wallet1");

        // create a local_chain_changeset, persist that and read it
        let local_chain_changeset = local_chain::ChangeSet {
            blocks: [
                (0, Some(hash!("B"))),
                (1, Some(hash!("D"))),
                (2, Some(hash!("K"))),
            ]
            .into(),
        };
        store.create_local_chain_tables().unwrap();
        store.persist_local_chain(&local_chain_changeset).unwrap();

        let mut changeset = local_chain::ChangeSet::default();
        store.read_local_chain(&mut changeset).unwrap();
        assert_eq!(local_chain_changeset, changeset);

        // create another local_chain_changeset, persist that and read it
        let local_chain_changeset = local_chain::ChangeSet {
            blocks: [(2, None)].into(),
        };

        store.persist_local_chain(&local_chain_changeset).unwrap();

        let mut changeset = local_chain::ChangeSet::default();
        store.read_local_chain(&mut changeset).unwrap();

        let local_chain_changeset = local_chain::ChangeSet {
            blocks: [(0, Some(hash!("B"))), (1, Some(hash!("D")))].into(),
        };

        assert_eq!(local_chain_changeset, changeset);
    }

    #[test]
    fn test_blocks_persistence() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = create_db(tmpfile.path());
        let store = create_test_store(Arc::new(db), "wallet1");
        let blocks: BTreeMap<u32, Option<BlockHash>> = [
            (0, Some(hash!("B"))),
            (1, Some(hash!("D"))),
            (2, Some(hash!("K"))),
        ]
        .into();

        let write_tx = store.db.begin_write().unwrap();
        let _ = write_tx.open_table(store.blocks_table_defn()).unwrap();
        store.persist_blocks(&write_tx, &blocks).unwrap();
        write_tx.commit().unwrap();
        let read_tx = store.db.begin_read().unwrap();
        let mut blocks_new: BTreeMap<u32, Option<BlockHash>> = BTreeMap::new();
        store.read_blocks(&read_tx, &mut blocks_new).unwrap();
        assert_eq!(blocks_new, blocks);

        let blocks: BTreeMap<u32, Option<BlockHash>> = [(2, None)].into();

        let write_tx = store.db.begin_write().unwrap();
        store.persist_blocks(&write_tx, &blocks).unwrap();
        write_tx.commit().unwrap();
        let read_tx = store.db.begin_read().unwrap();
        let mut blocks_new: BTreeMap<u32, Option<BlockHash>> = BTreeMap::new();
        store.read_blocks(&read_tx, &mut blocks_new).unwrap();

        let blocks: BTreeMap<u32, Option<BlockHash>> =
            [(0, Some(hash!("B"))), (1, Some(hash!("D")))].into();

        assert_eq!(blocks, blocks_new);
    }

    fn create_one_inp_one_out_tx(txid: Txid, amount: u64) -> Transaction {
        Transaction {
            version: transaction::Version::ONE,
            lock_time: absolute::LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::new(txid, 0),
                ..TxIn::default()
            }],
            output: vec![TxOut {
                value: Amount::from_sat(amount),
                script_pubkey: ScriptBuf::new(),
            }],
        }
    }

    #[test]
    fn test_persist_last_seen() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = create_db(tmpfile.path());
        let store = create_test_store(Arc::new(db), "wallet1");

        let tx1 = Arc::new(create_one_inp_one_out_tx(
            Txid::from_byte_array([0; 32]),
            30_000,
        ));
        let tx2 = Arc::new(create_one_inp_one_out_tx(tx1.compute_txid(), 20_000));
        let tx3 = Arc::new(create_one_inp_one_out_tx(tx2.compute_txid(), 19_000));

        // try persisting and reading last_seen
        let txs: BTreeSet<Arc<Transaction>> = [tx1.clone(), tx2.clone()].into();
        let mut last_seen: BTreeMap<Txid, u64> =
            [(tx1.compute_txid(), 100), (tx2.compute_txid(), 120)].into();

        let write_tx = store.db.begin_write().unwrap();
        let _ = write_tx.open_table(store.txs_table_defn()).unwrap();
        let _ = write_tx.open_table(store.last_seen_defn()).unwrap();
        write_tx.commit().unwrap();

        let write_tx = store.db.begin_write().unwrap();
        let read_tx = store.db.begin_read().unwrap();
        store
            .persist_last_seen(&write_tx, &read_tx, &last_seen, &txs)
            .unwrap();
        write_tx.commit().unwrap();

        let read_tx = store.db.begin_read().unwrap();
        let mut last_seen_read: BTreeMap<Txid, u64> = BTreeMap::new();
        store.read_last_seen(&read_tx, &mut last_seen_read).unwrap();
        assert_eq!(last_seen_read, last_seen);

        // persist another last_seen and see if what is read is same as merged one
        let txs_new: BTreeSet<Arc<Transaction>> = [tx3.clone()].into();
        let last_seen_new: BTreeMap<Txid, u64> = [(tx3.compute_txid(), 200)].into();

        let write_tx = store.db.begin_write().unwrap();
        let _ = write_tx.open_table(store.txs_table_defn()).unwrap();
        let _ = write_tx.open_table(store.last_seen_defn()).unwrap();
        write_tx.commit().unwrap();

        let write_tx = store.db.begin_write().unwrap();
        let read_tx = store.db.begin_read().unwrap();
        store
            .persist_last_seen(&write_tx, &read_tx, &last_seen_new, &txs_new)
            .unwrap();
        write_tx.commit().unwrap();

        let read_tx = store.db.begin_read().unwrap();
        let mut last_seen_read_new: BTreeMap<Txid, u64> = BTreeMap::new();
        store
            .read_last_seen(&read_tx, &mut last_seen_read_new)
            .unwrap();
        last_seen.merge(last_seen_new);
        assert_eq!(last_seen_read_new, last_seen);
    }

    #[test]
    fn test_persist_last_evicted() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = create_db(tmpfile.path());
        let store = create_test_store(Arc::new(db), "wallet1");

        let tx1 = Arc::new(create_one_inp_one_out_tx(
            Txid::from_byte_array([0; 32]),
            30_000,
        ));
        let tx2 = Arc::new(create_one_inp_one_out_tx(tx1.compute_txid(), 20_000));
        let tx3 = Arc::new(create_one_inp_one_out_tx(tx2.compute_txid(), 19_000));

        let txs: BTreeSet<Arc<Transaction>> = [tx1.clone(), tx2.clone()].into();
        let mut last_evicted: BTreeMap<Txid, u64> =
            [(tx1.compute_txid(), 100), (tx2.compute_txid(), 120)].into();

        let write_tx = store.db.begin_write().unwrap();
        let _ = write_tx.open_table(store.txs_table_defn()).unwrap();
        let _ = write_tx
            .open_table(store.last_evicted_table_defn())
            .unwrap();
        write_tx.commit().unwrap();

        let write_tx = store.db.begin_write().unwrap();
        let read_tx = store.db.begin_read().unwrap();
        store
            .persist_last_evicted(&write_tx, &read_tx, &last_evicted, &txs)
            .unwrap();
        write_tx.commit().unwrap();

        let read_tx = store.db.begin_read().unwrap();
        let mut last_evicted_read: BTreeMap<Txid, u64> = BTreeMap::new();
        store
            .read_last_evicted(&read_tx, &mut last_evicted_read)
            .unwrap();
        assert_eq!(last_evicted_read, last_evicted);

        let txs_new: BTreeSet<Arc<Transaction>> = [tx3.clone()].into();
        let last_evicted_new: BTreeMap<Txid, u64> = [(tx3.compute_txid(), 300)].into();

        let write_tx = store.db.begin_write().unwrap();
        let _ = write_tx.open_table(store.txs_table_defn()).unwrap();
        let _ = write_tx
            .open_table(store.last_evicted_table_defn())
            .unwrap();
        write_tx.commit().unwrap();

        let write_tx = store.db.begin_write().unwrap();
        let read_tx = store.db.begin_read().unwrap();
        store
            .persist_last_evicted(&write_tx, &read_tx, &last_evicted_new, &txs_new)
            .unwrap();
        write_tx.commit().unwrap();

        let read_tx = store.db.begin_read().unwrap();
        let mut last_evicted_read_new: BTreeMap<Txid, u64> = BTreeMap::new();
        store
            .read_last_evicted(&read_tx, &mut last_evicted_read_new)
            .unwrap();
        last_evicted.merge(last_evicted_new);
        assert_eq!(last_evicted_read_new, last_evicted);
    }

    #[test]
    fn test_persist_first_seen() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = create_db(tmpfile.path());
        let store = create_test_store(Arc::new(db), "wallet1");

        let tx1 = Arc::new(create_one_inp_one_out_tx(
            Txid::from_byte_array([0; 32]),
            30_000,
        ));
        let tx2 = Arc::new(create_one_inp_one_out_tx(tx1.compute_txid(), 20_000));
        let tx3 = Arc::new(create_one_inp_one_out_tx(tx2.compute_txid(), 19_000));

        let txs: BTreeSet<Arc<Transaction>> = [tx1.clone(), tx2.clone()].into();
        let mut first_seen: BTreeMap<Txid, u64> =
            [(tx1.compute_txid(), 100), (tx2.compute_txid(), 120)].into();

        let write_tx = store.db.begin_write().unwrap();
        let _ = write_tx.open_table(store.txs_table_defn()).unwrap();
        let _ = write_tx.open_table(store.first_seen_table_defn()).unwrap();
        write_tx.commit().unwrap();

        let write_tx = store.db.begin_write().unwrap();
        let read_tx = store.db.begin_read().unwrap();
        store
            .persist_first_seen(&write_tx, &read_tx, &first_seen, &txs)
            .unwrap();
        write_tx.commit().unwrap();

        let read_tx = store.db.begin_read().unwrap();
        let mut first_seen_read: BTreeMap<Txid, u64> = BTreeMap::new();
        store
            .read_first_seen(&read_tx, &mut first_seen_read)
            .unwrap();
        assert_eq!(first_seen_read, first_seen);

        let txs_new: BTreeSet<Arc<Transaction>> = [tx3.clone()].into();
        let first_seen_new: BTreeMap<Txid, u64> = [(tx3.compute_txid(), 200)].into();

        let write_tx = store.db.begin_write().unwrap();
        let _ = write_tx.open_table(store.txs_table_defn()).unwrap();
        let _ = write_tx.open_table(store.first_seen_table_defn()).unwrap();
        write_tx.commit().unwrap();

        let write_tx = store.db.begin_write().unwrap();
        let read_tx = store.db.begin_read().unwrap();
        store
            .persist_first_seen(&write_tx, &read_tx, &first_seen_new, &txs_new)
            .unwrap();
        write_tx.commit().unwrap();

        let read_tx = store.db.begin_read().unwrap();
        let mut first_seen_read_new: BTreeMap<Txid, u64> = BTreeMap::new();
        store
            .read_first_seen(&read_tx, &mut first_seen_read_new)
            .unwrap();
        first_seen.merge(first_seen_new);
        assert_eq!(first_seen_read_new, first_seen);
    }

    #[test]
    fn test_persist_txouts() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = create_db(tmpfile.path());
        let store = create_test_store(Arc::new(db), "wallet1");

        let mut txouts: BTreeMap<OutPoint, TxOut> = [
            (
                OutPoint::new(Txid::from_byte_array([0; 32]), 0),
                TxOut {
                    value: Amount::from_sat(1300),
                    script_pubkey: ScriptBuf::from_bytes(vec![0]),
                },
            ),
            (
                OutPoint::new(Txid::from_byte_array([1; 32]), 0),
                TxOut {
                    value: Amount::from_sat(1400),
                    script_pubkey: ScriptBuf::from_bytes(vec![2]),
                },
            ),
        ]
        .into();

        let write_tx = store.db.begin_write().unwrap();
        let _ = write_tx.open_table(store.txouts_table_defn()).unwrap();
        store.persist_txouts(&write_tx, &txouts).unwrap();
        write_tx.commit().unwrap();

        let read_tx = store.db.begin_read().unwrap();
        let mut txouts_read: BTreeMap<OutPoint, TxOut> = BTreeMap::new();
        store.read_txouts(&read_tx, &mut txouts_read).unwrap();
        assert_eq!(txouts, txouts_read);

        let txouts_new: BTreeMap<OutPoint, TxOut> = [(
            OutPoint::new(Txid::from_byte_array([2; 32]), 0),
            TxOut {
                value: Amount::from_sat(10000),
                script_pubkey: ScriptBuf::from_bytes(vec![1]),
            },
        )]
        .into();
        let write_tx = store.db.begin_write().unwrap();
        let _ = write_tx.open_table(store.txouts_table_defn()).unwrap();
        store.persist_txouts(&write_tx, &txouts_new).unwrap();
        write_tx.commit().unwrap();

        let read_tx = store.db.begin_read().unwrap();
        let mut txouts_read_new: BTreeMap<OutPoint, TxOut> = BTreeMap::new();
        store.read_txouts(&read_tx, &mut txouts_read_new).unwrap();

        txouts.merge(txouts_new);
        assert_eq!(txouts, txouts_read_new);
    }

    #[test]
    fn test_persist_txs() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = create_db(tmpfile.path());
        let store = create_test_store(Arc::new(db), "wallet1");

        let tx1 = Arc::new(create_one_inp_one_out_tx(
            Txid::from_byte_array([0; 32]),
            30_000,
        ));
        let tx2 = Arc::new(create_one_inp_one_out_tx(tx1.compute_txid(), 20_000));
        let tx3 = Arc::new(create_one_inp_one_out_tx(tx2.compute_txid(), 19_000));

        let mut txs: BTreeSet<Arc<Transaction>> = [tx1, tx2.clone()].into();

        let write_tx = store.db.begin_write().unwrap();
        let _ = write_tx.open_table(store.txs_table_defn()).unwrap();
        store.persist_txs(&write_tx, &txs).unwrap();
        write_tx.commit().unwrap();

        let read_tx = store.db.begin_read().unwrap();
        let mut txs_read: BTreeSet<Arc<Transaction>> = BTreeSet::new();
        store.read_txs(&read_tx, &mut txs_read).unwrap();
        assert_eq!(txs_read, txs);

        let txs_new: BTreeSet<Arc<Transaction>> = [tx3].into();
        let write_tx = store.db.begin_write().unwrap();
        let _ = write_tx.open_table(store.txs_table_defn()).unwrap();
        store.persist_txs(&write_tx, &txs_new).unwrap();
        write_tx.commit().unwrap();

        let read_tx = store.db.begin_read().unwrap();
        let mut txs_read_new: BTreeSet<Arc<Transaction>> = BTreeSet::new();
        store.read_txs(&read_tx, &mut txs_read_new).unwrap();

        txs.merge(txs_new);
        assert_eq!(txs_read_new, txs);
    }

    #[test]
    fn test_persist_anchors() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = create_db(tmpfile.path());
        let store = create_test_store(Arc::new(db), "wallet1");

        let tx1 = Arc::new(create_one_inp_one_out_tx(
            Txid::from_byte_array([0; 32]),
            30_000,
        ));
        let tx2 = Arc::new(create_one_inp_one_out_tx(tx1.compute_txid(), 20_000));
        let tx3 = Arc::new(create_one_inp_one_out_tx(tx2.compute_txid(), 19_000));

        let anchor1 = ConfirmationBlockTime {
            block_id: block_id!(23, "BTC"),
            confirmation_time: 1756838400,
        };

        let anchor2 = ConfirmationBlockTime {
            block_id: block_id!(25, "BDK"),
            confirmation_time: 1756839600,
        };

        let txs: BTreeSet<Arc<Transaction>> = [tx1.clone(), tx2.clone()].into();
        let mut anchors = [(anchor1, tx1.compute_txid()), (anchor2, tx2.compute_txid())].into();

        let write_tx = store.db.begin_write().unwrap();
        let _ = write_tx.open_table(store.txs_table_defn()).unwrap();
        let _ = write_tx
            .open_table(store.anchors_table_defn::<ConfirmationBlockTime>())
            .unwrap();
        write_tx.commit().unwrap();

        let write_tx = store.db.begin_write().unwrap();
        let read_tx = store.db.begin_read().unwrap();
        store
            .persist_anchors(&write_tx, &read_tx, &anchors, &txs)
            .unwrap();
        read_tx.close().unwrap();
        write_tx.commit().unwrap();

        let read_tx = store.db.begin_read().unwrap();
        let mut anchors_read: BTreeSet<(ConfirmationBlockTime, Txid)> = BTreeSet::new();
        store.read_anchors(&read_tx, &mut anchors_read).unwrap();
        assert_eq!(anchors_read, anchors);

        let txs_new: BTreeSet<Arc<Transaction>> = [tx3.clone()].into();
        let anchors_new: BTreeSet<(ConfirmationBlockTime, Txid)> =
            [(anchor2, tx3.compute_txid())].into();

        let write_tx = store.db.begin_write().unwrap();
        let read_tx = store.db.begin_read().unwrap();
        store
            .persist_anchors(&write_tx, &read_tx, &anchors_new, &txs_new)
            .unwrap();
        read_tx.close().unwrap();
        write_tx.commit().unwrap();

        let read_tx = store.db.begin_read().unwrap();
        let mut anchors_read_new: BTreeSet<(ConfirmationBlockTime, Txid)> = BTreeSet::new();
        store.read_anchors(&read_tx, &mut anchors_read_new).unwrap();

        anchors.merge(anchors_new);
        assert_eq!(anchors_read_new, anchors);
    }

    #[test]
    fn test_persist_anchors_blockid() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = create_db(tmpfile.path());
        let store = create_test_store(Arc::new(db), "wallet1");

        let tx1 = Arc::new(create_one_inp_one_out_tx(
            Txid::from_byte_array([0; 32]),
            30_000,
        ));
        let tx2 = Arc::new(create_one_inp_one_out_tx(tx1.compute_txid(), 20_000));
        let tx3 = Arc::new(create_one_inp_one_out_tx(tx2.compute_txid(), 19_000));

        let anchor1 = block_id!(23, "BTC");

        let anchor2 = block_id!(25, "BDK");

        let txs: BTreeSet<Arc<Transaction>> = [tx1.clone(), tx2.clone()].into();
        let mut anchors = [(anchor1, tx1.compute_txid()), (anchor2, tx2.compute_txid())].into();

        let write_tx = store.db.begin_write().unwrap();
        let _ = write_tx.open_table(store.txs_table_defn()).unwrap();
        let _ = write_tx
            .open_table(store.anchors_table_defn::<BlockId>())
            .unwrap();
        write_tx.commit().unwrap();

        let write_tx = store.db.begin_write().unwrap();
        let read_tx = store.db.begin_read().unwrap();
        store
            .persist_anchors(&write_tx, &read_tx, &anchors, &txs)
            .unwrap();
        read_tx.close().unwrap();
        write_tx.commit().unwrap();

        let read_tx = store.db.begin_read().unwrap();
        let mut anchors_read: BTreeSet<(BlockId, Txid)> = BTreeSet::new();
        store.read_anchors(&read_tx, &mut anchors_read).unwrap();
        assert_eq!(anchors_read, anchors);

        let txs_new: BTreeSet<Arc<Transaction>> = [tx3.clone()].into();
        let anchors_new: BTreeSet<(BlockId, Txid)> = [(anchor2, tx3.compute_txid())].into();

        let write_tx = store.db.begin_write().unwrap();
        let read_tx = store.db.begin_read().unwrap();
        store
            .persist_anchors(&write_tx, &read_tx, &anchors_new, &txs_new)
            .unwrap();
        read_tx.close().unwrap();
        write_tx.commit().unwrap();

        let read_tx = store.db.begin_read().unwrap();
        let mut anchors_read_new: BTreeSet<(BlockId, Txid)> = BTreeSet::new();
        store.read_anchors(&read_tx, &mut anchors_read_new).unwrap();

        anchors.merge(anchors_new);
        assert_eq!(anchors_read_new, anchors);
    }

    #[test]
    fn test_tx_graph_persistence() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = create_db(tmpfile.path());
        let store = create_test_store(Arc::new(db), "wallet1");
        let tx1 = Arc::new(create_one_inp_one_out_tx(
            Txid::from_byte_array([0; 32]),
            30_000,
        ));
        let tx2 = Arc::new(create_one_inp_one_out_tx(tx1.compute_txid(), 20_000));
        let block_id = block_id!(100, "B");

        let conf_anchor: ConfirmationBlockTime = ConfirmationBlockTime {
            block_id,
            confirmation_time: 1,
        };

        let mut tx_graph_changeset1 = tx_graph::ChangeSet::<ConfirmationBlockTime> {
            txs: [tx1.clone()].into(),
            txouts: [].into(),
            anchors: [(conf_anchor, tx1.compute_txid())].into(),
            last_seen: [(tx1.compute_txid(), 100)].into(),
            first_seen: [(tx1.compute_txid(), 50)].into(),
            last_evicted: [(tx1.compute_txid(), 150)].into(),
        };

        store
            .create_tx_graph_tables::<ConfirmationBlockTime>()
            .unwrap();

        store.persist_tx_graph(&tx_graph_changeset1).unwrap();

        let mut changeset = tx_graph::ChangeSet::default();
        store.read_tx_graph(&mut changeset).unwrap();
        assert_eq!(changeset, tx_graph_changeset1);

        let block_id = block_id!(101, "REDB");

        let conf_anchor: ConfirmationBlockTime = ConfirmationBlockTime {
            block_id,
            confirmation_time: 1,
        };

        let tx_graph_changeset2 = tx_graph::ChangeSet::<ConfirmationBlockTime> {
            txs: [tx2.clone()].into(),
            txouts: [].into(),
            anchors: [(conf_anchor, tx2.compute_txid())].into(),
            last_seen: [(tx2.compute_txid(), 200)].into(),
            first_seen: [(tx2.compute_txid(), 100)].into(),
            last_evicted: [(tx2.compute_txid(), 150)].into(),
        };

        store.persist_tx_graph(&tx_graph_changeset2).unwrap();

        let mut changeset = tx_graph::ChangeSet::default();
        store.read_tx_graph(&mut changeset).unwrap();

        tx_graph_changeset1.merge(tx_graph_changeset2);

        assert_eq!(tx_graph_changeset1, changeset);
    }

    fn parse_descriptor(descriptor: &str) -> Descriptor<DescriptorPublicKey> {
        let secp = bdk_chain::bitcoin::secp256k1::Secp256k1::signing_only();
        Descriptor::<DescriptorPublicKey>::parse_descriptor(&secp, descriptor)
            .unwrap()
            .0
    }

    #[test]
    fn test_last_revealed_persistence() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = create_db(tmpfile.path());
        let store = create_test_store(Arc::new(db), "wallet1");

        let descriptor_ids = utils::DESCRIPTORS.map(|d| parse_descriptor(d).descriptor_id());

        let mut last_revealed: BTreeMap<DescriptorId, u32> =
            [(descriptor_ids[0], 1), (descriptor_ids[1], 100)].into();

        let write_tx = store.db.begin_write().unwrap();
        let _ = write_tx
            .open_table(store.last_revealed_table_defn())
            .unwrap();
        store
            .persist_last_revealed(&write_tx, &last_revealed)
            .unwrap();
        write_tx.commit().unwrap();

        let mut last_revealed_read: BTreeMap<DescriptorId, u32> = BTreeMap::new();
        let read_tx = store.db.begin_read().unwrap();
        store
            .read_last_revealed(&read_tx, &mut last_revealed_read)
            .unwrap();

        assert_eq!(last_revealed, last_revealed_read);

        let last_revealed_new: BTreeMap<DescriptorId, u32> = [(descriptor_ids[0], 2)].into();

        let write_tx = store.db.begin_write().unwrap();
        store
            .persist_last_revealed(&write_tx, &last_revealed_new)
            .unwrap();
        write_tx.commit().unwrap();

        let mut last_revealed_read_new: BTreeMap<DescriptorId, u32> = BTreeMap::new();
        let read_tx = store.db.begin_read().unwrap();
        store
            .read_last_revealed(&read_tx, &mut last_revealed_read_new)
            .unwrap();

        last_revealed.merge(last_revealed_new);

        assert_eq!(last_revealed, last_revealed_read_new);
    }

    #[test]
    fn test_spks_persistence() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = create_db(tmpfile.path());
        let store = create_test_store(Arc::new(db), "wallet1");

        let descriptor_ids = utils::DESCRIPTORS.map(|d| parse_descriptor(d).descriptor_id());

        let spk_cache: BTreeMap<DescriptorId, BTreeMap<u32, ScriptBuf>> = [
            (
                descriptor_ids[0],
                [(0u32, ScriptBuf::from_bytes(vec![1, 2, 3]))].into(),
            ),
            (
                descriptor_ids[1],
                [
                    (100u32, ScriptBuf::from_bytes(vec![3])),
                    (1000u32, ScriptBuf::from_bytes(vec![5, 6, 8])),
                ]
                .into(),
            ),
        ]
        .into();

        let write_tx = store.db.begin_write().unwrap();
        let _ = write_tx.open_table(store.spk_table_defn()).unwrap();
        store.persist_spks(&write_tx, &spk_cache).unwrap();
        write_tx.commit().unwrap();

        let mut spk_cache_read: BTreeMap<DescriptorId, BTreeMap<u32, ScriptBuf>> = BTreeMap::new();
        let read_tx = store.db.begin_read().unwrap();
        store.read_spks(&read_tx, &mut spk_cache_read).unwrap();

        assert_eq!(spk_cache, spk_cache_read);

        let spk_cache_new: BTreeMap<DescriptorId, BTreeMap<u32, ScriptBuf>> = [(
            descriptor_ids[0],
            [(1u32, ScriptBuf::from_bytes(vec![1, 2, 3, 4]))].into(),
        )]
        .into();

        let write_tx = store.db.begin_write().unwrap();
        let _ = write_tx.open_table(store.spk_table_defn()).unwrap();
        store.persist_spks(&write_tx, &spk_cache_new).unwrap();
        write_tx.commit().unwrap();

        let mut spk_cache_read_new: BTreeMap<DescriptorId, BTreeMap<u32, ScriptBuf>> =
            BTreeMap::new();
        let read_tx = store.db.begin_read().unwrap();
        store.read_spks(&read_tx, &mut spk_cache_read_new).unwrap();

        let spk_cache: BTreeMap<DescriptorId, BTreeMap<u32, ScriptBuf>> = [
            (
                descriptor_ids[0],
                [
                    (0u32, ScriptBuf::from_bytes(vec![1, 2, 3])),
                    (1u32, ScriptBuf::from_bytes(vec![1, 2, 3, 4])),
                ]
                .into(),
            ),
            (
                descriptor_ids[1],
                [
                    (100u32, ScriptBuf::from_bytes(vec![3])),
                    (1000u32, ScriptBuf::from_bytes(vec![5, 6, 8])),
                ]
                .into(),
            ),
        ]
        .into();

        assert_eq!(spk_cache, spk_cache_read_new);
    }

    #[test]
    fn test_indexer_persistence() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = create_db(tmpfile.path());
        let store = create_test_store(Arc::new(db), "wallet1");

        let descriptor_ids = utils::DESCRIPTORS.map(|d| parse_descriptor(d).descriptor_id());

        let mut keychain_txout_changeset = keychain_txout::ChangeSet {
            last_revealed: [(descriptor_ids[0], 1), (descriptor_ids[1], 100)].into(),
            spk_cache: [
                (
                    descriptor_ids[0],
                    [(0u32, ScriptBuf::from_bytes(vec![1, 2, 3]))].into(),
                ),
                (
                    descriptor_ids[1],
                    [
                        (100u32, ScriptBuf::from_bytes(vec![3])),
                        (1000u32, ScriptBuf::from_bytes(vec![5, 6, 8])),
                    ]
                    .into(),
                ),
            ]
            .into(),
        };

        store.create_indexer_tables().unwrap();
        store.persist_indexer(&keychain_txout_changeset).unwrap();

        let mut changeset = keychain_txout::ChangeSet::default();
        store.read_indexer(&mut changeset).unwrap();

        let keychain_txout_changeset_new = keychain_txout::ChangeSet {
            last_revealed: [(descriptor_ids[0], 2)].into(),
            spk_cache: [(
                descriptor_ids[0],
                [(1u32, ScriptBuf::from_bytes(vec![1, 2, 3]))].into(),
            )]
            .into(),
        };

        store
            .persist_indexer(&keychain_txout_changeset_new)
            .unwrap();

        let mut changeset_new = keychain_txout::ChangeSet::default();
        store.read_indexer(&mut changeset_new).unwrap();
        keychain_txout_changeset.merge(keychain_txout_changeset_new);

        assert_eq!(changeset_new, keychain_txout_changeset);
    }

    #[cfg(feature = "wallet")]
    #[test]
    fn test_persist_wallet() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = Arc::new(create_db(tmpfile.path()));
        let store = create_test_store(db, "wallet1");

        let descriptor: Descriptor<DescriptorPublicKey> = DESCRIPTORS[0].parse().unwrap();
        let change_descriptor: Descriptor<DescriptorPublicKey> = DESCRIPTORS[1].parse().unwrap();

        let mut blocks: BTreeMap<u32, Option<BlockHash>> = BTreeMap::new();
        blocks.insert(0u32, Some(hash!("B")));
        blocks.insert(1u32, Some(hash!("T")));
        blocks.insert(2u32, Some(hash!("C")));
        let local_chain_changeset = local_chain::ChangeSet { blocks };

        let tx1 = Arc::new(create_one_inp_one_out_tx(
            Txid::from_byte_array([0; 32]),
            30_000,
        ));
        let tx2 = Arc::new(create_one_inp_one_out_tx(tx1.compute_txid(), 20_000));

        let block_id = block_id!(1, "BDK");

        let conf_anchor: ConfirmationBlockTime = ConfirmationBlockTime {
            block_id,
            confirmation_time: 123,
        };

        let tx_graph_changeset = tx_graph::ChangeSet::<ConfirmationBlockTime> {
            txs: [tx1.clone()].into(),
            txouts: [].into(),
            anchors: [(conf_anchor, tx1.compute_txid())].into(),
            last_seen: [(tx1.compute_txid(), 100)].into(),
            first_seen: [(tx1.compute_txid(), 80)].into(),
            last_evicted: [(tx1.compute_txid(), 150)].into(),
        };

        let keychain_txout_changeset = keychain_txout::ChangeSet {
            last_revealed: [
                (descriptor.descriptor_id(), 12),
                (change_descriptor.descriptor_id(), 10),
            ]
            .into(),
            spk_cache: [
                (
                    descriptor.descriptor_id(),
                    [(0u32, ScriptBuf::from_bytes(vec![245, 123, 112]))].into(),
                ),
                (
                    change_descriptor.descriptor_id(),
                    [
                        (100u32, ScriptBuf::from_bytes(vec![145, 234, 98])),
                        (1000u32, ScriptBuf::from_bytes(vec![5, 6, 8])),
                    ]
                    .into(),
                ),
            ]
            .into(),
        };

        let mut changeset = ChangeSet {
            descriptor: Some(descriptor.clone()),
            change_descriptor: Some(change_descriptor.clone()),
            network: Some(Network::Bitcoin),
            local_chain: local_chain_changeset,
            tx_graph: tx_graph_changeset,
            indexer: keychain_txout_changeset,
        };

        store.create_tables::<ConfirmationBlockTime>().unwrap();

        store.persist_wallet(&changeset).unwrap();
        let mut changeset_read = ChangeSet::default();
        store.read_wallet(&mut changeset_read).unwrap();

        assert_eq!(changeset, changeset_read);

        let mut blocks: BTreeMap<u32, Option<BlockHash>> = BTreeMap::new();
        blocks.insert(4u32, Some(hash!("RE")));
        blocks.insert(5u32, Some(hash!("DB")));
        let local_chain_changeset = local_chain::ChangeSet { blocks };

        let block_id = block_id!(2, "Bitcoin");

        let conf_anchor: ConfirmationBlockTime = ConfirmationBlockTime {
            block_id,
            confirmation_time: 214,
        };

        let tx_graph_changeset = tx_graph::ChangeSet::<ConfirmationBlockTime> {
            txs: [tx2.clone()].into(),
            txouts: [].into(),
            anchors: [(conf_anchor, tx2.compute_txid())].into(),
            last_seen: [(tx2.compute_txid(), 200)].into(),
            first_seen: [(tx2.compute_txid(), 160)].into(),
            last_evicted: [(tx2.compute_txid(), 300)].into(),
        };

        let keychain_txout_changeset = keychain_txout::ChangeSet {
            last_revealed: [(descriptor.descriptor_id(), 14)].into(),
            spk_cache: [(
                change_descriptor.descriptor_id(),
                [
                    (102u32, ScriptBuf::from_bytes(vec![8, 45, 78])),
                    (1001u32, ScriptBuf::from_bytes(vec![29, 56, 47])),
                ]
                .into(),
            )]
            .into(),
        };

        let changeset_new = ChangeSet {
            descriptor: Some(descriptor),
            change_descriptor: Some(change_descriptor),
            network: Some(Network::Bitcoin),
            local_chain: local_chain_changeset,
            tx_graph: tx_graph_changeset,
            indexer: keychain_txout_changeset,
        };

        store.persist_wallet(&changeset_new).unwrap();
        let mut changeset_read_new = ChangeSet::default();
        store.read_wallet(&mut changeset_read_new).unwrap();

        changeset.merge(changeset_new);

        assert_eq!(changeset, changeset_read_new);
    }

    #[cfg(feature = "wallet")]
    #[test]
    fn test_persist_multi_wallet() {
        let tmpfile = NamedTempFile::new().unwrap();
        let db = Arc::new(create_db(tmpfile.path()));

        let store1 = create_test_store(db.clone(), "wallet1");

        let descriptor: Descriptor<DescriptorPublicKey> = DESCRIPTORS[0].parse().unwrap();
        let change_descriptor: Descriptor<DescriptorPublicKey> = DESCRIPTORS[1].parse().unwrap();

        let changeset1 = ChangeSet {
            descriptor: Some(descriptor.clone()),
            change_descriptor: Some(change_descriptor.clone()),
            network: Some(Network::Bitcoin),
            ..ChangeSet::default()
        };

        store1.create_tables::<ConfirmationBlockTime>().unwrap();
        store1.persist_wallet(&changeset1).unwrap();

        let store2 = create_test_store(db, "wallet2");

        let descriptor: Descriptor<DescriptorPublicKey> = DESCRIPTORS[2].parse().unwrap();
        let change_descriptor: Descriptor<DescriptorPublicKey> = DESCRIPTORS[3].parse().unwrap();

        let changeset2 = ChangeSet {
            descriptor: Some(descriptor.clone()),
            change_descriptor: Some(change_descriptor.clone()),
            network: Some(Network::Bitcoin),
            ..ChangeSet::default()
        };

        store2.create_tables::<ConfirmationBlockTime>().unwrap();
        store2.persist_wallet(&changeset2).unwrap();

        let mut changeset_read = ChangeSet::default();
        store1.read_wallet(&mut changeset_read).unwrap();
        assert_eq!(changeset_read, changeset1);

        let mut changeset_read = ChangeSet::default();
        store2.read_wallet(&mut changeset_read).unwrap();
        assert_eq!(changeset_read, changeset2);
    }
}
