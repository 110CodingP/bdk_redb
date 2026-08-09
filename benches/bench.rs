use std::sync::Arc;
use tempfile::tempdir;

use bdk_chain::bitcoin::hashes::Hash;
use bdk_chain::bitcoin::{Amount, BlockHash, Network};
use bdk_chain::{BlockId, ConfirmationBlockTime};
use bdk_file_store::Store as FileStore;
use bdk_redb::{Store as RedbStore, redb::Database};
use bdk_wallet::test_utils::{
    ReceiveTo, get_test_tr_single_sig_xprv_and_change_desc, insert_checkpoint, receive_output,
};
use bdk_wallet::{ChangeSet, KeychainKind, PersistedWallet, Wallet, WalletPersister};
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};

const NETWORK: Network = Network::Regtest;
const TEST_MAGIC_BYTES_LEN: usize = 12;
const TEST_MAGIC_BYTES: [u8; TEST_MAGIC_BYTES_LEN] =
    [98, 100, 107, 102, 115, 49, 49, 49, 49, 49, 49, 49];

fn create_wallet<D>(db: &mut D) -> PersistedWallet<D>
where
    D: WalletPersister,
    <D as WalletPersister>::Error: std::fmt::Debug,
{
    let (descriptor, change_descriptor) = get_test_tr_single_sig_xprv_and_change_desc();
    Wallet::create(descriptor, change_descriptor)
        .network(NETWORK)
        .create_wallet(db)
        .unwrap()
}

fn load_wallet<D>(db: &mut D) -> PersistedWallet<D>
where
    D: WalletPersister,
    <D as WalletPersister>::Error: std::fmt::Debug,
{
    let (descriptor, change_descriptor) = get_test_tr_single_sig_xprv_and_change_desc();
    Wallet::load()
        .descriptor(KeychainKind::External, Some(descriptor))
        .descriptor(KeychainKind::Internal, Some(change_descriptor))
        .check_network(NETWORK)
        .load_wallet(db)
        .unwrap()
        .expect("wallet should exist in persister")
}

/// Emulates a wallet sync by using `bdk_wallet::test_utils`:
/// - Reveals addresses on both External and Internal keychains (2 * num_addresses total)
/// - Creates a chain of block checkpoints
/// - Confirms transactions anchored to blocks
/// - Adds unconfirmed mempool transactions
fn simulate_sync(
    wallet: &mut Wallet,
    num_addresses: u32,
    num_blocks: u32,
    txs_per_block: u32,
    unconfirmed_txs: u32,
) {
    // 1. Reveal num_addresses on both External and Internal keychains (2 * num_addresses total)
    for _ in 0..num_addresses {
        let _ = wallet.reveal_next_address(KeychainKind::External);
        let _ = wallet.reveal_next_address(KeychainKind::Internal);
    }

    // 2. Build chain
    let base_height = 100_000;
    for b in 0..num_blocks {
        let height = base_height + b;
        let block_id = BlockId {
            height,
            hash: BlockHash::all_zeros(),
        };

        insert_checkpoint(wallet, block_id);

        let conf_anchor = ConfirmationBlockTime {
            block_id,
            confirmation_time: b as u64,
        };

        for _ in 0..txs_per_block {
            let amount = Amount::from_sat(50_000);
            let _ = receive_output(wallet, amount, ReceiveTo::Block(conf_anchor));
        }
    }

    // 3. Unconfirmed mempool transactions
    for u in 0..unconfirmed_txs {
        let amount = Amount::from_sat(25_000);
        let seen_at = u as u64;
        let _ = receive_output(wallet, amount, ReceiveTo::Mempool(seen_at));
    }
}

/// Workload scales for sync simulation
#[derive(Clone, Copy, Debug)]
struct SyncScale {
    name: &'static str,
    addresses: u32,
    blocks: u32,
    txs_per_block: u32,
    unconfirmed_txs: u32,
}

const SYNC_SCALES: [SyncScale; 3] = [
    SyncScale {
        name: "small_sync",
        addresses: 5,
        blocks: 5,
        txs_per_block: 100,
        unconfirmed_txs: 1000,
    },
    SyncScale {
        name: "medium_sync",
        addresses: 20,
        blocks: 20,
        txs_per_block: 100,
        unconfirmed_txs: 4000,
    },
    SyncScale {
        name: "large_sync",
        addresses: 50,
        blocks: 50,
        txs_per_block: 100,
        unconfirmed_txs: 10000,
    },
];

fn persist_desc_network(c: &mut Criterion) {
    let mut group = c.benchmark_group("persist_desc_network");

    group.bench_function("redb", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let db_path = dir.path().join("db.redb");
                let db = Arc::new(Database::create(db_path).unwrap());
                let store = RedbStore::new(db, "wallet".to_string()).unwrap();
                (store, dir)
            },
            |(mut store, dir)| {
                let wallet = create_wallet(&mut store);
                (wallet, store, dir)
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("file_store", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let path = dir.path().join("db_file");
                let store = FileStore::<ChangeSet>::create(&TEST_MAGIC_BYTES, &path)
                    .expect("must not open as file does not exist yet");
                (store, dir)
            },
            |(mut store, dir)| {
                let wallet = create_wallet(&mut store);
                (wallet, store, dir)
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

fn bench_persist_synced_wallet(c: &mut Criterion) {
    let mut group = c.benchmark_group("persist_synced_wallet");

    for scale in SYNC_SCALES {
        group.bench_with_input(BenchmarkId::new("redb", scale.name), &scale, |b, &scale| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let db_path = dir.path().join("db.redb");
                    let db = Arc::new(Database::create(db_path).unwrap());
                    let mut store = RedbStore::new(db, "wallet".to_string()).unwrap();
                    let mut wallet = create_wallet(&mut store);
                    simulate_sync(
                        &mut wallet,
                        scale.addresses,
                        scale.blocks,
                        scale.txs_per_block,
                        scale.unconfirmed_txs,
                    );
                    (wallet, store, dir)
                },
                |(mut wallet, mut store, dir)| {
                    wallet.persist(&mut store).unwrap();
                    (wallet, store, dir)
                },
                BatchSize::PerIteration,
            );
        });

        group.bench_with_input(
            BenchmarkId::new("file_store", scale.name),
            &scale,
            |b, &scale| {
                b.iter_batched(
                    || {
                        let dir = tempdir().unwrap();
                        let db_path = dir.path().join("db_file");
                        let mut store =
                            FileStore::<ChangeSet>::create(&TEST_MAGIC_BYTES, &db_path).unwrap();
                        let mut wallet = create_wallet(&mut store);
                        simulate_sync(
                            &mut wallet,
                            scale.addresses,
                            scale.blocks,
                            scale.txs_per_block,
                            scale.unconfirmed_txs,
                        );
                        (wallet, store, dir)
                    },
                    |(mut wallet, mut store, dir)| {
                        wallet.persist(&mut store).unwrap();
                        (wallet, store, dir)
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

fn bench_load_synced_wallet(c: &mut Criterion) {
    let mut group = c.benchmark_group("load_synced_wallet");

    for scale in SYNC_SCALES {
        group.bench_with_input(BenchmarkId::new("redb", scale.name), &scale, |b, &scale| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let db_path = dir.path().join("db.redb");
                    {
                        let db = Arc::new(Database::create(&db_path).unwrap());
                        let mut store = RedbStore::new(db, "wallet".to_string()).unwrap();
                        let mut wallet = create_wallet(&mut store);
                        simulate_sync(
                            &mut wallet,
                            scale.addresses,
                            scale.blocks,
                            scale.txs_per_block,
                            scale.unconfirmed_txs,
                        );
                        wallet.persist(&mut store).unwrap();
                    }
                    let db = Arc::new(Database::open(&db_path).unwrap());
                    let store = RedbStore::new(db, "wallet".to_string()).unwrap();
                    (store, dir)
                },
                |(mut store, dir)| {
                    let res = load_wallet(&mut store);
                    (res, store, dir)
                },
                BatchSize::PerIteration,
            );
        });

        group.bench_with_input(
            BenchmarkId::new("file_store", scale.name),
            &scale,
            |b, &scale| {
                b.iter_batched(
                    || {
                        let dir = tempdir().unwrap();
                        let db_path = dir.path().join("db_file");
                        {
                            let mut store =
                                FileStore::<ChangeSet>::create(&TEST_MAGIC_BYTES, &db_path)
                                    .unwrap();
                            let mut wallet = create_wallet(&mut store);
                            simulate_sync(
                                &mut wallet,
                                scale.addresses,
                                scale.blocks,
                                scale.txs_per_block,
                                scale.unconfirmed_txs,
                            );
                            wallet.persist(&mut store).unwrap();
                        }
                        let (store, _) =
                            FileStore::<ChangeSet>::load(&TEST_MAGIC_BYTES, &db_path).unwrap();
                        (store, dir)
                    },
                    |(mut store, dir)| {
                        let res = load_wallet(&mut store);
                        (res, store, dir)
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    persist_desc_network,
    bench_persist_synced_wallet,
    bench_load_synced_wallet,
);
criterion_main!(benches);
