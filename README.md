# bdk_redb

## About

The `bdk_redb` project provides a [`redb`](https://docs.rs/redb/2.6.0/redb/index.html) based persistence backend for [`bdk_wallet`](https://docs.rs/bdk_wallet/2.0.0/bdk_wallet/index.html) and [`bdk_chain`](https://docs.rs/bdk_chain/0.23.0/bdk_chain/index.html).

## Status
The crate is currently EXPERIMENTAL. DO NOT use with MAINNET wallets.

## Architecture

There is currently one published crate in this repository:

- `bdk_redb`: Contains [`Store`](./src/lib.rs) type (that wraps around the [`redb`](https://docs.rs/redb/2.6.0/redb/index.html) database) along with persistence methods.

## Features
The crate has a default feature called `wallet` which provides methods on [`Store`](./src/lib.rs) to persist [`bdk_wallet::ChangeSet`](http://docs.rs/bdk_wallet/2.0.0/bdk_wallet/struct.ChangeSet.html) and [`bdk_wallet::WalletPersister`](https://docs.rs/bdk_wallet/2.0.0/bdk_wallet/trait.WalletPersister.html) implementation for [`Store`](./src/lib.rs).

## Minimum Supported Rust Version (MSRV)
The library maintains a MSRV of 1.85.0 due to dependency on [`redb`](https://docs.rs/redb/2.6.0/redb/index.html). 

## License

Licensed under either of

* Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.