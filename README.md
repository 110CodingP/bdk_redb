# bdk_redb

[![Crate Info](https://img.shields.io/crates/v/bdk_redb.svg)](https://crates.io/crates/bdk_redb)
![Rustc Version](https://img.shields.io/crates/msrv/bdk_redb)
[![Wallet API Docs](https://img.shields.io/badge/docs.rs-bdk_redb-green)](https://docs.rs/bdk_redb)
[![Coverage Status](https://coveralls.io/repos/github/110CodingP/bdk_redb/badge.svg)](https://coveralls.io/github/110CodingP/bdk_redb)
[![CI Status](https://github.com/110CodingP/bdk_redb/workflows/CI/badge.svg)](https://github.com/110CodingP/bdk_redb/actions?query=workflow:CI)
[![MIT or Apache-2.0 Licensed](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](https://github.com/110CodingP/bdk_redb/blob/master/LICENSE)

## About

The [`bdk_redb`](https://crates.io/crates/bdk_redb) project provides a [`redb`](https://crates.io/crates/redb) based persistence backend for [`bdk_wallet`](https://crates.io/crates/bdk_wallet) and [`bdk_chain`](https://crates.io/crates/bdk_chain).

## Status
The crate is currently EXPERIMENTAL. DO NOT use with MAINNET wallets.

## Architecture

There is currently one published crate in this repository:

- [`bdk_redb`](https://crates.io/crates/bdk_redb): Contains [`Store`](./src/lib.rs) type (that wraps around the [`redb`](https://crates.io/crates/redb) database) along with persistence methods.

## Features
The crate has a default feature called `wallet` which provides methods on [`Store`](./src/lib.rs) to persist [`bdk_wallet::ChangeSet`](http://docs.rs/bdk_wallet/2.0.0/bdk_wallet/struct.ChangeSet.html) and [`bdk_wallet::WalletPersister`](https://docs.rs/bdk_wallet/2.0.0/bdk_wallet/trait.WalletPersister.html) implementation for [`Store`](./src/lib.rs).

## Minimum Supported Rust Version (MSRV)
The library maintains a MSRV of 1.89.0 due to dependency on [`redb`](https://crates.io/crates/redb). 

## Contributing

The `bdk_redb` project operates an open contributor model where anyone is welcome to contribute towards development in the form of peer review, documentation, testing, and patches.

### Contribution Workflow

1. **Fork & Branch**: Fork the repository and create a dedicated topic branch for your feature or fix.
2. **Commits**: Ensure commits are atomic, self-contained, and easy to review.
3. **Testing & Documentation**: Ensure new features are covered by tests and public APIs are documented.

### First-Time Contributors

Please avoid submitting pull requests solely for fixing spelling mistakes, typos, or renaming items, as these will not be prioritized by the maintainers. First-time contributors are encouraged to tackle more substantial tasks such as resolving open issues, adding functional tests, or enhancing documentation.

### Using `justfile`

We use [`just`](https://github.com/casey/just) to streamline development commands. Key shortcuts include:

- `just b`: Build the project with all features.
- `just c`: Run code checks (formatting, compilation, clippy, commit signature).
- `just f`: Format code with `cargo +nightly fmt`.
- `just t`: Run tests across some feature configurations.
- `just p`: Run the pre-push check suite (`fmt`, `check`, `test`).

Please run `just p` before submitting a pull request.

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
