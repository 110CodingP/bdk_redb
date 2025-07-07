# bdk_redb

<div align="center">
  <h1>bdk_redb</h1>

  <img src="./static/bdk.png" width="220" />

  <p>
    <strong>A persistence backend for BDK using redb!</strong>
  </p>

</div>

## About

The `bdk_redb` project provides a high level descriptor based wallet API for building Bitcoin applications.
It is built upon the excellent [`rust-bitcoin`] and [`rust-miniscript`] crates.

## Architecture

There is currently only one published crate in this repository:

- [`wallet`](./wallet): Contains the central high level `Wallet` type that is built from the low-level mechanisms provided by the other components.

Crates that `bdk_wallet` depends on are found in the [`bdk`] repository.

Fully working examples of how to use these components are in `/examples`:

- [`example_wallet_esplora_blocking`](examples/example_wallet_esplora_blocking): Uses the `Wallet` to sync and spend using the Esplora blocking interface.
- [`example_wallet_esplora_async`](examples/example_wallet_esplora_async): Uses the `Wallet` to sync and spend using the Esplora asynchronous interface.
- [`example_wallet_electrum`](examples/example_wallet_electrum): Uses the `Wallet` to sync and spend using Electrum.

[`bdk`]: https://github.com/bitcoindevkit/bdk
[`rust-miniscript`]: https://github.com/rust-bitcoin/rust-miniscript
[`rust-bitcoin`]: https://github.com/rust-bitcoin/rust-bitcoin

## Minimum Supported Rust Version (MSRV)

The libraries in this repository maintain a MSRV of 1.63.0.

To build with the MSRV of 1.63.0 you will need to pin dependencies by running the [`pin-msrv.sh`](./ci/pin-msrv.sh) script.

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