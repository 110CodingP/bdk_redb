# bdk_redb

## About

The `bdk_redb` project provides a `redb` based persistence backend for `bdk_wallet` and `bdk_chain`.

## Architecture

There is currently only one published crate in this repository:

- `bdk_redb`: Contains `Store` type (that wraps around the `redb` database) along with persistence methods.

## Minimum Supported Rust Version (MSRV)


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