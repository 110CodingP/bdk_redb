alias b := build
alias c := check
alias f := fmt
alias t := test
alias p := pre-push

default:
  @just --list

# Build the project
build:
   cargo build
   cargo build --no-default-features

# Check code: formatting, compilation, linting, and commit signature
check:
   cargo +nightly fmt -- --check
   cargo check --all-features
   cargo clippy --all-features --all-targets -- -D warnings
   @[ "$(git log --pretty='format:%G?' -1 HEAD)" = "N" ] && \
       echo "\n⚠️  Unsigned commit: BDK requires that commits be signed." || \
       true

# Format all code
fmt:
   cargo +nightly fmt

# Run all tests with all, default and no-default features
test:
   cargo test --all-features
   cargo test
   cargo test --no-default-features

# Generate doc
doc:
   cargo doc --open --all-features
   cargo doc --open
   cargo doc --open --no-default-features

# Generate code coverage
code_cov:
   mkdir coverage
   touch coverage/lcov.info
   cargo +nightly llvm-cov -q --doctests --branch --all-features --lcov --output-path ./coverage/lcov.info
   @genhtml -o coverage-report.html --ignore-errors unmapped ./coverage/lcov.info
   open ./coverage-report.html/index.html
   rm -rf coverage

# Run pre-push suite: format, check, and test
pre-push: fmt check test